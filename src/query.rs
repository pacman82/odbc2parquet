use core::panic;
use std::{
    borrow::Cow,
    convert::TryInto,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{format_err, Error};
use log::{debug, info, warn};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarRowSet},
    ColumnDescription, Cursor, DataType, Environment, IntoParameter, Nullability,
};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
    },
    schema::types::{Type, TypePtr},
};

use crate::{open_connection, parquet_buffer::ParquetBuffer, QueryOpt};

/// Execute a query and writes the result to parquet.
pub fn query(environment: &Environment, opt: &QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size,
        batches_per_file,
        encoding,
    } = opt;

    // Convert the input strings into parameters suitable to for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.into_parameter())
        .collect();

    let odbc_conn = open_connection(&environment, connect_opts)?;

    if let Some(cursor) = odbc_conn.execute(query, params.as_slice())? {
        cursor_to_parquet(
            cursor,
            output,
            *batch_size,
            *batches_per_file,
            encoding.use_utf16(),
        )?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

fn cursor_to_parquet(
    cursor: impl Cursor,
    path: &Path,
    batch_size: u32,
    batches_per_file: u32,
    use_utf16: bool,
) -> Result<(), Error> {
    info!("Batch size set to {}", batch_size);

    let (parquet_schema, buffer_description) = make_schema(&cursor, use_utf16)?;
    let mut odbc_buffer = ColumnarRowSet::with_column_indices(
        batch_size,
        buffer_description
            .iter()
            .map(|(index, desc, _write_column)| (*index, *desc)),
    );
    let mut row_set_cursor = cursor.bind_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size as usize);
    let mut num_batch = 0;

    let mut writer =
        ParquetWriter::new(path, batch_size, parquet_schema.clone(), batches_per_file)?;

    while let Some(buffer) = row_set_cursor.fetch()? {
        let mut row_group_writer = writer.next_row_group(num_batch)?;
        let mut col_index = 0;
        num_batch += 1;
        let num_rows = buffer.num_rows();
        info!("Fetched batch {} with {} rows.", num_batch, num_rows);
        pb.set_num_rows_fetched(num_rows);
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            let col_name = parquet_schema.get_fields()[col_index]
                .get_basic_info()
                .name();
            debug!(
                "Writing column with index {} and name '{}'.",
                col_index, col_name
            );

            let odbc_column = buffer.column(col_index);

            let (_, _, ref odbc_to_parquet_col) = buffer_description[col_index];

            odbc_to_parquet_col(&mut pb, &mut column_writer, odbc_column)?;

            row_group_writer.close_column(column_writer)?;
            col_index += 1;
        }
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

/// Function used to copy the contents of `AnyColumnView` into `ColumnWriter`. The concrete instance
/// (not this signature) is dependent on the specif columns in questions.
type FnWriteParquetColumn =
    dyn Fn(&mut ParquetBuffer, &mut ColumnWriter, AnyColumnView) -> Result<(), Error>;

macro_rules! optional_col_writer {
    ($cw_variant:ident, $cr_variant:ident) => {
        Box::new(
            move |pb: &mut ParquetBuffer,
                  column_writer: &mut ColumnWriter,
                  column_reader: AnyColumnView| {
                if let (ColumnWriter::$cw_variant(cw), AnyColumnView::$cr_variant(it)) =
                    (column_writer, column_reader)
                {
                    pb.write_optional(cw, it)?
                } else {
                    panic_invalid_cw()
                }
                Ok(())
            },
        )
    };
}

fn write_decimal_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    length_in_bytes: usize,
    precision: usize,
) -> Result<(), Error> {
    if let (ColumnWriter::FixedLenByteArrayColumnWriter(cw), AnyColumnView::Text(it)) =
        (column_writer, column_reader)
    {
        pb.write_decimal(cw, it, length_in_bytes, precision)?;
    } else {
        panic_invalid_cw()
    }
    Ok(())
}

fn write_timestamp_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    precision: i16,
) -> Result<(), Error> {
    if let (ColumnWriter::Int64ColumnWriter(cw), AnyColumnView::NullableTimestamp(it)) =
        (column_writer, column_reader)
    {
        pb.write_timestamp(cw, it, precision)?;
    } else {
        panic_invalid_cw()
    }
    Ok(())
}

fn write_utf16_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::WText(it)) =
        (column_writer, column_reader)
    {
        pb.write_optional(
            cw,
            it.map(|item| {
                item.map(|ustr| {
                    ustr.to_string()
                        .expect("Data source must return valid UTF16 in wide character buffer")
                })
            }),
        )?;
    } else {
        panic_invalid_cw()
    }
    Ok(())
}

fn write_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::Text(it)) =
        (column_writer, column_reader)
    {
        pb.write_optional(cw, it.map(|item| item.map(bytes_to_string)))?;
    } else {
        panic_invalid_cw()
    }
    Ok(())
}

fn panic_invalid_cw() -> ! {
    panic!(
        "Invalid ColumnWriter type. This is not supposed to happen. Please \
        open a Bug at https://github.com/pacman82/odbc2parquet/issues."
    )
}

fn make_schema(
    cursor: &impl Cursor,
    use_utf16: bool,
) -> Result<
    (
        TypePtr,
        Vec<(u16, BufferDescription, Box<FnWriteParquetColumn>)>,
    ),
    Error,
> {
    let num_cols = cursor.num_result_cols()?;

    let mut odbc_buffer_desc = Vec::new();
    let mut fields = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        // Reserving helps with drivers not reporting column name size correctly.
        cd.name.reserve(128);
        cursor.describe_col(index as u16, &mut cd)?;

        debug!("ODBC column description for column {}: {:?}", index, cd);

        let name = cd.name_to_string()?;
        // Give a generated name, should we fail to retrieve one from the ODBC data source.
        let name = if name.is_empty() {
            format!("Column{}", index)
        } else {
            name
        };

        let ptb = |physical_type| Type::primitive_type_builder(&name, physical_type);

        let (field_builder, buffer_kind, odbc_to_parquet_column): (
            _,
            _,
            Box<FnWriteParquetColumn>,
        ) = match cd.data_type {
            DataType::Double => (
                ptb(PhysicalType::DOUBLE),
                BufferKind::F64,
                optional_col_writer!(DoubleColumnWriter, NullableF64),
            ),
            DataType::Float | DataType::Real => (
                ptb(PhysicalType::FLOAT),
                BufferKind::F32,
                optional_col_writer!(FloatColumnWriter, NullableF32),
            ),
            DataType::SmallInt => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_16),
                BufferKind::I32,
                optional_col_writer!(Int32ColumnWriter, NullableI32),
            ),
            DataType::Integer => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_32),
                BufferKind::I32,
                optional_col_writer!(Int32ColumnWriter, NullableI32),
            ),
            DataType::Date => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::DATE),
                BufferKind::Date,
                optional_col_writer!(Int32ColumnWriter, NullableDate),
            ),
            DataType::Decimal {
                scale: 0,
                precision: p @ 0..=9,
            }
            | DataType::Numeric {
                scale: 0,
                precision: p @ 0..=9,
            } => (
                ptb(PhysicalType::INT32)
                    .with_logical_type(LogicalType::DECIMAL)
                    .with_precision(p as i32)
                    .with_scale(0),
                BufferKind::I32,
                optional_col_writer!(Int32ColumnWriter, NullableI32),
            ),
            DataType::Decimal {
                scale: 0,
                precision: p @ 0..=18,
            }
            | DataType::Numeric {
                scale: 0,
                precision: p @ 0..=18,
            } => (
                ptb(PhysicalType::INT64)
                    .with_logical_type(LogicalType::DECIMAL)
                    .with_precision(p as i32)
                    .with_scale(0),
                BufferKind::I64,
                optional_col_writer!(Int64ColumnWriter, NullableI64),
            ),
            DataType::Numeric { scale, precision } | DataType::Decimal { scale, precision } => {
                // Length of the two's complement.
                let num_binary_digits = precision as f64 * 10f64.log2();
                // Plus one bit for the sign (+/-)
                let length_in_bits = num_binary_digits + 1.0;
                let length_in_bytes = (length_in_bits / 8.0).ceil() as i32;
                (
                    ptb(PhysicalType::FIXED_LEN_BYTE_ARRAY)
                        .with_length(length_in_bytes)
                        .with_logical_type(LogicalType::DECIMAL)
                        .with_precision(precision.try_into().unwrap())
                        .with_scale(scale.try_into().unwrap()),
                    BufferKind::Text {
                        max_str_len: cd.data_type.column_size(),
                    },
                    Box::new(
                        move |pb: &mut ParquetBuffer,
                              column_writer: &mut ColumnWriter,
                              column_reader: AnyColumnView| {
                            write_decimal_col(
                                pb,
                                column_writer,
                                column_reader,
                                length_in_bytes.try_into().unwrap(),
                                precision,
                            )
                        },
                    ),
                )
            }
            DataType::Timestamp { precision } => (
                ptb(PhysicalType::INT64).with_logical_type(if precision <= 3 {
                    LogicalType::TIMESTAMP_MILLIS
                } else {
                    LogicalType::TIMESTAMP_MICROS
                }),
                BufferKind::Timestamp,
                Box::new(
                    move |pb: &mut ParquetBuffer,
                          column_writer: &mut ColumnWriter,
                          column_reader: AnyColumnView| {
                        write_timestamp_col(pb, column_writer, column_reader, precision)
                    },
                ),
            ),
            DataType::BigInt => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::INT_64),
                BufferKind::I64,
                optional_col_writer!(Int64ColumnWriter, NullableI64),
            ),
            DataType::Bit => (
                ptb(PhysicalType::BOOLEAN),
                BufferKind::Bit,
                optional_col_writer!(BoolColumnWriter, NullableBit),
            ),
            DataType::TinyInt => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_8),
                BufferKind::I32,
                optional_col_writer!(Int32ColumnWriter, NullableI32),
            ),
            DataType::Binary { length } => (
                ptb(PhysicalType::FIXED_LEN_BYTE_ARRAY)
                    .with_length(length.try_into().unwrap())
                    .with_logical_type(LogicalType::NONE),
                BufferKind::Binary { length },
                optional_col_writer!(FixedLenByteArrayColumnWriter, Binary),
            ),
            DataType::Varbinary { length } => (
                ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::NONE),
                BufferKind::Binary { length },
                optional_col_writer!(ByteArrayColumnWriter, Binary),
            ),
            // For character data we consider binding to wide (16-Bit) buffers in order to avoid
            // depending on the system locale being utf-8. For other character buffers we always use
            // narrow (8-Bit) buffers, since we expect decimals, timestamps and so on to always be
            // represented in ASCII characters.
            DataType::Char { length }
            | DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::WChar { length } => {
                if use_utf16 {
                    (
                        ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                        BufferKind::WText {
                            // One UTF-16 code point may consist of up to two bytes.
                            max_str_len: length * 2,
                        },
                        Box::new(write_utf16_to_utf8),
                    )
                } else {
                    (
                        ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                        BufferKind::Text {
                            // One UTF-8 code point may consist of up to four bytes.
                            max_str_len: length * 4,
                        },
                        Box::new(write_utf8),
                    )
                }
            }
            DataType::Unknown | DataType::Time { .. } | DataType::Other { .. } => {
                let max_str_len = if let Some(len) = cd.data_type.utf8_len() {
                    len
                } else {
                    cursor.col_display_size(index.try_into().unwrap())? as usize
                };
                (
                    ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                    BufferKind::Text { max_str_len },
                    Box::new(write_utf8),
                )
            }
        };

        let buffer_description = BufferDescription {
            kind: buffer_kind,
            nullable: true,
        };

        debug!(
            "ODBC buffer description for column {}: {:?}",
            index, buffer_description
        );

        let repetition = match cd.nullability {
            Nullability::Nullable | Nullability::Unknown => Repetition::OPTIONAL,
            Nullability::NoNulls => Repetition::REQUIRED,
        };

        if matches!(buffer_kind, BufferKind::Text { max_str_len: 0 }) {
            warn!(
                "Ignoring column '{}' with index {}. Driver reported a display length of 0. \
              This can happen for types without a fixed size limit. If you feel this should be \
              supported open an issue (or PR) at \
              <https://github.com/pacman82/odbc2parquet/issues>.",
                name, index
            );
        } else {
            let field_builder = field_builder.with_repetition(repetition);
            fields.push(Arc::new(field_builder.build()?));
            odbc_buffer_desc.push((index as u16, buffer_description, odbc_to_parquet_column));
        }
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()?;

    Ok((Arc::new(schema), odbc_buffer_desc))
}

/// Wraps parquet SerializedFileWriter. Handles splitting into new files after maximum amount of
/// batches is reached.
struct ParquetWriter<'p> {
    path: &'p Path,
    schema: Arc<Type>,
    properties: Arc<WriterProperties>,
    writer: SerializedFileWriter<File>,
    batches_per_file: u32,
}

impl<'p> ParquetWriter<'p> {
    pub fn new(
        path: &'p Path,
        batch_size: u32,
        schema: Arc<Type>,
        batches_per_file: u32,
    ) -> Result<Self, Error> {
        // Write properties
        // Seems to also work fine without setting the batch size explicitly, but what the heck. Just to
        // be on the safe side.
        let wpb = WriterProperties::builder().set_write_batch_size(batch_size as usize);
        let properties = Arc::new(wpb.build());
        let file = if batches_per_file == 0 {
            File::create(path)?
        } else {
            File::create(Self::path_with_suffix(path, "_1")?)?
        };
        let writer = SerializedFileWriter::new(file, schema.clone(), properties.clone())?;

        Ok(Self {
            path,
            schema,
            properties,
            writer,
            batches_per_file,
        })
    }

    /// Retrieve the next row group writer. May trigger creation of a new file if limit of the
    /// previous one is reached.
    ///
    /// # Parameters
    ///
    /// * `num_batch`: Zero based num batch index
    pub fn next_row_group(&mut self, num_batch: u32) -> Result<Box<dyn RowGroupWriter>, Error> {
        // Check if we need to write the next batch into a new file
        if num_batch != 0 && self.batches_per_file != 0 && num_batch % self.batches_per_file == 0 {
            self.writer.close()?;
            let suffix = format!("_{}", (num_batch / self.batches_per_file) + 1);
            let path = Self::path_with_suffix(self.path, &suffix)?;
            let file = File::create(path)?;
            self.writer =
                SerializedFileWriter::new(file, self.schema.clone(), self.properties.clone())?;
        }
        Ok(self.writer.next_row_group()?)
    }

    fn close_row_group(
        &mut self,
        row_group_writer: Box<dyn RowGroupWriter>,
    ) -> Result<(), ParquetError> {
        self.writer.close_row_group(row_group_writer)
    }

    pub fn close(&mut self) -> Result<(), ParquetError> {
        self.writer.close()
    }

    fn path_with_suffix(path: &Path, suffix: &str) -> Result<PathBuf, Error> {
        let mut stem = path
            .file_stem()
            .ok_or_else(|| format_err!("Output needs To have a file stem."))?
            .to_owned();
        stem.push(suffix);
        let mut path_with_suffix = path.with_file_name(stem);
        path_with_suffix = path_with_suffix.with_extension("par");
        Ok(path_with_suffix)
    }
}

fn bytes_to_string(bytes: &[u8]) -> String {
    // Allocate string into a ByteArray and make sure it is all UTF-8 characters
    let utf8_str = String::from_utf8_lossy(bytes);
    // We need to allocate the string anyway to create a ByteArray (yikes!), yet if it already
    // happened after the to_string_lossy method, it implies we had to use a replacement
    // character!
    if matches!(utf8_str, Cow::Owned(_)) {
        warn!(
            "Non UTF-8 characters found in string. Try to execute odbc2parquet in a shell with \
        UTF-8 locale or try specifying `--encoding Utf16` on the command line. Value: {}",
            utf8_str
        );
    }
    utf8_str.into_owned()
}
