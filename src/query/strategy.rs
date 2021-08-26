use std::{borrow::Cow, convert::TryInto};

use anyhow::Error;
use log::{debug, warn};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
    ColumnDescription, Cursor, DataType, Nullability,
};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::{
        BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
    },
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

/// Function used to copy the contents of `AnyColumnView` into `ColumnWriter`. The concrete instance
/// (not this signature) is dependent on the specific columns in questions.
pub type FnWriteParquetColumn =
    dyn Fn(&mut ParquetBuffer, &mut ColumnWriter, AnyColumnView) -> Result<(), Error>;

/// All information required to fetch a column from odbc and transfer its data to Parquet.
pub struct ColumnFetchStrategy {
    /// Parquet column type used in parquet schema
    pub parquet_type: Type,
    /// Description of the buffer bound to the ODBC data source.
    pub buffer_description: BufferDescription,
    /// Function writing the data from an ODBC buffer with a parquet column writer.
    pub odbc_to_parquet: Box<FnWriteParquetColumn>,
}

macro_rules! optional_col_writer {
    ($pdt:ident, $cr_variant:ident) => {
        Box::new(
            move |pb: &mut ParquetBuffer,
                  column_writer: &mut ColumnWriter,
                  column_reader: AnyColumnView| {
                let cw = get_typed_column_writer_mut::<$pdt>(column_writer);
                if let AnyColumnView::$cr_variant(it) = column_reader {
                    pb.write_optional(cw, it)?
                } else {
                    panic_invalid_cv()
                }
                Ok(())
            },
        )
    };
}

pub fn strategy_from_column_description(
    cd: &ColumnDescription,
    name: &str,
    prefer_varbinary: bool,
    use_utf16: bool,
    cursor: &impl Cursor,
    index: i16,
) -> Result<Option<ColumnFetchStrategy>, Error> {
    let ptb = |physical_type| Type::primitive_type_builder(name, physical_type);

    let (field_builder, buffer_kind, odbc_to_parquet): (_, _, Box<FnWriteParquetColumn>) =
        match cd.data_type {
            DataType::Double => (
                ptb(PhysicalType::DOUBLE),
                BufferKind::F64,
                optional_col_writer!(DoubleType, NullableF64),
            ),
            DataType::Float | DataType::Real => (
                ptb(PhysicalType::FLOAT),
                BufferKind::F32,
                optional_col_writer!(FloatType, NullableF32),
            ),
            DataType::SmallInt => (
                ptb(PhysicalType::INT32).with_converted_type(ConvertedType::INT_16),
                BufferKind::I32,
                optional_col_writer!(Int32Type, NullableI32),
            ),
            DataType::Integer => (
                ptb(PhysicalType::INT32).with_converted_type(ConvertedType::INT_32),
                BufferKind::I32,
                optional_col_writer!(Int32Type, NullableI32),
            ),
            DataType::Date => (
                ptb(PhysicalType::INT32).with_converted_type(ConvertedType::DATE),
                BufferKind::Date,
                optional_col_writer!(Int32Type, NullableDate),
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
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_precision(p as i32)
                    .with_scale(0),
                BufferKind::I32,
                optional_col_writer!(Int32Type, NullableI32),
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
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_precision(p as i32)
                    .with_scale(0),
                BufferKind::I64,
                optional_col_writer!(Int64Type, NullableI64),
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
                        .with_converted_type(ConvertedType::DECIMAL)
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
                ptb(PhysicalType::INT64).with_converted_type(if precision <= 3 {
                    ConvertedType::TIMESTAMP_MILLIS
                } else {
                    ConvertedType::TIMESTAMP_MICROS
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
                ptb(PhysicalType::INT64).with_converted_type(ConvertedType::INT_64),
                BufferKind::I64,
                optional_col_writer!(Int64Type, NullableI64),
            ),
            DataType::Bit => (
                ptb(PhysicalType::BOOLEAN),
                BufferKind::Bit,
                optional_col_writer!(BoolType, NullableBit),
            ),
            DataType::TinyInt => (
                ptb(PhysicalType::INT32).with_converted_type(ConvertedType::INT_8),
                BufferKind::I32,
                optional_col_writer!(Int32Type, NullableI32),
            ),
            DataType::Binary { length } => {
                if prefer_varbinary {
                    (
                        ptb(PhysicalType::BYTE_ARRAY).with_converted_type(ConvertedType::NONE),
                        BufferKind::Binary { length },
                        optional_col_writer!(ByteArrayType, Binary),
                    )
                } else {
                    (
                        ptb(PhysicalType::FIXED_LEN_BYTE_ARRAY)
                            .with_length(length.try_into().unwrap())
                            .with_converted_type(ConvertedType::NONE),
                        BufferKind::Binary { length },
                        optional_col_writer!(FixedLenByteArrayType, Binary),
                    )
                }
            }
            DataType::Varbinary { length } | DataType::LongVarbinary { length } => (
                ptb(PhysicalType::BYTE_ARRAY).with_converted_type(ConvertedType::NONE),
                BufferKind::Binary { length },
                optional_col_writer!(ByteArrayType, Binary),
            ),
            // For character data we consider binding to wide (16-Bit) buffers in order to avoid
            // depending on the system locale being utf-8. For other character buffers we always use
            // narrow (8-Bit) buffers, since we expect decimals, timestamps and so on to always be
            // represented in ASCII characters.
            DataType::Char { length }
            | DataType::Varchar { length }
            | DataType::WVarchar { length }
            | DataType::LongVarchar { length }
            | DataType::WChar { length } => {
                if use_utf16 {
                    (
                        ptb(PhysicalType::BYTE_ARRAY).with_converted_type(ConvertedType::UTF8),
                        BufferKind::WText {
                            // One UTF-16 code point may consist of up to two bytes.
                            max_str_len: length * 2,
                        },
                        Box::new(write_utf16_to_utf8),
                    )
                } else {
                    (
                        ptb(PhysicalType::BYTE_ARRAY).with_converted_type(ConvertedType::UTF8),
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
                    ptb(PhysicalType::BYTE_ARRAY).with_converted_type(ConvertedType::UTF8),
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

    if matches!(
        buffer_kind,
        BufferKind::Text { max_str_len: 0 } | BufferKind::WText { max_str_len: 0 }
    ) {
        warn!(
            "Ignoring column '{}' with index {}. Driver reported a display length of 0. \
              This can happen for types without a fixed size limit. If you feel this should be \
              supported open an issue (or PR) at \
              <https://github.com/pacman82/odbc2parquet/issues>.",
            name, index
        );

        Ok(None)
    } else {
        let parquet_type = field_builder.with_repetition(repetition).build()?;

        Ok(Some(ColumnFetchStrategy {
            parquet_type,
            buffer_description,
            odbc_to_parquet,
        }))
    }
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
        panic_invalid_cv()
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
        panic_invalid_cv()
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
        panic_invalid_cv()
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
        panic_invalid_cv()
    }
    Ok(())
}

fn panic_invalid_cv() -> ! {
    panic!(
        "Invalid Column view type. This is not supposed to happen. Please \
        open a Bug at https://github.com/pacman82/odbc2parquet/issues."
    )
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
