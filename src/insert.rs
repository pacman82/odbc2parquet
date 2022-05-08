use core::panic;
use std::{
    fs::File,
    io::Write,
    marker::PhantomData,
    ops::{Add, DivAssign, MulAssign},
};

use anyhow::{bail, Error};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};
use log::info;
use num_traits::{FromPrimitive, PrimInt, Signed, ToPrimitive};
use odbc_api::{
    buffers::{
        buffer_from_description, AnyColumnViewMut, BinColumnWriter, BufferDescription, BufferKind,
        NullableSliceMut, TextColumnWriter,
    },
    sys::{Date, Timestamp},
    Bit, Environment, U16String,
};
use parquet::{
    basic::{ConvertedType, Type as PhysicalType},
    column::reader::ColumnReader,
    data_type::{
        AsBytes, BoolType, ByteArrayType, DataType, DoubleType, FixedLenByteArrayType, FloatType,
        Int32Type, Int64Type,
    },
    file::reader::{FileReader, SerializedFileReader},
    schema::types::ColumnDescriptor,
};

use crate::{
    open_connection,
    parquet_buffer::{BufferedDataType, ParquetBuffer},
    InsertOpt,
};

/// Message we emmit if we hit a code path we expected to be unreachable.
const BUG: &str = "This is not supposed to happen. Please open a Bug at \
                  https://github.com/pacman82/odbc2parquet/issues.";

/// Read the content of a parquet file and insert it into a table.
pub fn insert(odbc_env: &Environment, insert_opt: &InsertOpt) -> Result<(), Error> {
    let InsertOpt {
        encoding,
        input,
        connect_opts,
        table,
    } = insert_opt;

    let odbc_conn = open_connection(odbc_env, connect_opts)?;

    let file = File::open(&input)?;
    let reader = SerializedFileReader::new(file)?;

    let parquet_metadata = reader.metadata();
    let schema_desc = parquet_metadata.file_metadata().schema_descr();
    let num_columns = schema_desc.num_columns();

    let column_descriptions: Vec<_> = (0..num_columns).map(|i| schema_desc.column(i)).collect();
    let column_names: Vec<&str> = column_descriptions
        .iter()
        .map(|col_desc| col_desc.name())
        .collect();
    let column_buf_desc: Vec<_> = column_descriptions
        .iter()
        .map(|col_desc| parquet_type_to_odbc_buffer_desc(col_desc, encoding.use_utf16()))
        .collect::<Result<_, _>>()?;
    let insert_statement = insert_statement_text(table, &column_names);

    let mut statement = odbc_conn.prepare(&insert_statement)?;

    let num_row_groups = reader.num_row_groups();

    // Start with a small initial batch size and reallocate as we encounter larger row groups.
    let mut batch_size = 1;
    let mut odbc_buffer = buffer_from_description(
        batch_size,
        column_buf_desc.iter().map(|(desc, _copy_col)| *desc),
    );

    let mut pb = ParquetBuffer::new(batch_size as usize);

    for row_group_index in 0..num_row_groups {
        info!(
            "Insert row group {} of {}.",
            row_group_index, num_row_groups
        );
        let row_group_reader = reader.get_row_group(row_group_index)?;
        let num_rows: usize = row_group_reader
            .metadata()
            .num_rows()
            .try_into()
            .expect("Number of rows in row group of parquet file must be non negative");
        // Ensure that num rows is less than batch size of originally created buffers.
        if num_rows > batch_size as usize {
            batch_size = num_rows;
            let descs = column_buf_desc.iter().map(|(desc, _)| *desc);
            odbc_buffer = buffer_from_description(batch_size, descs);
        }
        odbc_buffer.set_num_rows(num_rows);
        pb.set_num_rows_fetched(num_rows);
        for (column_index, (_, parquet_to_odbc_col)) in column_buf_desc.iter().enumerate() {
            let column_reader = row_group_reader.get_column_reader(column_index)?;
            let column_writer = odbc_buffer.column_mut(column_index);
            parquet_to_odbc_col(num_rows, &mut pb, column_reader, column_writer)?;
        }

        statement.execute(&odbc_buffer)?;
    }

    Ok(())
}

/// Function extracting the contents of a single column out of the Parquet column reader and into an
/// ODBC buffer.
type FnParquetToOdbcCol =
    dyn Fn(usize, &mut ParquetBuffer, ColumnReader, AnyColumnViewMut) -> Result<(), Error>;

fn insert_statement_text(table: &str, column_names: &[&str]) -> String {
    // Generate statement text from table name and headline
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let statement_text = format!("INSERT INTO {} ({}) VALUES ({});", table, columns, values);
    info!("Insert statement Text: {}", statement_text);
    statement_text
}

/// We extend the parquet `DataType` to start of our builder pattern. These builders constructs the
/// functors we use to transfer data from Parquet to ODBC.
trait InserterBuilderStart: DataType + Sized {
    fn map_to_text<F>(f: F, nullable: bool) -> Box<FnParquetToOdbcCol>
    where
        F: Fn(&Self::T, usize, &mut TextColumnWriter<u8>) + 'static,
        Self::T: BufferedDataType,
    {
        if nullable {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = Text::unwrap_writer_optional(column_writer);
                    let it = pb.read_optional(&mut cr, num_rows)?;
                    for (index, opt) in it.enumerate() {
                        if let Some(value) = opt {
                            f(value, index, &mut cw);
                        } else {
                            cw.set_value(index, None);
                        }
                    }
                    Ok(())
                },
            )
        } else {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = Text::unwrap_writer_optional(column_writer);
                    let it = pb.read_required(&mut cr, num_rows)?;
                    for (index, value) in it.enumerate() {
                        f(value, index, &mut cw);
                    }
                    Ok(())
                },
            )
        }
    }

    fn map_to_wtext<F>(f: F, nullable: bool) -> Box<FnParquetToOdbcCol>
    where
        F: Fn(&Self::T, usize, &mut TextColumnWriter<u16>) + 'static,
        Self::T: BufferedDataType,
    {
        if nullable {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = WText::unwrap_writer_optional(column_writer);
                    let it = pb.read_optional(&mut cr, num_rows)?;
                    for (index, opt) in it.enumerate() {
                        if let Some(value) = opt {
                            f(value, index, &mut cw);
                        } else {
                            cw.set_value(index, None);
                        }
                    }
                    Ok(())
                },
            )
        } else {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = WText::unwrap_writer_optional(column_writer);
                    let it = pb.read_required(&mut cr, num_rows)?;
                    for (index, value) in it.enumerate() {
                        f(value, index, &mut cw);
                    }
                    Ok(())
                },
            )
        }
    }

    fn map_to_binary<F>(f: F, nullable: bool) -> Box<FnParquetToOdbcCol>
    where
        F: Fn(&Self::T, usize, &mut BinColumnWriter) + 'static,
        Self::T: BufferedDataType,
    {
        if nullable {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = Binary::unwrap_writer_optional(column_writer);
                    let it = pb.read_optional(&mut cr, num_rows)?;
                    for (index, value) in it.enumerate() {
                        if let Some(bytes) = value {
                            f(bytes, index, &mut cw);
                        }
                    }
                    Ok(())
                },
            )
        } else {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = Binary::unwrap_writer_optional(column_writer);
                    let it = pb.read_required(&mut cr, num_rows)?;
                    for (index, value) in it.enumerate() {
                        f(value, index, &mut cw);
                    }
                    Ok(())
                },
            )
        }
    }

    /// Generates a function which extracts values from a parquet column. Can be used for columns in
    /// which ODBC and Parquet representation is identical.
    fn map_identity(nullable: bool) -> Box<FnParquetToOdbcCol>
    where
        Self::T: BufferedDataType + Copy,
        Self: for<'a> OdbcDataType<
            'a,
            Required = &'a mut [<Self as DataType>::T],
            Optional = NullableSliceMut<'a, <Self as DataType>::T>,
        >,
    {
        if nullable {
            Box::new(
                |num_rows: usize,
                 pb: &mut ParquetBuffer,
                 column_reader: ColumnReader,
                 column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let mut cw = Self::unwrap_writer_optional(column_writer);
                    let it = pb.read_optional(&mut cr, num_rows)?;
                    cw.write(it.map(|opt| opt.copied()));
                    Ok(())
                },
            )
        } else {
            Box::new(
                |num_rows: usize,
                 _: &mut ParquetBuffer,
                 column_reader: ColumnReader,
                 column_writer: AnyColumnViewMut| {
                    let mut cr = Self::get_column_reader(column_reader).expect(BUG);
                    let values = Self::unwrap_writer_required(column_writer);
                    // Do not utilize parquet buffer. just pass the values through.
                    cr.read_batch(num_rows, None, None, values)?;
                    Ok(())
                },
            )
        }
    }

    fn map_to<Odt>() -> ParquetToOdbcBuilder<Self, Odt> {
        ParquetToOdbcBuilder {
            pdt: PhantomData,
            odt: PhantomData,
        }
    }
}

impl<T> InserterBuilderStart for T where T: DataType {}

struct ParquetToOdbcBuilder<Pdt: ?Sized, Odt> {
    pdt: PhantomData<Pdt>,
    odt: PhantomData<Odt>,
}

impl<Pdt, Odt> ParquetToOdbcBuilder<Pdt, Odt> {
    /// Generates a function which extracts values from a parquet column, applies a transformation
    /// and writes into an odbc buffer. This macro assumes there to be a plain slice in the ODBC
    /// buffer in case of a required column.
    fn with<F, E>(&self, f: F, nullable: bool) -> Box<FnParquetToOdbcCol>
    where
        Pdt: DataType,
        Odt: for<'a> OdbcDataType<'a, Required = &'a mut [E], Optional = NullableSliceMut<'a, E>>,
        F: Fn(&Pdt::T) -> E + 'static,
        Pdt::T: BufferedDataType,
    {
        if nullable {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Pdt::get_column_reader(column_reader).expect(BUG);
                    let mut cw = Odt::unwrap_writer_optional(column_writer);
                    let it = pb.read_optional(&mut cr, num_rows)?;
                    cw.write(it.map(|opt| opt.map(&f)));
                    Ok(())
                },
            )
        } else {
            Box::new(
                move |num_rows: usize,
                      pb: &mut ParquetBuffer,
                      column_reader: ColumnReader,
                      column_writer: AnyColumnViewMut| {
                    let mut cr = Pdt::get_column_reader(column_reader).expect(BUG);
                    let values = Odt::unwrap_writer_required(column_writer);
                    let it = pb.read_required(&mut cr, num_rows)?;
                    for (index, value) in it.enumerate() {
                        values[index] = f(value)
                    }
                    Ok(())
                },
            )
        }
    }
}

/// Takes a parquet column descriptor and chooses a strategy for inserting the column into the
/// database.
fn parquet_type_to_odbc_buffer_desc(
    col_desc: &ColumnDescriptor,
    use_utf16: bool,
) -> Result<(BufferDescription, Box<FnParquetToOdbcCol>), Error> {
    // Column name. Used in error messages.
    let name = col_desc.self_type().name();
    if !col_desc.self_type().is_primitive() {
        bail!(
            "Sorry, this tool is only able to insert primitive types. Column '{}' is not a \
            primitive type.",
            name
        );
    }
    let nullable = col_desc.self_type().is_optional();

    let lt = col_desc.converted_type();
    let pt = col_desc.physical_type();

    let unexpected = || {
        panic!(
            "Unexpected combination of Physical and Logical type. {:?} {:?}",
            pt, lt
        )
    };

    let (kind, parquet_to_odbc): (_, Box<FnParquetToOdbcCol>) = match pt {
        PhysicalType::BOOLEAN => match lt {
            ConvertedType::NONE => (
                BufferKind::Bit,
                BoolType::map_to::<Bit>().with(|&b| Bit(b as u8), nullable),
            ),
            _ => unexpected(),
        },
        PhysicalType::INT32 => match lt {
            // As buffer type int32 is perfectly ok, eventually we could be more precise with the
            // SQLDataType for the smaller integer variants.
            ConvertedType::NONE
            | ConvertedType::INT_32
            | ConvertedType::UINT_32
            | ConvertedType::INT_16
            | ConvertedType::UINT_16
            | ConvertedType::INT_8
            | ConvertedType::UINT_8 => (BufferKind::I32, Int32Type::map_identity(nullable)),
            ConvertedType::TIME_MILLIS => (
                // Time represented in format hh:mm:ss.fff
                BufferKind::Text { max_str_len: 12 },
                Int32Type::map_to_text(
                    |&milliseconds_since_midnight: &i32,
                     index: usize,
                     odbc_buf: &mut TextColumnWriter<u8>| {
                        let buf = odbc_buf.set_mut(index, 12);
                        write_as_time_ms(milliseconds_since_midnight, buf)
                    },
                    nullable,
                ),
            ),
            ConvertedType::DATE => (
                BufferKind::Date,
                Int32Type::map_to::<Date>().with(|&i| days_since_epoch_to_odbc_date(i), nullable),
            ),
            ConvertedType::DECIMAL => {
                let precision: usize = col_desc.type_precision().try_into().unwrap();
                let scale: usize = col_desc.type_scale().try_into().unwrap();
                // We need one character for each digit and maybe an additional character for the
                // decimal point and one sign.
                let max_str_len = if scale == 0 {
                    precision + 1
                } else {
                    precision + 1 + 1
                };
                (
                    BufferKind::Text { max_str_len },
                    Int32Type::map_to_text(
                        move |&n, index, odbc_buf| {
                            let buf = odbc_buf.set_mut(index, max_str_len);
                            write_integer_as_decimal(n, precision, scale, buf)
                        },
                        nullable,
                    ),
                )
            }
            _ => unexpected(),
        },
        PhysicalType::INT64 => match lt {
            ConvertedType::NONE | ConvertedType::INT_64 | ConvertedType::UINT_64 => {
                (BufferKind::I64, Int64Type::map_identity(nullable))
            }
            ConvertedType::TIME_MICROS => (
                // Time represented in format hh:mm::ss.ffffff
                BufferKind::Text { max_str_len: 15 },
                Int64Type::map_to_text(
                    |&microseconds_since_midnight: &i64,
                     index: usize,
                     odbc_buf: &mut TextColumnWriter<u8>| {
                        let buf = odbc_buf.set_mut(index, 15);
                        write_as_time_us(microseconds_since_midnight, buf)
                    },
                    nullable,
                ),
            ),
            ConvertedType::TIMESTAMP_MICROS => (
                BufferKind::Timestamp,
                Int64Type::map_to::<Timestamp>().with(
                    |&microseconds_since_epoch| {
                        let dt = NaiveDateTime::from_timestamp(
                            microseconds_since_epoch / 1_000_000,
                            ((microseconds_since_epoch % 1_000_000) * 1_000) as u32,
                        );
                        Timestamp {
                            year: dt.year().try_into().unwrap(),
                            month: dt.month() as u16,
                            day: dt.day() as u16,
                            hour: dt.hour() as u16,
                            minute: dt.minute() as u16,
                            second: dt.second() as u16,
                            fraction: dt.nanosecond(),
                        }
                    },
                    nullable,
                ),
            ),
            ConvertedType::TIMESTAMP_MILLIS => (
                BufferKind::Timestamp,
                Int64Type::map_to::<Timestamp>().with(
                    |&milliseconds_since_epoch| {
                        let dt = NaiveDateTime::from_timestamp(
                            milliseconds_since_epoch / 1000,
                            ((milliseconds_since_epoch % 1000) * 1_000_000) as u32,
                        );
                        Timestamp {
                            year: dt.year().try_into().unwrap(),
                            month: dt.month() as u16,
                            day: dt.day() as u16,
                            hour: dt.hour() as u16,
                            minute: dt.minute() as u16,
                            second: dt.second() as u16,
                            fraction: dt.nanosecond(),
                        }
                    },
                    nullable,
                ),
            ),
            ConvertedType::DECIMAL => {
                let precision: usize = col_desc.type_precision().try_into().unwrap();
                let scale: usize = col_desc.type_scale().try_into().unwrap();
                // We need one character for each digit and maybe an additional character for the
                // decimal point and one sign.
                let max_str_len = if scale == 0 {
                    precision + 1
                } else {
                    precision + 1 + 1
                };
                (
                    BufferKind::Text { max_str_len },
                    Int64Type::map_to_text(
                        move |&n, index, odbc_buf| {
                            let buf = odbc_buf.set_mut(index, max_str_len);
                            write_integer_as_decimal(n, precision, scale, buf)
                        },
                        nullable,
                    ),
                )
            }
            _ => unexpected(),
        },
        PhysicalType::INT96 => bail!(
            "'{}' is a column of type INT96. This tool currently offers no support for that type. \
            If you feel that it should, please raise an issue at \
            https://github.com/pacman82/odbc2parquet/issues.",
            name,
        ),
        PhysicalType::FLOAT => match lt {
            ConvertedType::NONE => (BufferKind::F32, FloatType::map_identity(nullable)),
            _ => unexpected(),
        },
        PhysicalType::DOUBLE => match lt {
            ConvertedType::NONE => (BufferKind::F64, DoubleType::map_identity(nullable)),
            _ => unexpected(),
        },
        PhysicalType::BYTE_ARRAY => {
            match lt {
                ConvertedType::UTF8 | ConvertedType::JSON | ConvertedType::ENUM => {
                    // Start small. We rebind the buffer as we encounter larger values in the file.
                    let max_str_len = 1;
                    if use_utf16 {
                        (
                            BufferKind::WText { max_str_len },
                            ByteArrayType::map_to_wtext(
                                move |text, index, odbc_buf| {
                                    // This allocation is not strictly neccessary, we could just as
                                    // write directly into the buffer or at least preallocate the
                                    // U16String.
                                    let value = U16String::from_str(
                                        text.as_utf8()
                                            .expect("Invalid UTF-8 sequence in parquet file."),
                                    );
                                    odbc_buf.append(index, Some(value.as_slice()))
                                },
                                nullable,
                            ),
                        )
                    } else {
                        (
                            BufferKind::Text { max_str_len },
                            ByteArrayType::map_to_text(
                                |text, index, odbc_buf| odbc_buf.append(index, Some(text.data())),
                                nullable,
                            ),
                        )
                    }
                }
                ConvertedType::NONE | ConvertedType::BSON => (
                    BufferKind::Binary { length: 1 },
                    ByteArrayType::map_to_binary(
                        |bytes, index, odbc_buf| odbc_buf.append(index, Some(bytes.as_bytes())),
                        nullable,
                    ),
                ),
                ConvertedType::DECIMAL => {
                    let precision: usize = col_desc.type_precision().try_into().unwrap();
                    // 128 * log(2) = 38.~
                    if precision > 38 {
                        bail!(
                            "Inserting decimals with more than 38 digits is currently not \
                            supported. Please raise an issue at \
                            https://github.com/pacman82/odbc2parquet/issues."
                        )
                    }
                    let scale: usize = col_desc.type_scale().try_into().unwrap();
                    let decimal_point_len: usize = if scale == 0 { 0 } else { 1 };
                    // + 1 for Sign
                    let max_str_len = precision + decimal_point_len + 1;
                    (
                        BufferKind::Text { max_str_len },
                        ByteArrayType::map_to_text(
                            move |bytes, index, odbc_buf| {
                                let n = i128_from_be_slice(bytes.as_bytes());
                                let text = odbc_buf.set_mut(index, max_str_len);
                                write_integer_as_decimal(n, precision, scale, text);
                            },
                            nullable,
                        ),
                    )
                }
                _ => unexpected(),
            }
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            let length = col_desc.type_length().try_into().unwrap();
            match lt {
                ConvertedType::NONE => (
                    BufferKind::Binary { length },
                    FixedLenByteArrayType::map_to_binary(
                        |bytes, index, odbc_buf| odbc_buf.append(index, Some(bytes.as_bytes())),
                        nullable,
                    ),
                ),
                ConvertedType::DECIMAL => {
                    let precision: usize = col_desc.type_precision().try_into().unwrap();
                    // 128 * log(2) = 38.~
                    if precision > 38 {
                        bail!(
                            "Inserting decimals with more than 38 digits is currently not \
                            supported. Please raise an issue at \
                            https://github.com/pacman82/odbc2parquet/issues."
                        )
                    }
                    let scale: usize = col_desc.type_scale().try_into().unwrap();
                    let decimal_point_len: usize = if scale == 0 { 0 } else { 1 };
                    // + 1 for Sign
                    let max_str_len = precision + decimal_point_len + 1;
                    (
                        BufferKind::Text { max_str_len },
                        FixedLenByteArrayType::map_to_text(
                            move |bytes, index, odbc_buf| {
                                let n = i128_from_be_slice(bytes.as_bytes());
                                let text = odbc_buf.set_mut(index, max_str_len);
                                write_integer_as_decimal(n, precision, scale, text);
                            },
                            nullable,
                        ),
                    )
                }
                ConvertedType::INTERVAL => bail!(
                    "
                    Inserting interval types is currently not supported. There is no reason for \
                    this besides the fact, that nobody has implemented it so far. Please raise an \
                    issue at https://github.com/pacman82/odbc2parquet/issues.
                "
                ), // implies len == 12
                _ => unexpected(),
            }
        }
    };

    Ok((BufferDescription { nullable, kind }, parquet_to_odbc))
}

trait OdbcDataType<'a> {
    type Required;
    type Optional;

    fn unwrap_writer_required(column_writer: AnyColumnViewMut<'a>) -> Self::Required;
    fn unwrap_writer_optional(column_writer: AnyColumnViewMut<'a>) -> Self::Optional;
}

fn i128_from_be_slice(bytes: &[u8]) -> i128 {
    let mut buf = if (bytes[0] as i8).is_negative() {
        [255; 16]
    } else {
        [0; 16]
    };
    buf[(16 - bytes.len())..].copy_from_slice(bytes);
    i128::from_be_bytes(buf)
}

fn days_since_epoch_to_odbc_date(days_since_epoch: i32) -> odbc_api::sys::Date {
    let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
    let naive_date = unix_epoch.add(Duration::days(days_since_epoch as i64));
    odbc_api::sys::Date {
        year: naive_date.year().try_into().unwrap(),
        month: naive_date.month().try_into().unwrap(),
        day: naive_date.day().try_into().unwrap(),
    }
}

fn write_as_time_ms(mut milliseconds_since_midnight: i32, mut text: &mut [u8]) {
    let hours = milliseconds_since_midnight / 3_600_000;
    milliseconds_since_midnight -= hours * 3_600_000;
    let minutes = milliseconds_since_midnight / 60_000;
    milliseconds_since_midnight -= minutes * 60_000;
    let seconds = milliseconds_since_midnight / 1_000;
    milliseconds_since_midnight -= seconds * 1_000;
    write!(
        text,
        "{:02}:{:02}:{:02}.{:03}",
        hours, minutes, seconds, milliseconds_since_midnight
    )
    .unwrap()
}

fn write_as_time_us(mut microseconds_since_midnight: i64, mut text: &mut [u8]) {
    let hours = microseconds_since_midnight / 3_600_000_000;
    microseconds_since_midnight -= hours * 3_600_000_000;
    let minutes = microseconds_since_midnight / 60_000_000;
    microseconds_since_midnight -= minutes * 60_000_000;
    let seconds = microseconds_since_midnight / 1_000_000;
    microseconds_since_midnight -= seconds * 1_000_000;
    write!(
        text,
        "{:02}:{:02}:{:02}.{:06}",
        hours, minutes, seconds, microseconds_since_midnight
    )
    .unwrap()
}

fn write_integer_as_decimal<I>(mut n: I, precision: usize, scale: usize, text: &mut [u8])
where
    I: PrimInt + FromPrimitive + DivAssign + ToPrimitive + Signed + MulAssign,
{
    if n.is_negative() {
        n *= n.signum();
        text[0] = b'-';
    } else {
        text[0] = b'+';
    }

    // Number of digits + one decimal separator (`.`)
    let str_len = if scale == 0 { precision } else { precision + 1 };

    let ten = I::from_u8(10).unwrap();
    for index in (0..str_len).rev() {
        // The separator will not be printed in case of scale == 0 since index is never going to
        // reach `precision`.
        let char = if index == precision - scale {
            b'.'
        } else {
            let digit: u8 = (n % ten).to_u8().unwrap();
            n /= ten;
            b'0' + digit
        };
        // +1 offset to make space for sign character
        text[index + 1] = char;
    }
}

struct Text;

impl<'a> OdbcDataType<'a> for Text {
    type Required = TextColumnWriter<'a, u8>;
    type Optional = TextColumnWriter<'a, u8>;

    fn unwrap_writer_required(column_writer: AnyColumnViewMut<'a>) -> Self::Required {
        if let AnyColumnViewMut::Text(inner) = column_writer {
            inner
        } else {
            panic!("Unexpected column writer. Expected text column writer. This is a Bug.")
        }
    }

    fn unwrap_writer_optional(column_writer: AnyColumnViewMut<'a>) -> Self::Optional {
        // Both implementations are identical since the buffer for text is the same.
        Self::unwrap_writer_required(column_writer)
    }
}

struct WText;

impl<'a> OdbcDataType<'a> for WText {
    type Required = TextColumnWriter<'a, u16>;
    type Optional = TextColumnWriter<'a, u16>;

    fn unwrap_writer_required(column_writer: AnyColumnViewMut<'a>) -> Self::Required {
        if let AnyColumnViewMut::WText(inner) = column_writer {
            inner
        } else {
            panic!("Unexpected column writer. Expected text column writer. This is a Bug.")
        }
    }

    fn unwrap_writer_optional(column_writer: AnyColumnViewMut<'a>) -> Self::Optional {
        // Both implementations are identical since the buffer for text is the same.
        Self::unwrap_writer_required(column_writer)
    }
}

struct Binary;

impl<'a> OdbcDataType<'a> for Binary {
    type Required = BinColumnWriter<'a>;
    type Optional = BinColumnWriter<'a>;

    fn unwrap_writer_required(column_writer: AnyColumnViewMut<'a>) -> Self::Required {
        if let AnyColumnViewMut::Binary(inner) = column_writer {
            inner
        } else {
            panic!("Unexpected column writer. Expected text column writer. This is a Bug.")
        }
    }

    fn unwrap_writer_optional(column_writer: AnyColumnViewMut<'a>) -> Self::Optional {
        // Both implementations are identical since the buffer for text is the same.
        Self::unwrap_writer_required(column_writer)
    }
}

macro_rules! impl_odbc_data_type {
    ($data_type:ident, $element:ident, $variant_cw_req:ident, $variant_cw_opt:ident) => {
        impl<'a> OdbcDataType<'a> for $data_type {
            type Required = &'a mut [$element];
            type Optional = NullableSliceMut<'a, $element>;

            fn unwrap_writer_required(column_writer: AnyColumnViewMut<'a>) -> Self::Required {
                if let AnyColumnViewMut::$variant_cw_req(inner) = column_writer {
                    inner
                } else {
                    panic!("Unexpected column writer. This is a Bug.")
                }
            }

            fn unwrap_writer_optional(column_writer: AnyColumnViewMut<'a>) -> Self::Optional {
                if let AnyColumnViewMut::$variant_cw_opt(inner) = column_writer {
                    inner
                } else {
                    panic!("Unexpected column writer. This is a Bug.")
                }
            }
        }
    };
}

impl_odbc_data_type!(Int32Type, i32, I32, NullableI32);
impl_odbc_data_type!(Int64Type, i64, I64, NullableI64);
impl_odbc_data_type!(FloatType, f32, F32, NullableF32);
impl_odbc_data_type!(DoubleType, f64, F64, NullableF64);
impl_odbc_data_type!(Bit, Bit, Bit, NullableBit);
impl_odbc_data_type!(Date, Date, Date, NullableDate);
impl_odbc_data_type!(Timestamp, Timestamp, Timestamp, NullableTimestamp);

#[cfg(test)]
mod tests {
    use super::{i128_from_be_slice, write_integer_as_decimal};

    #[test]
    fn format_i32_to_decimal() {
        let mut out = [0; 11];
        write_integer_as_decimal(123456789i32, 9, 2, &mut out);
        assert_eq!("+1234567.89", std::str::from_utf8(&out[..]).unwrap());

        let mut out = [0; 10];
        write_integer_as_decimal(123456789i32, 9, 0, &mut out);
        assert_eq!("+123456789", std::str::from_utf8(&out[..]).unwrap());

        let mut out = [0; 12];
        write_integer_as_decimal(-123456780i32, 10, 2, &mut out);
        assert_eq!("-01234567.80", std::str::from_utf8(&out[..]).unwrap());
    }

    #[test]
    fn format_i64_to_decimal() {
        let mut out = [0; 11];
        write_integer_as_decimal(123456789i64, 9, 2, &mut out);
        assert_eq!("+1234567.89", std::str::from_utf8(&out[..]).unwrap());

        let mut out = [0; 10];
        write_integer_as_decimal(123456789i64, 9, 0, &mut out);
        assert_eq!("+123456789", std::str::from_utf8(&out[..]).unwrap());

        let mut out = [0; 12];
        write_integer_as_decimal(-123456780i64, 10, 2, &mut out);
        assert_eq!("-01234567.80", std::str::from_utf8(&out[..]).unwrap());
    }

    #[test]
    fn i128_from_bytes() {
        assert_eq!(
            1,
            i128_from_be_slice(&[0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1][..])
        );
        assert_eq!(1, i128_from_be_slice(&[1u8][..]));
        assert_eq!(-1, i128_from_be_slice(&[255u8; 16][..]));
        assert_eq!(-1, i128_from_be_slice(&[255u8][..]));
    }
}
