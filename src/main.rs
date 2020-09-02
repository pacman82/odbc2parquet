mod buffer;

use anyhow::Error;
use buffer::{ColumnBufferDescription, OdbcBuffer};
use chrono::NaiveDate;
use odbc_api::{
    sys::{SqlDataType, USmallInt, NULL_DATA},
    ColumnDescription, Cursor, Environment, Nullable,
};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use std::{convert::TryInto, fs::File, path::PathBuf, rc::Rc};
use structopt::StructOpt;

/// Query an ODBC data source at store the result in a Parquet file.
#[derive(StructOpt, Debug)]
#[structopt()]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: usize,
    /// The connection string used to connect to the ODBC data source.
    #[structopt()]
    connection_string: String,
    /// Query executed against the ODBC data source.
    #[structopt()]
    query: String,
    #[structopt()]
    /// Name of the output parquet file.
    output: PathBuf,
    /// Size of a single batch in rows. The content of the data source is written into the output
    /// parquet files in batches. This way the content does never need to be materialized completly
    /// in memory at once.
    #[structopt(long, default_value = "500")]
    batch_size: usize,
}

fn main() -> Result<(), Error> {
    let opt = Cli::from_args();

    // Initialize logging
    stderrlog::new()
        .module(module_path!())
        .module("odbc_api")
        .quiet(false)
        .verbosity(opt.verbose)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // We know this is going to be the only ODBC environment in the entire process, so this is safe.
    let odbc_env = unsafe { Environment::new() }?;

    let mut odbc_conn = odbc_env.connect_with_connection_string(&opt.connection_string)?;

    if let Some(cursor) = odbc_conn.exec_direct(&opt.query)? {
        let file = File::create(&opt.output)?;
        cursor_to_parquet(cursor, file, opt.batch_size)?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }

    Ok(())
}

fn cursor_to_parquet(cursor: Cursor, file: File, batch_size: usize) -> Result<(), Error> {
    // Write properties
    let wpb = WriterProperties::builder();
    let properties = Rc::new(wpb.build());

    let (parquet_schema, buffer_description) = make_schema(&cursor)?;
    let mut writer = SerializedFileWriter::new(file, parquet_schema, properties)?;
    let mut odbc_buffer = OdbcBuffer::new(batch_size, buffer_description.into_iter());
    let mut row_set_cursor = cursor.bind_row_set_buffer(&mut odbc_buffer)?;

    let mut def_levels = Vec::with_capacity(batch_size);
    // Wo only deal with flat tabular data.
    let rep_levels = None;

    while let Some(buffer) = row_set_cursor.fetch()? {
        let mut row_group_writer = writer.next_row_group()?;
        let mut col_index = 0;
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            match &mut column_writer {
                // parquet::column::writer::ColumnWriter::BoolColumnWriter(_) => {}
                ColumnWriter::Int32ColumnWriter(cw) => {
                    def_levels.clear();
                    let mut values = Vec::new();
                    // Currently we use int32 only to represent dates
                    let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
                    for field in buffer.date_it(col_index) {
                        let (value, def) = field.map(|date| {
                            // Transform date to days since unix epoch as i32
                            let date = NaiveDate::from_ymd(
                                date.year as i32,
                                date.month as u32,
                                date.day as u32,
                            );
                            let duration = date.signed_duration_since(unix_epoch);
                            (duration.num_days().try_into().unwrap(), 1)
                        }).unwrap_or((0,0));
                        def_levels.push(def);
                        values.push(value);
                    }
                    cw.write_batch(&values, Some(&def_levels), rep_levels)?;
                }
                ColumnWriter::Int64ColumnWriter(cw) => {
                    def_levels.clear();
                    let mut values = Vec::new();
                    // Currently we use int32 only to represent dates
                    for field in buffer.timestamp_it(col_index) {
                        let (value, def) = field.map(|ts| {
                            // Transform date to days since unix epoch as i32
                            let datetime = NaiveDate::from_ymd(
                                ts.year as i32,
                                ts.month as u32,
                                ts.day as u32,
                            ).and_hms_nano(ts.hour as u32, ts.minute as u32, ts.second as u32, ts.fraction as u32);
                            (datetime.timestamp_nanos() / 1000, 1)
                        }).unwrap_or((0,0));
                        def_levels.push(def);
                        values.push(value);
                    }
                    cw.write_batch(&values, Some(&def_levels), rep_levels)?;
                }
                // parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => {}
                ColumnWriter::FloatColumnWriter(cw) => {
                    let (values, indicators) = buffer.f32_column(col_index);
                    def_levels.clear();
                    for &ind in indicators {
                        def_levels.push(if ind == NULL_DATA { 0 } else { 1 });
                    }
                    cw.write_batch(values, Some(&def_levels), rep_levels)?;
                }
                ColumnWriter::DoubleColumnWriter(cw) => {
                    let (values, indicators) = buffer.f64_column(col_index);
                    def_levels.clear();
                    for &ind in indicators {
                        def_levels.push(if ind == NULL_DATA { 0 } else { 1 });
                    }
                    cw.write_batch(values, Some(&def_levels), rep_levels)?;
                }
                ColumnWriter::ByteArrayColumnWriter(cw) => {
                    let field_it = buffer.text_column_it(col_index);
                    let mut values = Vec::new();
                    def_levels.clear();

                    for read_buf in field_it {
                        let (bytes, nul) = read_buf
                            // Value is not NULL
                            .map(|buf| (buf.to_owned().into(), 1))
                            // Value is NULL
                            .unwrap_or_else(|| (ByteArray::new(), 0));
                        values.push(bytes);
                        def_levels.push(nul);
                    }
                    cw.write_batch(&values, Some(&def_levels), rep_levels)?;
                }
                // parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(_) => {}
                _ => {} //panic!("Unsupported type for column writer type. Column index: {}", index)
            }
            row_group_writer.close_column(column_writer)?;
            col_index += 1;
        }
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

fn make_schema(cursor: &Cursor) -> Result<(Rc<Type>, Vec<ColumnBufferDescription>), Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut odbc_buffer_desc = Vec::new();
    let mut fields = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        cursor.describe_col(index as USmallInt, &mut cd)?;

        let name = cd.name_to_string()?;

        let (physical_type, logical_type, buffer_description) = match cd.data_type {
            SqlDataType::DOUBLE => (PhysicalType::DOUBLE, None, ColumnBufferDescription::F64),
            SqlDataType::FLOAT => (PhysicalType::FLOAT, None, ColumnBufferDescription::F32),
            SqlDataType::DATE => (
                PhysicalType::INT32,
                Some(LogicalType::DATE),
                ColumnBufferDescription::Date,
            ),
            SqlDataType::TIMESTAMP => (
                PhysicalType::INT64,
                Some(LogicalType::TIMESTAMP_MICROS),
                ColumnBufferDescription::Timestamp,
            ),
            _ => {
                let max_str_len = cursor.col_display_size(index.try_into().unwrap())? as usize;
                (
                    PhysicalType::BYTE_ARRAY,
                    Some(LogicalType::UTF8),
                    ColumnBufferDescription::Text { max_str_len },
                )
            }
        };

        let repetition = match cd.nullable {
            Nullable::Nullable | Nullable::Unknown => Repetition::OPTIONAL,
            Nullable::NoNulls => Repetition::REQUIRED,
        };

        let field_builder =
            Type::primitive_type_builder(&name, physical_type).with_repetition(repetition);
        let field_builder = if let Some(logical_type) = logical_type {
            field_builder.with_logical_type(logical_type)
        } else {
            field_builder
        };
        fields.push(Rc::new(field_builder.build()?));
        odbc_buffer_desc.push(buffer_description);
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()?;

    Ok((Rc::new(schema), odbc_buffer_desc))
}
