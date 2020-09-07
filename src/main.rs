mod odbc_buffer;
mod parquet_buffer;

use anyhow::Error;
use odbc_api::{
    sys::{SqlDataType, USmallInt},
    ColumnDescription, Cursor, Environment, Nullable,
};
use odbc_buffer::{ColumnBufferDescription, OdbcBuffer};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use parquet_buffer::ParquetBuffer;
use std::{convert::TryInto, fs::File, path::PathBuf, rc::Rc};
use structopt::StructOpt;
use log::{debug, info};

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
    info!("Batch size set to {}", batch_size);
    // Write properties
    let wpb = WriterProperties::builder();
    let properties = Rc::new(wpb.build());

    let (parquet_schema, buffer_description) = make_schema(&cursor)?;
    let mut writer = SerializedFileWriter::new(file, parquet_schema, properties)?;
    let mut odbc_buffer = OdbcBuffer::new(batch_size, buffer_description.iter().copied());
    let mut row_set_cursor = cursor.bind_row_set_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size);
    // Wo only deal with flat tabular data.

    let mut num_batch = 0;

    while let Some(buffer) = row_set_cursor.fetch()? {
        let mut row_group_writer = writer.next_row_group()?;
        let mut col_index = 0;
        num_batch += 1;
        let num_rows = buffer.num_rows_fetched() as usize;
        info!("Fetched batch {} with {} rows.", num_batch, num_rows);
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            pb.set_num_rows_fetched(num_rows);
            match &mut column_writer {
                // parquet::column::writer::ColumnWriter::BoolColumnWriter(_) => {}
                ColumnWriter::Int32ColumnWriter(cw) => match buffer_description[col_index] {
                    ColumnBufferDescription::Date => {
                        pb.write_dates(cw, buffer.date_it(col_index))?
                    }
                    ColumnBufferDescription::I32 => {
                        pb.write_directly(cw, buffer.i32_column(col_index))?
                    }
                    _ => panic!("Mismatched ODBC and Parquet buffer type."),
                },
                ColumnWriter::Int64ColumnWriter(cw) => match buffer_description[col_index] {
                    ColumnBufferDescription::Timestamp => {
                        pb.write_timestamps(cw, buffer.timestamp_it(col_index))?
                    }
                    ColumnBufferDescription::I64 => {
                        pb.write_directly(cw, buffer.i64_column(col_index))?
                    }
                    _ => panic!("Mismatched ODBC and Parquet buffer type."),
                },
                // parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => {}
                ColumnWriter::FloatColumnWriter(cw) => {
                    pb.write_directly(cw, buffer.f32_column(col_index))?;
                }
                ColumnWriter::DoubleColumnWriter(cw) => {
                    pb.write_directly(cw, buffer.f64_column(col_index))?;
                }
                ColumnWriter::ByteArrayColumnWriter(cw) => {
                    pb.write_strings(cw, buffer.text_column_it(col_index))?;
                }
                // parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(_) => {}
                _ => panic!("Invalid Columnwriter type"),
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

        debug!("ODBC column descripton for column {}: {:?}", index, cd);

        let name = cd.name_to_string()?;

        let (physical_type, logical_type, buffer_description) = match cd.data_type {
            SqlDataType::DOUBLE => (PhysicalType::DOUBLE, None, ColumnBufferDescription::F64),
            SqlDataType::FLOAT => (PhysicalType::FLOAT, None, ColumnBufferDescription::F32),
            SqlDataType::SMALLINT => (
                PhysicalType::INT32,
                Some(LogicalType::INT_16),
                ColumnBufferDescription::I32,
            ),
            SqlDataType::INTEGER => (
                PhysicalType::INT32,
                Some(LogicalType::INT_32),
                ColumnBufferDescription::I32,
            ),
            SqlDataType::EXT_BIG_INT => (
                PhysicalType::INT64,
                Some(LogicalType::INT_64),
                ColumnBufferDescription::I64,
            ),
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

        debug!("ODBC buffer description for column {}: {:?}", index, buffer_description);

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
