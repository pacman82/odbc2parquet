mod buffer;

use anyhow::Error;
use buffer::{derive_buffer_description, OdbcBuffer};
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
use std::{fs::File, path::PathBuf, rc::Rc};
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

    let parquet_schema = make_schema(&cursor)?;
    let mut writer = SerializedFileWriter::new(file, parquet_schema, properties)?;
    let buffer_description = derive_buffer_description(&cursor)?;
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
                // parquet::column::writer::ColumnWriter::Int32ColumnWriter(_) => {}
                // parquet::column::writer::ColumnWriter::Int64ColumnWriter(_) => {}
                // parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => {}
                // parquet::column::writer::ColumnWriter::FloatColumnWriter(_) => {}
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
                            .map(|buf| (buf.to_owned().into(), 1))
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

fn make_schema(cursor: &Cursor) -> Result<Rc<Type>, Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut fields = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        cursor.describe_col(index as USmallInt, &mut cd)?;

        let name = cd.name_to_string()?;

        let (physical_type, logical_type) = match cd.data_type {
            SqlDataType::DOUBLE => (PhysicalType::DOUBLE, None),
            _ => (PhysicalType::BYTE_ARRAY, Some(LogicalType::UTF8)),
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
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()?;

    Ok(Rc::new(schema))
}
