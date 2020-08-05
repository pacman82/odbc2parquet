use anyhow::{anyhow, Error};
use odbc_array::{ColumnDescription, Cursor, Environment, U16String, Nullable};
use odbc_sys::USmallInt;
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use std::{char::decode_utf16, fs::File, rc::Rc};
use structopt::StructOpt;

/// Query an ODBC data source at store the result in a Parquet file.
#[derive(StructOpt, Debug)]
#[structopt()]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: usize,
    /// The connection string used to connect to the ODBC datasource.
    #[structopt(short = "s")]
    connection_string: String,
    /// Query executed against the ODBC data source.
    #[structopt(long, short = "q")]
    query: String,
}

fn main() -> Result<(), Error> {
    let opt = Cli::from_args();

    // Initialize logging
    stderrlog::new()
        .module(module_path!())
        .module("odbc_array")
        .quiet(false)
        .verbosity(opt.verbose)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let mut odbc_env = unsafe { Environment::new() }?;

    let connection_string = U16String::from_str(&opt.connection_string);
    let odbc_conn = odbc_env.connect_with_connection_string(&connection_string)?;

    let query = U16String::from_str(&opt.query);
    if let Some(cursor) = odbc_conn.exec_direct(&query)? {
        let file = File::create("out.par")?;
        // Write properties
        let wpb = WriterProperties::builder();
        let properties = Rc::new(wpb.build());

        let schema = make_schema(&cursor)?;

        let mut writer = SerializedFileWriter::new(file, schema, properties)?;

        writer.close()?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }

    Ok(())
}

fn make_schema(cursor: &Cursor) -> Result<Rc<Type>, Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut fields = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        cursor.describe_col(index as USmallInt, &mut cd)?;

        let name: Result<String, _> = decode_utf16(cd.name).collect();
        let name = name?;

        let (physical_type, logical_type) = match cd.data_type {
            odbc_sys::SqlDataType::UnknownType => {
                return Err(anyhow!("Could not determine type of column {}", name))
            }
            // odbc_sys::SqlDataType::Char => {}
            // odbc_sys::SqlDataType::Numeric => {}
            // odbc_sys::SqlDataType::Decimal => {}
            // odbc_sys::SqlDataType::Integer => {}
            // odbc_sys::SqlDataType::Smallint => {}
            // odbc_sys::SqlDataType::Float => {}
            // odbc_sys::SqlDataType::Real => {}
            odbc_sys::SqlDataType::Double => (PhysicalType::DOUBLE, None),
            // odbc_sys::SqlDataType::Datetime => {}
            odbc_sys::SqlDataType::Varchar => (PhysicalType::BYTE_ARRAY, Some(LogicalType::UTF8)),
            // odbc_sys::SqlDataType::Date => {}
            // odbc_sys::SqlDataType::Time => {}
            odbc_sys::SqlDataType::Timestamp => {
                (PhysicalType::INT64, Some(LogicalType::TIMESTAMP_MILLIS))
            }
            // odbc_sys::SqlDataType::ExtTimeOrInterval => {}
            // odbc_sys::SqlDataType::ExtTimestamp => {}
            // odbc_sys::SqlDataType::ExtLongVarchar => {}
            // odbc_sys::SqlDataType::ExtBinary => {}
            // odbc_sys::SqlDataType::ExtVarBinary => {}
            // odbc_sys::SqlDataType::ExtLongVarBinary => {}
            // odbc_sys::SqlDataType::ExtBigInt => {}
            // odbc_sys::SqlDataType::ExtTinyInt => {}
            // odbc_sys::SqlDataType::ExtBit => {}
            // odbc_sys::SqlDataType::ExtWChar => {}
            // odbc_sys::SqlDataType::ExtWVarChar => {}
            // odbc_sys::SqlDataType::ExtWLongVarChar => {}
            // odbc_sys::SqlDataType::ExtGuid => {}
            // odbc_sys::SqlDataType::SsVariant => {}
            // odbc_sys::SqlDataType::SsUdt => {}
            // odbc_sys::SqlDataType::SsXml => {}
            // odbc_sys::SqlDataType::SsTable => {}
            // odbc_sys::SqlDataType::SsTime2 => {}
            // odbc_sys::SqlDataType::SsTimestampOffset => {}
            other => return Err(anyhow!("Unsupported SQL Data Type {:?}", other)),
        };

        let repetition = match cd.nullable {
            Nullable::Nullable | Nullable::Unknown => Repetition::OPTIONAL,
            Nullable::NoNulls => Repetition::REQUIRED,
        };

        let field_builder = Type::primitive_type_builder(&name, physical_type)
            .with_repetition(repetition);
        let field_builder = if let Some(logical_type) = logical_type {
            field_builder.with_logical_type(logical_type)
        } else {
            field_builder
        };
        fields.push(Rc::new(field_builder.build()?));
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()
        .unwrap();

    Ok(Rc::new(schema))
}
