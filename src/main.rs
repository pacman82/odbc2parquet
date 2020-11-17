mod odbc_buffer;
mod parquet_buffer;

use anyhow::{bail, Error};
use log::{debug, info};
use odbc_api::{
    sys::USmallInt, ColumnDescription, Connection, Cursor, DataType, Environment, IntoParameter,
    Nullable,
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

/// Query an ODBC data source at store the result in a Parquet file.
#[derive(StructOpt)]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short = "v", long, parse(from_occurrences))]
    verbose: usize,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    /// Query a data source and write the result as parquet.
    Query {
        #[structopt(flatten)]
        query_opt: QueryOpt,
    },
}

/// Command line arguments used to establish a connection with the ODBC data source
#[derive(StructOpt)]
struct ConnectOpts {
    /// The connection string used to connect to the ODBC data source. Alternatively you may
    /// specify the ODBC dsn.
    #[structopt(long, short = "c")]
    connection_string: Option<String>,
    /// ODBC Data Source Name. Either this or the connection string must be specified to identify
    /// the datasource. Data source name (dsn) and connection string, may not be specified both.
    #[structopt(long, conflicts_with = "connection-string")]
    dsn: Option<String>,
    /// User used to access the datasource specified in dsn.
    #[structopt(long, short = "u", env = "ODBC_USER")]
    user: Option<String>,
    /// Password used to log into the datasource. Only used if dsn is specified, instead of a
    /// connection string.
    #[structopt(long, short = "p", env = "ODBC_PASSWORD", hide_env_values = true)]
    password: Option<String>,
}

#[derive(StructOpt)]
struct QueryOpt {
    #[structopt(flatten)]
    connect_opts: ConnectOpts,
    /// Size of a single batch in rows. The content of the data source is written into the output
    /// parquet files in batches. This way the content does never need to be materialized completely
    /// in memory at once.
    #[structopt(long, default_value = "100000")]
    batch_size: u32,
    /// Name of the output parquet file.
    output: PathBuf,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters.
    query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
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

    match opt.command {
        Command::Query { query_opt } => {
            query(&odbc_env, &query_opt)?;
        }
    }

    Ok(())
}

/// Execute a query and writes the result to parquet.
fn query(environment: &Environment, opt: &QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size,
    } = opt;

    // Convert the input strings into parameters suitable to for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.into_parameter())
        .collect();

    let odbc_conn = open_connection(&environment, connect_opts)?;

    if let Some(cursor) = odbc_conn.execute(query, params.as_slice())? {
        let file = File::create(output)?;
        cursor_to_parquet(cursor, file, *batch_size as usize)?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

/// Open a database connection using the options provided on the command line.
fn open_connection<'e>(
    odbc_env: &'e Environment,
    opt: &ConnectOpts,
) -> Result<Connection<'e>, Error> {
    let conn = if let Some(dsn) = &opt.dsn {
        odbc_env.connect(
            &dsn,
            opt.user.as_deref().unwrap_or(""),
            opt.password.as_deref().unwrap_or(""),
        )?
    } else if let Some(connection_string) = &opt.connection_string {
        odbc_env.connect_with_connection_string(&connection_string)?
    } else {
        bail!("Please specify a data source either using --dsn or --connection-string.");
    };
    Ok(conn)
}

fn cursor_to_parquet(cursor: impl Cursor, file: File, batch_size: usize) -> Result<(), Error> {
    info!("Batch size set to {}", batch_size);
    // Write properties
    let wpb = WriterProperties::builder();
    let properties = Rc::new(wpb.build());

    let (parquet_schema, buffer_description) = make_schema(&cursor)?;
    let mut writer = SerializedFileWriter::new(file, parquet_schema, properties)?;
    let mut odbc_buffer = OdbcBuffer::new(batch_size, buffer_description.iter().copied());
    let mut row_set_cursor = cursor.bind_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size);
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
                ColumnWriter::BoolColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.bool_it(col_index))?;
                }
                ColumnWriter::Int32ColumnWriter(cw) => match buffer_description[col_index] {
                    ColumnBufferDescription::Date => {
                        pb.write_optional(cw, buffer.date_it(col_index))?
                    }
                    ColumnBufferDescription::I32 => {
                        // Used for Ints and Decimal logical types
                        pb.write_optional(cw, buffer.i32_it(col_index))?
                    }
                    _ => panic!("Mismatched ODBC and Parquet buffer type."),
                },
                ColumnWriter::Int64ColumnWriter(cw) => match buffer_description[col_index] {
                    ColumnBufferDescription::Timestamp => {
                        pb.write_optional(cw, buffer.timestamp_it(col_index))?
                    }
                    ColumnBufferDescription::I64 => {
                        // Used for Ints and Decimal logical types
                        pb.write_optional(cw, buffer.i64_it(col_index))?
                    }
                    _ => panic!("Mismatched ODBC and Parquet buffer type."),
                },
                // parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => {}
                ColumnWriter::FloatColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.f32_it(col_index))?;
                }
                ColumnWriter::DoubleColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.f64_it(col_index))?;
                }
                ColumnWriter::ByteArrayColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.text_column_it(col_index))?;
                }
                // parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(_) => {}
                _ => panic!("Invalid ColumnWriter type"),
            }
            row_group_writer.close_column(column_writer)?;
            col_index += 1;
        }
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

fn make_schema(cursor: &impl Cursor) -> Result<(Rc<Type>, Vec<ColumnBufferDescription>), Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut odbc_buffer_desc = Vec::new();
    let mut fields = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        // Reserving helps with drivers not reporting column name size correctly.
        cd.name.reserve(128);
        cursor.describe_col(index as USmallInt, &mut cd)?;

        debug!("ODBC column description for column {}: {:?}", index, cd);

        let name = cd.name_to_string()?;
        // Give a generated name, should we fail to retrieve one from the ODBC data source.
        let name = if name.is_empty() {
            format!("Column{}", index)
        } else {
            name
        };

        let ptb = |physical_type| Type::primitive_type_builder(&name, physical_type);

        let (field_builder, buffer_description) = match cd.data_type {
            DataType::Double => (ptb(PhysicalType::DOUBLE), ColumnBufferDescription::F64),
            DataType::Float | DataType::Real => {
                (ptb(PhysicalType::FLOAT), ColumnBufferDescription::F32)
            }
            DataType::SmallInt => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_16),
                ColumnBufferDescription::I32,
            ),
            DataType::Integer => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_32),
                ColumnBufferDescription::I32,
            ),
            DataType::Date => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::DATE),
                ColumnBufferDescription::Date,
            ),
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 10 =>
            {
                (
                    ptb(PhysicalType::INT32)
                        .with_logical_type(LogicalType::DECIMAL)
                        .with_precision(precision.try_into().unwrap())
                        .with_scale(scale.try_into().unwrap()),
                    ColumnBufferDescription::I32,
                )
            }
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 19 =>
            {
                (
                    ptb(PhysicalType::INT64)
                        .with_logical_type(LogicalType::DECIMAL)
                        .with_precision(precision.try_into().unwrap())
                        .with_scale(scale.try_into().unwrap()),
                    ColumnBufferDescription::I64,
                )
            }
            DataType::Timestamp { .. } => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::TIMESTAMP_MICROS),
                ColumnBufferDescription::Timestamp,
            ),
            DataType::Bigint => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::INT_64),
                ColumnBufferDescription::I64,
            ),
            DataType::Char { length } | DataType::Varchar { length } => (
                ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                ColumnBufferDescription::Text {
                    max_str_len: length.try_into().unwrap(),
                },
            ),
            DataType::Bit => (ptb(PhysicalType::BOOLEAN), ColumnBufferDescription::Bit),
            DataType::Tinyint => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_8),
                ColumnBufferDescription::I32,
            ),
            DataType::Unknown
            | DataType::Numeric { .. }
            | DataType::Decimal { .. }
            | DataType::Time { .. }
            | DataType::Other { .. } => {
                let max_str_len = cursor.col_display_size(index.try_into().unwrap())? as usize;
                (
                    ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                    ColumnBufferDescription::Text { max_str_len },
                )
            }
        };

        debug!(
            "ODBC buffer description for column {}: {:?}",
            index, buffer_description
        );

        let repetition = match cd.nullable {
            Nullable::Nullable | Nullable::Unknown => Repetition::OPTIONAL,
            Nullable::NoNulls => Repetition::REQUIRED,
        };

        let field_builder = field_builder.with_repetition(repetition);
        fields.push(Rc::new(field_builder.build()?));
        odbc_buffer_desc.push(buffer_description);
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()?;

    Ok((Rc::new(schema), odbc_buffer_desc))
}
