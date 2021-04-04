mod encoding;
mod insert;
mod parquet_buffer;
mod query;

use crate::encoding::EncodingArgument;
use anyhow::{bail, Error};
use odbc_api::{escape_attribute_value, Connection, Environment};
use std::path::PathBuf;
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
    /// List available drivers and their attributes.
    ListDrivers,
    /// List preconfigured data sources. Useful to find data source name to connect to database.
    ListDataSources,
    /// Read the content of a parquet and insert it into a table.
    Insert {
        #[structopt(flatten)]
        insert_opt: InsertOpt,
    },
}

/// Command line arguments used to establish a connection with the ODBC data source
#[derive(StructOpt)]
struct ConnectOpts {
    /// The connection string used to connect to the ODBC data source. Alternatively you may specify
    /// the ODBC dsn.
    #[structopt(long, short = "c")]
    connection_string: Option<String>,
    /// ODBC Data Source Name. Either this or the connection string must be specified to identify
    /// the datasource. Data source name (dsn) and connection string, may not be specified both.
    #[structopt(long, conflicts_with = "connection-string")]
    dsn: Option<String>,
    /// User used to access the datasource specified in dsn. Should you specify a connection string
    /// instead of a Data Source Name the user name is going to be appended at the end of it as the
    /// `UID` attribute.
    #[structopt(long, short = "u", env = "ODBC_USER")]
    user: Option<String>,
    /// Password used to log into the datasource. Only used if dsn is specified, instead of a
    /// connection string. Should you specify a Connection string instead of a Data Source Name the
    /// password is going to be appended at the end of it as the `PWD` attribute.
    #[structopt(long, short = "p", env = "ODBC_PASSWORD", hide_env_values = true)]
    password: Option<String>,
}

#[derive(StructOpt)]
pub struct QueryOpt {
    #[structopt(flatten)]
    connect_opts: ConnectOpts,
    /// Size of a single batch in rows. The content of the data source is written into the output
    /// parquet files in batches. This way the content does never need to be materialized completely
    /// in memory at once.
    #[structopt(long, default_value = "100000")]
    batch_size: u32,
    /// Maximum number of batches in a single output parquet file. If this option is omitted or 0 a
    /// single output file is produces. Otherwise each output file is closed after the maximum
    /// number of batches have been written and a new one with the suffix `_n` is started. There n
    /// is the of the produced output file starting at one for the first one. E.g. `out_1.par`,
    /// `out_2.par`, ...
    #[structopt(long, default_value = "0")]
    batches_per_file: u32,
    /// Encoding used for character data requested from the data source.
    ///
    /// `Utf16`: The tool will use 16Bit characters for requesting text from the data source,
    /// implying the use of UTF-16 encoding. This should work well independent of the system
    /// configuration, but implies additional work since text is always stored as UTF-8 in parquet.
    ///
    /// `System`: The tool will use 8Bit characters for requesting text from the data source,
    /// implying the use of the encoding from the system locale. This only works for non ASCII
    /// characters if the locales character set is UTF-8.
    ///
    /// `Auto`: Since on OS-X and Linux the default locales character set is always UTF-8 the
    /// default option is the same as `System` on non-windows platforms. On windows the default is
    /// `Utf16`.
    #[structopt(
        long,
        possible_values = &EncodingArgument::variants(),
        default_value = "Auto",
        case_insensitive = true)
    ]
    encoding: EncodingArgument,
    /// Name of the output parquet file.
    output: PathBuf,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters.
    query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
}

#[derive(StructOpt)]
pub struct InsertOpt {
    #[structopt(flatten)]
    connect_opts: ConnectOpts,
    /// Encoding used for transferring character data to the database.
    ///
    /// `Utf16`: Use 16Bit characters to send text text to the database, which implies the using
    /// UTF-16 encoding. This should work well independent of the system configuration, but requires
    /// additional work since text is always stored as UTF-8 in parquet.
    ///
    /// `System`: Use 8Bit characters for requesting text from the data source, implies using
    /// the encoding defined by the system locale. This only works for non ASCII characters if the
    /// locales character set is UTF-8.
    ///
    /// `Auto`: Since on OS-X and Linux the default locales character set is always UTF-8 the
    /// default option is the same as `System` on non-windows platforms. On windows the default is
    /// `Utf16`.
    #[structopt(
        long,
        possible_values = &EncodingArgument::variants(),
        default_value = "Auto",
        case_insensitive = true)
    ]
    encoding: EncodingArgument,
    /// Path to the input parquet file which is used to fill the database table with values.
    input: PathBuf,
    /// Name of the table to insert the values into. No precautions against SQL injection are
    /// taken. The insert statement is created by the tool. It will only work if the column names
    /// are the same in the parquet file and the database.
    table: String,
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
    let mut odbc_env = unsafe { Environment::new() }?;

    match opt.command {
        Command::Query { query_opt } => {
            query::query(&odbc_env, &query_opt)?;
        }
        Command::Insert { insert_opt } => {
            insert::insert(&odbc_env, &insert_opt)?;
        }
        Command::ListDrivers => {
            for driver_info in odbc_env.drivers()? {
                println!("{}", driver_info.description);
                for (key, value) in &driver_info.attributes {
                    println!("\t{}={}", key, value);
                }
                println!()
            }
        }
        Command::ListDataSources => {
            let mut first = true;
            for data_source_info in odbc_env.data_sources()? {
                // After first item, always place an additional newline in between.
                if first {
                    first = false;
                } else {
                    println!()
                }
                println!("Server name: {}", data_source_info.server_name);
                println!("Driver: {}", data_source_info.driver);
            }
        }
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
        // Append user and or password to connection string
        let mut cs = connection_string.to_owned();
        if let Some(uid) = opt.user.as_deref() {
            cs = format!("{}UID={};", cs, &escape_attribute_value(uid));
        }
        if let Some(pwd) = opt.password.as_deref() {
            cs = format!("{}PWD={};", cs, &escape_attribute_value(pwd));
        }

        odbc_env.connect_with_connection_string(&cs)?
    } else {
        bail!("Please specify a data source either using --dsn or --connection-string.");
    };
    Ok(conn)
}
