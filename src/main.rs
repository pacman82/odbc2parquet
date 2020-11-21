mod odbc_buffer;
mod parquet_buffer;
mod query;

use anyhow::{bail, Error};
use odbc_api::{
    Connection, Environment,
};
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
pub struct QueryOpt {
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
    let mut odbc_env = unsafe { Environment::new() }?;

    match opt.command {
        Command::Query { query_opt } => {
            query::query(&odbc_env, &query_opt)?;
        },
        Command::ListDrivers => {
            for driver_info in odbc_env.drivers()? {
                println!("{}", driver_info.description);
                for (key, value) in &driver_info.attributes {
                    println!("\t{}={}", key,value);
                }
                println!()
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
        odbc_env.connect_with_connection_string(&connection_string)?
    } else {
        bail!("Please specify a data source either using --dsn or --connection-string.");
    };
    Ok(conn)
}
