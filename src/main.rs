mod enum_args;
mod insert;
mod parquet_buffer;
mod query;

use crate::enum_args::{
    column_encoding_from_str, EncodingArgument,
};
use anyhow::{bail, Error};
use bytesize::ByteSize;
use enum_args::CompressionVariants;
use io_arg::IoArg;
use odbc_api::{
    escape_attribute_value, handles::OutputStringBuffer, Connection, DriverCompleteOption,
    Environment,
};
use parquet::basic::Encoding;
use std::{fs::File, path::PathBuf};
use stderrlog::ColorChoice;

use clap::{Args, Parser, CommandFactory, ArgAction};
use clap_complete::{generate, Shell};

/// Query an ODBC data source at store the result in a Parquet file.
#[derive(Parser)]
#[clap(version)]
struct Cli {
    /// Only print errors to standard error stream. Supresses warnings and all other log levels
    /// independent of the verbose mode.
    #[arg(short = 'q', long)]
    quiet: bool,
    /// Verbose mode (-v, -vv, -vvv, etc)
    ///
    /// 'v': Info level logging
    /// 'vv': Debug level logging
    /// 'vvv': Trace level logging
    #[arg(short = 'v', long, action = ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Never emit colors.
    ///
    /// Controls the colors of the log output. If specified the log output will never be colored.
    /// If not specified the tool will try to emit Colors, but not force it. If `TERM=dumb` or
    /// `NO_COLOR` is defined, then colors will not be used.
    no_color: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    /// Query a data source and write the result as parquet.
    Query {
        #[clap(flatten)]
        query_opt: QueryOpt,
    },
    /// List available drivers and their attributes.
    ListDrivers,
    /// List preconfigured data sources. Useful to find data source name to connect to database.
    ListDataSources,
    /// Read the content of a parquet and insert it into a table.
    Insert {
        #[clap(flatten)]
        insert_opt: InsertOpt,
    },
    /// Generate shell completions
    Completions {
        #[arg(long, short = 'o', default_value = ".")]
        /// Output directory
        output: PathBuf,
        /// Name of the shell to generate completions for.
        shell: Shell,
    },
}

/// Command line arguments used to establish a connection with the ODBC data source
#[derive(Args)]
struct ConnectOpts {
    #[arg(long, conflicts_with = "dsn")]
    /// Prompts the user for missing information from the connection string. Only supported on
    /// windows platform.
    prompt: bool,
    /// The connection string used to connect to the ODBC data source. Alternatively you may specify
    /// the ODBC dsn.
    #[arg(long, short = 'c', env = "ODBC_CONNECTION_STRING")]
    connection_string: Option<String>,
    /// ODBC Data Source Name. Either this or the connection string must be specified to identify
    /// the datasource. Data source name (dsn) and connection string, may not be specified both.
    #[arg(long, conflicts_with = "connection_string")]
    dsn: Option<String>,
    /// User used to access the datasource specified in dsn. Should you specify a connection string
    /// instead of a Data Source Name the user name is going to be appended at the end of it as the
    /// `UID` attribute.
    #[arg(long, short = 'u', env = "ODBC_USER")]
    user: Option<String>,
    /// Password used to log into the datasource. Only used if dsn is specified, instead of a
    /// connection string. Should you specify a Connection string instead of a Data Source Name the
    /// password is going to be appended at the end of it as the `PWD` attribute.
    #[arg(long, short = 'p', env = "ODBC_PASSWORD", hide_env_values = true)]
    password: Option<String>,
}

#[derive(Args)]
pub struct QueryOpt {
    #[clap(flatten)]
    connect_opts: ConnectOpts,
    /// Size of a single batch in rows. The content of the data source is written into the output
    /// parquet files in batches. This way the content does never need to be materialized completely
    /// in memory at once. If `--batch-size-memory` is not specified this value defaults to 65535.
    /// This avoids issues with some ODBC drivers using 16Bit integers to represent batch sizes. If
    /// `--batch-size-memory` is specified no other limit is applied by default. If both option are
    /// specified the batch size is the largest possible which satisfies both constraints.
    #[arg(long)]
    batch_size_row: Option<usize>,
    /// Limits the size of a single batch. It does so by calculating the amount of memory each row
    /// requires in the allocated buffers and then limits the maximum number of rows so that the
    /// maximum buffer size comes as close as possbile, but does not exceed the specified amount.
    /// Default is 2GiB on 64 Bit platforms and 1GiB on 32 Bit Platforms if `--batch-size-row` is
    /// not specified. If `--batch-size-row` is not specified no memory limit is applied by default.
    /// If both option are specified the batch size is the largest possible which satisfies both
    /// constraints. This option controls the size of the buffers of data in transit, and therfore
    /// the memory usage of this tool. It indirectly controls the size of the row groups written to
    /// parquet (since each batch is written as one row group). It is hard to make a generic
    /// statement about how much smaller the average row group will be.
    /// This options allows you to specify the memory usage using SI units. So you can pass `2Gib`,
    /// `600Mb` and so on.
    #[arg(long)]
    batch_size_memory: Option<ByteSize>,
    /// Maximum number of batches in a single output parquet file. If this option is omitted or 0 a
    /// single output file is produces. Otherwise each output file is closed after the maximum
    /// number of batches have been written and a new one with the suffix `_n` is started. There n
    /// is the of the produced output file starting at one for the first one. E.g. `out_01.par`,
    /// `out_2.par`, ...
    #[arg(long, default_value = "0")]
    row_groups_per_file: u32,
    /// Then the size of the currently written parquet files goes beyond this threshold the current
    /// row group will be finished and then the file will be closed. So the file will be somewhat
    /// larger than the threshold. All furthrer row groups will be written into new files to which
    /// the threshold size limit is applied as well. If this option is not set, no size threshold is
    /// applied. If the threshold is applied the first file name will have the suffix `_01`, the
    /// second the suffix `_2` and so on. Therfore the first resulting file will be called e.g.
    /// `out_1.par`, if `out.par` has been specified as the output argument.
    /// Also note that this option will not act as an upper bound. It will act as a lower bound for
    /// all but the last file, all others however will not be larger than this threshold by more
    /// than the size of one row group. You can use the `batch_size_row` and `batch_size_memory`
    /// options to control the size of the row groups. Do not expect the `batch_size_memory` however
    /// to be equal to the row group size. The row group size depends on the actual data in the
    /// database, and is due to compression likely much smaller. Values of this option can be
    /// specified in SI units. E.g. `--file-size-threshold 1GiB`.
    #[arg(long)]
    file_size_threshold: Option<ByteSize>,
    /// Default compression used by the parquet file writer.
    #[arg(
        long,
        value_enum,
        default_value="gzip",
    )]
    column_compression_default: CompressionVariants,
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
    #[arg(long, value_enum, default_value = "Auto", ignore_case = true)]
    encoding: EncodingArgument,
    /// Map `BINARY` SQL colmuns to `BYTE_ARRAY` instead of `FIXED_LEN_BYTE_ARRAY`. This flag has
    /// been introduced in an effort to increase the compatibility of the output with Apache Spark.
    #[clap(long)]
    prefer_varbinary: bool,
    /// Specify the fallback encoding of the parquet output column. You can parse mutliple values
    /// in format `COLUMN:ENCODING`. `ENCODING` must be one of: `plain`, `bit-packed`,
    /// `delta-binary-packed`, `delta-byte-array`, `delta-length-byte-array` or `rle`.
    #[arg(
        long,
        value_parser=column_encoding_from_str,
        action = ArgAction::Append
    )]
    parquet_column_encoding: Vec<(String, Encoding)>,
    /// Tells the odbc2parquet, that the ODBC driver does not support binding 64 Bit integers (aka
    /// S_C_BIGINT in ODBC speak). This will cause the odbc2parquet to query large integers as text
    /// instead and convert them to 64 Bit integers itself. Setting this flag will not affect the
    /// output, but may incurr a performance penality. In case you are using an Oracle Database it
    /// can make queries work which did not before, because Oracle does not support 64 Bit integers.
    #[clap(long)]
    driver_does_not_support_64bit_integers: bool,
    /// When writing to Parquet file prefer using Int over Decimal as the Converted type
    /// when scale is 0.
    /// Decimal(1-9, 0) -> INT_32,
    /// Decimal(10-19, 0) -> INT_64
    #[clap(long)]
    prefer_int_over_decimal: bool,
    /// In case fetch results gets split into multiple files a suffix with a number will be appended
    /// to each file name. Default suffix length is 2 leading to suffixes like e.g. `_03`. In case
    /// you would expect thousands of files in your output you may want to set this to say `4` so
    /// the zeros pad this to a 4 digit number in order to make the filenames more friendly for
    /// lexical sorting.
    #[clap(long, default_value = "2")]
    suffix_length: usize,
    /// Name of the output parquet file. Use `-` to indicate that the output should be written to
    /// standard out instead.
    output: IoArg,
    /// Query executed against the ODBC data source. Question marks (`?`) can be used as
    /// placeholders for positional parameters. E.g. "SELECT Name FROM Employees WHERE salary > ?;".
    /// Instead of passing a query verbatum, you may pass a plain dash (`-`), to indicate that the
    /// query should be read from standard input. In this case the entire input until EOF will be
    /// considered the query.
    query: String,
    /// For each placeholder question mark (`?`) in the query text one parameter must be passed at
    /// the end of the command line.
    parameters: Vec<String>,
}

#[derive(Args)]
pub struct InsertOpt {
    #[clap(flatten)]
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
    #[arg(long, value_enum, default_value = "Auto", ignore_case = true)]
    encoding: EncodingArgument,
    /// Path to the input parquet file which is used to fill the database table with values.
    input: PathBuf,
    /// Name of the table to insert the values into. No precautions against SQL injection are
    /// taken. The insert statement is created by the tool. It will only work if the column names
    /// are the same in the parquet file and the database.
    table: String,
}

impl Cli {
    /// Perform some validation logic, beyond what is possible (or sensible) to verify directly with
    /// clap.
    pub fn perform_extra_validation(&self) -> Result<(), Error> {
        if let Command::Query { query_opt } = &self.command {
            if !query_opt.output.is_file() {
                if query_opt.file_size_threshold.is_some() {
                    bail!("file-size-threshold conflicts with specifying stdout ('-') as output.")
                }
                if query_opt.row_groups_per_file != 0 {
                    bail!("row-groups-per-file conflicts with specifying stdout ('-') as output.")
                }
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    let opt = Cli::parse();
    opt.perform_extra_validation()?;

    let verbose = if opt.quiet {
        // Log errors, but nothing else
        0
    } else {
        // Log warnings and one additional log level for each `-v` passed in the command line.
        opt.verbose as usize + 1
    };

    let color_choice = if opt.no_color {
        ColorChoice::Never
    } else {
        ColorChoice::Auto
    };

    // Initialize logging
    stderrlog::new()
        .module(module_path!())
        .module("odbc_api")
        .quiet(false) // Even if `opt.quiet` is true, we still want to print errors
        .verbosity(verbose)
        .color(color_choice)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // Initialize ODBC environment used to create the connection to the Database
    let odbc_env = Environment::new()?;

    match opt.command {
        Command::Query { query_opt } => {
            query::query(&odbc_env, query_opt)?;
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
        Command::Completions { shell, output } => {
            let mut output = File::create(output)?;
            generate(shell, &mut Cli::command(), "odbc2parquet", &mut output);
        }
    }

    Ok(())
}

/// Open a database connection using the options provided on the command line.
fn open_connection<'e>(
    odbc_env: &'e Environment,
    opt: &ConnectOpts,
) -> Result<Connection<'e>, Error> {
    // If a data source name has been given, try connecting with that.
    if let Some(dsn) = opt.dsn.as_deref() {
        let conn = odbc_env.connect(
            dsn,
            opt.user.as_deref().unwrap_or(""),
            opt.password.as_deref().unwrap_or(""),
        )?;
        return Ok(conn);
    }

    // There is no data source name, so at least there must be prompt or a connection string
    if !opt.prompt && opt.connection_string.is_none() {
        bail!("Either DSN, connection string or prompt must be specified.")
    }

    // Append user and or password to connection string
    let mut cs = opt.connection_string.clone().unwrap_or_default();
    if let Some(uid) = opt.user.as_deref() {
        cs = format!("{}UID={};", cs, &escape_attribute_value(uid));
    }
    if let Some(pwd) = opt.password.as_deref() {
        cs = format!("{}PWD={};", cs, &escape_attribute_value(pwd));
    }

    #[cfg(target_os = "windows")]
    let driver_completion = if opt.prompt {
        // Only show the prompt to the user if the connection string does not contain all
        // information required to create a connection.
        DriverCompleteOption::Complete
    } else {
        DriverCompleteOption::NoPrompt
    };

    #[cfg(not(target_os = "windows"))]
    let driver_completion = if opt.prompt {
        // Would rather use conditional compilation on the flag itself. While this works fine, it
        // does mess with rust analyzer, so I keep it and panic here to keep development experience
        // smooth.
        bail!("--prompt is only supported on windows.");
    } else {
        DriverCompleteOption::NoPrompt
    };

    // We are not interessted in the completed connection string, beyond creating a connection, so
    // we pass an empty buffer.
    let mut completed_connection_string = OutputStringBuffer::empty();

    let conn = odbc_env.driver_connect(&cs, &mut completed_connection_string, driver_completion)?;
    Ok(conn)
}
