use anyhow::{bail, Error};
use clap::Args;
use odbc_api::{
    environment, escape_attribute_value, handles::OutputStringBuffer, Connection,
    ConnectionOptions, DriverCompleteOption,
};

/// Command line arguments used to establish a connection with the ODBC data source
#[derive(Args)]
pub struct ConnectOpts {
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

/// Open a database connection using the options provided on the command line.
pub fn open_connection<'e>(opt: &ConnectOpts) -> Result<Connection<'e>, Error> {
    let odbc_env = environment().expect("Enviornment must already be initialized in main.");
    // If a data source name has been given, try connecting with that.
    if let Some(dsn) = opt.dsn.as_deref() {
        let conn = odbc_env.connect(
            dsn,
            opt.user.as_deref().unwrap_or(""),
            opt.password.as_deref().unwrap_or(""),
            ConnectionOptions::default(),
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

    // We are not interested in the completed connection string, beyond creating a connection, so
    // we pass an empty buffer.
    let mut completed_connection_string = OutputStringBuffer::empty();

    let conn = odbc_env.driver_connect(&cs, &mut completed_connection_string, driver_completion)?;
    Ok(conn)
}
