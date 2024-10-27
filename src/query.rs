mod batch_size_limit;
mod binary;
mod boolean;
mod column_strategy;
mod conversion_strategy;
mod current_file;
mod date;
mod decimal;
mod fetch_batch;
mod identical;
mod parquet_writer;
mod text;
mod time;
mod timestamp;
mod timestamp_precision;
mod timestamp_tz;

use anyhow::{bail, Error};
use fetch_batch::{FetchBatch, SequentialFetch};
use io_arg::IoArg;
use log::info;
use odbc_api::{Cursor, IntoParameter};
use std::io::{stdin, Read};

use self::{
    batch_size_limit::{BatchSizeLimit, FileSizeLimit},
    column_strategy::{ColumnStrategy, MappingOptions},
    conversion_strategy::ConversionStrategy,
    parquet_writer::{parquet_output, ParquetWriterOptions},
};

use crate::{open_connection, QueryOpt};

/// Execute a query and writes the result to parquet.
pub fn query(opt: QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size_row,
        batch_size_memory,
        row_groups_per_file,
        concurrent_fetching,
        file_size_threshold,
        encoding,
        prefer_varbinary,
        column_compression_default,
        column_compression_level_default,
        parquet_column_encoding,
        avoid_decimal,
        driver_does_not_support_64bit_integers,
        suffix_length,
        no_empty_file,
        column_length_limit,
    } = opt;

    let batch_size = BatchSizeLimit::new(batch_size_row, batch_size_memory);
    let file_size = FileSizeLimit::new(row_groups_per_file, file_size_threshold);
    let query = query_statement_text(query)?;

    // Convert the input strings into parameters suitable for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.as_str().into_parameter())
        .collect();

    let odbc_conn = open_connection(&connect_opts)?;
    let db_name = odbc_conn.database_management_system_name()?;
    info!("Database Management System Name: {db_name}");

    let parquet_format_options = ParquetWriterOptions {
        column_compression_default: column_compression_default
            .to_compression(column_compression_level_default)?,
        column_encodings: parquet_column_encoding,
        file_size,
        suffix_length,
        no_empty_file,
    };

    let mapping_options = MappingOptions {
        db_name: &db_name,
        use_utf16: encoding.use_utf16(),
        prefer_varbinary,
        avoid_decimal,
        driver_does_support_i64: !driver_does_not_support_64bit_integers,
        column_length_limit,
    };

    if let Some(cursor) = odbc_conn
        .into_cursor(&query, params.as_slice())
        // Drop the connection for odbc_api::ConnectionAndError in order to make the error
        // convertible into an anyhow error. The connection is offered by odbc_api in the error type
        // to allow reusing the same connection, even after conversion into cursor failed. However
        // within the context of `odbc2parquet`, we just want to shutdown the application and
        // present an error to the user.
        .map_err(odbc_api::Error::from)?
    {
        cursor_to_parquet(
            cursor,
            output,
            batch_size,
            concurrent_fetching,
            mapping_options,
            parquet_format_options,
        )?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

/// The query statement is either passed verbatim at the command line, or via stdin. The latter is
/// indicated by passing `-` at the command line instead of the string. This method reads stdin
/// until EOF if required and always returns the statement text.
fn query_statement_text(query: String) -> Result<String, Error> {
    Ok(if query == "-" {
        let mut buf = String::new();
        stdin().lock().read_to_string(&mut buf)?;
        buf
    } else {
        query
    })
}

fn cursor_to_parquet(
    mut cursor: impl Cursor + Send + 'static,
    path: IoArg,
    batch_size: BatchSizeLimit,
    concurrent_fetching: bool,
    mapping_options: MappingOptions,
    parquet_format_options: ParquetWriterOptions,
) -> Result<(), Error> {
    let table_strategy = ConversionStrategy::new(&mut cursor, mapping_options)?;
    let parquet_schema = table_strategy.parquet_schema();
    let writer = parquet_output(path, parquet_schema.clone(), parquet_format_options)?;
    let fetch_strategy: Box<dyn FetchBatch> = if concurrent_fetching {
        bail!("Concurrent fetching not yet supported")
    } else {
        Box::new(SequentialFetch::new(cursor, &table_strategy, batch_size)?)
    };
    table_strategy.block_cursor_to_parquet(fetch_strategy, writer)?;
    Ok(())
}
