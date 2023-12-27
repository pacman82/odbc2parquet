mod batch_size_limit;
mod binary;
mod boolean;
mod date;
mod decimal;
mod identical;
mod parquet_writer;
mod strategy;
mod text;
mod time;
mod timestamp;
mod timestamp_precision;
mod timestamp_tz;

use self::{
    batch_size_limit::{BatchSizeLimit, FileSizeLimit},
    parquet_writer::{parquet_output, ParquetWriterOptions},
    strategy::{strategy_from_column_description, FetchStrategy, MappingOptions},
};

use std::{
    io::{stdin, Read},
    sync::Arc,
};

use anyhow::{bail, Context, Error};
use io_arg::IoArg;
use log::{debug, info};
use odbc_api::{
    buffers::ColumnarAnyBuffer, ColumnDescription, Cursor, Environment, IntoParameter, Quirks,
    ResultSetMetadata,
};
use parquet::schema::types::{Type, TypePtr};

use crate::{open_connection, parquet_buffer::ParquetBuffer, QueryOpt};

/// Execute a query and writes the result to parquet.
pub fn query(environment: &Environment, opt: QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size_row,
        batch_size_memory,
        row_groups_per_file,
        file_size_threshold,
        encoding,
        prefer_varbinary,
        column_compression_default,
        column_compression_level_default,
        parquet_column_encoding,
        avoid_decimal,
        driver_does_not_support_64bit_integers,
        driver_returns_memory_garbage_for_indicators,
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

    let odbc_conn = open_connection(environment, &connect_opts)?;
    let db_name = odbc_conn.database_management_system_name()?;
    info!("Database Managment System Name: {db_name}");

    let mut quirks = Quirks::new();

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
        quirks: &quirks,
    };

    if let Some(cursor) = odbc_conn.execute(&query, params.as_slice())? {
        cursor_to_parquet(
            cursor,
            output,
            batch_size,
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
    mut cursor: impl Cursor,
    path: IoArg,
    batch_size: BatchSizeLimit,
    mapping_options: MappingOptions,
    parquet_format_options: ParquetWriterOptions,
) -> Result<(), Error> {
    let strategies = make_fetch_strategies(&mut cursor, mapping_options)?;

    let parquet_schema = parquet_schema_from_strategies(&strategies);

    if strategies.is_empty() {
        bail!("Resulting parquet file would not have any columns!")
    }

    let mem_usage_odbc_buffer_per_row: usize = strategies
        .iter()
        .map(|(_name, strategy)| strategy.buffer_desc().bytes_per_row())
        .sum();
    let total_mem_usage_per_row =
        mem_usage_odbc_buffer_per_row + ParquetBuffer::MEMORY_USAGE_BYTES_PER_ROW;
    info!(
        "Memory usage per row is {} bytes. This excludes memory directly allocated by the ODBC \
        driver.",
        total_mem_usage_per_row,
    );

    let batch_size_row = batch_size.batch_size_in_rows(total_mem_usage_per_row)?;

    info!("Batch size set to {} rows.", batch_size_row);

    let mut odbc_buffer = ColumnarAnyBuffer::from_descs(
        batch_size_row,
        strategies
            .iter()
            .map(|(_name, strategy)| (strategy.buffer_desc())),
    );

    let mut row_set_cursor = cursor.bind_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size_row);
    let mut num_batch = 0;
    // Count the number of total rows fetched so far for logging. This should be identical to
    // `num_batch * batch_size_row + num_rows`.
    let mut total_rows_fetched = 0;

    let mut writer = parquet_output(path, parquet_schema.clone(), parquet_format_options)?;

    while let Some(buffer) = row_set_cursor
        .fetch()
        .map_err(give_hint_about_flag_for_oracle_users)?
    {
        let mut row_group_writer = writer.next_row_group(num_batch)?;
        let mut col_index = 0;
        let num_rows = buffer.num_rows();
        total_rows_fetched += num_rows;
        num_batch += 1;
        info!("Fetched batch {num_batch} with {num_rows} rows.");
        info!("Fetched {total_rows_fetched} rows in total.");
        pb.set_num_rows_fetched(num_rows);
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            let col_name = parquet_schema.get_fields()[col_index]
                .get_basic_info()
                .name();
            debug!(
                "Writing column with index {} and name '{}'.",
                col_index, col_name
            );

            let odbc_column = buffer.column(col_index);

            strategies[col_index]
                .1
                .copy_odbc_to_parquet(&mut pb, column_writer.untyped(), odbc_column)
                .with_context(|| {
                    format!(
                        "Failed to copy column '{col_name}' from ODBC representation into Parquet"
                    )
                })?;
            column_writer.close()?;
            col_index += 1;
        }
        let metadata = row_group_writer.close()?;
        writer.update_current_file_size(metadata.compressed_size());
    }

    writer.close_box()?;

    Ok(())
}

type ColumnInfo = (String, Box<dyn FetchStrategy>);

fn make_fetch_strategies(
    cursor: &mut impl ResultSetMetadata,
    mapping_options: MappingOptions,
) -> Result<Vec<ColumnInfo>, Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut odbc_buffer_desc = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        // Reserving helps with drivers not reporting column name size correctly.
        cd.name.reserve(128);
        cursor.describe_col(index as u16, &mut cd)?;

        debug!("ODBC column description for column {}: {:?}", index, cd);

        let name = cd.name_to_string()?;
        // Give a generated name, should we fail to retrieve one from the ODBC data source.
        let name = if name.is_empty() {
            format!("Column{index}")
        } else {
            name
        };

        let column_fetch_strategy =
            strategy_from_column_description(&cd, &name, mapping_options, cursor, index)?;
        odbc_buffer_desc.push((name, column_fetch_strategy));
    }

    Ok(odbc_buffer_desc)
}

fn parquet_schema_from_strategies(strategies: &[(String, Box<dyn FetchStrategy>)]) -> TypePtr {
    let fields = strategies
        .iter()
        .map(|(name, s)| Arc::new(s.parquet_type(name)))
        .collect();
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap(),
    )
}

/// If we hit the issue with oracle not supporting 64Bit, let's tell our users that we have
/// implemented a solution to it.
fn give_hint_about_flag_for_oracle_users(error: odbc_api::Error) -> Error {
    match error {
        error @ odbc_api::Error::OracleOdbcDriverDoesNotSupport64Bit(_) => {
            let error: Error = error.into();
            error.context(
                "Looks like you are using an Oracle database. Try the \
                `--driver-does-not-support-64bit-integers` flag.",
            )
        }
        other => other.into(),
    }
}
