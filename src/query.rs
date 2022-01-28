mod batch_size_limit;
mod binary;
mod boolean;
mod date;
mod decimal;
mod identical;
mod parquet_writer;
mod strategy;
mod text;
mod timestamp;

use self::{
    batch_size_limit::BatchSizeLimit,
    parquet_writer::ParquetFormatOptions,
    parquet_writer::ParquetWriter,
    strategy::{strategy_from_column_description, ColumnFetchStrategy},
};

use std::{path::Path, sync::Arc};

use anyhow::{bail, Error};
use log::{debug, info};
use odbc_api::{
    buffers::buffer_from_description_and_indices, ColumnDescription, Cursor, Environment,
    IntoParameter,
};
use parquet::schema::types::{Type, TypePtr};

use crate::{open_connection, parquet_buffer::ParquetBuffer, QueryOpt};

/// Execute a query and writes the result to parquet.
pub fn query(environment: &Environment, opt: &QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size_row,
        batch_size_mib,
        batches_per_file,
        encoding,
        prefer_varbinary,
        column_compression_default,
        parquet_column_encoding,
    } = opt;

    let batch_size = BatchSizeLimit::new(*batch_size_row, *batch_size_mib);

    // Convert the input strings into parameters suitable for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.as_str().into_parameter())
        .collect();

    let odbc_conn = open_connection(environment, connect_opts)?;

    let parquet_format_options = ParquetFormatOptions {
        column_compression_default: *column_compression_default,
        column_encodings: parquet_column_encoding.clone(),
    };

    if let Some(cursor) = odbc_conn.execute(query, params.as_slice())? {
        cursor_to_parquet(
            cursor,
            output,
            batch_size,
            *batches_per_file,
            encoding.use_utf16(),
            *prefer_varbinary,
            parquet_format_options,
        )?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

fn cursor_to_parquet(
    cursor: impl Cursor,
    path: &Path,
    batch_size: BatchSizeLimit,
    batches_per_file: u32,
    use_utf16: bool,
    prefer_varbinary: bool,
    parquet_format_options: ParquetFormatOptions,
) -> Result<(), Error> {
    let strategies = make_schema(&cursor, use_utf16, prefer_varbinary)?;

    let parquet_schema = parquet_schema_from_strategies(&strategies);

    if strategies.is_empty() {
        bail!("Resulting parquet file would not have any columns!")
    }

    let mem_usage_odbc_buffer_per_row: usize = strategies
        .iter()
        .map(|(_index, _name, strategy)| strategy.buffer_description().bytes_per_row())
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

    let mut odbc_buffer = buffer_from_description_and_indices(
        batch_size_row,
        strategies
            .iter()
            .map(|(index, _name, strategy)| (*index, strategy.buffer_description())),
    );

    let mut row_set_cursor = cursor.bind_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size_row as usize);
    let mut num_batch = 0;

    let mut writer = ParquetWriter::new(
        path,
        parquet_schema.clone(),
        batches_per_file,
        parquet_format_options,
    )?;

    while let Some(buffer) = row_set_cursor.fetch()? {
        let mut row_group_writer = writer.next_row_group(num_batch)?;
        let mut col_index = 0;
        num_batch += 1;
        let num_rows = buffer.num_rows();
        info!("Fetched batch {} with {} rows.", num_batch, num_rows);
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

            strategies[col_index].2.copy_odbc_to_parquet(
                &mut pb,
                &mut column_writer,
                odbc_column,
            )?;

            row_group_writer.close_column(column_writer)?;
            col_index += 1;
        }
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

type ColumnInfo = (u16, String, Box<dyn ColumnFetchStrategy>);

fn make_schema(
    cursor: &impl Cursor,
    use_utf16: bool,
    prefer_varbinary: bool,
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
            format!("Column{}", index)
        } else {
            name
        };

        if let Some(column_fetch_strategy) = strategy_from_column_description(
            &cd,
            &name,
            prefer_varbinary,
            use_utf16,
            cursor,
            index,
        )? {
            odbc_buffer_desc.push((index as u16, name, column_fetch_strategy));
        }
    }

    Ok(odbc_buffer_desc)
}

fn parquet_schema_from_strategies(
    strategies: &[(u16, String, Box<dyn ColumnFetchStrategy>)],
) -> TypePtr {
    let mut fields = strategies
        .iter()
        .map(|(_index, name, s)| Arc::new(s.parquet_type(name)))
        .collect();
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}
