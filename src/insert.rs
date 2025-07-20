use std::fs::File;

use anyhow::Error;
use log::info;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::{
    connection::open_connection,
    input::{copy_from_db_to_parquet, parquet_type_to_odbc_buffer_desc, IndexMapping},
    InsertOpt,
};

/// Read the content of a parquet file and insert it into a table.
pub fn insert(insert_opt: &InsertOpt) -> Result<(), Error> {
    let InsertOpt {
        encoding,
        input,
        connect_opts,
        table,
    } = insert_opt;

    let odbc_conn = open_connection(connect_opts)?;

    let file = File::open(input)?;
    let reader = SerializedFileReader::new(file)?;

    let parquet_metadata = reader.metadata();
    let schema_desc = parquet_metadata.file_metadata().schema_descr();
    let num_columns = schema_desc.num_columns();

    let column_descriptions: Vec<_> = (0..num_columns).map(|i| schema_desc.column(i)).collect();
    let column_names: Vec<&str> = column_descriptions
        .iter()
        .map(|col_desc| col_desc.name())
        .collect();
    let mut odbc_buf_desc = Vec::new();
    let mut copy_col_fns = Vec::new();
    for col_desc in &column_descriptions {
        let (buf_desc, odbc_to_parquet) =
            parquet_type_to_odbc_buffer_desc(col_desc, encoding.use_utf16())?;
        odbc_buf_desc.push(buf_desc);
        copy_col_fns.push(odbc_to_parquet);
    }
    let insert_statement = insert_statement_text(table, &column_names);
    let statement = odbc_conn.prepare(&insert_statement)?;

    let odbc_inserter = statement.into_column_inserter(1, odbc_buf_desc)?;

    let mapping = IndexMapping::ordered_parameters(num_columns);

    copy_from_db_to_parquet(reader, &mapping, odbc_inserter, copy_col_fns)?;
    Ok(())
}

fn insert_statement_text(table: &str, column_names: &[&str]) -> String {
    // Generate statement text from table name and headline
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let statement_text = format!("INSERT INTO {table} ({columns}) VALUES ({values});");
    info!("Insert statement Text: {}", statement_text);
    statement_text
}
