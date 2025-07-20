use std::{fs::File, mem::swap};

use anyhow::Error;
use parquet::file::reader::{FileReader as _, SerializedFileReader};

use crate::{
    connection::open_connection,
    input::{copy_from_db_to_parquet, parquet_type_to_odbc_buffer_desc, IndexMapping},
    ExecOpt,
};

pub fn execute(exec_opt: &ExecOpt) -> Result<(), Error> {
    let ExecOpt {
        connect_opts,
        encoding,
        input,
        statement,
    } = exec_opt;

    let odbc_conn = open_connection(connect_opts)?;

    let file = File::open(input)?;
    let reader = SerializedFileReader::new(file)?;
    let (statement_text, placeholder_names_by_position) = to_positional_arguments(&statement);
    let parquet_metadata = reader.metadata();
    let schema_desc = parquet_metadata.file_metadata().schema_descr();

    let mapping = IndexMapping::from_named_parameters(placeholder_names_by_position, schema_desc)?;
    let statement = odbc_conn.prepare(&statement_text)?;

    let parquet_column_descs_in_order_of_column_bufs: Vec<_> = mapping
        .parquet_indices_in_order_of_column_buffers()
        .map(|index_pq| {
            let desc = schema_desc.column(index_pq);
            Ok(desc)
        })
        .collect::<Result<_, Error>>()?;

    let mut odbc_buf_desc = Vec::new();
    let mut copy_col_fns = Vec::new();
    for col_desc in &parquet_column_descs_in_order_of_column_bufs {
        let (buf_desc, odbc_to_parquet) =
            parquet_type_to_odbc_buffer_desc(col_desc, encoding.use_utf16())?;
        odbc_buf_desc.push(buf_desc);
        copy_col_fns.push(odbc_to_parquet);
    }

    let odbc_inserter = statement.into_column_inserter_with_mapping(1, odbc_buf_desc, &mapping)?;
    copy_from_db_to_parquet(reader, &mapping, odbc_inserter, copy_col_fns)?;

    Ok(())
}

/// Takes an SQL statement with named arguments and repalaces them with positional arguments.
/// Additionally, the mapping of positions to names is returned.
///
/// # Returns
///
/// * First element is the SQL statement with positional placeholders (`?`) as required by ODBC.
/// * Second element is a list which contains an entry for each positional placeholder in order of
///   appearance. Each entry is the name of the parquet column, that it is corresponding to. The
///   sameparquet column can appear in multiple positions, and therefore may appear multiple times
///   in the list.
fn to_positional_arguments(statement_with_named_args: &str) -> (String, Vec<String>) {
    let mut statement_with_positional_args = String::new();
    let mut mapping = Vec::new();
    // `true` if we currently parse a placeholder name. `false`, if we parse statement text.
    let mut is_placeholder_name = false;
    // Keeps track of the current placeholder name, if we are parsing one.
    let mut current_placeholder_name = String::new();
    // If we encounter a backslash, we mask the next character.
    let mut mask_next_char = false;
    for c in statement_with_named_args.chars() {
        match c {
            _ if mask_next_char => {
                if is_placeholder_name {
                    current_placeholder_name.push(c);
                } else {
                    statement_with_positional_args.push(c);
                }
                mask_next_char = false;
            }
            '\\' => {
                mask_next_char = true;
            }
            '?' => {
                if is_placeholder_name {
                    // At the end of a placeholder name the current placeholder name is finished and
                    // we can add it to the mapping.
                    mapping.push(String::new());
                    swap(&mut current_placeholder_name, mapping.last_mut().unwrap());
                } else {
                    // At the beginning of a new placeholder, we place a positional placeholder in
                    // the resulting statement.
                    statement_with_positional_args.push('?')
                }
                is_placeholder_name = !is_placeholder_name;
            }
            _ => {
                if is_placeholder_name {
                    current_placeholder_name.push(c);
                } else {
                    statement_with_positional_args.push(c);
                }
            }
        }
    }
    (statement_with_positional_args, mapping)
}

#[cfg(test)]
mod tests {
    use super::to_positional_arguments;

    #[test]
    fn replace_named_args_with_positional_placeholders() {
        // Given
        let statement_with_named_args = "INSERT INTO table (col1, col2) VALUES (?col1?, ?col2?)";
        // When
        let (statement_with_positional_args, mapping) =
            to_positional_arguments(statement_with_named_args);
        // Then
        assert_eq!(
            statement_with_positional_args,
            "INSERT INTO table (col1, col2) VALUES (?, ?)"
        );
        assert_eq!(mapping, &["col1".to_string(), "col2".to_string()]);
    }

    #[test]
    fn use_backslash_to_escape_question_mark() {
        // Given
        let statement_with_named_args = "UPDATE table SET col1 = '\\?' WHERE col2 = ?a?";
        // When
        let (statement_with_positional_args, mapping) =
            to_positional_arguments(statement_with_named_args);
        // Then
        assert_eq!(
            statement_with_positional_args,
            "UPDATE table SET col1 = '?' WHERE col2 = ?"
        );
        assert_eq!(mapping, &["a".to_string()]);
    }
}
