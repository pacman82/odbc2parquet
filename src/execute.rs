use std::{collections::HashMap, fs::File, mem::swap};

use anyhow::{anyhow, Error};
use log::info;
use odbc_api::InputParameterMapping;
use parquet::{
    file::reader::{FileReader as _, SerializedFileReader},
    schema::types::SchemaDescriptor,
};

use crate::{
    connection::open_connection, input::parquet_type_to_odbc_buffer_desc,
    parquet_buffer::ParquetBuffer, ExecOpt,
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

    let mapping = IndexMapping::new(placeholder_names_by_position, schema_desc)?;
    let statement = odbc_conn.prepare(&statement_text)?;

    let parquet_column_descs_in_order_of_column_bufs: Vec<_> = mapping
        .parquet_indices_in_order_of_column_buffers()
        .map(|index_pq| {
            let desc = schema_desc.column(index_pq);
            Ok(desc)
        })
        .collect::<Result<_, Error>>()?;

    // The order of the column buffer descriptions will be the order of the positional parameters
    // and the order of the columns in the allocated ODBC transport buffer. Yet, it may not be the
    // order of the columns in the parquet file, which is why we keep track of the index in parquet
    // separately.
    let column_buf_desc: Vec<_> = parquet_column_descs_in_order_of_column_bufs
        .iter()
        .map(|col_desc| {
            let (buf_desc, odbc_to_parquet) =
                parquet_type_to_odbc_buffer_desc(col_desc, encoding.use_utf16())?;
            Ok((buf_desc, odbc_to_parquet))
        })
        .collect::<Result<_, Error>>()?;

    let num_row_groups = reader.num_row_groups();

    // Start with a small initial batch size and reallocate as we encounter larger row groups.
    let mut batch_size = 1;
    let mut odbc_buffer = statement.into_column_inserter_with_mapping(
        batch_size,
        column_buf_desc.iter().map(|(desc, _copy_col)| *desc),
        &mapping,
    )?;

    let mut pb = ParquetBuffer::new(batch_size);

    for row_group_index in 0..num_row_groups {
        info!(
            "Insert row group {} of {}.",
            row_group_index, num_row_groups
        );
        let row_group_reader = reader.get_row_group(row_group_index)?;
        let num_rows: usize = row_group_reader
            .metadata()
            .num_rows()
            .try_into()
            .expect("Number of rows in row group of parquet file must be non negative");
        // Ensure that num rows is less than batch size of originally created buffers.
        if num_rows > batch_size {
            batch_size = num_rows;
            let descs = column_buf_desc.iter().map(|(desc, _)| *desc);
            // An inefficiency here: Currently `odbc-api`s interface forces us to prepare the
            // statement again, in case we need to allocate more row groups.
            odbc_buffer = odbc_conn
                .prepare(&statement_text)?
                .into_column_inserter(batch_size, descs)?;
        }
        odbc_buffer.set_num_rows(num_rows);
        pb.set_num_rows_fetched(num_rows);
        for (index_buf, index_pq) in mapping
            .parquet_indices_in_order_of_column_buffers()
            .enumerate()
        {
            let column_reader = row_group_reader.get_column_reader(index_pq)?;
            let column_writer = odbc_buffer.column_mut(index_buf);
            let (_, parquet_to_odbc_col) = &column_buf_desc[index_buf];
            parquet_to_odbc_col(num_rows, &mut pb, column_reader, column_writer)?;
        }

        odbc_buffer.execute()?;
    }

    Ok(())
}

// Governs the relation between the indices of the positional placeholders in the SQL statement,
// the inidices of the ODBC transport buffer columns and the indices of the parquet columns.
struct IndexMapping {
    buffer_to_parquet_index: Vec<usize>,
    // A zero based (!) parameter index is used to find the index of the matching odbc transport
    // column buffer. In ODBC a parameter index is 1-based, but shifting it to 0-based matches our
    // `Vec` better.
    parameter_to_buffer_index: Vec<usize>,
}

impl IndexMapping {
    /// # Parameters
    ///
    /// - `placeholder_names_by_position`: List of the placeholders. The index of the placeholder
    ///   indicates its position in the query. The value is the name. The same name can appear
    ///   multiple times.
    /// - `schema_desc`: Schema descriptor of parquet file obtained from its metadata. It is used to
    ///   reference the used placeholder names with the names of parquet columns and identify their
    ///   indices within the parquet file.
    pub fn new(
        placeholder_names_by_position: Vec<String>,
        schema_desc: &SchemaDescriptor,
    ) -> Result<Self, Error> {
        let num_columns = schema_desc.num_columns();
        let parquet_index_by_name: HashMap<String, usize> = (0..num_columns)
            .map(|index_pq| {
                let desc = schema_desc.column(index_pq);
                (desc.name().to_owned(), index_pq)
            })
            .collect();
        let buffer_to_parquet_index: Vec<usize> = placeholder_names_by_position
            .iter()
            .map(|name| {
                parquet_index_by_name
                    .get(name)
                    .ok_or_else(|| {
                        anyhow!("Parameter name {name} does not exist in parquet schema")
                    })
                    .copied()
            })
            .collect::<Result<_, Error>>()?;
        let parameter_to_buffer_index =
            (0..placeholder_names_by_position.len()).collect::<Vec<_>>();

        Ok(IndexMapping {
            buffer_to_parquet_index,
            parameter_to_buffer_index,
        })
    }

    /// Iterates over the parquet indices in order of the ODBC transparto buffers
    pub fn parquet_indices_in_order_of_column_buffers(&self) -> impl Iterator<Item = usize> + '_ {
        self.buffer_to_parquet_index.iter().copied()
    }
}

impl InputParameterMapping for &IndexMapping {
    fn parameter_index_to_column_index(&self, paramteter_index: u16) -> usize {
        self.parameter_to_buffer_index[(paramteter_index - 1) as usize]
    }

    fn num_parameters(&self) -> usize {
        self.parameter_to_buffer_index.len()
    }
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
