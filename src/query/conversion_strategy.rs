use anyhow::{anyhow, bail, Context, Error};
use log::{debug, info};
use odbc_api::{buffers::ColumnarAnyBuffer, ColumnDescription, ResultSetMetadata};
use parquet::{
    file::writer::SerializedColumnWriter,
    schema::types::{Type, TypePtr},
};
use std::sync::Arc;

use crate::parquet_buffer::ParquetBuffer;

use super::{
    column_strategy::{strategy_from_column_description, ColumnStrategy, MappingOptions},
    fetch_batch::FetchBatch,
    parquet_writer::ParquetOutput,
};

/// Contains the decisions of how to fetch each columns of a table from an ODBC data source and copy
/// it into a parquet file. This decisions include what kind of ODBC C_TYPE to use to fetch the data
/// and in what these columns are transformed.
pub struct ConversionStrategy {
    columns: Vec<ColumnInfo>,
    parquet_schema: TypePtr,
}

/// Name, ColumnStrategy
type ColumnInfo = (String, Box<dyn ColumnStrategy>);

impl ConversionStrategy {
    pub fn new(
        cursor: &mut impl ResultSetMetadata,
        mapping_options: MappingOptions,
    ) -> Result<Self, Error> {
        let num_cols = cursor.num_result_cols()?;

        let mut columns = Vec::new();

        for index in 1..(num_cols + 1) {
            let mut cd = ColumnDescription::default();
            // Reserving helps with drivers not reporting column name size correctly.
            cd.name.reserve(128);
            cursor.describe_col(index as u16, &mut cd)?;

            debug!(
                "ODBC column description for column {index}: name: '{}', \
                relational type: '{:?}', \
                nullability: {:?}",
                cd.name_to_string().unwrap_or_default(),
                cd.data_type,
                cd.nullability
            );

            let name = cd.name_to_string()?;
            // Give a generated name, should we fail to retrieve one from the ODBC data source.
            let name = if name.is_empty() {
                format!("Column{index}")
            } else {
                name
            };

            let column_fetch_strategy =
                strategy_from_column_description(&cd, &name, mapping_options, cursor, index)?;
            columns.push((name, column_fetch_strategy));
        }

        if columns.is_empty() {
            bail!("Resulting parquet file would not have any columns!")
        }

        let fields = columns
            .iter()
            .map(|(name, s)| Arc::new(s.parquet_type(name)))
            .collect();
        let parquet_schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(fields)
                .build()
                .unwrap(),
        );

        Ok(ConversionStrategy {
            columns,
            parquet_schema,
        })
    }

    /// Size of a single fetch buffer per row
    pub fn fetch_buffer_size_per_row(&self) -> usize {
        self.columns
            .iter()
            .map(|(_name, strategy)| strategy.buffer_desc().bytes_per_row())
            .sum()
    }

    pub fn allocate_fetch_buffer(&self, batch_size_row: usize) -> ColumnarAnyBuffer {
        ColumnarAnyBuffer::from_descs(
            batch_size_row,
            self.columns
                .iter()
                .map(|(_name, strategy)| strategy.buffer_desc()),
        )
    }

    pub fn parquet_schema(&self) -> TypePtr {
        self.parquet_schema.clone()
    }

    pub fn block_cursor_to_parquet(
        &self,
        mut fetch_strategy: Box<dyn FetchBatch>,
        mut writer: Box<dyn ParquetOutput>,
    ) -> Result<(), Error> {
        let mut num_batch = 0;
        // Count the number of total rows fetched so far for logging. This should be identical to
        // `num_batch * batch_size_row + num_rows`.
        let mut total_rows_fetched = 0;

        let mut pb = ParquetBuffer::new(fetch_strategy.max_batch_size_in_rows());

        while let Some(buffer) = fetch_strategy
            .next_batch()
            .map_err(|e| self.translate_fetch_error(e))?
        {
            num_batch += 1;
            let num_rows = buffer.num_rows();
            total_rows_fetched += num_rows;
            info!("Fetched batch {num_batch} with {num_rows} rows.");
            info!("Fetched {total_rows_fetched} rows in total.");
            self.write_batch(&mut writer, num_batch, buffer, &mut pb)?;
        }
        writer.close_box()?;
        Ok(())
    }

    fn write_batch(
        &self,
        writer: &mut Box<dyn ParquetOutput>,
        num_batch: u32,
        buffer: &ColumnarAnyBuffer,
        pb: &mut ParquetBuffer,
    ) -> Result<(), Error> {
        let num_rows = buffer.num_rows();
        pb.set_num_rows_fetched(num_rows);

        let column_exporter = ColumnExporter {
            buffer,
            conversion_buffer: pb,
            columns: &self.columns,
        };

        writer.write_row_group(num_batch, column_exporter)?;
        Ok(())
    }

    /// Enrich or translate the `odbc_api::Error` with information about flags and options which
    /// could be set in order to resolve them in the next run of `odbc2parquet`.
    fn translate_fetch_error(&self, error: odbc_api::Error) -> Error {
        match error {
            // If we hit the issue with oracle not supporting 64Bit, let's tell our users that we
            // have implemented a solution to it.
            error @ odbc_api::Error::OracleOdbcDriverDoesNotSupport64Bit(_) => {
                let error: Error = error.into();
                error.context(
                    "Looks like you are using an Oracle database. Try the \
                `--driver-does-not-support-64bit-integers` flag.",
                )
            }
            // It is important to give good hints for truncation errors, as they are rooted in the
            // how ODBC expects column buffers allocated, which is fair to assume few people know.
            // At least in the context of `odbc2parquet` we can tell them which flags to set, in
            // order to get rid of the problem.
            odbc_api::Error::TooLargeValueForBuffer {
                indicator,
                buffer_index,
            } => {
                let indicator_msg = if let Some(length) = indicator {
                    format!(
                        "The driver indicated an actual length of {length}."
                    )
                } else {
                    "Sadly the driver did not return a length indicator for the value, so you will \
                    have to guess its actual length."
                        .to_owned()
                };
                let column_name = self.columns[buffer_index].0.as_str();
                anyhow!(format!(
                    "A field exceeds the maximum element length of a column buffer. You can use \
                    the `--column-length-limit` flag to adjust the limit for text columns in \
                    characters. {indicator_msg} The error occurred for column {column_name}."
                ))
            }
            other => other.into(),
        }
    }
}

/// Exposes the contents from a fetch buffer column by column to a parquet serializer
pub struct ColumnExporter<'a> {
    buffer: &'a ColumnarAnyBuffer,
    conversion_buffer: &'a mut ParquetBuffer,
    columns: &'a [(String, Box<dyn ColumnStrategy>)],
}

impl ColumnExporter<'_> {
    pub fn export_nth_column(
        &mut self,
        col_index: usize,
        column_writer: &mut SerializedColumnWriter,
    ) -> Result<(), Error> {
        let col_name = &self.columns[col_index].0;
        debug!("Writing column with index {col_index} and name '{col_name}'.");
        let odbc_column = self.buffer.column(col_index);
        self.columns[col_index]
            .1
            .copy_odbc_to_parquet(self.conversion_buffer, column_writer.untyped(), odbc_column)
            .with_context(|| {
                format!("Failed to copy column '{col_name}' from ODBC representation into Parquet.")
            })?;
        Ok::<(), Error>(())
    }
}
