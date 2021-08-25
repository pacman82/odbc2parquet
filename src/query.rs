mod batch_size_limit;
mod strategy;

use self::{
    batch_size_limit::BatchSizeLimit,
    strategy::{strategy_from_column_description, FnWriteParquetColumn},
};

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, format_err, Error};
use log::{debug, info};
use odbc_api::{
    buffers::{BufferDescription, ColumnarRowSet},
    ColumnDescription, Cursor, Environment, IntoParameter,
};
use parquet::{
    basic::{Compression, Encoding},
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
    },
    schema::types::{ColumnPath, Type, TypePtr},
};

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

/// Options influencing the output parquet format.
struct ParquetFormatOptions {
    column_compression_default: Compression,
    column_encodings: Vec<(String, Encoding)>,
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
        .map(|strategy| strategy.buffer_description.bytes_per_row())
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

    let mut odbc_buffer = ColumnarRowSet::with_column_indices(
        batch_size_row,
        strategies
            .iter()
            .map(|strategy| (strategy.index, strategy.buffer_description)),
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

            let odbc_to_parquet_col = &strategies[col_index].odbc_to_parquet;

            odbc_to_parquet_col(&mut pb, &mut column_writer, odbc_column)?;

            row_group_writer.close_column(column_writer)?;
            col_index += 1;
        }
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

/// All information required to fetch a column from odbc and transfer its data to Parquet.
struct ColumnFetchStrategy {
    /// One based index of the column.
    index: u16,
    /// Parquet column type
    parquet_type: Type,
    /// Description of the buffer bound to the ODBC data source.
    buffer_description: BufferDescription,
    /// Function writing the data from an ODBC buffer with a parquet column writer.
    odbc_to_parquet: Box<FnWriteParquetColumn>,
}

fn make_schema(
    cursor: &impl Cursor,
    use_utf16: bool,
    prefer_varbinary: bool,
) -> Result<Vec<ColumnFetchStrategy>, Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut odbc_buffer_desc = Vec::new();
    let mut fields = Vec::new();

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

        if let Some((parquet_type, odbc_to_parquet, buffer_description)) =
            strategy_from_column_description(
                &cd,
                &name,
                prefer_varbinary,
                use_utf16,
                cursor,
                index,
            )?
        {
            fields.push(Arc::new(parquet_type.clone()));
            odbc_buffer_desc.push(ColumnFetchStrategy {
                index: index as u16,
                parquet_type,
                buffer_description,
                odbc_to_parquet,
            });
        }
    }

    Ok(odbc_buffer_desc)
}

fn parquet_schema_from_strategies(strategies: &[ColumnFetchStrategy]) -> TypePtr {
    let mut fields = strategies
        .iter()
        .map(|s| Arc::new(s.parquet_type.clone()))
        .collect();
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}

/// Wraps parquet SerializedFileWriter. Handles splitting into new files after maximum amount of
/// batches is reached.
struct ParquetWriter<'p> {
    path: &'p Path,
    schema: Arc<Type>,
    properties: Arc<WriterProperties>,
    writer: SerializedFileWriter<File>,
    batches_per_file: u32,
}

impl<'p> ParquetWriter<'p> {
    pub fn new(
        path: &'p Path,
        schema: Arc<Type>,
        batches_per_file: u32,
        format_options: ParquetFormatOptions,
    ) -> Result<Self, Error> {
        // Write properties
        // Seems to also work fine without setting the batch size explicitly, but what the heck. Just to
        // be on the safe side.
        let mut wpb =
            WriterProperties::builder().set_compression(format_options.column_compression_default);
        for (column_name, encoding) in format_options.column_encodings {
            let col = ColumnPath::new(vec![column_name]);
            wpb = wpb.set_column_encoding(col, encoding)
        }
        let properties = Arc::new(wpb.build());
        let file = if batches_per_file == 0 {
            File::create(path)?
        } else {
            File::create(Self::path_with_suffix(path, "_1")?)?
        };
        let writer = SerializedFileWriter::new(file, schema.clone(), properties.clone())?;

        Ok(Self {
            path,
            schema,
            properties,
            writer,
            batches_per_file,
        })
    }

    /// Retrieve the next row group writer. May trigger creation of a new file if limit of the
    /// previous one is reached.
    ///
    /// # Parameters
    ///
    /// * `num_batch`: Zero based num batch index
    pub fn next_row_group(&mut self, num_batch: u32) -> Result<Box<dyn RowGroupWriter>, Error> {
        // Check if we need to write the next batch into a new file
        if num_batch != 0 && self.batches_per_file != 0 && num_batch % self.batches_per_file == 0 {
            self.writer.close()?;
            let suffix = format!("_{}", (num_batch / self.batches_per_file) + 1);
            let path = Self::path_with_suffix(self.path, &suffix)?;
            let file = File::create(path)?;
            self.writer =
                SerializedFileWriter::new(file, self.schema.clone(), self.properties.clone())?;
        }
        Ok(self.writer.next_row_group()?)
    }

    fn close_row_group(
        &mut self,
        row_group_writer: Box<dyn RowGroupWriter>,
    ) -> Result<(), ParquetError> {
        self.writer.close_row_group(row_group_writer)
    }

    pub fn close(&mut self) -> Result<(), ParquetError> {
        self.writer.close()?;
        Ok(())
    }

    fn path_with_suffix(path: &Path, suffix: &str) -> Result<PathBuf, Error> {
        let mut stem = path
            .file_stem()
            .ok_or_else(|| format_err!("Output needs To have a file stem."))?
            .to_owned();
        stem.push(suffix);
        let mut path_with_suffix = path.with_file_name(stem);
        path_with_suffix = path_with_suffix.with_extension("par");
        Ok(path_with_suffix)
    }
}
