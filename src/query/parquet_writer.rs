use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{format_err, Error};
use parquet::{
    basic::{Compression, Encoding},
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
    },
    schema::types::{ColumnPath, Type},
};

/// Options influencing the output parquet format.
pub struct ParquetFormatOptions {
    pub column_compression_default: Compression,
    pub column_encodings: Vec<(String, Encoding)>,
}

/// Wraps parquet SerializedFileWriter. Handles splitting into new files after maximum amount of
/// batches is reached.
pub struct ParquetWriter<'p> {
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

    pub fn close_row_group(
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
