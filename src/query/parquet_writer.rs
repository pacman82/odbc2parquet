use std::{
    fs::File,
    mem::swap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{format_err, Error};
use bytesize::ByteSize;
use parquet::{
    basic::{Compression, Encoding},
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::types::{ColumnPath, Type},
};

use super::batch_size_limit::FileSizeLimit;

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
    file_size: FileSizeLimit,
    num_file: u32,
    /// Keep track of curret file size so we can split it, should it get too large.
    current_file_size: ByteSize,
}

impl<'p> ParquetWriter<'p> {
    pub fn new(
        path: &'p Path,
        schema: Arc<Type>,
        file_size: FileSizeLimit,
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
        let file = if file_size.output_is_splitted() {
            File::create(Self::path_with_suffix(path, "_1")?)?
        } else {
            File::create(path)?
        };
        let writer = SerializedFileWriter::new(file, schema.clone(), properties.clone())?;

        Ok(Self {
            path,
            schema,
            properties,
            writer,
            file_size,
            num_file: 1,
            current_file_size: ByteSize::b(0)
        })
    }

    pub fn update_current_file_size(&mut self, row_group_size: i64) {
        self.current_file_size += ByteSize::b(row_group_size.try_into().unwrap());
    }

    /// Retrieve the next row group writer. May trigger creation of a new file if limit of the
    /// previous one is reached.
    ///
    /// # Parameters
    ///
    /// * `num_batch`: Zero based num batch index
    pub fn next_row_group(
        &mut self,
        num_batch: u32,
    ) -> Result<SerializedRowGroupWriter<'_, File>, Error> {
        // Check if we need to write the next batch into a new file
        if self.file_size.should_start_new_file(num_batch, self.current_file_size) {
            self.num_file += 1;
            self.current_file_size = ByteSize::b(0);
            let suffix = format!("_{}", self.num_file);
            let path = Self::path_with_suffix(self.path, &suffix)?;
            let file = File::create(path)?;

            // Create new writer as tmp writer
            let mut tmp_writer =
                SerializedFileWriter::new(file, self.schema.clone(), self.properties.clone())?;
            // Make the old writer the tmp_writer, so we can call .close on it, which destroys it.
            // Make the new writer self.writer, so we will use it to insert the new data.
            swap(&mut self.writer, &mut tmp_writer);
            tmp_writer.close()?;
        }
        Ok(self.writer.next_row_group()?)
    }

    pub fn close(self) -> Result<(), ParquetError> {
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
