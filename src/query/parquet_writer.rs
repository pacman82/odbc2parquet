use std::{
    fs::{remove_file, File},
    io::{stdout, Write},
    mem::swap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{format_err, Error};
use bytesize::ByteSize;
use io_arg::IoArg;
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

/// Options influencing the output parquet file independent of schema or row content.
pub struct ParquetWriterOptions {
    /// Directly correlated to the `--column-compression-default` command line option
    pub column_compression_default: Compression,
    /// Tuples of column name and encoding which control the encoding for the associated columns.
    pub column_encodings: Vec<(String, Encoding)>,
    /// Number of digits in the suffix, appended to the end of a file in case they are numbered.
    pub suffix_length: usize,
    /// A fuzzy limit for file size, causing the rest of the query to be written into new files if
    /// a threshold is passed.
    pub file_size: FileSizeLimit,
    /// Do not create a file if no row was in the result set.
    pub no_empty_file: bool,
}

/// Wraps parquet SerializedFileWriter. Handles splitting into new files after maximum amount of
/// batches is reached.
pub struct ParquetWriter {
    path: Option<PathBuf>,
    schema: Arc<Type>,
    properties: Arc<WriterProperties>,
    writer: SerializedFileWriter<Box<dyn Write>>,
    file_size: FileSizeLimit,
    num_file: u32,
    /// Keep track of curret file size so we can split it, should it get too large.
    current_file_size: ByteSize,
    /// Length of the suffix, appended to the end of a file in case they are numbered.
    suffix_length: usize,
    no_empty_file: bool,
}

impl ParquetWriter {
    pub fn new(
        output: IoArg,
        schema: Arc<Type>,
        options: ParquetWriterOptions,
    ) -> Result<Self, Error> {
        // Write properties
        // Seems to also work fine without setting the batch size explicitly, but what the heck. Just to
        // be on the safe side.
        let mut wpb =
            WriterProperties::builder().set_compression(options.column_compression_default);
        for (column_name, encoding) in options.column_encodings {
            let col = ColumnPath::new(vec![column_name]);
            wpb = wpb.set_column_encoding(col, encoding)
        }
        let properties = Arc::new(wpb.build());

        let (file_path, base_path) = match output {
            IoArg::StdStream => (None, None),
            IoArg::File(path) => {
                if options.file_size.output_is_splitted() {
                    let file_path = Self::path_with_suffix(&path, 1, options.suffix_length)?;
                    (Some(file_path), Some(path))
                } else {
                    let file_path = path.clone();
                    (Some(file_path), Some(path))
                }
            }
        };

        let output: Box<dyn Write> = if let Some(path) = &file_path {
            Box::new(create_output_file(path)?)
        } else {
            Box::new(stdout().lock())
        };

        let writer = SerializedFileWriter::new(output, schema.clone(), properties.clone())?;

        Ok(Self {
            path: base_path,
            schema,
            properties,
            writer,
            file_size: options.file_size,
            num_file: 1,
            current_file_size: ByteSize::b(0),
            suffix_length: options.suffix_length,
            no_empty_file: options.no_empty_file,
        })
    }

    pub fn update_current_file_size(&mut self, row_group_size: i64) {
        self.current_file_size += ByteSize::b(row_group_size.try_into().unwrap());
    }

    /// Retrieve the next row group writer. May trigger creation of a new file if limit of the
    /// previous one is reached.
    ///
    /// # Parametersc
    ///
    /// * `num_batch`: Zero based num batch index
    pub fn next_row_group(
        &mut self,
        num_batch: u32,
    ) -> Result<SerializedRowGroupWriter<'_, Box<dyn Write>>, Error> {
        // Check if we need to write the next batch into a new file
        if self
            .file_size
            .should_start_new_file(num_batch, self.current_file_size)
        {
            self.num_file += 1;
            self.current_file_size = ByteSize::b(0);
            let path = Self::path_with_suffix(
                self.path.as_deref().unwrap(),
                self.num_file,
                self.suffix_length,
            )?;
            let file: Box<dyn Write> = Box::new(create_output_file(&path)?);

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
        if let Some(path) = (self.current_file_size == ByteSize::b(0) && self.no_empty_file)
            .then_some(self.path)
            .flatten()
        {
            remove_file(path)?;
        }
        Ok(())
    }

    fn path_with_suffix(
        path: &Path,
        num_file: u32,
        suffix_length: usize,
    ) -> Result<PathBuf, Error> {
        let suffix = pad_number(num_file, suffix_length);
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

fn create_output_file(path: &Path) -> Result<File, Error> {
    File::create(path).map_err(|io_err| {
        Error::from(io_err).context(format!(
            "Could not create output file '{}'",
            path.to_string_lossy()
        ))
    })
}

fn pad_number(num_file: u32, suffix_length: usize) -> String {
    let num_file = num_file.to_string();
    let num_leading_zeroes = if suffix_length > num_file.len() {
        suffix_length - num_file.len()
    } else {
        // Suffix is already large enough (if not too large) without leading zeroes
        0
    };
    let padding = "0".repeat(num_leading_zeroes);
    let suffix = format!("_{padding}{num_file}");
    suffix
}
