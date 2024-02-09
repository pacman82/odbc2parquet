use std::{
    fs::File,
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
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::types::{ColumnPath, Type},
};
use tempfile::TempPath;

use super::{batch_size_limit::FileSizeLimit, current_file::CurrentFile};

/// Options influencing the output parquet file independent of schema or row content.
pub struct ParquetWriterOptions {
    /// Directly correlated to the `--column-compression-default` command line option
    pub column_compression_default: Compression,
    /// Tuples of column name and encoding which control the encoding for the associated columns.
    pub column_encodings: Vec<(String, Encoding)>,
    /// Number of digits in the suffix, appended to the end of a file in case they are numbered.
    pub suffix_length: usize,
    /// A fuzzy limit for file size, causing the rest of the query to be written into new files if a
    /// threshold is passed.
    pub file_size: FileSizeLimit,
    /// Do not create a file if no row was in the result set.
    pub no_empty_file: bool,
}

pub fn parquet_output(
    output: IoArg,
    schema: Arc<Type>,
    options: ParquetWriterOptions,
) -> Result<Box<dyn ParquetOutput>, Error> {
    // Write properties
    // Seems to also work fine without setting the batch size explicitly, but what the heck. Just to
    // be on the safe side.
    let mut wpb = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(options.column_compression_default);
    for (column_name, encoding) in options.column_encodings.clone() {
        let col = ColumnPath::new(vec![column_name]);
        wpb = wpb.set_column_encoding(col, encoding)
    }
    let properties = Arc::new(wpb.build());

    let writer: Box<dyn ParquetOutput> = match output {
        IoArg::StdStream => Box::new(StandardOut::new(schema, properties)?),
        IoArg::File(path) => Box::new(FileWriter::new(path, schema, options, properties)?),
    };

    Ok(writer)
}

/// Writes row groups to the output, which could be either standard out, a single parquet file or
/// multiple parquet files with incrementing number suffixes.
pub trait ParquetOutput {
    /// In case we have a size limit for individual files, we need to keep track of the individual
    /// file size.
    fn update_current_file_size(&mut self, row_group_size: i64);

    /// Retrieve the next row group writer. May trigger creation of a new file if limit of the
    /// previous one is reached.
    ///
    /// # Parametersc
    ///
    /// * `num_batch`: Zero based num batch index
    fn next_row_group(
        &mut self,
        num_batch: u32,
    ) -> Result<SerializedRowGroupWriter<'_, Box<dyn Write + Send>>, Error>;

    /// Indicate that no further output is written. this triggers writing the parquet meta data and
    /// potentially persists a temporary file.
    fn close(self) -> Result<(), Error>;

    fn close_box(self: Box<Self>) -> Result<(), Error>;
}

/// Wraps parquet SerializedFileWriter. Handles splitting into new files after maximum amount of
/// batches is reached.
struct FileWriter {
    base_path: PathBuf,
    schema: Arc<Type>,
    properties: Arc<WriterProperties>,
    file_size: FileSizeLimit,
    num_file: u32,
    /// Length of the suffix, appended to the end of a file in case they are numbered.
    suffix_length: usize,
    no_empty_file: bool,
    current_file: CurrentFile,
}

impl FileWriter {
    pub fn new(
        path: PathBuf,
        schema: Arc<Type>,
        options: ParquetWriterOptions,
        properties: Arc<WriterProperties>,
    ) -> Result<Self, Error> {
        let suffix = {
            if options.file_size.output_is_splitted() {
                Some((1, options.suffix_length))
            } else {
                None
            }
        };

        let current_path = Self::current_path(&path, suffix)?;
        let current_file = CurrentFile::new(current_path, schema.clone(), properties.clone())?;

        Ok(Self {
            base_path: path,
            schema,
            properties,
            file_size: options.file_size,
            num_file: 1,
            suffix_length: options.suffix_length,
            no_empty_file: options.no_empty_file,
            current_file,
        })
    }

    fn current_path(base_path: &Path, suffix: Option<(u32, usize)>) -> Result<PathBuf, Error> {
        let path = if let Some((num_file, suffix_length)) = suffix {
            path_with_suffix(base_path, num_file, suffix_length)?
        } else {
            base_path.to_owned()
        };
        Ok(path)
    }

    fn create_output_file(path: &Path) -> Result<File, Error> {
        File::create(path).map_err(|io_err| {
            Error::from(io_err).context(format!(
                "Could not create output file '{}'",
                path.to_string_lossy()
            ))
        })
    }
}

impl ParquetOutput for FileWriter {
    fn update_current_file_size(&mut self, row_group_size: i64) {
        self.current_file.file_size += ByteSize::b(row_group_size.try_into().unwrap());
    }

    fn next_row_group(
        &mut self,
        num_batch: u32,
    ) -> Result<SerializedRowGroupWriter<'_, Box<dyn Write + Send>>, Error> {
        // Check if we need to write the next batch into a new file
        if self
            .file_size
            .should_start_new_file(num_batch, self.current_file.file_size)
        {
            // Create next file path
            self.num_file += 1;
            // Reset current file size, so the next file will not be considered too large immediatly
            self.current_file.file_size = ByteSize::b(0);
            let mut tmp_current_path = TempPath::from_path(Self::current_path(
                &self.base_path,
                Some((self.num_file, self.suffix_length)),
            )?);
            swap(&mut self.current_file.path, &mut tmp_current_path);
            // Persist last file
            tmp_current_path.keep()?;
            let file: Box<dyn Write + Send> =
                Box::new(Self::create_output_file(&self.current_file.path)?);

            // Create new writer as tmp writer
            let mut tmp_writer =
                SerializedFileWriter::new(file, self.schema.clone(), self.properties.clone())?;
            // Make the old writer the tmp_writer, so we can call .close on it, which destroys it.
            // Make the new writer self.writer, so we will use it to insert the new data.
            swap(&mut self.current_file.writer, &mut tmp_writer);
            tmp_writer.close()?;
        }
        Ok(self.current_file.writer.next_row_group()?)
    }

    fn close(self) -> Result<(), Error> {
        self.current_file.writer.close()?;
        if self.current_file.file_size != ByteSize::b(0) || !self.no_empty_file {
            self.current_file.path.keep()?;
        }
        Ok(())
    }

    fn close_box(self: Box<Self>) -> Result<(), Error> {
        self.close()
    }
}

/// Stream parquet directly to standard out
struct StandardOut {
    writer: SerializedFileWriter<Box<dyn Write + Send>>,
}

impl StandardOut {
    pub fn new(schema: Arc<Type>, properties: Arc<WriterProperties>) -> Result<Self, Error> {
        let output: Box<dyn Write + Send> = Box::new(stdout());
        let writer = SerializedFileWriter::new(output, schema.clone(), properties.clone())?;

        Ok(Self { writer })
    }
}

impl ParquetOutput for StandardOut {
    fn update_current_file_size(&mut self, _row_group_size: i64) {}

    fn next_row_group(
        &mut self,
        _num_batch: u32,
    ) -> Result<SerializedRowGroupWriter<'_, Box<dyn Write + Send>>, Error> {
        Ok(self.writer.next_row_group()?)
    }

    fn close(self) -> Result<(), Error> {
        self.writer.close()?;
        Ok(())
    }

    fn close_box(self: Box<Self>) -> Result<(), Error> {
        self.close()
    }
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

fn path_with_suffix(path: &Path, num_file: u32, suffix_length: usize) -> Result<PathBuf, Error> {
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
