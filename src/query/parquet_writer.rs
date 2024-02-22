use std::{
    io::{stdout, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{format_err, Error};
use io_arg::IoArg;
use parquet::{
    basic::{Compression, Encoding},
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    schema::types::{ColumnPath, Type},
};

use super::{
    batch_size_limit::FileSizeLimit, current_file::CurrentFile, table_strategy::ColumnExporter,
};

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
    /// Retrieve the next row group writer. May trigger creation of a new file if limit of the
    /// previous one is reached.
    ///
    /// # Parametersc
    ///
    /// * `num_batch`: Zero based num batch index
    fn write_row_group(
        &mut self,
        num_batch: u32,
        export_nth_column: ColumnExporter,
    ) -> Result<(), Error>;

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
    /// Current file open for writing. `None`, if we are in between files, i.e. a file has been
    /// closed, due to the size threshold, but a new row group has not yet been received from the
    /// database.
    current_file: Option<CurrentFile>,
}

impl FileWriter {
    pub fn new(
        path: PathBuf,
        schema: Arc<Type>,
        options: ParquetWriterOptions,
        properties: Arc<WriterProperties>,
    ) -> Result<Self, Error> {
        let mut file_writer = Self {
            base_path: path,
            schema,
            properties,
            file_size: options.file_size,
            num_file: 0,
            suffix_length: options.suffix_length,
            current_file: None,
        };

        if !options.no_empty_file {
            file_writer.next_file()?;
        }

        Ok(file_writer)
    }

    fn next_file(&mut self) -> Result<(), Error> {
        let suffix = self.file_size.output_is_splitted().then_some((self.num_file + 1, self.suffix_length));
        let path = Self::current_path(&self.base_path, suffix)?;
        self.current_file = Some(CurrentFile::new(path, self.schema.clone(), self.properties.clone())?);
        self.num_file += 1;
        Ok(())
    }

    fn current_path(base_path: &Path, suffix: Option<(u32, usize)>) -> Result<PathBuf, Error> {
        let path = if let Some((num_file, suffix_length)) = suffix {
            path_with_suffix(base_path, num_file, suffix_length)?
        } else {
            base_path.to_owned()
        };
        Ok(path)
    }
}

impl ParquetOutput for FileWriter {
    fn write_row_group(
        &mut self,
        num_batch: u32,
        column_exporter: ColumnExporter,
    ) -> Result<(), Error> {
        // There is no file. Let us create one so we can write the row group.
        if self.current_file.is_none() {
            self.next_file()?
        }

        // Write next row group
        let file_size = self
            .current_file
            .as_mut()
            .unwrap()
            .write_row_group(column_exporter)?;

        if self
            .file_size
            .should_start_new_file(num_batch + 1, file_size)
        {
            self.current_file.take().unwrap().finalize()?;
        }

        Ok(())
    }

    fn close(self) -> Result<(), Error> {
        // An active file might, or might not exsist at this point, dependening on wether or not the
        // file splitting to due size thresholds coincides with the data source being consumed and
        // all data being read from it. If our data source ran out of data, just after we closed the
        // current file due to its size threshold it is `None`. In this case there is nothing to do
        // though.
        if let Some(open_file) = self.current_file {
            open_file.finalize()?;
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
    fn write_row_group(
        &mut self,
        _num_batch: u32,
        mut column_exporter: ColumnExporter,
    ) -> Result<(), Error> {
        let mut row_group_writer = self.writer.next_row_group()?;
        let mut col_index = 0;
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            column_exporter.export_nth_column(col_index, &mut column_writer)?;
            column_writer.close()?;
            col_index += 1;
        }
        row_group_writer.close()?;
        Ok(())
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
