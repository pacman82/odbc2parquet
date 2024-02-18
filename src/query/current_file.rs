use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use anyhow::Error;
use bytesize::ByteSize;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use tempfile::TempPath;

use super::table_strategy::ColumnExporter;

pub struct CurrentFile {
    writer: SerializedFileWriter<Box<dyn Write + Send>>,
    /// Path to the file currently being written to.
    path: TempPath,
    /// Keep track of curret file size so we can split it, should it get too large.
    file_size: ByteSize,
}

impl CurrentFile {
    pub fn new(
        path: PathBuf,
        schema: Arc<Type>,
        properties: Arc<WriterProperties>,
    ) -> Result<CurrentFile, Error> {
        let output: Box<dyn Write + Send> = Box::new(File::create(&path).map_err(|io_err| {
            Error::from(io_err).context(format!(
                "Could not create output file '{}'",
                path.to_string_lossy()
            ))
        })?);
        let path = TempPath::from_path(path);
        let writer = SerializedFileWriter::new(output, schema.clone(), properties.clone())?;

        Ok(Self {
            writer,
            path,
            file_size: ByteSize::b(0),
        })
    }

    pub fn write_row_group(
        &mut self,
        mut column_exporter: ColumnExporter,
    ) -> Result<ByteSize, Error> {
        let mut col_index = 0;
        let mut row_group_writer = self.writer.next_row_group()?;
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            column_exporter.export_nth_column(col_index, &mut column_writer)?;
            column_writer.close()?;
            col_index += 1;
        }
        let metadata = row_group_writer.close()?;
        // Of course writing a row group increases file size. We keep track of it here, so we can
        // split on file size if we go over a threshold.
        self.file_size += ByteSize::b(metadata.compressed_size().try_into().unwrap());
        Ok(self.file_size)
    }

    /// Writes metadata at the end and persists the file. Called if we do not want to continue
    /// writing batches into this file.
    pub fn finalize(self) -> Result<(), Error> {
        self.writer.close()?;
        // Do not persist empty files
        let _ = self.path.keep()?;
        Ok(())
    }
}
