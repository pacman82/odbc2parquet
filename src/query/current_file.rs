use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use anyhow::Error;
use bytesize::ByteSize;
use parquet::{
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use tempfile::TempPath;

pub struct CurrentFile {
    pub writer: SerializedFileWriter<Box<dyn Write + Send>>,
    /// Path to the file currently being written to.
    path: TempPath,
    /// Keep track of curret file size so we can split it, should it get too large.
    pub file_size: ByteSize,
    /// Wether to persist a file with no rows
    no_empty_file: bool,
}

impl CurrentFile {
    pub fn new(
        path: PathBuf,
        schema: Arc<Type>,
        properties: Arc<WriterProperties>,
        no_empty_file: bool,
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
            no_empty_file,
        })
    }

    /// Writes metadata at the end and persists the file. Called if we do not want to continue
    /// writing batches into this file.
    pub fn finalize(self) -> Result<(), Error> {
        self.writer.close()?;
        // Do not persist empty files
        if !self.no_empty_file || self.file_size != ByteSize::b(0) {
            let _ = self.path.keep()?;
        }
        Ok(())
    }
}
