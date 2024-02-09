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
    pub path: TempPath,
    /// Keep track of curret file size so we can split it, should it get too large.
    pub file_size: ByteSize,
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
}
