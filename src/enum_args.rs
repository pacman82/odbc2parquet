use anyhow::{bail, Error};
use parquet::basic::Compression;
use structopt::clap::arg_enum;

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum EncodingArgument {
        System,
        Utf16,
        Auto,
    }
}

impl EncodingArgument {
    /// Translate the command line option to a boolean indicating whether or not wide character
    /// buffers, should be bound.
    pub fn use_utf16(self) -> bool {
        match self {
            EncodingArgument::System => false,
            EncodingArgument::Utf16 => true,
            // Most windows systems do not utilize UTF-8 as their default encoding, yet.
            #[cfg(target_os = "windows")]
            EncodingArgument::Auto => true,
            // UTF-8 is the default on Linux and OS-X.
            #[cfg(not(target_os = "windows"))]
            EncodingArgument::Auto => false,
        }
    }
}

pub const COMPRESSION_VARIANTS: &[&str] = &[
    "uncompressed",
    "gzip",
    "lz4",
    "lz0",
    "zstd",
    "snappy",
    "brotli",
];

pub fn compression_from_str(source: &str) -> Result<Compression, Error> {
    let compression = match source {
        "uncompressed" => Compression::UNCOMPRESSED,
        "gzip" => Compression::GZIP,
        "lz4" => Compression::LZ4,
        "lz0" => Compression::LZO,
        "zstd" => Compression::ZSTD,
        "snappy" => Compression::SNAPPY,
        "brotli" => Compression::BROTLI,
        _ => bail!("Sorry, I do not know this compression identifier."),
    };
    Ok(compression)
}
