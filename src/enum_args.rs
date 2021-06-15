use anyhow::{anyhow, bail, Error};
use parquet::basic::{Compression, Encoding};
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

/// Used to display a number of valid values for the compression argumet to the user.
pub const COMPRESSION_VARIANTS: &[&str] = &[
    "uncompressed",
    "gzip",
    "lz4",
    "lz0",
    "zstd",
    "snappy",
    "brotli",
];

/// Used to parse the compression from the command line argument.
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

pub fn encoding_from_str(source: &str) -> Result<Encoding, Error> {
    let encoding = match source {
        "plain" => Encoding::PLAIN,
        // Causes error 'NYI("Encoding BIT_PACKED is not supported")'
        //"bit-packed" => Encoding::BIT_PACKED,
        "delta-binary-packed" => Encoding::DELTA_BINARY_PACKED,
        "delta-byte-array" => Encoding::DELTA_BYTE_ARRAY,
        "delta-length-byte-array" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        "rle" => Encoding::RLE,
        // ommitted, not a valid fallback encoding
        //"rle-dictionary" => Encoding::RLE_DICTIONARY,
        _ => bail!("Sorry, I do not know a column encoding called '{}'.", source)
    };
    Ok(encoding)
}

pub fn column_encoding_from_str(source: &str) -> Result<(String, Encoding), Error> {
    let pos = source.rfind(':').ok_or(anyhow!("Column encoding must be parsed in format: 'COLUMN_NAME:ENCODING'"))?;
    let (name, encoding) = source.split_at(pos);
    Ok((name.to_owned(), encoding_from_str(&encoding[1..])?))
}