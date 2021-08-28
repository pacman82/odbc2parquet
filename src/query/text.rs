use std::borrow::Cow;

use anyhow::Error;
use log::warn;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

pub struct Utf16ToUtf8 {
    repetition: Repetition,
    length: usize,
}

impl Utf16ToUtf8 {
    pub fn new(repetition: Repetition, length: usize) -> Self {
        Self { repetition, length }
    }
}

impl ColumnFetchStrategy for Utf16ToUtf8 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: BufferKind::WText {
                // One UTF-16 code point may consist of up to two bytes.
                max_str_len: self.length * 2,
            },
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        write_utf16_to_utf8(parquet_buffer, column_writer, column_view)
    }
}

fn write_utf16_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::WText(it)) =
        (column_writer, column_reader)
    {
        pb.write_optional(
            cw,
            it.map(|item| {
                item.map(|ustr| {
                    let byte_array: ByteArray = ustr
                        .to_string()
                        .expect("Data source must return valid UTF16 in wide character buffer")
                        .into_bytes()
                        .into();
                    byte_array
                })
            }),
        )?;
    } else {
        panic!(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues."
        )
    }
    Ok(())
}

pub struct Utf8 {
    repetition: Repetition,
    // Maximum string length in bytes
    length: usize,
}

impl Utf8 {
    pub fn with_bytes_length(repetition: Repetition, length: usize) -> Self {
        Self { repetition, length }
    }

    pub fn with_char_length(repetition: Repetition, length: usize) -> Self {
        Self {
            repetition,
            // One UTF-8 code point may consist of up to four bytes.
            length: length * 4,
        }
    }
}

impl ColumnFetchStrategy for Utf8 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: BufferKind::Text {
                // One UTF-16 code point may consist of up to two bytes.
                max_str_len: self.length * 4,
            },
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        write_to_utf8(parquet_buffer, column_writer, column_view)
    }
}

fn write_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::Text(it)) =
        (column_writer, column_reader)
    {
        pb.write_optional(cw, it.map(|item| item.map(utf8_bytes_to_byte_array)))?;
    } else {
        panic!(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues."
        )
    }
    Ok(())
}

fn utf8_bytes_to_byte_array(bytes: &[u8]) -> ByteArray {
    // Allocate string into a ByteArray and make sure it is all UTF-8 characters
    let utf8_str = String::from_utf8_lossy(bytes);
    // We need to allocate the string anyway to create a ByteArray (yikes!), yet if it already
    // happened after the to_string_lossy method, it implies we had to use a replacement
    // character!
    if matches!(utf8_str, Cow::Owned(_)) {
        warn!(
            "Non UTF-8 characters found in string. Try to execute odbc2parquet in a shell with \
        UTF-8 locale or try specifying `--encoding Utf16` on the command line. Value: {}",
            utf8_str
        );
    }
    utf8_str.into_owned().into_bytes().into()
}
