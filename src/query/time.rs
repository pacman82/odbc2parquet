use std::borrow::Cow;

use anyhow::Error;
use log::warn;
use odbc_api::buffers::{AnySlice, BufferDescription, BufferKind};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::FetchStrategy;

/// Parse wallclock time with fractional seconds from text into time. E.g. 16:04:12.0000000
pub fn time_from_text(repetition: Repetition, precision: u8) -> Box<dyn FetchStrategy> {
    Box::new(TimeFromText::new(repetition, precision))
}

pub struct TimeFromText {
    repetition: Repetition,
    precision: u8,
}

impl TimeFromText {
    pub fn new(repetition: Repetition, precision: u8) -> Self {
        Self { repetition, precision }
    }
}

impl FetchStrategy for TimeFromText {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        let length = if self.precision == 0 {
            8
        } else {
            9 + self.precision as usize
        };
        BufferDescription {
            kind: BufferKind::Text {
                max_str_len: length,
            },
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_to_utf8(self.precision, parquet_buffer, column_writer, column_view)
    }
}

fn write_to_utf8(
    _precision: u8,
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnySlice::Text(view)) =
        (column_writer, column_reader)
    {
        pb.write_optional(
            cw,
            view.iter().map(|item| {
                item.map(utf8_bytes_to_byte_array)}
            ),
        )?;
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