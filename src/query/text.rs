use std::{borrow::Cow, ffi::CStr};

use anyhow::{anyhow, Error};
use log::warn;
use odbc_api::buffers::{AnySlice, BufferDesc};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::{ByteArray, ByteArrayType},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::column_strategy::ColumnStrategy;

pub fn text_strategy(
    use_utf16: bool,
    repetition: Repetition,
    length: usize,
    indicators_returned_from_bulk_fetch_are_memory_garbage: bool,
) -> Box<dyn ColumnStrategy> {
    if use_utf16 {
        Box::new(Utf16ToUtf8::new(repetition, length))
    } else {
        // If the indicators are garbage we need to choose a strategy which ignores them. So far
        // this only happend with the linux drivers of IBM DB2 so we did not implement the
        // workaround for UTF-16, which is dominant on windows.
        if indicators_returned_from_bulk_fetch_are_memory_garbage {
            Box::new(Utf8IgnoreIndicators::with_bytes_length(length))
        } else {
            Box::new(Utf8::with_bytes_length(repetition, length))
        }
    }
}

struct Utf16ToUtf8 {
    repetition: Repetition,
    /// Length of the column elements in `u16` (as opposed to code points).
    length: usize,
}

impl Utf16ToUtf8 {
    pub fn new(repetition: Repetition, length: usize) -> Self {
        Self { repetition, length }
    }
}

impl ColumnStrategy for Utf16ToUtf8 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::WText {
            max_str_len: self.length,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_utf16_to_utf8(parquet_buffer, column_writer, column_view)
    }
}

fn write_utf16_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
) -> Result<(), Error> {
    let cw = get_typed_column_writer_mut::<ByteArrayType>(column_writer);
    let view = column_reader.as_w_text_view().unwrap();

    pb.write_optional_falliable(
        cw,
        view.iter().map(|item| {
            if let Some(ustr) = item {
                let byte_array: ByteArray = ustr
                    .to_string()
                    .map_err(|_utf_16_error| {
                        anyhow!("Data source must return valid UTF16 in wide character buffer")
                    })?
                    .into_bytes()
                    .into();
                Ok(Some(byte_array))
            } else {
                Ok(None)
            }
        }),
    )?;
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
}

impl ColumnStrategy for Utf8 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            max_str_len: self.length,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_to_utf8(parquet_buffer, column_writer, column_view)
    }
}

/// Alternative fetch strategy for UTF-8. Ignoring indicators. Cannot distinguish between empty
/// strings and NULL. Always chooses NULL as target representation.
pub struct Utf8IgnoreIndicators {
    // Maximum string length in bytes
    length: usize,
}

impl Utf8IgnoreIndicators {
    pub fn with_bytes_length(length: usize) -> Self {
        Self { length }
    }
}

impl ColumnStrategy for Utf8IgnoreIndicators {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            max_str_len: self.length,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_to_utf8_ignoring_indicators(parquet_buffer, column_writer, column_view, self.length)
    }
}

fn write_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
) -> Result<(), Error> {
    let cw = get_typed_column_writer_mut::<ByteArrayType>(column_writer);
    let view = column_reader.as_text_view().unwrap();

    pb.write_optional(
        cw,
        view.iter().map(|item| item.map(utf8_bytes_to_byte_array)),
    )?;

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

fn write_to_utf8_ignoring_indicators(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
    length: usize,
) -> Result<(), Error> {
    let cw = get_typed_column_writer_mut::<ByteArrayType>(column_writer);
    let view = column_reader.as_text_view().unwrap();

    // We cannot use `view.iter()` as this implementation relies on the indices being correct.
    // This workaround assumes that these are memory garbage though due to a bug in the driver. So
    // we start from the raw value buffer and use the terminating zeroes.
    let length_including_terminating_zero = length + 1;
    let utf8_bytes = view
        .raw_value_buffer()
        .chunks_exact(length_including_terminating_zero)
        .map(|bytes| {
            let bytes = CStr::from_bytes_until_nul(bytes)
                .expect("ODBC driver must return strings terminated by zero")
                .to_bytes();
            if bytes.is_empty() {
                None // Map empty strings to NULL
            } else {
                Some(bytes)
            }
        });

    pb.write_optional(
        cw,
        utf8_bytes.map(|item| item.map(utf8_bytes_to_byte_array)),
    )?;

    Ok(())
}
