use std::convert::TryInto;

use anyhow::Error;
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
    DataType,
};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::{DataType as _, FixedLenByteArrayType},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

/// Strategy for fetching decimal values which can not be represented as either 32Bit or 64Bit
pub struct Decimal {
    repetition: Repetition,
    scale: i32,
    precision: usize,
    length_in_bytes: usize,
}

impl Decimal {
    pub fn new(repetition: Repetition, scale: i32, precision: usize) -> Self {
        // Length of the two's complement.
        let num_binary_digits = precision as f64 * 10f64.log2();
        // Plus one bit for the sign (+/-)
        let length_in_bits = num_binary_digits + 1.0;
        let length_in_bytes = (length_in_bits / 8.0).ceil() as usize;

        Self {
            repetition,
            scale,
            precision,
            length_in_bytes,
        }
    }
}

impl ColumnFetchStrategy for Decimal {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(self.length_in_bytes.try_into().unwrap())
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(self.precision.try_into().unwrap())
            .with_scale(self.scale)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> odbc_api::buffers::BufferDescription {
        // Precision + 2. (One byte for the radix character and another for the sign)
        let max_str_len = DataType::Decimal {
            precision: self.precision,
            scale: self.scale.try_into().unwrap(),
        }
        .display_size()
        .unwrap();
        BufferDescription {
            kind: BufferKind::Text { max_str_len },
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        write_decimal_col(
            parquet_buffer,
            column_writer,
            column_view,
            self.length_in_bytes,
            self.precision,
        )
    }
}

fn write_decimal_col(
    parquet_buffer: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    length_in_bytes: usize,
    precision: usize,
) -> Result<(), Error> {
    let column_writer = FixedLenByteArrayType::get_column_writer_mut(column_writer).unwrap();
    if let AnyColumnView::Text(it) = column_reader {
        parquet_buffer.write_decimal(column_writer, it, length_in_bytes, precision)?;
    } else {
        panic!(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues."
        )
    }
    Ok(())
}
