use anyhow::Error;
use atoi::FromRadix10Signed;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use parquet::{
    basic::{ConvertedType, Repetition},
    column::writer::ColumnWriter,
    data_type::{DataType as _, Int64Type},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

/// Query a column as text and write it as 64 Bit integer.
pub struct Int64FromText {
    /// Maximum number of expected digits of the integer
    num_digits: usize,
    /// `true` if NULL is allowed, `false` otherwise
    is_optional: bool,
}

impl Int64FromText {
    pub fn new(num_digits: usize, is_optional: bool) -> Self {
        Self {
            num_digits,
            is_optional,
        }
    }
}

impl ColumnFetchStrategy for Int64FromText {
    fn parquet_type(&self, name: &str) -> Type {
        let repetition = if self.is_optional {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        };
        let physical_type = Int64Type::get_physical_type();

        Type::primitive_type_builder(name, physical_type)
            .with_repetition(repetition)
            .with_converted_type(ConvertedType::NONE)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: self.is_optional,
            kind: BufferKind::Text {
                // +1 not for terminating zero, but for the sign charactor like `-` or `+`.
                max_str_len: self.num_digits + 1,
            },
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        let column_writer = Int64Type::get_column_writer_mut(column_writer).unwrap();
        if let AnyColumnView::Text(view) = column_view {
            parquet_buffer.write_optional(
                column_writer,
                view.iter().map(|value| value.map(|text| i64::from_radix_10_signed(text).0)),
            )?;
        } else {
            panic!(
                "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
                https://github.com/pacman82/odbc2parquet/issues."
            )
        }
        Ok(())
    }
}
