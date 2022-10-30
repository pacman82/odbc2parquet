use anyhow::Error;
use odbc_api::{
    buffers::{AnySlice, BufferDescription, BufferKind, Item},
    Bit,
};
use parquet::{
    basic::{Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::BoolType,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

/// Could be the identical strategy on most platform. Yet Rust does not give any guarantees with
/// regard to the memory layout of a bool, so we do an explicit conversion from `Bit`.
pub struct Boolean {
    repetition: Repetition,
}

impl Boolean {
    pub fn new(repetetion: Repetition) -> Self {
        Self {
            repetition: repetetion,
        }
    }
}

impl ColumnFetchStrategy for Boolean {
    fn parquet_type(&self, name: &str) -> parquet::schema::types::Type {
        Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> odbc_api::buffers::BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Bit,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        let it = Bit::as_nullable_slice(column_view).unwrap();
        let column_writer = get_typed_column_writer_mut::<BoolType>(column_writer);
        parquet_buffer.write_optional(column_writer, it.map(|bit| bit.map(|bit| bit.as_bool())))?;
        Ok(())
    }
}
