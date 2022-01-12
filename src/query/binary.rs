use std::marker::PhantomData;

use anyhow::Error;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use parquet::{
    basic::{Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::{ByteArray, DataType},
    schema::types::Type,
};

use crate::parquet_buffer::{BufferedDataType, ParquetBuffer};

use super::strategy::ColumnFetchStrategy;

pub struct Binary<Pdt> {
    repetition: Repetition,
    length: usize,
    _phantom: PhantomData<Pdt>,
}

impl<Pdt> Binary<Pdt> {
    pub fn new(repetition: Repetition, length: usize) -> Self {
        Self {
            repetition,
            length,
            _phantom: PhantomData,
        }
    }
}

impl<Pdt> ColumnFetchStrategy for Binary<Pdt>
where
    Pdt: DataType,
    Pdt::T: BufferedDataType + From<ByteArray>,
{
    fn parquet_type(&self, name: &str) -> Type {
        let physical_type = Pdt::get_physical_type();

        match physical_type {
            PhysicalType::BYTE_ARRAY => Type::primitive_type_builder(name, physical_type)
                .with_repetition(self.repetition)
                .build()
                .unwrap(),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => Type::primitive_type_builder(name, physical_type)
                .with_repetition(self.repetition)
                .with_length(self.length.try_into().unwrap())
                .build()
                .unwrap(),
            _ => {
                panic!("Only ByteArray and FixedLenByteArray are allowed to instantiate Binary<_>")
            }
        }
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: BufferKind::Binary {
                length: self.length,
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
        let cw = get_typed_column_writer_mut::<Pdt>(column_writer);
        if let AnyColumnView::Binary(it) = column_view {
            parquet_buffer.write_optional(
                cw,
                it.map(|maybe_bytes| {
                    maybe_bytes.map(|bytes| {
                        let byte_array: ByteArray = bytes.to_owned().into();
                        // Transforms ByteArray into FixedLenByteArray or does nothing depending `Pdt`.
                        let out: Pdt::T = byte_array.into();
                        out
                    })
                }),
            )?
        } else {
            panic!(
                "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
                https://github.com/pacman82/odbc2parquet/issues."
            )
        }
        Ok(())
    }
}
