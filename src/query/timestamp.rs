use anyhow::Error;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::{DataType, Int64Type},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

pub struct Timestamp {
    repetition: Repetition,
    precision: i16,
}

impl Timestamp {
    pub fn new(repetition: Repetition, precision: i16) -> Self {
        Self {
            repetition,
            precision,
        }
    }
}

impl ColumnFetchStrategy for Timestamp {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_converted_type(if self.precision <= 3 {
                ConvertedType::TIMESTAMP_MILLIS
            } else {
                ConvertedType::TIMESTAMP_MICROS
            })
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: BufferKind::Timestamp,
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        write_timestamp_col(parquet_buffer, column_writer, column_view, self.precision)
    }
}

fn write_timestamp_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    precision: i16,
) -> Result<(), Error> {
    let column_writer = Int64Type::get_column_writer_mut(column_writer).unwrap();
    if let AnyColumnView::NullableTimestamp(it) = column_reader {
        pb.write_timestamp(column_writer, it, precision)?;
    } else {
        panic!(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues."
        )
    }
    Ok(())
}
