use anyhow::Error;
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::Timestamp,
};
use parquet::{
    basic::{LogicalType, Repetition},
    column::writer::ColumnWriter,
    data_type::{DataType, Int64Type},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::{column_strategy::ColumnStrategy, timestamp_precision::TimestampPrecision};

pub fn timestamp_without_tz(repetition: Repetition, precision: u8) -> Box<dyn ColumnStrategy> {
    Box::new(TimestampToI64 {
        repetition,
        precision: TimestampPrecision::new(precision),
    })
}

struct TimestampToI64 {
    repetition: Repetition,
    precision: TimestampPrecision,
}

impl ColumnStrategy for TimestampToI64 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, Int64Type::get_physical_type())
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: self.precision.as_time_unit(),
            }))
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Timestamp { nullable: true }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_timestamp_col(parquet_buffer, column_writer, column_view, self.precision)
    }
}

fn write_timestamp_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
    precision: TimestampPrecision,
) -> Result<(), Error> {
    let from = column_reader.as_nullable_slice::<Timestamp>().unwrap();
    let into = Int64Type::get_column_writer_mut(column_writer).unwrap();
    let from = from.map(|option| option.map(|ts| precision.timestamp_to_i64(ts)).transpose());
    pb.write_optional_falliable(into, from)?;
    Ok(())
}
