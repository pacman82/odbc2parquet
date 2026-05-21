use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::{
    buffers::{AnyColumnBufferSlice, BufferDesc},
    sys::Date as OdbcDate,
};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::Int32Type,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::column_strategy::ColumnStrategy;

pub struct Date {
    repetition: Repetition,
}

impl Date {
    pub fn new(repetition: Repetition) -> Self {
        Self { repetition }
    }
}

impl ColumnStrategy for Date {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_repetition(self.repetition)
            .with_logical_type(Some(LogicalType::Date))
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Date { nullable: true }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnBufferSlice,
    ) -> Result<(), Error> {
        let it = column_view.as_nullable_slice().unwrap();
        let column_writer = get_typed_column_writer_mut::<Int32Type>(column_writer);
        parquet_buffer.write_optional(column_writer, it.map(|date| date.map(days_since_epoch)))?;
        Ok(())
    }
}

/// Transform date to days since unix epoch as i32
fn days_since_epoch(date: &OdbcDate) -> i32 {
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date =
        NaiveDate::from_ymd_opt(date.year as i32, date.month as u32, date.day as u32).unwrap();
    let duration = date.signed_duration_since(unix_epoch);
    duration.num_days().try_into().unwrap()
}
