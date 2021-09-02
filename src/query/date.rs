use std::convert::TryInto;

use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, Item},
    sys::Date as OdbcDate,
};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::Int32Type,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

pub struct Date {
    repetition: Repetition,
}

impl Date {
    pub fn new(repetetion: Repetition) -> Self {
        Self {
            repetition: repetetion,
        }
    }
}

impl ColumnFetchStrategy for Date {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_repetition(self.repetition)
            .with_converted_type(ConvertedType::DATE)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Date,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        let it = OdbcDate::as_nullable_slice(column_view).unwrap();
        let column_writer = get_typed_column_writer_mut::<Int32Type>(column_writer);
        parquet_buffer.write_optional(column_writer, it.map(|date| date.map(days_since_epoch)))?;
        Ok(())
    }
}

/// Transform date to days since unix epoch as i32
fn days_since_epoch(date: &OdbcDate) -> i32 {
    let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
    let date = NaiveDate::from_ymd(date.year as i32, date.month as u32, date.day as u32);
    let duration = date.signed_duration_since(unix_epoch);
    duration.num_days().try_into().unwrap()
}
