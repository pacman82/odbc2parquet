use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::{
    buffers::{AnySlice, BufferDescription, BufferKind},
    sys::Timestamp,
};
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType, ConvertedType},
    column::writer::ColumnWriter,
    data_type::{DataType, Int64Type},
    format::{MicroSeconds, MilliSeconds},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

pub struct TimestampToInt {
    repetition: Repetition,
    precision: u8,
}

impl TimestampToInt {
    pub fn new(repetition: Repetition, precision: u8) -> Self {
        Self {
            repetition,
            precision,
        }
    }
}

impl ColumnFetchStrategy for TimestampToInt {
    fn parquet_type(&self, name: &str) -> Type {
        let ct = if self.precision <= 3 {
            ConvertedType::TIMESTAMP_MILLIS
        } else {
            ConvertedType::TIMESTAMP_MICROS
        };

        Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: precision_to_time_unit(self.precision),
            }))
            .with_converted_type(ct)
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
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_timestamp_col(parquet_buffer, column_writer, column_view, self.precision)
    }
}

pub fn precision_to_time_unit(precision: u8) -> TimeUnit {
    if precision <= 3 {
        TimeUnit::MILLIS(MilliSeconds {})
    } else {
        TimeUnit::MICROS(MicroSeconds {})
    }
}

fn write_timestamp_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
    precision: u8,
) -> Result<(), Error> {
    let from = column_reader.as_nullable_slice::<Timestamp>().unwrap();
    let into = Int64Type::get_column_writer_mut(column_writer).unwrap();
    let from = from.map(|option| option.map(|ts| timestamp_to_int(ts, precision)));
    pb.write_optional(into, from)?;
    Ok(())
}

/// Convert an ODBC timestamp struct into nanoseconds.
fn timestamp_to_int(ts: &Timestamp, precision: u8) -> i64 {
    let datetime = NaiveDate::from_ymd(ts.year as i32, ts.month as u32, ts.day as u32)
        .and_hms_nano(
            ts.hour as u32,
            ts.minute as u32,
            ts.second as u32,
            ts.fraction as u32,
        );
    if precision <= 3 {
        datetime.timestamp_millis()
    } else {
        datetime.timestamp_micros()
    }
}
