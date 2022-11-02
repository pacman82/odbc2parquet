use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::{
    buffers::{AnySlice, BufferDescription, BufferKind},
    sys::Timestamp,
};
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit},
    column::writer::ColumnWriter,
    data_type::{DataType, Int64Type},
    format::{MicroSeconds, MilliSeconds},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

pub fn timestamp_without_tz(repetition: Repetition, precision: u8) -> Box<dyn ColumnFetchStrategy> {
    Box::new(TimestampToI64 {
        repetition,
        precision,
    })
}

struct TimestampToI64 {
    repetition: Repetition,
    precision: u8,
}

impl ColumnFetchStrategy for TimestampToI64 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, Int64Type::get_physical_type())
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: precision_to_time_unit(self.precision),
            }))
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
    let from = from.map(|option| option.map(|ts| i64::from_timestamp_and_precision(ts, precision)));
    pb.write_optional(into, from)?;
    Ok(())
}

trait FromTimestampAndPrecision {
    fn from_timestamp_and_precision(ts: &Timestamp, precision: u8) -> Self;
}

impl FromTimestampAndPrecision for i64 {
    fn from_timestamp_and_precision(ts: &Timestamp, precision: u8) -> Self {
        timestamp_to_i64(ts, precision)
    }
}

impl FromTimestampAndPrecision for i32 {
    fn from_timestamp_and_precision(ts: &Timestamp, precision: u8) -> Self {
        timestamp_to_i64(ts, precision).try_into().unwrap()
    }
}

/// Convert an ODBC timestamp struct into nanoseconds.
fn timestamp_to_i64(ts: &Timestamp, precision: u8) -> i64 {
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
