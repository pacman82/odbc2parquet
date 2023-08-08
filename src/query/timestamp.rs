use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::Timestamp,
};
use parquet::{
    basic::{LogicalType, Repetition, TimeUnit},
    column::writer::ColumnWriter,
    data_type::{DataType, Int64Type},
    format::{MicroSeconds, MilliSeconds, NanoSeconds},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::FetchStrategy;

pub fn timestamp_without_tz(repetition: Repetition, precision: u8) -> Box<dyn FetchStrategy> {
    Box::new(TimestampToI64 {
        repetition,
        precision: TimestampPrecision::new(precision),
    })
}

struct TimestampToI64 {
    repetition: Repetition,
    precision: TimestampPrecision,
}

impl FetchStrategy for TimestampToI64 {
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

/// Relational types communicate the precision of timestamps in number of fraction digits, while
/// parquet uses time units (milli, micro, nano). This enumartion stores the the decision which time
/// unit to use and how to map it to parquet representations (both units and values)
#[derive(Clone, Copy)]
pub enum TimestampPrecision {
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl TimestampPrecision {
    pub fn new(precision: u8) -> Self {
        match precision {
            0..=3 => TimestampPrecision::Milliseconds,
            4..=6 => TimestampPrecision::Microseconds,
            7.. => TimestampPrecision::Nanoseconds,
        }
    }

    pub fn as_time_unit(self) -> TimeUnit {
        match self {
            TimestampPrecision::Milliseconds => TimeUnit::MILLIS(MilliSeconds {}),
            TimestampPrecision::Microseconds => TimeUnit::MICROS(MicroSeconds {}),
            TimestampPrecision::Nanoseconds => TimeUnit::NANOS(NanoSeconds {}),
        }
    }

    /// Convert an ODBC timestamp struct into nano, milli or microseconds based on precision.
    pub fn timestamp_to_i64(self, ts: &Timestamp) -> i64 {
        let datetime = NaiveDate::from_ymd_opt(ts.year as i32, ts.month as u32, ts.day as u32)
            .unwrap()
            .and_hms_nano_opt(
                ts.hour as u32,
                ts.minute as u32,
                ts.second as u32,
                ts.fraction,
            )
            .unwrap();

        match self {
            TimestampPrecision::Milliseconds => datetime.timestamp_millis(),
            TimestampPrecision::Microseconds => datetime.timestamp_micros(),
            TimestampPrecision::Nanoseconds => datetime.timestamp_nanos(),
        }
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
    precision: TimestampPrecision,
) -> Result<(), Error> {
    let from = column_reader.as_nullable_slice::<Timestamp>().unwrap();
    let into = Int64Type::get_column_writer_mut(column_writer).unwrap();
    let from = from.map(|option| option.map(|ts| precision.timestamp_to_i64(ts)));
    pb.write_optional(into, from)?;
    Ok(())
}
