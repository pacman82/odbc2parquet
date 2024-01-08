use anyhow::Error;
use chrono::{DateTime, Utc};
use odbc_api::buffers::{AnySlice, BufferDesc};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::Int64Type,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::{column_strategy::ColumnStrategy, timestamp_precision::TimestampPrecision};

pub fn timestamp_tz(precision: u8, repetition: Repetition) -> Result<Box<TimestampTz>, Error> {
    Ok(Box::new(TimestampTz::with_bytes_length(
        repetition, precision,
    )))
}

pub struct TimestampTz {
    repetition: Repetition,
    // We store digit precision, rather than TimestampPrecision, in order to be able to adequatly
    // calculate ODBC text buffer length.
    precision: u8,
}

impl TimestampTz {
    pub fn with_bytes_length(repetition: Repetition, precision: u8) -> Self {
        Self {
            repetition,
            precision,
        }
    }
}

impl ColumnStrategy for TimestampTz {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimestampPrecision::new(self.precision).as_time_unit(),
            }))
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        // Text representation looks like e.g. 2022-09-07 16:04:12 +02:00
        // Text representation looks like e.g. 2022-09-07 16:04:12.123 +02:00

        let max_str_len = 26
            + if self.precision == 0 {
                0
            } else {
                // Radix character `.` and precision.
                1 + self.precision as usize
            };
        BufferDesc::Text { max_str_len }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_timestamp_tz(parquet_buffer, column_writer, column_view, self.precision)
    }
}

fn write_timestamp_tz(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
    precision: u8,
) -> Result<(), Error> {
    let view = column_reader.as_text_view().expect(
        "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
        https://github.com/pacman82/odbc2parquet/issues.",
    );
    let cw = get_typed_column_writer_mut::<Int64Type>(column_writer);
    pb.write_optional_falliable(
        cw,
        view.iter()
            .map(|item| item.map(|text| to_utc_epoch(text, precision)).transpose()),
    )?;
    Ok(())
}

fn to_utc_epoch(bytes: &[u8], precision: u8) -> Result<i64, Error> {
    // Text representation looks like e.g. 2022-09-07 16:04:12 +02:00
    let utf8 = String::from_utf8_lossy(bytes);

    // Parse to datetime
    let date_time = DateTime::parse_from_str(&utf8, "%Y-%m-%d %H:%M:%S%.9f %:z")?;
    // let utc = date_time.naive_utc();
    let utc = date_time.with_timezone(&Utc);
    let integer = TimestampPrecision::new(precision).datetime_to_i64(&utc)?;
    Ok(integer)
}
