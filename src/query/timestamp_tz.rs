use anyhow::Error;
use chrono::{DateTime, Utc};
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::Int64Type,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::{strategy::ColumnFetchStrategy, timestamp::precision_to_time_unit};

pub fn timestamp_tz(precision: u8, repetition: Repetition) -> Result<Box<TimestampTz>, Error> {
    Ok(Box::new(TimestampTz::with_bytes_length(
        repetition, precision,
    )))
}

pub struct TimestampTz {
    repetition: Repetition,
    // Precision
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

impl ColumnFetchStrategy for TimestampTz {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: precision_to_time_unit(self.precision),
            }))
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        // Text representation looks like e.g. 2022-09-07 16:04:12 +02:00
        // Text representation looks like e.g. 2022-09-07 16:04:12.123 +02:00

        let max_str_len = 26
            + if self.precision == 0 {
                0
            } else {
                // Radix character `.` and precision.
                1 + self.precision as usize
            };
        BufferDescription {
            kind: BufferKind::Text { max_str_len },
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        write_timestamp_tz(parquet_buffer, column_writer, column_view, self.precision)
    }
}

fn write_timestamp_tz(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    precision: u8,
) -> Result<(), Error> {
    let view = column_reader.as_text_view().expect(
        "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
        https://github.com/pacman82/odbc2parquet/issues.",
    );
    let cw = get_typed_column_writer_mut::<Int64Type>(column_writer);
    pb.write_optional(
        cw,
        view.iter()
            .map(|item| item.map(|text| to_utc_epoch(text, precision))),
    )?;
    Ok(())
}

fn to_utc_epoch(bytes: &[u8], precision: u8) -> i64 {
    // Text representation looks like e.g. 2022-09-07 16:04:12 +02:00
    let utf8 = String::from_utf8_lossy(bytes);

    // Parse to datetime
    let date_time = DateTime::parse_from_str(&utf8, "%Y-%m-%d %H:%M:%S%.9f %:z")
        .expect("Database must return timestamp in expecetd format.");
    // let utc = date_time.naive_utc();
    let utc = date_time.with_timezone(&Utc);
    if precision <= 3 {
        utc.timestamp_millis()
    } else {
        utc.timestamp_micros()
    }
}
