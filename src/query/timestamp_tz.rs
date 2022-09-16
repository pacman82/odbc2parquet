use std::borrow::Cow;

use anyhow::Error;
use chrono::{DateTime, Utc};
use log::warn;
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::strategy::ColumnFetchStrategy;

pub fn timestamp_tz(
    precision: usize,
    repetition: Repetition,
) -> Result<Box<TimestampTz>, Error> {
    Ok(Box::new(TimestampTz::with_bytes_length(
        repetition,
        precision,
    )))
}

pub struct TimestampTz {
    repetition: Repetition,
    // Precision
    precision: usize,
}

impl TimestampTz {
    pub fn with_bytes_length(repetition: Repetition, precision: usize) -> Self {
        Self { repetition, precision }
    }
}

impl ColumnFetchStrategy for TimestampTz {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        // Text representation looks like e.g. 2022-09-07 16:04:12 +02:00
        // Text representation looks like e.g. 2022-09-07 16:04:12.123 +02:00

        let max_str_len = 26 + if self.precision == 0 {
            0
        } else {
            // Radix character `.` and precision.
            1 + self.precision
        };
        BufferDescription {
            kind: BufferKind::Text {
                max_str_len,
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
        write_to_utf8(parquet_buffer, column_writer, column_view)
    }
}

fn write_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::Text(view)) =
        (column_writer, column_reader)
    {
        pb.write_optional(
            cw,
            view.iter().map(|item| item.map(to_utc_text_representation)),
        )?;
    } else {
        panic!(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues."
        )
    }
    Ok(())
}

fn to_utc_text_representation(bytes: &[u8]) -> ByteArray {
    // Text representation looks like e.g. 2022-09-07 16:04:12 +02:00
    let utf8 = String::from_utf8_lossy(bytes);

    // Parse to datetime
    let date_time = DateTime::parse_from_str(&utf8, "%Y-%m-%d %H:%M:%S%.9f %:z")
        .expect("Database must return timestamp in expecetd format.");
    // let utc = date_time.naive_utc();
    let utc = date_time.with_timezone(&Utc);
    let epoch = utc.timestamp_nanos();

    // We need to allocate the string anyway to create a ByteArray (yikes!), yet if it already
    // happened after the to_string_lossy method, it implies we had to use a replacement
    // character!
    if matches!(utf8, Cow::Owned(_)) {
        warn!(
            "Non UTF-8 characters found in string. Try to execute odbc2parquet in a shell with \
            UTF-8 locale or try specifying `--encoding Utf16` on the command line. Value: {}",
            utf8
        );
    }
    utf8.into_owned().into_bytes().into()
}
