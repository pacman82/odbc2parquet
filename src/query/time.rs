use std::ops::{Add, Div, Mul};

use anyhow::Error;
use atoi::FromRadix10;
use chrono::{NaiveTime, Timelike};
use odbc_api::buffers::{AnySlice, BufferDesc};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::{DataType, Int32Type, Int64Type},
    format::{MicroSeconds, MilliSeconds, NanoSeconds, TimeUnit},
    schema::types::Type,
};

use crate::parquet_buffer::{BufferedDataType, ParquetBuffer};

use super::column_strategy::ColumnStrategy;

/// Parse wallclock time with fractional seconds from text into time. E.g. 16:04:12.0000000
pub fn time_from_text(repetition: Repetition, precision: u8) -> Box<dyn ColumnStrategy> {
    Box::new(TimeFromText::new(repetition, precision))
}

struct TimeFromText {
    repetition: Repetition,
    precision: u8,
}

impl TimeFromText {
    pub fn new(repetition: Repetition, precision: u8) -> Self {
        Self {
            repetition,
            precision,
        }
    }
}

impl ColumnStrategy for TimeFromText {
    fn parquet_type(&self, name: &str) -> Type {
        let (unit, pt) = match self.precision {
            0..=3 => (TimeUnit::MILLIS(MilliSeconds {}), PhysicalType::INT32),
            4..=6 => (TimeUnit::MICROS(MicroSeconds {}), PhysicalType::INT64),
            _ => (TimeUnit::NANOS(NanoSeconds {}), PhysicalType::INT64),
        };

        Type::primitive_type_builder(name, pt)
            .with_logical_type(Some(LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit,
            }))
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        let length = if self.precision == 0 {
            8
        } else {
            9 + self.precision as usize
        };
        BufferDesc::Text {
            max_str_len: length,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        match self.precision {
            0..=3 => write_time_ms(parquet_buffer, column_writer, column_view),
            4..=6 => write_time_us(parquet_buffer, column_writer, column_view),
            _ => write_time_ns(parquet_buffer, column_writer, column_view),
        }
    }
}

fn write_time_ns(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
) -> Result<(), Error> {
    write_time_with::<Int64Type>(pb, column_writer, column_reader, 1_000_000_000, 1)
}

fn write_time_us(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
) -> Result<(), Error> {
    write_time_with::<Int64Type>(pb, column_writer, column_reader, 1_000_000, 1_000)
}

fn write_time_ms(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
) -> Result<(), Error> {
    write_time_with::<Int32Type>(pb, column_writer, column_reader, 1_000, 1_000_000)
}

fn write_time_with<Pdt>(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
    s_factor: Pdt::T,
    ns_divisor: Pdt::T,
) -> Result<(), Error>
where
    Pdt: DataType,
    Pdt::T: BufferedDataType
        + TryFrom<u32>
        + Mul<Output = Pdt::T>
        + Div<Output = Pdt::T>
        + Add<Output = Pdt::T>
        + Copy,
    <Pdt::T as TryFrom<u32>>::Error: std::fmt::Debug,
{
    let from = column_reader.as_text_view().unwrap();
    let into = Pdt::get_column_writer_mut(column_writer).unwrap();
    pb.write_optional(
        into,
        from.iter().map(|field| {
            field.map(|text| {
                let time = parse_time(text);
                let seconds = time.num_seconds_from_midnight();
                let nanoseconds = time.nanosecond();
                let seconds: Pdt::T = seconds.try_into().unwrap();
                let nanoseconds: Pdt::T = nanoseconds.try_into().unwrap();
                seconds * s_factor + nanoseconds as Pdt::T / ns_divisor
            })
        }),
    )?;
    Ok(())
}

/// Parse timestamp from representation HH:MM:SS[.FFF]
fn parse_time(bytes: &[u8]) -> NaiveTime {
    // From radix ten also returns the number of bytes extracted. We don't care. Should always
    // be two, for hour, min and sec.
    let (hour, _) = u32::from_radix_10(&bytes[0..2]);
    let (min, _) = u32::from_radix_10(&bytes[3..5]);
    let (sec, _) = u32::from_radix_10(&bytes[6..8]);
    // If a fractional part is present, we parse it.
    let nano = if bytes.len() > 9 {
        let (fraction, precision) = u32::from_radix_10(&bytes[9..]);
        match precision {
            0..=8 => {
                // Pad value with `0` to represent nanoseconds
                fraction * 10_u32.pow(9 - precision as u32)
            }
            9 => fraction,
            _ => {
                // More than nanoseconds precision. Let's just remove the additional digits at the
                // end.
                fraction / 10_u32.pow(precision as u32 - 9)
            }
        }
    } else {
        0
    };
    NaiveTime::from_hms_nano_opt(hour, min, sec, nano).unwrap()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveTime;

    use crate::query::time::parse_time;

    #[test]
    fn parse_timestamps() {
        assert_eq!(
            parse_time(b"16:04:12"),
            NaiveTime::from_hms_opt(16, 4, 12).unwrap()
        );
        assert_eq!(
            parse_time(b"16:04:12.0000000"),
            NaiveTime::from_hms_opt(16, 4, 12).unwrap()
        );
        assert_eq!(
            parse_time(b"16:04:12.123456"),
            NaiveTime::from_hms_micro_opt(16, 4, 12, 123456).unwrap()
        );
    }
}
