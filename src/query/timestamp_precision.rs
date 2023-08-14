use anyhow::{anyhow, Error};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use odbc_api::sys::Timestamp;
use parquet::format::{MicroSeconds, MilliSeconds, NanoSeconds, TimeUnit};

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
    pub fn timestamp_to_i64(self, ts: &Timestamp) -> Result<i64, Error> {
        let datetime = NaiveDate::from_ymd_opt(ts.year as i32, ts.month as u32, ts.day as u32)
            .unwrap()
            .and_hms_nano_opt(
                ts.hour as u32,
                ts.minute as u32,
                ts.second as u32,
                ts.fraction,
            )
            .unwrap();

        let ret = match self {
            TimestampPrecision::Milliseconds => datetime.timestamp_millis(),
            TimestampPrecision::Microseconds => datetime.timestamp_micros(),
            TimestampPrecision::Nanoseconds => {
                let secs = i64::MAX / 1_000_000_000;
                let nsecs = (i64::MAX % 1_000_000_000) as u32;
                let max = NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap();

                if datetime > max {
                    return Err(anyhow!("Timestamp exceeds maximum valid range for timestamp with nanoseconds precision."));
                }

                datetime.timestamp_nanos()
            }
        };

        Ok(ret)
    }

    pub fn datetime_to_i64(self, datetime: &DateTime<Utc>) -> i64 {
        match self {
            TimestampPrecision::Milliseconds => datetime.timestamp_millis(),
            TimestampPrecision::Microseconds => datetime.timestamp_micros(),
            TimestampPrecision::Nanoseconds => datetime.timestamp_nanos(),
        }
    }
}