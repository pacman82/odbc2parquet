use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::{
    sys::{Date, Timestamp},
    Bit,
};
use parquet::{
    column::writer::ColumnWriterImpl,
    data_type::{ByteArray, DataType},
};
use std::{convert::TryInto, ffi::CStr};

pub struct ParquetBuffer {
    /// Used to hold date values converted from ODBC `Date` types or int or decimals with scale 0.
    pub values_i32: Vec<i32>,
    /// Used to hold timestamp values converted from ODBC `Timestamp` types or int or decimal with
    /// scale 0.
    pub values_i64: Vec<i64>,
    pub values_f32: Vec<f32>,
    pub values_f64: Vec<f64>,
    pub values_bytes_array: Vec<ByteArray>,
    pub values_bool: Vec<bool>,
    pub def_levels: Vec<i16>,
}

impl ParquetBuffer {
    pub fn new(batch_size: usize) -> ParquetBuffer {
        ParquetBuffer {
            values_i32: Vec::with_capacity(batch_size),
            values_i64: Vec::with_capacity(batch_size),
            values_f32: Vec::with_capacity(batch_size),
            values_f64: Vec::with_capacity(batch_size),
            values_bytes_array: Vec::with_capacity(batch_size),
            values_bool: Vec::with_capacity(batch_size),
            def_levels: Vec::with_capacity(batch_size),
        }
    }

    pub fn set_num_rows_fetched(&mut self, num_rows: usize) {
        self.def_levels.resize(num_rows, 0);
        self.values_i32.resize(num_rows, 0);
        self.values_i64.resize(num_rows, 0);
        self.values_f32.resize(num_rows, 0.);
        self.values_f64.resize(num_rows, 0.);
        self.values_bytes_array.resize(num_rows, ByteArray::new());
        self.values_bool.resize(num_rows, false);
    }

    /// In case the ODBC C Type matches the physical Parquet type, we can write the buffer directly
    /// without transforming. The definition levels still require transformation, though.
    pub fn write_optional<T, S>(
        &mut self,
        cw: &mut ColumnWriterImpl<T>,
        source: impl Iterator<Item = Option<S>>,
    ) -> Result<(), Error>
    where
        T: DataType,
        T::T: BufferedDataType,
        S: IntoPhysical<T::T>,
    {
        let (values, def_levels) = T::T::mut_buf(self);
        let mut values_index = 0;
        for (item, definition_level) in source.zip(&mut def_levels.iter_mut()) {
            *definition_level = if let Some(value) = item {
                values[values_index] = value.into_physical();
                values_index += 1;
                1
            } else {
                0
            }
        }
        cw.write_batch(values, Some(&def_levels), None)?;
        Ok(())
    }
}

pub trait BufferedDataType: Sized {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]);
}

impl BufferedDataType for i32 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_i32.as_mut_slice(),
            buffer.def_levels.as_mut_slice(),
        )
    }
}

impl BufferedDataType for i64 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_i64.as_mut_slice(),
            buffer.def_levels.as_mut_slice(),
        )
    }
}

impl BufferedDataType for f32 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_f32.as_mut_slice(),
            buffer.def_levels.as_mut_slice(),
        )
    }
}

impl BufferedDataType for f64 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_f64.as_mut_slice(),
            buffer.def_levels.as_mut_slice(),
        )
    }
}

impl BufferedDataType for bool {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_bool.as_mut_slice(),
            buffer.def_levels.as_mut_slice(),
        )
    }
}

impl BufferedDataType for ByteArray {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_bytes_array.as_mut_slice(),
            buffer.def_levels.as_mut_slice(),
        )
    }
}

pub trait IntoPhysical<T> {
    fn into_physical(self) -> T;
}

impl<T> IntoPhysical<T> for &T
where
    T: Copy,
{
    fn into_physical(self) -> T {
        *self
    }
}

impl IntoPhysical<i32> for &Date {
    fn into_physical(self) -> i32 {
        let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
        // Transform date to days since unix epoch as i32
        let date = NaiveDate::from_ymd(self.year as i32, self.month as u32, self.day as u32);
        let duration = date.signed_duration_since(unix_epoch);
        duration.num_days().try_into().unwrap()
    }
}

impl IntoPhysical<i64> for &Timestamp {
    fn into_physical(self) -> i64 {
        let datetime = NaiveDate::from_ymd(self.year as i32, self.month as u32, self.day as u32)
            .and_hms_nano(
                self.hour as u32,
                self.minute as u32,
                self.second as u32,
                self.fraction as u32,
            );
        datetime.timestamp_nanos() / 1000
    }
}

impl IntoPhysical<bool> for &Bit {
    fn into_physical(self) -> bool {
        self.as_bool()
    }
}

impl IntoPhysical<ByteArray> for &CStr {
    fn into_physical(self) -> ByteArray {
        self.to_bytes().to_owned().into()
    }
}
