use anyhow::Error;
use chrono::NaiveDate;
use num_bigint::BigInt;
use odbc_api::{
    sys::{Date, Timestamp},
    Bit,
};
use parquet::{
    column::{reader::ColumnReaderImpl, writer::ColumnWriterImpl},
    data_type::{ByteArray, DataType, FixedLenByteArray, FixedLenByteArrayType, Int64Type},
};
use std::{convert::TryInto, mem::size_of};

/// Holds preallocated buffers for every possible physical parquet type. This way we do not need to
/// reallocate them.
pub struct ParquetBuffer {
    pub values_i32: Vec<i32>,
    pub values_i64: Vec<i64>,
    pub values_f32: Vec<f32>,
    pub values_f64: Vec<f64>,
    pub values_bytes_array: Vec<ByteArray>,
    pub values_fixed_bytes_array: Vec<FixedLenByteArray>,
    pub values_bool: Vec<bool>,
    pub def_levels: Vec<i16>,
}

impl ParquetBuffer {
    /// Memory usage of this buffer per row. Used together with the size of the ODBC buffer to
    /// estimate good batch sizes.
    pub const MEMORY_USAGE_BYTES_PER_ROW: usize = size_of::<i32>()
        + size_of::<i64>()
        + size_of::<f32>()
        + size_of::<f64>()
        + size_of::<ByteArray>()
        + size_of::<FixedLenByteArrayType>()
        + size_of::<bool>()
        + size_of::<i16>();

    pub fn new(batch_size: usize) -> ParquetBuffer {
        ParquetBuffer {
            values_i32: Vec::with_capacity(batch_size),
            values_i64: Vec::with_capacity(batch_size),
            values_f32: Vec::with_capacity(batch_size),
            values_f64: Vec::with_capacity(batch_size),
            values_bytes_array: Vec::with_capacity(batch_size),
            values_fixed_bytes_array: Vec::with_capacity(batch_size),
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
        self.values_fixed_bytes_array
            .resize(num_rows, ByteArray::new().into());
        self.values_bool.resize(num_rows, false);
    }

    /// Use an i128 to calculate the twos complement of Decimals with a precision up to and including 38
    fn twos_complement_i128(
        decimal: &[u8],
        length: usize,
        digits: &mut Vec<u8>,
    ) -> FixedLenByteArray {
        use atoi::FromRadix10Signed;

        digits.clear();
        digits.extend(decimal.iter().filter(|&&c| c != b'.'));

        let (num, _consumed) = i128::from_radix_10_signed(&digits);

        let out = num.to_be_bytes()[(16 - length)..].to_owned();
        // Vec<u8> -> ByteArray -> FixedLenByteArray
        let out: ByteArray = out.into();
        out.into()
    }

    // Use num big int to calculate the two complements of arbitrary size
    fn twos_complement_big_int(
        decimal: &[u8],
        length: usize,
        digits: &mut Vec<u8>,
    ) -> FixedLenByteArray {
        use atoi::FromRadix10Signed;

        digits.clear();
        digits.extend(decimal.iter().filter(|&&c| c != b'.'));

        let (num, _consumed) = BigInt::from_radix_10_signed(&digits);
        let mut out = num.to_signed_bytes_be();

        let num_leading_bytes = length - out.len();
        let fill: u8 = if num.sign() == num_bigint::Sign::Minus {
            255
        } else {
            0
        };
        out.resize(length, fill);
        out.rotate_right(num_leading_bytes);
        // Vec<u8> -> ByteArray -> FixedByteArray
        let out: ByteArray = out.into();
        out.into()
    }

    fn timestamp_nanos(ts: &Timestamp) -> i64 {
        let datetime = NaiveDate::from_ymd(ts.year as i32, ts.month as u32, ts.day as u32)
            .and_hms_nano(
                ts.hour as u32,
                ts.minute as u32,
                ts.second as u32,
                ts.fraction as u32,
            );
        datetime.timestamp_nanos()
    }

    pub fn write_timestamp<'o>(
        &mut self,
        cw: &mut ColumnWriterImpl<Int64Type>,
        source: impl Iterator<Item = Option<&'o Timestamp>>,
        precision: i16,
    ) -> Result<(), Error> {
        if precision <= 3 {
            // Milliseconds precision
            self.write_optional_any(cw, source, |ts| Self::timestamp_nanos(ts) / 1_000_000)?;
        } else {
            // Microseconds precision
            self.write_optional_any(cw, source, |ts| Self::timestamp_nanos(ts) / 1_000)?;
        }
        Ok(())
    }

    pub fn write_decimal<'o>(
        &mut self,
        cw: &mut ColumnWriterImpl<FixedLenByteArrayType>,
        source: impl Iterator<Item = Option<&'o [u8]>>,
        length_in_bytes: usize,
        precision: usize,
    ) -> Result<(), Error> {
        // This vec is going to hold the digits with sign, but without the decimal point. It is
        // allocated once and reused for each value.
        let mut digits: Vec<u8> = Vec::with_capacity(precision + 1);

        if precision < 39 {
            self.write_optional_any(cw, source, |item| {
                Self::twos_complement_i128(item, length_in_bytes, &mut digits)
            })
        } else {
            // The big int implementation is slow, let's use it only if we have to
            self.write_optional_any(cw, source, |item| {
                Self::twos_complement_big_int(item, length_in_bytes, &mut digits)
            })
        }
    }

    fn write_optional_any<T, S>(
        &mut self,
        cw: &mut ColumnWriterImpl<T>,
        source: impl Iterator<Item = Option<S>>,
        mut into_physical: impl FnMut(S) -> T::T,
    ) -> Result<(), Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        let (values, def_levels) = T::T::mut_buf(self);
        let mut values_index = 0;
        for (item, definition_level) in source.zip(&mut def_levels.iter_mut()) {
            *definition_level = if let Some(value) = item {
                values[values_index] = into_physical(value);
                values_index += 1;
                1
            } else {
                0
            }
        }
        cw.write_batch(values, Some(&def_levels), None)?;
        Ok(())
    }

    /// Write to a parquet buffer using an iterator over optional source items. A default
    /// transformation, defined via the `IntoPhysical` trait is used to transform the items into
    /// buffer elements.
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
        self.write_optional_any(cw, source, |s| s.into_physical())
    }

    /// Iterate over the elements of a column reader over an optional column.
    ///
    /// Be careful with calling this method on required columns as the bound definition buffer will
    /// always be filled with zeros, which will make all elements `None`.
    pub fn read_optional<'a, T>(
        &'a mut self,
        cr: &mut ColumnReaderImpl<T>,
        batch_size: usize,
    ) -> Result<impl Iterator<Item = Option<&T::T>> + 'a, Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        let (values, def_levels) = T::T::mut_buf(self);
        let (_num_val, _num_lvl) = cr.read_batch(batch_size, Some(def_levels), None, values)?;
        let it = def_levels
            .iter()
            .scan(&values[..], |values, def| match def {
                0 => Some(None),
                1 => {
                    let val = &values[0];
                    *values = &values[1..];
                    Some(Some(val))
                }
                _ => panic!("Only definition level 0 and 1 are supported"),
            });

        Ok(it)
    }

    /// Iterate over the elements of a column reader over a required column.
    pub fn read_required<'a, T>(
        &'a mut self,
        cr: &mut ColumnReaderImpl<T>,
        batch_size: usize,
    ) -> Result<impl Iterator<Item = &T::T> + 'a, Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        let (values, _def_levels) = T::T::mut_buf(self);
        let (_num_val, _num_lvl) = cr.read_batch(batch_size, None, None, values)?;
        let it = values.iter();

        Ok(it)
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

impl BufferedDataType for FixedLenByteArray {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut [Self], &mut [i16]) {
        (
            buffer.values_fixed_bytes_array.as_mut_slice(),
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

impl IntoPhysical<bool> for &Bit {
    fn into_physical(self) -> bool {
        self.as_bool()
    }
}

impl IntoPhysical<ByteArray> for String {
    fn into_physical(self) -> ByteArray {
        self.into_bytes().into()
    }
}

impl IntoPhysical<ByteArray> for &[u8] {
    fn into_physical(self) -> ByteArray {
        self.to_owned().into()
    }
}

impl IntoPhysical<FixedLenByteArray> for &[u8] {
    fn into_physical(self) -> FixedLenByteArray {
        let byte_array: ByteArray = self.to_owned().into();
        byte_array.into()
    }
}

#[cfg(test)]
mod test {

    use super::ParquetBuffer;

    
    #[test]
    #[cfg(target_pointer_width = "64")] // Memory usage is platform dependent
    fn memory_usage() {
        assert_eq!(59, ParquetBuffer::MEMORY_USAGE_BYTES_PER_ROW);
    }
}