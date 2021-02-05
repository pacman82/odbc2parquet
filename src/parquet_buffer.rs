use anyhow::Error;
use chrono::NaiveDate;
use num_bigint::BigInt;
use odbc_api::{
    sys::{Date, Timestamp},
    Bit,
};
use parquet::{
    basic::Type as PhysicalType,
    column::writer::ColumnWriterImpl,
    data_type::{ByteArray, DataType, FixedLenByteArray, FixedLenByteArrayType, Int64Type},
    schema::types::Type,
};
use std::{convert::TryInto, ffi::CStr};

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
        decimal: &CStr,
        length: usize,
        digits: &mut Vec<u8>,
    ) -> FixedLenByteArray {
        use atoi::FromRadix10Signed;

        digits.clear();
        digits.extend(decimal.to_bytes().iter().filter(|&&c| c != b'.'));

        let (num, _consumed) = i128::from_radix_10_signed(&digits);

        let out = num.to_be_bytes()[(16 - length)..].to_owned();
        // Vec<u8> -> ByteArray -> FixedLenByteArray
        let out: ByteArray = out.into();
        out.into()
    }

    // Use num big int to calculate the two complements of arbitrary size
    fn twos_complement_big_int(
        decimal: &CStr,
        length: usize,
        digits: &mut Vec<u8>,
    ) -> FixedLenByteArray {
        use atoi::FromRadix10Signed;

        digits.clear();
        digits.extend(decimal.to_bytes().iter().filter(|&&c| c != b'.'));

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
        primitive_type: &Type,
    ) -> Result<(), Error> {
        let &precision = match primitive_type {
            Type::PrimitiveType {
                basic_info: _,
                physical_type: pt,
                type_length: _,
                scale: _,
                precision,
            } => {
                debug_assert_eq!(*pt, PhysicalType::INT64);
                precision
            }
            Type::GroupType { .. } => panic!("Column must be a primitive type"),
        };

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
        source: impl Iterator<Item = Option<&'o CStr>>,
        primitive_type: &Type,
    ) -> Result<(), Error> {
        let (&length, &precision) = match primitive_type {
            Type::PrimitiveType {
                basic_info: _,
                physical_type: pt,
                type_length,
                scale: _,
                precision,
            } => {
                debug_assert_eq!(*pt, PhysicalType::FIXED_LEN_BYTE_ARRAY);
                (type_length, precision)
            }
            Type::GroupType { .. } => panic!("Column must be a primitive type"),
        };

        let precision: usize = precision.try_into().unwrap();

        // This vec is going to hold the digits with sign, but without the decimal point. It is
        // allocated once and reused for each value.
        let mut digits: Vec<u8> = Vec::with_capacity(precision + 1);

        if precision < 39 {
            self.write_optional_any(cw, source, |item| {
                Self::twos_complement_i128(item, length.try_into().unwrap(), &mut digits)
            })
        } else {
            // The big int implementation is slow, let's use it only if we have to
            self.write_optional_any(cw, source, |item| {
                Self::twos_complement_big_int(item, length.try_into().unwrap(), &mut digits)
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

impl IntoPhysical<ByteArray> for &CStr {
    fn into_physical(self) -> ByteArray {
        self.to_bytes().to_owned().into()
    }
}
