use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::sys::Timestamp;
use parquet::{
    column::{reader::ColumnReaderImpl, writer::ColumnWriterImpl},
    data_type::{ByteArray, DataType, FixedLenByteArray, FixedLenByteArrayType, Int64Type},
};
use std::mem::size_of;

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

    /// Writes an i128 twos complement representation into a fixed sized byte array
    pub fn write_twos_complement_i128(
        &mut self,
        cw: &mut ColumnWriterImpl<FixedLenByteArrayType>,
        source: impl Iterator<Item = Option<i128>>,
        length_in_bytes: usize,
    ) -> Result<(), Error> {
        self.write_optional_any(cw, source, |num| {
            let out = num.to_be_bytes()[(16 - length_in_bytes)..].to_owned();
            // Vec<u8> -> ByteArray -> FixedLenByteArray
            let out: ByteArray = out.into();
            out.into()
        })
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
        cw.write_batch(values, Some(def_levels), None)?;
        Ok(())
    }

    /// Write to a parquet buffer using an iterator over optional source items. A default
    /// transformation, defined via the `IntoPhysical` trait is used to transform the items into
    /// buffer elements.
    pub fn write_optional<T>(
        &mut self,
        cw: &mut ColumnWriterImpl<T>,
        source: impl Iterator<Item = Option<T::T>>,
    ) -> Result<(), Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        self.write_optional_any(cw, source, |s| s)
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
        // Strip mutability form the element of values, so we can use it in scan, there we only want
        // to mutate which part of values we see, not the elements of values themselfes.
        let values: &_ = values;
        let it = def_levels.iter().scan(values, |values, def| match def {
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

#[cfg(test)]
mod test {

    use super::ParquetBuffer;

    #[test]
    #[cfg(target_pointer_width = "64")] // Memory usage is platform dependent
    fn memory_usage() {
        assert_eq!(59, ParquetBuffer::MEMORY_USAGE_BYTES_PER_ROW);
    }
}
