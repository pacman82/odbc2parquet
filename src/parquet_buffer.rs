use anyhow::Error;
use parquet::{
    column::{reader::ColumnReaderImpl, writer::ColumnWriterImpl},
    data_type::{ByteArray, DataType, FixedLenByteArray, FixedLenByteArrayType},
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

    /// Writes an i128 twos complement representation into a fixed sized byte array
    pub fn write_twos_complement_i128(
        &mut self,
        cw: &mut ColumnWriterImpl<FixedLenByteArrayType>,
        source: impl Iterator<Item = Option<i128>>,
        length_in_bytes: usize,
    ) -> Result<(), Error> {
        self.write_optional_any_falliable(cw, source.map(Ok), |num| {
            let out = num.to_be_bytes()[(16 - length_in_bytes)..].to_owned();
            // Vec<u8> -> ByteArray -> FixedLenByteArray
            let out: ByteArray = out.into();
            out.into()
        })
    }

    fn write_optional_any_falliable<T, S>(
        &mut self,
        cw: &mut ColumnWriterImpl<T>,
        source: impl Iterator<Item = Result<Option<S>, Error>>,
        mut into_physical: impl FnMut(S) -> T::T,
    ) -> Result<(), Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        let (values, def_levels) = T::T::mut_buf(self);
        let mut values_index = 0;
        for (item, definition_level) in source.zip(&mut def_levels.iter_mut()) {
            *definition_level = if let Some(value) = item? {
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
    pub fn write_optional_falliable<T>(
        &mut self,
        cw: &mut ColumnWriterImpl<T>,
        source: impl Iterator<Item = Result<Option<T::T>, Error>>,
    ) -> Result<(), Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        self.write_optional_any_falliable(cw, source, |s| s)
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
        self.write_optional_any_falliable(cw, source.map(Ok), |s| s)
    }

    /// Iterate over the elements of a column reader over an optional column.
    ///
    /// Be careful with calling this method on required columns as the bound definition buffer will
    /// always be filled with zeros, which will make all elements `None`.
    pub fn read_optional<T>(
        &mut self,
        cr: &mut ColumnReaderImpl<T>,
        batch_size: usize,
    ) -> Result<impl Iterator<Item = Option<&T::T>> + '_, Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        let (values, def_levels) = T::T::mut_buf(self);
        let (_complete_rec, _num_val, _num_lvl) =
            cr.read_records(batch_size, Some(def_levels), None, values)?;
        // Strip mutability form the element of values, so we can use it in scan, there we only want
        // to mutate which part of values we see, not the elements of values themselfes.
        let values = values.as_slice();
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
    pub fn read_required<T>(
        &mut self,
        cr: &mut ColumnReaderImpl<T>,
        batch_size: usize,
    ) -> Result<impl Iterator<Item = &T::T> + '_, Error>
    where
        T: DataType,
        T::T: BufferedDataType,
    {
        let (values, _def_levels) = T::T::mut_buf(self);
        let (_complete_rec, _num_val, _num_lvl) =
            cr.read_records(batch_size, None, None, values)?;
        let it = values.iter();

        Ok(it)
    }
}

pub trait BufferedDataType: Sized {
    /// The tuple returned is (Values, Definiton levels)
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>);
}

impl BufferedDataType for i32 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_i32,
            &mut buffer.def_levels,
        )
    }
}

impl BufferedDataType for i64 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_i64,
            &mut buffer.def_levels,
        )
    }
}

impl BufferedDataType for f32 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_f32,
            &mut buffer.def_levels,
        )
    }
}

impl BufferedDataType for f64 {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_f64,
            &mut buffer.def_levels,
        )
    }
}

impl BufferedDataType for bool {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_bool,
            &mut buffer.def_levels,
        )
    }
}

impl BufferedDataType for ByteArray {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_bytes_array,
            &mut buffer.def_levels,
        )
    }
}

impl BufferedDataType for FixedLenByteArray {
    fn mut_buf(buffer: &mut ParquetBuffer) -> (&mut Vec<Self>, &mut Vec<i16>) {
        (
            &mut buffer.values_fixed_bytes_array,
            &mut buffer.def_levels,
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
