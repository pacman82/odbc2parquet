use odbc_api::{
    buffers::{BindColParameters, ColumnBuffer, FixedSizedCType, TextColumn},
    sys::{Date, Len, Pointer, Timestamp, ULen, USmallInt, NULL_DATA},
    Cursor, Error, RowSetBuffer,
};
use std::{convert::TryInto, mem::size_of};

#[derive(Clone, Copy, Debug)]
pub enum ColumnBufferDescription {
    Text { max_str_len: usize },
    F64,
    F32,
    Date,
    Timestamp,
    I32,
    I64,
}

pub struct OdbcBuffer {
    batch_size: usize,
    num_rows_fetched: ULen,
    text_buffers: Vec<(usize, TextColumn)>,
    f64_buffers: Vec<(usize, F64ColumnBuffer)>,
    f32_buffers: Vec<(usize, F32ColumnBuffer)>,
    date_buffers: Vec<(usize, DateColumnBuffer)>,
    timestamp_buffers: Vec<(usize, TimestampColumnBuffer)>,
    i32_buffers: Vec<(usize, I32ColumnBuffer)>,
    i64_buffers: Vec<(usize, I64ColumnBuffer)>,
}

impl OdbcBuffer {
    pub fn new(batch_size: usize, desc: impl Iterator<Item = ColumnBufferDescription>) -> Self {
        let mut text_buffers = Vec::new();
        let mut f64_buffers = Vec::new();
        let mut f32_buffers = Vec::new();
        let mut date_buffers = Vec::new();
        let mut timestamp_buffers = Vec::new();
        let mut i32_buffers = Vec::new();
        let mut i64_buffers = Vec::new();
        for (col_index, column_desc) in desc.enumerate() {
            match column_desc {
                ColumnBufferDescription::Text { max_str_len } => {
                    text_buffers.push((col_index, TextColumn::new(batch_size, max_str_len)))
                }
                ColumnBufferDescription::F64 => {
                    f64_buffers.push((col_index, F64ColumnBuffer::new(batch_size)))
                }
                ColumnBufferDescription::F32 => {
                    f32_buffers.push((col_index, F32ColumnBuffer::new(batch_size)))
                }
                ColumnBufferDescription::Date => {
                    date_buffers.push((col_index, DateColumnBuffer::new(batch_size)))
                }
                ColumnBufferDescription::Timestamp => {
                    timestamp_buffers.push((col_index, TimestampColumnBuffer::new(batch_size)))
                }
                ColumnBufferDescription::I32 => {
                    i32_buffers.push((col_index, I32ColumnBuffer::new(batch_size)))
                }
                ColumnBufferDescription::I64 => {
                    i64_buffers.push((col_index, I64ColumnBuffer::new(batch_size)))
                }
            };
        }
        Self {
            num_rows_fetched: 0,
            batch_size,
            text_buffers,
            f64_buffers,
            f32_buffers,
            date_buffers,
            timestamp_buffers,
            i32_buffers,
            i64_buffers,
        }
    }

    pub fn num_rows_fetched(&self) -> ULen {
        self.num_rows_fetched
    }

    pub fn text_column_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<&[u8]>> {
        let buffer = Self::find_buffer(&self.text_buffers, col_index, "text");
        unsafe {
            (0..self.num_rows_fetched as usize).map(move |row_index| buffer.value_at(row_index))
        }
    }

    pub fn f64_column(&self, col_index: usize) -> (&[f64], &[Len]) {
        self.fixed_size_column_buffer(&self.f64_buffers, col_index, "f64")
    }

    pub fn f32_column(&self, col_index: usize) -> (&[f32], &[Len]) {
        self.fixed_size_column_buffer(&self.f32_buffers, col_index, "f32")
    }

    pub fn i32_column(&self, col_index: usize) -> (&[i32], &[Len]) {
        self.fixed_size_column_buffer(&self.i32_buffers, col_index, "i32")
    }

    pub fn i64_column(&self, col_index: usize) -> (&[i64], &[Len]) {
        self.fixed_size_column_buffer(&self.i64_buffers, col_index, "i64")
    }

    pub fn date_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<&Date>> {
        let buffer = Self::find_buffer(&self.date_buffers, col_index, "date");
        unsafe {
            (0..self.num_rows_fetched as usize).map(move |row_index| buffer.value_at(row_index))
        }
    }

    pub fn timestamp_it(
        &self,
        col_index: usize,
    ) -> impl ExactSizeIterator<Item = Option<&Timestamp>> {
        let buffer = Self::find_buffer(&self.timestamp_buffers, col_index, "date");
        unsafe {
            (0..self.num_rows_fetched as usize).map(move |row_index| buffer.value_at(row_index))
        }
    }

    fn fixed_size_column_buffer<'a, T: FixedSizedCType>(
        &self,
        buffers: &'a [(usize, FixedSizedColumnBuffer<T>)],
        col_index: usize,
        typename: &'static str,
    ) -> (&'a [T], &'a [Len]) {
        let buffer = Self::find_buffer(buffers, col_index, typename);
        (
            &buffer.values()[..self.num_rows_fetched as usize],
            &buffer.indicators()[..self.num_rows_fetched as usize],
        )
    }

    fn find_buffer<'a, T>(
        buffers: &'a [(usize, T)],
        col_index: usize,
        typename: &'static str,
    ) -> &'a T {
        let (_col_index, buffer) = buffers
            .iter()
            .find(|(index, _buf)| *index == col_index)
            .unwrap_or_else(|| panic!("No {} buffer found with specified index", typename));
        buffer
    }
}

fn bind_column_to_cursor(
    cursor: &mut Cursor,
    column_buffer: &mut impl ColumnBuffer,
    column_number: USmallInt,
) -> Result<(), Error> {
    let BindColParameters {
        target_type,
        target_value,
        target_length,
        indicator,
    } = column_buffer.bind_arguments();
    unsafe {
        cursor.bind_col(
            column_number,
            target_type,
            target_value,
            target_length,
            indicator,
        )
    }
}

unsafe impl RowSetBuffer for OdbcBuffer {
    unsafe fn bind_to_cursor(
        &mut self,
        cursor: &mut odbc_api::Cursor,
    ) -> Result<(), odbc_api::Error> {
        cursor.set_row_array_size(self.batch_size.try_into().unwrap())?;
        cursor.set_num_rows_fetched(&mut self.num_rows_fetched)?;
        // Text buffers
        for &mut (index, ref mut column_buffer) in self.text_buffers.iter_mut() {
            bind_column_to_cursor(cursor, column_buffer, (index + 1) as USmallInt)?;
        }
        // f64 buffers
        for &mut (index, ref mut column_buffer) in self.f64_buffers.iter_mut() {
            bind_column_to_cursor(cursor, column_buffer, (index + 1) as USmallInt)?;
        }
        Ok(())
    }
}

type F64ColumnBuffer = FixedSizedColumnBuffer<f64>;
type F32ColumnBuffer = FixedSizedColumnBuffer<f32>;
type DateColumnBuffer = FixedSizedColumnBuffer<Date>;
type TimestampColumnBuffer = FixedSizedColumnBuffer<Timestamp>;
type I32ColumnBuffer = FixedSizedColumnBuffer<i32>;
type I64ColumnBuffer = FixedSizedColumnBuffer<i64>;

pub struct FixedSizedColumnBuffer<F> {
    values: Vec<F>,
    indicators: Vec<Len>,
}

impl<F> FixedSizedColumnBuffer<F>
where
    F: Default + Clone,
{
    pub fn new(batch_size: usize) -> Self {
        Self {
            values: vec![F::default(); batch_size],
            indicators: vec![NULL_DATA; batch_size],
        }
    }

    pub unsafe fn value_at(&self, row_index: usize) -> Option<&F> {
        if self.indicators[row_index] == NULL_DATA {
            None
        } else {
            Some(&self.values[row_index])
        }
    }

    pub fn values(&self) -> &[F] {
        &self.values
    }

    pub fn indicators(&self) -> &[Len] {
        &self.indicators
    }
}

unsafe impl<T> ColumnBuffer for FixedSizedColumnBuffer<T>
where
    T: FixedSizedCType,
{
    fn bind_arguments(&mut self) -> BindColParameters {
        BindColParameters {
            target_type: T::C_DATA_TYPE,
            target_value: self.values.as_mut_ptr() as Pointer,
            target_length: size_of::<T>().try_into().unwrap(),
            indicator: self.indicators.as_mut_ptr(),
        }
    }
}
