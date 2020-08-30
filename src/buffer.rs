use odbc_api::{
    buffers::{BindColParameters, ColumnBuffer, TextColumn},
    sys::{CDataType, Len, Pointer, SqlDataType, ULen, USmallInt, NULL_DATA},
    Cursor, Error, RowSetBuffer,
};
use std::convert::TryInto;

#[derive(Clone, Copy)]
pub enum ColumnBufferDescription {
    Text { max_str_len: usize },
    F64,
    F32,
}

pub fn derive_buffer_description(cursor: &Cursor) -> Result<Vec<ColumnBufferDescription>, Error> {
    (1..(cursor.num_result_cols()? + 1))
        .map(|column_number| {
            match cursor.col_data_type(column_number as USmallInt)? {
                SqlDataType::DOUBLE => Ok(ColumnBufferDescription::F64),
                SqlDataType::FLOAT => Ok(ColumnBufferDescription::F32),
                _ => {
                    // +1 for terminating zero
                    let max_str_len =
                        cursor.col_display_size(column_number.try_into().unwrap())? as usize;
                    Ok(ColumnBufferDescription::Text { max_str_len })
                }
            }
        })
        .collect()
}

pub struct OdbcBuffer {
    batch_size: usize,
    num_rows_fetched: ULen,
    text_buffers: Vec<(usize, TextColumn)>,
    f64_buffers: Vec<(usize, F64ColumnBuffer)>,
    f32_buffers: Vec<(usize, F32ColumnBuffer)>,
}

impl OdbcBuffer {
    pub fn new(batch_size: usize, desc: impl Iterator<Item = ColumnBufferDescription>) -> Self {
        let mut text_buffers = Vec::new();
        let mut f64_buffers = Vec::new();
        let mut f32_buffers = Vec::new();
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
            };
        }
        Self {
            num_rows_fetched: 0,
            batch_size,
            text_buffers,
            f64_buffers,
            f32_buffers,
        }
    }

    pub fn text_column_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<&[u8]>> {
        let (_col_index, text_buffer) = self
            .text_buffers
            .iter()
            .find(|(index, _buf)| *index == col_index)
            .expect("No text buffer found with specified index");
        unsafe {
            (0..self.num_rows_fetched as usize)
                .map(move |row_index| text_buffer.value_at(row_index))
        }
    }

    pub fn f64_column(&self, col_index: usize) -> (&[f64], &[Len]) {
        let (_col_index, f64_buffer) = self
            .f64_buffers
            .iter()
            .find(|(index, _buf)| *index == col_index)
            .expect("No f64 buffer found with specified index");
        (
            &f64_buffer.values()[..self.num_rows_fetched as usize],
            &f64_buffer.indicators()[..self.num_rows_fetched as usize],
        )
    }

    pub fn f32_column(&self, col_index: usize) -> (&[f32], &[Len]) {
        let (_col_index, f32_buffer) = self
            .f32_buffers
            .iter()
            .find(|(index, _buf)| *index == col_index)
            .expect("No f32 buffer found with specified index");
        (
            &f32_buffer.values()[..self.num_rows_fetched as usize],
            &f32_buffer.indicators()[..self.num_rows_fetched as usize],
        )
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

type F64ColumnBuffer = FloatColumnBuffer<f64>;
type F32ColumnBuffer = FloatColumnBuffer<f32>;

pub struct FloatColumnBuffer<F> {
    values: Vec<F>,
    indicators: Vec<Len>,
}

impl<F> FloatColumnBuffer<F> where F: Default + Clone {
    pub fn new(batch_size: usize) -> Self {
        Self {
            values: vec![F::default(); batch_size],
            indicators: vec![NULL_DATA; batch_size],
        }
    }

    pub fn values(&self) -> &[F] {
        &self.values
    }

    pub fn indicators(&self) -> &[Len] {
        &self.indicators
    }
}

unsafe impl ColumnBuffer for F64ColumnBuffer {
    fn bind_arguments(&mut self) -> BindColParameters {
        BindColParameters {
            target_type: CDataType::Double,
            target_value: self.values.as_mut_ptr() as Pointer,
            target_length: 0,
            indicator: self.indicators.as_mut_ptr(),
        }
    }
}

unsafe impl ColumnBuffer for F32ColumnBuffer {
    fn bind_arguments(&mut self) -> BindColParameters {
        BindColParameters {
            target_type: CDataType::Float,
            target_value: self.values.as_mut_ptr() as Pointer,
            target_length: 0,
            indicator: self.indicators.as_mut_ptr(),
        }
    }
}