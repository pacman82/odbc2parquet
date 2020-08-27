use odbc_api::{
    buffers::{BindColParameters, ColumnBuffer, TextColumn},
    sys::{CDataType, Len, Pointer, SqlDataType, ULen, USmallInt},
    Cursor, Error, RowSetBuffer,
};
use std::convert::TryInto;

#[derive(Clone, Copy)]
pub enum ColumnBufferDescription {
    Text { max_str_len: usize },
    F64,
}

pub fn derive_buffer_description(cursor: &Cursor) -> Result<Vec<ColumnBufferDescription>, Error> {
    (1..(cursor.num_result_cols()? + 1))
        .map(|column_number| {
            match cursor.col_data_type(column_number as USmallInt)? {
                SqlDataType::DOUBLE => Ok(ColumnBufferDescription::F64),
                _ => {
                    // +1 for terminating zero
                    let max_str_len =
                        cursor.col_display_size(column_number.try_into().unwrap())? as usize + 1;
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
}

impl OdbcBuffer {
    pub fn new(batch_size: usize, desc: impl Iterator<Item = ColumnBufferDescription>) -> Self {
        let mut text_buffers = Vec::new();
        let mut f64_buffers = Vec::new();
        for (col_index, column_desc) in desc.enumerate() {
            match column_desc {
                ColumnBufferDescription::Text { max_str_len } => {
                    text_buffers.push((col_index, TextColumn::new(batch_size, max_str_len)))
                }
                ColumnBufferDescription::F64 => {
                    f64_buffers.push((col_index, F64ColumnBuffer::new(batch_size)))
                }
            };
        }
        Self {
            num_rows_fetched: 0,
            batch_size,
            text_buffers,
            f64_buffers,
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
        if let Err(e) = cursor.bind_col(
            column_number,
            target_type,
            target_value,
            target_length,
            indicator,
        ) {
            cursor.unbind_cols().expect(&format!(
                "Error unbinding columns. Unbinding was triggerd due to an error binding a \
            column: {}",
                e
            ));
            Err(e)
        } else {
            Ok(())
        }
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

pub struct F64ColumnBuffer {
    values: Vec<f64>,
    indicators: Vec<Len>,
}

impl F64ColumnBuffer {
    pub fn new(batch_size: usize) -> Self {
        F64ColumnBuffer {
            values: vec![0.; batch_size],
            indicators: vec![0; batch_size],
        }
    }

    pub fn values(&self) -> &[f64] {
        &self.values
    }

    pub fn indicators(&self) -> &[Len] {
        &self.indicators
    }

    // pub unsafe fn value_at(&self, row_index: usize) -> Option<f64> {
    //     let str_len = self.indicators[row_index];
    //     if str_len == NULL_DATA {
    //         None
    //     } else {
    //         Some(self.values[row_index])
    //     }
    // }
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
