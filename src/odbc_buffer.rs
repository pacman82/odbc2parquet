use odbc_api::{
    Cursor,
    buffers::{
        ColumnBuffer, OptBitColumn, OptDateColumn, OptF32Column, OptF64Column, OptI32Column,
        OptI64Column, OptTimestampColumn, TextColumn, BindColArgs
    },
    sys::{Date, Timestamp, ULen},
    RowSetBuffer,
};
use std::convert::TryInto;

#[derive(Clone, Copy, Debug)]
pub enum ColumnBufferDescription {
    Text { max_str_len: usize },
    F64,
    F32,
    Date,
    Timestamp,
    I32,
    I64,
    Bit,
}

enum AnyColumnBuffer {
    Text(TextColumn),
    F64(OptF64Column),
    F32(OptF32Column),
    Date(OptDateColumn),
    Timestamp(OptTimestampColumn),
    I32(OptI32Column),
    I64(OptI64Column),
    Bit(OptBitColumn),
}

impl AnyColumnBuffer {
    pub fn new(desc: ColumnBufferDescription, batch_size: usize) -> AnyColumnBuffer {
        match desc {
            ColumnBufferDescription::Text { max_str_len } => {
                AnyColumnBuffer::Text(TextColumn::new(batch_size, max_str_len))
            }
            ColumnBufferDescription::F64 => AnyColumnBuffer::F64(OptF64Column::new(batch_size)),
            ColumnBufferDescription::F32 => AnyColumnBuffer::F32(OptF32Column::new(batch_size)),
            ColumnBufferDescription::Date => AnyColumnBuffer::Date(OptDateColumn::new(batch_size)),
            ColumnBufferDescription::Timestamp => {
                AnyColumnBuffer::Timestamp(OptTimestampColumn::new(batch_size))
            }
            ColumnBufferDescription::I32 => AnyColumnBuffer::I32(OptI32Column::new(batch_size)),
            ColumnBufferDescription::I64 => AnyColumnBuffer::I64(OptI64Column::new(batch_size)),
            ColumnBufferDescription::Bit => AnyColumnBuffer::Bit(OptBitColumn::new(batch_size)),
        }
    }

    pub fn bind_arguments(&mut self) -> BindColArgs {
        match self {
            AnyColumnBuffer::Bit(buf) => buf.bind_arguments(),
            AnyColumnBuffer::Text(buf) => buf.bind_arguments(),
            AnyColumnBuffer::F64(buf) => buf.bind_arguments(),
            AnyColumnBuffer::F32(buf) => buf.bind_arguments(),
            AnyColumnBuffer::Date(buf) => buf.bind_arguments(),
            AnyColumnBuffer::Timestamp(buf) => buf.bind_arguments(),
            AnyColumnBuffer::I32(buf) => buf.bind_arguments(),
            AnyColumnBuffer::I64(buf) => buf.bind_arguments(),
        }
    }
}

pub struct OdbcBuffer {
    batch_size: usize,
    num_rows_fetched: ULen,
    buffers: Vec<AnyColumnBuffer>,
}

impl OdbcBuffer {
    pub fn new(batch_size: usize, desc: impl Iterator<Item = ColumnBufferDescription>) -> Self {
        Self {
            num_rows_fetched: 0,
            batch_size,
            buffers: desc.map(|d| AnyColumnBuffer::new(d, batch_size)).collect(),
        }
    }

    pub fn num_rows_fetched(&self) -> ULen {
        self.num_rows_fetched
    }

    pub fn text_column_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<&[u8]>> {
        if let AnyColumnBuffer::Text(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize).map(move |row_index| buffer.value_at(row_index))
            }
        } else {
            panic!("Index {}, doest not hold a text buffer.", col_index)
        }
    }

    pub fn f64_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<f64>> + '_ {
        if let AnyColumnBuffer::F64(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize)
                    .map(move |row_index| buffer.value_at(row_index).copied())
            }
        } else {
            panic!("Index {}, doest not hold an f64 buffer.", col_index)
        }
    }

    pub fn f32_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<f32>> + '_ {
        if let AnyColumnBuffer::F32(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize)
                    .map(move |row_index| buffer.value_at(row_index).copied())
            }
        } else {
            panic!("Index {}, doest not hold an f32 buffer.", col_index)
        }
    }

    pub fn i32_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<i32>> + '_ {
        if let AnyColumnBuffer::I32(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize)
                    .map(move |row_index| buffer.value_at(row_index).copied())
            }
        } else {
            panic!("Index {}, doest not hold an i32 buffer.", col_index)
        }
    }

    pub fn i64_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<i64>> + '_ {
        if let AnyColumnBuffer::I64(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize)
                    .map(move |row_index| buffer.value_at(row_index).copied())
            }
        } else {
            panic!("Index {}, doest not hold an i64 buffer.", col_index)
        }
    }

    pub fn date_it(&self, col_index: usize) -> impl ExactSizeIterator<Item = Option<&Date>> {
        if let AnyColumnBuffer::Date(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize).map(move |row_index| buffer.value_at(row_index))
            }
        } else {
            panic!("Index {}, doest not hold a date buffer.", col_index)
        }
    }

    pub fn bool_it(
        &self,
        col_index: usize,
    ) -> impl ExactSizeIterator<Item = Option<bool>> + '_ {
        if let AnyColumnBuffer::Bit(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize)
                    .map(move |row_index| buffer.value_at(row_index).map(|&bit| bit.as_bool()))
            }
        } else {
            panic!("Index {}, doest not hold a boolean buffer.", col_index)
        }
    }

    pub fn timestamp_it(
        &self,
        col_index: usize,
    ) -> impl ExactSizeIterator<Item = Option<&Timestamp>> {
        if let AnyColumnBuffer::Timestamp(ref buffer) = self.buffers[col_index] {
            unsafe {
                (0..self.num_rows_fetched as usize).map(move |row_index| buffer.value_at(row_index))
            }
        } else {
            panic!("Index {}, doest not hold a timestamp buffer.", col_index)
        }
    }
}

unsafe impl RowSetBuffer for OdbcBuffer {
    unsafe fn bind_to_cursor(
        &mut self,
        cursor: &mut impl Cursor,
    ) -> Result<(), odbc_api::Error> {
        cursor.set_row_array_size(self.batch_size.try_into().unwrap())?;
        cursor.set_num_rows_fetched(&mut self.num_rows_fetched)?;
        for (index, buf) in self.buffers.iter_mut().enumerate() {
            cursor.bind_col((index + 1).try_into().unwrap(), buf.bind_arguments())?
        }
        Ok(())
    }
}
