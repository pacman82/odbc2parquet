//! Strategies for fetching data where ODBC and parquet type are binary identical.

use std::marker::PhantomData;

use anyhow::Error;
use odbc_api::buffers::{AnyColumnView, BufferDescription};
use parquet::{
    basic::{ConvertedType, Repetition},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::DataType,
    schema::types::Type,
};

use crate::parquet_buffer::{BufferedDataType, ParquetBuffer};

use super::{odbc_buffer_item::OdbcBufferItem, ColumnFetchStrategy};

/// Copy identical optional data from ODBC to Parquet.
pub struct FetchIdentical<Pdt> {
    repetition: Repetition,
    converted_type: ConvertedType,
    precision: Option<i32>,
    _parquet_data_type: PhantomData<Pdt>,
}

/// Columnar fetch strategy to be applied if Parquet and Odbc value type are binary identical.
/// Generic argument is a parquet data type.
impl<Pdt> FetchIdentical<Pdt> {
    pub fn new(repetition: Repetition) -> Self {
        Self::with_converted_type(repetition, ConvertedType::NONE)
    }

    /// Odbc buffer and parquet type are identical, but we want to annotate the parquet column with
    /// a specific converted type (aka. former logical type).
    pub fn with_converted_type(repetition: Repetition, converted_type: ConvertedType) -> Self {
        Self {
            repetition,
            converted_type,
            precision: None,
            _parquet_data_type: PhantomData,
        }
    }

    /// For decimal types with a Scale of zero we can have a binary identical ODBC parquet
    /// representation as either 32 or 64 bit integers.
    pub fn decimal_with_precision(repetition: Repetition, precision: i32) -> Self {
        Self {
            repetition,
            converted_type: ConvertedType::DECIMAL,
            precision: Some(precision),
            _parquet_data_type: PhantomData,
        }
    }
}

impl<Pdt> ColumnFetchStrategy for FetchIdentical<Pdt>
where
    Pdt: DataType,
    Pdt::T: OdbcBufferItem + BufferedDataType,
{
    fn parquet_type(&self, name: &str) -> Type {
        let physical_type = Pdt::get_physical_type();
        let mut builder = Type::primitive_type_builder(name, physical_type)
            .with_repetition(self.repetition)
            .with_converted_type(self.converted_type);
        if let Some(precision) = self.precision {
            builder = builder.with_scale(0).with_precision(precision);
        }
        builder.build().unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: Pdt::T::BUFFER_KIND,
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        let it = Pdt::T::nullable_buffer(column_view);
        let column_writer = get_typed_column_writer_mut::<Pdt>(column_writer);
        parquet_buffer.write_optional(column_writer, it)?;
        Ok(())
    }
}
