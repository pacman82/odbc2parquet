//! Strategies for fetching data where ODBC and parquet type are binary identical.

use std::marker::PhantomData;

use anyhow::Error;
use odbc_api::buffers::{AnyColumnView, BufferDescription, Item};
use parquet::{
    basic::{ConvertedType, LogicalType, Repetition},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::DataType,
    schema::types::Type,
};

use crate::parquet_buffer::{BufferedDataType, ParquetBuffer};

use super::ColumnFetchStrategy;

/// Copy identical optional data from ODBC to Parquet.
pub struct IdenticalOptional<Pdt> {
    converted_type: ConvertedType,
    logical_type: Option<LogicalType>,
    precision: Option<i32>,
    _parquet_data_type: PhantomData<Pdt>,
}

/// Columnar fetch strategy to be applied if Parquet and Odbc value type are binary identical.
/// Generic argument is a parquet data type.
impl<Pdt> IdenticalOptional<Pdt> {
    pub fn new() -> Self {
        Self::with_logical_type(None)
    }

    /// Odbc buffer and physical parquet type are identical, but we want to annotate the parquet
    /// column with a specific logical type.
    pub fn with_logical_type(logical_type: Option<LogicalType>) -> Self {
        Self {
            converted_type: ConvertedType::NONE,
            logical_type,
            precision: None,
            _parquet_data_type: PhantomData,
        }
    }
}

impl<Pdt> ColumnFetchStrategy for IdenticalOptional<Pdt>
where
    Pdt: DataType,
    Pdt::T: Item + BufferedDataType,
{
    fn parquet_type(&self, name: &str) -> Type {
        let physical_type = Pdt::get_physical_type();
        let mut builder = Type::primitive_type_builder(name, physical_type)
            .with_logical_type(self.logical_type.clone());
        if let Some(LogicalType::Decimal { scale, precision }) = self.logical_type {
            builder = builder.with_scale(scale).with_precision(precision);
        }
        if self.logical_type.is_none() {
            let physical_type = Pdt::get_physical_type();
            let b = Type::primitive_type_builder(name, physical_type)
                .with_repetition(Repetition::OPTIONAL)
                .with_converted_type(self.converted_type);
            if let Some(precision) = self.precision {
                builder = b.with_scale(0).with_precision(precision);
            }
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
        let it = Pdt::T::as_nullable_slice(column_view).unwrap();
        let column_writer = get_typed_column_writer_mut::<Pdt>(column_writer);
        parquet_buffer.write_optional(column_writer, it.map(|opt_ref| opt_ref.copied()))?;
        Ok(())
    }
}

/// Optimized strategy if ODBC and Parquet type are identical, and we know the data source not to
/// contain any NULLs.
pub struct IdenticalRequired<Pdt> {
    logical_type: Option<LogicalType>,
    converted_type: ConvertedType,
    precision: Option<i32>,
    _parquet_data_type: PhantomData<Pdt>,
}

impl<Pdt> IdenticalRequired<Pdt> {
    pub fn new() -> Self {
        Self::with_logical_type(None)
    }

    /// ODBC buffer and parquet type are identical, but we want to annotate the parquet column with
    /// a specific logical type which is not implied by its physical type.
    pub fn with_logical_type(logical_type: Option<LogicalType>) -> Self {
        Self {
            logical_type,
            converted_type: ConvertedType::NONE,
            precision: None,
            _parquet_data_type: PhantomData,
        }
    }
}

impl<Pdt> ColumnFetchStrategy for IdenticalRequired<Pdt>
where
    Pdt: DataType,
    Pdt::T: Item + BufferedDataType,
{
    fn parquet_type(&self, name: &str) -> Type {
        let physical_type = Pdt::get_physical_type();
        let mut builder = Type::primitive_type_builder(name, physical_type)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(self.logical_type.clone());
        if let Some(LogicalType::Decimal { scale, precision }) = self.logical_type {
            builder = builder.with_scale(scale).with_precision(precision);
        }
        if self.logical_type.is_none() {
            builder = builder.with_converted_type(self.converted_type);
        }
        if let Some(precision) = self.precision {
            builder = builder.with_scale(0).with_precision(precision);
        }
        builder.build().unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: Pdt::T::BUFFER_KIND,
            nullable: false,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        _parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        // We do not require to buffer the values, as they must neither be transformed, nor contain
        // any gaps due to null, we can use the ODBC buffer directly to write the batch.

        let values = Pdt::T::as_slice(column_view).unwrap();
        let column_writer = get_typed_column_writer_mut::<Pdt>(column_writer);
        column_writer.write_batch(values, None, None)?;
        Ok(())
    }
}

pub fn fetch_identical<Pdt>(is_optional: bool) -> Box<dyn ColumnFetchStrategy>
where
    Pdt: DataType,
    Pdt::T: Item + BufferedDataType,
{
    if is_optional {
        Box::new(IdenticalOptional::<Pdt>::new())
    } else {
        Box::new(IdenticalRequired::<Pdt>::new())
    }
}

pub fn fetch_identical_with_logical_type<Pdt>(
    is_optional: bool,
    logical_type: LogicalType,
) -> Box<dyn ColumnFetchStrategy>
where
    Pdt: DataType,
    Pdt::T: Item + BufferedDataType,
{
    if is_optional {
        Box::new(IdenticalOptional::<Pdt>::with_logical_type(
            Some(logical_type)
        ))
    } else {
        Box::new(IdenticalRequired::<Pdt>::with_logical_type(
            Some(logical_type),
        ))
    }
}

pub fn fetch_decimal_as_identical_with_precision<Pdt>(
    is_optional: bool,
    precision: i32,
) -> Box<dyn ColumnFetchStrategy>
where
    Pdt: DataType,
    Pdt::T: Item + BufferedDataType,
{
    let logical_type = LogicalType::Decimal {
        scale: 0,
        precision,
    };
    fetch_identical_with_logical_type::<Pdt>(is_optional, logical_type)
}
