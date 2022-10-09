//! Strategies for fetching data where ODBC and parquet type are binary identical.

use std::marker::PhantomData;

use anyhow::Error;
use odbc_api::buffers::{AnyColumnView, BufferDescription, Item};
use parquet::{
    basic::{LogicalType, Repetition},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::DataType,
    schema::types::Type,
};

use crate::parquet_buffer::{BufferedDataType, ParquetBuffer};

use super::ColumnFetchStrategy;

/// Copy identical optional data from ODBC to Parquet.
pub struct IdenticalOptional<Pdt> {
    logical_type: Option<LogicalType>,
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
            logical_type,
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
        parquet_data_type::<Pdt>(name, self.logical_type.clone(), Repetition::OPTIONAL)
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
        parquet_data_type::<Pdt>(name, self.logical_type.clone(), Repetition::REQUIRED)
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

fn parquet_data_type<Pdt>(
    name: &str,
    logical_type: Option<LogicalType>,
    repetition: Repetition,
) -> Type
where
    Pdt: DataType,
{
    let physical_type = Pdt::get_physical_type();
    let mut builder = Type::primitive_type_builder(name, physical_type)
        .with_repetition(repetition)
        .with_logical_type(logical_type.clone());
    if let Some(LogicalType::Decimal { scale, precision }) = logical_type {
        builder = builder.with_scale(scale).with_precision(precision);
    }
    builder.build().unwrap()
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
        Box::new(IdenticalOptional::<Pdt>::with_logical_type(Some(
            logical_type,
        )))
    } else {
        Box::new(IdenticalRequired::<Pdt>::with_logical_type(Some(
            logical_type,
        )))
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
