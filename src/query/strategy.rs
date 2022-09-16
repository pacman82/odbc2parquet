use std::convert::TryInto;

use anyhow::Error;
use log::{debug, warn};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
    sys::SqlDataType,
    ColumnDescription, Cursor, DataType, Nullability,
};
use parquet::{
    basic::{ConvertedType, Repetition},
    column::writer::ColumnWriter,
    data_type::{
        ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
    },
    schema::types::Type,
};

use crate::{
    parquet_buffer::ParquetBuffer,
    query::{
        binary::Binary,
        boolean::Boolean,
        date::Date,
        decimal::decmial_fetch_strategy,
        identical::{fetch_identical, fetch_identical_with_converted_type},
        text::{Utf16ToUtf8, Utf8},
        timestamp::Timestamp, timestamp_tz::timestamp_tz,
    },
};

/// Decisions on how to handle a particular column of the ODBC result set. What buffer to bind to it
/// for fetching, into what parquet type it is going to be translated and how to translate it from
/// the odbc buffer elements to afformentioned parquet type.
pub trait ColumnFetchStrategy {
    /// Parquet column type used in parquet schema
    fn parquet_type(&self, name: &str) -> Type;
    /// Description of the buffer bound to the ODBC data source.
    fn buffer_description(&self) -> BufferDescription;
    /// copy the contents of an ODBC `AnyColumnView` into a Parquet `ColumnWriter`.
    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error>;
}

/// Controls how columns a queried and mapped onto parquet columns
#[derive(Clone, Copy)]
pub struct MappingOptions<'a> {
    pub db_name: &'a str,
    pub use_utf16: bool,
    pub prefer_varbinary: bool,
    pub driver_does_support_i64: bool,
}

pub fn strategy_from_column_description(
    cd: &ColumnDescription,
    name: &str,
    mapping_options: MappingOptions,
    cursor: &mut impl Cursor,
    index: i16,
) -> Result<Option<Box<dyn ColumnFetchStrategy>>, Error> {
    let MappingOptions {
        db_name,
        use_utf16,
        prefer_varbinary,
        driver_does_support_i64,
    } = mapping_options;

    // Convert ODBC nullability to Parquet repetition. If the ODBC driver can not tell wether a
    // given column in the result may contain NULLs we assume it does.
    let repetition = match cd.nullability {
        Nullability::Nullable | Nullability::Unknown => Repetition::OPTIONAL,
        Nullability::NoNulls => Repetition::REQUIRED,
    };

    let is_optional = cd.could_be_nullable();

    let strategy: Box<dyn ColumnFetchStrategy> = match cd.data_type {
        DataType::Float { precision: 0..=24 } | DataType::Real => {
            fetch_identical::<FloatType>(is_optional)
        }
        // Map all precisions larger than 24 to double. Double would be technically precision 53.
        DataType::Float { precision: _ } => fetch_identical::<DoubleType>(is_optional),
        DataType::Double => fetch_identical::<DoubleType>(is_optional),
        DataType::SmallInt => {
            fetch_identical_with_converted_type::<Int32Type>(is_optional, ConvertedType::INT_16)
        }
        DataType::Integer => {
            fetch_identical_with_converted_type::<Int32Type>(is_optional, ConvertedType::INT_32)
        }
        DataType::Date => Box::new(Date::new(repetition)),
        DataType::Numeric { scale, precision } | DataType::Decimal { scale, precision } => {
            decmial_fetch_strategy(
                is_optional,
                scale as i32,
                precision,
                driver_does_support_i64,
            )
        }
        DataType::Timestamp { precision } => Box::new(Timestamp::new(repetition, precision.try_into().unwrap())),
        DataType::BigInt => fetch_identical::<Int64Type>(is_optional),
        DataType::Bit => Box::new(Boolean::new(repetition)),
        DataType::TinyInt => {
            fetch_identical_with_converted_type::<Int32Type>(is_optional, ConvertedType::INT_8)
        }
        DataType::Binary { length } => {
            if prefer_varbinary {
                Box::new(Binary::<ByteArrayType>::new(repetition, length))
            } else {
                Box::new(Binary::<FixedLenByteArrayType>::new(repetition, length))
            }
        }
        DataType::Varbinary { length } | DataType::LongVarbinary { length } => {
            Box::new(Binary::<ByteArrayType>::new(repetition, length))
        }
        // For character data we consider binding to wide (16-Bit) buffers in order to avoid
        // depending on the system locale being utf-8. For other character buffers we always use
        // narrow (8-Bit) buffers, since we expect decimals, timestamps and so on to always be
        // represented in ASCII characters.
        dt @ (DataType::Char { length: _ }
        | DataType::Varchar { length: _ }
        | DataType::WVarchar { length: _ }
        | DataType::LongVarchar { length: _ }
        | DataType::WChar { length: _ }) => {
            if use_utf16 {
                Box::new(Utf16ToUtf8::new(repetition, dt.utf16_len().unwrap()))
            } else {
                Box::new(Utf8::with_bytes_length(repetition, dt.utf8_len().unwrap()))
            }
        }
        DataType::Other {
            data_type: SqlDataType(-155),
            column_size: _,
            decimal_digits: precision,
        } => {
            if db_name == "Microsoft SQL Server" {
                // -155 is an indication for "Timestamp with timezone" on Microsoft SQL Server. We
                // give it special treatment so users can sort by time instead lexographically.
                timestamp_tz(precision as usize, repetition)?
            } else {
                unknown_non_char_type(cd, cursor, index, repetition)?
            }
        }
        DataType::Unknown | DataType::Time { .. } | DataType::Other { .. } => {
            unknown_non_char_type(cd, cursor, index, repetition)?
        }
    };

    debug!(
        "ODBC buffer description for column {}: {:?}",
        index,
        strategy.buffer_description()
    );

    if matches!(
        strategy.buffer_description().kind,
        BufferKind::Text { max_str_len: 0 } | BufferKind::WText { max_str_len: 0 }
    ) {
        warn!(
            "Ignoring column '{}' with index {}. Driver reported a display length of 0. \
              This can happen for types without a fixed size limit. If you feel this should be \
              supported open an issue (or PR) at \
              <https://github.com/pacman82/odbc2parquet/issues>.",
            name, index
        );

        Ok(None)
    } else {
        Ok(Some(strategy))
    }
}

fn unknown_non_char_type(
    cd: &ColumnDescription,
    cursor: &mut impl Cursor,
    index: i16,
    repetition: Repetition,
) -> Result<Box<Utf8>, Error> {
    let length = if let Some(len) = cd.data_type.utf8_len() {
        len
    } else {
        cursor.col_display_size(index.try_into().unwrap())? as usize
    };
    Ok(Box::new(Utf8::with_bytes_length(repetition, length)))
}
