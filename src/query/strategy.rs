use std::convert::TryInto;

use anyhow::Error;
use log::{debug, warn};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
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
        decimal::Decimal,
        identical::{
            fetch_decimal_as_identical_with_precision, fetch_identical,
            fetch_identical_with_converted_type,
        },
        text::{Utf16ToUtf8, Utf8},
        timestamp::Timestamp,
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

pub fn strategy_from_column_description(
    cd: &ColumnDescription,
    name: &str,
    prefer_varbinary: bool,
    use_utf16: bool,
    cursor: &impl Cursor,
    index: i16,
) -> Result<Option<Box<dyn ColumnFetchStrategy>>, Error> {
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
        DataType::Decimal {
            scale: 0,
            precision: p @ 0..=9,
        }
        | DataType::Numeric {
            scale: 0,
            precision: p @ 0..=9,
        } => fetch_decimal_as_identical_with_precision::<Int32Type>(is_optional, p as i32),
        DataType::Decimal {
            scale: 0,
            precision: p @ 0..=18,
        }
        | DataType::Numeric {
            scale: 0,
            precision: p @ 0..=18,
        } => fetch_decimal_as_identical_with_precision::<Int64Type>(is_optional, p as i32),
        DataType::Numeric { scale, precision } | DataType::Decimal { scale, precision } => {
            Box::new(Decimal::new(repetition, scale as i32, precision))
        }
        DataType::Timestamp { precision } => Box::new(Timestamp::new(repetition, precision)),
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
        DataType::Char { length }
        | DataType::Varchar { length }
        | DataType::WVarchar { length }
        | DataType::LongVarchar { length }
        | DataType::WChar { length } => {
            if use_utf16 {
                Box::new(Utf16ToUtf8::new(repetition, length))
            } else {
                Box::new(Utf8::with_char_length(repetition, length))
            }
        }
        DataType::Unknown | DataType::Time { .. } | DataType::Other { .. } => {
            let length = if let Some(len) = cd.data_type.utf8_len() {
                len
            } else {
                cursor.col_display_size(index.try_into().unwrap())? as usize
            };
            // We assume the string representation of non character types to consist entirely of
            // ASCII characters.
            Box::new(Utf8::with_bytes_length(repetition, length))
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
