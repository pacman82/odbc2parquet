use std::{cmp::max, convert::TryInto};

use anyhow::Error;
use log::{debug, info, warn};
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::SqlDataType,
    ColumnDescription, Cursor, DataType, Nullability,
};
use parquet::{
    basic::{LogicalType, Repetition},
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
        identical::{fetch_identical, fetch_identical_with_logical_type},
        text::text_strategy,
        time::time_from_text,
        timestamp::timestamp_without_tz,
        timestamp_tz::timestamp_tz,
    },
};

/// Decisions on how to handle a particular column of the ODBC result set. What buffer to bind to it
/// for fetching, into what parquet type it is going to be translated and how to translate it from
/// the odbc buffer elements to afformentioned parquet type.
pub trait FetchStrategy {
    /// Parquet column type used in parquet schema
    fn parquet_type(&self, name: &str) -> Type;
    /// Description of the buffer bound to the ODBC data source.
    fn buffer_desc(&self) -> BufferDesc;
    /// copy the contents of an ODBC `AnySlice` into a Parquet `ColumnWriter`.
    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error>;
}

/// Controls how columns a queried and mapped onto parquet columns
#[derive(Clone, Copy)]
pub struct MappingOptions<'a> {
    pub db_name: &'a str,
    pub use_utf16: bool,
    pub prefer_varbinary: bool,
    pub avoid_decimal: bool,
    pub driver_does_support_i64: bool,
    pub column_length_limit: Option<usize>
}

pub fn strategy_from_column_description(
    cd: &ColumnDescription,
    name: &str,
    mapping_options: MappingOptions,
    cursor: &mut impl Cursor,
    index: i16,
) -> Result<Option<Box<dyn FetchStrategy>>, Error> {
    let MappingOptions {
        db_name,
        use_utf16,
        prefer_varbinary,
        avoid_decimal,
        driver_does_support_i64,
        column_length_limit,
    } = mapping_options;

    // Convert ODBC nullability to Parquet repetition. If the ODBC driver can not tell wether a
    // given column in the result may contain NULLs we assume it does.
    let repetition = match cd.nullability {
        Nullability::Nullable | Nullability::Unknown => Repetition::OPTIONAL,
        Nullability::NoNulls => Repetition::REQUIRED,
    };

    let is_optional = cd.could_be_nullable();

    let apply_length_limit = |length| {
        column_length_limit
            .map(|limit| max(limit, length))
            .unwrap_or(length)
    };

    let strategy: Box<dyn FetchStrategy> = match cd.data_type {
        DataType::Float { precision: 0..=24 } | DataType::Real => {
            fetch_identical::<FloatType>(is_optional)
        }
        // Map all precisions larger than 24 to double. Double would be technically precision 53.
        DataType::Float { precision: _ } => fetch_identical::<DoubleType>(is_optional),
        DataType::Double => fetch_identical::<DoubleType>(is_optional),
        DataType::SmallInt => fetch_identical_with_logical_type::<Int32Type>(
            is_optional,
            LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            },
        ),
        DataType::Integer => fetch_identical_with_logical_type::<Int32Type>(
            is_optional,
            LogicalType::Integer {
                bit_width: 32,
                is_signed: true,
            },
        ),
        DataType::Date => Box::new(Date::new(repetition)),
        DataType::Numeric { scale, precision } | DataType::Decimal { scale, precision } => {
            decmial_fetch_strategy(
                is_optional,
                scale as i32,
                precision.try_into().unwrap(),
                avoid_decimal,
                driver_does_support_i64,
            )
        }
        DataType::Timestamp { precision } => {
            timestamp_without_tz(repetition, precision.try_into().unwrap())
        }
        DataType::BigInt => fetch_identical::<Int64Type>(is_optional),
        DataType::Bit => Box::new(Boolean::new(repetition)),
        DataType::TinyInt => fetch_identical_with_logical_type::<Int32Type>(
            is_optional,
            LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            },
        ),
        DataType::Binary { length } => {
            let length = apply_length_limit(length);
            if prefer_varbinary {
                Box::new(Binary::<ByteArrayType>::new(repetition, length))
            } else {
                Box::new(Binary::<FixedLenByteArrayType>::new(repetition, length))
            }
        }
        DataType::Varbinary { length } | DataType::LongVarbinary { length } => {
            let length = apply_length_limit(length);
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
                let length = apply_length_limit(dt.utf16_len().unwrap());
                text_strategy(use_utf16, repetition, length)
            } else {
                let length = apply_length_limit(dt.utf8_len().unwrap());
                text_strategy(use_utf16, repetition, length)
            }
        }
        DataType::Other {
            data_type: SqlDataType(-154),
            column_size: _,
            decimal_digits: precision,
        } => {
            if db_name == "Microsoft SQL Server" {
                time_from_text(repetition, precision.try_into().unwrap())
            } else {
                unknown_non_char_type(cd, cursor, index, repetition, apply_length_limit)?
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
                info!(
                    "Detected Timestamp type with time zone. Appyling instant semantics for \
                    column {}.",
                    cd.name_to_string()?
                );
                timestamp_tz(precision.try_into().unwrap(), repetition)?
            } else {
                unknown_non_char_type(cd, cursor, index, repetition, apply_length_limit)?
            }
        }
        DataType::Unknown | DataType::Time { .. } | DataType::Other { .. } => {
            unknown_non_char_type(cd, cursor, index, repetition, apply_length_limit)?
        }
    };

    debug!(
        "ODBC buffer description for column {}: {:?}",
        index,
        strategy.buffer_desc()
    );

    if matches!(
        strategy.buffer_desc(),
        BufferDesc::Text { max_str_len: 0 } | BufferDesc::WText { max_str_len: 0 }
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
    apply_length_limit: impl Fn(usize) -> usize,
) -> Result<Box<dyn FetchStrategy>, Error> {
    let length = if let Some(len) = cd.data_type.utf8_len() {
        len
    } else {
        cursor.col_display_size(index.try_into().unwrap())? as usize
    };
    let length = apply_length_limit(length);
    let use_utf16 = false;
    Ok(text_strategy(use_utf16, repetition, length))
}
