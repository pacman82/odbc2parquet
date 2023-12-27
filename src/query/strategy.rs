use std::{cmp::min, convert::TryInto, num::NonZeroUsize};

use anyhow::{bail, Error};
use log::{debug, info};
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::SqlDataType,
    ColumnDescription, DataType, Nullability, Quirks, ResultSetMetadata,
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
    pub column_length_limit: Option<usize>,
    pub quirks: &'a Quirks,
}

/// Fetch strategies based on column description and enviroment arguments `MappingOptions`.
///
/// * `cd`: Description of the column for which we need to pick a fetch strategy
/// * `name`: Name of the column which we fetch
/// * `mapping_options`: Options describing the environment and desired outcome which are also
///   influencing the decision of what to pick.
/// * `cursor`: Used to query additional inforamtion about the columns, not contained in the initial
///   column description. Passing them here, allows us to query these only lazily then needed. ODBC
///   calls can be quite costly, although an argument could be made, that these times do not matter
///   within the runtime of the odbc2parquet command line tool.
/// * `index`: One based column index. Usefull if additional metainformation needs to be acquired
///   using `cursor`
pub fn strategy_from_column_description(
    cd: &ColumnDescription,
    name: &str,
    mapping_options: MappingOptions,
    cursor: &mut impl ResultSetMetadata,
    index: i16,
) -> Result<Box<dyn FetchStrategy>, Error> {
    let MappingOptions {
        db_name,
        use_utf16,
        prefer_varbinary,
        avoid_decimal,
        driver_does_support_i64,
        column_length_limit,
        quirks,
    } = mapping_options;

    // Convert ODBC nullability to Parquet repetition. If the ODBC driver can not tell wether a
    // given column in the result may contain NULLs we assume it does.
    let repetition = match cd.nullability {
        Nullability::Nullable | Nullability::Unknown => Repetition::OPTIONAL,
        Nullability::NoNulls => Repetition::REQUIRED,
    };

    let is_optional = cd.could_be_nullable();

    let apply_length_limit = |reported_length: Option<NonZeroUsize>| {
        match (reported_length, column_length_limit) {
            (None, None) => bail!(
                "Column '{}' with index {}. Driver reported a display length of 0. This can happen for \
                variadic types without a fixed upper bound. You can manually specify an upper bound \
                for variadic columns using the `--column-length-limit` command line argument.",
                name, index
            ),
            // No upper bound has been reported by the driver, so we use the one supplied by the
            // user.
            (None, Some(column_length_limit)) => Ok(column_length_limit),
            // Driver provided us with a length and no upper bound has been specified by the user.
            (Some(reported_length), None) => Ok(reported_length.get()),
            (Some(reported_length), Some(column_length_limit)) => {
                Ok(min(reported_length.get(), column_length_limit))
            }
        }
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
        DataType::TinyInt => {
            let is_signed = !cursor.column_is_unsigned(index.try_into().unwrap())?;
            fetch_identical_with_logical_type::<Int32Type>(
                is_optional,
                LogicalType::Integer {
                    bit_width: 8,
                    is_signed,
                },
            )
        }
        DataType::Binary { length } => {
            let length = apply_length_limit(length)?;
            if prefer_varbinary {
                Box::new(Binary::<ByteArrayType>::new(repetition, length))
            } else {
                Box::new(Binary::<FixedLenByteArrayType>::new(repetition, length))
            }
        }
        DataType::Varbinary { length } | DataType::LongVarbinary { length } => {
            let length = apply_length_limit(length)?;
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
            let len_in_chars = if use_utf16 {
                dt.utf16_len()
            } else {
                dt.utf8_len()
            };
            let length = apply_length_limit(len_in_chars)?;
            text_strategy(
                use_utf16,
                repetition,
                length,
                quirks.indicators_returned_from_bulk_fetch_are_memory_garbage,
            )
        }
        DataType::Other {
            data_type: SqlDataType(-154),
            column_size: _,
            decimal_digits: precision,
        } => {
            if db_name == "Microsoft SQL Server" {
                time_from_text(repetition, precision.try_into().unwrap())
            } else {
                unknown_non_char_type(
                    cd,
                    cursor,
                    index,
                    repetition,
                    apply_length_limit,
                    quirks.indicators_returned_from_bulk_fetch_are_memory_garbage,
                )?
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
                unknown_non_char_type(
                    cd,
                    cursor,
                    index,
                    repetition,
                    apply_length_limit,
                    quirks.indicators_returned_from_bulk_fetch_are_memory_garbage,
                )?
            }
        }
        DataType::Unknown | DataType::Time { .. } | DataType::Other { .. } => {
            unknown_non_char_type(
                cd,
                cursor,
                index,
                repetition,
                apply_length_limit,
                quirks.indicators_returned_from_bulk_fetch_are_memory_garbage,
            )?
        }
    };

    debug!(
        "ODBC buffer description for column {}: {:?}",
        index,
        strategy.buffer_desc()
    );

    Ok(strategy)
}

fn unknown_non_char_type(
    cd: &ColumnDescription,
    cursor: &mut impl ResultSetMetadata,
    index: i16,
    repetition: Repetition,
    apply_length_limit: impl FnOnce(Option<NonZeroUsize>) -> Result<usize, Error>,
    indicators_returned_from_bulk_fetch_are_memory_garbage: bool,
) -> Result<Box<dyn FetchStrategy>, Error> {
    let length = if let Some(len) = cd.data_type.utf8_len() {
        Some(len)
    } else {
        cursor.col_display_size(index.try_into().unwrap())?
    };
    let length = apply_length_limit(length)?;
    let use_utf16 = false;
    Ok(text_strategy(
        use_utf16,
        repetition,
        length,
        indicators_returned_from_bulk_fetch_are_memory_garbage,
    ))
}
