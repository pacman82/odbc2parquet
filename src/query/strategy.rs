use std::{borrow::Cow, convert::TryInto};

use anyhow::Error;
use log::{debug, warn};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
    sys::Date,
    Bit, ColumnDescription, Cursor, DataType, Nullability,
};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::{get_typed_column_writer_mut, ColumnWriter},
    data_type::{
        BoolType, ByteArrayType, DataType as ParquetDataType, DoubleType, FixedLenByteArrayType,
        FloatType, Int32Type, Int64Type,
    },
    schema::types::Type,
};

use super::odbc_buffer_item::OdbcBufferItem;
use crate::parquet_buffer::{BufferedDataType, IntoPhysical, ParquetBuffer};

/// Function used to copy the contents of `AnyColumnView` into `ColumnWriter`. The concrete instance
/// (not this signature) is dependent on the specific columns in questions.
pub type FnWriteParquetColumn =
    dyn Fn(&mut ParquetBuffer, &mut ColumnWriter, AnyColumnView) -> Result<(), Error>;

macro_rules! optional_col_writer {
    ($pdt:ident, $cr_variant:ident) => {
        Box::new(
            move |pb: &mut ParquetBuffer,
                  column_writer: &mut ColumnWriter,
                  column_reader: AnyColumnView| {
                let cw = get_typed_column_writer_mut::<$pdt>(column_writer);
                if let AnyColumnView::$cr_variant(it) = column_reader {
                    pb.write_optional(cw, it)?
                } else {
                    panic_invalid_cv()
                }
                Ok(())
            },
        )
    };
}

pub trait ColumnFetchStrategy {
    fn parquet_type(&self) -> Type;
    fn buffer_description(&self) -> BufferDescription;
    fn odbc_to_parquet(&self) -> &Box<FnWriteParquetColumn>;
}

impl ColumnFetchStrategy for ColumnFetchStrategyImpl {
    fn parquet_type(&self) -> Type {
        self.parquet_type.clone()
    }

    fn buffer_description(&self) -> BufferDescription {
        self.buffer_description
    }

    fn odbc_to_parquet(&self) -> &Box<FnWriteParquetColumn> {
        &self.odbc_to_parquet
    }
}

/// Decisions on how to handle a particular column of the ODBC result set. What buffer to bind to it
/// for fetching, into what parquet type it is going to be translated and how to translate it from
/// the odbc buffer elements to afformentioned parquet type.
struct ColumnFetchStrategyImpl {
    /// Parquet column type used in parquet schema
    parquet_type: Type,
    /// Description of the buffer bound to the ODBC data source.
    buffer_description: BufferDescription,
    /// Function writing the data from an ODBC buffer with a parquet column writer.
    odbc_to_parquet: Box<FnWriteParquetColumn>,
}

impl ColumnFetchStrategyImpl {
    /// Columnar fetch strategy to be applied if Parquet and Odbc value type are binary identical.
    /// Generic argument is a parquet data type.
    fn trivial<Pdt>(name: &str, nullability: Nullability) -> Self
    where
        Pdt: ParquetDataType,
        Pdt::T: OdbcBufferItem + BufferedDataType,
    {
        Self::with_converted_type::<Pdt>(name, nullability, ConvertedType::NONE)
    }

    fn with_converted_type<Pdt>(
        name: &str,
        nullability: Nullability,
        converted_type: ConvertedType,
    ) -> Self
    where
        Pdt: ParquetDataType,
        Pdt::T: OdbcBufferItem + BufferedDataType,
    {
        Self::with_conversion::<Pdt, Pdt::T>(name, nullability, converted_type)
    }

    fn with_conversion<Pdt, Obt>(
        name: &str,
        nullability: Nullability,
        converted_type: ConvertedType,
    ) -> Self
    where
        Pdt: ParquetDataType,
        Pdt::T: BufferedDataType,
        Obt: OdbcBufferItem,
        for<'a> &'a Obt: IntoPhysical<Pdt::T>,
    {
        Self::new::<Pdt, Obt>(name, nullability, converted_type, None, None)
    }

    fn new<Pdt, Obt>(
        name: &str,
        nullability: Nullability,
        converted_type: ConvertedType,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> Self
    where
        Pdt: ParquetDataType,
        Pdt::T: BufferedDataType,
        Obt: OdbcBufferItem,
        for<'a> &'a Obt: IntoPhysical<Pdt::T>,
    {
        let physical_type = Pdt::get_physical_type();
        let mut field_builder =
            Type::primitive_type_builder(name, physical_type).with_converted_type(converted_type);
        if let Some(p) = precision {
            field_builder = field_builder.with_precision(p);
        }

        if let Some(s) = scale {
            field_builder = field_builder.with_scale(s);
        }

        let odbc_to_parquet = Box::new(
            move |pb: &mut ParquetBuffer,
                  column_writer: &mut ColumnWriter,
                  column_reader: AnyColumnView| {
                let cw = get_typed_column_writer_mut::<Pdt>(column_writer);
                let it = Obt::nullable_buffer(column_reader);
                pb.write_optional(cw, it)?;
                Ok(())
            },
        );

        let buffer_description = BufferDescription {
            kind: Obt::BUFFER_KIND,
            nullable: true,
        };

        let repetition = to_repetition(nullability);

        let parquet_type = field_builder.with_repetition(repetition).build().unwrap();

        ColumnFetchStrategyImpl {
            parquet_type,
            buffer_description,
            odbc_to_parquet,
        }
    }
}

/// Convert ODBC nullability to Parquet repetition. If the ODBC driver can not tell wether a given
/// column in the result may contain NULLs we assume it does.
fn to_repetition(nullability: Nullability) -> Repetition {
    let repetition = match nullability {
        Nullability::Nullable | Nullability::Unknown => Repetition::OPTIONAL,
        Nullability::NoNulls => Repetition::REQUIRED,
    };
    repetition
}

pub fn strategy_from_column_description(
    cd: &ColumnDescription,
    name: &str,
    prefer_varbinary: bool,
    use_utf16: bool,
    cursor: &impl Cursor,
    index: i16,
) -> Result<Option<Box<dyn ColumnFetchStrategy>>, Error> {
    let ptb = |physical_type| Type::primitive_type_builder(name, physical_type);

    let strategy = match cd.data_type {
        DataType::Double => Box::new(ColumnFetchStrategyImpl::trivial::<DoubleType>(
            name,
            cd.nullability,
        )),
        DataType::Float | DataType::Real => Box::new(
            ColumnFetchStrategyImpl::trivial::<FloatType>(name, cd.nullability),
        ),
        DataType::SmallInt => Box::new(ColumnFetchStrategyImpl::with_converted_type::<Int32Type>(
            name,
            cd.nullability,
            ConvertedType::INT_16,
        )),
        DataType::Integer => Box::new(ColumnFetchStrategyImpl::with_converted_type::<Int32Type>(
            name,
            cd.nullability,
            ConvertedType::INT_32,
        )),
        DataType::Date => Box::new(ColumnFetchStrategyImpl::with_conversion::<Int32Type, Date>(
            name,
            cd.nullability,
            ConvertedType::DATE,
        )),
        DataType::Decimal {
            scale: 0,
            precision: p @ 0..=9,
        }
        | DataType::Numeric {
            scale: 0,
            precision: p @ 0..=9,
        } => Box::new(ColumnFetchStrategyImpl::new::<Int32Type, i32>(
            name,
            cd.nullability,
            ConvertedType::DECIMAL,
            Some(p as i32),
            Some(0),
        )),
        DataType::Decimal {
            scale: 0,
            precision: p @ 0..=18,
        }
        | DataType::Numeric {
            scale: 0,
            precision: p @ 0..=18,
        } => Box::new(ColumnFetchStrategyImpl::new::<Int64Type, i64>(
            name,
            cd.nullability,
            ConvertedType::DECIMAL,
            Some(p as i32),
            Some(0),
        )),
        DataType::Numeric { scale, precision } | DataType::Decimal { scale, precision } => {
            // Length of the two's complement.
            let num_binary_digits = precision as f64 * 10f64.log2();
            // Plus one bit for the sign (+/-)
            let length_in_bits = num_binary_digits + 1.0;
            let length_in_bytes = (length_in_bits / 8.0).ceil() as i32;
            let parquet_type = ptb(PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(length_in_bytes)
                .with_converted_type(ConvertedType::DECIMAL)
                .with_precision(precision.try_into().unwrap())
                .with_scale(scale.try_into().unwrap())
                .with_repetition(to_repetition(cd.nullability))
                .build()
                .unwrap();
            let buffer_description = BufferDescription {
                kind: BufferKind::Text {
                    max_str_len: cd.data_type.column_size(),
                },
                nullable: true,
            };
            let odbc_to_parquet = Box::new(
                move |pb: &mut ParquetBuffer,
                      column_writer: &mut ColumnWriter,
                      column_reader: AnyColumnView| {
                    write_decimal_col(
                        pb,
                        column_writer,
                        column_reader,
                        length_in_bytes.try_into().unwrap(),
                        precision,
                    )
                },
            );
            Box::new(ColumnFetchStrategyImpl {
                parquet_type,
                buffer_description,
                odbc_to_parquet,
            })
        }
        DataType::Timestamp { precision } => {
            let parquet_type = ptb(PhysicalType::INT64)
                .with_converted_type(if precision <= 3 {
                    ConvertedType::TIMESTAMP_MILLIS
                } else {
                    ConvertedType::TIMESTAMP_MICROS
                })
                .with_repetition(to_repetition(cd.nullability))
                .build()
                .unwrap();
            let buffer_description = BufferDescription {
                kind: BufferKind::Timestamp,
                nullable: true,
            };
            let odbc_to_parquet = Box::new(
                move |pb: &mut ParquetBuffer,
                      column_writer: &mut ColumnWriter,
                      column_reader: AnyColumnView| {
                    write_timestamp_col(pb, column_writer, column_reader, precision)
                },
            );
            Box::new(ColumnFetchStrategyImpl {
                parquet_type,
                buffer_description,
                odbc_to_parquet,
            })
        }
        DataType::BigInt => Box::new(ColumnFetchStrategyImpl::trivial::<Int64Type>(
            name,
            cd.nullability,
        )),
        DataType::Bit => Box::new(ColumnFetchStrategyImpl::with_conversion::<BoolType, Bit>(
            name,
            cd.nullability,
            ConvertedType::NONE,
        )),
        DataType::TinyInt => Box::new(ColumnFetchStrategyImpl::with_converted_type::<Int32Type>(
            name,
            cd.nullability,
            ConvertedType::INT_8,
        )),
        DataType::Binary { length } => {
            if prefer_varbinary {
                Box::new(ColumnFetchStrategyImpl {
                    parquet_type: ptb(PhysicalType::BYTE_ARRAY)
                        .with_converted_type(ConvertedType::NONE)
                        .with_repetition(to_repetition(cd.nullability))
                        .build()
                        .unwrap(),
                    buffer_description: BufferDescription {
                        kind: BufferKind::Binary { length },
                        nullable: true,
                    },
                    odbc_to_parquet: optional_col_writer!(ByteArrayType, Binary),
                })
            } else {
                Box::new(ColumnFetchStrategyImpl {
                    parquet_type: ptb(PhysicalType::FIXED_LEN_BYTE_ARRAY)
                        .with_length(length.try_into().unwrap())
                        .with_converted_type(ConvertedType::NONE)
                        .with_repetition(to_repetition(cd.nullability))
                        .build()
                        .unwrap(),
                    buffer_description: BufferDescription {
                        kind: BufferKind::Binary { length },
                        nullable: true,
                    },
                    odbc_to_parquet: optional_col_writer!(FixedLenByteArrayType, Binary),
                })
            }
        }
        DataType::Varbinary { length } | DataType::LongVarbinary { length } => {
            Box::new(ColumnFetchStrategyImpl {
                parquet_type: ptb(PhysicalType::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::NONE)
                    .with_repetition(to_repetition(cd.nullability))
                    .build()
                    .unwrap(),
                buffer_description: BufferDescription {
                    kind: BufferKind::Binary { length },
                    nullable: true,
                },
                odbc_to_parquet: optional_col_writer!(ByteArrayType, Binary),
            })
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
                Box::new(ColumnFetchStrategyImpl {
                    parquet_type: ptb(PhysicalType::BYTE_ARRAY)
                        .with_converted_type(ConvertedType::UTF8)
                        .with_repetition(to_repetition(cd.nullability))
                        .build()
                        .unwrap(),
                    buffer_description: BufferDescription {
                        kind: BufferKind::WText {
                            // One UTF-16 code point may consist of up to two bytes.
                            max_str_len: length * 2,
                        },
                        nullable: true,
                    },
                    odbc_to_parquet: Box::new(write_utf16_to_utf8),
                })
            } else {
                Box::new(ColumnFetchStrategyImpl {
                    parquet_type: ptb(PhysicalType::BYTE_ARRAY)
                        .with_converted_type(ConvertedType::UTF8)
                        .with_repetition(to_repetition(cd.nullability))
                        .build()
                        .unwrap(),
                    buffer_description: BufferDescription {
                        kind: BufferKind::Text {
                            // One UTF-8 code point may consist of up to four bytes.
                            max_str_len: length * 4,
                        },
                        nullable: true,
                    },
                    odbc_to_parquet: Box::new(write_utf8),
                })
            }
        }
        DataType::Unknown | DataType::Time { .. } | DataType::Other { .. } => {
            let max_str_len = if let Some(len) = cd.data_type.utf8_len() {
                len
            } else {
                cursor.col_display_size(index.try_into().unwrap())? as usize
            };
            Box::new(ColumnFetchStrategyImpl {
                parquet_type: ptb(PhysicalType::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::UTF8)
                    .with_repetition(to_repetition(cd.nullability))
                    .build()
                    .unwrap(),
                buffer_description: BufferDescription {
                    kind: BufferKind::Text { max_str_len },
                    nullable: true,
                },
                odbc_to_parquet: Box::new(write_utf8),
            })
        }
    };

    debug!(
        "ODBC buffer description for column {}: {:?}",
        index, strategy.buffer_description
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

fn write_decimal_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    length_in_bytes: usize,
    precision: usize,
) -> Result<(), Error> {
    if let (ColumnWriter::FixedLenByteArrayColumnWriter(cw), AnyColumnView::Text(it)) =
        (column_writer, column_reader)
    {
        pb.write_decimal(cw, it, length_in_bytes, precision)?;
    } else {
        panic_invalid_cv()
    }
    Ok(())
}

fn write_timestamp_col(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    precision: i16,
) -> Result<(), Error> {
    if let (ColumnWriter::Int64ColumnWriter(cw), AnyColumnView::NullableTimestamp(it)) =
        (column_writer, column_reader)
    {
        pb.write_timestamp(cw, it, precision)?;
    } else {
        panic_invalid_cv()
    }
    Ok(())
}

fn write_utf16_to_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::WText(it)) =
        (column_writer, column_reader)
    {
        pb.write_optional(
            cw,
            it.map(|item| {
                item.map(|ustr| {
                    ustr.to_string()
                        .expect("Data source must return valid UTF16 in wide character buffer")
                })
            }),
        )?;
    } else {
        panic_invalid_cv()
    }
    Ok(())
}

fn write_utf8(
    pb: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
) -> Result<(), Error> {
    if let (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::Text(it)) =
        (column_writer, column_reader)
    {
        pb.write_optional(cw, it.map(|item| item.map(bytes_to_string)))?;
    } else {
        panic_invalid_cv()
    }
    Ok(())
}

fn panic_invalid_cv() -> ! {
    panic!(
        "Invalid Column view type. This is not supposed to happen. Please \
        open a Bug at https://github.com/pacman82/odbc2parquet/issues."
    )
}

fn bytes_to_string(bytes: &[u8]) -> String {
    // Allocate string into a ByteArray and make sure it is all UTF-8 characters
    let utf8_str = String::from_utf8_lossy(bytes);
    // We need to allocate the string anyway to create a ByteArray (yikes!), yet if it already
    // happened after the to_string_lossy method, it implies we had to use a replacement
    // character!
    if matches!(utf8_str, Cow::Owned(_)) {
        warn!(
            "Non UTF-8 characters found in string. Try to execute odbc2parquet in a shell with \
        UTF-8 locale or try specifying `--encoding Utf16` on the command line. Value: {}",
            utf8_str
        );
    }
    utf8_str.into_owned()
}
