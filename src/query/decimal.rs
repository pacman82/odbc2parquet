use std::{convert::TryInto, marker::PhantomData};

use anyhow::Error;
use atoi::FromRadix10Signed;
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    decimal_text_to_i128, DataType,
};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::{DataType as ParquetDataType, FixedLenByteArrayType, Int32Type, Int64Type},
    schema::types::Type,
};

use crate::parquet_buffer::{BufferedDataType, ParquetBuffer};

use super::{
    column_strategy::ColumnStrategy, identical::fetch_identical_with_logical_type, text::Utf8,
};

/// Choose how to fetch decimals from ODBC and store them in parquet
pub fn decimal_fetch_strategy(
    is_optional: bool,
    scale: i32,
    precision: u8,
    avoid_decimal: bool,
    driver_does_support_i64: bool,
) -> Box<dyn ColumnStrategy> {
    let repetition = if is_optional {
        Repetition::OPTIONAL
    } else {
        Repetition::REQUIRED
    };

    if avoid_decimal && scale != 0 {
        // Precision + sign and radix character
        let length = precision as usize + 2;
        return Box::new(Utf8::with_bytes_length(repetition, length));
    }

    match (precision, scale) {
        (0..=9, 0) => {
            let logical_type = if avoid_decimal {
                LogicalType::Integer {
                    bit_width: 32,
                    is_signed: true,
                }
            } else {
                LogicalType::Decimal {
                    scale: 0,
                    precision: precision as i32,
                }
            };
            // Values with scale 0 and precision <= 9 can be fetched as i32 from the ODBC and we can
            // use the same physical type to store them in parquet.
            fetch_identical_with_logical_type::<Int32Type>(is_optional, logical_type)
        }
        (0..=9, 1..=9) => {
            // As these values have a scale unequal to 0 we read them from the database as text, but
            // since their precision is <= 9 we will store them as i32 (physical parquet type)
            Box::new(DecimalTextToInteger::<Int32Type>::new(
                precision,
                scale,
                repetition,
                LogicalType::Decimal {
                    scale,
                    precision: precision as i32,
                },
            ))
        }
        (10..=18, 0) => {
            // Values with scale 0 and precision <= 18 can be fetched as i64 from the ODBC and we
            // can use the same physical type to store them in parquet. That is, if the database
            // does support fetching values as 64Bit integers.
            let logical_type = if avoid_decimal {
                LogicalType::Integer {
                    bit_width: 64,
                    is_signed: true,
                }
            } else {
                LogicalType::Decimal {
                    scale: 0,
                    precision: precision as i32,
                }
            };
            if driver_does_support_i64 {
                fetch_identical_with_logical_type::<Int64Type>(is_optional, logical_type)
            } else {
                let logical_type = if avoid_decimal {
                    LogicalType::Integer {
                        bit_width: 64,
                        is_signed: true,
                    }
                } else {
                    LogicalType::Decimal {
                        scale,
                        precision: precision as i32,
                    }
                };
                // The database does not support 64Bit integers (looking at you Oracle). So we fetch
                // the values from the database as text and convert them into 64Bit integers.
                Box::new(DecimalTextToInteger::<Int64Type>::new(
                    precision,
                    0,
                    repetition,
                    logical_type,
                ))
            }
        }
        (10..=18, 1..=18) => {
            // As these values have a scale unequal to 0 we read them from the database as text, but
            // since their precision is <= 18 we will store them as i64 (physical parquet type)
            Box::new(DecimalTextToInteger::<Int64Type>::new(
                precision,
                scale,
                repetition,
                LogicalType::Decimal {
                    scale,
                    precision: precision as i32,
                },
            ))
        }
        (0..=38, _) => Box::new(DecimalAsBinary::new(repetition, scale, precision)),
        (_, _) => {
            let length = odbc_api::DataType::Decimal {
                precision: precision as usize,
                scale: scale.try_into().unwrap(),
            }
            .display_size()
            .unwrap();
            Box::new(Utf8::with_bytes_length(repetition, length.get()))
        }
    }
}

struct DecimalTextToInteger<Pdt> {
    precision: u8,
    scale: i32,
    repetition: Repetition,
    logical_type: LogicalType,
    _pdt: PhantomData<Pdt>,
}

impl<Pdt> DecimalTextToInteger<Pdt> {
    fn new(precision: u8, scale: i32, repetition: Repetition, logical_type: LogicalType) -> Self {
        Self {
            precision,
            scale,
            repetition,
            logical_type,
            _pdt: PhantomData,
        }
    }
}

impl<Pdt> ColumnStrategy for DecimalTextToInteger<Pdt>
where
    Pdt: ParquetDataType,
    Pdt::T: FromRadix10Signed + BufferedDataType,
{
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, Pdt::get_physical_type())
            .with_logical_type(Some(self.logical_type.clone()))
            .with_precision(self.precision as i32)
            .with_scale(self.scale)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        // Since we cannot assume scale to be zero we fetch these decimal as text

        // Precision + 2. (One byte for the radix character and another for the sign)
        let max_str_len = DataType::Decimal {
            precision: self.precision as usize,
            scale: self.scale.try_into().unwrap(),
        }
        .display_size()
        .unwrap()
        .get();
        BufferDesc::Text { max_str_len }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        // This vec is going to hold the digits with sign and decimal point. It is
        // allocated once and reused for each value.
        let mut digits: Vec<u8> = Vec::with_capacity(self.precision as usize + 2);

        let column_writer = Pdt::get_column_writer_mut(column_writer).unwrap();
        let view = column_view.as_text_view().expect(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues.",
        );
        parquet_buffer.write_optional(
            column_writer,
            view.iter().map(|value| {
                value.map(|text| {
                    digits.clear();
                    digits.extend(text.iter().filter(|&&c| c != b'.'));
                    Pdt::T::from_radix_10_signed(&digits).0
                })
            }),
        )
    }
}

/// Strategy for fetching decimal values which can not be represented as either 32Bit or 64Bit
struct DecimalAsBinary {
    repetition: Repetition,
    scale: i32,
    precision: u8,
    length_in_bytes: usize,
}

impl DecimalAsBinary {
    pub fn new(repetition: Repetition, scale: i32, precision: u8) -> Self {
        // Length of the two's complement.
        let num_binary_digits = precision as f64 * 10f64.log2();
        // Plus one bit for the sign (+/-)
        let length_in_bits = num_binary_digits + 1.0;
        let length_in_bytes = (length_in_bits / 8.0).ceil() as usize;

        Self {
            repetition,
            scale,
            precision,
            length_in_bytes,
        }
    }
}

impl ColumnStrategy for DecimalAsBinary {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(self.length_in_bytes.try_into().unwrap())
            .with_logical_type(Some(LogicalType::Decimal {
                scale: self.scale,
                precision: self.precision as i32,
            }))
            .with_precision(self.precision.into())
            .with_scale(self.scale)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_desc(&self) -> BufferDesc {
        // Precision + 2. (One byte for the radix character and another for the sign)
        let max_str_len = DataType::Decimal {
            precision: self.precision as usize,
            scale: self.scale.try_into().unwrap(),
        }
        .display_size()
        .unwrap()
        .get();
        BufferDesc::Text { max_str_len }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnySlice,
    ) -> Result<(), Error> {
        write_decimal_col(
            parquet_buffer,
            column_writer,
            column_view,
            self.length_in_bytes,
            self.scale,
        )
    }
}

fn write_decimal_col(
    parquet_buffer: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnySlice,
    length_in_bytes: usize,
    scale: i32,
) -> Result<(), Error> {
    let column_writer = FixedLenByteArrayType::get_column_writer_mut(column_writer).unwrap();
    let view = column_reader.as_text_view().expect(
        "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
        https://github.com/pacman82/odbc2parquet/issues.",
    );

    let scale = scale as usize;

    parquet_buffer.write_twos_complement_i128(
        column_writer,
        view.iter()
            .map(|field| field.map(|text| decimal_text_to_i128(text, scale))),
        length_in_bytes,
    )?;

    Ok(())
}
