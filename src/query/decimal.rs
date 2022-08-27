use std::convert::TryInto;

use anyhow::Error;
use atoi::FromRadix10Signed;
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
    DataType,
};
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::{DataType as _, FixedLenByteArrayType, Int32Type, Int64Type},
    schema::types::Type,
};

use crate::parquet_buffer::ParquetBuffer;

use super::{
    identical::fetch_decimal_as_identical_with_precision, strategy::ColumnFetchStrategy, text::Utf8,
};

/// Choose how to fetch decimals from ODBC and store them in parquet
pub fn decmial_fetch_strategy(
    is_optional: bool,
    scale: i32,
    precision: usize,
    driver_does_support_i64: bool,
) -> Box<dyn ColumnFetchStrategy> {
    let repetition = if is_optional {
        Repetition::OPTIONAL
    } else {
        Repetition::REQUIRED
    };

    match (precision, scale) {
        (0..=9, 0) => {
            // Values with scale 0 and precision <= 9 can be fetched as i32 from the ODBC and we can
            // use the same physical type to store them in parquet.
            fetch_decimal_as_identical_with_precision::<Int32Type>(is_optional, precision as i32)
        }
        (0..=9, 1..=9) => {
            // As these values have a scale unequal to 0 we read them from the datebase as text, but
            // since their precision is <= 9 we will store them as i32 (physical parquet type)
            Box::new(DecimalAsI32::new(precision, scale, repetition))
        }
        (10..=18, 0) => {
            // Values with scale 0 and precision <= 18 can be fetched as i64 from the ODBC and we
            // can use the same physical type to store them in parquet. That is, if the database
            // does support fetching values as 64Bit integers.
            if driver_does_support_i64 {
                fetch_decimal_as_identical_with_precision::<Int64Type>(
                    is_optional,
                    precision as i32,
                )
            } else {
                // The database does not support 64Bit integers (looking at you Oracle). So we fetch
                // the values from the database as text and convert them into 64Bit integers.
                Box::new(TextAsI64::decimal(is_optional, precision as i32))
            }
        }
        (0..=38, _) => Box::new(DecimalAsBinary::new(repetition, scale, precision)),
        (_, _) => {
            let length = odbc_api::DataType::Decimal {
                precision,
                scale: scale.try_into().unwrap(),
            }
            .display_size()
            .unwrap();
            Box::new(Utf8::with_bytes_length(repetition, length))
        }
    }
}

struct DecimalAsI32 {
    precision: usize,
    scale: i32,
    repetition: Repetition,
}

impl DecimalAsI32 {
    fn new(precision: usize, scale: i32, repetition: Repetition) -> Self {
        Self {
            precision,
            scale,
            repetition,
        }
    }
}

impl ColumnFetchStrategy for DecimalAsI32 {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(self.precision as i32)
            .with_scale(self.scale)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        // Since we cannot assume scale to be zero we fetch these decimal as text

        // Precision + 2. (One byte for the radix character and another for the sign)
        let max_str_len = DataType::Decimal {
            precision: self.precision,
            scale: self.scale.try_into().unwrap(),
        }
        .display_size()
        .unwrap();
        BufferDescription {
            nullable: self.repetition == Repetition::OPTIONAL,
            kind: BufferKind::Text { max_str_len },
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        // This vec is going to hold the digits with sign and decimal point. It is
        // allocated once and reused for each value.
        let mut digits: Vec<u8> = Vec::with_capacity(self.precision + 2);

        let column_writer = Int32Type::get_column_writer_mut(column_writer).unwrap();
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
                    i32::from_radix_10_signed(&digits).0
                })
            }),
        )
    }
}

/// Strategy for fetching decimal values which can not be represented as either 32Bit or 64Bit
struct DecimalAsBinary {
    repetition: Repetition,
    scale: i32,
    precision: usize,
    length_in_bytes: usize,
}

impl DecimalAsBinary {
    pub fn new(repetition: Repetition, scale: i32, precision: usize) -> Self {
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

impl ColumnFetchStrategy for DecimalAsBinary {
    fn parquet_type(&self, name: &str) -> Type {
        Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(self.length_in_bytes.try_into().unwrap())
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(self.precision.try_into().unwrap())
            .with_scale(self.scale)
            .with_repetition(self.repetition)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> odbc_api::buffers::BufferDescription {
        // Precision + 2. (One byte for the radix character and another for the sign)
        let max_str_len = DataType::Decimal {
            precision: self.precision,
            scale: self.scale.try_into().unwrap(),
        }
        .display_size()
        .unwrap();
        BufferDescription {
            kind: BufferKind::Text { max_str_len },
            nullable: true,
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        write_decimal_col(
            parquet_buffer,
            column_writer,
            column_view,
            self.length_in_bytes,
            self.precision,
        )
    }
}

fn write_decimal_col(
    parquet_buffer: &mut ParquetBuffer,
    column_writer: &mut ColumnWriter,
    column_reader: AnyColumnView,
    length_in_bytes: usize,
    precision: usize,
) -> Result<(), Error> {
    // This vec is going to hold the digits with sign, but without the decimal point. It is
    // allocated once and reused for each value.
    let mut digits: Vec<u8> = Vec::with_capacity(precision + 1);

    let column_writer = FixedLenByteArrayType::get_column_writer_mut(column_writer).unwrap();
    let view = column_reader.as_text_view().expect(
        "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
        https://github.com/pacman82/odbc2parquet/issues.",
    );

    parquet_buffer.write_twos_complement_i128(
        column_writer,
        view.iter().map(|field| {
            field.map(|text| {
                digits.clear();
                digits.extend(text.iter().filter(|&&c| c != b'.'));
                let (num, _consumed) = i128::from_radix_10_signed(&digits);
                num
            })
        }),
        length_in_bytes,
    )?;

    Ok(())
}

/// Query a column as text and write it as 64 Bit integer.
struct TextAsI64 {
    /// `true` if NULL is allowed, `false` otherwise
    is_optional: bool,
    /// Maximum total number of digits in the decimal
    precision: i32,
}

impl TextAsI64 {
    /// Converted type is decimal
    pub fn decimal(is_optional: bool, precision: i32) -> Self {
        Self {
            is_optional,
            precision,
        }
    }
}

impl ColumnFetchStrategy for TextAsI64 {
    fn parquet_type(&self, name: &str) -> Type {
        let repetition = if self.is_optional {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        };
        let physical_type = Int64Type::get_physical_type();

        Type::primitive_type_builder(name, physical_type)
            .with_repetition(repetition)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_precision(self.precision)
            .with_scale(0)
            .build()
            .unwrap()
    }

    fn buffer_description(&self) -> BufferDescription {
        // +1 not for terminating zero, but for the sign charactor like `-` or `+`. Also one
        // additional space for the radix character
        let max_str_len = odbc_api::DataType::Decimal {
            precision: self.precision.try_into().unwrap(),
            scale: 0,
        }
        .display_size()
        .unwrap();
        BufferDescription {
            nullable: self.is_optional,
            kind: BufferKind::Text { max_str_len },
        }
    }

    fn copy_odbc_to_parquet(
        &self,
        parquet_buffer: &mut ParquetBuffer,
        column_writer: &mut ColumnWriter,
        column_view: AnyColumnView,
    ) -> Result<(), Error> {
        let column_writer = Int64Type::get_column_writer_mut(column_writer).unwrap();
        let view = column_view.as_text_view().expect(
            "Invalid Column view type. This is not supposed to happen. Please open a Bug at \
            https://github.com/pacman82/odbc2parquet/issues.",
        );
        parquet_buffer.write_optional(
            column_writer,
            view.iter()
                .map(|value| value.map(|text| i64::from_radix_10_signed(text).0)),
        )?;
        Ok(())
    }
}
