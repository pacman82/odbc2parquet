use std::{fs::File, rc::Rc, convert::TryInto};

use anyhow::Error;
use log::{debug, info};
use odbc_api::{ColumnDescription, Cursor, DataType, Environment, IntoParameter, Nullable};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};

use crate::{QueryOpt, odbc_buffer::{ColumnBufferDescription, OdbcBuffer}, open_connection, parquet_buffer::ParquetBuffer};

/// Execute a query and writes the result to parquet.
pub fn query(environment: &Environment, opt: &QueryOpt) -> Result<(), Error> {
    let QueryOpt {
        connect_opts,
        output,
        parameters,
        query,
        batch_size,
    } = opt;

    // Convert the input strings into parameters suitable to for use with ODBC.
    let params: Vec<_> = parameters
        .iter()
        .map(|param| param.into_parameter())
        .collect();

    let odbc_conn = open_connection(&environment, connect_opts)?;

    if let Some(cursor) = odbc_conn.execute(query, params.as_slice())? {
        let file = File::create(output)?;
        cursor_to_parquet(cursor, file, *batch_size as usize)?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

fn cursor_to_parquet(cursor: impl Cursor, file: File, batch_size: usize) -> Result<(), Error> {
    info!("Batch size set to {}", batch_size);
    // Write properties
    let wpb = WriterProperties::builder();
    let properties = Rc::new(wpb.build());

    let (parquet_schema, buffer_description) = make_schema(&cursor)?;
    let mut writer = SerializedFileWriter::new(file, parquet_schema, properties)?;
    let mut odbc_buffer = OdbcBuffer::new(batch_size, buffer_description.iter().copied());
    let mut row_set_cursor = cursor.bind_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size);
    let mut num_batch = 0;

    while let Some(buffer) = row_set_cursor.fetch()? {
        let mut row_group_writer = writer.next_row_group()?;
        let mut col_index = 0;
        num_batch += 1;
        let num_rows = buffer.num_rows_fetched() as usize;
        info!("Fetched batch {} with {} rows.", num_batch, num_rows);
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            pb.set_num_rows_fetched(num_rows);
            match &mut column_writer {
                ColumnWriter::BoolColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.bool_it(col_index))?;
                }
                ColumnWriter::Int32ColumnWriter(cw) => match buffer_description[col_index] {
                    ColumnBufferDescription::Date => {
                        pb.write_optional(cw, buffer.date_it(col_index))?
                    }
                    ColumnBufferDescription::I32 => {
                        // Used for Ints and Decimal logical types
                        pb.write_optional(cw, buffer.i32_it(col_index))?
                    }
                    _ => panic!("Mismatched ODBC and Parquet buffer type."),
                },
                ColumnWriter::Int64ColumnWriter(cw) => match buffer_description[col_index] {
                    ColumnBufferDescription::Timestamp => {
                        pb.write_optional(cw, buffer.timestamp_it(col_index))?
                    }
                    ColumnBufferDescription::I64 => {
                        // Used for Ints and Decimal logical types
                        pb.write_optional(cw, buffer.i64_it(col_index))?
                    }
                    _ => panic!("Mismatched ODBC and Parquet buffer type."),
                },
                // parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => {}
                ColumnWriter::FloatColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.f32_it(col_index))?;
                }
                ColumnWriter::DoubleColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.f64_it(col_index))?;
                }
                ColumnWriter::ByteArrayColumnWriter(cw) => {
                    pb.write_optional(cw, buffer.text_column_it(col_index))?;
                }
                // parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(_) => {}
                _ => panic!("Invalid ColumnWriter type"),
            }
            row_group_writer.close_column(column_writer)?;
            col_index += 1;
        }
        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;

    Ok(())
}

fn make_schema(cursor: &impl Cursor) -> Result<(Rc<Type>, Vec<ColumnBufferDescription>), Error> {
    let num_cols = cursor.num_result_cols()?;

    let mut odbc_buffer_desc = Vec::new();
    let mut fields = Vec::new();

    for index in 1..(num_cols + 1) {
        let mut cd = ColumnDescription::default();
        // Reserving helps with drivers not reporting column name size correctly.
        cd.name.reserve(128);
        cursor.describe_col(index as u16, &mut cd)?;

        debug!("ODBC column description for column {}: {:?}", index, cd);

        let name = cd.name_to_string()?;
        // Give a generated name, should we fail to retrieve one from the ODBC data source.
        let name = if name.is_empty() {
            format!("Column{}", index)
        } else {
            name
        };

        let ptb = |physical_type| Type::primitive_type_builder(&name, physical_type);

        let (field_builder, buffer_description) = match cd.data_type {
            DataType::Double => (ptb(PhysicalType::DOUBLE), ColumnBufferDescription::F64),
            DataType::Float | DataType::Real => {
                (ptb(PhysicalType::FLOAT), ColumnBufferDescription::F32)
            }
            DataType::SmallInt => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_16),
                ColumnBufferDescription::I32,
            ),
            DataType::Integer => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_32),
                ColumnBufferDescription::I32,
            ),
            DataType::Date => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::DATE),
                ColumnBufferDescription::Date,
            ),
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 10 =>
            {
                (
                    ptb(PhysicalType::INT32)
                        .with_logical_type(LogicalType::DECIMAL)
                        .with_precision(precision.try_into().unwrap())
                        .with_scale(scale.try_into().unwrap()),
                    ColumnBufferDescription::I32,
                )
            }
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 19 =>
            {
                (
                    ptb(PhysicalType::INT64)
                        .with_logical_type(LogicalType::DECIMAL)
                        .with_precision(precision.try_into().unwrap())
                        .with_scale(scale.try_into().unwrap()),
                    ColumnBufferDescription::I64,
                )
            }
            DataType::Timestamp { .. } => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::TIMESTAMP_MICROS),
                ColumnBufferDescription::Timestamp,
            ),
            DataType::Bigint => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::INT_64),
                ColumnBufferDescription::I64,
            ),
            DataType::Char { length } | DataType::Varchar { length } => (
                ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                ColumnBufferDescription::Text {
                    max_str_len: length.try_into().unwrap(),
                },
            ),
            DataType::Bit => (ptb(PhysicalType::BOOLEAN), ColumnBufferDescription::Bit),
            DataType::Tinyint => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_8),
                ColumnBufferDescription::I32,
            ),
            DataType::Unknown
            | DataType::Numeric { .. }
            | DataType::Decimal { .. }
            | DataType::Time { .. }
            | DataType::Other { .. } => {
                let max_str_len = cursor.col_display_size(index.try_into().unwrap())? as usize;
                (
                    ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                    ColumnBufferDescription::Text { max_str_len },
                )
            }
        };

        debug!(
            "ODBC buffer description for column {}: {:?}",
            index, buffer_description
        );

        let repetition = match cd.nullable {
            Nullable::Nullable | Nullable::Unknown => Repetition::OPTIONAL,
            Nullable::NoNulls => Repetition::REQUIRED,
        };

        let field_builder = field_builder.with_repetition(repetition);
        fields.push(Rc::new(field_builder.build()?));
        odbc_buffer_desc.push(buffer_description);
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()?;

    Ok((Rc::new(schema), odbc_buffer_desc))
}