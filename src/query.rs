use std::{convert::TryInto, fs::File, rc::Rc};

use anyhow::Error;
use log::{debug, info};
use odbc_api::{
    buffers::AnyColumnView,
    buffers::BufferKind,
    buffers::{BufferDescription, ColumnarRowSet},
    ColumnDescription, Cursor, DataType, Environment, IntoParameter, Nullable,
};
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};

use crate::{open_connection, parquet_buffer::ParquetBuffer, QueryOpt};

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
        cursor_to_parquet(cursor, file, *batch_size)?;
    } else {
        eprintln!(
            "Query came back empty (not even a schema has been returned). No file has been created"
        );
    }
    Ok(())
}

fn cursor_to_parquet(cursor: impl Cursor, file: File, batch_size: u32) -> Result<(), Error> {
    info!("Batch size set to {}", batch_size);
    // Write properties
    let wpb = WriterProperties::builder();
    let properties = Rc::new(wpb.build());

    let (parquet_schema, buffer_description) = make_schema(&cursor)?;
    let mut writer = SerializedFileWriter::new(file, parquet_schema, properties)?;
    let mut odbc_buffer = ColumnarRowSet::new(batch_size, buffer_description.iter().copied());
    let mut row_set_cursor = cursor.bind_buffer(&mut odbc_buffer)?;

    let mut pb = ParquetBuffer::new(batch_size as usize);
    let mut num_batch = 0;

    while let Some(buffer) = row_set_cursor.fetch()? {
        let mut row_group_writer = writer.next_row_group()?;
        let mut col_index = 0;
        num_batch += 1;
        let num_rows = buffer.num_rows();
        info!("Fetched batch {} with {} rows.", num_batch, num_rows);
        while let Some(mut column_writer) = row_group_writer.next_column()? {
            pb.set_num_rows_fetched(num_rows);
            let odbc_column = buffer.column(col_index);
            match (&mut column_writer, odbc_column) {
                (ColumnWriter::BoolColumnWriter(cw), AnyColumnView::NullableBit(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::Int32ColumnWriter(cw), AnyColumnView::NullableDate(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::Int32ColumnWriter(cw), AnyColumnView::NullableI32(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::Int64ColumnWriter(cw), AnyColumnView::NullableTimestamp(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::Int64ColumnWriter(cw), AnyColumnView::NullableI64(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::FloatColumnWriter(cw), AnyColumnView::NullableF32(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::DoubleColumnWriter(cw), AnyColumnView::NullableF64(it)) => {
                    pb.write_optional(cw, it)?;
                }
                (ColumnWriter::ByteArrayColumnWriter(cw), AnyColumnView::Text(it)) => {
                    pb.write_optional(cw, it)?;
                }
                // ColumnWriter::Int96ColumnWriter(_) => {}
                // ColumnWriter::FixedLenByteArrayColumnWriter(_) => {}
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

fn make_schema(cursor: &impl Cursor) -> Result<(Rc<Type>, Vec<BufferDescription>), Error> {
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

        let (field_builder, buffer_kind) = match cd.data_type {
            DataType::Double => (ptb(PhysicalType::DOUBLE), BufferKind::F64),
            DataType::Float | DataType::Real => (ptb(PhysicalType::FLOAT), BufferKind::F32),
            DataType::SmallInt => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_16),
                BufferKind::I32,
            ),
            DataType::Integer => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_32),
                BufferKind::I32,
            ),
            DataType::Date => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::DATE),
                BufferKind::Date,
            ),
            DataType::Decimal { scale, precision } | DataType::Numeric { scale, precision }
                if scale == 0 && precision < 10 =>
            {
                (
                    ptb(PhysicalType::INT32)
                        .with_logical_type(LogicalType::DECIMAL)
                        .with_precision(precision.try_into().unwrap())
                        .with_scale(scale.try_into().unwrap()),
                    BufferKind::I32,
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
                    BufferKind::I64,
                )
            }
            DataType::Timestamp { .. } => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::TIMESTAMP_MICROS),
                BufferKind::Timestamp,
            ),
            DataType::Bigint => (
                ptb(PhysicalType::INT64).with_logical_type(LogicalType::INT_64),
                BufferKind::I64,
            ),
            DataType::Char { length } | DataType::Varchar { length } => (
                ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                BufferKind::Text {
                    max_str_len: length.try_into().unwrap(),
                },
            ),
            DataType::Bit => (ptb(PhysicalType::BOOLEAN), BufferKind::Bit),
            DataType::Tinyint => (
                ptb(PhysicalType::INT32).with_logical_type(LogicalType::INT_8),
                BufferKind::I32,
            ),
            DataType::Unknown
            | DataType::Numeric { .. }
            | DataType::Decimal { .. }
            | DataType::Time { .. }
            | DataType::Other { .. } => {
                let max_str_len = cursor.col_display_size(index.try_into().unwrap())? as usize;
                (
                    ptb(PhysicalType::BYTE_ARRAY).with_logical_type(LogicalType::UTF8),
                    BufferKind::Text { max_str_len },
                )
            }
        };

        let buffer_description = BufferDescription {
            kind: buffer_kind,
            nullable: true,
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
