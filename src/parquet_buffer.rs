use anyhow::Error;
use chrono::NaiveDate;
use odbc_api::sys::{Date, Len, Timestamp, NULL_DATA};
use parquet::{
    column::writer::ColumnWriterImpl,
    data_type::{BoolType, ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
};
use std::convert::TryInto;

pub struct ParquetBuffer {
    /// Used to hold date values converted from ODBC `Date` types or int or decimals with scale 0.
    pub values_i32: Vec<i32>,
    /// Used to hold timestamp values converted from ODBC `Timestamp` types or int or decimal with
    /// scale 0.
    pub values_i64: Vec<i64>,
    pub values_bytes_array: Vec<ByteArray>,
    pub values_bool: Vec<bool>,
    pub def_levels: Vec<i16>,
}

impl ParquetBuffer {
    pub fn new(batch_size: usize) -> ParquetBuffer {
        ParquetBuffer {
            values_i32: Vec::with_capacity(batch_size),
            values_i64: Vec::with_capacity(batch_size),
            values_bytes_array: Vec::with_capacity(batch_size),
            values_bool: Vec::with_capacity(batch_size),
            def_levels: Vec::with_capacity(batch_size),
        }
    }

    pub fn set_num_rows_fetched(&mut self, num_rows: usize) {
        self.def_levels.resize(num_rows, 0);
        self.values_i32.resize(num_rows, 0);
        self.values_i64.resize(num_rows, 0);
        self.values_bytes_array.resize(num_rows, ByteArray::new());
        self.values_bool.resize(num_rows, false);
    }

    /// In case the ODBC C Type matches the physical Parquet type, we can write the buffer directly
    /// without transforming. The definition levels still require transformation, though.
    pub fn write_directly<T>(
        &mut self,
        cw: &mut ColumnWriterImpl<T>,
        source: (&[T::T], &[Len]),
    ) -> Result<(), Error>
    where
        T: DataType,
    {
        let (values, indicators) = source;
        for (def, &ind) in self.def_levels.iter_mut().zip(indicators) {
            *def = if ind == NULL_DATA { 0 } else { 1 };
        }
        cw.write_batch(values, Some(&self.def_levels), None)?;
        Ok(())
    }

    pub fn write_dates<'a>(
        &mut self,
        cw: &mut ColumnWriterImpl<Int32Type>,
        dates: impl Iterator<Item = Option<&'a Date>>,
    ) -> Result<(), Error> {
        // Currently we use int32 only to represent dates
        let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
        for (row_index, field) in dates.enumerate() {
            let (value, def) = field
                .map(|date| {
                    // Transform date to days since unix epoch as i32
                    let date =
                        NaiveDate::from_ymd(date.year as i32, date.month as u32, date.day as u32);
                    let duration = date.signed_duration_since(unix_epoch);
                    (duration.num_days().try_into().unwrap(), 1)
                })
                .unwrap_or((0, 0));
            self.def_levels[row_index] = def;
            self.values_i32[row_index] = value;
        }
        cw.write_batch(&self.values_i32, Some(&self.def_levels), None)?;
        Ok(())
    }

    pub fn write_timestamps<'a>(
        &mut self,
        cw: &mut ColumnWriterImpl<Int64Type>,
        timestamps: impl Iterator<Item = Option<&'a Timestamp>>,
    ) -> Result<(), Error> {
        // Currently we use int32 only to represent dates
        for (row_index, field) in timestamps.enumerate() {
            let (value, def) = field
                .map(|ts| {
                    // Transform date to days since unix epoch as i32
                    let datetime =
                        NaiveDate::from_ymd(ts.year as i32, ts.month as u32, ts.day as u32)
                            .and_hms_nano(
                                ts.hour as u32,
                                ts.minute as u32,
                                ts.second as u32,
                                ts.fraction as u32,
                            );
                    (datetime.timestamp_nanos() / 1000, 1)
                })
                .unwrap_or((0, 0));
            self.def_levels[row_index] = def;
            self.values_i64[row_index] = value;
        }
        cw.write_batch(&self.values_i64, Some(&self.def_levels), None)?;
        Ok(())
    }

    pub fn write_strings<'a>(
        &mut self,
        cw: &mut ColumnWriterImpl<ByteArrayType>,
        strings: impl Iterator<Item = Option<&'a [u8]>>,
    ) -> Result<(), Error> {
        for (row_index, read_buf) in strings.enumerate() {
            let (bytes, nul) = read_buf
                // Value is not NULL
                .map(|buf| (buf.to_owned().into(), 1))
                // Value is NULL
                .unwrap_or_else(|| (ByteArray::new(), 0));
            self.values_bytes_array[row_index] = bytes;
            self.def_levels[row_index] = nul;
        }
        cw.write_batch(&self.values_bytes_array, Some(&self.def_levels), None)?;

        Ok(())
    }

    pub fn write_bools<'a>(
        &mut self,
        cw: &mut ColumnWriterImpl<BoolType>,
        booleans: impl Iterator<Item = Option<bool>>,
    ) -> Result<(), Error> {
        for (row_index, field) in booleans.enumerate() {
            if let Some(val) = field {
                self.values_bool[row_index] = val;
                self.def_levels[row_index] = 1;
            } else {
                self.def_levels[row_index] = 0;
            }
        }
        cw.write_batch(&self.values_bool, Some(&self.def_levels), None)?;

        Ok(())
    }
}
