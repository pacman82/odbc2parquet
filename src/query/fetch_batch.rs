use anyhow::{bail, Error};
use log::info;
use odbc_api::{buffers::ColumnarAnyBuffer, BlockCursor, Cursor};

use crate::parquet_buffer::ParquetBuffer;

use super::{
    batch_size_limit::BatchSizeLimit,
    conversion_strategy::ConversionStrategy,
};

pub trait FetchBatch {
    /// Maximum batch size in rows. This is used to allocate the parquet buffer of correct size.
    fn max_batch_size_in_rows(&self) -> usize;

    /// Borrows a buffer containing the next batch to be written to the output parquet file
    fn next_batch(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error>;
}

pub fn fetch_strategy<'a>(
    concurrent_fetching: bool,
    cursor: impl Cursor + 'a,
    conversion_strategy: &ConversionStrategy,
    batch_size_limit: BatchSizeLimit,
) -> Result<Box<dyn FetchBatch + 'a>, Error> {
    if concurrent_fetching {
        bail!("Concurrent fetching not yet supported")
    } else {
        Ok(Box::new(SequentialFetch::new(
            cursor,
            conversion_strategy,
            batch_size_limit,
        )?))
    }
}

/// Fetch one fetch buffer and write its contents to parquet. Then fill it again. This is not as
/// fast as double buffering with concurrent fetching, but it uses less memory due to only requiring
/// one fetch buffer.
struct SequentialFetch<C: Cursor> {
    block_cursor: BlockCursor<C, ColumnarAnyBuffer>,
}

impl<C> SequentialFetch<C>
where
    C: Cursor,
{
    pub fn new(
        cursor: C,
        table_strategy: &ConversionStrategy,
        batch_size_limit: BatchSizeLimit,
    ) -> Result<Self, Error> {
        let mem_usage_odbc_buffer_per_row: usize = table_strategy.fetch_buffer_size_per_row();
        let total_mem_usage_per_row =
            mem_usage_odbc_buffer_per_row + ParquetBuffer::MEMORY_USAGE_BYTES_PER_ROW;
        info!(
            "Memory usage per row is {} bytes. This excludes memory directly allocated by the ODBC \
            driver.",
            total_mem_usage_per_row,
        );

        let batch_size_row = batch_size_limit.batch_size_in_rows(total_mem_usage_per_row)?;

        info!("Batch size set to {} rows.", batch_size_row);

        let fetch_buffer = table_strategy.allocate_fetch_buffer(batch_size_row);

        let block_cursor = cursor.bind_buffer(fetch_buffer)?;
        Ok(Self { block_cursor })
    }
}

impl<C> FetchBatch for SequentialFetch<C>
where
    C: Cursor,
{
    fn next_batch(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error> {
        let batch = self.block_cursor.fetch()?;
        Ok(batch)
    }

    fn max_batch_size_in_rows(&self) -> usize {
        self.block_cursor.row_array_size()
    }
}
