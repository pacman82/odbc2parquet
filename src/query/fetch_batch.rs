use std::mem::swap;

use anyhow::Error;
use log::info;
use odbc_api::{
    buffers::ColumnarAnyBuffer, BlockCursor, ConcurrentBlockCursor, Cursor, RowSetBuffer,
};

use crate::parquet_buffer::ParquetBuffer;

use super::{batch_size_limit::BatchSizeLimit, conversion_strategy::ConversionStrategy};

pub trait FetchBatch {
    /// Maximum batch size in rows. This is used to allocate the parquet buffer of correct size.
    fn max_batch_size_in_rows(&self) -> usize;

    /// Borrows a buffer containing the next batch to be written to the output parquet file
    fn next_batch(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error>;
}

pub fn fetch_strategy(
    concurrent_fetching: bool,
    cursor: impl Cursor + 'static + Send,
    conversion_strategy: &ConversionStrategy,
    batch_size_limit: BatchSizeLimit,
) -> Result<Box<dyn FetchBatch>, Error> {
    if concurrent_fetching {
        Ok(Box::new(ConcurrentFetch::new(
            cursor,
            conversion_strategy,
            batch_size_limit,
        )?))
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

/// Use a concurrent cursor and an extra buffers. One buffers content is read and written into
/// parquet, while the other is filled in an extra system thread.
struct ConcurrentFetch<C: Cursor> {
    // This buffer is read from and its contents is written into parquet.
    buffer: ColumnarAnyBuffer,
    block_cursor: ConcurrentBlockCursor<C, ColumnarAnyBuffer>,
}

impl<C> ConcurrentFetch<C>
where
    C: Cursor + Send + 'static,
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
            "Memory usage per row is 2x {} bytes (buffer is alloctated two times, because of \
            concurrent fetching). This excludes memory directly allocated by the ODBC driver.",
            total_mem_usage_per_row,
        );

        let batch_size_row = batch_size_limit.batch_size_in_rows(total_mem_usage_per_row)?;

        info!("Batch size set to {} rows.", batch_size_row);

        let fetch_buffer = table_strategy.allocate_fetch_buffer(batch_size_row);
        let buffer = table_strategy.allocate_fetch_buffer(batch_size_row);

        let block_cursor = cursor.bind_buffer(fetch_buffer)?;
        let block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);
        Ok(Self {
            buffer,
            block_cursor,
        })
    }
}

impl<C> FetchBatch for ConcurrentFetch<C>
where
    C: Cursor,
{
    fn next_batch(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error> {
        let batch = self.block_cursor.fetch()?;
        if let Some(mut batch) = batch {
            swap(&mut batch, &mut self.buffer);
            self.block_cursor.fill(batch);
            Ok(Some(&self.buffer))
        } else {
            Ok(None)
        }
    }

    fn max_batch_size_in_rows(&self) -> usize {
        self.buffer.row_array_size()
    }
}
