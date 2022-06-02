use std::cmp::min;

use anyhow::bail;
use bytesize::ByteSize;

#[cfg(target_pointer_width = "64")]
const DEFAULT_BATCH_SIZE_BYTES: ByteSize = ByteSize::gib(2); // 2GB
#[cfg(target_pointer_width = "32")]
const DEFAULT_BATCH_SIZE_BYTES: ByteSize = ByteSize::gib(1); // 1GB

/// We limit the maximum numbers of rows to 65535 by default to avoid trouble with ODBC drivers
/// using a 16Bit integer to represent fetch size. Most drivers work fine though with larger
/// batches. Anyway the trade off seems worth it because 65535 is already a pretty large batch size
/// for most applications, and this way the tool runs fine out of the box in even more situations.
const DEFAULT_BATCH_SIZE_ROWS: usize = u16::MAX as usize; // 65535 rows

/// Batches can be limitied by either number of rows or the total size of the rows in the batch in
/// bytes.
pub enum BatchSizeLimit {
    Rows(usize),
    Bytes(ByteSize),
    Both { rows: usize, memory: ByteSize },
}

impl BatchSizeLimit {
    pub fn new(num_rows_limit: Option<usize>, memory_limit: Option<ByteSize>) -> Self {
        match (num_rows_limit, memory_limit) {
            (Some(rows), None) => BatchSizeLimit::Rows(rows),
            (None, Some(memory)) => BatchSizeLimit::Bytes(memory),
            // User specified nothing => Use default
            (None, None) => BatchSizeLimit::Both {
                rows: DEFAULT_BATCH_SIZE_ROWS,
                memory: DEFAULT_BATCH_SIZE_BYTES,
            },
            (Some(rows), Some(memory)) => BatchSizeLimit::Both { rows, memory },
        }
    }

    pub fn batch_size_in_rows(
        &self,
        total_mem_usage_per_row: usize,
    ) -> Result<usize, anyhow::Error> {
        let to_num_rows = |num_bytes: usize| {
            let rows = num_bytes / total_mem_usage_per_row;
            if rows == 0 {
                bail!(
                    "Memory required to hold a single row is larger than the limit. Memory Limit: \
                    {} bytes, Memory per row: {} bytes.\nYou can use either '--batch-size-row' or \
                    '--batch-size-mib' to raise the limit. You may also try more verbose output to \
                    see which columns require so much memory and consider casting them into \
                    something smaller.",
                    num_bytes,
                    total_mem_usage_per_row
                )
            }
            Ok(rows)
        };

        match self {
            BatchSizeLimit::Rows(rows) => Ok(*rows),
            BatchSizeLimit::Bytes(memory) => to_num_rows(memory.as_u64().try_into().unwrap()),
            BatchSizeLimit::Both { rows, memory } => {
                let limit_rows = to_num_rows(memory.as_u64().try_into().unwrap())?;
                Ok(min(limit_rows, *rows))
            }
        }
    }
}
