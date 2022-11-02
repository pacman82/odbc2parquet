use parquet::basic::Repetition;

use super::{strategy::FetchStrategy, text::Utf8};

/// Parse wallclock time with fractional seconds from text into time. E.g. 16:04:12.0000000
pub fn time_from_text(repetition: Repetition, precision: u8) -> Box<dyn FetchStrategy> {
    let length = if precision == 0 {
        8
    } else {
        9 + precision as usize
    };
    Box::new(Utf8::with_bytes_length(repetition, length))
}
