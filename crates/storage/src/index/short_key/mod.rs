//! # Short Key Index Module
//!
//! Short key index for fast row block location.
//!
//! ## Architecture
//!
//! Short key index stores the first few columns (short key) of each row block,
//! enabling binary search to locate the block containing a specific key.
//!
//! ## Key Markers
//!
//! - `KEY_MINIMAL_MARKER` (0x00): Minimal value for a field
//! - `KEY_NULL_FIRST_MARKER` (0x01): Null value (sorted first)
//! - `KEY_NORMAL_MARKER` (0x02): Normal non-null value
//! - `KEY_NULL_LAST_MARKER` (0xFE): Null value (sorted last)
//! - `KEY_MAXIMAL_MARKER` (0xFF): Maximal value for a field
//!
//! ## Usage
//!
//! ```ignore
//! // Writing
//! let mut builder = ShortKeyIndexBuilder::new(segment_id, rows_per_block);
//! builder.add_item(&key1)?;
//! builder.add_item(&key2)?;
//! let (body, footer) = builder.finalize(num_rows)?;
//!
//! // Reading
//! let decoder = ShortKeyIndexDecoder::parse(&body, &footer)?;
//! let iter = decoder.lower_bound(&search_key);
//! ```

mod short_key_index;

pub use short_key_index::{
    ShortKeyFooter, ShortKeyIndexBuilder, ShortKeyIndexDecoder, ShortKeyIndexIterator,
    KEY_MAXIMAL_MARKER, KEY_MINIMAL_MARKER, KEY_NORMAL_MARKER, KEY_NULL_FIRST_MARKER,
    KEY_NULL_LAST_MARKER,
};
