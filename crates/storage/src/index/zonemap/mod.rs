//! # ZoneMap Index Module
//!
//! ZoneMap index stores min/max/has_null statistics per page for predicate pushdown.
//!
//! ## Architecture
//!
//! ZoneMap is a lightweight index that enables skipping pages that don't contain
//! matching values. Each page has a zone map entry with:
//! - min: minimum non-null value in the page
//! - max: maximum non-null value in the page
//! - has_null: whether the page contains null values
//!
//! ## Usage
//!
//! ```ignore
//! // Writing
//! let mut writer = ZoneMapIndexWriter::new();
//! writer.add(min_bytes, max_bytes, has_null);
//! let data = writer.finish();
//!
//! // Reading
//! let reader = ZoneMapIndexReader::from_bytes(&data)?;
//! if reader.page_may_contain_value(page_idx, &value, cmp) {
//!     // Read the page
//! }
//! ```

mod bound_index;
mod zonemap_index;

pub use bound_index::ZoneMapIndex;
pub use zonemap_index::{ZoneMapEntry, ZoneMapIndexReader, ZoneMapIndexWriter};
