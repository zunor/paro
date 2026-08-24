// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Bitmap Index Module
//!
//! Bitmap index for low-cardinality columns.
//!
//! ## Architecture
//!
//! Bitmap index stores a bitmap for each distinct value in the column.
//! Each bitmap indicates which rows contain that value. This is efficient
//! for columns with low cardinality (few distinct values).
//!
//! ## Components
//!
//! - Dictionary: Ordered list of distinct values
//! - Bitmaps: One RoaringBitmap per dictionary entry
//!
//! ## Usage
//!
//! ```ignore
//! // Writing
//! let mut writer = BitmapIndexWriter::new();
//! writer.add_values(&values);
//! let data = writer.finish()?;
//!
//! // Reading
//! let reader = BitmapIndexReader::from_bytes(&data)?;
//! let mut iter = reader.new_iterator();
//! iter.seek_dictionary(&value, &mut exact_match)?;
//! let bitmap = iter.read_bitmap(ordinal)?;
//! ```

mod bitmap_index;
mod bound_index;

#[cfg(test)]
pub(crate) use bitmap_index::posting_fingerprint;
pub(crate) use bitmap_index::posting_fingerprint_rows;
pub use bitmap_index::{
    BitmapIndexIterator, BitmapIndexReader, BitmapIndexWriter, BitmapType, OrderedBitmapBlock,
};
pub use bound_index::BitmapIndex;
