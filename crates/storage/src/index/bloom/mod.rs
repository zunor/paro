// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Bloom Filter Index Module
//!
//! Bloom filter index for accelerating equality queries.
//!
//! ## Architecture
//!
//! Each page has a bloom filter that can quickly determine if a value
//! is definitely NOT in the page. This enables skipping pages during
//! point lookups.
//!
//! ## Usage
//!
//! ```ignore
//! // Writing
//! let mut writer = BloomFilterIndexWriter::new(BloomFilterOptions::default());
//! writer.add_values(&values);
//! writer.flush(); // Finish current page's bloom filter
//! let data = writer.finish();
//!
//! // Reading
//! let reader = BloomFilterIndexReader::from_bytes(&data)?;
//! let mut iter = reader.new_iterator();
//! let bf = iter.read_bloom_filter(page_idx)?;
//! if bf.may_contain(&value) {
//!     // Read the page
//! }
//! ```

mod bloom_filter_index;
mod bound_index;

pub use bloom_filter_index::{
    BloomFilter, BloomFilterAlgorithm, BloomFilterIndexReader, BloomFilterIndexWriter,
    BloomFilterOptions, HashStrategy,
};
pub use bound_index::BloomFilterIndex;
