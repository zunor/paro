// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Segment Module
//!
//! Segment is the fundamental storage unit within a Rowset.
//! Each Segment is an immutable columnar file containing:
//! - Column data pages (encoded and compressed)
//! - Column indexes (ordinal, zonemap, bloom filter)
//! - Segment footer with metadata
//!
//! ## Architecture
//!
//! ```text
//! Segment File Layout:
//! ┌─────────────────────────────────────────┐
//! │ Column 0 Data Pages                     │
//! │ Column 0 Ordinal Index Page             │
//! │ Column 0 ZoneMap Index Page             │
//! ├─────────────────────────────────────────┤
//! │ Column 1 Data Pages                     │
//! │ Column 1 Ordinal Index Page             │
//! │ Column 1 ZoneMap Index Page             │
//! ├─────────────────────────────────────────┤
//! │ ...                                     │
//! ├─────────────────────────────────────────┤
//! │ Short Key Index Page                    │
//! ├─────────────────────────────────────────┤
//! │ Segment Footer                          │
//! │ - Column metadata array                 │
//! │ - Short key index pointer               │
//! │ - Num rows, checksum, etc.              │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## References
//!
//! Implementation notes live alongside the module sources.

mod segment;
mod segment_delete_vector;
mod segment_format;
mod segment_indexes;
mod segment_iterator;
mod segment_loader;
mod segment_predicate;
mod segment_search;
#[cfg(test)]
mod segment_tests;
mod segment_writer;

pub use segment::{Segment, SegmentMeta, SegmentOptions, SegmentSharedPtr};
pub use segment_format::{ColumnMeta, SegmentFooter};
pub use segment_iterator::{SegmentBatch, SegmentIterator};
pub use segment_writer::{
    ColumnData, SegmentInlineIndexKind, SegmentInlineIndexPage, SegmentWriter,
    SegmentWriterBuilder, SegmentWriterOptions,
};
