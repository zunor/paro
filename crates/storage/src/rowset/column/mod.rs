// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Column Module
//!
//! Column reader and writer for the Segment V2 format.
//!
//! ## Architecture
//!
//! - `ColumnWriter`: Writes column data with automatic encoding selection
//! - `ColumnReader`: Reads column data with predicate pushdown
//! - `ColumnIterator`: Iterates over column values
//!
//! ## Column Types
//!
//! - `ScalarColumnWriter`: Writes scalar (non-nested) columns
//! - `ArrayColumnWriter`: Writes array columns (offsets + elements)
//! - `ScalarColumnIterator`: Reads scalar columns
//! - `ArrayColumnIterator`: Reads array columns

mod array_column_reader;
mod array_column_writer;
mod column_iterator;
mod column_reader;
mod column_writer;

pub use array_column_reader::{ArrayColumnIterator, ArrayColumnReaderMeta, ArrayValue};
pub use array_column_writer::{ArrayColumnWriter, ArrayColumnWriterMeta};
pub use column_iterator::{
    ColumnBatch, ColumnIterator, FilteredColumnIterator, ScalarColumnIterator,
    StorageDictionaryBatch,
};
pub use column_reader::{
    ColumnReader, ColumnReaderMeta, ColumnReaderOptions, OrdinalIndexEntry, OrdinalIndexReader,
    SharedColumnReader, ZoneMapEntry, ZoneMapIndexReader,
};
pub use column_writer::{
    ColumnWriter, ColumnWriterMeta, ColumnWriterOptions, OrdinalIndexWriter, ScalarColumnWriter,
    ZoneMapIndexWriter, DEFAULT_PAGE_SIZE,
};
