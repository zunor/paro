// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Rowset Module
//!
//! Segment V2 compatible rowset implementation.
//!
//! ## Architecture
//!
//! A Rowset contains multiple Segments, each Segment contains:
//! - Column data pages (encoded and compressed)
//! - Column indexes (ordinal, zonemap, bloom filter)
//! - Segment footer with metadata
//!
//! ## Modules
//!
//! - `page`: Page structure and I/O operations
//! - `encoding`: Column encoding algorithms (Plain, Dictionary, BitShuffle, RLE, etc.)
//! - `column`: Column reader and writer
//! - `segment`: Segment structure and iterator
//! - `rowset_meta`: Rowset metadata for persistence and management
//! - `rowset`: Core Rowset structure managing Segment collection

pub mod column;
pub mod encoding;
pub mod page;
pub mod page_reader;
pub mod partial_row;
mod row_id;
pub mod rowset;
pub mod rowset_meta;
pub mod rowset_statistics;
pub mod rowset_writer;
pub mod scan_cost;
pub mod segment;
pub mod segment_statistics;
pub mod sparse_vector;

// Re-export page types
pub use page::{
    BlockCompressionCodec, CompressionType, DataPageFooter, DictPageFooter, EncodingType,
    IndexPageFooter, IndexPageType, Lz4Codec, NoCompressionCodec, NullEncoding, Page, PageBuilder,
    PageBuilderOptions, PageDecoder, PageDecoderOptions, PageFooter, PageIO, PagePointer,
    PageReadOptions, PageType, ShortKeyFooter, ZstdCodec, DEFAULT_MIN_SPACE_SAVING,
};
pub use page_reader::{PageReader, PageReaderContext, PageReaderOptions};
pub use partial_row::{load_base_rowids, load_base_rowids_for_offsets, save_base_rowids};
pub use row_id::{BatchRowOrdinal, PhysicalRowRef, SegmentRowId};

// Re-export encoding types
pub use encoding::{
    get_encoding_registry, BinaryDictPageBuilder, BinaryDictPageDecoder, BinaryPlainPageBuilder,
    BinaryPlainPageDecoder, BitShufflePageBuilder, BitShufflePageDecoder, EncodingInfo,
    EncodingRegistry, FieldType, PlainPageBuilder, PlainPageDecoder, RlePageBuilder,
    RlePageDecoder, BITSHUFFLE_PAGE_HEADER_SIZE, PLAIN_PAGE_HEADER_SIZE, RLE_PAGE_HEADER_SIZE,
};

// Re-export column types
pub use column::{
    ArrayColumnIterator, ArrayColumnReaderMeta, ArrayColumnWriter, ArrayColumnWriterMeta,
    ArrayValue, ColumnIterator, ColumnReader, ColumnReaderMeta, ColumnReaderOptions, ColumnWriter,
    ColumnWriterMeta, ColumnWriterOptions, FilteredColumnIterator, OrdinalIndexEntry,
    OrdinalIndexReader, OrdinalIndexWriter, ScalarColumnIterator, ScalarColumnWriter,
    SharedColumnReader, ZoneMapEntry, ZoneMapIndexReader, ZoneMapIndexWriter,
};

// Re-export rowset_meta types
pub use rowset_meta::{
    generate_rowset_id, set_next_rowset_id, RowsetId, RowsetMeta, RowsetMetaBuilder, RowsetState,
    SegmentsOverlap,
};

// Re-export rowset types.
pub use rowset::{Rowset, RowsetBuilder, RowsetIterator, RowsetSharedPtr};

// Re-export segment types.
pub use segment::{
    ColumnMeta, Segment, SegmentFooter, SegmentIterator, SegmentMeta, SegmentOptions,
    SegmentSharedPtr,
};

// Segment statistics.
pub use rowset_statistics::{RowsetColumnStatistics, RowsetStatistics};
pub use segment_statistics::{ColumnSegmentStatistics, SegmentStatistics};

// Re-export segment_writer types.
pub use segment::{ColumnData, SegmentWriter, SegmentWriterBuilder, SegmentWriterOptions};

// Re-export rowset_writer types.
pub use rowset_writer::{RowsetWriter, RowsetWriterBuilder, RowsetWriterContext};

// Re-export sparse vector storage types.
pub use sparse_vector::{DimWeight, DimensionId, SparseVector, SparseVectorColumnFile};
