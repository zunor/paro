// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Index Module
//!
//! Extensible index framework for Paro database.
//!
//! ## Architecture
//!
//! This module implements an extensible index framework with multiple index types:
//!
//! ### Core Index Interfaces
//! - `Index` trait: Base interface for all indexes
//! - `BoundIndex` trait: Interface for indexes bound to a table
//! - `IndexType`: Registration structure for index types with callbacks
//! - `IndexTypeSet`: Registry for callback-driven build types
//! - `IndexPointer`: 64-bit pointer for index nodes
//! - `art`: Adaptive Radix Tree implementation used as a segment-local runtime predicate index
//!
//! ### Built-in Column Indexes
//! - `zonemap`: Per-page min/max/has_null statistics for predicate pushdown
//! - `bloom`: Bloom filter index for accelerating equality queries
//! - `short_key`: Short key index for fast row block location
//! - `bitmap`: Bitmap index for low-cardinality columns
//!
//! ## Extensibility
//!
//! New index types can be registered via `IndexTypeSet::register_index_type()`.
//! Each index type provides callbacks for building and creating indexes.
//!
//! ## Example
//!
//! ```ignore
//! // Register a custom index type
//! let mut index_type_set = IndexTypeSet::new();
//! index_type_set.register_index_type(get_custom_index_type())?;
//!
//! // Find an index type by name
//! let bloom_type = index_type_set.find_by_name("BLOOM");
//! ```

// Core index implementations.
pub mod art;
pub mod fulltext;
pub mod graph;
pub mod hnsw;
pub mod sparse;

// Built-in column-oriented index implementations.
pub mod bitmap;
pub mod bloom;
pub mod short_key;
pub mod zonemap;

mod bound_index;
mod evaluator;
mod fixed_membership;
mod fixed_size_allocator;
mod fixed_size_buffer;
mod index;
mod index_builder;
mod index_constraint_type;
mod index_pointer;
mod index_storage_info;
mod index_type;
mod index_type_set;
mod page_layout;
mod predicate;
mod predicate_result;

pub use bound_index::{BoundIndex, DeltaIndexType, IndexAppendInfo, IndexAppendMode};
pub(crate) use bound_index::{PredicateIndexBinding, SegmentLocalComplete};
pub use evaluator::IndexEvaluator;
pub use fixed_membership::{FixedMembership, FixedMembershipBuildPolicy};
pub(crate) use fixed_membership::{
    FixedMembershipKind, FixedMembershipSet, FixedMembershipValue, FixedMembershipView,
};
pub use fixed_size_allocator::{
    FixedSizeAllocator, FixedSizeAllocatorInfo, DEFAULT_BLOCK_SIZE, VACUUM_THRESHOLD,
};
pub use fixed_size_buffer::{
    BlockPointer, FixedSizeBuffer, SegmentHandle, ValidityT, BASE, BITS_PER_VALIDITY, SHIFT,
};
pub use index::{ColumnId, Index};
pub use index_builder::{
    bitmap_build_bind, bitmap_build_combine, bitmap_build_finalize, bitmap_build_global_init,
    bitmap_build_local_init, bitmap_build_sink, bitmap_build_sort, bloom_build_bind,
    bloom_build_combine, bloom_build_finalize, bloom_build_global_init, bloom_build_local_init,
    bloom_build_sink, bloom_build_sort, get_bitmap_index_type, get_bloom_index_type,
    BitmapBuildBindData, BitmapBuildGlobalState, BitmapBuildLocalState, BloomBuildBindData,
    BloomBuildGlobalState, BloomBuildLocalState,
};
pub use index_constraint_type::IndexConstraintType;
pub use index_pointer::IndexPointer;
pub use index_storage_info::{IndexBufferInfo, IndexStorageInfo};
pub use index_type::{
    CreateIndexInput, IndexBuildBindData, IndexBuildBindInput, IndexBuildCombineInput,
    IndexBuildFinalizeInput, IndexBuildGlobalState, IndexBuildInitGlobalStateInput,
    IndexBuildInitLocalStateInput, IndexBuildLocalState, IndexBuildSinkInput, IndexBuildSortInput,
    IndexType, IndexTypeInfo, PlanIndexInput,
};
pub use index_type_set::IndexTypeSet;
pub use page_layout::PageLayout;
pub use predicate::{
    collect_predicate_columns, compare_bytes, value_to_bytes, Predicate, PredicateComparison,
    PredicateTree,
};
pub use predicate_result::{
    decode_page_ranges, encode_page_ranges, intersect, to_row_ranges, union, PageRange,
    PredicateResult,
};

// Re-export column indexes
pub use bitmap::BitmapIndex;
pub use bitmap::{BitmapIndexIterator, BitmapIndexReader, BitmapIndexWriter, BitmapType};
pub use bloom::BloomFilterIndex;
pub use bloom::{
    BloomFilter, BloomFilterAlgorithm, BloomFilterIndexReader, BloomFilterIndexWriter,
    BloomFilterOptions, HashStrategy,
};
pub use graph::{
    AdjacencyCSR, CSRData, EdgeBuildInput, GraphBuildInput, GraphProjectionIndex,
    GraphProjectionIndexManager, LocalVertexId, VertexBuildInput, VertexIdMap, VertexKey,
};
pub use hnsw::{
    build_missing_hnsw_indexes_with_scheduler, DistanceMetric, HnswBuildSummary,
    HnswColumnBuildConfig, HnswIndex, MmapVectorStorage,
};
pub use short_key::{
    ShortKeyFooter, ShortKeyIndexBuilder, ShortKeyIndexDecoder, ShortKeyIndexIterator,
    KEY_MAXIMAL_MARKER, KEY_MINIMAL_MARKER, KEY_NORMAL_MARKER, KEY_NULL_FIRST_MARKER,
    KEY_NULL_LAST_MARKER,
};
pub use zonemap::{
    BoundsPrecision, ZoneMapEntry, ZoneMapIndex, ZoneMapIndexReader, ZoneMapIndexWriter,
};
