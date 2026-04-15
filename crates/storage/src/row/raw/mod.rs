//! Internal raw row-format backend for execution-time row storage.
//!
//! `storage::row` wraps these lower-level building blocks with the sealed
//! `RowStore` / `RowStoreBuilder` APIs used by operators. This module stays
//! crate-private on purpose so the rest of the codebase does not depend on the
//! legacy append/scatter/gather state machine directly.

#[allow(dead_code)]
mod allocator;
#[allow(dead_code)]
mod collection;
#[allow(dead_code)]
mod gather;
#[allow(dead_code)]
mod layout;
#[allow(dead_code)]
mod partitioned;
#[allow(dead_code)]
mod radix_partitioned;
#[allow(dead_code)]
pub mod scatter;
#[allow(dead_code)]
mod segment;
#[allow(dead_code)]
mod states;

// Re-export layout types
pub use layout::{RawRowLayout, RawRowNestednessType, RawRowValidityType};

// Re-export states types
#[allow(unused_imports)]
pub use states::{
    BufferHandleMap, CombinedListData, ListEntry, RawRowAppendState, RawRowChunkState,
    RawRowParallelScanState, RawRowPinProperties, RawRowPinState, RawRowScanState,
    RawRowVectorFormat,
};

// Re-export allocator types
#[allow(unused_imports)]
pub use allocator::{HeapAllocation, RawRowAllocator, RawRowBlock, RowAllocation};

// Re-export segment types
#[allow(unused_imports)]
pub use segment::{ContinuousIdSet, RawRowChunk, RawRowChunkPart, RawRowSegment};

// Re-export collection types
pub use collection::RawRowCollection;

// Re-export partitioned raw-row substrate
pub use partitioned::{PartitionIndexComputer, PartitionedRawRow, PartitionedRawRowAppendState};
#[allow(unused_imports)]
pub use radix_partitioned::{RadixPartitionedRawRow, RadixPartitioning};

// Re-export scatter functions
#[allow(unused_imports)]
pub use scatter::{
    append_chunk, append_chunk_with_sel, build_rows, compute_heap_sizes, scatter_chunk,
};

// Re-export gather functions
#[allow(unused_imports)]
pub use gather::{
    fetch_chunk, gather_chunk, gather_chunk_with_sel, gather_column,
    gather_column_from_row_locations, is_row_valid, read_value, scan_chunk,
};
