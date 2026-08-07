// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Segment/chunk organization for the raw row backend.

use std::sync::{Arc, Mutex};

use super::{RawRowAllocator, RawRowLayout};
use crate::buffer::BufferHandle;

/// Invalid index marker for empty heap blocks.
const INVALID_INDEX: u32 = u32::MAX;

/// A contiguous range of IDs (min to max inclusive).
///
/// Used to track which row/heap blocks are referenced by a chunk.
/// More efficient than a HashSet for contiguous ranges.
#[derive(Debug, Clone, Default)]
pub struct ContinuousIdSet {
    min_id: u32,
    max_id: u32,
    empty: bool,
}

impl ContinuousIdSet {
    /// Create a new empty set.
    pub fn new() -> Self {
        Self {
            min_id: 0,
            max_id: 0,
            empty: true,
        }
    }

    /// Insert a block ID into the set.
    pub fn insert(&mut self, block_id: u32) {
        if self.empty {
            self.min_id = block_id;
            self.max_id = block_id;
            self.empty = false;
        } else {
            self.min_id = self.min_id.min(block_id);
            self.max_id = self.max_id.max(block_id);
        }
    }

    /// Check if a block ID is in the set.
    pub fn contains(&self, block_id: u32) -> bool {
        if self.empty {
            return false;
        }
        block_id >= self.min_id && block_id <= self.max_id
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    /// Get the start (minimum) ID.
    pub fn start(&self) -> u32 {
        debug_assert!(!self.empty);
        self.min_id
    }

    /// Get the end (one past maximum) ID.
    pub fn end(&self) -> u32 {
        debug_assert!(!self.empty);
        self.max_id + 1
    }

    /// Get the size (number of IDs in range).
    pub fn size(&self) -> u32 {
        if self.empty {
            0
        } else {
            self.max_id - self.min_id + 1
        }
    }

    /// Decrement the max ID (used when merging parts).
    pub fn decrement_max(&mut self) {
        debug_assert!(!self.empty);
        debug_assert!(self.size() > 1);
        self.max_id -= 1;
    }
}

/// A contiguous region of rows within row/heap blocks.
///
/// Represents a portion of data that was appended together.
/// Multiple parts can form a single RawRowChunk.
#[derive(Debug, Clone)]
pub struct RawRowChunkPart {
    /// Index of the row block
    pub row_block_index: u32,
    /// Offset within the row block (in bytes)
    pub row_block_offset: u32,
    /// Index of the heap block (INVALID_INDEX if no heap)
    pub heap_block_index: u32,
    /// Offset within the heap block (in bytes)
    pub heap_block_offset: u32,
    /// Total heap size for this chunk part
    pub total_heap_size: usize,
    /// Number of rows in this part
    pub count: u32,
    /// Last pinned base address of this part's heap range.
    ///
    /// Row-local varlen pointers are swizzled whenever the buffer pool reloads
    /// the heap block at another address. Keeping the address as an integer
    /// avoids giving a stale raw pointer a false lifetime, and sharing the
    /// state makes cloned part metadata observe the same swizzle generation.
    pub heap_base_address: Arc<Mutex<Option<usize>>>,
}

/// Stable append-time location of one row inside a raw row collection.
///
/// Sealed [`RowStore`](crate::row::RowStore) metadata retains this location so
/// later gathers can pin the exact chunk part without scanning segments and
/// chunks again for every projected column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RawRowLocation {
    pub(crate) segment_index: usize,
    pub(crate) part_index: usize,
    pub(crate) row_in_part: usize,
}

impl RawRowChunkPart {
    /// Create a new chunk part.
    pub fn new(
        row_block_index: u32,
        row_block_offset: u32,
        heap_block_index: u32,
        heap_block_offset: u32,
        total_heap_size: usize,
        count: u32,
    ) -> Self {
        Self {
            row_block_index,
            row_block_offset,
            heap_block_index,
            heap_block_offset,
            total_heap_size,
            count,
            heap_base_address: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a chunk part with no heap data.
    pub fn new_without_heap(row_block_index: u32, row_block_offset: u32, count: u32) -> Self {
        Self {
            row_block_index,
            row_block_offset,
            heap_block_index: INVALID_INDEX,
            heap_block_offset: INVALID_INDEX,
            total_heap_size: 0,
            count,
            heap_base_address: Arc::new(Mutex::new(None)),
        }
    }

    /// Mark heap as empty.
    pub fn set_heap_empty(&mut self) {
        self.heap_block_index = INVALID_INDEX;
        self.heap_block_offset = INVALID_INDEX;
        self.total_heap_size = 0;
        *self.heap_base_address.lock().unwrap() = None;
    }

    /// Check if this part has heap data.
    pub fn has_heap(&self) -> bool {
        self.heap_block_index != INVALID_INDEX
    }
}

/// Persistent handles to keep blocks pinned in a segment.
#[derive(Debug, Default)]
pub struct SegmentPinnedHandles {
    /// Handles to pinned row blocks
    pub row_handles: Vec<BufferHandle>,
    /// Handles to pinned heap blocks
    pub heap_handles: Vec<BufferHandle>,
}

/// A logical chunk of rows (up to `VECTOR_SIZE` rows).
///
/// Contains one or more RawRowChunkPart that together form
/// a complete chunk. Tracks which blocks are referenced.
#[derive(Debug)]
pub struct RawRowChunk {
    /// Indices of parts in the segment's chunk_parts vector
    pub part_indices: ContinuousIdSet,
    /// Row block IDs referenced by this chunk
    pub row_block_ids: ContinuousIdSet,
    /// Heap block IDs referenced by this chunk
    pub heap_block_ids: ContinuousIdSet,
    /// Total row count in this chunk
    pub count: usize,
}

impl Default for RawRowChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl RawRowChunk {
    /// Create a new empty chunk.
    pub fn new() -> Self {
        Self {
            part_indices: ContinuousIdSet::new(),
            row_block_ids: ContinuousIdSet::new(),
            heap_block_ids: ContinuousIdSet::new(),
            count: 0,
        }
    }

    /// Add a part to this chunk.
    ///
    /// # Arguments
    /// * `part` - The part to add
    /// * `all_constant` - Whether the layout has all constant-size columns
    ///
    /// # Returns
    /// The part that was added (for further processing).
    pub fn add_part_info(&mut self, part: &RawRowChunkPart, all_constant: bool) {
        self.count += part.count as usize;
        self.row_block_ids.insert(part.row_block_index);

        if !all_constant && part.total_heap_size > 0 {
            self.heap_block_ids.insert(part.heap_block_index);
        }
    }

    /// Get the number of parts in this chunk.
    pub fn part_count(&self) -> usize {
        if self.part_indices.is_empty() {
            0
        } else {
            self.part_indices.size() as usize
        }
    }

    /// Verify the chunk's counts match the sum of its parts.
    #[cfg(debug_assertions)]
    pub fn verify(&self, segment: &RawRowSegment) {
        if self.part_indices.is_empty() {
            assert_eq!(self.count, 0);
            return;
        }

        let mut total_count = 0usize;
        for part_id in self.part_indices.start()..self.part_indices.end() {
            total_count += segment.chunk_parts[part_id as usize].count as usize;
        }
        assert_eq!(self.count, total_count);
    }
}

/// A segment of raw row data with a shared allocator.
///
/// Contains multiple chunks and tracks total count/size.
/// This is the main unit of data organization in RawRowCollection.
#[derive(Debug)]
pub struct RawRowSegment {
    /// The allocator for this segment
    allocator: Arc<RawRowAllocator>,
    /// The chunks in this segment
    pub chunks: Vec<RawRowChunk>,
    /// All chunk parts (referenced by chunks via indices)
    pub chunk_parts: Vec<RawRowChunkPart>,
    /// Total row count
    pub count: usize,
    /// Total data size in bytes
    pub data_size: usize,
    /// Persistent handles to keep blocks pinned
    pub pinned_handles: Mutex<SegmentPinnedHandles>,
}

impl RawRowSegment {
    /// Create a new segment with the given allocator.
    pub fn new(allocator: Arc<RawRowAllocator>) -> Self {
        Self {
            allocator,
            chunks: Vec::new(),
            chunk_parts: Vec::new(),
            count: 0,
            data_size: 0,
            pinned_handles: Mutex::new(SegmentPinnedHandles::default()),
        }
    }

    /// Unpin all persistent handles in this segment.
    pub fn unpin(&self) {
        if let Ok(mut handles) = self.pinned_handles.lock() {
            handles.row_handles.clear();
            handles.heap_handles.clear();
        }
    }

    /// Get the allocator for this segment.
    #[inline]
    pub fn allocator(&self) -> &Arc<RawRowAllocator> {
        &self.allocator
    }

    /// Get a mutable reference to the allocator.
    ///
    /// # Safety
    /// This is safe because RawRowAllocator uses interior mutability
    /// for its internal state (blocks are managed through Arc/Mutex).
    #[inline]
    pub fn allocator_mut(&mut self) -> &mut RawRowAllocator {
        // SAFETY: We have exclusive mutable access to the segment,
        // and RawRowAllocator uses interior mutability for thread-safe operations
        unsafe {
            let ptr = Arc::as_ptr(&self.allocator) as *mut RawRowAllocator;
            &mut *ptr
        }
    }

    /// Get the layout for this segment.
    #[inline]
    pub fn layout(&self) -> &RawRowLayout {
        self.allocator.layout()
    }

    /// Get the number of chunks in this segment.
    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get the size in bytes of this segment.
    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.data_size
    }

    /// Get the total row count.
    #[inline]
    pub fn row_count(&self) -> usize {
        self.count
    }

    /// Get a reference to the chunks.
    #[inline]
    pub fn chunks(&self) -> &[RawRowChunk] {
        &self.chunks
    }

    /// Get a reference to the chunk parts.
    #[inline]
    pub fn chunk_parts(&self) -> &[RawRowChunkPart] {
        &self.chunk_parts
    }

    /// Add a new chunk to this segment.
    pub fn add_chunk(&mut self, chunk: RawRowChunk) {
        self.count += chunk.count;
        self.chunks.push(chunk);
    }

    /// Create a new chunk and add it to this segment.
    /// Returns the index of the new chunk.
    pub fn create_chunk(&mut self) -> usize {
        self.chunks.push(RawRowChunk::new());
        self.chunks.len() - 1
    }

    /// Get the last chunk index, or create one if empty or if the last chunk is full.
    ///
    /// Each chunk must have at most `STANDARD_VECTOR_SIZE` rows so scan state
    /// can map positions to chunk boundaries without extra indirection.
    pub fn get_or_create_chunk_index(&mut self) -> usize {
        const STANDARD_VECTOR_SIZE: usize = 2048;

        // Create a new chunk if:
        // 1. No chunks exist, or
        // 2. The last chunk is full (count >= STANDARD_VECTOR_SIZE)
        if self.chunks.is_empty() || self.chunks.last().unwrap().count >= STANDARD_VECTOR_SIZE {
            self.chunks.push(RawRowChunk::new());
        }
        self.chunks.len() - 1
    }

    /// Add a part to a specific chunk.
    ///
    /// # Arguments
    /// * `chunk_index` - Index of the chunk to add to
    /// * `part` - The part to add
    ///
    /// # Returns
    /// The index of the added part in chunk_parts.
    pub fn add_part_to_chunk(&mut self, chunk_index: usize, part: RawRowChunkPart) -> usize {
        const STANDARD_VECTOR_SIZE: usize = 2048;
        let row_width = self.allocator.layout().get_row_width();
        let all_constant = self.allocator.layout().all_constant();
        let part_index = self.chunk_parts.len();

        // Update counts
        self.count += part.count as usize;
        self.data_size += (part.count as usize) * row_width + part.total_heap_size;

        // Update chunk
        let chunk = &mut self.chunks[chunk_index];
        chunk.add_part_info(&part, all_constant);
        debug_assert!(
            chunk.count <= STANDARD_VECTOR_SIZE,
            "raw row chunk exceeded STANDARD_VECTOR_SIZE: chunk_index={}, count={}, limit={STANDARD_VECTOR_SIZE}",
            chunk_index,
            chunk.count
        );
        chunk.part_indices.insert(part_index as u32);

        // Store part
        self.chunk_parts.push(part);
        part_index
    }

    /// Update the data size based on added rows.
    pub fn add_data_size(&mut self, row_count: usize, heap_size: usize) {
        let row_width = self.allocator.layout().get_row_width();
        self.data_size += row_count * row_width + heap_size;
    }

    /// Get a chunk part by index.
    pub fn get_chunk_part(&self, index: usize) -> Option<&RawRowChunkPart> {
        self.chunk_parts.get(index)
    }

    /// Get a mutable chunk part by index.
    pub fn get_chunk_part_mut(&mut self, index: usize) -> Option<&mut RawRowChunkPart> {
        self.chunk_parts.get_mut(index)
    }

    /// Verify the segment's integrity.
    #[cfg(debug_assertions)]
    pub fn verify(&self) {
        let row_width = self.allocator.layout().get_row_width();
        let all_constant = self.allocator.layout().all_constant();

        let mut total_count = 0usize;
        let mut total_size = 0usize;

        for chunk in &self.chunks {
            chunk.verify(self);
            total_count += chunk.count;

            total_size += chunk.count * row_width;
            if !all_constant && !chunk.part_indices.is_empty() {
                for part_id in chunk.part_indices.start()..chunk.part_indices.end() {
                    total_size += self.chunk_parts[part_id as usize].total_heap_size;
                }
            }
        }

        assert_eq!(total_count, self.count);
        assert_eq!(total_size, self.data_size);
    }
}

impl Drop for RawRowSegment {
    fn drop(&mut self) {
        self.unpin();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, MemoryTag};
    use crate::row::raw::RawRowValidityType;
    use paro_common::types::LogicalType;

    fn create_test_segment(types: Vec<LogicalType>) -> RawRowSegment {
        let pool = Arc::new(BufferPool::new(10 * 1024 * 1024));
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        let allocator = Arc::new(RawRowAllocator::new(
            pool,
            Arc::new(layout),
            MemoryTag::HashTable,
        ));
        RawRowSegment::new(allocator)
    }

    #[test]
    fn test_continuous_id_set() {
        let mut set = ContinuousIdSet::new();
        assert!(set.is_empty());

        set.insert(5);
        assert!(!set.is_empty());
        assert_eq!(set.start(), 5);
        assert_eq!(set.end(), 6);
        assert_eq!(set.size(), 1);
        assert!(set.contains(5));
        assert!(!set.contains(4));
        assert!(!set.contains(6));

        set.insert(3);
        assert_eq!(set.start(), 3);
        assert_eq!(set.end(), 6);
        assert_eq!(set.size(), 3);
        assert!(set.contains(3));
        assert!(set.contains(4));
        assert!(set.contains(5));

        set.insert(7);
        assert_eq!(set.start(), 3);
        assert_eq!(set.end(), 8);
        assert_eq!(set.size(), 5);
    }

    #[test]
    fn test_chunk_part_creation() {
        let part = RawRowChunkPart::new(0, 100, 1, 200, 500, 10);
        assert_eq!(part.row_block_index, 0);
        assert_eq!(part.row_block_offset, 100);
        assert_eq!(part.heap_block_index, 1);
        assert_eq!(part.heap_block_offset, 200);
        assert_eq!(part.total_heap_size, 500);
        assert_eq!(part.count, 10);
        assert!(part.has_heap());

        let part_no_heap = RawRowChunkPart::new_without_heap(0, 0, 50);
        assert!(!part_no_heap.has_heap());
        assert_eq!(part_no_heap.total_heap_size, 0);
    }

    #[test]
    fn test_chunk_add_part() {
        let mut segment = create_test_segment(vec![LogicalType::Integer, LogicalType::BigInt]);
        let chunk_idx = segment.create_chunk();

        let part1 = RawRowChunkPart::new_without_heap(0, 0, 100);
        segment.add_part_to_chunk(chunk_idx, part1);

        assert_eq!(segment.chunks[chunk_idx].count, 100);
        assert_eq!(segment.chunks[chunk_idx].part_count(), 1);
        assert!(segment.chunks[chunk_idx].row_block_ids.contains(0));
        assert_eq!(segment.chunk_parts.len(), 1);

        let part2 = RawRowChunkPart::new_without_heap(0, 1300, 50);
        segment.add_part_to_chunk(chunk_idx, part2);

        assert_eq!(segment.chunks[chunk_idx].count, 150);
        assert_eq!(segment.chunks[chunk_idx].part_count(), 2);
        assert_eq!(segment.chunk_parts.len(), 2);
    }

    #[test]
    fn test_chunk_with_heap() {
        let mut segment = create_test_segment(vec![LogicalType::Integer, LogicalType::Varchar]);
        let chunk_idx = segment.create_chunk();

        let part = RawRowChunkPart::new(0, 0, 0, 0, 1024, 50);
        segment.add_part_to_chunk(chunk_idx, part);

        assert_eq!(segment.chunks[chunk_idx].count, 50);
        assert!(segment.chunks[chunk_idx].row_block_ids.contains(0));
        assert!(segment.chunks[chunk_idx].heap_block_ids.contains(0));
    }

    #[test]
    fn test_segment_creation() {
        let segment = create_test_segment(vec![LogicalType::Integer]);
        assert_eq!(segment.chunk_count(), 0);
        assert_eq!(segment.row_count(), 0);
        assert_eq!(segment.size_in_bytes(), 0);
    }

    #[test]
    fn test_segment_add_chunk() {
        let mut segment = create_test_segment(vec![LogicalType::Integer, LogicalType::BigInt]);

        let chunk_idx = segment.create_chunk();
        let part = RawRowChunkPart::new_without_heap(0, 0, 100);
        segment.add_part_to_chunk(chunk_idx, part);

        // Note: add_part_to_chunk now automatically updates count and data_size
        assert_eq!(segment.chunk_count(), 1);
        assert_eq!(segment.row_count(), 100);
        // row_width = 1 (validity) + 4 (int) + 8 (bigint) = 13
        assert_eq!(segment.size_in_bytes(), 100 * 13);
    }

    #[test]
    fn test_segment_multiple_chunks() {
        let mut segment = create_test_segment(vec![LogicalType::Double]);

        // Add first chunk
        let chunk1_idx = segment.create_chunk();
        let part1 = RawRowChunkPart::new_without_heap(0, 0, 200);
        segment.add_part_to_chunk(chunk1_idx, part1);

        // Add second chunk
        let chunk2_idx = segment.create_chunk();
        let part2 = RawRowChunkPart::new_without_heap(0, 1800, 150);
        segment.add_part_to_chunk(chunk2_idx, part2);

        // Note: add_part_to_chunk now automatically updates count and data_size
        assert_eq!(segment.chunk_count(), 2);
        assert_eq!(segment.row_count(), 350);
        assert_eq!(segment.chunks[0].count, 200);
        assert_eq!(segment.chunks[1].count, 150);
    }

    #[test]
    fn test_get_or_create_chunk() {
        let mut segment = create_test_segment(vec![LogicalType::Integer]);

        // First call creates a chunk
        let chunk1_idx = segment.get_or_create_chunk_index();
        let part = RawRowChunkPart::new_without_heap(0, 0, 50);
        segment.add_part_to_chunk(chunk1_idx, part);
        assert_eq!(segment.chunk_count(), 1);

        // Second call returns the same chunk index
        let chunk2_idx = segment.get_or_create_chunk_index();
        assert_eq!(chunk1_idx, chunk2_idx);
        assert_eq!(segment.chunks[chunk2_idx].count, 50);
        assert_eq!(segment.chunk_count(), 1);
    }
}
