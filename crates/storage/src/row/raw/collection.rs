// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Raw segmented storage backing execution-time row stores.

use std::sync::Arc;
use std::sync::Mutex;

use crate::buffer::{BufferPool, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

use super::{
    RawRowAllocator, RawRowAppendState, RawRowChunkPart, RawRowChunkState, RawRowLayout,
    RawRowParallelScanState, RawRowPinProperties, RawRowPinState, RawRowScanState, RawRowSegment,
    RawRowValidityType,
};

/// A collection of raw row data.
///
/// This is the low-level segmented backing for execution row stores.
///
/// Long-term ownership is deliberately narrow: spill/replay, retained varlen
/// rows, sort runs, and late gather may use it; ordinary high-frequency
/// columnar pipelines should exchange `Chunk`/`Vector` data directly.
///
/// # Design
/// - Data is organized into segments, each with its own allocator
/// - Each segment contains multiple chunks (≤ VECTOR_SIZE rows each)
/// - Supports efficient append and scan operations
#[derive(Debug)]
pub struct RawRowCollection {
    /// Buffer pool for memory allocation
    buffer_pool: Arc<BufferPool>,
    /// Layout of the stored rows
    layout: Arc<RawRowLayout>,
    /// Memory tag for tracking
    tag: MemoryTag,
    /// Logical memory owner for newly allocated row blocks.
    memory: MemoryAccountingContext,
    /// Data segments
    segments: Vec<RawRowSegment>,
    /// Total row count
    count: usize,
    /// Total data size in bytes
    data_size: usize,
    /// Destroyed prefix metadata for external scan cleanup.
    destroyed_prefix: Mutex<DestroyedPrefixState>,
}

#[derive(Debug, Default)]
struct DestroyedPrefixState {
    chunk_prefix: usize,
    row_block_prefix: usize,
    heap_block_prefix: usize,
}

impl RawRowCollection {
    /// Create a new RawRowCollection with the specified layout.
    ///
    /// # Arguments
    /// * `buffer_pool` - Buffer pool for memory allocation
    /// * `layout` - Layout of the rows to store
    /// * `tag` - Memory tag for tracking
    pub fn new(buffer_pool: Arc<BufferPool>, layout: Arc<RawRowLayout>, tag: MemoryTag) -> Self {
        let memory =
            MemoryAccountingContext::detached(tag, MemoryAccountingClass::default_for_tag(tag));
        Self::new_with_memory(buffer_pool, layout, tag, memory)
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RawRowLayout>,
        tag: MemoryTag,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            buffer_pool,
            layout,
            tag,
            memory,
            segments: Vec::new(),
            count: 0,
            data_size: 0,
            destroyed_prefix: Mutex::new(DestroyedPrefixState::default()),
        }
    }

    /// Create a new RawRowCollection from column types.
    ///
    /// # Arguments
    /// * `buffer_pool` - Buffer pool for memory allocation
    /// * `types` - Column types
    /// * `tag` - Memory tag for tracking
    pub fn from_types(
        buffer_pool: Arc<BufferPool>,
        types: Vec<LogicalType>,
        tag: MemoryTag,
    ) -> Self {
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        Self::new(buffer_pool, Arc::new(layout), tag)
    }

    /// Create a new RawRowCollection with the same layout.
    pub fn create_empty(&self) -> Self {
        Self::new_with_memory(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.layout),
            self.tag,
            self.memory.clone(),
        )
    }

    fn new_allocator(&self) -> Arc<RawRowAllocator> {
        Arc::new(RawRowAllocator::new_with_memory(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.layout),
            self.tag,
            self.memory.clone(),
        ))
    }

    // === Accessor Methods ===

    /// Get the buffer pool.
    #[inline]
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Get the layout.
    #[inline]
    pub fn layout(&self) -> &RawRowLayout {
        &self.layout
    }

    /// Get the layout as Arc.
    #[inline]
    pub fn layout_ptr(&self) -> Arc<RawRowLayout> {
        Arc::clone(&self.layout)
    }

    /// Get the memory tag.
    #[inline]
    pub fn tag(&self) -> MemoryTag {
        self.tag
    }

    /// Get the number of rows stored.
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the number of chunks stored.
    pub fn chunk_count(&self) -> usize {
        self.segments.iter().map(|s| s.chunk_count()).sum()
    }

    /// Get the size in bytes.
    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.data_size
    }

    /// Get the number of segments.
    #[inline]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Get a reference to the segments.
    #[inline]
    pub fn segments(&self) -> &[RawRowSegment] {
        &self.segments
    }

    /// Finalize a pin state, releasing or storing handles.
    pub fn finalize_pin_state(&self, pin_state: &mut RawRowPinState) {
        for segment in &self.segments {
            segment
                .allocator()
                .release_or_store_handles(pin_state, &segment.pinned_handles);
        }
    }

    /// Unpin all segments in the collection.
    pub fn unpin(&self) {
        for segment in &self.segments {
            segment.unpin();
        }
    }

    /// Pin all blocks in the collection.
    ///
    /// This is useful when we need random access to the entire collection,
    /// for example during reordering.
    pub fn pin_all(&self, pin_state: &mut RawRowPinState) -> Result<(), String> {
        for segment in &self.segments {
            let allocator = segment.allocator();
            for chunk_part in segment.chunk_parts() {
                allocator
                    .get_row_pointer(pin_state, chunk_part)
                    .map_err(|e| e.to_string())?;

                if chunk_part.has_heap() {
                    allocator
                        .pin_heap_block(pin_state, chunk_part.heap_block_index as usize)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }

    /// How many rows fit per block.
    pub fn rows_per_block(&self) -> usize {
        let row_width = self.layout.get_row_width();
        if row_width == 0 {
            return 0;
        }
        crate::buffer::DEFAULT_BLOCK_SIZE / row_width
    }

    /// Check if the collection is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Fetch a chunk for scanning, initializing the scan state.
    ///
    /// This method prepares the scan state for accessing a specific chunk,
    /// pinning the necessary blocks in memory.
    ///
    /// Supports swizzling by recomputing pointers when blocks move after eviction.
    /// Heap pointers and sort key payload pointers stay aligned through the
    /// allocator's chunk-state initialization.
    ///
    /// # Arguments
    /// * `state` - The scan state to initialize
    /// * `chunk_idx` - The chunk index to fetch
    /// * `init_heap` - Whether to initialize heap pointers
    ///
    /// # Returns
    /// The number of rows in the chunk, or an error if the chunk index is invalid
    pub fn fetch_chunk(
        &self,
        state: &mut RawRowScanState,
        chunk_idx: usize,
        init_heap: bool,
    ) -> Result<usize, String> {
        let mut remaining_idx = chunk_idx;

        for (segment_idx, segment) in self.segments.iter().enumerate() {
            let segment_chunk_count = segment.chunk_count();

            if remaining_idx < segment_chunk_count {
                // Found the segment containing this chunk
                state.segment_index = Some(segment_idx);
                state.chunk_index = Some(remaining_idx);

                let row_chunk = &segment.chunks()[remaining_idx];
                let allocator = segment.allocator();

                // Prepare row_locations vector
                state
                    .chunk_state
                    .row_locations
                    .try_set_count(row_chunk.count)
                    .map_err(|err| err.to_string())?;

                // Collect chunk parts for this chunk
                let mut chunk_parts_refs: Vec<&RawRowChunkPart> = Vec::new();
                for part_idx in row_chunk.part_indices.start()..row_chunk.part_indices.end() {
                    let part = &segment.chunk_parts()[part_idx as usize];
                    chunk_parts_refs.push(part);
                }

                // Use initialize_chunk_state with recompute=true so pointer swizzling
                // can detect block address changes after eviction.
                allocator
                    .initialize_chunk_state(
                        &mut state.pin_state,
                        &mut state.chunk_state,
                        0,         // offset
                        true,      // recompute=true to enable swizzling
                        init_heap, // init_heap_pointers
                        false,     // init_heap_sizes (not needed for fetch)
                        &chunk_parts_refs,
                    )
                    .map_err(|e| e.to_string())?;

                return Ok(row_chunk.count);
            }

            remaining_idx -= segment_chunk_count;
        }

        Err(format!(
            "Chunk index {} out of bounds in RawRowCollection",
            chunk_idx
        ))
    }

    // === Append Operations ===

    /// Initialize an append state for appending data.
    ///
    /// # Arguments
    /// * `append_state` - State to initialize
    /// * `properties` - Pin properties for the operation
    pub fn initialize_append(
        &mut self,
        append_state: &mut RawRowAppendState,
        properties: RawRowPinProperties,
    ) {
        self.initialize_append_pin_state(&mut append_state.pin_state, properties);
        self.initialize_chunk_state(&mut append_state.chunk_state, None);
    }

    /// Initialize the pin state for appending.
    pub fn initialize_append_pin_state(
        &mut self,
        pin_state: &mut RawRowPinState,
        properties: RawRowPinProperties,
    ) {
        pin_state.properties = properties;

        // Ensure we have at least one segment
        if self.segments.is_empty() {
            self.segments.push(RawRowSegment::new(self.new_allocator()));
        }
    }

    /// Initialize the chunk state for appending.
    ///
    /// # Arguments
    /// * `chunk_state` - State to initialize
    /// * `column_ids` - Optional column IDs to operate on (None = all columns)
    pub fn initialize_chunk_state(
        &self,
        chunk_state: &mut RawRowChunkState,
        column_ids: Option<Vec<usize>>,
    ) {
        let ids = column_ids.unwrap_or_else(|| (0..self.layout.column_count()).collect());
        chunk_state.set_column_ids(ids.clone());
        chunk_state.initialize_array_cast_vectors(self.layout.get_types(), &ids);
    }

    /// Append a Chunk to the collection.
    ///
    /// This method decodes the chunk once, allocates row and heap storage,
    /// scatters the values into that storage, and updates collection statistics.
    ///
    /// # Arguments
    /// * `chunk` - The data chunk to append
    ///
    /// # Returns
    /// Tuple of (count, row_locations, heap_locations) where:
    /// - count: Number of rows appended
    /// - row_locations: Pointers to the start of each row
    /// - heap_locations: Pointers to heap storage (for variable-length data)
    ///
    pub fn append(
        &mut self,
        append_state: &mut RawRowAppendState,
        chunk: &Chunk,
    ) -> paro_common::error::Result<(usize, Vec<*mut u8>, Vec<*mut u8>)> {
        use super::scatter;

        let count = chunk.size();
        if count == 0 {
            return Ok((0, Vec::new(), Vec::new()));
        }

        // Decode the input vectors once as a borrowed view, then compute heap sizes.
        let mut chunk_view = append_state.chunk_state.try_decode(chunk)?;
        let heap_sizes = scatter::compute_heap_sizes(&self.layout, &mut chunk_view, None, count);

        // Get or create the target segment.
        if self.segments.is_empty() {
            self.segments.push(RawRowSegment::new(self.new_allocator()));
        }

        let (row_locations, mut heap_locations) = {
            let segment = self.segments.last_mut().unwrap();

            scatter::build_rows(
                &mut append_state.pin_state,
                segment,
                &self.layout,
                &heap_sizes,
                count,
            )?
        };
        // allocator borrow ends here

        scatter::scatter_chunk(
            &self.layout,
            chunk,
            &mut chunk_view,
            &row_locations,
            &mut heap_locations,
            None,
            count,
        );

        let total_heap_size: usize = heap_sizes.iter().sum();
        self.add_count(count, total_heap_size);

        self.release_append_handles_if_needed(append_state);

        Ok((count, row_locations, heap_locations))
    }

    /// Append selected rows from a Chunk to the collection.
    ///
    /// This path mirrors [`append`] but passes a selection vector through heap-size
    /// computation and scatter, allowing partitioned append without materializing
    /// per-partition Chunks.
    pub fn append_with_sel(
        &mut self,
        append_state: &mut RawRowAppendState,
        chunk: &Chunk,
        sel: &SelectionVector,
        count: usize,
    ) -> paro_common::error::Result<usize> {
        use super::scatter;

        if count == 0 {
            return Ok(0);
        }

        let mut chunk_view = append_state.chunk_state.try_decode(chunk)?;
        let heap_sizes =
            scatter::compute_heap_sizes(&self.layout, &mut chunk_view, Some(sel), count);

        if self.segments.is_empty() {
            self.segments.push(RawRowSegment::new(self.new_allocator()));
        }

        let (row_locations, mut heap_locations) = {
            let segment = self.segments.last_mut().unwrap();
            scatter::build_rows(
                &mut append_state.pin_state,
                segment,
                &self.layout,
                &heap_sizes,
                count,
            )?
        };

        scatter::scatter_chunk(
            &self.layout,
            chunk,
            &mut chunk_view,
            &row_locations,
            &mut heap_locations,
            Some(sel),
            count,
        );

        let total_heap_size: usize = heap_sizes.iter().sum();
        self.add_count(count, total_heap_size);
        self.release_append_handles_if_needed(append_state);
        Ok(count)
    }

    /// Finalize append by transferring pinned handles to the segment.
    ///
    /// This should be called after all appends are complete to ensure
    /// that blocks stay pinned (if KeepEverythingPinned was used) or
    /// are released correctly.
    pub fn finalize_append(&mut self, state: &mut RawRowAppendState) {
        if let Some(segment) = self.segments.last_mut() {
            segment
                .allocator()
                .release_or_store_handles(&mut state.pin_state, &segment.pinned_handles);
        }
    }

    fn release_append_handles_if_needed(&mut self, state: &mut RawRowAppendState) {
        if state.pin_state.properties != RawRowPinProperties::UnpinAfterDone {
            return;
        }
        if let Some(segment) = self.segments.last_mut() {
            segment
                .allocator()
                .release_or_store_handles(&mut state.pin_state, &segment.pinned_handles);
        }
    }

    // === Gather Operations ===

    /// Gather a single column from specific row locations.
    ///
    /// This is the core gather operation that reads data from row-based storage
    /// back into columnar format.
    ///
    /// # Arguments
    /// * `row_indices` - Indices of rows to gather (in the original insertion order)
    /// * `column_idx` - Index of the column to gather
    /// * `output` - Output vector to write to
    /// * `count` - Number of rows to gather
    ///
    /// # Implementation Note
    /// This is a simplified version of Gather. The original uses row_locations
    /// (pointers) and supports more complex scenarios. We use indices since our
    /// current implementation doesn't expose row pointers from append operations.
    ///
    pub fn gather_column(
        &self,
        row_indices: &[usize],
        column_idx: usize,
        output: &mut paro_common::vector::Vector,
    ) -> paro_common::error::Result<()> {
        use super::gather;

        let count = row_indices.len();
        if count == 0 {
            output.try_set_count(0)?;
            return Ok(());
        }

        // Gather data from segments
        gather::gather_column(
            &self.segments,
            &self.layout,
            row_indices,
            column_idx,
            output,
            count,
        )?;

        output.try_set_count(count)?;
        Ok(())
    }

    // === Scan Operations ===

    /// Initialize a scan state for scanning all columns.
    ///
    /// # Arguments
    /// * `state` - State to initialize
    /// * `properties` - Pin properties for the operation
    pub fn initialize_scan(&self, state: &mut RawRowScanState, properties: RawRowPinProperties) {
        self.initialize_scan_with_columns(state, None, properties);
    }

    /// Initialize a scan state for scanning specific columns.
    ///
    /// # Arguments
    /// * `state` - State to initialize
    /// * `column_ids` - Optional column IDs to scan (None = all columns)
    /// * `properties` - Pin properties for the operation
    pub fn initialize_scan_with_columns(
        &self,
        state: &mut RawRowScanState,
        column_ids: Option<Vec<usize>>,
        properties: RawRowPinProperties,
    ) {
        state.pin_state.properties = properties;
        let ids = column_ids.unwrap_or_else(|| (0..self.layout.column_count()).collect());
        state.chunk_state.set_column_ids(ids.clone());
        state
            .chunk_state
            .initialize_array_cast_vectors(self.layout.get_types(), &ids);
        state.segment_index = if self.segments.is_empty() {
            None
        } else {
            Some(0)
        };
        state.chunk_index = if self.chunk_count() == 0 {
            None
        } else {
            Some(0)
        };
    }

    /// Initialize a parallel scan state for scanning all columns.
    pub fn initialize_parallel_scan(
        &self,
        state: &mut RawRowParallelScanState,
        properties: RawRowPinProperties,
    ) {
        self.initialize_parallel_scan_with_columns(state, None, properties);
    }

    /// Initialize a parallel scan state for scanning specific columns.
    pub fn initialize_parallel_scan_with_columns(
        &self,
        state: &mut RawRowParallelScanState,
        column_ids: Option<Vec<usize>>,
        properties: RawRowPinProperties,
    ) {
        self.initialize_scan_with_columns(&mut state.scan_state, column_ids, properties);
    }

    /// Check if the scan is complete.
    pub fn scan_complete(&self, state: &RawRowScanState) -> bool {
        match (state.segment_index, state.chunk_index) {
            (None, _) | (_, None) => true,
            (Some(seg_idx), Some(chunk_idx)) => {
                if seg_idx >= self.segments.len() {
                    return true;
                }
                // Check if we've reached the end of current segment
                let segment = &self.segments[seg_idx];
                chunk_idx >= segment.chunk_count()
            }
        }
    }

    /// Advance to the next chunk in the scan.
    ///
    /// Returns true if there is a next chunk, false if scan is complete.
    pub fn next_scan_index(&self, state: &mut RawRowScanState) -> bool {
        let (seg_idx, chunk_idx) = match (state.segment_index, state.chunk_index) {
            (Some(s), Some(c)) => (s, c),
            _ => return false,
        };

        if seg_idx >= self.segments.len() {
            return false;
        }

        let segment = &self.segments[seg_idx];
        let next_chunk = chunk_idx + 1;

        if next_chunk < segment.chunk_count() {
            state.chunk_index = Some(next_chunk);
            true
        } else {
            // Move to next segment
            let next_seg = seg_idx + 1;
            if next_seg < self.segments.len() {
                state.segment_index = Some(next_seg);
                state.chunk_index = Some(0);
                true
            } else {
                state.segment_index = None;
                state.chunk_index = None;
                false
            }
        }
    }

    /// Scan one chunk using a shared global parallel scan state and a local scan state.
    ///
    /// Returns number of rows scanned (0 when scan is complete).
    pub fn scan_parallel(
        &self,
        gstate: &mut RawRowParallelScanState,
        lstate: &mut RawRowScanState,
        chunk: &mut Chunk,
    ) -> paro_common::error::Result<usize> {
        // Keep local pin behavior aligned with global scan properties.
        lstate.pin_state.properties = gstate.scan_state.pin_state.properties;

        let assigned = {
            let _guard = gstate.lock.lock().unwrap();
            if self.scan_complete(&gstate.scan_state) {
                chunk.try_set_cardinality(0)?;
                return Ok(0);
            }

            let assigned = match (
                gstate.scan_state.segment_index,
                gstate.scan_state.chunk_index,
            ) {
                (Some(seg_idx), Some(chunk_idx)) => Some((seg_idx, chunk_idx)),
                _ => None,
            };
            // Advance global state for the next worker.
            let _ = self.next_scan_index(&mut gstate.scan_state);
            assigned
        };

        let Some((segment_idx, chunk_idx)) = assigned else {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        };

        self.scan_chunk_at(segment_idx, chunk_idx, &mut lstate.pin_state, chunk)
    }

    // === Combine/Reset Operations ===

    /// Combine another collection into this one, consuming the other.
    ///
    /// Both collections must have the same layout.
    pub fn combine(&mut self, mut other: RawRowCollection) {
        debug_assert_eq!(
            self.layout.get_row_width(),
            other.layout.get_row_width(),
            "Cannot combine collections with different layouts"
        );

        self.count += other.count;
        self.data_size += other.data_size;
        self.segments.append(&mut other.segments);
    }

    /// Reset the collection, clearing all data.
    pub fn reset(&mut self) {
        self.unpin();
        self.segments.clear();
        self.count = 0;
        self.data_size = 0;
        *self.destroyed_prefix.lock().unwrap() = DestroyedPrefixState::default();
    }

    /// Destroy a chunk prefix that has already been fully consumed by an external scan.
    ///
    /// This currently supports the single-segment collections used by sorted runs.
    pub fn destroy_chunks(&self, _chunk_begin: usize, chunk_end: usize) {
        if self.segments.is_empty() || chunk_end == 0 {
            return;
        }

        let segment = &self.segments[0];
        if segment.chunk_count() == 0 {
            return;
        }

        let target_chunk_end = chunk_end.min(segment.chunk_count());
        let mut destroyed_prefix = self.destroyed_prefix.lock().unwrap();
        if target_chunk_end <= destroyed_prefix.chunk_prefix {
            return;
        }

        let row_block_end = if target_chunk_end < segment.chunk_count() {
            segment.chunks[target_chunk_end].row_block_ids.start() as usize
        } else {
            segment.allocator().row_block_count()
        };

        let heap_block_end = if self.layout.all_constant() {
            0
        } else {
            self.next_heap_block_boundary(segment, target_chunk_end)
        };

        let allocator = unsafe {
            let ptr = Arc::as_ptr(segment.allocator()) as *mut RawRowAllocator;
            &mut *ptr
        };
        allocator.release_row_blocks_range(destroyed_prefix.row_block_prefix, row_block_end);
        allocator.release_heap_blocks_range(destroyed_prefix.heap_block_prefix, heap_block_end);

        destroyed_prefix.chunk_prefix = target_chunk_end;
        destroyed_prefix.row_block_prefix = row_block_end;
        destroyed_prefix.heap_block_prefix = heap_block_end;
    }

    // === Internal Helpers ===

    fn scan_chunk_at(
        &self,
        segment_idx: usize,
        chunk_idx: usize,
        pin_state: &mut RawRowPinState,
        chunk: &mut Chunk,
    ) -> paro_common::error::Result<usize> {
        let Some(segment) = self.get_segment(segment_idx) else {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        };

        if chunk_idx >= segment.chunks.len() {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        }

        let row_chunk = &segment.chunks[chunk_idx];
        if row_chunk.count == 0 || row_chunk.part_indices.is_empty() {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        }

        let layout = self.layout();
        let row_width = layout.get_row_width();
        let allocator = segment.allocator();
        let mut row_locations: Vec<*const u8> = Vec::with_capacity(row_chunk.count);

        for part_idx in row_chunk.part_indices.start()..row_chunk.part_indices.end() {
            let part = &segment.chunk_parts[part_idx as usize];
            let block_ptr = match allocator.get_row_pointer(pin_state, part) {
                Ok(p) => p,
                Err(_) => continue,
            };

            for row_in_part in 0..part.count {
                let offset = row_in_part as usize * row_width;
                let row_ptr = unsafe { block_ptr.add(offset) };
                row_locations.push(row_ptr as *const u8);
            }
        }

        let count = row_locations.len();
        if count == 0 {
            chunk.try_set_cardinality(0)?;
            return Ok(0);
        }

        super::gather::gather_chunk(self, &row_locations, chunk, count)?;
        Ok(count)
    }

    /// Get a segment by index.
    pub fn get_segment(&self, index: usize) -> Option<&RawRowSegment> {
        self.segments.get(index)
    }

    /// Get a mutable segment by index.
    pub fn get_segment_mut(&mut self, index: usize) -> Option<&mut RawRowSegment> {
        self.segments.get_mut(index)
    }

    /// Add count and data size (called after successful append).
    pub fn add_count(&mut self, row_count: usize, heap_size: usize) {
        self.count += row_count;
        let row_width = self.layout.get_row_width();
        self.data_size += row_count * row_width + heap_size;
    }

    /// Verify the collection's integrity.
    #[cfg(debug_assertions)]
    pub fn verify(&self) {
        let mut total_count = 0usize;
        let mut total_size = 0usize;

        for segment in &self.segments {
            total_count += segment.row_count();
            total_size += segment.size_in_bytes();
        }

        assert_eq!(total_count, self.count);
        assert_eq!(total_size, self.data_size);
    }

    fn next_heap_block_boundary(&self, segment: &RawRowSegment, chunk_end: usize) -> usize {
        for chunk_idx in chunk_end..segment.chunk_count() {
            let chunk = &segment.chunks[chunk_idx];
            if !chunk.heap_block_ids.is_empty() {
                return chunk.heap_block_ids.start() as usize;
            }
        }
        segment.allocator().heap_block_count()
    }
}

impl Drop for RawRowCollection {
    fn drop(&mut self) {
        self.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::raw::scan_chunk;
    use crate::test_utils::*;
    use paro_common::vector::VECTOR_SIZE;

    fn create_test_collection(types: Vec<LogicalType>) -> RawRowCollection {
        let pool = Arc::new(BufferPool::new(10 * 1024 * 1024));
        RawRowCollection::from_types(pool, types, MemoryTag::HashTable)
    }

    #[test]
    fn test_collection_creation() {
        let collection = create_test_collection(vec![LogicalType::Integer, LogicalType::BigInt]);

        assert_eq!(collection.count(), 0);
        assert_eq!(collection.chunk_count(), 0);
        assert_eq!(collection.size_in_bytes(), 0);
        assert_eq!(collection.segment_count(), 0);
        assert!(collection.is_empty());
    }

    #[test]
    fn test_collection_layout() {
        let collection = create_test_collection(vec![
            LogicalType::Integer,
            LogicalType::Double,
            LogicalType::Varchar,
        ]);

        assert_eq!(collection.layout().column_count(), 3);
        assert!(!collection.layout().all_constant()); // Has VARCHAR
    }

    #[test]
    fn test_rows_per_block() {
        let collection = create_test_collection(vec![LogicalType::Integer, LogicalType::BigInt]);
        let row_width = collection.layout().get_row_width();
        let expected = crate::buffer::DEFAULT_BLOCK_SIZE / row_width;
        assert_eq!(collection.rows_per_block(), expected);
    }

    #[test]
    fn test_initialize_append() {
        let mut collection = create_test_collection(vec![LogicalType::Integer]);
        let mut append_state = RawRowAppendState::new();

        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);

        assert_eq!(
            append_state.pin_state.properties,
            RawRowPinProperties::KeepEverythingPinned
        );
        assert_eq!(collection.segment_count(), 1);
    }

    #[test]
    fn test_initialize_scan() {
        let collection = create_test_collection(vec![LogicalType::Integer, LogicalType::Double]);
        let mut scan_state = RawRowScanState::new();

        collection.initialize_scan(&mut scan_state, RawRowPinProperties::UnpinAfterDone);

        assert_eq!(
            scan_state.pin_state.properties,
            RawRowPinProperties::UnpinAfterDone
        );
        // Empty collection - no segments to scan
        assert!(scan_state.segment_index.is_none() || collection.scan_complete(&scan_state));
    }

    #[test]
    fn test_create_empty() {
        let collection = create_test_collection(vec![LogicalType::Integer, LogicalType::Varchar]);
        let empty = collection.create_empty();

        assert_eq!(
            empty.layout().column_count(),
            collection.layout().column_count()
        );
        assert_eq!(
            empty.layout().get_row_width(),
            collection.layout().get_row_width()
        );
        assert_eq!(empty.count(), 0);
    }

    #[test]
    fn test_combine_collections() {
        let mut collection1 = create_test_collection(vec![LogicalType::Integer]);
        let mut collection2 = create_test_collection(vec![LogicalType::Integer]);

        // Manually set counts for testing
        collection1.count = 100;
        collection1.data_size = 500;
        collection2.count = 50;
        collection2.data_size = 250;

        collection1.combine(collection2);

        assert_eq!(collection1.count(), 150);
        assert_eq!(collection1.size_in_bytes(), 750);
    }

    #[test]
    fn test_reset() {
        let mut collection = create_test_collection(vec![LogicalType::Integer]);

        // Initialize to create a segment
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.count = 100;
        collection.data_size = 500;

        assert_eq!(collection.segment_count(), 1);

        collection.reset();

        assert_eq!(collection.count(), 0);
        assert_eq!(collection.size_in_bytes(), 0);
        assert_eq!(collection.segment_count(), 0);
    }

    #[test]
    fn test_scan_complete_empty() {
        let collection = create_test_collection(vec![LogicalType::Integer]);
        let mut scan_state = RawRowScanState::new();

        collection.initialize_scan(&mut scan_state, RawRowPinProperties::UnpinAfterDone);

        // Empty collection should be immediately complete
        assert!(collection.scan_complete(&scan_state));
    }

    #[test]
    fn test_parallel_scan_reads_all_rows_once() {
        let mut collection = create_test_collection(vec![LogicalType::Integer]);
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);

        let chunk1 = test_chunk_from_vectors(vec![test_i32_vector(&(0..2048).collect::<Vec<_>>())]);
        let chunk2 =
            test_chunk_from_vectors(vec![test_i32_vector(&(2048..3072).collect::<Vec<_>>())]);
        collection.append(&mut append_state, &chunk1).unwrap();
        collection.append(&mut append_state, &chunk2).unwrap();
        collection.finalize_append(&mut append_state);

        let mut gstate = RawRowParallelScanState::new();
        collection.initialize_parallel_scan(&mut gstate, RawRowPinProperties::UnpinAfterDone);

        let mut lstate1 = RawRowScanState::new();
        let mut lstate2 = RawRowScanState::new();
        let mut out1 = test_chunk_with_capacity(&[LogicalType::Integer], 2048);
        let mut out2 = test_chunk_with_capacity(&[LogicalType::Integer], 2048);

        let mut seen = Vec::new();
        loop {
            let scanned1 = collection
                .scan_parallel(&mut gstate, &mut lstate1, &mut out1)
                .unwrap();
            for i in 0..scanned1 {
                seen.push(out1.column(0).unwrap().get_i32(i).unwrap());
            }

            let scanned2 = collection
                .scan_parallel(&mut gstate, &mut lstate2, &mut out2)
                .unwrap();
            for i in 0..scanned2 {
                seen.push(out2.column(0).unwrap().get_i32(i).unwrap());
            }

            if scanned1 == 0 && scanned2 == 0 {
                break;
            }
        }

        seen.sort_unstable();
        let expected: Vec<i32> = (0..3072).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn test_large_single_append_splits_collection_chunks_at_vector_size() {
        let mut collection = create_test_collection(vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::UBigInt,
        ]);
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);

        let row_count = 10_000usize;
        let k1 = (1..=row_count).map(|v| v as i32).collect::<Vec<_>>();
        let k2 = (1..=row_count)
            .map(|v| (v % 257) as i32)
            .collect::<Vec<_>>();
        let v = vec![1i32; row_count];
        let hashes = (1..=row_count).map(|v| v as u64).collect::<Vec<_>>();
        let mut hash_vector = test_vector_with_capacity(LogicalType::UBigInt, row_count);
        hash_vector.set_count(row_count);
        for (idx, hash) in hashes.iter().enumerate() {
            hash_vector.set_u64(idx, *hash);
        }
        let chunk = test_chunk_from_vectors(vec![
            test_i32_vector(&k1),
            test_i32_vector(&k2),
            test_i32_vector(&v),
            hash_vector,
        ]);

        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        assert_eq!(collection.count(), row_count);
        assert!(collection.chunk_count() >= row_count.div_ceil(VECTOR_SIZE));
        for segment in collection.segments() {
            for (chunk_idx, row_chunk) in segment.chunks().iter().enumerate() {
                assert!(
                    row_chunk.count <= VECTOR_SIZE,
                    "chunk {chunk_idx} exceeded VECTOR_SIZE: {} > {}",
                    row_chunk.count,
                    VECTOR_SIZE
                );
            }
        }

        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output = test_chunk_with_capacity(
            &[
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::UBigInt,
            ],
            VECTOR_SIZE,
        );

        let mut seen_rows = 0usize;
        loop {
            let scanned = scan_chunk(&collection, &mut scan_state, &mut output).unwrap();
            if scanned == 0 {
                break;
            }
            for row in 0..scanned {
                assert_eq!(
                    output.column(0).unwrap().get_i32(row),
                    Some((seen_rows + row + 1) as i32)
                );
                assert_eq!(
                    output.column(1).unwrap().get_i32(row),
                    Some(((seen_rows + row + 1) % 257) as i32)
                );
                assert_eq!(output.column(2).unwrap().get_i32(row), Some(1));
                assert_eq!(
                    output.column(3).unwrap().get_u64(row),
                    Some((seen_rows + row + 1) as u64)
                );
            }
            seen_rows += scanned;
        }
        assert_eq!(seen_rows, row_count);
    }

    #[test]
    fn test_fetch_chunk_after_eviction_recompute_swizzle() {
        let pool = BufferPool::new_arc(8 * 1024 * 1024);
        let temp_dir = std::env::temp_dir().join(format!(
            "paro_row_swizzle_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("t")
        ));
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .unwrap();
        let mut collection = RawRowCollection::from_types(
            pool.clone(),
            vec![LogicalType::Varchar],
            MemoryTag::OrderBy,
        );

        let values: Vec<String> = (0..256)
            .map(|i| format!("long_value_{:04}_{}", i, "x".repeat(64)))
            .collect();
        let mut append_chunk = test_chunk_with_capacity(&[LogicalType::Varchar], values.len());
        append_chunk.set_cardinality(values.len());
        if let Some(col) = append_chunk.column_mut(0) {
            for (idx, value) in values.iter().enumerate() {
                col.set_string(idx, value);
            }
        }

        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &append_chunk).unwrap();
        collection.finalize_append(&mut append_state);

        // Release persistent pins and force eviction.
        collection.unpin();
        let _ = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);

        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let count = collection.fetch_chunk(&mut scan_state, 0, true).unwrap();
        assert_eq!(count, values.len());

        let row_locations_vec = &scan_state.chunk_state.row_locations;
        let mut row_locations: Vec<*const u8> = Vec::with_capacity(count);
        unsafe {
            let ptrs = row_locations_vec.flat_data::<u64>();
            for idx in 0..count {
                row_locations.push(*ptrs.add(idx) as *const u8);
            }
        }

        let mut out = test_chunk_with_capacity(&[LogicalType::Varchar], count);
        crate::row::raw::gather_chunk(&collection, &row_locations, &mut out, count).unwrap();
        for (idx, expected) in values.iter().enumerate() {
            assert_eq!(
                out.column(0).unwrap().get_string(idx),
                Some(expected.as_str())
            );
        }

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_collection_size_accounting_matches_segments_and_pool_usage() {
        let pool = Arc::new(BufferPool::new(64 * 1024 * 1024));
        let mut collection = RawRowCollection::from_types(
            pool.clone(),
            vec![LogicalType::Integer, LogicalType::Varchar],
            MemoryTag::HashTable,
        );
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);

        let mut chunk =
            test_chunk_with_capacity(&[LogicalType::Integer, LogicalType::Varchar], 256);
        chunk.set_cardinality(256);
        for row in 0..256 {
            chunk.column_mut(0).unwrap().set_i32(row, row as i32);
            chunk
                .column_mut(1)
                .unwrap()
                .set_string(row, &format!("value_{row}_{}", "y".repeat(20)));
        }
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        let segment_bytes: usize = collection
            .segments()
            .iter()
            .map(|s| s.size_in_bytes())
            .sum();
        assert_eq!(collection.size_in_bytes(), segment_bytes);
        assert!(pool.used_memory() >= collection.size_in_bytes());
    }

    #[test]
    fn test_drop_cleans_spilled_temp_blocks() {
        let temp_dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new_arc(64 * 1024 * 1024);
        pool.set_temporary_directory(temp_dir.path().to_string_lossy().to_string())
            .unwrap();

        {
            let mut collection = RawRowCollection::from_types(
                pool.clone(),
                vec![LogicalType::Varchar],
                MemoryTag::OrderBy,
            );
            let values: Vec<String> = (0..256)
                .map(|i| format!("spill_value_{:04}_{}", i, "z".repeat(64)))
                .collect();
            let mut append_chunk = test_chunk_with_capacity(&[LogicalType::Varchar], values.len());
            append_chunk.set_cardinality(values.len());
            if let Some(col) = append_chunk.column_mut(0) {
                for (idx, value) in values.iter().enumerate() {
                    col.set_string(idx, value);
                }
            }

            let mut append_state = RawRowAppendState::new();
            collection
                .initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
            collection.append(&mut append_state, &append_chunk).unwrap();
            collection.finalize_append(&mut append_state);

            collection.unpin();
            let eviction = pool.evict_blocks(MemoryTag::OrderBy, 0, 0, None);
            assert!(eviction.success);
            assert!(
                !pool.get_temporary_files().is_empty(),
                "expected spilled row blocks before collection drop"
            );
        }

        assert!(
            pool.get_temporary_files().is_empty(),
            "spilled row blocks should be removed when the collection drops"
        );
    }

    #[test]
    fn test_collection_array_append_scan() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 3);
        let mut collection = create_test_collection(vec![array_type.clone()]);

        let mut array_vec = test_vector_with_capacity(array_type.clone(), 2);
        array_vec.set_len(2);
        {
            let child = array_vec.child_mut().unwrap();
            let child_mut = Arc::make_mut(child);
            child_mut.set_len(6);
            let data = unsafe { child_mut.flat_data_mut::<i32>() };
            for i in 0..6 {
                unsafe {
                    std::ptr::write(data.add(i), (i + 1) as i32);
                }
            }
        }

        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(array_vec)]);

        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);

        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        assert_eq!(collection.count(), 2);

        // Scan back
        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);

        let mut output_chunk =
            test_chunk_with_capacity(&[array_type], crate::buffer::DEFAULT_BLOCK_SIZE / 8);
        let scanned = scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();

        assert_eq!(scanned, 2);

        let output_vec = output_chunk.column(0).unwrap();
        let child = output_vec.child().unwrap();
        let data = unsafe { child.flat_data::<i32>() };

        for i in 0..6 {
            assert_eq!(unsafe { *data.add(i) }, (i + 1) as i32);
        }
    }

    #[test]
    fn test_collection_array_with_nulls() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let mut collection = create_test_collection(vec![array_type.clone()]);

        // Row 0: [1, NULL], Row 1: NULL, Row 2: [NULL, 4]
        let mut array_vec = test_vector_with_capacity(array_type.clone(), 3);
        array_vec.set_len(3);
        array_vec.set_null(1, true); // Row 1 is NULL

        {
            let child = array_vec.child_mut().unwrap();
            let child_mut = Arc::make_mut(child);
            child_mut.set_len(6);
            child_mut.set_null(1, true); // [1, NULL]
            child_mut.set_null(4, true); // [NULL, 4]

            let data = unsafe { child_mut.flat_data_mut::<i32>() };
            unsafe {
                *data.add(0) = 1;
                *data.add(5) = 4;
            }
        }

        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(array_vec)]);
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        // Scan back
        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output_chunk = test_chunk_with_capacity(&[array_type], 1024);
        scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();

        let output_vec = output_chunk.column(0).unwrap();
        assert!(!output_vec.is_null(0));
        assert!(output_vec.is_null(1));
        assert!(!output_vec.is_null(2));

        let child = output_vec.child().unwrap();
        assert!(!child.is_null(0));
        assert!(child.is_null(1)); // element of row 0

        assert!(child.is_null(4));
        assert!(!child.is_null(5));
        assert_eq!(unsafe { *child.flat_data::<i32>().add(0) }, 1);
        assert_eq!(unsafe { *child.flat_data::<i32>().add(5) }, 4);
    }

    #[test]
    fn test_collection_array_dictionary() {
        let array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let mut collection = create_test_collection(vec![array_type.clone()]);

        // Logical rows in child: [1, 2], [3, 4]
        let mut child_vec = test_vector_with_capacity(LogicalType::Integer, 4);
        child_vec.set_len(4);
        unsafe {
            let data = child_vec.flat_data_mut::<i32>();
            *data.add(0) = 1;
            *data.add(1) = 2; // Array index 0
            *data.add(2) = 3;
            *data.add(3) = 4; // Array index 1
        }

        let mut array_vec = test_vector(array_type.clone());
        array_vec.set_len(2);
        array_vec.set_child(Arc::new(child_vec));

        // Create dictionary vector [1, 0]
        let mut sel = test_selection_with_capacity(2);
        sel.set_len(2);
        sel.set(0, 1);
        sel.set(1, 0);

        let dict_vec = paro_common::test_utils::test_dictionary(Arc::new(array_vec), sel);

        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(dict_vec)]);
        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        // Scan back
        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output_chunk = test_chunk_with_capacity(&[array_type], 1024);
        scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();

        assert_eq!(output_chunk.size(), 2);
        let output_vec = output_chunk.column(0).unwrap();
        let child = output_vec.child().unwrap();
        let data = unsafe { child.flat_data::<i32>() };

        // Should be [3, 4] then [1, 2]
        assert_eq!(unsafe { *data.add(0) }, 3);
        assert_eq!(unsafe { *data.add(1) }, 4);
        assert_eq!(unsafe { *data.add(2) }, 1);
        assert_eq!(unsafe { *data.add(3) }, 2);
    }

    #[test]
    fn test_collection_nested_dictionary_roundtrip() {
        let mut collection = create_test_collection(vec![LogicalType::Integer]);

        let base = Arc::new(test_i32_vector(&[10, 20, 30]));
        let first = Arc::new(paro_common::test_utils::test_dictionary(
            base,
            vec![2, 0, 1],
        ));
        let nested = Arc::new(paro_common::test_utils::test_dictionary(first, vec![1, 2]));
        let chunk = test_chunk_from_arc_vectors(vec![nested]);

        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output_chunk = test_chunk_with_capacity(&[LogicalType::Integer], 1024);
        scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();

        assert_eq!(output_chunk.size(), 2);
        let output = output_chunk.column(0).unwrap();
        assert_eq!(output.get_i32(0), Some(10));
        assert_eq!(output.get_i32(1), Some(20));
    }

    #[test]
    fn test_collection_nested_array_roundtrip_flat() {
        let inner_array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let outer_array_type = LogicalType::Array(Box::new(inner_array_type.clone()), 2);
        let mut collection = create_test_collection(vec![outer_array_type.clone()]);

        let inner_child = Arc::new(test_i32_vector(&[1, 2, 3, 4, 5, 6, 7, 8]));
        let inner_array_vec = Arc::new(paro_common::test_utils::test_array_vector(
            LogicalType::Integer,
            inner_child,
            4,
            2,
        ));
        let outer_vec =
            paro_common::test_utils::test_array_vector(inner_array_type, inner_array_vec, 2, 2);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(outer_vec)]);

        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output_chunk = test_chunk_with_capacity(&[outer_array_type], 1024);
        let scanned = scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();
        assert_eq!(scanned, 2);

        let output_vec = output_chunk.column(0).unwrap();
        let inner = output_vec.child().unwrap();
        let values = inner.child().unwrap();
        let data = unsafe { values.flat_data::<i32>() };
        let expected = [1, 2, 3, 4, 5, 6, 7, 8];
        for (idx, expected_value) in expected.iter().enumerate() {
            assert_eq!(unsafe { *data.add(idx) }, *expected_value);
        }
    }

    #[test]
    fn test_collection_nested_array_roundtrip_dictionary() {
        let inner_array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let outer_array_type = LogicalType::Array(Box::new(inner_array_type.clone()), 2);
        let mut collection = create_test_collection(vec![outer_array_type.clone()]);

        let inner_child = Arc::new(test_i32_vector(&[1, 2, 3, 4, 5, 6, 7, 8]));
        let inner_array_vec = Arc::new(paro_common::test_utils::test_array_vector(
            LogicalType::Integer,
            inner_child,
            4,
            2,
        ));
        let outer_vec = Arc::new(paro_common::test_utils::test_array_vector(
            inner_array_type,
            inner_array_vec,
            2,
            2,
        ));
        let dict_outer = paro_common::test_utils::test_dictionary(outer_vec, vec![1, 0]);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(dict_outer)]);

        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output_chunk = test_chunk_with_capacity(&[outer_array_type], 1024);
        scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();

        let output_vec = output_chunk.column(0).unwrap();
        let inner = output_vec.child().unwrap();
        let values = inner.child().unwrap();
        let data = unsafe { values.flat_data::<i32>() };
        let expected = [5, 6, 7, 8, 1, 2, 3, 4];
        for (idx, expected_value) in expected.iter().enumerate() {
            assert_eq!(unsafe { *data.add(idx) }, *expected_value);
        }
    }

    #[test]
    fn test_collection_nested_array_roundtrip_constant() {
        let inner_array_type = LogicalType::Array(Box::new(LogicalType::Integer), 2);
        let outer_array_type = LogicalType::Array(Box::new(inner_array_type.clone()), 2);
        let mut collection = create_test_collection(vec![outer_array_type.clone()]);

        let constant_value = paro_common::runtime_value::Value::Array(
            vec![
                paro_common::runtime_value::Value::Array(
                    vec![
                        paro_common::runtime_value::Value::Integer(9),
                        paro_common::runtime_value::Value::Integer(10),
                    ],
                    LogicalType::Integer,
                    2,
                ),
                paro_common::runtime_value::Value::Array(
                    vec![
                        paro_common::runtime_value::Value::Integer(11),
                        paro_common::runtime_value::Value::Integer(12),
                    ],
                    LogicalType::Integer,
                    2,
                ),
            ],
            inner_array_type,
            2,
        );
        let constant_vec = test_constant_from_value(outer_array_type.clone(), &constant_value, 2);
        let chunk = test_chunk_from_arc_vectors(vec![Arc::new(constant_vec)]);

        let mut append_state = RawRowAppendState::new();
        collection.initialize_append(&mut append_state, RawRowPinProperties::KeepEverythingPinned);
        collection.append(&mut append_state, &chunk).unwrap();
        collection.finalize_append(&mut append_state);

        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
        let mut output_chunk = test_chunk_with_capacity(&[outer_array_type], 1024);
        scan_chunk(&collection, &mut scan_state, &mut output_chunk).unwrap();

        let output_vec = output_chunk.column(0).unwrap();
        let inner = output_vec.child().unwrap();
        let values = inner.child().unwrap();
        let data = unsafe { values.flat_data::<i32>() };
        let expected = [9, 10, 11, 12, 9, 10, 11, 12];
        for (idx, expected_value) in expected.iter().enumerate() {
            assert_eq!(unsafe { *data.add(idx) }, *expected_value);
        }
    }
}
