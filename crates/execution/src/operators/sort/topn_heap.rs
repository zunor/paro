// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Heap implementation used by [`super::topn::TopN`].

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem::size_of;
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::sort_key::{compare_keys, encode_column, OrderModifiers};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::OrderByNode;

use crate::memory_runtime::RetainedChunkVec;

/// Global boundary value for TopN optimization.
///
/// Tracks the current maximum value in the heap across all parallel sinks.
/// Used to filter out rows that cannot possibly be in the final result.
#[derive(Debug)]
pub struct TopNBoundaryValue {
    /// The current boundary sort key (maximum value in heap)
    boundary_key: Mutex<Option<Vec<u8>>>,
}

impl TopNBoundaryValue {
    /// Create a new boundary value tracker.
    pub fn new() -> Self {
        Self {
            boundary_key: Mutex::new(None),
        }
    }

    /// Get the current boundary value.
    pub fn get_boundary(&self) -> Option<Vec<u8>> {
        self.boundary_key.lock().unwrap().clone()
    }

    /// Update the boundary value if the new value is smaller.
    ///
    /// Returns true if the boundary was updated.
    pub fn update(&self, new_key: &[u8]) -> bool {
        let mut boundary = self.boundary_key.lock().unwrap();

        match boundary.as_ref() {
            None => {
                // No boundary set yet
                *boundary = Some(new_key.to_vec());
                true
            }
            Some(current) => {
                // Update if new key is smaller (better boundary)
                if new_key < current.as_slice() {
                    *boundary = Some(new_key.to_vec());
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn allocation_size(&self) -> usize {
        self.boundary_key
            .lock()
            .unwrap()
            .as_ref()
            .map(|value| value.capacity())
            .unwrap_or(0)
    }
}

impl Default for TopNBoundaryValue {
    fn default() -> Self {
        Self::new()
    }
}

/// Entry in the TopN heap.
///
/// Contains the encoded sort key and the index of the row in the heap data.
#[derive(Debug, Clone)]
struct TopNEntry {
    /// Encoded sort key for comparison
    sort_key: Vec<u8>,
    /// Index in the heap_data chunk
    index: usize,
}

#[derive(Debug, Clone, Copy)]
struct HeapRowSource {
    chunk_index: usize,
    row_index: usize,
}

/// Immutable directory translating heap-global row ids into chunk-local ids.
///
/// Compaction, merge, and result extraction all consume the same address
/// contract. Keeping the translation here prevents individual paths from
/// accidentally treating a batch of local row ids as if it came from one
/// source chunk.
struct HeapDataDirectory {
    chunk_ends: Vec<usize>,
}

impl HeapDataDirectory {
    fn try_new(chunks: &[Chunk]) -> Result<Self> {
        let mut chunk_ends = Vec::with_capacity(chunks.len());
        let mut end = 0usize;
        for chunk in chunks {
            end = end
                .checked_add(chunk.size())
                .ok_or_else(|| paro_common::error::internal("TopN heap row directory overflow"))?;
            chunk_ends.push(end);
        }
        Ok(Self { chunk_ends })
    }

    fn resolve(&self, global_index: usize) -> Result<HeapRowSource> {
        let chunk_index = self
            .chunk_ends
            .partition_point(|&chunk_end| chunk_end <= global_index);
        let Some(&chunk_end) = self.chunk_ends.get(chunk_index) else {
            return Err(paro_common::error::internal(format!(
                "TopN heap row index {global_index} is out of bounds"
            )));
        };
        let chunk_start = chunk_index
            .checked_sub(1)
            .map_or(0, |previous| self.chunk_ends[previous]);
        debug_assert!(global_index < chunk_end);
        Ok(HeapRowSource {
            chunk_index,
            row_index: global_index - chunk_start,
        })
    }
}

impl PartialEq for TopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}

impl Eq for TopNEntry {}

impl PartialOrd for TopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap, so we compare in reverse order
        // to get the largest element at the top
        self.sort_key.cmp(&other.sort_key)
    }
}

/// TopN heap for maintaining top-K elements.
///
/// Uses a max-heap to efficiently track the smallest K elements.
/// When the heap is full, new elements are only added if they are
/// smaller than the current maximum.
#[derive(Debug)]
pub struct TopNHeap {
    /// Max-heap of entries
    heap: BinaryHeap<TopNEntry>,
    /// Materialized payload data
    heap_data: RetainedChunkVec,
    /// Memory owner for retained chunk buffers.
    memory: MemoryAccountingContext,
    /// Total heap size (limit + offset)
    heap_size: usize,
    /// OFFSET value
    offset: usize,
    /// ORDER BY modifiers for each sort column.
    modifiers: Vec<OrderModifiers>,
    /// Payload types
    payload_types: Vec<LogicalType>,
}

/// Threshold for small heap optimization
const SMALL_HEAP_THRESHOLD: usize = 100;

impl TopNHeap {
    /// Create a new TopN heap.
    ///
    /// # Arguments
    /// * `payload_types` - Types of the payload columns
    /// * `orders` - ORDER BY specifications
    /// * `limit` - LIMIT value
    /// * `offset` - OFFSET value
    pub fn new(
        payload_types: Vec<LogicalType>,
        orders: &[OrderByNode],
        limit: usize,
        offset: usize,
    ) -> Self {
        Self::new_with_memory(
            payload_types,
            orders,
            limit,
            offset,
            MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::OrderBy,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn new_with_memory(
        payload_types: Vec<LogicalType>,
        orders: &[OrderByNode],
        limit: usize,
        offset: usize,
        memory: MemoryAccountingContext,
    ) -> Self {
        let heap_size = limit.saturating_add(offset);
        let modifiers = orders
            .iter()
            .map(|order| OrderModifiers::new(order.ascending, order.nulls_first))
            .collect();

        Self {
            heap: BinaryHeap::with_capacity(heap_size),
            heap_data: RetainedChunkVec::new(memory.clone()),
            memory,
            heap_size,
            offset,
            modifiers,
            payload_types,
        }
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.heap.capacity() * size_of::<TopNEntry>()
            + self
                .heap
                .iter()
                .map(|entry| entry.sort_key.capacity())
                .sum::<usize>()
            + self.heap_data.retained_bytes()
    }

    /// Sink input data into the heap.
    ///
    /// Processes each row and adds it to the heap if it qualifies.
    ///
    /// # Arguments
    /// * `payload_chunk` - Input data chunk (payload columns)
    /// * `sort_chunk` - Computed ORDER BY expression results
    /// * `boundary` - Optional global boundary value for filtering
    ///
    /// then uses the results for sorting. In Paro, we separate this:
    /// - TopN::sink computes ORDER BY expressions using ExpressionExecutor
    /// - TopNHeap::sink_with_sort_chunk receives payload and computed sort values
    pub fn sink_with_sort_chunk(
        &mut self,
        payload_chunk: &Chunk,
        sort_chunk: &Chunk,
        boundary: Option<&TopNBoundaryValue>,
    ) -> Result<()> {
        if payload_chunk.is_empty() {
            return Ok(());
        }

        // Check boundary value first to filter out rows early
        let (filtered_payload, filtered_sort) = if let Some(boundary_val) = boundary {
            self.filter_by_boundary_with_sort(payload_chunk, sort_chunk, boundary_val)?
        } else {
            (payload_chunk.clone(), sort_chunk.clone())
        };

        if filtered_payload.is_empty() {
            return Ok(());
        }

        if self.heap_size <= SMALL_HEAP_THRESHOLD {
            self.add_small_heap_with_sort(&filtered_payload, &filtered_sort)?;
        } else {
            self.add_large_heap_with_sort(&filtered_payload, &filtered_sort)?;
        }

        // Update global boundary if heap is full
        // Note: Only update when heap is FULL to avoid premature filtering
        if self.heap.len() >= self.heap_size {
            if let Some(boundary_val) = boundary {
                if let Some(max_entry) = self.heap.peek() {
                    boundary_val.update(&max_entry.sort_key);
                }
            }
        }

        Ok(())
    }

    /// Get the reduce threshold.
    ///
    /// Reduce is triggered when heap_data size exceeds this threshold.
    /// This prevents memory fragmentation and excessive memory usage.
    fn reduce_threshold(&self) -> usize {
        use paro_common::vector::VECTOR_SIZE;
        // max(5 * VECTOR_SIZE, 2 * heap_size)
        std::cmp::max(5 * VECTOR_SIZE, 2 * self.heap_size)
    }

    /// Reduce the heap data to compact memory.
    ///
    /// This reorganizes the heap_data to only contain entries currently in the heap,
    /// reducing memory fragmentation and usage. Called periodically when heap_data
    /// grows too large relative to the actual heap size.
    pub fn reduce(&mut self) -> Result<()> {
        if self.total_heap_data_size() < self.reduce_threshold() {
            // Only reduce when we pass the reduce threshold
            return Ok(());
        }

        self.compact_live()
    }

    /// Transactionally compact the payload store to the rows referenced by
    /// the live heap entries.
    ///
    /// The complete replacement is copied and admitted to memory before any
    /// entry index or old ownership is changed. A copy/accounting failure thus
    /// leaves both the heap and its payload address domain untouched.
    fn compact_live(&mut self) -> Result<()> {
        let mut live_indices = self
            .heap
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>();
        if live_indices.is_empty() {
            self.heap_data.clear();
            return Ok(());
        }
        live_indices.sort_unstable();
        if live_indices.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(paro_common::error::internal(
                "TopN heap contains duplicate live payload indices",
            ));
        }

        let mut remapped_entries = Vec::with_capacity(self.heap.len());
        let mut new_heap_data = RetainedChunkVec::new(self.memory.clone());
        Self::for_each_gathered_chunk(
            &self.payload_types,
            self.heap_data.as_slice(),
            &live_indices,
            |chunk| {
                new_heap_data.push(chunk)?;
                Ok(())
            },
        )?;

        // Copy and accounting are complete. Only now publish the new address
        // domain and remap every live entry into it.
        remapped_entries.extend(self.heap.drain());
        for entry in &mut remapped_entries {
            entry.index = live_indices
                .binary_search(&entry.index)
                .expect("live TopN index was collected before compaction");
        }
        self.heap = BinaryHeap::from(remapped_entries);
        self.heap_data = new_heap_data;

        Ok(())
    }

    /// Gather global row ids from any number of source chunks into vector-sized
    /// output chunks. All TopN materialization paths use this implementation.
    fn for_each_gathered_chunk(
        payload_types: &[LogicalType],
        source_chunks: &[Chunk],
        row_indices: &[usize],
        mut consume: impl FnMut(Chunk) -> Result<()>,
    ) -> Result<()> {
        if row_indices.is_empty() {
            return Ok(());
        }
        let directory = HeapDataDirectory::try_new(source_chunks)?;
        for batch in row_indices.chunks(paro_common::vector::VECTOR_SIZE) {
            let sources = batch
                .iter()
                .map(|&index| directory.resolve(index))
                .collect::<Result<Vec<_>>>()?;
            consume(Self::gather_row_batch(
                payload_types,
                source_chunks,
                &sources,
            )?)?;
        }
        Ok(())
    }

    fn gather_row_batch(
        payload_types: &[LogicalType],
        source_chunks: &[Chunk],
        rows: &[HeapRowSource],
    ) -> Result<Chunk> {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let first = rows.first().ok_or_else(|| {
            paro_common::error::internal("TopN gather requires at least one source row")
        })?;
        let allocator = source_chunks
            .get(first.chunk_index)
            .ok_or_else(|| paro_common::error::internal("TopN source chunk is out of bounds"))?
            .allocator()
            .clone();
        let mut output_vectors = Vec::with_capacity(payload_types.len());

        for (column_index, column_type) in payload_types.iter().enumerate() {
            let mut destination =
                Vector::try_new(column_type.clone(), rows.len(), allocator.clone())?;

            for (destination_index, source) in rows.iter().enumerate() {
                let source_vector = source_chunks
                    .get(source.chunk_index)
                    .and_then(|chunk| chunk.column(column_index))
                    .ok_or_else(|| {
                        paro_common::error::internal(format!(
                            "TopN source is missing payload column {column_index}"
                        ))
                    })?;

                if source_vector.is_null(source.row_index) {
                    destination.try_set_null(destination_index, true)?;
                } else {
                    destination.try_copy_at(destination_index, source_vector, source.row_index)?;
                }
            }

            destination.try_set_count(rows.len())?;
            output_vectors.push(Arc::new(destination));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors, allocator);
        result.try_set_cardinality(rows.len())?;
        Ok(result)
    }

    /// Filter chunks by boundary value (with separate sort chunk).
    ///
    /// Returns filtered payload and sort chunks containing only rows that could
    fn filter_by_boundary_with_sort(
        &self,
        payload_chunk: &Chunk,
        sort_chunk: &Chunk,
        boundary: &TopNBoundaryValue,
    ) -> Result<(Chunk, Chunk)> {
        let boundary_key = match boundary.get_boundary() {
            Some(key) => key,
            None => return Ok((payload_chunk.clone(), sort_chunk.clone())), // No boundary yet
        };

        // The sort chunk materializes ORDER BY expressions densely as [0, 1, 2, ...].
        let sort_indices: Vec<usize> = (0..sort_chunk.column_count()).collect();

        // Collect indices of rows that pass the boundary check
        let mut passing_rows = Vec::new();
        let mut sort_key = Vec::new();

        for row_idx in 0..sort_chunk.size() {
            self.encode_sort_key_into(sort_chunk, row_idx, &sort_indices, &mut sort_key)?;

            // Row passes if its sort key is less than the boundary
            if compare_keys(&sort_key, &boundary_key) == Ordering::Less {
                passing_rows.push(row_idx);
            }
        }

        if passing_rows.is_empty() {
            // No rows pass - return empty chunks
            return Ok((
                Chunk::try_new(payload_chunk.allocator().clone())?,
                Chunk::try_new(sort_chunk.allocator().clone())?,
            ));
        }

        if passing_rows.len() == payload_chunk.size() {
            // All rows pass - return original chunks
            return Ok((payload_chunk.clone(), sort_chunk.clone()));
        }

        // Some rows pass - create filtered chunks
        let filtered_payload = self.copy_rows(payload_chunk, &passing_rows)?;
        let filtered_sort = self.copy_rows_generic(sort_chunk, &passing_rows)?;
        Ok((filtered_payload, filtered_sort))
    }

    /// Add entries to a small heap with separate sort chunk (delayed payload copy).
    fn add_small_heap_with_sort(
        &mut self,
        payload_chunk: &Chunk,
        sort_chunk: &Chunk,
    ) -> Result<()> {
        const BASE_INDEX: usize = u32::MAX as usize;

        // The sort chunk materializes ORDER BY expressions densely as [0, 1, 2, ...].
        let sort_indices: Vec<usize> = (0..sort_chunk.column_count()).collect();

        let mut any_added = false;
        let mut sort_key = Vec::new();

        // First pass: add entries with temporary indices
        for row_idx in 0..sort_chunk.size() {
            self.encode_sort_key_into(sort_chunk, row_idx, &sort_indices, &mut sort_key)?;

            if !self.should_add_entry(&sort_key) {
                continue;
            }

            let entry = TopNEntry {
                sort_key: std::mem::take(&mut sort_key),
                index: BASE_INDEX + row_idx,
            };

            self.add_entry_to_heap(entry);
            any_added = true;
        }

        if !any_added {
            return Ok(());
        }

        // Second pass: copy payload data for entries that were added
        let base_heap_data_size = self.total_heap_data_size();
        let mut rows_to_copy = Vec::new();

        // Update indices and collect rows to copy
        for entry in self.heap.iter() {
            if entry.index >= BASE_INDEX {
                rows_to_copy.push(entry.index - BASE_INDEX);
            }
        }

        if !rows_to_copy.is_empty() {
            // Copy the selected rows from payload chunk
            let new_chunk = self.copy_rows(payload_chunk, &rows_to_copy)?;

            // Update indices in heap
            let mut row_map = std::collections::HashMap::new();
            for (new_idx, &old_idx) in rows_to_copy.iter().enumerate() {
                row_map.insert(BASE_INDEX + old_idx, base_heap_data_size + new_idx);
            }

            // We need to rebuild the heap with updated indices
            let mut entries: Vec<TopNEntry> = self.heap.drain().collect();
            for entry in &mut entries {
                if let Some(&new_index) = row_map.get(&entry.index) {
                    entry.index = new_index;
                }
            }

            self.heap = BinaryHeap::from(entries);
            self.heap_data.push(new_chunk)?;
        }

        Ok(())
    }

    /// Add entries to a large heap with separate sort chunk (immediate payload copy).
    fn add_large_heap_with_sort(
        &mut self,
        payload_chunk: &Chunk,
        sort_chunk: &Chunk,
    ) -> Result<()> {
        let base_index = self.total_heap_data_size();
        let mut rows_to_copy = Vec::new();

        // The sort chunk materializes ORDER BY expressions densely as [0, 1, 2, ...].
        let sort_indices: Vec<usize> = (0..sort_chunk.column_count()).collect();
        let mut sort_key = Vec::new();

        // Process each row
        for row_idx in 0..sort_chunk.size() {
            self.encode_sort_key_into(sort_chunk, row_idx, &sort_indices, &mut sort_key)?;

            if !self.should_add_entry(&sort_key) {
                continue;
            }

            let entry = TopNEntry {
                sort_key: std::mem::take(&mut sort_key),
                index: base_index + rows_to_copy.len(),
            };

            self.add_entry_to_heap(entry);
            rows_to_copy.push(row_idx);
        }

        if !rows_to_copy.is_empty() {
            let new_chunk = self.copy_rows(payload_chunk, &rows_to_copy)?;
            self.heap_data.push(new_chunk)?;
        }

        Ok(())
    }

    /// Copy selected rows from a chunk (generic version for any chunk).
    fn copy_rows_generic(&self, chunk: &Chunk, row_indices: &[usize]) -> Result<Chunk> {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let mut output_vectors = Vec::with_capacity(chunk.data.len());

        for col_idx in 0..chunk.column_count() {
            let src_vec = &chunk.data[col_idx];
            let col_type = src_vec.logical_type().clone();
            let mut dst_vec =
                Vector::try_new(col_type, row_indices.len(), chunk.allocator().clone())?;

            for (dst_idx, &src_idx) in row_indices.iter().enumerate() {
                if src_vec.is_null(src_idx) {
                    dst_vec.try_set_null(dst_idx, true)?;
                } else {
                    dst_vec.try_copy_at(dst_idx, src_vec, src_idx)?;
                }
            }

            dst_vec.try_set_count(row_indices.len())?;
            output_vectors.push(Arc::new(dst_vec));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors, chunk.allocator().clone());
        result.try_set_cardinality(row_indices.len())?;
        Ok(result)
    }

    /// Check if an entry should be added to the heap.
    fn should_add_entry(&self, sort_key: &[u8]) -> bool {
        if self.heap.len() < self.heap_size {
            // Heap not full - always add
            return true;
        }

        // Heap is full - check if this entry is smaller than the max
        if let Some(max_entry) = self.heap.peek() {
            return sort_key < max_entry.sort_key.as_slice();
        }

        false
    }

    fn encode_sort_key_into(
        &self,
        chunk: &Chunk,
        row_idx: usize,
        columns: &[usize],
        out: &mut Vec<u8>,
    ) -> Result<()> {
        debug_assert_eq!(columns.len(), self.modifiers.len());
        out.clear();
        for (&column_idx, modifiers) in columns.iter().zip(self.modifiers.iter().copied()) {
            let vector = chunk.column(column_idx).expect("sort column must exist");
            encode_column(vector, row_idx, modifiers, out)?;
        }
        Ok(())
    }

    /// Add an entry to the heap, removing the max if necessary.
    fn add_entry_to_heap(&mut self, entry: TopNEntry) {
        if self.heap.len() >= self.heap_size {
            // Heap is full - remove the max element
            self.heap.pop();
        }
        self.heap.push(entry);
    }

    /// Copy selected rows from a chunk.
    fn copy_rows(&self, chunk: &Chunk, row_indices: &[usize]) -> Result<Chunk> {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let mut output_vectors = Vec::with_capacity(chunk.data.len());

        for (col_idx, col_type) in self.payload_types.iter().enumerate() {
            let src_vec = &chunk.data[col_idx];
            let mut dst_vec = Vector::try_new(
                col_type.clone(),
                row_indices.len(),
                chunk.allocator().clone(),
            )?;

            for (dst_idx, &src_idx) in row_indices.iter().enumerate() {
                if src_vec.is_null(src_idx) {
                    dst_vec.try_set_null(dst_idx, true)?;
                } else {
                    self.copy_value(src_vec, src_idx, &mut dst_vec, dst_idx)?;
                }
            }

            dst_vec.try_set_count(row_indices.len())?;
            output_vectors.push(Arc::new(dst_vec));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors, chunk.allocator().clone());
        result.try_set_cardinality(row_indices.len())?;
        Ok(result)
    }

    /// Copy a single value between vectors.
    fn copy_value(
        &self,
        src: &paro_common::vector::Vector,
        src_idx: usize,
        dst: &mut paro_common::vector::Vector,
        dst_idx: usize,
    ) -> Result<()> {
        dst.try_copy_at(dst_idx, src, src_idx)
    }

    /// Combine another heap into this one.
    ///
    /// Used to merge results from parallel sinks.
    pub fn combine(&mut self, other: &mut TopNHeap) -> Result<()> {
        if self.heap_size != other.heap_size
            || self.offset != other.offset
            || self.modifiers != other.modifiers
            || self.payload_types != other.payload_types
            || !self.memory.has_same_target(&other.memory)
        {
            return Err(paro_common::error::internal(
                "cannot combine incompatible TopN heaps",
            ));
        }

        // Select the final live set without mutating either heap. Payload copy
        // and memory admission can fail, so both ownership domains must remain
        // valid until every selected row has been gathered.
        let (selected_self, selected_other) = self.select_combined_rows(other)?;
        let expected_entries = selected_self.len() + selected_other.len();

        // Prepare all fallible metadata before staging payload. Publication
        // below must not allocate after either source heap starts to drain.
        let mut self_remap = selected_self
            .iter()
            .copied()
            .enumerate()
            .map(|(new_index, old_index)| (old_index, new_index))
            .collect::<Vec<_>>();
        self_remap.sort_unstable_by_key(|&(old_index, _)| old_index);
        let other_base = selected_self.len();
        let mut other_remap = selected_other
            .iter()
            .copied()
            .enumerate()
            .map(|(new_index, old_index)| (old_index, other_base + new_index))
            .collect::<Vec<_>>();
        other_remap.sort_unstable_by_key(|&(old_index, _)| old_index);
        let mut combined = Vec::with_capacity(expected_entries);

        // Gather the complete final live set into an independent ownership
        // domain. Both old heaps stay intact until every payload allocation and
        // retention charge has succeeded, including later vector-sized batches.
        let mut staged_data = RetainedChunkVec::new(self.memory.clone());
        Self::for_each_gathered_chunk(
            &self.payload_types,
            self.heap_data.as_slice(),
            &selected_self,
            |chunk| {
                staged_data.push(chunk)?;
                Ok(())
            },
        )?;
        Self::for_each_gathered_chunk(
            &self.payload_types,
            other.heap_data.as_slice(),
            &selected_other,
            |chunk| {
                staged_data.push(chunk)?;
                Ok(())
            },
        )?;

        // Copy is complete. Transfer sort-key ownership and publish the new
        // entry address domain without cloning keys or allocating metadata.
        for mut entry in self.heap.drain() {
            let Ok(position) =
                self_remap.binary_search_by_key(&entry.index, |&(old_index, _)| old_index)
            else {
                continue;
            };
            entry.index = self_remap[position].1;
            combined.push(entry);
        }
        for mut entry in other.heap.drain() {
            let Ok(position) =
                other_remap.binary_search_by_key(&entry.index, |&(old_index, _)| old_index)
            else {
                continue;
            };
            entry.index = other_remap[position].1;
            combined.push(entry);
        }
        debug_assert_eq!(combined.len(), expected_entries);
        self.heap = BinaryHeap::from(combined);
        self.heap_data = staged_data;
        other.heap_data = RetainedChunkVec::new(other.memory.clone());

        Ok(())
    }

    fn select_combined_rows(&self, other: &TopNHeap) -> Result<(Vec<usize>, Vec<usize>)> {
        let mut left = self.heap.iter().collect::<Vec<_>>();
        let mut right = other.heap.iter().collect::<Vec<_>>();
        let order = |left: &&TopNEntry, right: &&TopNEntry| {
            left.sort_key
                .cmp(&right.sort_key)
                .then_with(|| left.index.cmp(&right.index))
        };
        left.sort_unstable_by(order);
        right.sort_unstable_by(order);
        Self::validate_unique_payload_indices(&left)?;
        Self::validate_unique_payload_indices(&right)?;

        let target = self.heap_size.min(left.len().saturating_add(right.len()));
        let mut selected_left = Vec::with_capacity(target.min(left.len()));
        let mut selected_right = Vec::with_capacity(target.min(right.len()));
        let (mut left_index, mut right_index) = (0usize, 0usize);
        while selected_left.len() + selected_right.len() < target {
            let take_left = match (left.get(left_index), right.get(right_index)) {
                (Some(left), Some(right)) => left.sort_key <= right.sort_key,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            if take_left {
                selected_left.push(left[left_index].index);
                left_index += 1;
            } else {
                selected_right.push(right[right_index].index);
                right_index += 1;
            }
        }
        Ok((selected_left, selected_right))
    }

    fn validate_unique_payload_indices(entries: &[&TopNEntry]) -> Result<()> {
        let mut indices = entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
        indices.sort_unstable();
        if indices.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(paro_common::error::internal(
                "TopN heap contains duplicate live payload indices",
            ));
        }
        Ok(())
    }

    /// Get the total number of rows in heap_data.
    fn total_heap_data_size(&self) -> usize {
        self.heap_data.iter().map(|c| c.size()).sum()
    }

    /// Extract sorted results, applying OFFSET.
    ///
    /// Returns chunks of sorted data, skipping the first `offset` rows.
    pub fn extract_results(&mut self) -> Result<Vec<Chunk>> {
        // A heap has no stable iteration order; consume and sort exactly once.
        let mut sorted_entries: Vec<TopNEntry> = self.heap.drain().collect();
        sorted_entries.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        // Apply offset
        let start_idx = self.offset.min(sorted_entries.len());
        let result_entries = &sorted_entries[start_idx..];

        if result_entries.is_empty() {
            return Ok(vec![]);
        }

        let row_indices = result_entries
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>();
        let mut result_chunks = Vec::new();
        Self::for_each_gathered_chunk(
            &self.payload_types,
            self.heap_data.as_slice(),
            &row_indices,
            |chunk| {
                result_chunks.push(chunk);
                Ok(())
            },
        )?;

        Ok(result_chunks)
    }
}

#[cfg(test)]
#[path = "topn_heap_contract_tests.rs"]
mod contract_tests;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Arc;

    use paro_common::allocator::{Allocator, DefaultAllocator, MemoryTag};
    use paro_common::memory::{MemoryDomain, MemoryOwner};
    use paro_common::vector::VECTOR_SIZE;
    use paro_planner::expression::{Expression, ReferenceExpression};

    use crate::memory_runtime::QueryMemoryPool;

    use super::*;

    #[derive(Debug)]
    struct ToggleAllocator {
        inner: DefaultAllocator,
        fail: AtomicBool,
    }

    impl ToggleAllocator {
        fn new() -> Self {
            Self {
                inner: DefaultAllocator::new(),
                fail: AtomicBool::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.fail.store(fail, AtomicOrdering::SeqCst);
        }

        fn check(&self, bytes: usize) -> Result<()> {
            if self.fail.load(AtomicOrdering::SeqCst) {
                Err(paro_common::error::out_of_memory(format!(
                    "injected TopN allocation failure: {bytes} bytes"
                )))
            } else {
                Ok(())
            }
        }
    }

    impl Allocator for ToggleAllocator {
        fn allocate(&self, size: usize) -> Result<*mut u8> {
            self.check(size)?;
            self.inner.allocate(size)
        }

        fn allocate_zeroed(&self, size: usize) -> Result<*mut u8> {
            self.check(size)?;
            self.inner.allocate_zeroed(size)
        }

        fn free(&self, ptr: *mut u8, size: usize) {
            self.inner.free(ptr, size);
        }

        fn reallocate(&self, ptr: *mut u8, old_size: usize, new_size: usize) -> Result<*mut u8> {
            self.check(new_size)?;
            self.inner.reallocate(ptr, old_size, new_size)
        }

        fn name(&self) -> &'static str {
            "ToggleAllocator"
        }
    }

    fn make_int_chunk(values: &[i32]) -> Chunk {
        make_int_chunk_with_allocator(values, paro_common::test_utils::test_allocator())
    }

    fn make_int_chunk_with_allocator(values: &[i32], allocator: Arc<dyn Allocator>) -> Chunk {
        let vector = paro_common::test_utils::test_i32_vector_with_allocator(values, allocator);
        let mut chunk = paro_common::test_utils::test_chunk_from_vectors(vec![vector]);
        chunk.set_cardinality(values.len());
        chunk
    }

    fn manual_heap(
        chunks: &[Vec<i32>],
        entries: impl IntoIterator<Item = (u32, usize)>,
        heap_size: usize,
    ) -> TopNHeap {
        let mut heap_data = RetainedChunkVec::detached(
            paro_common::allocator::MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        );
        for values in chunks {
            heap_data.push(make_int_chunk(values)).unwrap();
        }
        TopNHeap {
            heap: BinaryHeap::from(
                entries
                    .into_iter()
                    .map(|(key, index)| TopNEntry {
                        sort_key: key.to_be_bytes().to_vec(),
                        index,
                    })
                    .collect::<Vec<_>>(),
            ),
            heap_data,
            memory: MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::OrderBy,
                MemoryAccountingClass::Revocable,
            ),
            heap_size,
            offset: 0,
            modifiers: vec![OrderModifiers::new(true, false)],
            payload_types: vec![LogicalType::Integer],
        }
    }

    fn sequential_chunks(chunk_count: usize) -> Vec<Vec<i32>> {
        (0..chunk_count)
            .map(|chunk_index| {
                let start = chunk_index * VECTOR_SIZE;
                (start..start + VECTOR_SIZE)
                    .map(|value| value as i32)
                    .collect()
            })
            .collect()
    }

    fn extract_ints(heap: &mut TopNHeap) -> Vec<i32> {
        heap.extract_results()
            .unwrap()
            .into_iter()
            .flat_map(|chunk| {
                (0..chunk.size())
                    .map(|row| chunk.column(0).unwrap().get_i32(row).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn test_topn_boundary_value() {
        let boundary = TopNBoundaryValue::new();

        // Initially no boundary
        assert!(boundary.get_boundary().is_none());

        // Set a boundary
        let key1 = vec![1, 2, 3];
        assert!(boundary.update(&key1));
        assert_eq!(boundary.get_boundary(), Some(key1.clone()));

        // Try to update with larger key (should not update)
        let key2 = vec![2, 3, 4];
        assert!(!boundary.update(&key2));
        assert_eq!(boundary.get_boundary(), Some(key1.clone()));

        // Update with smaller key (should update)
        let key3 = vec![0, 1, 2];
        assert!(boundary.update(&key3));
        assert_eq!(boundary.get_boundary(), Some(key3));
    }

    #[test]
    fn test_topn_entry_ordering() {
        let entry1 = TopNEntry {
            sort_key: vec![1, 2, 3],
            index: 0,
        };

        let entry2 = TopNEntry {
            sort_key: vec![2, 3, 4],
            index: 1,
        };

        // BinaryHeap is a max-heap, so larger sort_key should be "greater"
        assert!(entry2 > entry1);
        assert!(entry1 < entry2);
    }

    #[test]
    fn test_order_modifiers() {
        let modifiers = OrderModifiers::new(true, false);
        assert_eq!(modifiers.ascending, true);
        assert_eq!(modifiers.nulls_first, false);

        let modifiers2 = OrderModifiers::new(false, true);
        assert_eq!(modifiers2.ascending, false);
        assert_eq!(modifiers2.nulls_first, true);
    }

    #[test]
    fn test_small_heap_threshold() {
        // Verify the threshold constant
        assert_eq!(SMALL_HEAP_THRESHOLD, 100);
    }

    #[test]
    fn test_combine_sorts_other_entries_before_early_stop() {
        let modifiers = vec![OrderModifiers::new(true, false)];
        let mut left_data = RetainedChunkVec::detached(
            paro_common::allocator::MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        );
        left_data.push(make_int_chunk(&[5, 6, 7])).unwrap();
        let mut right_data = RetainedChunkVec::detached(
            paro_common::allocator::MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        );
        right_data.push(make_int_chunk(&[9, 1, 2])).unwrap();

        let mut left = TopNHeap {
            heap_size: 3,
            offset: 0,
            heap: BinaryHeap::from(vec![
                TopNEntry {
                    sort_key: vec![5],
                    index: 0,
                },
                TopNEntry {
                    sort_key: vec![6],
                    index: 1,
                },
                TopNEntry {
                    sort_key: vec![7],
                    index: 2,
                },
            ]),
            heap_data: left_data,
            memory: MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::OrderBy,
                MemoryAccountingClass::Revocable,
            ),
            modifiers: modifiers.clone(),
            payload_types: vec![LogicalType::Integer],
        };
        let mut right = TopNHeap {
            heap_size: 3,
            offset: 0,
            heap: BinaryHeap::from(vec![
                TopNEntry {
                    sort_key: vec![9],
                    index: 0,
                },
                TopNEntry {
                    sort_key: vec![1],
                    index: 1,
                },
                TopNEntry {
                    sort_key: vec![2],
                    index: 2,
                },
            ]),
            heap_data: right_data,
            memory: MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::OrderBy,
                MemoryAccountingClass::Revocable,
            ),
            modifiers,
            payload_types: vec![LogicalType::Integer],
        };

        left.combine(&mut right).unwrap();

        let result_chunks = left.extract_results().unwrap();
        assert_eq!(result_chunks.len(), 1);
        let result_vec = &result_chunks[0].data[0];
        let values = (0..result_chunks[0].size())
            .map(|idx| result_vec.get_i32(idx).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1, 2, 5]);
    }

    #[test]
    fn reduce_compacts_sub_vector_heap_with_holes_across_source_chunks() {
        let chunks = sequential_chunks(5);
        let live = [
            1usize,
            VECTOR_SIZE + 7,
            2 * VECTOR_SIZE + 3,
            4 * VECTOR_SIZE - 1,
        ];
        let mut heap = manual_heap(
            &chunks,
            live.into_iter()
                .enumerate()
                .map(|(rank, index)| (rank as u32, index)),
            live.len(),
        );
        assert_eq!(heap.total_heap_data_size(), 5 * VECTOR_SIZE);

        heap.reduce().unwrap();

        assert_eq!(heap.total_heap_data_size(), live.len());
        assert_eq!(extract_ints(&mut heap), live.map(|index| index as i32));
    }

    #[test]
    fn reduce_compacts_more_than_one_vector_from_interleaved_source_chunks() {
        let chunks = sequential_chunks(5);
        let mut live = Vec::with_capacity(VECTOR_SIZE + 1);
        for local_index in 0..VECTOR_SIZE / 2 {
            live.push(local_index);
            live.push(VECTOR_SIZE + local_index);
        }
        live.push(2 * VECTOR_SIZE + 17);
        let mut heap = manual_heap(
            &chunks,
            live.iter()
                .copied()
                .enumerate()
                .map(|(rank, index)| (rank as u32, index)),
            live.len(),
        );

        heap.reduce().unwrap();

        assert_eq!(heap.total_heap_data_size(), live.len());
        assert_eq!(heap.heap_data.len(), 2);
        assert_eq!(
            extract_ints(&mut heap),
            live.into_iter()
                .map(|index| index as i32)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn combine_gathers_selected_rows_in_vector_batches_then_transfers_entries() {
        let row_count = VECTOR_SIZE + 1;
        let left_chunks = vec![
            (0..VECTOR_SIZE)
                .map(|row| (100_000 + row) as i32)
                .collect::<Vec<_>>(),
            vec![(100_000 + VECTOR_SIZE) as i32],
        ];
        let right_chunks = vec![
            (0..VECTOR_SIZE).map(|row| row as i32).collect::<Vec<_>>(),
            vec![VECTOR_SIZE as i32],
        ];
        let mut left = manual_heap(
            &left_chunks,
            (0..row_count).map(|index| ((100_000 + index) as u32, index)),
            row_count,
        );
        let mut right = manual_heap(
            &right_chunks,
            (0..row_count).map(|index| (index as u32, index)),
            row_count,
        );

        left.combine(&mut right).unwrap();

        assert!(right.heap.is_empty());
        assert!(right.heap_data.is_empty());
        // The final live set is rebuilt into two vector-sized chunks. The
        // previous row-at-a-time merge produced one retained chunk per row.
        assert_eq!(left.heap_data.len(), 2);
        assert_eq!(
            extract_ints(&mut left),
            (0..row_count).map(|value| value as i32).collect::<Vec<_>>()
        );
    }

    #[test]
    fn compact_copy_failure_preserves_heap_and_payload_address_domain() {
        let allocator = Arc::new(ToggleAllocator::new());
        let chunks = sequential_chunks(5);
        let mut heap_data =
            RetainedChunkVec::detached(MemoryTag::OrderBy, MemoryAccountingClass::Revocable);
        for values in &chunks {
            heap_data
                .push(make_int_chunk_with_allocator(
                    values,
                    allocator.clone() as Arc<dyn Allocator>,
                ))
                .unwrap();
        }
        let live = [3usize, VECTOR_SIZE + 5, 4 * VECTOR_SIZE - 1];
        let mut heap = manual_heap(&[], std::iter::empty(), live.len());
        heap.heap_data = heap_data;
        heap.heap = BinaryHeap::from(
            live.into_iter()
                .enumerate()
                .map(|(rank, index)| TopNEntry {
                    sort_key: (rank as u32).to_be_bytes().to_vec(),
                    index,
                })
                .collect::<Vec<_>>(),
        );
        let original_indices = heap
            .heap
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>();

        allocator.set_fail(true);
        heap.reduce().expect_err("compaction copy must fail");
        assert_eq!(heap.total_heap_data_size(), 5 * VECTOR_SIZE);
        assert_eq!(
            heap.heap
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            original_indices
        );

        allocator.set_fail(false);
        assert_eq!(extract_ints(&mut heap), live.map(|index| index as i32));
    }

    #[test]
    fn combine_copy_failure_preserves_both_heap_ownership_domains() {
        let allocator = Arc::new(ToggleAllocator::new());
        let mut left = manual_heap(&[vec![10, 11]], [(10, 0), (11, 1)], 2);
        let mut right = manual_heap(&[], std::iter::empty(), 2);
        right
            .heap_data
            .push(make_int_chunk_with_allocator(
                &[1, 2],
                allocator.clone() as Arc<dyn Allocator>,
            ))
            .unwrap();
        right.heap = BinaryHeap::from(vec![
            TopNEntry {
                sort_key: 1u32.to_be_bytes().to_vec(),
                index: 0,
            },
            TopNEntry {
                sort_key: 2u32.to_be_bytes().to_vec(),
                index: 1,
            },
        ]);

        allocator.set_fail(true);
        left.combine(&mut right)
            .expect_err("merge gather must fail");
        allocator.set_fail(false);

        assert_eq!(extract_ints(&mut left), vec![10, 11]);
        assert_eq!(extract_ints(&mut right), vec![1, 2]);
    }

    #[test]
    fn topn_heap_retained_chunks_respect_query_quota() {
        let pool = Arc::new(QueryMemoryPool::new(1));
        let owner: Arc<dyn MemoryOwner> = pool;
        let memory = MemoryAccountingContext::from_owner(
            owner,
            MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        );
        let orders = [OrderByNode {
            expression: Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
            ascending: true,
            nulls_first: false,
        }];
        let mut heap = TopNHeap::new_with_memory(vec![LogicalType::Integer], &orders, 4, 0, memory);
        let payload = make_int_chunk(&[3, 2, 1]);
        let sort = make_int_chunk(&[3, 2, 1]);

        let err = heap
            .sink_with_sort_chunk(&payload, &sort, None)
            .expect_err("tiny query quota must reject retained top-n chunks");
        assert!(err.to_string().contains("quota"));
    }
}
