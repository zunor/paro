//! Heap implementation used by [`super::topn::TopN`].

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem::size_of;
use std::sync::Mutex;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::sort_key::{compare_keys, encode_column, OrderModifiers};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::OrderByNode;

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
    heap_data: Vec<Chunk>,
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
        let heap_size = limit.saturating_add(offset);
        let modifiers = orders
            .iter()
            .map(|order| OrderModifiers::new(order.ascending, order.nulls_first))
            .collect();

        Self {
            heap: BinaryHeap::with_capacity(heap_size),
            heap_data: Vec::new(),
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
            + self.heap_data.capacity() * size_of::<Chunk>()
            + self
                .heap_data
                .iter()
                .map(Chunk::get_allocation_size)
                .sum::<usize>()
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

        // We have too many values in heap_data - compact them
        // Extract current heap entries (sorted)
        let mut entries: Vec<TopNEntry> = self.heap.drain().collect();
        entries.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        if entries.is_empty() {
            return Ok(());
        }

        // Build new compacted heap_data with only the entries we need
        let mut new_heap_data = Vec::new();
        let mut current_chunk_rows = Vec::new();

        for (new_idx, entry) in entries.iter_mut().enumerate() {
            // Find the row in old heap_data
            let (chunk_idx, local_idx) = self.find_row_in_chunks(&self.heap_data, entry.index);
            let old_chunk = &self.heap_data[chunk_idx];

            // Collect row for new chunk
            current_chunk_rows.push(local_idx);

            // Update entry index to new position
            entry.index = new_idx;

            // Flush when we have enough rows for a chunk
            if current_chunk_rows.len() >= paro_common::vector::VECTOR_SIZE {
                let new_chunk = self.copy_rows(old_chunk, &current_chunk_rows);
                new_heap_data.push(new_chunk);
                current_chunk_rows.clear();
            }
        }

        // Flush remaining rows
        if !current_chunk_rows.is_empty() {
            // Need to collect from potentially multiple source chunks
            let mut all_rows = Vec::new();
            for entry in &entries[entries.len() - current_chunk_rows.len()..] {
                let (chunk_idx, local_idx) = self.find_row_in_chunks(&self.heap_data, entry.index);
                all_rows.push((chunk_idx, local_idx));
            }

            // Build final chunk by copying from source chunks
            let new_chunk = self.build_chunk_from_multiple_sources(&all_rows)?;
            new_heap_data.push(new_chunk);
        }

        // Replace old heap_data with compacted version
        self.heap_data = new_heap_data;

        // Rebuild heap from sorted entries
        self.heap = BinaryHeap::from(entries);

        Ok(())
    }

    /// Build a chunk by copying rows from multiple source chunks.
    fn build_chunk_from_multiple_sources(&self, rows: &[(usize, usize)]) -> Result<Chunk> {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let mut output_vectors = Vec::with_capacity(self.payload_types.len());

        for (col_idx, col_type) in self.payload_types.iter().enumerate() {
            let mut dst_vec = Vector::with_capacity(col_type.clone(), rows.len());

            for (dst_idx, &(chunk_idx, local_idx)) in rows.iter().enumerate() {
                let src_vec = &self.heap_data[chunk_idx].data[col_idx];

                if src_vec.is_null(local_idx) {
                    dst_vec.set_null(dst_idx, true);
                } else {
                    self.copy_value(src_vec, local_idx, &mut dst_vec, dst_idx);
                }
            }

            dst_vec.set_count(rows.len());
            output_vectors.push(Arc::new(dst_vec));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors);
        result.set_cardinality(rows.len());
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

        for row_idx in 0..sort_chunk.size() {
            let sort_key = self.encode_sort_key(sort_chunk, row_idx, &sort_indices)?;

            // Row passes if its sort key is less than the boundary
            if compare_keys(&sort_key, &boundary_key) == Ordering::Less {
                passing_rows.push(row_idx);
            }
        }

        if passing_rows.is_empty() {
            // No rows pass - return empty chunks
            return Ok((Chunk::new(), Chunk::new()));
        }

        if passing_rows.len() == payload_chunk.size() {
            // All rows pass - return original chunks
            return Ok((payload_chunk.clone(), sort_chunk.clone()));
        }

        // Some rows pass - create filtered chunks
        let filtered_payload = self.copy_rows(payload_chunk, &passing_rows);
        let filtered_sort = self.copy_rows_generic(sort_chunk, &passing_rows);
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

        // First pass: add entries with temporary indices
        for row_idx in 0..sort_chunk.size() {
            let sort_key = self.encode_sort_key(sort_chunk, row_idx, &sort_indices)?;

            if !self.should_add_entry(&sort_key) {
                continue;
            }

            let entry = TopNEntry {
                sort_key,
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
            let new_chunk = self.copy_rows(payload_chunk, &rows_to_copy);

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
            self.heap_data.push(new_chunk);
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

        // Process each row
        for row_idx in 0..sort_chunk.size() {
            let sort_key = self.encode_sort_key(sort_chunk, row_idx, &sort_indices)?;

            if !self.should_add_entry(&sort_key) {
                continue;
            }

            let entry = TopNEntry {
                sort_key,
                index: base_index + rows_to_copy.len(),
            };

            self.add_entry_to_heap(entry);
            rows_to_copy.push(row_idx);
        }

        if !rows_to_copy.is_empty() {
            let new_chunk = self.copy_rows(payload_chunk, &rows_to_copy);
            self.heap_data.push(new_chunk);
        }

        Ok(())
    }

    /// Copy selected rows from a chunk (generic version for any chunk).
    fn copy_rows_generic(&self, chunk: &Chunk, row_indices: &[usize]) -> Chunk {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let mut output_vectors = Vec::with_capacity(chunk.data.len());

        for col_idx in 0..chunk.column_count() {
            let src_vec = &chunk.data[col_idx];
            let col_type = src_vec.logical_type().clone();
            let mut dst_vec = Vector::with_capacity(col_type, row_indices.len());

            for (dst_idx, &src_idx) in row_indices.iter().enumerate() {
                if src_vec.is_null(src_idx) {
                    dst_vec.set_null(dst_idx, true);
                } else {
                    dst_vec.copy_at(dst_idx, src_vec, src_idx);
                }
            }

            dst_vec.set_count(row_indices.len());
            output_vectors.push(Arc::new(dst_vec));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors);
        result.set_cardinality(row_indices.len());
        result
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

    fn encode_sort_key(&self, chunk: &Chunk, row_idx: usize, columns: &[usize]) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        self.encode_sort_key_into(chunk, row_idx, columns, &mut result)?;
        Ok(result)
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
    fn copy_rows(&self, chunk: &Chunk, row_indices: &[usize]) -> Chunk {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let mut output_vectors = Vec::with_capacity(chunk.data.len());

        for (col_idx, col_type) in self.payload_types.iter().enumerate() {
            let src_vec = &chunk.data[col_idx];
            let mut dst_vec = Vector::with_capacity(col_type.clone(), row_indices.len());

            for (dst_idx, &src_idx) in row_indices.iter().enumerate() {
                if src_vec.is_null(src_idx) {
                    dst_vec.set_null(dst_idx, true);
                } else {
                    self.copy_value(src_vec, src_idx, &mut dst_vec, dst_idx);
                }
            }

            dst_vec.set_count(row_indices.len());
            output_vectors.push(Arc::new(dst_vec));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors);
        result.set_cardinality(row_indices.len());
        result
    }

    /// Copy a single value between vectors.
    fn copy_value(
        &self,
        src: &paro_common::vector::Vector,
        src_idx: usize,
        dst: &mut paro_common::vector::Vector,
        dst_idx: usize,
    ) {
        // Use copy_at which handles all types including Array
        dst.copy_at(dst_idx, src, src_idx);
    }

    /// Combine another heap into this one.
    ///
    /// Used to merge results from parallel sinks.
    pub fn combine(&mut self, other: &mut TopNHeap) -> Result<()> {
        // Drain and sort the other heap explicitly before merging.
        // `BinaryHeap::drain()` does not yield items in heap order, so relying on
        // `finalize()` + `drain()` can make the early-stop optimization skip better rows.
        let mut other_entries: Vec<TopNEntry> = other.heap.drain().collect();
        other_entries.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        let base_index = self.total_heap_data_size();
        let mut rows_to_copy = Vec::new();

        for entry in other_entries {
            if !self.should_add_entry(&entry.sort_key) {
                break; // Since other is sorted, we can stop here
            }

            let new_entry = TopNEntry {
                sort_key: entry.sort_key,
                index: base_index + rows_to_copy.len(),
            };

            self.add_entry_to_heap(new_entry);
            rows_to_copy.push(entry.index);
        }

        if !rows_to_copy.is_empty() {
            // Copy data from other's heap_data
            for &row_idx in &rows_to_copy {
                let (chunk_idx, local_idx) = self.find_row_in_chunks(&other.heap_data, row_idx);
                let chunk = &other.heap_data[chunk_idx];
                let copied = self.copy_rows(chunk, &[local_idx]);
                self.heap_data.push(copied);
            }
        }

        Ok(())
    }

    /// Find which chunk and local index a global row index refers to.
    fn find_row_in_chunks(&self, chunks: &[Chunk], global_idx: usize) -> (usize, usize) {
        let mut offset = 0;
        for (chunk_idx, chunk) in chunks.iter().enumerate() {
            if global_idx < offset + chunk.size() {
                return (chunk_idx, global_idx - offset);
            }
            offset += chunk.size();
        }
        panic!("Row index {} out of bounds", global_idx);
    }

    /// Finalize the heap by sorting entries.
    ///
    /// After finalization, entries are in ascending order.
    pub fn finalize(&mut self) {
        // Convert heap to sorted vector
        let mut entries: Vec<TopNEntry> = self.heap.drain().collect();
        entries.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        // Rebuild heap from sorted entries
        self.heap = BinaryHeap::from(entries);
    }

    /// Get the total number of rows in heap_data.
    fn total_heap_data_size(&self) -> usize {
        self.heap_data.iter().map(|c| c.size()).sum()
    }

    /// Extract sorted results, applying OFFSET.
    ///
    /// Returns chunks of sorted data, skipping the first `offset` rows.
    pub fn extract_results(&mut self) -> Result<Vec<Chunk>> {
        // Finalize to ensure sorted order
        self.finalize();

        // Extract sorted entries
        let mut sorted_entries: Vec<TopNEntry> = self.heap.drain().collect();
        sorted_entries.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));

        // Apply offset
        let start_idx = self.offset.min(sorted_entries.len());
        let result_entries = &sorted_entries[start_idx..];

        if result_entries.is_empty() {
            return Ok(vec![]);
        }

        // Build result chunks
        let mut result_chunks = Vec::new();
        let mut current_rows = Vec::new();

        for entry in result_entries {
            current_rows.push(entry.index);

            if current_rows.len() >= paro_common::vector::VECTOR_SIZE {
                let chunk = self.build_result_chunk(&current_rows)?;
                result_chunks.push(chunk);
                current_rows.clear();
            }
        }

        if !current_rows.is_empty() {
            let chunk = self.build_result_chunk(&current_rows)?;
            result_chunks.push(chunk);
        }

        Ok(result_chunks)
    }

    /// Build a result chunk from row indices.
    fn build_result_chunk(&self, row_indices: &[usize]) -> Result<Chunk> {
        use paro_common::vector::Vector;
        use std::sync::Arc;

        let mut output_vectors = Vec::with_capacity(self.payload_types.len());

        for (col_idx, col_type) in self.payload_types.iter().enumerate() {
            let mut dst_vec = Vector::with_capacity(col_type.clone(), row_indices.len());

            for (dst_idx, &global_idx) in row_indices.iter().enumerate() {
                let (chunk_idx, local_idx) = self.find_row_in_chunks(&self.heap_data, global_idx);
                let src_vec = &self.heap_data[chunk_idx].data[col_idx];

                if src_vec.is_null(local_idx) {
                    dst_vec.set_null(dst_idx, true);
                } else {
                    self.copy_value(src_vec, local_idx, &mut dst_vec, dst_idx);
                }
            }

            dst_vec.set_count(row_indices.len());
            output_vectors.push(Arc::new(dst_vec));
        }

        let mut result = Chunk::from_arc_vectors(output_vectors);
        result.set_cardinality(row_indices.len());
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::vector::Vector;

    fn make_int_chunk(values: &[i32]) -> Chunk {
        let mut vector = Vector::with_capacity(LogicalType::Integer, values.len());
        for (idx, value) in values.iter().enumerate() {
            vector.set_i32(idx, *value);
        }
        vector.set_count(values.len());
        let mut chunk = Chunk::from_vectors(vec![vector]);
        chunk.set_cardinality(values.len());
        chunk
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
            heap_data: vec![make_int_chunk(&[5, 6, 7])],
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
            heap_data: vec![make_int_chunk(&[9, 1, 2])],
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
}
