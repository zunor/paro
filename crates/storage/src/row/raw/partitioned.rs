// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PartitionedRawRow - partitioned row spill substrate.
//!
//! This module provides the shared row-partition substrate used by external
//! hash aggregate/join spill paths.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::buffer::{BufferPool, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::SelectionVector;

use super::{RawRowAppendState, RawRowCollection, RawRowLayout, RawRowPinProperties};

const PARTITION_MAP_THRESHOLD: usize = 256;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PartitionEntry {
    offset: usize,
    length: usize,
}

/// Virtual dispatch interface for computing per-row partition indices.
pub trait PartitionIndexComputer: Send + Sync + std::fmt::Debug {
    /// Compute partition indices for rows selected by `append_sel`.
    ///
    /// `output.len()` is guaranteed to equal `append_count`.
    fn compute_partition_indices(
        &self,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
        output: &mut [usize],
    ) -> Result<()>;

    /// Maximum valid partition index.
    fn max_partition_index(&self) -> usize;
}

/// Local append state for parallel partition append.
#[derive(Debug)]
pub struct PartitionedRawRowAppendState {
    pub partition_indices: Vec<usize>,
    pub partition_sel: SelectionVector,
    pub reverse_partition_sel: SelectionVector,
    fixed_partition_counts: Vec<usize>,
    hash_partition_counts: HashMap<usize, usize>,
    partition_entries: Vec<(usize, PartitionEntry)>,
    pub partition_append_states: Vec<RawRowAppendState>,
}

impl PartitionedRawRowAppendState {
    pub fn new() -> Self {
        Self {
            partition_indices: Vec::new(),
            partition_sel: SelectionVector::with_capacity(0),
            reverse_partition_sel: SelectionVector::with_capacity(0),
            fixed_partition_counts: Vec::new(),
            hash_partition_counts: HashMap::new(),
            partition_entries: Vec::new(),
            partition_append_states: Vec::new(),
        }
    }

    fn use_fixed_map(partition_count: usize) -> bool {
        partition_count < PARTITION_MAP_THRESHOLD
    }

    fn prepare_for_partition_count(&mut self, partition_count: usize) {
        if Self::use_fixed_map(partition_count) {
            self.fixed_partition_counts.resize(partition_count, 0);
            self.hash_partition_counts.clear();
        } else {
            self.fixed_partition_counts.clear();
            self.hash_partition_counts.clear();
        }
    }

    fn clear_partition_map(&mut self, partition_count: usize) {
        if Self::use_fixed_map(partition_count) {
            self.fixed_partition_counts.fill(0);
        } else {
            self.hash_partition_counts.clear();
        }
        self.partition_entries.clear();
    }

    fn add_partition_count(&mut self, partition_count: usize, partition_idx: usize) {
        if Self::use_fixed_map(partition_count) {
            self.fixed_partition_counts[partition_idx] += 1;
        } else {
            *self.hash_partition_counts.entry(partition_idx).or_insert(0) += 1;
        }
    }

    fn rebuild_partition_entries(&mut self, partition_count: usize) {
        self.partition_entries.clear();
        let mut running_offset = 0usize;

        if Self::use_fixed_map(partition_count) {
            for partition_idx in 0..partition_count {
                let length = self.fixed_partition_counts[partition_idx];
                if length == 0 {
                    continue;
                }
                self.partition_entries.push((
                    partition_idx,
                    PartitionEntry {
                        offset: running_offset,
                        length,
                    },
                ));
                running_offset += length;
            }
        } else {
            let mut keys: Vec<usize> = self.hash_partition_counts.keys().copied().collect();
            keys.sort_unstable();
            for partition_idx in keys {
                let length = *self.hash_partition_counts.get(&partition_idx).unwrap_or(&0);
                if length == 0 {
                    continue;
                }
                self.partition_entries.push((
                    partition_idx,
                    PartitionEntry {
                        offset: running_offset,
                        length,
                    },
                ));
                running_offset += length;
            }
        }
    }

    fn single_partition_index(&self) -> Option<usize> {
        if self.partition_entries.len() == 1 {
            Some(self.partition_entries[0].0)
        } else {
            None
        }
    }
}

impl Default for PartitionedRawRowAppendState {
    fn default() -> Self {
        Self::new()
    }
}

/// Partitioned row-data substrate.
#[derive(Debug)]
pub struct PartitionedRawRow {
    buffer_pool: Arc<BufferPool>,
    layout: Arc<RawRowLayout>,
    tag: MemoryTag,
    partitioner: Arc<dyn PartitionIndexComputer>,
    partitions: Vec<RawRowCollection>,
    count: usize,
    data_size: usize,
    lock: Mutex<()>,
}

impl PartitionedRawRow {
    fn make_partition_collection(
        buffer_pool: &Arc<BufferPool>,
        layout: &Arc<RawRowLayout>,
        tag: MemoryTag,
    ) -> RawRowCollection {
        RawRowCollection::new(Arc::clone(buffer_pool), Arc::clone(layout), tag)
    }

    pub fn new(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RawRowLayout>,
        tag: MemoryTag,
        partitioner: Arc<dyn PartitionIndexComputer>,
    ) -> Self {
        let partition_count = partitioner.max_partition_index().saturating_add(1).max(1);
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(Self::make_partition_collection(&buffer_pool, &layout, tag));
        }

        Self {
            buffer_pool,
            layout,
            tag,
            partitioner,
            partitions,
            count: 0,
            data_size: 0,
            lock: Mutex::new(()),
        }
    }

    pub fn create_shared(&self) -> Self {
        Self::new(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.layout),
            self.tag,
            Arc::clone(&self.partitioner),
        )
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn size_in_bytes(&self) -> usize {
        self.data_size
    }

    pub fn get_partitions(&self) -> &[RawRowCollection] {
        &self.partitions
    }

    pub fn get_partitions_mut(&mut self) -> &mut [RawRowCollection] {
        &mut self.partitions
    }

    pub fn initialize_append_state(
        &mut self,
        state: &mut PartitionedRawRowAppendState,
        properties: RawRowPinProperties,
    ) {
        state.partition_indices.clear();
        state.partition_entries.clear();
        state.partition_sel = SelectionVector::with_capacity(0);
        state.reverse_partition_sel = SelectionVector::with_capacity(0);
        state.prepare_for_partition_count(self.partition_count());

        state.partition_append_states.clear();
        state
            .partition_append_states
            .reserve(self.partition_count());
        for partition in &mut self.partitions {
            let mut append_state = RawRowAppendState::new();
            partition.initialize_append(&mut append_state, properties);
            state.partition_append_states.push(append_state);
        }
    }

    pub fn append(
        &mut self,
        state: &mut PartitionedRawRowAppendState,
        input: &Chunk,
    ) -> Result<()> {
        let append_sel = SelectionVector::incremental(input.size());
        self.append_with_sel(state, input, &append_sel, input.size())
    }

    pub fn append_with_sel(
        &mut self,
        state: &mut PartitionedRawRowAppendState,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
    ) -> Result<()> {
        if append_count == 0 {
            return Ok(());
        }
        if append_sel.len() < append_count {
            return Err(paro_error::invalid_input(format!(
                "append selection too small: sel_len={}, append_count={append_count}",
                append_sel.len()
            )));
        }

        state.partition_indices.resize(append_count, 0);
        self.partitioner.compute_partition_indices(
            input,
            append_sel,
            append_count,
            &mut state.partition_indices,
        )?;

        for &partition_idx in &state.partition_indices {
            if partition_idx >= self.partition_count() {
                return Err(paro_error::internal(format!(
                    "partition index out of bounds: index={partition_idx}, partition_count={}",
                    self.partition_count()
                )));
            }
        }

        self.build_partition_sel(state, input.size(), append_sel, append_count);

        if let Some(single_partition_idx) = state.single_partition_index() {
            let partition = &mut self.partitions[single_partition_idx];
            let partition_state = &mut state.partition_append_states[single_partition_idx];
            let size_before = partition.size_in_bytes();
            let appended =
                partition.append_with_sel(partition_state, input, append_sel, append_count)?;
            self.count += appended;
            self.data_size += partition.size_in_bytes().saturating_sub(size_before);
            self.verify();
            return Ok(());
        }

        let use_fixed_map = PartitionedRawRowAppendState::use_fixed_map(self.partition_count());
        let mut fixed_write_offsets = if use_fixed_map {
            vec![usize::MAX; self.partition_count()]
        } else {
            Vec::new()
        };
        let mut hash_write_offsets: HashMap<usize, usize> = if use_fixed_map {
            HashMap::new()
        } else {
            HashMap::with_capacity(state.partition_entries.len())
        };

        for (partition_idx, entry) in &state.partition_entries {
            if use_fixed_map {
                fixed_write_offsets[*partition_idx] = entry.offset;
            } else {
                hash_write_offsets.insert(*partition_idx, entry.offset);
            }
        }

        for (partition_idx, entry) in &state.partition_entries {
            let mut partition_sel_indices = Vec::with_capacity(entry.length);
            for _ in 0..entry.length {
                let write_offset = if use_fixed_map {
                    let offset = fixed_write_offsets[*partition_idx];
                    fixed_write_offsets[*partition_idx] = offset.saturating_add(1);
                    offset
                } else {
                    let offset = hash_write_offsets
                        .get_mut(partition_idx)
                        .expect("partition write offset must exist");
                    let current = *offset;
                    *offset = current.saturating_add(1);
                    current
                };
                partition_sel_indices.push(state.partition_sel.get(write_offset) as u32);
            }

            let partition_sel = SelectionVector::from_indices(partition_sel_indices);
            let partition = &mut self.partitions[*partition_idx];
            let partition_state = &mut state.partition_append_states[*partition_idx];
            let size_before = partition.size_in_bytes();
            let appended =
                partition.append_with_sel(partition_state, input, &partition_sel, entry.length)?;
            self.count += appended;
            self.data_size += partition.size_in_bytes().saturating_sub(size_before);
        }

        self.verify();
        Ok(())
    }

    pub fn flush_append_state(&mut self, state: &mut PartitionedRawRowAppendState) {
        for (partition, append_state) in self
            .partitions
            .iter_mut()
            .zip(state.partition_append_states.iter_mut())
        {
            partition.finalize_append(append_state);
        }
    }

    pub fn combine(&mut self, other: &mut PartitionedRawRow) -> Result<()> {
        if self.partition_count() != other.partition_count() {
            return Err(paro_error::invalid_input(format!(
                "partition count mismatch: left={}, right={}",
                self.partition_count(),
                other.partition_count()
            )));
        }

        if self.layout.get_types() != other.layout.get_types() {
            return Err(paro_error::invalid_input(
                "cannot combine partitioned raw rows with mismatched layouts",
            ));
        }

        if other.count == 0 {
            return Ok(());
        }

        {
            let _guard = self.lock.lock().unwrap();
            let other_buffer_pool = Arc::clone(&other.buffer_pool);
            let other_layout = Arc::clone(&other.layout);
            let other_tag = other.tag;
            for idx in 0..self.partition_count() {
                let replacement =
                    Self::make_partition_collection(&other_buffer_pool, &other_layout, other_tag);
                let other_partition = std::mem::replace(&mut other.partitions[idx], replacement);
                self.partitions[idx].combine(other_partition);
            }
        }

        self.recompute_totals();
        other.recompute_totals();
        Ok(())
    }

    pub fn unpin(&self) {
        for partition in &self.partitions {
            partition.unpin();
        }
    }

    pub fn get_unpartitioned(&mut self) -> RawRowCollection {
        let mut combined =
            Self::make_partition_collection(&self.buffer_pool, &self.layout, self.tag);
        let buffer_pool = Arc::clone(&self.buffer_pool);
        let layout = Arc::clone(&self.layout);
        let tag = self.tag;

        for idx in 0..self.partition_count() {
            let replacement = Self::make_partition_collection(&buffer_pool, &layout, tag);
            let partition = std::mem::replace(&mut self.partitions[idx], replacement);
            combined.combine(partition);
        }

        self.count = 0;
        self.data_size = 0;
        self.verify();
        combined
    }

    pub fn get_sizes_and_counts(&self) -> (Vec<usize>, Vec<usize>) {
        let mut partition_sizes = Vec::with_capacity(self.partition_count());
        let mut partition_counts = Vec::with_capacity(self.partition_count());
        for partition in &self.partitions {
            partition_sizes.push(partition.size_in_bytes());
            partition_counts.push(partition.count());
        }
        (partition_sizes, partition_counts)
    }

    pub fn reset(&mut self) {
        for partition in &mut self.partitions {
            partition.reset();
        }
        self.count = 0;
        self.data_size = 0;
        self.verify();
    }

    fn build_partition_sel(
        &self,
        state: &mut PartitionedRawRowAppendState,
        input_size: usize,
        append_sel: &SelectionVector,
        append_count: usize,
    ) {
        state.clear_partition_map(self.partition_count());

        for i in 0..append_count {
            let partition_idx = state.partition_indices[i];
            state.add_partition_count(self.partition_count(), partition_idx);
        }

        state.rebuild_partition_entries(self.partition_count());

        state.partition_sel = SelectionVector::with_capacity(append_count);
        state.partition_sel.set_len(append_count);
        let reverse_capacity = input_size.max(append_count);
        state.reverse_partition_sel = SelectionVector::with_capacity(reverse_capacity);
        state.reverse_partition_sel.set_len(reverse_capacity);

        let mut write_offsets = vec![0usize; self.partition_count()];
        for (partition_idx, entry) in &state.partition_entries {
            write_offsets[*partition_idx] = entry.offset;
        }

        for i in 0..append_count {
            let source_idx = append_sel.get(i);
            let partition_idx = state.partition_indices[i];
            let write_offset = write_offsets[partition_idx];
            write_offsets[partition_idx] = write_offset.saturating_add(1);
            state.partition_sel.set(write_offset, source_idx);
            if source_idx < reverse_capacity {
                state.reverse_partition_sel.set(source_idx, write_offset);
            }
        }
    }

    fn recompute_totals(&mut self) {
        self.count = self.partitions.iter().map(RawRowCollection::count).sum();
        self.data_size = self
            .partitions
            .iter()
            .map(RawRowCollection::size_in_bytes)
            .sum();
        self.verify();
    }

    fn verify(&self) {
        let total_count: usize = self.partitions.iter().map(RawRowCollection::count).sum();
        let total_size: usize = self
            .partitions
            .iter()
            .map(RawRowCollection::size_in_bytes)
            .sum();
        debug_assert_eq!(total_count, self.count);
        debug_assert_eq!(total_size, self.data_size);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use crate::buffer::{BufferPool, MemoryTag};

    use super::{PartitionIndexComputer, PartitionedRawRow, PartitionedRawRowAppendState};
    use crate::row::raw::{
        gather_chunk, RawRowCollection, RawRowLayout, RawRowPinProperties, RawRowScanState,
        RawRowValidityType,
    };

    #[derive(Debug)]
    struct ModuloPartitionComputer {
        partition_count: usize,
        column_idx: usize,
    }

    impl ModuloPartitionComputer {
        fn new(partition_count: usize, column_idx: usize) -> Self {
            Self {
                partition_count,
                column_idx,
            }
        }
    }

    impl PartitionIndexComputer for ModuloPartitionComputer {
        fn compute_partition_indices(
            &self,
            input: &Chunk,
            append_sel: &paro_common::vector::SelectionVector,
            append_count: usize,
            output: &mut [usize],
        ) -> Result<()> {
            let key_col = input.column(self.column_idx).ok_or_else(|| {
                paro_common::error::internal(format!(
                    "partition key column out of bounds: idx={}, column_count={}",
                    self.column_idx,
                    input.column_count()
                ))
            })?;

            for i in 0..append_count {
                let row_idx = append_sel.get(i);
                let key = key_col.get_i32(row_idx).ok_or_else(|| {
                    paro_common::error::internal(format!("partition key at row {row_idx} is NULL"))
                })?;
                output[i] = (key.unsigned_abs() as usize) % self.partition_count;
            }
            Ok(())
        }

        fn max_partition_index(&self) -> usize {
            self.partition_count.saturating_sub(1)
        }
    }

    fn create_layout(types: Vec<LogicalType>) -> Arc<RawRowLayout> {
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        Arc::new(layout)
    }

    fn build_chunk(start: i32, count: usize) -> Chunk {
        let ints: Vec<i32> = (start..start + count as i32).collect();
        Chunk::from_vectors(vec![Vector::from_i32(&ints)])
    }

    fn collect_keys_from_collection(
        collection: &RawRowCollection,
        types: &[LogicalType],
    ) -> Vec<i32> {
        let mut seen = Vec::new();
        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);

        for chunk_idx in 0..collection.chunk_count() {
            let count = collection
                .fetch_chunk(&mut scan_state, chunk_idx, true)
                .expect("fetch_chunk should succeed");
            let mut row_locations = Vec::with_capacity(count);
            let row_locations_vec = &scan_state.chunk_state.row_locations;
            unsafe {
                let ptrs = row_locations_vec.flat_data::<u64>();
                for idx in 0..count {
                    row_locations.push(*ptrs.add(idx) as *const u8);
                }
            }

            let mut out = Chunk::initialize(types, count.max(1));
            gather_chunk(collection, &row_locations, &mut out, count);
            for row_idx in 0..count {
                seen.push(out.column(0).unwrap().get_i32(row_idx).unwrap());
            }
        }
        seen
    }

    fn collect_keys(partitioned: &PartitionedRawRow, types: &[LogicalType]) -> Vec<i32> {
        let mut seen = Vec::new();
        for partition in partitioned.get_partitions() {
            seen.extend(collect_keys_from_collection(partition, types));
        }
        seen
    }

    #[test]
    fn test_parallel_append_flush_combine_scan_roundtrip() {
        let buffer_pool = BufferPool::new_arc(64 * 1024 * 1024);
        let types = vec![LogicalType::Integer];
        let layout = create_layout(types.clone());
        let partitioner = Arc::new(ModuloPartitionComputer::new(8, 0));

        let mut global = PartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            partitioner,
        );

        let mut local1 = global.create_shared();
        let mut local2 = global.create_shared();

        let mut state1 = PartitionedRawRowAppendState::new();
        let mut state2 = PartitionedRawRowAppendState::new();

        local1.initialize_append_state(&mut state1, RawRowPinProperties::UnpinAfterDone);
        local2.initialize_append_state(&mut state2, RawRowPinProperties::UnpinAfterDone);

        local1.append(&mut state1, &build_chunk(0, 256)).unwrap();
        local1.append(&mut state1, &build_chunk(256, 128)).unwrap();
        local2.append(&mut state2, &build_chunk(384, 192)).unwrap();
        local2.append(&mut state2, &build_chunk(576, 64)).unwrap();

        local1.flush_append_state(&mut state1);
        local2.flush_append_state(&mut state2);

        let mut local1_seen = collect_keys(&local1, &types);
        local1_seen.sort_unstable();
        assert_eq!(local1_seen, (0..384).collect::<Vec<i32>>());
        let mut local2_seen = collect_keys(&local2, &types);
        local2_seen.sort_unstable();
        assert_eq!(local2_seen, (384..640).collect::<Vec<i32>>());

        global.combine(&mut local1).unwrap();
        global.combine(&mut local2).unwrap();

        let expected_rows = 640usize;
        assert_eq!(global.count(), expected_rows);

        let (sizes, counts) = global.get_sizes_and_counts();
        assert_eq!(counts.iter().sum::<usize>(), expected_rows);
        assert_eq!(sizes.iter().sum::<usize>(), global.size_in_bytes());
        assert_eq!(global.partition_count(), 8);

        let mut seen = Vec::new();
        for (partition_idx, partition) in global.get_partitions().iter().enumerate() {
            let partition_keys = collect_keys_from_collection(partition, &types);
            for key in partition_keys {
                assert_eq!((key.unsigned_abs() as usize) % 8, partition_idx);
                seen.push(key);
            }
        }

        seen.sort_unstable();
        let expected: Vec<i32> = (0..expected_rows as i32).collect();
        assert_eq!(seen, expected);

        let unpartitioned = global.get_unpartitioned();
        assert_eq!(unpartitioned.count(), expected_rows);
        assert_eq!(global.count(), 0);
        assert_eq!(global.size_in_bytes(), 0);

        let mut merged_seen = collect_keys_from_collection(&unpartitioned, &types);
        merged_seen.sort_unstable();
        assert_eq!(merged_seen, expected);
    }
}
