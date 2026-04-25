// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PartitionedColumnData - partitioned column spill substrate.
//!
//! This module provides a partitioned spill substrate for column data:
//! - unified partition index computation via virtual dispatch
//! - per-partition append state
//! - per-partition buffering with half-buffer flush threshold

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::buffer::{BufferPool, MemoryTag};
use paro_common::allocator::{default_allocator, Allocator, BufferAllocator, BufferManager};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

use super::{ColumnDataAllocator, ColumnDataAppendState, ColumnDataCollection};

const PARTITION_MAP_THRESHOLD: usize = 256;
const DEFAULT_BUFFER_SIZE: usize = 128;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PartitionEntry {
    offset: usize,
    length: usize,
}

/// Virtual dispatch interface for computing per-row partition indices.
pub trait ColumnPartitionIndexComputer: Send + Sync + std::fmt::Debug {
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

    /// Buffer size used by per-partition append buffers.
    ///
    /// Must be a power-of-two and at least 2.
    fn buffer_size(&self) -> usize {
        DEFAULT_BUFFER_SIZE
    }
}

/// Local append state for parallel partition append.
#[derive(Debug)]
pub struct PartitionedColumnDataAppendState {
    pub partition_indices: Vec<usize>,
    pub partition_sel: SelectionVector,
    fixed_partition_counts: Vec<usize>,
    hash_partition_counts: HashMap<usize, usize>,
    partition_entries: Vec<(usize, PartitionEntry)>,
    pub partition_buffers: Vec<Option<Chunk>>,
    pub partition_append_states: Vec<Option<ColumnDataAppendState>>,
}

impl PartitionedColumnDataAppendState {
    pub fn new() -> Self {
        Self {
            partition_indices: Vec::new(),
            partition_sel: SelectionVector::try_with_capacity(0, Arc::new(default_allocator()))
                .expect("zero-capacity partition selection allocation failed"),
            fixed_partition_counts: Vec::new(),
            hash_partition_counts: HashMap::new(),
            partition_entries: Vec::new(),
            partition_buffers: Vec::new(),
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

impl Default for PartitionedColumnDataAppendState {
    fn default() -> Self {
        Self::new()
    }
}

/// Partitioned column-data substrate.
#[derive(Debug)]
pub struct PartitionedColumnData {
    buffer_pool: Arc<BufferPool>,
    types: Vec<LogicalType>,
    tag: MemoryTag,
    partitioner: Arc<dyn ColumnPartitionIndexComputer>,
    buffer_size: usize,
    allocators: Arc<Vec<Arc<ColumnDataAllocator>>>,
    partitions: Vec<ColumnDataCollection>,
    count: usize,
    data_size: usize,
    lock: Mutex<()>,
}

impl PartitionedColumnData {
    fn chunk_allocator(&self) -> Arc<dyn Allocator> {
        Arc::new(BufferAllocator::new(
            Arc::clone(&self.buffer_pool) as Arc<dyn BufferManager>,
            self.tag,
        ))
    }

    fn make_partition_collection(
        allocator: &Arc<ColumnDataAllocator>,
        types: &[LogicalType],
    ) -> ColumnDataCollection {
        ColumnDataCollection::new(Arc::clone(allocator), types.to_vec())
    }

    fn create_allocators(
        buffer_pool: &Arc<BufferPool>,
        tag: MemoryTag,
        partition_count: usize,
    ) -> Arc<Vec<Arc<ColumnDataAllocator>>> {
        let mut allocators = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            allocators.push(Arc::new(ColumnDataAllocator::buffer_manager(
                Arc::clone(buffer_pool),
                tag,
            )));
        }
        Arc::new(allocators)
    }

    fn create_partitions(
        allocators: &Arc<Vec<Arc<ColumnDataAllocator>>>,
        types: &[LogicalType],
    ) -> Vec<ColumnDataCollection> {
        allocators
            .iter()
            .map(|allocator| Self::make_partition_collection(allocator, types))
            .collect()
    }

    fn normalize_buffer_size(buffer_size: usize) -> usize {
        let normalized = buffer_size.max(2);
        if !normalized.is_power_of_two() {
            normalized.next_power_of_two()
        } else {
            normalized
        }
    }

    fn from_shared(
        buffer_pool: Arc<BufferPool>,
        types: Vec<LogicalType>,
        tag: MemoryTag,
        partitioner: Arc<dyn ColumnPartitionIndexComputer>,
        buffer_size: usize,
        allocators: Arc<Vec<Arc<ColumnDataAllocator>>>,
    ) -> Self {
        let partitions = Self::create_partitions(&allocators, &types);
        Self {
            buffer_pool,
            types,
            tag,
            partitioner,
            buffer_size: Self::normalize_buffer_size(buffer_size),
            allocators,
            partitions,
            count: 0,
            data_size: 0,
            lock: Mutex::new(()),
        }
    }

    pub fn new(
        buffer_pool: Arc<BufferPool>,
        types: Vec<LogicalType>,
        tag: MemoryTag,
        partitioner: Arc<dyn ColumnPartitionIndexComputer>,
    ) -> Self {
        let partition_count = partitioner.max_partition_index().saturating_add(1).max(1);
        let allocators = Self::create_allocators(&buffer_pool, tag, partition_count);
        let buffer_size = partitioner.buffer_size();
        Self::from_shared(
            buffer_pool,
            types,
            tag,
            partitioner,
            buffer_size,
            allocators,
        )
    }

    pub fn create_shared(&self) -> Self {
        Self::from_shared(
            Arc::clone(&self.buffer_pool),
            self.types.clone(),
            self.tag,
            Arc::clone(&self.partitioner),
            self.buffer_size,
            Arc::clone(&self.allocators),
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

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn get_partitions(&self) -> &[ColumnDataCollection] {
        &self.partitions
    }

    pub fn get_partitions_mut(&mut self) -> &mut [ColumnDataCollection] {
        &mut self.partitions
    }

    pub fn initialize_append_state(&mut self, state: &mut PartitionedColumnDataAppendState) {
        state.partition_indices.clear();
        state.partition_sel = SelectionVector::try_with_capacity(0, self.chunk_allocator())
            .expect("zero-capacity partition selection allocation failed");
        state.partition_entries.clear();
        state.prepare_for_partition_count(self.partition_count());

        state.partition_append_states.clear();
        state
            .partition_append_states
            .resize_with(self.partition_count(), || None);
        state.partition_buffers.clear();
        state
            .partition_buffers
            .resize_with(self.partition_count(), || None);
    }

    pub fn append(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        input: &Chunk,
    ) -> Result<()> {
        let append_sel = SelectionVector::try_incremental(input.size(), input.allocator().clone())?;
        self.append_with_sel(state, input, &append_sel, input.size())
    }

    pub fn append_with_sel(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
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

        self.build_partition_sel(state, append_sel, append_count)?;

        if let Some(single_partition_idx) = state.single_partition_index() {
            if append_count == input.size() && append_count == append_sel.len() {
                self.flush_partition_buffer(state, single_partition_idx)?;
                self.ensure_partition_append_state(state, single_partition_idx);
                let partition = &mut self.partitions[single_partition_idx];
                let partition_state = state.partition_append_states[single_partition_idx]
                    .as_mut()
                    .expect("partition append state should exist");
                let count_before = partition.count();
                let size_before = partition.size_in_bytes();
                partition.append(partition_state, input)?;
                self.count += partition.count().saturating_sub(count_before);
                self.data_size += partition.size_in_bytes().saturating_sub(size_before);
            } else {
                self.append_selected_direct(state, input, single_partition_idx, 0, append_count)?;
            }
            self.verify();
            return Ok(());
        }

        let partition_entries = state.partition_entries.clone();
        for (partition_idx, entry) in partition_entries {
            if entry.length >= self.half_buffer_size() {
                self.append_selected_direct(
                    state,
                    input,
                    partition_idx,
                    entry.offset,
                    entry.length,
                )?;
            } else {
                self.append_selected_to_buffer(
                    state,
                    input,
                    partition_idx,
                    entry.offset,
                    entry.length,
                )?;
            }
        }

        self.verify();
        Ok(())
    }

    pub fn flush_append_state(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
    ) -> Result<()> {
        for partition_idx in 0..self.partition_count() {
            self.flush_partition_buffer(state, partition_idx)?;
        }
        self.verify();
        Ok(())
    }

    pub fn combine(&mut self, other: &mut PartitionedColumnData) -> Result<()> {
        if self.partition_count() != other.partition_count() {
            return Err(paro_error::invalid_input(format!(
                "partition count mismatch: left={}, right={}",
                self.partition_count(),
                other.partition_count()
            )));
        }
        if self.types != other.types {
            return Err(paro_error::invalid_input(
                "cannot combine partitioned column data with mismatched types",
            ));
        }

        if other.count == 0 {
            return Ok(());
        }

        {
            let _guard = self.lock.lock().unwrap();
            for idx in 0..self.partition_count() {
                self.partitions[idx].combine(&mut other.partitions[idx])?;
            }
        }

        self.recompute_totals();
        other.recompute_totals();
        Ok(())
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

    pub fn reset(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.reset()?;
        }
        self.count = 0;
        self.data_size = 0;
        self.verify();
        Ok(())
    }

    fn half_buffer_size(&self) -> usize {
        debug_assert!(self.buffer_size.is_power_of_two());
        self.buffer_size / 2
    }

    fn append_selected_direct(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        input: &Chunk,
        partition_idx: usize,
        partition_offset: usize,
        partition_length: usize,
    ) -> Result<()> {
        self.flush_partition_buffer(state, partition_idx)?;
        let selected =
            self.gather_selected_chunk(state, input, partition_offset, partition_length)?;
        self.ensure_partition_append_state(state, partition_idx);
        let partition = &mut self.partitions[partition_idx];
        let partition_state = state.partition_append_states[partition_idx]
            .as_mut()
            .expect("partition append state should exist");
        let count_before = partition.count();
        let size_before = partition.size_in_bytes();
        partition.append(partition_state, &selected)?;
        self.count += partition.count().saturating_sub(count_before);
        self.data_size += partition.size_in_bytes().saturating_sub(size_before);
        Ok(())
    }

    fn append_selected_to_buffer(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        input: &Chunk,
        partition_idx: usize,
        partition_offset: usize,
        partition_length: usize,
    ) -> Result<()> {
        if partition_length == 0 {
            return Ok(());
        }

        self.ensure_partition_buffer(state, partition_idx)?;
        let buffer = state.partition_buffers[partition_idx]
            .as_mut()
            .expect("partition buffer should exist");
        let mut buffer_count = buffer.size();
        let buffer_capacity = buffer.capacity();
        let new_count = buffer_count.saturating_add(partition_length);
        if new_count > buffer_capacity {
            return Err(paro_error::internal(format!(
                "partition buffer would overflow: partition_idx={partition_idx}, current={buffer_count}, incoming={partition_length}, capacity={buffer_capacity}"
            )));
        }
        // Grow vector lengths up-front so row copy can write into [buffer_count, new_count).
        buffer.try_set_cardinality(new_count)?;

        for idx in partition_offset..partition_offset + partition_length {
            let source_row = state.partition_sel.get(idx);
            if source_row >= input.size() {
                return Err(paro_error::internal(format!(
                    "partition selection row out of bounds: source_row={source_row}, input_size={}",
                    input.size()
                )));
            }

            for col_idx in 0..self.types.len() {
                let source_col = input.column(col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "missing source column while buffering partition rows: col_idx={col_idx}"
                    ))
                })?;
                let target_col = buffer.column_mut(col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "missing target column while buffering partition rows: col_idx={col_idx}"
                    ))
                })?;
                target_col.try_copy_at(buffer_count, source_col.as_ref(), source_row)?;
            }
            buffer_count += 1;
        }

        if buffer_count >= self.half_buffer_size() {
            self.flush_partition_buffer(state, partition_idx)?;
        }
        Ok(())
    }

    fn flush_partition_buffer(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        partition_idx: usize,
    ) -> Result<()> {
        let buffered_count = state.partition_buffers[partition_idx]
            .as_ref()
            .map(|buffer| buffer.size())
            .unwrap_or(0);
        if buffered_count == 0 {
            return Ok(());
        }

        self.ensure_partition_append_state(state, partition_idx);
        let partition = &mut self.partitions[partition_idx];
        let partition_state = state.partition_append_states[partition_idx]
            .as_mut()
            .expect("partition append state should exist");
        let partition_buffer = state.partition_buffers[partition_idx]
            .as_mut()
            .expect("partition buffer should exist");
        let count_before = partition.count();
        let size_before = partition.size_in_bytes();
        partition.append(partition_state, partition_buffer)?;
        self.count += partition.count().saturating_sub(count_before);
        self.data_size += partition.size_in_bytes().saturating_sub(size_before);
        partition_buffer.try_set_cardinality(0)?;
        Ok(())
    }

    fn ensure_partition_append_state(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        partition_idx: usize,
    ) {
        if state.partition_append_states[partition_idx].is_some() {
            return;
        }

        let mut append_state = ColumnDataAppendState::new();
        self.partitions[partition_idx].initialize_append(&mut append_state);
        state.partition_append_states[partition_idx] = Some(append_state);
    }

    fn ensure_partition_buffer(
        &self,
        state: &mut PartitionedColumnDataAppendState,
        partition_idx: usize,
    ) -> Result<()> {
        if state.partition_buffers[partition_idx].is_none() {
            state.partition_buffers[partition_idx] = Some(Chunk::try_initialize(
                &self.types,
                self.buffer_size,
                self.chunk_allocator(),
            )?);
        }
        Ok(())
    }

    fn gather_selected_chunk(
        &self,
        state: &PartitionedColumnDataAppendState,
        input: &Chunk,
        partition_offset: usize,
        partition_length: usize,
    ) -> Result<Chunk> {
        let mut rows = Vec::with_capacity(partition_length);
        for idx in partition_offset..partition_offset + partition_length {
            let row_idx = state.partition_sel.get(idx);
            if row_idx >= input.size() {
                return Err(paro_error::internal(format!(
                    "partition selection row out of bounds while gathering: row_idx={row_idx}, input_size={}",
                    input.size()
                )));
            }
            rows.push(row_idx);
        }
        gather_chunk_rows(input, &rows, &self.types)
    }

    fn build_partition_sel(
        &self,
        state: &mut PartitionedColumnDataAppendState,
        append_sel: &SelectionVector,
        append_count: usize,
    ) -> Result<()> {
        state.clear_partition_map(self.partition_count());

        for i in 0..append_count {
            let partition_idx = state.partition_indices[i];
            state.add_partition_count(self.partition_count(), partition_idx);
        }
        state.rebuild_partition_entries(self.partition_count());

        state.partition_sel =
            SelectionVector::try_with_capacity(append_count, self.chunk_allocator())?;
        state.partition_sel.set_len(append_count);

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
        }
        Ok(())
    }

    fn recompute_totals(&mut self) {
        self.count = self
            .partitions
            .iter()
            .map(ColumnDataCollection::count)
            .sum();
        self.data_size = self
            .partitions
            .iter()
            .map(ColumnDataCollection::size_in_bytes)
            .sum();
        self.verify();
    }

    fn verify(&self) {
        let total_count: usize = self
            .partitions
            .iter()
            .map(ColumnDataCollection::count)
            .sum();
        let total_size: usize = self
            .partitions
            .iter()
            .map(ColumnDataCollection::size_in_bytes)
            .sum();
        debug_assert_eq!(total_count, self.count);
        debug_assert_eq!(total_size, self.data_size);
    }
}

fn gather_chunk_rows(input: &Chunk, rows: &[usize], types: &[LogicalType]) -> Result<Chunk> {
    let mut gathered = Chunk::try_initialize(types, rows.len().max(1), input.allocator().clone())?;
    gathered.try_set_cardinality(rows.len())?;

    for col_idx in 0..input.column_count() {
        let source_col = input.column(col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing source column while gathering partition rows: col_idx={col_idx}"
            ))
        })?;
        let target_col = gathered.column_mut(col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing target column while gathering partition rows: col_idx={col_idx}"
            ))
        })?;
        for (target_row, source_row) in rows.iter().copied().enumerate() {
            target_col.try_copy_at(target_row, source_col.as_ref(), source_row)?;
        }
    }

    Ok(gathered)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::types::LogicalType;
    use paro_common::vector::SelectionVector;

    use crate::buffer::{BufferPool, MemoryTag};

    use super::{
        ColumnPartitionIndexComputer, PartitionedColumnData, PartitionedColumnDataAppendState,
    };
    use crate::column::{ColumnDataCollection, ColumnDataScanState};

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

    impl ColumnPartitionIndexComputer for ModuloPartitionComputer {
        fn compute_partition_indices(
            &self,
            input: &Chunk,
            append_sel: &SelectionVector,
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

    fn build_chunk(start: i32, count: usize) -> Chunk {
        let ints: Vec<i32> = (start..start + count as i32).collect();
        test_chunk_from_vectors(vec![test_i32_vector(&ints)])
    }

    fn collect_keys_from_collection(collection: &ColumnDataCollection) -> Vec<i32> {
        let mut keys = Vec::new();
        let mut scan_state = ColumnDataScanState::new();
        collection.initialize_scan(&mut scan_state, None);
        let mut out = test_chunk_with_capacity(&[LogicalType::Integer], 1);
        while collection
            .scan(&mut scan_state, &mut out)
            .expect("scan should succeed")
        {
            for row_idx in 0..out.size() {
                keys.push(out.column(0).unwrap().get_i32(row_idx).unwrap());
            }
        }
        keys
    }

    #[test]
    fn test_parallel_append_combine_scan_roundtrip() {
        let buffer_pool = BufferPool::new_arc(64 * 1024 * 1024);
        let types = vec![LogicalType::Integer];
        let partitioner = Arc::new(ModuloPartitionComputer::new(8, 0));

        let mut global = PartitionedColumnData::new(
            Arc::clone(&buffer_pool),
            types.clone(),
            MemoryTag::ColumnData,
            partitioner,
        );

        let mut local1 = global.create_shared();
        let mut local2 = global.create_shared();

        let mut state1 = PartitionedColumnDataAppendState::new();
        let mut state2 = PartitionedColumnDataAppendState::new();
        local1.initialize_append_state(&mut state1);
        local2.initialize_append_state(&mut state2);

        // 8 partitions -> each append contributes 40 rows per partition (< half-buffer 64).
        // The second append pushes per-partition buffers over half-buffer and triggers flush.
        local1.append(&mut state1, &build_chunk(0, 320)).unwrap();
        local1.append(&mut state1, &build_chunk(320, 320)).unwrap();
        local2.append(&mut state2, &build_chunk(640, 320)).unwrap();
        local2.append(&mut state2, &build_chunk(960, 320)).unwrap();

        local1.flush_append_state(&mut state1).unwrap();
        local2.flush_append_state(&mut state2).unwrap();

        assert_eq!(local1.count(), 640);
        assert_eq!(local2.count(), 640);

        global.combine(&mut local1).unwrap();
        global.combine(&mut local2).unwrap();

        let expected_rows = 1280usize;
        assert_eq!(global.count(), expected_rows);
        assert_eq!(global.partition_count(), 8);

        let (sizes, counts) = global.get_sizes_and_counts();
        assert_eq!(counts.iter().sum::<usize>(), expected_rows);
        assert_eq!(sizes.iter().sum::<usize>(), global.size_in_bytes());

        let mut seen = Vec::new();
        for (partition_idx, partition) in global.get_partitions().iter().enumerate() {
            let partition_keys = collect_keys_from_collection(partition);
            for key in partition_keys {
                assert_eq!((key.unsigned_abs() as usize) % 8, partition_idx);
                seen.push(key);
            }
        }

        seen.sort_unstable();
        let expected: Vec<i32> = (0..expected_rows as i32).collect();
        assert_eq!(seen, expected);
    }
}
