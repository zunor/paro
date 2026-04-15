// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Radix-partitioned raw row substrate.
//!
//! This module provides a radix wrapper around `PartitionedRawRow`
//! plus repartition support for external hash workflows.

use std::sync::Arc;

use crate::buffer::{BufferPool, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

use super::{
    gather_chunk, PartitionIndexComputer, PartitionedRawRow, PartitionedRawRowAppendState,
    RawRowCollection, RawRowLayout, RawRowPinProperties, RawRowScanState,
};

const MAX_RADIX_BITS: usize = 12;

#[derive(Debug, Clone, Copy)]
pub struct RadixPartitioning;

impl RadixPartitioning {
    pub const MAX_RADIX_BITS: usize = MAX_RADIX_BITS;

    #[inline]
    pub const fn number_of_partitions(radix_bits: usize) -> usize {
        1usize << radix_bits
    }

    /// Radix bits are taken from the high bits of the hash.
    #[inline]
    pub const fn shift(radix_bits: usize) -> usize {
        (u64::BITS as usize) - radix_bits
    }

    /// Mask covering the selected radix bits.
    #[inline]
    pub const fn mask(radix_bits: usize) -> u64 {
        if radix_bits == 0 {
            0
        } else {
            ((1u64 << radix_bits) - 1) << Self::shift(radix_bits)
        }
    }

    #[inline]
    pub const fn apply_mask(hash: u64, radix_bits: usize) -> usize {
        if radix_bits == 0 {
            0
        } else {
            ((hash & Self::mask(radix_bits)) >> Self::shift(radix_bits)) as usize
        }
    }
}

#[derive(Debug)]
struct RadixPartitionComputer {
    radix_bits: usize,
    hash_col_idx: usize,
    partition_mask: usize,
}

impl RadixPartitionComputer {
    fn new(radix_bits: usize, hash_col_idx: usize) -> Self {
        let partition_count = RadixPartitioning::number_of_partitions(radix_bits);
        Self {
            radix_bits,
            hash_col_idx,
            partition_mask: partition_count.saturating_sub(1),
        }
    }
}

impl PartitionIndexComputer for RadixPartitionComputer {
    fn compute_partition_indices(
        &self,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
        output: &mut [usize],
    ) -> Result<()> {
        let hash_column = input.column(self.hash_col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "hash column out of bounds while computing radix partitions: hash_col_idx={}, column_count={}",
                self.hash_col_idx,
                input.column_count()
            ))
        })?;

        if hash_column.logical_type() != &LogicalType::UBigInt {
            return Err(paro_error::invalid_input(format!(
                "radix hash column must be UBigInt, found {:?}",
                hash_column.logical_type()
            )));
        }

        for (i, output_val) in output.iter_mut().enumerate().take(append_count) {
            let row_idx = append_sel.get(i);
            let hash = hash_column.get_u64(row_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "hash value is NULL while computing radix partitions: row_idx={row_idx}"
                ))
            })?;
            *output_val =
                RadixPartitioning::apply_mask(hash, self.radix_bits) & self.partition_mask;
        }

        Ok(())
    }

    fn max_partition_index(&self) -> usize {
        self.partition_mask
    }
}

#[derive(Debug)]
struct RawRowChunkIterator<'a> {
    collection: &'a RawRowCollection,
    init_heap: bool,
    chunk_segment_idx: Vec<usize>,
    state: RawRowScanState,
    current_chunk_idx: usize,
    current_chunk_count: usize,
    done: bool,
}

impl<'a> RawRowChunkIterator<'a> {
    fn new(
        collection: &'a RawRowCollection,
        properties: RawRowPinProperties,
        init_heap: bool,
    ) -> Result<Self> {
        let total_chunks = collection.chunk_count();
        let mut chunk_segment_idx = Vec::with_capacity(total_chunks);
        for (segment_idx, segment) in collection.segments().iter().enumerate() {
            for _ in 0..segment.chunk_count() {
                chunk_segment_idx.push(segment_idx);
            }
        }

        let mut iter = Self {
            collection,
            init_heap,
            chunk_segment_idx,
            state: RawRowScanState::with_properties(properties),
            current_chunk_idx: 0,
            current_chunk_count: 0,
            done: total_chunks == 0,
        };

        if !iter.done {
            iter.current_chunk_count = iter.fetch_current_chunk()?;
        }

        Ok(iter)
    }

    fn done(&self) -> bool {
        self.done
    }

    fn current_chunk_count(&self) -> usize {
        self.current_chunk_count
    }

    fn current_row_locations(&self) -> Vec<*const u8> {
        let count = self.current_chunk_count;
        let mut row_locations = Vec::with_capacity(count);
        let row_locations_vec = &self.state.chunk_state.row_locations;
        unsafe {
            let ptrs = row_locations_vec.flat_data::<u64>();
            for idx in 0..count {
                row_locations.push(*ptrs.add(idx) as *const u8);
            }
        }
        row_locations
    }

    fn next(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }

        let prev_chunk_idx = self.current_chunk_idx;
        let next_chunk_idx = prev_chunk_idx.saturating_add(1);
        if next_chunk_idx >= self.chunk_segment_idx.len() {
            self.finalize_pin_state();
            self.done = true;
            self.current_chunk_count = 0;
            return Ok(false);
        }

        let prev_segment_idx = self.chunk_segment_idx[prev_chunk_idx];
        let next_segment_idx = self.chunk_segment_idx[next_chunk_idx];
        if next_segment_idx != prev_segment_idx {
            self.finalize_pin_state();
        }

        self.current_chunk_idx = next_chunk_idx;
        self.current_chunk_count = self.fetch_current_chunk()?;
        Ok(true)
    }

    fn fetch_current_chunk(&mut self) -> Result<usize> {
        self.collection
            .fetch_chunk(&mut self.state, self.current_chunk_idx, self.init_heap)
            .map_err(paro_error::internal)
    }

    fn finalize_pin_state(&mut self) {
        self.collection
            .finalize_pin_state(&mut self.state.pin_state);
    }
}

impl Drop for RawRowChunkIterator<'_> {
    fn drop(&mut self) {
        self.finalize_pin_state();
    }
}

/// Radix-specialized partitioned raw row substrate.
#[derive(Debug)]
pub struct RadixPartitionedRawRow {
    radix_bits: usize,
    hash_col_idx: usize,
    data: PartitionedRawRow,
}

impl RadixPartitionedRawRow {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RawRowLayout>,
        tag: MemoryTag,
        radix_bits: usize,
        hash_col_idx: usize,
    ) -> Result<Self> {
        if radix_bits == 0 || radix_bits > RadixPartitioning::MAX_RADIX_BITS {
            return Err(paro_error::invalid_input(format!(
                "invalid radix bits: radix_bits={radix_bits}, allowed=1..={}",
                RadixPartitioning::MAX_RADIX_BITS
            )));
        }

        if hash_col_idx >= layout.column_count() {
            return Err(paro_error::invalid_input(format!(
                "hash column out of bounds: hash_col_idx={hash_col_idx}, column_count={}",
                layout.column_count()
            )));
        }

        let hash_type = &layout.get_types()[hash_col_idx];
        if hash_type != &LogicalType::UBigInt {
            return Err(paro_error::invalid_input(format!(
                "radix hash column must be UBigInt, found {:?}",
                hash_type
            )));
        }

        let partitioner = Arc::new(RadixPartitionComputer::new(radix_bits, hash_col_idx));
        let data = PartitionedRawRow::new(buffer_pool, layout, tag, partitioner);
        Ok(Self {
            radix_bits,
            hash_col_idx,
            data,
        })
    }

    pub fn create_shared(&self) -> Self {
        Self {
            radix_bits: self.radix_bits,
            hash_col_idx: self.hash_col_idx,
            data: self.data.create_shared(),
        }
    }

    pub fn radix_bits(&self) -> usize {
        self.radix_bits
    }

    pub fn hash_col_idx(&self) -> usize {
        self.hash_col_idx
    }

    pub fn partition_count(&self) -> usize {
        self.data.partition_count()
    }

    pub fn count(&self) -> usize {
        self.data.count()
    }

    pub fn size_in_bytes(&self) -> usize {
        self.data.size_in_bytes()
    }

    pub fn get_partitions(&self) -> &[RawRowCollection] {
        self.data.get_partitions()
    }

    pub fn get_partitions_mut(&mut self) -> &mut [RawRowCollection] {
        self.data.get_partitions_mut()
    }

    pub fn initialize_append_state(
        &mut self,
        state: &mut PartitionedRawRowAppendState,
        properties: RawRowPinProperties,
    ) {
        self.data.initialize_append_state(state, properties);
    }

    pub fn append(
        &mut self,
        state: &mut PartitionedRawRowAppendState,
        input: &Chunk,
    ) -> Result<()> {
        self.data.append(state, input)
    }

    pub fn append_with_sel(
        &mut self,
        state: &mut PartitionedRawRowAppendState,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
    ) -> Result<()> {
        self.data
            .append_with_sel(state, input, append_sel, append_count)
    }

    pub fn flush_append_state(&mut self, state: &mut PartitionedRawRowAppendState) {
        self.data.flush_append_state(state);
    }

    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.validate_compatible(other)?;
        self.data.combine(&mut other.data)
    }

    pub fn unpin(&self) {
        self.data.unpin();
    }

    pub fn get_unpartitioned(&mut self) -> RawRowCollection {
        self.data.get_unpartitioned()
    }

    pub fn get_sizes_and_counts(&self) -> (Vec<usize>, Vec<usize>) {
        self.data.get_sizes_and_counts()
    }

    pub fn reset(&mut self) {
        self.data.reset();
    }

    /// Repartition into a target radix layout.
    ///
    /// This follows this flow:
    /// - Iterate old partitions using `RawRowChunkIterator`
    /// - `DESTROY_AFTER_DONE` pin policy for consumed partitions
    /// - Append into target radix partitions
    /// - Finalize target append states for finished partition ranges
    pub fn repartition(&mut self, new_partitioned_data: &mut Self) -> Result<()> {
        self.validate_compatible(new_partitioned_data)?;

        if self.count() == 0 {
            return Ok(());
        }

        if self.radix_bits == new_partitioned_data.radix_bits {
            return new_partitioned_data.combine(self);
        }

        if new_partitioned_data.radix_bits < self.radix_bits {
            return Err(paro_error::invalid_input(format!(
                "cannot repartition to fewer radix bits: from={} to={}",
                self.radix_bits, new_partitioned_data.radix_bits
            )));
        }

        let types = self.layout_types();
        let mut append_state = PartitionedRawRowAppendState::new();
        new_partitioned_data
            .initialize_append_state(&mut append_state, RawRowPinProperties::UnpinAfterDone);

        for partition_idx in 0..self.partition_count() {
            let partition_count = self.data.get_partitions()[partition_idx].count();
            if partition_count > 0 {
                {
                    let partition = &self.data.get_partitions()[partition_idx];
                    let mut iterator = RawRowChunkIterator::new(
                        partition,
                        RawRowPinProperties::DestroyAfterDone,
                        true,
                    )?;

                    while !iterator.done() {
                        let chunk_count = iterator.current_chunk_count();
                        if chunk_count > 0 {
                            let row_locations = iterator.current_row_locations();
                            let mut output = Chunk::initialize(&types, chunk_count.max(1));
                            gather_chunk(partition, &row_locations, &mut output, chunk_count);
                            new_partitioned_data.append(&mut append_state, &output)?;
                        }

                        if !iterator.next()? {
                            break;
                        }
                    }
                }

                self.repartition_finalize_states(
                    new_partitioned_data,
                    &mut append_state,
                    partition_idx,
                )?;
            }

            self.data.get_partitions_mut()[partition_idx].reset();
        }

        new_partitioned_data.flush_append_state(&mut append_state);
        self.data.reset();
        Ok(())
    }

    fn repartition_finalize_states(
        &self,
        new_partitioned_data: &mut Self,
        append_state: &mut PartitionedRawRowAppendState,
        finished_partition_idx: usize,
    ) -> Result<()> {
        if new_partitioned_data.radix_bits <= self.radix_bits {
            return Ok(());
        }

        let multiplier = RadixPartitioning::number_of_partitions(
            new_partitioned_data.radix_bits - self.radix_bits,
        );
        let from_idx = finished_partition_idx.saturating_mul(multiplier);
        let to_idx = from_idx.saturating_add(multiplier);
        if to_idx > new_partitioned_data.partition_count()
            || to_idx > append_state.partition_append_states.len()
        {
            return Err(paro_error::internal(format!(
                "invalid repartition finalize range: from_idx={from_idx}, to_idx={to_idx}, partition_count={}, append_states={}",
                new_partitioned_data.partition_count(),
                append_state.partition_append_states.len()
            )));
        }

        let partitions = new_partitioned_data.data.get_partitions_mut();
        for (partition_idx, partition) in partitions
            .iter_mut()
            .enumerate()
            .skip(from_idx)
            .take(to_idx - from_idx)
        {
            partition.finalize_append(&mut append_state.partition_append_states[partition_idx]);
        }
        Ok(())
    }

    fn validate_compatible(&self, other: &Self) -> Result<()> {
        if self.hash_col_idx != other.hash_col_idx {
            return Err(paro_error::invalid_input(format!(
                "hash column mismatch: left={}, right={}",
                self.hash_col_idx, other.hash_col_idx
            )));
        }
        if self.layout_types() != other.layout_types() {
            return Err(paro_error::invalid_input(
                "cannot combine/repartition radix data with different layouts",
            ));
        }
        Ok(())
    }

    fn layout_types(&self) -> Vec<LogicalType> {
        self.data
            .get_partitions()
            .first()
            .map(|partition| partition.layout().get_types().to_vec())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use crate::buffer::{BufferPool, MemoryTag};

    use super::{PartitionedRawRowAppendState, RadixPartitionedRawRow, RadixPartitioning};
    use crate::row::raw::{
        gather_chunk, RawRowCollection, RawRowLayout, RawRowPinProperties, RawRowScanState,
        RawRowValidityType,
    };

    fn create_layout(types: Vec<LogicalType>) -> Arc<RawRowLayout> {
        let mut layout = RawRowLayout::new();
        layout.initialize(types, RawRowValidityType::CanHaveNullValues);
        Arc::new(layout)
    }

    fn build_chunk_with_hashes(keys: &[i32], hashes: &[u64]) -> Chunk {
        let mut hash_vector = Vector::with_capacity(LogicalType::UBigInt, hashes.len().max(1));
        hash_vector.set_count(hashes.len());
        for (idx, hash) in hashes.iter().enumerate() {
            hash_vector.set_u64(idx, *hash);
        }
        Chunk::from_vectors(vec![Vector::from_i32(keys), hash_vector])
    }

    fn build_spill_aggregate_chunk(
        k1: &[i32],
        k2: &[i32],
        values: &[i32],
        hashes: &[u64],
    ) -> Chunk {
        let mut hash_vector = Vector::with_capacity(LogicalType::UBigInt, hashes.len().max(1));
        hash_vector.set_count(hashes.len());
        for (idx, hash) in hashes.iter().enumerate() {
            hash_vector.set_u64(idx, *hash);
        }
        Chunk::from_vectors(vec![
            Vector::from_i32(k1),
            Vector::from_i32(k2),
            Vector::from_i32(values),
            hash_vector,
        ])
    }

    #[inline]
    fn spread_hash(value: u64) -> u64 {
        value.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    fn collect_rows(collection: &RawRowCollection, types: &[LogicalType]) -> Vec<(i32, u64)> {
        let mut rows = Vec::new();
        let mut scan_state = RawRowScanState::new();
        collection.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);

        for chunk_idx in 0..collection.chunk_count() {
            let count = collection
                .fetch_chunk(&mut scan_state, chunk_idx, true)
                .expect("fetch_chunk should succeed");
            let mut row_locations = Vec::with_capacity(count);
            unsafe {
                let ptrs = scan_state.chunk_state.row_locations.flat_data::<u64>();
                for idx in 0..count {
                    row_locations.push(*ptrs.add(idx) as *const u8);
                }
            }

            let mut output = Chunk::initialize(types, count.max(1));
            gather_chunk(collection, &row_locations, &mut output, count);
            for row_idx in 0..count {
                let key = output.column(0).unwrap().get_i32(row_idx).unwrap();
                let hash = output.column(1).unwrap().get_u64(row_idx).unwrap();
                rows.push((key, hash));
            }
        }
        rows
    }

    #[test]
    fn test_repartition_correctness_and_partition_stats() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let types = vec![LogicalType::Integer, LogicalType::UBigInt];
        let layout = create_layout(types.clone());

        let mut old = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            2,
            1,
        )
        .unwrap();
        let mut new = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            4,
            1,
        )
        .unwrap();

        let row_count = 4096usize;
        let mut keys = Vec::with_capacity(row_count);
        let mut hashes = Vec::with_capacity(row_count);
        for i in 0..row_count {
            let high4 = (i % 16) as u64;
            let hash = (high4 << (u64::BITS as usize - 4)) | (i as u64);
            keys.push(i as i32);
            hashes.push(hash);
        }

        let mut append_state = PartitionedRawRowAppendState::new();
        old.initialize_append_state(&mut append_state, RawRowPinProperties::UnpinAfterDone);
        old.append(
            &mut append_state,
            &build_chunk_with_hashes(&keys[..2500], &hashes[..2500]),
        )
        .unwrap();
        old.append(
            &mut append_state,
            &build_chunk_with_hashes(&keys[2500..], &hashes[2500..]),
        )
        .unwrap();
        old.flush_append_state(&mut append_state);

        assert_eq!(old.count(), row_count);
        old.repartition(&mut new).unwrap();

        assert_eq!(old.count(), 0);
        assert_eq!(new.count(), row_count);
        assert_eq!(new.partition_count(), 16);

        let (sizes, counts) = new.get_sizes_and_counts();
        assert_eq!(counts.iter().sum::<usize>(), row_count);
        assert_eq!(sizes.iter().sum::<usize>(), new.size_in_bytes());

        let mut seen = 0usize;
        for (partition_idx, partition) in new.get_partitions().iter().enumerate() {
            for (_, hash) in collect_rows(partition, &types) {
                assert_eq!(RadixPartitioning::apply_mask(hash, 4), partition_idx);
                seen += 1;
            }
        }
        assert_eq!(seen, row_count);
    }

    #[test]
    fn test_repartition_large_skew_case() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let types = vec![LogicalType::Integer, LogicalType::UBigInt];
        let layout = create_layout(types.clone());

        let mut old = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            2,
            1,
        )
        .unwrap();
        let mut new = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            5,
            1,
        )
        .unwrap();

        let row_count = 6000usize;
        let skew_partition = 20usize;
        let mut keys = Vec::with_capacity(row_count);
        let mut hashes = Vec::with_capacity(row_count);
        for i in 0..row_count {
            let hash = ((skew_partition as u64) << (u64::BITS as usize - 5)) | (i as u64);
            keys.push(i as i32);
            hashes.push(hash);
        }

        let mut append_state = PartitionedRawRowAppendState::new();
        old.initialize_append_state(&mut append_state, RawRowPinProperties::UnpinAfterDone);
        old.append(
            &mut append_state,
            &build_chunk_with_hashes(&keys[..2048], &hashes[..2048]),
        )
        .unwrap();
        old.append(
            &mut append_state,
            &build_chunk_with_hashes(&keys[2048..4096], &hashes[2048..4096]),
        )
        .unwrap();
        old.append(
            &mut append_state,
            &build_chunk_with_hashes(&keys[4096..], &hashes[4096..]),
        )
        .unwrap();
        old.flush_append_state(&mut append_state);

        old.repartition(&mut new).unwrap();

        let (_, counts) = new.get_sizes_and_counts();
        assert_eq!(new.partition_count(), 32);
        assert_eq!(counts.iter().sum::<usize>(), row_count);
        for (idx, count) in counts.iter().enumerate() {
            if idx == skew_partition {
                assert_eq!(*count, row_count);
            } else {
                assert_eq!(*count, 0);
            }
        }

        let mut seen_keys = Vec::with_capacity(row_count);
        for partition in new.get_partitions() {
            for (key, hash) in collect_rows(partition, &types) {
                assert_eq!(RadixPartitioning::apply_mask(hash, 5), skew_partition);
                seen_keys.push(key);
            }
        }
        seen_keys.sort_unstable();
        assert_eq!(seen_keys.len(), row_count);
        assert_eq!(seen_keys[0], 0);
        assert_eq!(seen_keys[row_count - 1], (row_count - 1) as i32);
    }

    #[test]
    fn test_large_radix_partitioned_roundtrip_preserves_all_rows() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let types = vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::UBigInt,
        ];
        let layout = create_layout(types.clone());

        let mut data = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            8,
            3,
        )
        .unwrap();

        let row_count = 200_000usize;
        let k1 = (1..=row_count).map(|v| v as i32).collect::<Vec<_>>();
        let k2 = (1..=row_count)
            .map(|v| (v % 257) as i32)
            .collect::<Vec<_>>();
        let values = vec![1i32; row_count];
        let hashes = (1..=row_count)
            .map(|v| spread_hash(v as u64))
            .collect::<Vec<_>>();

        let mut append_state = PartitionedRawRowAppendState::new();
        data.initialize_append_state(&mut append_state, RawRowPinProperties::UnpinAfterDone);
        data.append(
            &mut append_state,
            &build_spill_aggregate_chunk(&k1, &k2, &values, &hashes),
        )
        .unwrap();
        data.flush_append_state(&mut append_state);

        assert_eq!(data.count(), row_count);

        let mut seen_rows = 0usize;
        let mut sum_k1 = 0i64;
        for partition in data.get_partitions() {
            let mut scan_state = RawRowScanState::new();
            partition.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
            for chunk_idx in 0..partition.chunk_count() {
                let count = partition
                    .fetch_chunk(&mut scan_state, chunk_idx, true)
                    .expect("fetch_chunk should succeed");
                let mut row_locations = Vec::with_capacity(count);
                unsafe {
                    let ptrs = scan_state.chunk_state.row_locations.flat_data::<u64>();
                    for idx in 0..count {
                        row_locations.push(*ptrs.add(idx) as *const u8);
                    }
                }

                let mut output = Chunk::initialize(&types, count.max(1));
                gather_chunk(partition, &row_locations, &mut output, count);
                for row_idx in 0..count {
                    seen_rows += 1;
                    sum_k1 += output.column(0).unwrap().get_i32(row_idx).unwrap() as i64;
                    assert_eq!(output.column(2).unwrap().get_i32(row_idx), Some(1));
                }
            }
        }

        assert_eq!(seen_rows, row_count);
        let expected_sum = (row_count as i64) * ((row_count as i64) + 1) / 2;
        assert_eq!(sum_k1, expected_sum);
    }

    #[test]
    fn test_large_multi_append_radix_partitioned_roundtrip_preserves_all_rows() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let types = vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::UBigInt,
        ];
        let layout = create_layout(types.clone());

        let mut data = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            8,
            3,
        )
        .unwrap();

        let row_count = 200_000usize;
        let mut append_state = PartitionedRawRowAppendState::new();
        data.initialize_append_state(&mut append_state, RawRowPinProperties::UnpinAfterDone);
        let mut start = 1usize;
        while start <= row_count {
            let end = (start + 2048).min(row_count + 1);
            let k1 = (start..end).map(|v| v as i32).collect::<Vec<_>>();
            let k2 = (start..end).map(|v| (v % 257) as i32).collect::<Vec<_>>();
            let values = vec![1i32; end - start];
            let hashes = (start..end)
                .map(|v| spread_hash(v as u64))
                .collect::<Vec<_>>();
            data.append(
                &mut append_state,
                &build_spill_aggregate_chunk(&k1, &k2, &values, &hashes),
            )
            .unwrap();
            start = end;
        }
        data.flush_append_state(&mut append_state);

        assert_eq!(data.count(), row_count);

        let mut seen_rows = 0usize;
        let mut sum_k1 = 0i64;
        for partition in data.get_partitions() {
            let mut scan_state = RawRowScanState::new();
            partition.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
            for chunk_idx in 0..partition.chunk_count() {
                let count = partition
                    .fetch_chunk(&mut scan_state, chunk_idx, true)
                    .expect("fetch_chunk should succeed");
                let mut row_locations = Vec::with_capacity(count);
                unsafe {
                    let ptrs = scan_state.chunk_state.row_locations.flat_data::<u64>();
                    for idx in 0..count {
                        row_locations.push(*ptrs.add(idx) as *const u8);
                    }
                }

                let mut output = Chunk::initialize(&types, count.max(1));
                gather_chunk(partition, &row_locations, &mut output, count);
                for row_idx in 0..count {
                    seen_rows += 1;
                    sum_k1 += output.column(0).unwrap().get_i32(row_idx).unwrap() as i64;
                    assert_eq!(output.column(2).unwrap().get_i32(row_idx), Some(1));
                }
            }
        }

        assert_eq!(seen_rows, row_count);
        let expected_sum = (row_count as i64) * ((row_count as i64) + 1) / 2;
        assert_eq!(sum_k1, expected_sum);
    }

    #[test]
    fn test_large_multi_append_radix_partitioned_low_memory_preserves_all_rows() {
        let temp_dir = tempfile::tempdir().unwrap();
        let buffer_pool = BufferPool::new_arc(32 * 1024 * 1024);
        buffer_pool
            .set_temporary_directory(temp_dir.path().to_string_lossy().to_string())
            .unwrap();
        let types = vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::UBigInt,
        ];
        let layout = create_layout(types.clone());

        let mut data = RadixPartitionedRawRow::new(
            Arc::clone(&buffer_pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            8,
            3,
        )
        .unwrap();

        let row_count = 200_000usize;
        let mut append_state = PartitionedRawRowAppendState::new();
        data.initialize_append_state(&mut append_state, RawRowPinProperties::UnpinAfterDone);
        let mut start = 1usize;
        while start <= row_count {
            let end = (start + 2048).min(row_count + 1);
            let k1 = (start..end).map(|v| v as i32).collect::<Vec<_>>();
            let k2 = (start..end).map(|v| (v % 257) as i32).collect::<Vec<_>>();
            let values = vec![1i32; end - start];
            let hashes = (start..end)
                .map(|v| spread_hash(v as u64))
                .collect::<Vec<_>>();
            data.append(
                &mut append_state,
                &build_spill_aggregate_chunk(&k1, &k2, &values, &hashes),
            )
            .unwrap();
            start = end;
        }
        data.flush_append_state(&mut append_state);
        let (_, partition_counts_before_eviction) = data.get_sizes_and_counts();
        assert_eq!(
            partition_counts_before_eviction.iter().sum::<usize>(),
            row_count
        );
        let chunk_rows_before_eviction = data
            .get_partitions()
            .iter()
            .flat_map(|partition| partition.segments().iter())
            .flat_map(|segment| segment.chunks().iter())
            .map(|chunk| chunk.count)
            .sum::<usize>();
        assert_eq!(chunk_rows_before_eviction, row_count);
        let pinned_blocks_before_eviction = data
            .get_partitions()
            .iter()
            .flat_map(|partition| partition.segments().iter())
            .flat_map(|segment| {
                let allocator = segment.allocator();
                (0..allocator.row_block_count())
                    .filter_map(|idx| allocator.get_row_block(idx))
                    .filter_map(|block| block.handle.as_ref())
                    .map(|handle| handle.pin_count())
                    .collect::<Vec<_>>()
            })
            .sum::<i32>();
        assert_eq!(pinned_blocks_before_eviction, 0);
        assert!(
            !buffer_pool.get_temporary_files().is_empty(),
            "expected low-memory radix append to spill temporary blocks"
        );

        let mut seen_rows = 0usize;
        let mut sum_k1 = 0i64;
        for partition in data.get_partitions() {
            let partition_chunk_rows = partition
                .segments()
                .iter()
                .flat_map(|segment| segment.chunks().iter())
                .map(|chunk| chunk.count)
                .sum::<usize>();
            assert_eq!(partition_chunk_rows, partition.count());
            let mut scan_state = RawRowScanState::new();
            partition.initialize_scan(&mut scan_state, RawRowPinProperties::KeepEverythingPinned);
            for chunk_idx in 0..partition.chunk_count() {
                let count = partition
                    .fetch_chunk(&mut scan_state, chunk_idx, true)
                    .expect("fetch_chunk should succeed");
                let mut row_locations = Vec::with_capacity(count);
                unsafe {
                    let ptrs = scan_state.chunk_state.row_locations.flat_data::<u64>();
                    for idx in 0..count {
                        row_locations.push(*ptrs.add(idx) as *const u8);
                    }
                }

                let mut output = Chunk::initialize(&types, count.max(1));
                gather_chunk(partition, &row_locations, &mut output, count);
                for row_idx in 0..count {
                    seen_rows += 1;
                    sum_k1 += output.column(0).unwrap().get_i32(row_idx).unwrap() as i64;
                    assert_eq!(output.column(2).unwrap().get_i32(row_idx), Some(1));
                }
            }
        }

        assert_eq!(seen_rows, row_count);
        let expected_sum = (row_count as i64) * ((row_count as i64) + 1) / 2;
        assert_eq!(sum_k1, expected_sum);
    }
}
