// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! RadixPartitionedColumnData - radix partitioned column substrate.

use std::sync::Arc;

use crate::buffer::{BufferPool, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

use super::{
    ColumnPartitionIndexComputer, PartitionedColumnData, PartitionedColumnDataAppendState,
};
use crate::column::ColumnDataCollection;

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

    fn adaptive_buffer_size(radix_bits: usize) -> usize {
        match radix_bits {
            1..=4 => 64,
            5 => 32,
            6 => 16,
            _ => 8,
        }
    }
}

impl ColumnPartitionIndexComputer for RadixPartitionComputer {
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

        for (i, out) in output.iter_mut().enumerate().take(append_count) {
            let row_idx = append_sel.get(i);
            let hash = hash_column.get_u64(row_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "hash value is NULL while computing radix partitions: row_idx={row_idx}"
                ))
            })?;
            *out = RadixPartitioning::apply_mask(hash, self.radix_bits) & self.partition_mask;
        }

        Ok(())
    }

    fn max_partition_index(&self) -> usize {
        self.partition_mask
    }

    fn buffer_size(&self) -> usize {
        Self::adaptive_buffer_size(self.radix_bits)
    }
}

/// Radix-specialized partitioned column substrate.
#[derive(Debug)]
pub struct RadixPartitionedColumnData {
    radix_bits: usize,
    hash_col_idx: usize,
    data: PartitionedColumnData,
}

impl RadixPartitionedColumnData {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        types: Vec<LogicalType>,
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
        if hash_col_idx >= types.len() {
            return Err(paro_error::invalid_input(format!(
                "hash column out of bounds: hash_col_idx={hash_col_idx}, column_count={}",
                types.len()
            )));
        }
        if types[hash_col_idx] != LogicalType::UBigInt {
            return Err(paro_error::invalid_input(format!(
                "radix hash column must be UBigInt, found {:?}",
                types[hash_col_idx]
            )));
        }

        let partitioner = Arc::new(RadixPartitionComputer::new(radix_bits, hash_col_idx));
        let data = PartitionedColumnData::new(buffer_pool, types, tag, partitioner);
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

    pub fn buffer_size(&self) -> usize {
        self.data.buffer_size()
    }

    pub fn get_partitions(&self) -> &[ColumnDataCollection] {
        self.data.get_partitions()
    }

    pub fn get_partitions_mut(&mut self) -> &mut [ColumnDataCollection] {
        self.data.get_partitions_mut()
    }

    pub fn initialize_append_state(&mut self, state: &mut PartitionedColumnDataAppendState) {
        self.data.initialize_append_state(state);
    }

    pub fn append(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        input: &Chunk,
    ) -> Result<()> {
        self.data.append(state, input)
    }

    pub fn append_with_sel(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
    ) -> Result<()> {
        self.data
            .append_with_sel(state, input, append_sel, append_count)
    }

    pub fn flush_append_state(
        &mut self,
        state: &mut PartitionedColumnDataAppendState,
    ) -> Result<()> {
        self.data.flush_append_state(state)
    }

    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        self.validate_compatible(other)?;
        self.data.combine(&mut other.data)
    }

    pub fn get_sizes_and_counts(&self) -> (Vec<usize>, Vec<usize>) {
        self.data.get_sizes_and_counts()
    }

    pub fn reset(&mut self) -> Result<()> {
        self.data.reset()
    }

    fn validate_compatible(&self, other: &Self) -> Result<()> {
        if self.hash_col_idx != other.hash_col_idx {
            return Err(paro_error::invalid_input(format!(
                "hash column mismatch: left={}, right={}",
                self.hash_col_idx, other.hash_col_idx
            )));
        }
        if self.radix_bits != other.radix_bits {
            return Err(paro_error::invalid_input(format!(
                "radix bits mismatch: left={}, right={}",
                self.radix_bits, other.radix_bits
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use crate::buffer::{BufferPool, MemoryTag};
    use crate::column::ColumnDataScanState;

    use super::{PartitionedColumnDataAppendState, RadixPartitionedColumnData, RadixPartitioning};

    fn build_chunk_with_hashes(keys: &[i32], hashes: &[u64]) -> Chunk {
        let mut hash_vector = test_vector_with_capacity(LogicalType::UBigInt, hashes.len().max(1));
        hash_vector.set_count(hashes.len());
        for (idx, hash) in hashes.iter().enumerate() {
            hash_vector.set_u64(idx, *hash);
        }
        test_chunk_from_vectors(vec![test_i32_vector(keys), hash_vector])
    }

    fn collect_rows(collection: &crate::column::ColumnDataCollection) -> Vec<(i32, u64)> {
        let mut rows = Vec::new();
        let mut scan_state = ColumnDataScanState::new();
        collection.initialize_scan(&mut scan_state, None);
        let mut out = test_chunk_with_capacity(&[LogicalType::Integer, LogicalType::UBigInt], 1);
        while collection
            .scan(&mut scan_state, &mut out)
            .expect("scan should succeed")
        {
            for row_idx in 0..out.size() {
                let key = out.column(0).unwrap().get_i32(row_idx).unwrap();
                let hash = out.column(1).unwrap().get_u64(row_idx).unwrap();
                rows.push((key, hash));
            }
        }
        rows
    }

    #[test]
    fn test_radix_buffer_size_policy() {
        let pool = BufferPool::new_arc(32 * 1024 * 1024);
        let types = vec![LogicalType::Integer, LogicalType::UBigInt];
        let cases = vec![
            (1usize, 64usize),
            (4, 64),
            (5, 32),
            (6, 16),
            (7, 8),
            (10, 8),
        ];

        for (radix_bits, expected_buffer_size) in cases {
            let data = RadixPartitionedColumnData::new(
                Arc::clone(&pool),
                types.clone(),
                MemoryTag::ColumnData,
                radix_bits,
                1,
            )
            .unwrap();
            assert_eq!(data.buffer_size(), expected_buffer_size);
        }
    }

    #[test]
    fn test_radix_partition_correctness_and_combine() {
        let buffer_pool = BufferPool::new_arc(128 * 1024 * 1024);
        let types = vec![LogicalType::Integer, LogicalType::UBigInt];

        let mut global = RadixPartitionedColumnData::new(
            Arc::clone(&buffer_pool),
            types.clone(),
            MemoryTag::ColumnData,
            4,
            1,
        )
        .unwrap();
        let mut local1 = global.create_shared();
        let mut local2 = global.create_shared();

        let row_count = 4096usize;
        let mut keys = Vec::with_capacity(row_count);
        let mut hashes = Vec::with_capacity(row_count);
        for i in 0..row_count {
            let high4 = (i % 16) as u64;
            let hash = (high4 << (u64::BITS as usize - 4)) | (i as u64);
            keys.push(i as i32);
            hashes.push(hash);
        }

        let mut state1 = PartitionedColumnDataAppendState::new();
        let mut state2 = PartitionedColumnDataAppendState::new();
        local1.initialize_append_state(&mut state1);
        local2.initialize_append_state(&mut state2);

        local1
            .append(
                &mut state1,
                &build_chunk_with_hashes(&keys[..2048], &hashes[..2048]),
            )
            .unwrap();
        local2
            .append(
                &mut state2,
                &build_chunk_with_hashes(&keys[2048..], &hashes[2048..]),
            )
            .unwrap();
        local1.flush_append_state(&mut state1).unwrap();
        local2.flush_append_state(&mut state2).unwrap();

        global.combine(&mut local1).unwrap();
        global.combine(&mut local2).unwrap();

        assert_eq!(global.count(), row_count);
        assert_eq!(global.partition_count(), 16);
        assert_eq!(global.buffer_size(), 64);

        let (sizes, counts) = global.get_sizes_and_counts();
        assert_eq!(counts.iter().sum::<usize>(), row_count);
        assert_eq!(sizes.iter().sum::<usize>(), global.size_in_bytes());

        let mut seen_keys = Vec::with_capacity(row_count);
        for (partition_idx, partition) in global.get_partitions().iter().enumerate() {
            for (key, hash) in collect_rows(partition) {
                assert_eq!(RadixPartitioning::apply_mask(hash, 4), partition_idx);
                seen_keys.push(key);
            }
        }

        seen_keys.sort_unstable();
        assert_eq!(seen_keys, (0..row_count as i32).collect::<Vec<_>>());
    }
}
