// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::{BufferAllocator, BufferManager};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{GrantAllocator, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_common::vector::SelectionVector;

use crate::buffer::{BufferPool, MemoryTag};
use crate::row::partition::{PartitionIndexComputer, PartitionedRows, PartitionedRowsBuilder};
use crate::row::{RowLayout, RowStore};

const MAX_RADIX_BITS: usize = 12;

#[derive(Debug, Clone, Copy)]
pub struct RadixPartitioning;

impl RadixPartitioning {
    pub const MAX_RADIX_BITS: usize = MAX_RADIX_BITS;

    #[inline]
    pub const fn number_of_partitions(radix_bits: usize) -> usize {
        1usize << radix_bits
    }

    #[inline]
    pub const fn shift(radix_bits: usize) -> usize {
        (u64::BITS as usize) - radix_bits
    }

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
    fn try_new(layout: &RowLayout, radix_bits: usize, hash_col_idx: usize) -> Result<Self> {
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
        if layout.types()[hash_col_idx] != LogicalType::UBigInt {
            return Err(paro_error::invalid_input(format!(
                "radix hash column must be UBigInt, found {:?}",
                layout.types()[hash_col_idx]
            )));
        }

        Ok(Self {
            radix_bits,
            hash_col_idx,
            partition_mask: RadixPartitioning::number_of_partitions(radix_bits).saturating_sub(1),
        })
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
pub struct RadixPartitionedRowsBuilder {
    radix_bits: usize,
    hash_col_idx: usize,
    inner: PartitionedRowsBuilder,
}

impl RadixPartitionedRowsBuilder {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RowLayout>,
        tag: MemoryTag,
        radix_bits: usize,
        hash_col_idx: usize,
    ) -> Result<Self> {
        let memory = MemoryAccountingContext::detached(
            tag,
            paro_common::memory::MemoryAccountingClass::default_for_tag(tag),
        );
        Self::new_with_memory(buffer_pool, layout, tag, radix_bits, hash_col_idx, memory)
    }

    pub fn new_with_grant_allocator(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RowLayout>,
        tag: MemoryTag,
        radix_bits: usize,
        hash_col_idx: usize,
        grant_allocator: GrantAllocator<'_>,
    ) -> Result<Self> {
        Self::new_with_memory(
            buffer_pool,
            layout,
            tag,
            radix_bits,
            hash_col_idx,
            MemoryAccountingContext::from_grant_allocator(&grant_allocator),
        )
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RowLayout>,
        tag: MemoryTag,
        radix_bits: usize,
        hash_col_idx: usize,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        let partitioner = Arc::new(RadixPartitionComputer::try_new(
            layout.as_ref(),
            radix_bits,
            hash_col_idx,
        )?);
        Ok(Self {
            radix_bits,
            hash_col_idx,
            inner: PartitionedRowsBuilder::new_with_memory(
                buffer_pool,
                layout,
                tag,
                partitioner,
                memory,
            ),
        })
    }

    pub fn append(&mut self, input: &Chunk) -> Result<()> {
        self.inner.append(input)
    }

    pub fn append_with_sel(
        &mut self,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
    ) -> Result<()> {
        self.inner.append_with_sel(input, append_sel, append_count)
    }

    pub fn absorb(&mut self, other: RadixPartitionedRowsBuilder) {
        self.try_absorb(other)
            .expect("cannot absorb radix partitioned row builders");
    }

    pub fn combine(mut self, other: RadixPartitionedRowsBuilder) -> Result<Self> {
        self.try_absorb(other)?;
        Ok(self)
    }

    pub fn try_absorb(&mut self, other: RadixPartitionedRowsBuilder) -> Result<()> {
        self.ensure_compatible(&other)?;
        self.inner.try_absorb(other.inner)
    }

    #[inline]
    pub fn partition_count(&self) -> usize {
        self.inner.partition_count()
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.inner.count()
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }

    pub fn get_sizes_and_counts(&self) -> (Vec<usize>, Vec<u64>) {
        self.inner.get_sizes_and_counts()
    }

    pub fn seal(self) -> RadixPartitionedRows {
        RadixPartitionedRows {
            radix_bits: self.radix_bits,
            hash_col_idx: self.hash_col_idx,
            inner: self.inner.seal(),
        }
    }

    fn ensure_compatible(&self, other: &RadixPartitionedRowsBuilder) -> Result<()> {
        if self.radix_bits != other.radix_bits || self.hash_col_idx != other.hash_col_idx {
            return Err(paro_error::invalid_input(format!(
                "radix builder mismatch: left=(bits={}, hash_col_idx={}), right=(bits={}, hash_col_idx={})",
                self.radix_bits,
                self.hash_col_idx,
                other.radix_bits,
                other.hash_col_idx
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct RadixPartitionedRows {
    radix_bits: usize,
    hash_col_idx: usize,
    inner: PartitionedRows,
}

impl RadixPartitionedRows {
    #[inline]
    pub fn radix_bits(&self) -> usize {
        self.radix_bits
    }

    #[inline]
    pub fn hash_col_idx(&self) -> usize {
        self.hash_col_idx
    }

    #[inline]
    pub fn partition_count(&self) -> usize {
        self.inner.partition_count()
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.inner.count()
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.inner.size_in_bytes()
    }

    #[inline]
    pub fn partitions(&self) -> &[RowStore] {
        self.inner.partitions()
    }

    #[inline]
    pub fn partition(&self, index: usize) -> &RowStore {
        self.inner.partition(index)
    }

    #[inline]
    pub fn take_partition(&mut self, index: usize) -> RowStore {
        self.inner.take_partition(index)
    }

    pub fn repartition(&self, new_radix_bits: usize) -> Result<Self> {
        let partitioner = Arc::new(RadixPartitionComputer::try_new(
            self.inner.layout(),
            new_radix_bits,
            self.hash_col_idx,
        )?);
        Ok(Self {
            radix_bits: new_radix_bits,
            hash_col_idx: self.hash_col_idx,
            inner: self.inner.repartition(partitioner)?,
        })
    }

    pub fn into_repartitioned(self, new_radix_bits: usize) -> Result<Self> {
        let buffer_pool = Arc::clone(self.inner.buffer_pool());
        let layout = Arc::new(self.inner.layout().clone());
        let tag = self.inner.tag();
        let partitioner = Arc::new(RadixPartitionComputer::try_new(
            layout.as_ref(),
            new_radix_bits,
            self.hash_col_idx,
        )?);
        let allocator = Arc::new(BufferAllocator::new(
            Arc::clone(&buffer_pool) as Arc<dyn BufferManager>,
            tag,
        ));
        let memory = self.inner.memory().clone();
        let mut builder =
            PartitionedRowsBuilder::new_with_memory(buffer_pool, layout, tag, partitioner, memory);
        let mut chunk = Chunk::try_new(allocator)?;

        for partition in self.inner.into_partitions() {
            let mut scanner = partition.scanner();
            loop {
                let count = scanner.next_chunk(&mut chunk)?;
                if count == 0 {
                    break;
                }
                builder.append(&chunk)?;
            }
        }

        Ok(Self {
            radix_bits: new_radix_bits,
            hash_col_idx: self.hash_col_idx,
            inner: builder.seal(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use super::*;

    fn radix_input() -> Chunk {
        let mut hashes = test_vector_with_capacity(LogicalType::UBigInt, 4);
        hashes.set_u64(0, 0);
        hashes.set_u64(1, 1 << 63);
        hashes.set_u64(2, 0);
        hashes.set_u64(3, 1 << 63);
        hashes.set_count(4);

        let mut payload = test_vector_with_capacity(LogicalType::Integer, 4);
        payload.set_i32(0, 10);
        payload.set_i32(1, 20);
        payload.set_i32(2, 30);
        payload.set_i32(3, 40);
        payload.set_count(4);

        test_chunk_from_vectors(vec![hashes, payload])
    }

    #[test]
    fn radix_builder_appends_and_seals_partitions() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let layout = Arc::new(RowLayout::from_types(
            vec![LogicalType::UBigInt, LogicalType::Integer],
            crate::row::RowValidityType::CanHaveNullValues,
        ));
        let mut builder =
            RadixPartitionedRowsBuilder::new(pool, layout, MemoryTag::HashTable, 1, 0).unwrap();
        builder.append(&radix_input()).unwrap();
        let sealed = builder.seal();

        assert_eq!(sealed.partition_count(), 2);
        assert_eq!(sealed.count(), 4);
        assert_eq!(sealed.partition(0).count(), 2);
        assert_eq!(sealed.partition(1).count(), 2);
    }

    #[test]
    fn radix_builder_absorb_consumes_other_builder() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let layout = Arc::new(RowLayout::from_types(
            vec![LogicalType::UBigInt, LogicalType::Integer],
            crate::row::RowValidityType::CanHaveNullValues,
        ));
        let mut left = RadixPartitionedRowsBuilder::new(
            Arc::clone(&pool),
            Arc::clone(&layout),
            MemoryTag::HashTable,
            1,
            0,
        )
        .unwrap();
        let mut right =
            RadixPartitionedRowsBuilder::new(pool, layout, MemoryTag::HashTable, 1, 0).unwrap();

        let input = radix_input();
        left.append(&input).unwrap();
        right.append(&input).unwrap();
        left.absorb(right);

        let sealed = left.seal();
        assert_eq!(sealed.count(), 8);
        assert_eq!(sealed.partition(0).count(), 4);
        assert_eq!(sealed.partition(1).count(), 4);
    }

    #[test]
    fn radix_repartition_keeps_old_partition_addresses_valid() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let layout = Arc::new(RowLayout::from_types(
            vec![LogicalType::UBigInt, LogicalType::Integer],
            crate::row::RowValidityType::CanHaveNullValues,
        ));
        let mut builder =
            RadixPartitionedRowsBuilder::new(pool, layout, MemoryTag::HashTable, 1, 0).unwrap();
        builder.append(&radix_input()).unwrap();
        let sealed = builder.seal();

        let first_partition = sealed.partition(0);
        let addr = first_partition.addr_at_ordinal(0).unwrap();

        let repartitioned = sealed.repartition(2).unwrap();
        assert_eq!(repartitioned.partition_count(), 4);

        let pinned = first_partition.pin_rows(&[addr]).unwrap();
        let mut output = test_chunk_with_capacity(first_partition.layout().types(), 1);
        pinned.gather_columns(&[0, 1], &mut output, 0).unwrap();

        assert_eq!(output.get_value(0, 0), Some(Value::UBigInt(0)));
        assert!(matches!(
            output.get_value(1, 0),
            Some(Value::Integer(10)) | Some(Value::Integer(30))
        ));
    }
}
