// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingContext;
use paro_common::vector::SelectionVector;

use crate::buffer::{BufferPool, MemoryTag};
use crate::row::partition::partitioned::PartitionedRows;
use crate::row::{RowLayout, RowStoreBuilder};

/// Compute the destination partition for each appended row.
pub trait PartitionIndexComputer: Send + Sync + std::fmt::Debug {
    fn compute_partition_indices(
        &self,
        input: &Chunk,
        append_sel: &SelectionVector,
        append_count: usize,
        output: &mut [usize],
    ) -> Result<()>;

    fn max_partition_index(&self) -> usize;
}

/// Builder for partitioned row stores.
#[derive(Debug)]
pub struct PartitionedRowsBuilder {
    buffer_pool: Arc<BufferPool>,
    layout: Arc<RowLayout>,
    tag: MemoryTag,
    memory: MemoryAccountingContext,
    partitioner: Arc<dyn PartitionIndexComputer>,
    partitions: Vec<RowStoreBuilder>,
    count: u64,
}

impl PartitionedRowsBuilder {
    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RowLayout>,
        tag: MemoryTag,
        partitioner: Arc<dyn PartitionIndexComputer>,
        memory: MemoryAccountingContext,
    ) -> Self {
        let partition_count = partitioner.max_partition_index().saturating_add(1).max(1);
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(RowStoreBuilder::new_with_memory(
                Arc::clone(&buffer_pool),
                Arc::clone(&layout),
                tag,
                memory.clone(),
            ));
        }

        Self {
            buffer_pool,
            layout,
            tag,
            memory,
            partitioner,
            partitions,
            count: 0,
        }
    }

    #[cfg(test)]
    pub fn from_types(
        buffer_pool: Arc<BufferPool>,
        types: Vec<paro_common::types::LogicalType>,
        tag: MemoryTag,
        partitioner: Arc<dyn PartitionIndexComputer>,
    ) -> Self {
        Self::new_with_memory(
            buffer_pool,
            Arc::new(RowLayout::from_types(
                types,
                crate::row::RowValidityType::CanHaveNullValues,
            )),
            tag,
            partitioner,
            MemoryAccountingContext::detached(
                tag,
                paro_common::memory::MemoryAccountingClass::default_for_tag(tag),
            ),
        )
    }

    #[inline]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(RowStoreBuilder::size_in_bytes)
            .sum()
    }

    pub fn get_sizes_and_counts(&self) -> (Vec<usize>, Vec<u64>) {
        let sizes = self
            .partitions
            .iter()
            .map(RowStoreBuilder::size_in_bytes)
            .collect();
        let counts = self.partitions.iter().map(RowStoreBuilder::count).collect();
        (sizes, counts)
    }

    pub fn append(&mut self, input: &Chunk) -> Result<()> {
        let sel = SelectionVector::try_incremental(input.size(), input.allocator().clone())?;
        self.append_with_sel(input, &sel, input.size())
    }

    pub fn append_with_sel(
        &mut self,
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

        let mut partition_indices = vec![0usize; append_count];
        self.partitioner.compute_partition_indices(
            input,
            append_sel,
            append_count,
            &mut partition_indices,
        )?;

        let mut per_partition: Vec<Vec<u32>> = vec![Vec::new(); self.partition_count()];
        for (sel_idx, partition_idx) in partition_indices.into_iter().enumerate() {
            if partition_idx >= self.partition_count() {
                return Err(paro_error::internal(format!(
                    "partition index out of bounds: index={partition_idx}, partition_count={}",
                    self.partition_count()
                )));
            }
            per_partition[partition_idx].push(append_sel.get(sel_idx) as u32);
        }

        for (partition_idx, selected_rows) in per_partition.into_iter().enumerate() {
            if selected_rows.is_empty() {
                continue;
            }

            let mut sliced = input.clone();
            let selection =
                SelectionVector::try_from_indices(selected_rows, input.allocator().clone())?;
            let partition_count = selection.len();
            sliced.try_slice(&selection, partition_count)?;
            self.partitions[partition_idx].append(&sliced)?;
            self.count += partition_count as u64;
        }

        Ok(())
    }

    pub fn try_absorb(&mut self, mut other: PartitionedRowsBuilder) -> Result<()> {
        self.ensure_compatible(&other)?;
        for (target, source) in self.partitions.iter_mut().zip(other.partitions.drain(..)) {
            target.absorb(source);
        }
        self.count += other.count;
        Ok(())
    }

    pub fn seal(self) -> PartitionedRows {
        let partitions = self
            .partitions
            .into_iter()
            .map(RowStoreBuilder::seal)
            .collect();
        PartitionedRows::new(
            self.buffer_pool,
            self.layout,
            self.tag,
            self.memory,
            partitions,
            self.count,
        )
    }

    fn ensure_compatible(&self, other: &PartitionedRowsBuilder) -> Result<()> {
        if self.partition_count() != other.partition_count() {
            return Err(paro_error::invalid_input(format!(
                "partition count mismatch: left={}, right={}",
                self.partition_count(),
                other.partition_count()
            )));
        }
        if self.layout.types() != other.layout.types()
            || self.layout.validity() != other.layout.validity()
        {
            return Err(paro_error::invalid_input(
                "cannot absorb partitioned row builders with mismatched layouts",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::*;
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use crate::buffer::{BufferPool, MemoryTag};

    use super::{PartitionIndexComputer, PartitionedRowsBuilder};

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

    fn build_chunk(values: &[(i32, &str)]) -> Chunk {
        let mut ids = test_vector_with_capacity(LogicalType::Integer, values.len());
        let mut names = test_vector_with_capacity(LogicalType::Varchar, values.len());
        for (idx, (id, name)) in values.iter().enumerate() {
            ids.set_i32(idx, *id);
            names.set_string(idx, name);
        }
        ids.set_count(values.len());
        names.set_count(values.len());
        test_chunk_from_vectors(vec![ids, names])
    }

    fn collect_partition_ids(store: &crate::row::RowStore) -> Vec<i32> {
        if store.is_empty() {
            return Vec::new();
        }

        let pinned = store.pin_ordinal_range(0, store.count() as u32).unwrap();
        let mut out = test_chunk_with_capacity(&[LogicalType::Integer], store.count() as usize);
        pinned.gather_columns(&[0], &mut out, 0).unwrap();
        (0..store.count() as usize)
            .map(|row| out.get_value(0, row).unwrap())
            .map(|value| match value {
                Value::Integer(v) => v,
                other => panic!("unexpected value {:?}", other),
            })
            .collect()
    }

    #[test]
    fn multi_partition_append_and_seal_keeps_partition_order() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let partitioner: Arc<dyn PartitionIndexComputer> =
            Arc::new(ModuloPartitionComputer::new(4, 0));
        let mut builder = PartitionedRowsBuilder::from_types(
            pool,
            vec![LogicalType::Integer, LogicalType::Varchar],
            MemoryTag::HashTable,
            partitioner,
        );

        builder
            .append(&build_chunk(&[
                (0, "zero"),
                (1, "one"),
                (2, "two"),
                (3, "three"),
                (4, "four"),
                (5, "five"),
                (6, "six"),
                (7, "seven"),
            ]))
            .unwrap();

        let rows = builder.seal();
        assert_eq!(rows.partition_count(), 4);
        assert_eq!(rows.count(), 8);

        let partition_rows: Vec<Vec<i32>> = rows
            .partitions()
            .iter()
            .map(collect_partition_ids)
            .collect();
        assert_eq!(partition_rows[0], vec![0, 4]);
        assert_eq!(partition_rows[1], vec![1, 5]);
        assert_eq!(partition_rows[2], vec![2, 6]);
        assert_eq!(partition_rows[3], vec![3, 7]);
    }

    #[test]
    fn try_absorb_consumes_other_builder() {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let partitioner: Arc<dyn PartitionIndexComputer> =
            Arc::new(ModuloPartitionComputer::new(2, 0));

        let mut left = PartitionedRowsBuilder::from_types(
            Arc::clone(&pool),
            vec![LogicalType::Integer, LogicalType::Varchar],
            MemoryTag::HashTable,
            Arc::clone(&partitioner),
        );
        let mut right = PartitionedRowsBuilder::from_types(
            pool,
            vec![LogicalType::Integer, LogicalType::Varchar],
            MemoryTag::HashTable,
            partitioner,
        );

        left.append(&build_chunk(&[(0, "a")])).unwrap();
        right.append(&build_chunk(&[(1, "b")])).unwrap();

        left.try_absorb(right).unwrap();
        let sealed = left.seal();
        assert_eq!(collect_partition_ids(sealed.partition(0)), vec![0]);
        assert_eq!(collect_partition_ids(sealed.partition(1)), vec![1]);
    }
}
