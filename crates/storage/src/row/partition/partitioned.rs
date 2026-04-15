use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::vector::VECTOR_SIZE;

use crate::buffer::{BufferPool, MemoryTag};
use crate::row::partition::{PartitionIndexComputer, PartitionedRowsBuilder};
use crate::row::{RowLayout, RowStore, RowStoreBuilder};

/// Sealed partitioned row stores.
#[derive(Debug)]
pub struct PartitionedRows {
    buffer_pool: Arc<BufferPool>,
    layout: Arc<RowLayout>,
    tag: MemoryTag,
    partitions: Vec<RowStore>,
    count: u64,
}

impl PartitionedRows {
    pub(crate) fn new(
        buffer_pool: Arc<BufferPool>,
        layout: Arc<RowLayout>,
        tag: MemoryTag,
        partitions: Vec<RowStore>,
        count: u64,
    ) -> Self {
        Self {
            buffer_pool,
            layout,
            tag,
            partitions,
            count,
        }
    }

    #[inline]
    pub fn layout(&self) -> &RowLayout {
        &self.layout
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
        self.partitions.iter().map(RowStore::size_in_bytes).sum()
    }

    #[inline]
    pub fn partitions(&self) -> &[RowStore] {
        &self.partitions
    }

    #[inline]
    pub fn partition(&self, index: usize) -> &RowStore {
        &self.partitions[index]
    }

    #[inline]
    pub(crate) fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    #[inline]
    pub(crate) fn tag(&self) -> MemoryTag {
        self.tag
    }

    #[inline]
    pub(crate) fn into_partitions(self) -> Vec<RowStore> {
        self.partitions
    }

    pub fn take_partition(&mut self, index: usize) -> RowStore {
        let empty = RowStoreBuilder::new(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.layout),
            self.tag,
        )
        .seal();
        let taken = std::mem::replace(&mut self.partitions[index], empty);
        self.count = self.count.saturating_sub(taken.count());
        taken
    }

    pub fn repartition(&self, partitioner: Arc<dyn PartitionIndexComputer>) -> Result<Self> {
        let mut builder = PartitionedRowsBuilder::new(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.layout),
            self.tag,
            partitioner,
        );
        let mut chunk = Chunk::initialize(self.layout.types(), VECTOR_SIZE);

        for partition in &self.partitions {
            let mut scanner = partition.scanner();
            loop {
                let count = scanner.next_chunk(&mut chunk)?;
                if count == 0 {
                    break;
                }
                builder.append(&chunk)?;
            }
        }

        Ok(builder.seal())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::chunk::Chunk;
    use paro_common::error::Result;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;

    use crate::buffer::{BufferPool, MemoryTag};

    use super::super::builder::{PartitionIndexComputer, PartitionedRowsBuilder};
    use super::PartitionedRows;

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

    fn build_chunk(values: &[i32]) -> Chunk {
        let mut ids = Vector::with_capacity(LogicalType::Integer, values.len());
        for (idx, value) in values.iter().enumerate() {
            ids.set_i32(idx, *value);
        }
        ids.set_count(values.len());
        Chunk::from_vectors(vec![ids])
    }

    fn collect_partition_ids(store: &crate::row::RowStore) -> Vec<i32> {
        if store.is_empty() {
            return Vec::new();
        }
        let pinned = store.pin_ordinal_range(0, store.count() as u32).unwrap();
        let mut out = Chunk::initialize(&[LogicalType::Integer], store.count() as usize);
        pinned.gather_columns(&[0], &mut out, 0).unwrap();
        (0..store.count() as usize)
            .map(|row| out.get_value(0, row).unwrap())
            .map(|value| match value {
                Value::Integer(v) => v,
                other => panic!("unexpected value {:?}", other),
            })
            .collect()
    }

    fn build_rows(values: &[i32], partition_count: usize) -> PartitionedRows {
        let pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let partitioner: Arc<dyn PartitionIndexComputer> =
            Arc::new(ModuloPartitionComputer::new(partition_count, 0));
        let mut builder = PartitionedRowsBuilder::from_types(
            pool,
            vec![LogicalType::Integer],
            MemoryTag::HashTable,
            partitioner,
        );
        builder.append(&build_chunk(values)).unwrap();
        builder.seal()
    }

    #[test]
    fn repartition_returns_new_object_and_keeps_old_addrs_valid() {
        let rows = build_rows(&[10, 11, 12, 13], 2);
        let addr = rows.partition(0).addr_at_ordinal(0).unwrap();

        let repartitioned = rows
            .repartition(Arc::new(ModuloPartitionComputer::new(4, 0)))
            .unwrap();

        let pinned = rows.partition(0).pin_rows(&[addr]).unwrap();
        let mut out = Chunk::initialize(&[LogicalType::Integer], 1);
        pinned.gather_columns(&[0], &mut out, 0).unwrap();
        assert_eq!(out.get_value(0, 0), Some(Value::Integer(10)));

        assert_eq!(repartitioned.partition_count(), 4);
        let mut seen = Vec::new();
        for partition in repartitioned.partitions() {
            seen.extend(collect_partition_ids(partition));
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![10, 11, 12, 13]);
    }
}
