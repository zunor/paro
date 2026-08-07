// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash-table state for unordered DISTINCT aggregate inputs.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};

use super::group_hash::GroupHashScratch;
use super::grouped_aggregate_hashtable::GroupedAggregateHashTable;
use super::grouped_aggregate_hashtable::HashTableCapacityHint;
use super::radix_partitioned_aggregate_hashtable::{AggregateHTScanPosition, AggregateHashTable};

/// Unique keys for one DISTINCT aggregate.
///
/// The table has no aggregate payload: its group tuple is the complete
/// `(group keys..., aggregate inputs...)` DISTINCT key. This keeps collection,
/// local/global combination, and final scanning vectorized through the same
/// tuple hash-table implementation used by ordinary grouped aggregation.
#[derive(Debug)]
pub(crate) struct DistinctKeyTable {
    key_types: Box<[LogicalType]>,
    group_key_count: usize,
    table: AggregateHashTable,
    hash_scratch: GroupHashScratch,
    addresses: Vector,
    new_groups: SelectionVector,
}

impl DistinctKeyTable {
    pub(crate) fn try_new(
        key_types: Vec<LogicalType>,
        group_key_count: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        parallelism: usize,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<Self> {
        if group_key_count > key_types.len() {
            return Err(paro_error::internal(format!(
                "distinct group-key prefix exceeds key width: groups={group_key_count}, keys={}",
                key_types.len()
            )));
        }
        let table = if parallelism <= 1 {
            AggregateHashTable::new_flat_with_memory_capacity_hint(
                key_types.clone(),
                Vec::new(),
                Vec::new(),
                allocator.clone(),
                memory,
                capacity_hint,
            )?
        } else {
            let partition_bits =
                parallelism.next_power_of_two().trailing_zeros().clamp(1, 4) as usize;
            AggregateHashTable::new_radix_with_memory_capacity_hint(
                key_types.clone(),
                Vec::new(),
                Vec::new(),
                partition_bits,
                allocator.clone(),
                memory,
                capacity_hint,
            )?
        };
        Self::from_table(key_types.into_boxed_slice(), group_key_count, table)
    }

    fn from_table(
        key_types: Box<[LogicalType]>,
        group_key_count: usize,
        table: AggregateHashTable,
    ) -> Result<Self> {
        let allocator = table.allocator();
        Ok(Self {
            key_types,
            group_key_count,
            table,
            hash_scratch: GroupHashScratch::try_new(VECTOR_SIZE, allocator.clone())?,
            addresses: Vector::try_new(LogicalType::BigInt, VECTOR_SIZE, allocator.clone())?,
            new_groups: SelectionVector::try_with_capacity(VECTOR_SIZE, allocator)?,
        })
    }

    pub(crate) fn key_types(&self) -> &[LogicalType] {
        &self.key_types
    }

    pub(crate) fn count(&self) -> usize {
        self.table.count()
    }

    pub(crate) fn allocator(&self) -> Arc<dyn Allocator> {
        self.table.allocator()
    }

    pub(crate) fn insert(&mut self, keys: &Chunk) -> Result<()> {
        self.validate_keys(keys)?;
        if keys.is_empty() {
            return Ok(());
        }
        let allocator = self.table.allocator();
        if self.addresses.capacity() < keys.size() {
            self.addresses = Vector::try_new(LogicalType::BigInt, keys.size(), allocator.clone())?;
        }
        if self.new_groups.capacity() < keys.size() {
            self.new_groups = SelectionVector::try_with_capacity(keys.size(), allocator)?;
        }
        self.new_groups.set_len(0);
        let (lookup_hashes, partition_hashes) = self
            .hash_scratch
            .hash_with_partition_prefix(keys, self.group_key_count)?;
        self.table.find_or_create_groups_partitioned(
            keys,
            lookup_hashes,
            partition_hashes,
            &mut self.addresses,
            &mut self.new_groups,
        )?;
        Ok(())
    }

    pub(crate) fn combine(&mut self, other: &mut Self) -> Result<()> {
        if self.key_types != other.key_types {
            return Err(paro_error::internal(format!(
                "cannot combine distinct key tables with different schemas: target={:?}, source={:?}",
                self.key_types, other.key_types
            )));
        }
        if self.group_key_count != other.group_key_count {
            return Err(paro_error::internal(format!(
                "cannot combine distinct key tables with different group prefixes: target={}, source={}",
                self.group_key_count, other.group_key_count
            )));
        }
        self.table.combine(&mut other.table)
    }

    pub(crate) fn scan(
        &mut self,
        position: &mut AggregateHTScanPosition,
        output: &mut Chunk,
    ) -> Result<bool> {
        self.table.scan(position, output)
    }

    pub(crate) fn visit_flat_partitions(
        &self,
        visit: impl FnMut(&GroupedAggregateHashTable) -> Result<()>,
    ) -> Result<()> {
        self.table.visit_flat_partitions(visit)
    }

    /// Borrow the single flat partition owned by this table.
    ///
    /// Parallel finalization calls this after [`Self::into_partitions`], when
    /// retaining fragment ownership is preferable to coalescing their rows.
    pub(crate) fn flat_partition(&self) -> Result<&GroupedAggregateHashTable> {
        match &self.table {
            AggregateHashTable::Flat(table) => Ok(table),
            AggregateHashTable::Radix(_) => Err(paro_error::internal(
                "DISTINCT fragment must be split before flat partition access",
            )),
        }
    }

    /// Split a radix table into independently owned flat partitions.
    ///
    /// All fragments use the same high hash bits, so partitions with the same
    /// ordinal can be combined independently without re-partitioning.
    pub(crate) fn into_partitions(self) -> Result<Vec<Self>> {
        let key_types = self.key_types;
        let group_key_count = self.group_key_count;
        self.table
            .into_scan_partitions()
            .into_iter()
            .map(|table| Self::from_table(key_types.clone(), group_key_count, table))
            .collect()
    }

    fn validate_keys(&self, keys: &Chunk) -> Result<()> {
        if keys.column_count() != self.key_types.len() {
            return Err(paro_error::internal(format!(
                "distinct key column count mismatch: expected={}, actual={}",
                self.key_types.len(),
                keys.column_count()
            )));
        }
        for (column_idx, expected) in self.key_types.iter().enumerate() {
            let actual = keys
                .column(column_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "distinct key column not found: column_idx={column_idx}"
                    ))
                })?
                .logical_type();
            if actual != expected {
                return Err(paro_error::internal(format!(
                    "distinct key type mismatch at column {column_idx}: expected={expected:?}, actual={actual:?}"
                )));
            }
        }
        Ok(())
    }
}

/// Per-aggregate DISTINCT key fragments shared by grouped and ungrouped sinks.
///
/// Each worker owns one local table. Sink merge transfers those tables into
/// the global state without serially rehashing them; finalization combines
/// matching radix partitions and consumes the globally unique keys.
#[derive(Debug, Default)]
pub(crate) struct DistinctAggregateState {
    fragments: Box<[Vec<DistinctKeyTable>]>,
}

impl DistinctAggregateState {
    pub(crate) fn new(aggregate_count: usize) -> Self {
        Self {
            fragments: (0..aggregate_count).map(|_| Vec::new()).collect(),
        }
    }

    pub(crate) fn get_or_create(
        &mut self,
        aggregate_idx: usize,
        key_types: Vec<LogicalType>,
        group_key_count: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        parallelism: usize,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<&mut DistinctKeyTable> {
        let aggregate_count = self.fragments.len();
        let fragments = self.fragments.get_mut(aggregate_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "distinct aggregate index out of bounds: index={aggregate_idx}, count={aggregate_count}"
            ))
        })?;
        if fragments.is_empty() {
            fragments.push(DistinctKeyTable::try_new(
                key_types.clone(),
                group_key_count,
                allocator,
                memory,
                parallelism,
                capacity_hint,
            )?);
        }
        if fragments.len() != 1 {
            return Err(paro_error::internal(format!(
                "cannot collect into merged distinct aggregate state: index={aggregate_idx}, fragments={}",
                fragments.len()
            )));
        }
        let table = fragments.first_mut().ok_or_else(|| {
            paro_error::internal(format!(
                "distinct aggregate table was not initialized: index={aggregate_idx}"
            ))
        })?;
        if table.key_types() != key_types.as_slice() {
            return Err(paro_error::internal(format!(
                "distinct aggregate schema changed at index {aggregate_idx}: expected={:?}, actual={key_types:?}",
                table.key_types()
            )));
        }
        if table.group_key_count != group_key_count {
            return Err(paro_error::internal(format!(
                "distinct aggregate group prefix changed at index {aggregate_idx}: expected={}, actual={group_key_count}",
                table.group_key_count
            )));
        }
        Ok(table)
    }

    pub(crate) fn take_coalesced(
        &mut self,
        aggregate_idx: usize,
    ) -> Result<Option<DistinctKeyTable>> {
        let mut fragments = self.take_fragments(aggregate_idx)?;
        let Some(mut target) = fragments.pop() else {
            return Ok(None);
        };
        for mut source in fragments {
            target.combine(&mut source)?;
        }
        Ok(Some(target))
    }

    pub(crate) fn take_partition_groups(
        &mut self,
        aggregate_idx: usize,
    ) -> Result<Vec<Vec<DistinctKeyTable>>> {
        let fragments = self.take_fragments(aggregate_idx)?;
        let mut partition_groups: Vec<Vec<DistinctKeyTable>> = Vec::new();
        for fragment in fragments {
            let partitions = fragment.into_partitions()?;
            if partition_groups.is_empty() {
                partition_groups = (0..partitions.len()).map(|_| Vec::new()).collect();
            } else if partition_groups.len() != partitions.len() {
                return Err(paro_error::internal(format!(
                    "distinct fragment partition count changed at aggregate {aggregate_idx}: expected={}, actual={}",
                    partition_groups.len(),
                    partitions.len()
                )));
            }
            for (group, partition) in partition_groups.iter_mut().zip(partitions) {
                group.push(partition);
            }
        }
        Ok(partition_groups)
    }

    fn take_fragments(&mut self, aggregate_idx: usize) -> Result<Vec<DistinctKeyTable>> {
        let aggregate_count = self.fragments.len();
        self.fragments
            .get_mut(aggregate_idx)
            .map(std::mem::take)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "distinct aggregate index out of bounds: index={aggregate_idx}, count={aggregate_count}"
                ))
            })
    }

    pub(crate) fn merge_from(&mut self, other: &mut Self) -> Result<()> {
        if self.fragments.len() != other.fragments.len() {
            return Err(paro_error::internal(format!(
                "distinct aggregate state count mismatch: target={}, source={}",
                self.fragments.len(),
                other.fragments.len()
            )));
        }
        for (aggregate_idx, (target, source)) in self
            .fragments
            .iter_mut()
            .zip(other.fragments.iter_mut())
            .enumerate()
        {
            if source.is_empty() {
                continue;
            }
            if let (Some(expected), Some(actual)) = (target.first(), source.first()) {
                if expected.key_types() != actual.key_types() {
                    return Err(paro_error::internal(format!(
                        "distinct aggregate schema changed while merging index {aggregate_idx}: expected={:?}, actual={:?}",
                        expected.key_types(),
                        actual.key_types()
                    )));
                }
                if expected.group_key_count != actual.group_key_count {
                    return Err(paro_error::internal(format!(
                        "distinct aggregate group prefix changed while merging index {aggregate_idx}: expected={}, actual={}",
                        expected.group_key_count, actual.group_key_count
                    )));
                }
            }
            target.append(source);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use paro_common::allocator::MemoryTag;
    use paro_common::chunk::Chunk;
    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::test_utils::{
        test_allocator, test_i32_vector_with_allocator, test_i64_vector_with_allocator,
    };
    use paro_common::types::LogicalType;

    use super::{AggregateHTScanPosition, DistinctKeyTable, HashTableCapacityHint};

    #[test]
    fn radix_partitions_keep_each_output_group_together() {
        let allocator = test_allocator();
        let groups = (0..96)
            .map(|row| if row < 64 { 7 } else { 8 })
            .collect::<Vec<_>>();
        let inputs = (0..96).map(i64::from).collect::<Vec<_>>();
        let keys = Chunk::from_vectors(
            vec![
                test_i32_vector_with_allocator(&groups, allocator.clone()),
                test_i64_vector_with_allocator(&inputs, allocator.clone()),
            ],
            allocator.clone(),
        );
        let mut table = DistinctKeyTable::try_new(
            vec![LogicalType::Integer, LogicalType::BigInt],
            1,
            allocator.clone(),
            MemoryAccountingContext::detached(
                MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
            4,
            HashTableCapacityHint::default(),
        )
        .expect("distinct table");
        table.insert(&keys).expect("insert distinct keys");

        let mut group_partitions = HashMap::new();
        for (partition_idx, mut partition) in table
            .into_partitions()
            .expect("split distinct partitions")
            .into_iter()
            .enumerate()
        {
            let mut output = Chunk::try_initialize(
                &[LogicalType::Integer, LogicalType::BigInt],
                128,
                allocator.clone(),
            )
            .expect("scan output");
            let mut position = AggregateHTScanPosition::default();
            while partition
                .scan(&mut position, &mut output)
                .expect("scan partition")
            {
                for row_idx in 0..output.size() {
                    let group = output.column(0).expect("group").get_i32(row_idx);
                    assert_eq!(
                        group_partitions.entry(group).or_insert(partition_idx),
                        &partition_idx,
                        "one output group must not span DISTINCT radix partitions"
                    );
                }
            }
        }
        assert_eq!(group_partitions.len(), 2);
    }
}
