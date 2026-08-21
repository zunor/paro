// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Radix-partitioned grouped aggregate hash table.
//!
//! This wraps multiple [`GroupedAggregateHashTable`] partitions and routes
//! rows by hash high bits, so each partition resizes/scans independently.

use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};

use super::aggregate_object::AggregateObject;
use super::grouped_aggregate_hashtable::{
    GroupedAggregateHashTable, HTScanPosition, HashTableCapacityHint, SerializedSourceRows,
};

const MAX_RADIX_PARTITION_BITS: usize = 8;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RadixHTScanPosition {
    pub partition_idx: usize,
    pub partition_positions: Vec<HTScanPosition>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregateHTScanPosition {
    pub flat: HTScanPosition,
    pub radix: RadixHTScanPosition,
}

#[derive(Debug)]
pub enum AggregateHashTable {
    Flat(GroupedAggregateHashTable),
    Radix(RadixPartitionedAggregateHashTable),
}

/// Concurrent ownership target for independently processed radix partitions.
///
/// DISTINCT finalization routes source keys by their output-group hash. Each
/// task therefore owns one complete output partition and can install its flat
/// table directly instead of re-routing and copying every row through another
/// radix table. Ordinary aggregate merge also uses this container to hand a
/// populated target partition to exactly one task. The coordinator calls
/// [`Self::finish`] after all installations.
#[derive(Debug)]
pub(crate) struct ConcurrentRadixAggregateBuild {
    group_types: Vec<LogicalType>,
    scan_output_types: Vec<LogicalType>,
    partition_bits: usize,
    partitions: Box<[Mutex<Option<GroupedAggregateHashTable>>]>,
}

impl ConcurrentRadixAggregateBuild {
    pub(crate) fn try_new(table: AggregateHashTable) -> Result<Self> {
        let AggregateHashTable::Radix(table) = table else {
            return Err(paro_error::internal(
                "concurrent aggregate merge requires a radix table",
            ));
        };
        let RadixPartitionedAggregateHashTable {
            group_types,
            partition_bits,
            partitions,
            ..
        } = table;
        validate_radix_partition_count(partition_bits, partitions.len())?;
        let scan_output_types = partitions
            .first()
            .map(GroupedAggregateHashTable::scan_output_types)
            .ok_or_else(|| paro_error::internal("radix aggregate target has no partitions"))?;
        if partitions
            .iter()
            .any(|partition| partition.scan_output_types() != scan_output_types)
        {
            return Err(paro_error::internal(
                "radix aggregate target partitions have inconsistent output schemas",
            ));
        }
        Ok(Self {
            group_types,
            scan_output_types,
            partition_bits,
            partitions: partitions
                .into_iter()
                .map(|partition| Mutex::new(Some(partition)))
                .collect(),
        })
    }

    /// Transfer exclusive ownership of one target partition to its task.
    pub(crate) fn take_partition(&self, partition_idx: usize) -> Result<AggregateHashTable> {
        let partition = self.partitions.get(partition_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "radix aggregate claim partition out of bounds: index={partition_idx}, count={}",
                self.partitions.len()
            ))
        })?;
        let table = partition.lock().take().ok_or_else(|| {
            paro_error::internal(format!(
                "radix aggregate partition was already claimed: index={partition_idx}"
            ))
        })?;
        Ok(AggregateHashTable::Flat(table))
    }

    pub(crate) fn install(&self, partition_idx: usize, table: AggregateHashTable) -> Result<()> {
        let AggregateHashTable::Flat(table) = table else {
            return Err(paro_error::internal(
                "direct radix aggregate assembly requires a flat partition",
            ));
        };
        if table.group_types() != self.group_types {
            return Err(paro_error::internal(format!(
                "radix aggregate partition schema mismatch: expected={:?}, actual={:?}",
                self.group_types,
                table.group_types()
            )));
        }
        let partition = self.partitions.get(partition_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "radix aggregate install partition out of bounds: index={partition_idx}, count={}",
                self.partitions.len()
            ))
        })?;
        let mut target = partition.lock();
        if target.is_some() {
            return Err(paro_error::internal(format!(
                "radix aggregate partition was installed without being claimed: index={partition_idx}"
            )));
        }
        if self.scan_output_types != table.scan_output_types() {
            return Err(paro_error::internal(format!(
                "radix aggregate partition output schema mismatch at index {partition_idx}: expected={:?}, actual={:?}",
                self.scan_output_types,
                table.scan_output_types()
            )));
        }
        *target = Some(table);
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<AggregateHashTable> {
        let mut partitions = Vec::with_capacity(self.partitions.len());
        for partition in &self.partitions {
            partitions.push(partition.lock().take().ok_or_else(|| {
                paro_error::internal("concurrent radix aggregate build was finalized twice")
            })?);
        }
        validate_radix_partition_count(self.partition_bits, partitions.len())?;
        Ok(AggregateHashTable::Radix(
            RadixPartitionedAggregateHashTable {
                group_types: self.group_types.clone(),
                partition_bits: self.partition_bits,
                partition_mask: partitions.len() - 1,
                partitions,
                scratch: RadixRoutingScratch::default(),
            },
        ))
    }
}

impl AggregateHashTable {
    /// Split a finalized table into independently scannable ownership units.
    pub fn into_scan_partitions(self) -> Vec<Self> {
        match self {
            Self::Flat(table) => vec![Self::Flat(table)],
            Self::Radix(table) => table
                .into_partitions()
                .into_iter()
                .map(Self::Flat)
                .collect(),
        }
    }

    pub(crate) fn visit_flat_partitions(
        &self,
        mut visit: impl FnMut(&GroupedAggregateHashTable) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Flat(table) => visit(table),
            Self::Radix(table) => {
                for partition in &table.partitions {
                    visit(partition)?;
                }
                Ok(())
            }
        }
    }

    /// Visit finalized aggregate columns across every physical partition
    /// while retaining the table for the later output scan.
    pub(crate) fn visit_finalized_aggregates(
        &mut self,
        capacity: usize,
        allocator: Arc<dyn Allocator>,
        mut visit: impl FnMut(&Chunk) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Flat(table) => table.visit_finalized_aggregates(capacity, allocator, &mut visit),
            Self::Radix(table) => {
                for partition in &mut table.partitions {
                    partition.visit_finalized_aggregates(
                        capacity,
                        allocator.clone(),
                        &mut visit,
                    )?;
                }
                Ok(())
            }
        }
    }

    pub fn new_flat(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::new_flat_with_memory(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            allocator,
            MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn new_flat_with_memory(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        Ok(Self::Flat(GroupedAggregateHashTable::new_with_memory(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            allocator,
            memory,
        )?))
    }

    pub(crate) fn new_flat_with_memory_capacity_hint(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<Self> {
        Ok(Self::Flat(
            GroupedAggregateHashTable::new_with_memory_capacity_hint(
                group_types,
                aggregate_objects,
                aggregate_inputs,
                allocator,
                memory,
                capacity_hint,
            )?,
        ))
    }

    pub fn new_radix(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        Self::new_radix_with_memory(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            partition_bits,
            allocator,
            MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
        )
    }

    pub fn new_radix_with_memory(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        Ok(Self::Radix(RadixPartitionedAggregateHashTable::new(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            partition_bits,
            allocator,
            memory,
        )?))
    }

    pub(crate) fn new_radix_with_memory_capacity_hint(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<Self> {
        Ok(Self::Radix(
            RadixPartitionedAggregateHashTable::new_with_capacity_hint(
                group_types,
                aggregate_objects,
                aggregate_inputs,
                partition_bits,
                allocator,
                memory,
                capacity_hint,
            )?,
        ))
    }

    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        match self {
            Self::Flat(table) => table.hash_groups(groups),
            Self::Radix(table) => table.hash_groups(groups),
        }
    }

    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.find_or_create_groups_partitioned(groups, hashes, hashes, addresses, new_groups)
    }

    /// Probe using `lookup_hashes` while routing radix ownership using
    /// `partition_hashes`.
    ///
    /// Ordinary aggregation passes the same vector for both. DISTINCT
    /// aggregation routes by the output-group prefix so one final group never
    /// spans multiple partitions, while exact deduplication still probes by
    /// the complete `(groups..., inputs...)` key.
    pub(crate) fn find_or_create_groups_partitioned(
        &mut self,
        groups: &Chunk,
        lookup_hashes: &Vector,
        partition_hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        match self {
            Self::Flat(table) => {
                table.find_or_create_groups(groups, lookup_hashes, addresses, new_groups)
            }
            Self::Radix(table) => table.find_or_create_groups_partitioned(
                groups,
                lookup_hashes,
                partition_hashes,
                addresses,
                new_groups,
            ),
        }
    }

    pub(crate) fn find_or_create_serialized_group_prefix(
        &mut self,
        source: &GroupedAggregateHashTable,
        source_rows: SerializedSourceRows<'_>,
        hashes: &Vector,
        addresses: &mut Vector,
    ) -> Result<()> {
        let count = source_rows.len();
        validate_hashes(hashes, count)?;
        validate_address_capacity(addresses, count)?;
        let hash_values = &hashes.as_slice::<u64>()[..count];
        match self {
            Self::Flat(table) => table.find_or_create_serialized_group_prefix(
                source,
                source_rows,
                hash_values,
                addresses,
            ),
            Self::Radix(table) => {
                table.find_or_create_serialized_group_prefix(source, source_rows, hashes, addresses)
            }
        }
    }

    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        hashes: Option<&Vector>,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        match self {
            Self::Flat(table) => table.update_aggregates(payload, addresses, filter),
            Self::Radix(table) => table.update_aggregates(payload, hashes, addresses, filter),
        }
    }

    pub fn update_aggregates_per_filter(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
        filters: &[Option<SelectionVector>],
    ) -> Result<()> {
        match self {
            Self::Flat(table) => table.update_aggregates_per_filter(payload, addresses, filters),
            Self::Radix(_) => Err(paro_error::internal(
                "radix partitioned aggregate does not support per-filter updates",
            )),
        }
    }

    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        match (self, other) {
            (Self::Flat(left), Self::Flat(right)) => left.combine(right),
            (Self::Radix(left), Self::Radix(right)) => left.combine(right),
            (left, right) => Err(paro_error::internal(format!(
                "Cannot combine aggregate hash tables with different implementations: left={:?}, right={:?}",
                left.table_kind(),
                right.table_kind()
            ))),
        }
    }

    pub fn scan(
        &mut self,
        position: &mut AggregateHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        match self {
            Self::Flat(table) => {
                let produced = table.scan(&mut position.flat, result)?;
                if !produced {
                    table.destroy()?;
                }
                Ok(produced)
            }
            Self::Radix(table) => table.scan(&mut position.radix, result),
        }
    }

    pub fn scan_with_aggregate_filter(
        &mut self,
        position: &mut AggregateHTScanPosition,
        result: &mut Chunk,
        selection: &mut SelectionVector,
        mut select: impl FnMut(&Chunk, usize, &mut SelectionVector) -> Result<usize>,
    ) -> Result<bool> {
        match self {
            Self::Flat(table) => {
                table.scan_with_aggregate_filter(&mut position.flat, result, selection, select)
            }
            Self::Radix(table) => table.scan_with_aggregate_filter(
                &mut position.radix,
                result,
                selection,
                &mut select,
            ),
        }
    }

    pub fn scan_state_rows(
        &self,
        position: &mut AggregateHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        match self {
            Self::Flat(table) => table.scan_state_rows(&mut position.flat, result),
            Self::Radix(table) => table.scan_state_rows(&mut position.radix, result),
        }
    }

    pub fn scan_serialized_state_rows(
        &self,
        position: &mut AggregateHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        match self {
            Self::Flat(table) => table.scan_serialized_state_rows(&mut position.flat, result),
            Self::Radix(table) => table.scan_serialized_state_rows(&mut position.radix, result),
        }
    }

    pub fn destroy(&mut self) -> Result<()> {
        match self {
            Self::Flat(table) => table.destroy(),
            Self::Radix(table) => table.destroy(),
        }
    }

    pub fn inline_key_width(&self) -> Option<usize> {
        match self {
            Self::Flat(table) => table.inline_key_width(),
            Self::Radix(table) => table.inline_key_width(),
        }
    }

    pub fn scan_output_types(&self) -> Vec<LogicalType> {
        match self {
            Self::Flat(table) => table.scan_output_types(),
            Self::Radix(table) => table.scan_output_types(),
        }
    }

    pub fn aggregate_count(&self) -> usize {
        match self {
            Self::Flat(table) => table.aggregate_count(),
            Self::Radix(table) => table.aggregate_count(),
        }
    }

    pub fn radix_partition_count(&self) -> Option<usize> {
        match self {
            Self::Flat(_) => None,
            Self::Radix(table) => Some(table.partition_count()),
        }
    }

    pub fn memory_usage(&self) -> usize {
        match self {
            Self::Flat(table) => table.memory_usage(),
            Self::Radix(table) => table.memory_usage(),
        }
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        match self {
            Self::Flat(table) => table.external_accounted_memory_usage(),
            Self::Radix(table) => table.external_accounted_memory_usage(),
        }
    }

    pub fn reclaimable_finalized_memory(&self) -> usize {
        match self {
            Self::Flat(table) => table.reclaimable_finalized_memory(),
            Self::Radix(table) => table.reclaimable_finalized_memory(),
        }
    }

    pub fn reclaimable_build_memory(&self) -> usize {
        match self {
            Self::Flat(table) => table.reclaimable_build_memory(),
            Self::Radix(table) => table.reclaimable_build_memory(),
        }
    }

    pub fn reclaim_build_memory(&mut self, target_bytes: usize) -> usize {
        match self {
            Self::Flat(table) => table.reclaim_build_memory(target_bytes),
            Self::Radix(table) => table.reclaim_build_memory(target_bytes),
        }
    }

    pub fn reclaim_finalized_memory(&mut self, target_bytes: usize) -> usize {
        match self {
            Self::Flat(table) => table.reclaim_finalized_memory(target_bytes),
            Self::Radix(table) => table.reclaim_finalized_memory(target_bytes),
        }
    }

    pub fn count(&self) -> usize {
        match self {
            Self::Flat(table) => table.count(),
            Self::Radix(table) => table.count(),
        }
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        match self {
            Self::Flat(table) => table.allocator(),
            Self::Radix(table) => table.allocator(),
        }
    }

    fn table_kind(&self) -> &'static str {
        match self {
            Self::Flat(_) => "flat",
            Self::Radix(_) => "radix",
        }
    }
}

#[derive(Debug)]
pub struct RadixPartitionedAggregateHashTable {
    group_types: Vec<LogicalType>,
    partition_bits: usize,
    partition_mask: usize,
    partitions: Vec<GroupedAggregateHashTable>,
    scratch: RadixRoutingScratch,
}

#[derive(Debug, Default)]
struct RadixRoutingScratch {
    partition_ids: Vec<usize>,
    counts: Vec<usize>,
    offsets: Vec<usize>,
    cursors: Vec<usize>,
    rows_by_partition: Vec<u32>,
    serialized_rows_by_partition: Vec<u32>,
    decoded_hashes: Vec<u64>,
    hashes_by_partition: Vec<u64>,
    selection: Option<SelectionVector>,
    address_vector: Option<Vector>,
    partition_addresses: Option<Vector>,
    partition_new_groups: Option<SelectionVector>,
}

impl RadixPartitionedAggregateHashTable {
    fn into_partitions(self) -> Vec<GroupedAggregateHashTable> {
        self.partitions
    }

    pub fn new(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
    ) -> Result<Self> {
        Self::new_with_capacity_hint(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            partition_bits,
            allocator,
            memory,
            HashTableCapacityHint::default(),
        )
    }

    fn new_with_capacity_hint(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<Self> {
        let partition_count = radix_partition_count(partition_bits)?;
        let partition_hint = capacity_hint.divided_across(partition_count);
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(GroupedAggregateHashTable::new_with_memory_capacity_hint(
                group_types.clone(),
                aggregate_objects.clone(),
                aggregate_inputs.clone(),
                allocator.clone(),
                memory.clone(),
                partition_hint,
            )?);
        }
        Ok(Self {
            group_types,
            partition_bits,
            partition_mask: partition_count - 1,
            partitions,
            scratch: RadixRoutingScratch::default(),
        })
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn inline_key_width(&self) -> Option<usize> {
        self.partitions
            .first()
            .and_then(GroupedAggregateHashTable::inline_key_width)
    }

    pub fn scan_output_types(&self) -> Vec<LogicalType> {
        self.partitions
            .first()
            .map(GroupedAggregateHashTable::scan_output_types)
            .unwrap_or_else(|| self.group_types.clone())
    }

    pub fn aggregate_count(&self) -> usize {
        self.partitions
            .first()
            .map(GroupedAggregateHashTable::aggregate_count)
            .unwrap_or(0)
    }

    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        self.partitions
            .first()
            .ok_or_else(|| {
                paro_error::internal("Radix aggregate hash table has no partitions".to_string())
            })?
            .hash_groups(groups)
    }

    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.find_or_create_groups_partitioned(groups, hashes, hashes, addresses, new_groups)
    }

    fn find_or_create_groups_partitioned(
        &mut self,
        groups: &Chunk,
        lookup_hashes: &Vector,
        partition_hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        validate_hashes(lookup_hashes, groups.size())?;
        validate_hashes(partition_hashes, groups.size())?;
        validate_address_capacity(addresses, groups.size())?;

        if groups.size() == 0 {
            addresses.try_set_count(0)?;
            new_groups.set_len(0);
            return Ok(0);
        }

        self.scratch.route_hashes(
            self.partition_bits,
            self.partition_mask,
            self.partitions.len(),
            partition_hashes,
            lookup_hashes,
            groups.size(),
        )?;

        addresses.try_set_count(groups.size())?;
        if new_groups.capacity() < groups.size() {
            *new_groups =
                SelectionVector::try_with_capacity(groups.size(), groups.allocator().clone())?;
        }
        new_groups.set_len(groups.size());
        let new_group_data = new_groups.as_mut_slice().as_mut_ptr();
        let mut new_group_count = 0usize;

        let RadixPartitionedAggregateHashTable {
            partitions,
            scratch,
            ..
        } = self;
        let mut partition_new_groups = take_selection_scratch(
            &mut scratch.partition_new_groups,
            groups.allocator().clone(),
        )?;

        for partition_idx in 0..partitions.len() {
            let (start, end) = scratch.partition_range(partition_idx)?;
            if start == end {
                continue;
            }
            let partition_row_count = end - start;
            ensure_selection_scratch(
                &mut partition_new_groups,
                partition_row_count,
                groups.allocator().clone(),
            )?;

            let partition = partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds: partition_idx={partition_idx}"
                ))
            })?;
            partition.find_or_create_groups_selected(
                groups,
                &scratch.rows_by_partition[start..end],
                &scratch.hashes_by_partition[start..end],
                addresses,
                &mut partition_new_groups,
            )?;

            for idx in 0..partition_new_groups.len() {
                let global_row = partition_new_groups.get(idx);
                if global_row >= groups.size() {
                    return Err(paro_error::internal(format!(
                        "Partition new-group index out of bounds: partition_idx={partition_idx}, row={global_row}, groups={}",
                        groups.size()
                    )));
                }
                // SAFETY: `new_groups` was sized to the full input cardinality and
                // every partition contributes at most one entry per routed row.
                unsafe {
                    *new_group_data.add(new_group_count) = global_row as u32;
                }
                new_group_count += 1;
            }
        }

        scratch.partition_new_groups = Some(partition_new_groups);
        new_groups.set_len(new_group_count);
        Ok(new_groups.len())
    }

    fn find_or_create_serialized_group_prefix(
        &mut self,
        source: &GroupedAggregateHashTable,
        source_rows: SerializedSourceRows<'_>,
        hashes: &Vector,
        addresses: &mut Vector,
    ) -> Result<()> {
        let count = source_rows.len();
        validate_hashes(hashes, count)?;
        validate_address_capacity(addresses, count)?;
        if count == 0 {
            addresses.try_set_count(0)?;
            return Ok(());
        }
        self.scratch.route_hashes(
            self.partition_bits,
            self.partition_mask,
            self.partitions.len(),
            hashes,
            hashes,
            count,
        )?;
        self.scratch
            .route_serialized_source_rows(source_rows, count)?;

        addresses.try_set_count(count)?;
        let address_data = unsafe { addresses.flat_data_mut::<*mut u8>() };
        let RadixPartitionedAggregateHashTable {
            partitions,
            scratch,
            ..
        } = self;
        let mut partition_addresses = take_vector_scratch(
            &mut scratch.partition_addresses,
            LogicalType::BigInt,
            source.allocator(),
        )?;

        for (partition_idx, partition) in partitions.iter_mut().enumerate() {
            let (start, end) = scratch.partition_range(partition_idx)?;
            if start == end {
                continue;
            }
            let partition_count = end - start;
            ensure_vector_scratch(
                &mut partition_addresses,
                LogicalType::BigInt,
                partition_count,
                source.allocator(),
            )?;
            partition.find_or_create_serialized_group_prefix(
                source,
                SerializedSourceRows::new(
                    source_rows.start(),
                    &scratch.serialized_rows_by_partition[start..end],
                ),
                &scratch.hashes_by_partition[start..end],
                &mut partition_addresses,
            )?;

            let partition_address_data = unsafe { partition_addresses.flat_data::<*mut u8>() };
            for local_row in 0..partition_count {
                let global_row = scratch.rows_by_partition[start + local_row] as usize;
                unsafe {
                    *address_data.add(global_row) = *partition_address_data.add(local_row);
                }
            }
        }
        scratch.partition_addresses = Some(partition_addresses);
        Ok(())
    }

    pub fn update_aggregates(
        &mut self,
        payload: &Chunk,
        hashes: Option<&Vector>,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        if payload.size() == 0 {
            return Ok(());
        }
        if filter.is_some() {
            return Err(paro_error::internal(
                "Radix partitioned aggregate hash table does not support filtered updates directly"
                    .to_string(),
            ));
        }
        let hashes = hashes.ok_or_else(|| {
            paro_error::internal(
                "Radix partitioned aggregate hash table requires hash vector for updates"
                    .to_string(),
            )
        })?;
        validate_hashes(hashes, payload.size())?;
        if addresses.len() < payload.size() {
            return Err(paro_error::internal(format!(
                "Address vector too small for radix aggregate update: addresses={} payload_rows={}",
                addresses.len(),
                payload.size()
            )));
        }

        self.scratch.route_hashes(
            self.partition_bits,
            self.partition_mask,
            self.partitions.len(),
            hashes,
            hashes,
            payload.size(),
        )?;

        let RadixPartitionedAggregateHashTable {
            partitions,
            scratch,
            ..
        } = self;

        for partition_idx in 0..partitions.len() {
            let (start, end) = scratch.partition_range(partition_idx)?;
            if start == end {
                continue;
            }
            let selection = scratch.partition_selection(start, end, payload.allocator().clone())?;
            let mut partition_payload = payload.clone_referencing_vectors();
            partition_payload.try_slice(selection, end - start)?;
            let partition_addresses = scratch.selected_address_vector(
                addresses,
                start,
                end,
                payload.allocator().clone(),
            )?;
            let partition = partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during update: partition_idx={partition_idx}"
                ))
            })?;
            partition.update_aggregates(&partition_payload, partition_addresses, None)?;
        }

        Ok(())
    }

    pub fn combine(&mut self, other: &mut Self) -> Result<()> {
        if self.partition_bits != other.partition_bits
            || self.group_types != other.group_types
            || self.partitions.len() != other.partitions.len()
        {
            return Err(paro_error::internal(format!(
                "Cannot combine radix aggregate hash tables with different layouts: \
bits {}/{} partitions {}/{} group_types {:?}/{:?}",
                self.partition_bits,
                other.partition_bits,
                self.partitions.len(),
                other.partitions.len(),
                self.group_types,
                other.group_types
            )));
        }
        for partition_idx in 0..self.partitions.len() {
            let left = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during combine: partition_idx={partition_idx}"
                ))
            })?;
            let right = other.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during combine: partition_idx={partition_idx}"
                ))
            })?;
            left.combine(right)?;
        }
        Ok(())
    }

    pub fn scan(&mut self, position: &mut RadixHTScanPosition, result: &mut Chunk) -> Result<bool> {
        if position.partition_positions.len() != self.partitions.len() {
            position.partition_positions = vec![HTScanPosition::default(); self.partitions.len()];
            position.partition_idx = 0;
        }
        while position.partition_idx < self.partitions.len() {
            let partition_idx = position.partition_idx;
            let partition = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during scan: partition_idx={partition_idx}"
                ))
            })?;
            let part_position = position
                .partition_positions
                .get_mut(partition_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Radix partition scan position missing: partition_idx={partition_idx}"
                    ))
                })?;
            if partition.scan(part_position, result)? {
                return Ok(true);
            }
            partition.destroy()?;
            position.partition_idx += 1;
        }
        result.try_set_cardinality(0)?;
        Ok(false)
    }

    pub fn scan_with_aggregate_filter(
        &mut self,
        position: &mut RadixHTScanPosition,
        result: &mut Chunk,
        selection: &mut SelectionVector,
        mut select: impl FnMut(&Chunk, usize, &mut SelectionVector) -> Result<usize>,
    ) -> Result<bool> {
        while position.partition_idx < self.partitions.len() {
            let partition_idx = position.partition_idx;
            let partition = self.partitions.get_mut(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during filtered scan: partition_idx={partition_idx}"
                ))
            })?;
            if position.partition_positions.len() <= partition_idx {
                position
                    .partition_positions
                    .resize_with(partition_idx + 1, HTScanPosition::default);
            }
            let partition_position = &mut position.partition_positions[partition_idx];
            if partition.scan_with_aggregate_filter(
                partition_position,
                result,
                selection,
                &mut select,
            )? {
                return Ok(true);
            }
            partition.destroy()?;
            position.partition_idx += 1;
        }
        result.try_set_cardinality(0)?;
        Ok(false)
    }

    pub fn scan_state_rows(
        &self,
        position: &mut RadixHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        if position.partition_positions.len() != self.partitions.len() {
            position.partition_positions = vec![HTScanPosition::default(); self.partitions.len()];
            position.partition_idx = 0;
        }
        while position.partition_idx < self.partitions.len() {
            let partition_idx = position.partition_idx;
            let partition = self.partitions.get(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during state scan: partition_idx={partition_idx}"
                ))
            })?;
            let part_position = position
                .partition_positions
                .get_mut(partition_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Radix partition state scan position missing: partition_idx={partition_idx}"
                    ))
                })?;
            if partition.scan_state_rows(part_position, result)? {
                return Ok(true);
            }
            position.partition_idx += 1;
        }
        result.try_set_cardinality(0)?;
        Ok(false)
    }

    pub fn scan_serialized_state_rows(
        &self,
        position: &mut RadixHTScanPosition,
        result: &mut Chunk,
    ) -> Result<bool> {
        if position.partition_positions.len() != self.partitions.len() {
            position.partition_positions = vec![HTScanPosition::default(); self.partitions.len()];
            position.partition_idx = 0;
        }
        while position.partition_idx < self.partitions.len() {
            let partition_idx = position.partition_idx;
            let partition = self.partitions.get(partition_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Radix partition index out of bounds during serialized state scan: partition_idx={partition_idx}"
                ))
            })?;
            let part_position = position
                .partition_positions
                .get_mut(partition_idx)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Radix partition serialized state scan position missing: partition_idx={partition_idx}"
                    ))
                })?;
            if partition.scan_serialized_state_rows(part_position, result)? {
                return Ok(true);
            }
            position.partition_idx += 1;
        }
        result.try_set_cardinality(0)?;
        Ok(false)
    }

    pub fn destroy(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.destroy()?;
        }
        Ok(())
    }

    pub fn memory_usage(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::memory_usage)
            .sum()
    }

    pub fn external_accounted_memory_usage(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::external_accounted_memory_usage)
            .sum()
    }

    pub fn reclaimable_finalized_memory(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::reclaimable_finalized_memory)
            .sum()
    }

    pub fn reclaimable_build_memory(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::reclaimable_build_memory)
            .sum()
    }

    pub fn reclaim_build_memory(&mut self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }
        let mut reclaimed = 0usize;
        for partition in &mut self.partitions {
            if reclaimed >= target_bytes {
                break;
            }
            reclaimed =
                reclaimed.saturating_add(partition.reclaim_build_memory(target_bytes - reclaimed));
        }
        reclaimed
    }

    pub fn reclaim_finalized_memory(&mut self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }
        let mut reclaimed = 0usize;
        for partition in &mut self.partitions {
            if reclaimed >= target_bytes {
                break;
            }
            reclaimed = reclaimed
                .saturating_add(partition.reclaim_finalized_memory(target_bytes - reclaimed));
        }
        reclaimed
    }

    pub fn count(&self) -> usize {
        self.partitions
            .iter()
            .map(GroupedAggregateHashTable::count)
            .sum()
    }

    pub fn allocator(&self) -> Arc<dyn Allocator> {
        self.partitions
            .first()
            .map(GroupedAggregateHashTable::allocator)
            .expect("radix aggregate hash table should have partitions")
    }
}

fn validate_hashes(hashes: &Vector, row_count: usize) -> Result<()> {
    if hashes.logical_type() != &LogicalType::UBigInt {
        return Err(paro_error::internal(format!(
            "Hash vector type must be UBigInt, found {:?}",
            hashes.logical_type()
        )));
    }
    if hashes.len() < row_count {
        return Err(paro_error::internal(format!(
            "Hash vector too small: required={row_count}, actual={}",
            hashes.len()
        )));
    }
    Ok(())
}

fn validate_address_capacity(addresses: &Vector, row_count: usize) -> Result<()> {
    if addresses.capacity() < row_count {
        return Err(paro_error::internal(format!(
            "Address vector capacity too small: required={row_count}, capacity={}",
            addresses.capacity()
        )));
    }
    Ok(())
}

fn radix_partition_count(partition_bits: usize) -> Result<usize> {
    if partition_bits == 0 || partition_bits > MAX_RADIX_PARTITION_BITS {
        return Err(paro_error::internal(format!(
            "Invalid radix partition bits for aggregate hash table: bits={partition_bits}, allowed=1..={MAX_RADIX_PARTITION_BITS}"
        )));
    }
    1usize.checked_shl(partition_bits as u32).ok_or_else(|| {
        paro_error::internal(format!(
            "Radix partition count overflow for bits={partition_bits}"
        ))
    })
}

fn validate_radix_partition_count(partition_bits: usize, actual: usize) -> Result<()> {
    let expected = radix_partition_count(partition_bits)?;
    if actual != expected {
        return Err(paro_error::internal(format!(
            "Radix aggregate partition count mismatch: bits={partition_bits}, expected={expected}, actual={actual}"
        )));
    }
    Ok(())
}

impl RadixRoutingScratch {
    fn route_serialized_source_rows(
        &mut self,
        source_rows: SerializedSourceRows<'_>,
        row_count: usize,
    ) -> Result<()> {
        if source_rows.len() != row_count || self.rows_by_partition.len() != row_count {
            return Err(paro_error::internal(format!(
                "Serialized radix route size mismatch: source={}, routed={}, expected={row_count}",
                source_rows.len(),
                self.rows_by_partition.len()
            )));
        }
        self.serialized_rows_by_partition.resize(row_count, 0);
        for routed_idx in 0..row_count {
            let input_idx = self.rows_by_partition[routed_idx] as usize;
            let relative = source_rows.relative_row(input_idx)?;
            self.serialized_rows_by_partition[routed_idx] =
                u32::try_from(relative).map_err(|_| {
                    paro_error::internal(format!(
                        "Serialized source row offset exceeds u32: offset={relative}"
                    ))
                })?;
        }
        Ok(())
    }

    fn route_hashes(
        &mut self,
        partition_bits: usize,
        partition_mask: usize,
        partition_count: usize,
        partition_hashes: &Vector,
        lookup_hashes: &Vector,
        row_count: usize,
    ) -> Result<()> {
        self.partition_ids.resize(row_count, 0);
        self.rows_by_partition.resize(row_count, 0);
        self.decoded_hashes.resize(row_count, 0);
        self.hashes_by_partition.resize(row_count, 0);
        self.counts.resize(partition_count, 0);
        self.offsets.resize(partition_count + 1, 0);
        self.cursors.resize(partition_count, 0);
        self.counts.fill(0);

        validate_hashes(partition_hashes, row_count)?;
        validate_hashes(lookup_hashes, row_count)?;
        let partition_format = partition_hashes.try_decode_ref(row_count)?;
        let partition_data = partition_format.get_data::<u64>();
        let shift = (u64::BITS as usize).saturating_sub(partition_bits);
        for row_idx in 0..row_count {
            let physical_idx = partition_format.physical_index(row_idx);
            if !partition_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Group partition hash contains NULL at row {row_idx}"
                )));
            }
            let hash = unsafe { *partition_data.add(physical_idx) };
            let partition_idx = ((hash >> shift) as usize) & partition_mask;
            self.partition_ids[row_idx] = partition_idx;
            self.decoded_hashes[row_idx] = hash;
            self.counts[partition_idx] += 1;
        }

        if !std::ptr::eq(partition_hashes, lookup_hashes) {
            let lookup_format = lookup_hashes.try_decode_ref(row_count)?;
            let lookup_data = lookup_format.get_data::<u64>();
            for row_idx in 0..row_count {
                let physical_idx = lookup_format.physical_index(row_idx);
                if !lookup_format.validity().is_valid(physical_idx) {
                    return Err(paro_error::internal(format!(
                        "Group lookup hash contains NULL at row {row_idx}"
                    )));
                }
                self.decoded_hashes[row_idx] = unsafe { *lookup_data.add(physical_idx) };
            }
        }

        self.offsets[0] = 0;
        for partition_idx in 0..partition_count {
            self.offsets[partition_idx + 1] =
                self.offsets[partition_idx].saturating_add(self.counts[partition_idx]);
            self.cursors[partition_idx] = self.offsets[partition_idx];
        }

        for row_idx in 0..row_count {
            let partition_idx = self.partition_ids[row_idx];
            let target = self.cursors[partition_idx];
            self.rows_by_partition[target] = row_idx as u32;
            self.hashes_by_partition[target] = self.decoded_hashes[row_idx];
            self.cursors[partition_idx] += 1;
        }
        Ok(())
    }

    fn partition_range(&self, partition_idx: usize) -> Result<(usize, usize)> {
        let start = *self.offsets.get(partition_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Radix partition offset missing: partition_idx={partition_idx}"
            ))
        })?;
        let end = *self.offsets.get(partition_idx + 1).ok_or_else(|| {
            paro_error::internal(format!(
                "Radix partition end offset missing: partition_idx={partition_idx}"
            ))
        })?;
        Ok((start, end))
    }

    fn partition_selection(
        &mut self,
        start: usize,
        end: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&SelectionVector> {
        let count = end - start;
        let selection = self.selection.get_or_insert_with(|| {
            SelectionVector::try_with_capacity(count.max(1), allocator.clone())
                .expect("selection allocation")
        });
        if selection.capacity() < count.max(1) {
            *selection = SelectionVector::try_with_capacity(count.max(1), allocator)?;
        }
        selection.set_len(count);
        selection
            .as_mut_slice()
            .copy_from_slice(&self.rows_by_partition[start..end]);
        Ok(selection)
    }

    fn selected_address_vector(
        &mut self,
        addresses: &Vector,
        start: usize,
        end: usize,
        allocator: Arc<dyn Allocator>,
    ) -> Result<&Vector> {
        let count = end - start;
        let vector = ensure_vector(
            &mut self.address_vector,
            LogicalType::BigInt,
            count,
            allocator,
        )?;
        let address_format = addresses.try_decode_ref(addresses.len())?;
        let address_data = address_format.get_data::<*mut u8>();
        let target = unsafe { vector.flat_data_mut::<*mut u8>() };
        for (target_idx, &source_row) in self.rows_by_partition[start..end].iter().enumerate() {
            let source_row = source_row as usize;
            if source_row >= addresses.len() {
                return Err(paro_error::internal(format!(
                    "Address gather row index out of bounds: source_row={source_row}, addresses={}",
                    addresses.len()
                )));
            }
            let physical_idx = address_format.physical_index(source_row);
            if !address_format.validity().is_valid(physical_idx) {
                return Err(paro_error::internal(format!(
                    "Address vector contains NULL while gathering rows: source_row={source_row}"
                )));
            }
            unsafe {
                *target.add(target_idx) = *address_data.add(physical_idx);
            }
        }
        Ok(vector)
    }
}

fn ensure_vector(
    slot: &mut Option<Vector>,
    ty: LogicalType,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<&mut Vector> {
    let required = count.max(1);
    let needs_new = slot
        .as_ref()
        .is_none_or(|vector| vector.logical_type() != &ty || vector.capacity() < required);
    if needs_new {
        *slot = Some(Vector::try_new(ty, required, allocator)?);
    }
    let vector = slot.as_mut().expect("vector initialized above");
    vector.try_set_count(count)?;
    Ok(vector)
}

fn take_vector_scratch(
    slot: &mut Option<Vector>,
    ty: LogicalType,
    allocator: Arc<dyn Allocator>,
) -> Result<Vector> {
    match slot.take() {
        Some(mut vector) => {
            ensure_vector_scratch(&mut vector, ty, 0, allocator)?;
            Ok(vector)
        }
        None => Vector::try_new(ty, 1, allocator),
    }
}

fn ensure_vector_scratch(
    vector: &mut Vector,
    ty: LogicalType,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let required = count.max(1);
    if vector.logical_type() != &ty || vector.capacity() < required {
        *vector = Vector::try_new(ty, required, allocator)?;
    }
    vector.try_set_count(count)?;
    Ok(())
}

fn take_selection_scratch(
    slot: &mut Option<SelectionVector>,
    allocator: Arc<dyn Allocator>,
) -> Result<SelectionVector> {
    match slot.take() {
        Some(selection) => Ok(selection),
        None => SelectionVector::try_with_capacity(1, allocator),
    }
}

fn ensure_selection_scratch(
    selection: &mut SelectionVector,
    count: usize,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let required = count.max(1);
    if selection.capacity() < required {
        *selection = SelectionVector::try_with_capacity(required, allocator)?;
    }
    selection.set_len(count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_common::test_utils::{
        test_allocator, test_chunk_with_capacity, test_i32_vector_with_allocator,
        test_i64_vector_with_allocator, test_selection_with_capacity, test_vector_with_capacity,
    };

    fn insert_integer_groups(table: &mut AggregateHashTable, values: &[i32]) {
        let allocator = test_allocator();
        let groups = Chunk::from_vectors(
            vec![test_i32_vector_with_allocator(values, allocator.clone())],
            allocator,
        );
        let hashes = table.hash_groups(&groups).expect("hash groups");
        let mut addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = test_selection_with_capacity(groups.size());
        table
            .find_or_create_groups(&groups, &hashes, &mut addresses, &mut new_groups)
            .expect("find/create groups");
    }

    fn drain_integer_group_table(table: &mut AggregateHashTable) -> usize {
        let mut position = AggregateHTScanPosition::default();
        let mut output = test_chunk_with_capacity(&[LogicalType::Integer], 2);
        let mut rows = 0usize;
        while table
            .scan(&mut position, &mut output)
            .expect("scan aggregate table")
        {
            rows += output.size();
        }
        rows
    }

    #[test]
    fn flat_aggregate_scan_releases_completed_table_memory() {
        let mut table = AggregateHashTable::new_flat(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            test_allocator(),
        )
        .expect("flat aggregate table");
        insert_integer_groups(&mut table, &[1, 2, 3, 4, 5]);
        let before = table.memory_usage();
        assert!(before > 0);

        assert_eq!(drain_integer_group_table(&mut table), 5);

        assert_eq!(table.count(), 0);
        assert_eq!(table.memory_usage(), 0);
    }

    #[test]
    fn radix_aggregate_scan_releases_completed_partition_memory() {
        let mut table = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            test_allocator(),
        )
        .expect("radix aggregate table");
        insert_integer_groups(&mut table, &[1, 2, 3, 4, 5, 6, 7, 8]);
        let before = table.memory_usage();
        assert!(before > 0);

        assert_eq!(drain_integer_group_table(&mut table), 8);

        assert_eq!(table.count(), 0);
        assert_eq!(table.memory_usage(), 0);
    }

    #[test]
    fn concurrent_radix_build_merges_into_owned_populated_partitions() {
        let mut target = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            test_allocator(),
        )
        .expect("target table");
        insert_integer_groups(&mut target, &[10, 11]);

        let build = Arc::new(
            ConcurrentRadixAggregateBuild::try_new(target).expect("concurrent build target"),
        );
        std::thread::scope(|scope| {
            let first_build = Arc::clone(&build);
            let first_task = scope.spawn(move || {
                let mut first = first_build.take_partition(0)?;
                insert_integer_groups(&mut first, &[1, 2]);
                first_build.install(0, first)
            });
            let second_build = Arc::clone(&build);
            let second_task = scope.spawn(move || {
                let mut second = second_build.take_partition(1)?;
                insert_integer_groups(&mut second, &[3, 4]);
                second_build.install(1, second)
            });
            first_task
                .join()
                .expect("first merge task")
                .expect("install first partition");
            second_task
                .join()
                .expect("second merge task")
                .expect("install second partition");
        });

        let mut table = build.finish().expect("finish concurrent build");
        assert_eq!(drain_integer_group_table(&mut table), 6);
    }

    #[test]
    fn serialized_prefix_projection_routes_radix_addresses_to_original_rows() {
        let allocator = test_allocator();
        let groups = (0..64).map(|value| value % 17).collect::<Vec<i32>>();
        let inputs = (0..64).map(i64::from).collect::<Vec<i64>>();
        let source_chunk = Chunk::from_vectors(
            vec![
                test_i32_vector_with_allocator(&groups, allocator.clone()),
                test_i64_vector_with_allocator(&inputs, allocator.clone()),
            ],
            allocator.clone(),
        );
        let mut source = GroupedAggregateHashTable::new(
            vec![LogicalType::Integer, LogicalType::BigInt],
            Vec::new(),
            Vec::new(),
            allocator.clone(),
        )
        .expect("source table");
        let source_hashes = source.hash_groups(&source_chunk).expect("source hashes");
        let mut source_addresses = test_vector_with_capacity(LogicalType::BigInt, groups.len());
        let mut source_new_groups = test_selection_with_capacity(groups.len());
        source
            .find_or_create_groups(
                &source_chunk,
                &source_hashes,
                &mut source_addresses,
                &mut source_new_groups,
            )
            .expect("insert source rows");

        let mut run_starts = test_selection_with_capacity(groups.len());
        let mut projected_hashes = test_vector_with_capacity(LogicalType::UBigInt, groups.len());
        source
            .project_serialized_group_prefix_runs(
                0,
                groups.len(),
                1,
                &mut run_starts,
                &mut projected_hashes,
            )
            .expect("project serialized prefix runs");
        let mut target = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            allocator,
        )
        .expect("target radix table");
        let mut target_addresses = test_vector_with_capacity(LogicalType::BigInt, groups.len());
        target
            .find_or_create_serialized_group_prefix(
                &source,
                SerializedSourceRows::new(0, run_starts.as_slice()),
                &projected_hashes,
                &mut target_addresses,
            )
            .expect("project serialized prefixes");

        assert_eq!(target.count(), 17);
        let mut addresses_by_group = std::collections::HashMap::new();
        for (row_idx, group) in groups.into_iter().enumerate() {
            let address = target_addresses.get_i64(row_idx).expect("target address");
            match addresses_by_group.insert(group, address) {
                Some(previous) => assert_eq!(address, previous),
                None => {}
            }
        }
    }
}
