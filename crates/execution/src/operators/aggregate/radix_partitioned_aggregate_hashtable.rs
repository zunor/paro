// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Radix-partitioned grouped aggregate hash table.
//!
//! This wraps multiple [`GroupedAggregateHashTable`] partitions and routes
//! rows by hash high bits, so each partition resizes/scans independently.

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};

use super::aggregate_object::AggregateObject;
use super::group_hash::{
    hash_group_columns, hash_group_columns_prefix, AggregateHashContract, AggregateHashVectors,
    DistinctHashVectors, GroupHashScratch, IncomingHashContract, RoutingHashContract,
};
use super::grouped_aggregate_hashtable::{
    AggregateHashRuntimeStats, GroupedAggregateHashTable, GroupedAggregateHashTableConfig,
    HTScanPosition, HashTableCapacityHint, HashTableGrowthRequirement, SerializedSourceRows,
};

use paro_common::memory::MemoryGrant;

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

/// Ownership-preserving result of dismantling an aggregate table for parallel
/// partition work. Runtime observations belong to the table as much as its
/// tuples do, so they must travel through the same ownership transfer.
#[must_use = "partition ownership and runtime observations must be transferred together"]
#[derive(Debug)]
pub(crate) struct AggregateHashTablePartitionBundle {
    pub(crate) partitions: Vec<AggregateHashTable>,
    pub(crate) hash_runtime_stats: AggregateHashRuntimeStats,
}

/// Process-unique identity for the routing scratch owned by one radix table.
///
/// A routing epoch is meaningful only within its owner. Keeping the identity
/// separate from the epoch prevents two freshly-created tables at the same
/// epoch from accepting each other's lookup capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RadixTableIdentity(NonZeroU64);

impl RadixTableIdentity {
    fn try_new() -> Result<Self> {
        static NEXT_IDENTITY: AtomicU64 = AtomicU64::new(1);

        let identity = NEXT_IDENTITY
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| paro_error::internal("radix aggregate table identity space exhausted"))?;
        Ok(Self(
            NonZeroU64::new(identity).expect("radix identity starts non-zero"),
        ))
    }
}

/// Move-only proof that a group lookup populated the address vector for one
/// batch. Radix lookups additionally carry the table identity and exact
/// routing epoch that own the row permutation needed by the following update.
///
/// The fields and constructors deliberately stay private: callers can only
/// obtain this capability by completing a lookup, and consuming APIs prevent
/// accidentally updating from stale or never-populated addresses.
#[must_use = "a completed group lookup must authorize its corresponding update"]
#[derive(Debug)]
pub(crate) struct AggregateGroupLookup {
    new_group_count: usize,
    radix_owner: Option<RadixTableIdentity>,
    radix_routing_epoch: Option<RadixRoutingEpoch>,
}

impl AggregateGroupLookup {
    fn flat(new_group_count: usize) -> Self {
        Self {
            new_group_count,
            radix_owner: None,
            radix_routing_epoch: None,
        }
    }

    fn radix(
        new_group_count: usize,
        owner: RadixTableIdentity,
        routing_epoch: RadixRoutingEpoch,
    ) -> Self {
        Self {
            new_group_count,
            radix_owner: Some(owner),
            radix_routing_epoch: Some(routing_epoch),
        }
    }

    pub(crate) fn new_group_count(&self) -> usize {
        self.new_group_count
    }

    fn into_radix_epoch(self, owner: RadixTableIdentity) -> Result<RadixRoutingEpoch> {
        let token_owner = self.radix_owner.ok_or_else(|| {
            paro_error::internal("flat aggregate lookup token used for a radix update")
        })?;
        if token_owner != owner {
            return Err(paro_error::internal(format!(
                "radix aggregate lookup token belongs to another table: token_owner={token_owner:?}, table_owner={owner:?}"
            )));
        }
        self.radix_routing_epoch.ok_or_else(|| {
            paro_error::internal("radix aggregate lookup token has no routing epoch")
        })
    }

    /// Authorize a custom update that dereferences state addresses directly.
    /// Ordered aggregates are planned as flat tables, so accepting a radix
    /// token here would silently leave its routing epoch unconsumed.
    pub(crate) fn consume_for_flat_custom_update(self) -> Result<()> {
        if self.radix_owner.is_some() || self.radix_routing_epoch.is_some() {
            return Err(paro_error::internal(
                "radix aggregate lookup token used for a flat custom update",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AggregateHashTableGrowthPlan {
    partition_rows: Box<[usize]>,
    partition_varlen_bytes: Box<[usize]>,
    prepared_radix_route: Option<PreparedRadixGroupRoute>,
}

/// Move-only proof that growth planning has already hashed and routed one
/// radix input batch. The later lookup consumes this capability instead of
/// repeating that work, and the owner/epoch pair rejects stale or cross-table
/// reuse after any intervening route.
#[derive(Debug)]
struct PreparedRadixGroupRoute {
    owner: RadixTableIdentity,
    epoch: RadixRoutingEpoch,
    row_count: usize,
    lookup_contract: IncomingHashContract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateHashTableLayout {
    Flat,
    Radix { partition_bits: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AggregateHashTableConfig {
    layout: AggregateHashTableLayout,
    hash_contract: AggregateHashContract,
    capacity_hint: HashTableCapacityHint,
}

impl AggregateHashTableConfig {
    pub(crate) fn new(
        layout: AggregateHashTableLayout,
        hash_contract: AggregateHashContract,
        capacity_hint: HashTableCapacityHint,
    ) -> Self {
        Self {
            layout,
            hash_contract,
            capacity_hint,
        }
    }
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
    hash_contract: AggregateHashContract,
    scan_output_types: Vec<LogicalType>,
    partition_bits: usize,
    table_identity: RadixTableIdentity,
    partitions: Box<[Mutex<Option<GroupedAggregateHashTable>>]>,
    // Observations owned by the radix wrapper (currently partition skew) are
    // independent of the child tables handed to merge tasks. Keep them alive
    // across disassembly/reassembly just like the child-owned observations.
    hash_runtime_stats: AggregateHashRuntimeStats,
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
            hash_contract,
            partition_bits,
            partitions,
            hash_runtime_stats,
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
        if partitions
            .iter()
            .any(|partition| partition.routing_hash_contract() != hash_contract.routing())
        {
            return Err(paro_error::internal(
                "radix aggregate target partitions have inconsistent routing contracts",
            ));
        }
        Ok(Self {
            group_types,
            hash_contract,
            scan_output_types,
            partition_bits,
            // Reassembly installs a fresh routing scratch. Give that scratch a
            // fresh identity as well so capabilities issued before dismantling
            // can never become valid again when its epoch restarts from zero.
            table_identity: RadixTableIdentity::try_new()?,
            partitions: partitions
                .into_iter()
                .map(|partition| Mutex::new(Some(partition)))
                .collect(),
            hash_runtime_stats,
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
        if table.routing_hash_contract() != self.hash_contract.routing() {
            return Err(paro_error::internal(format!(
                "radix aggregate partition routing contract mismatch: expected={:?}, actual={:?}",
                self.hash_contract.routing(),
                table.routing_hash_contract()
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
                hash_contract: self.hash_contract,
                partition_bits: self.partition_bits,
                partition_mask: partitions.len() - 1,
                table_identity: self.table_identity,
                partitions,
                scratch: RadixRoutingScratch::default(),
                hash_runtime_stats: self.hash_runtime_stats,
            },
        ))
    }

    /// Attach observations transferred from source wrappers that were
    /// dismantled into independently owned partitions.
    pub(crate) fn merge_hash_runtime_stats(&mut self, stats: AggregateHashRuntimeStats) {
        self.hash_runtime_stats.merge(stats);
    }
}

impl AggregateHashTable {
    pub(crate) fn growth_plan(
        &mut self,
        groups: &Chunk,
        hash_scratch: &mut GroupHashScratch,
    ) -> Result<(AggregateHashTableGrowthPlan, HashTableGrowthRequirement)> {
        let (partition_rows, partition_varlen_bytes, prepared_radix_route) = match self {
            Self::Flat(table) => (
                vec![groups.size()],
                vec![table.varlen_bytes_upper_bound(groups, None)?],
                None,
            ),
            Self::Radix(table) => {
                let hashes = table.hash_groups_with_scratch(groups, hash_scratch)?;
                let prepared_route = table.prepare_group_route(groups, hashes)?;
                let rows = table.scratch.counts.clone();
                let mut varlen_bytes = Vec::with_capacity(table.partitions.len());
                for (partition_idx, partition) in table.partitions.iter().enumerate() {
                    let (start, end) = table.scratch.partition_range(partition_idx)?;
                    varlen_bytes.push(partition.varlen_bytes_upper_bound(
                        groups,
                        Some(&table.scratch.rows_by_partition[start..end]),
                    )?);
                }
                (rows, varlen_bytes, Some(prepared_route))
            }
        };
        let mut requirement = HashTableGrowthRequirement::default();
        match self {
            Self::Flat(table) => {
                requirement =
                    table.growth_requirement(partition_rows[0], partition_varlen_bytes[0])?;
            }
            Self::Radix(table) => {
                for ((partition, rows), varlen_bytes) in table
                    .partitions
                    .iter()
                    .zip(partition_rows.iter())
                    .zip(partition_varlen_bytes.iter())
                {
                    let current = partition.growth_requirement(*rows, *varlen_bytes)?;
                    requirement.persistent_bytes = requirement
                        .persistent_bytes
                        .checked_add(current.persistent_bytes)
                        .ok_or_else(|| {
                            paro_error::internal("radix aggregate persistent growth overflow")
                        })?;
                    requirement.overlap_bytes =
                        requirement.overlap_bytes.max(current.overlap_bytes);
                }
            }
        }
        Ok((
            AggregateHashTableGrowthPlan {
                partition_rows: partition_rows.into_boxed_slice(),
                partition_varlen_bytes: partition_varlen_bytes.into_boxed_slice(),
                prepared_radix_route,
            },
            requirement,
        ))
    }

    pub(crate) fn prepare_growth(
        &mut self,
        plan: &AggregateHashTableGrowthPlan,
        reservation: &MemoryGrant,
    ) -> Result<()> {
        match self {
            Self::Flat(table) => {
                if plan.prepared_radix_route.is_some() {
                    return Err(paro_error::internal(
                        "flat aggregate received a radix growth capability",
                    ));
                }
                let ([rows], [varlen_bytes]) = (
                    plan.partition_rows.as_ref(),
                    plan.partition_varlen_bytes.as_ref(),
                ) else {
                    return Err(paro_error::internal(
                        "flat aggregate growth plan has the wrong partition count",
                    ));
                };
                table.prepare_growth(*rows, *varlen_bytes, reservation)
            }
            Self::Radix(table) => {
                if plan.partition_rows.len() != table.partitions.len()
                    || plan.partition_varlen_bytes.len() != table.partitions.len()
                {
                    return Err(paro_error::internal(
                        "radix aggregate growth plan has the wrong partition count",
                    ));
                }
                for ((partition, rows), varlen_bytes) in table
                    .partitions
                    .iter_mut()
                    .zip(plan.partition_rows.iter())
                    .zip(plan.partition_varlen_bytes.iter())
                {
                    partition.prepare_growth(*rows, *varlen_bytes, reservation)?;
                }
                Ok(())
            }
        }
    }

    /// Split a finalized table into independently scannable ownership units,
    /// transferring all table-owned runtime observations with them.
    pub(crate) fn into_scan_partitions(mut self) -> AggregateHashTablePartitionBundle {
        let hash_runtime_stats = self.take_hash_runtime_stats();
        match self {
            Self::Flat(table) => AggregateHashTablePartitionBundle {
                partitions: vec![Self::Flat(table)],
                hash_runtime_stats,
            },
            Self::Radix(table) => AggregateHashTablePartitionBundle {
                partitions: table
                    .into_partitions()
                    .into_iter()
                    .map(Self::Flat)
                    .collect(),
                hash_runtime_stats,
            },
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

    pub(crate) fn fuse_disjoint_filter_group(&mut self, filter_inputs: &[usize]) -> bool {
        match self {
            Self::Flat(table) => table.fuse_disjoint_filter_group(filter_inputs),
            Self::Radix(table) => {
                let mut changed = false;
                for partition in &mut table.partitions {
                    changed |= partition.fuse_disjoint_filter_group(filter_inputs);
                }
                changed
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
        let hash_contract = AggregateHashContract::try_new(group_types.len(), group_types.len())?;
        Self::new_configured(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            allocator,
            memory,
            AggregateHashTableConfig::new(
                AggregateHashTableLayout::Flat,
                hash_contract,
                HashTableCapacityHint::default(),
            ),
        )
    }

    pub(crate) fn new_configured(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        config: AggregateHashTableConfig,
    ) -> Result<Self> {
        if config.hash_contract.key_width() != group_types.len() {
            return Err(paro_error::internal(format!(
                "Aggregate hash table config/key mismatch: contract={}, groups={}",
                config.hash_contract.key_width(),
                group_types.len()
            )));
        }
        match config.layout {
            AggregateHashTableLayout::Flat => {
                Ok(Self::Flat(GroupedAggregateHashTable::new_configured(
                    group_types,
                    aggregate_objects,
                    aggregate_inputs,
                    allocator,
                    memory,
                    GroupedAggregateHashTableConfig::estimated(
                        config.hash_contract,
                        config.capacity_hint,
                    ),
                )?))
            }
            AggregateHashTableLayout::Radix { partition_bits } => {
                // Ordinary Radix owns rows by the full key. That hash is
                // already available and dominates any prefix lookup, so keep
                // adaptive prefix lookup exclusively in flat tables. DISTINCT
                // has a different exact contract: ownership by output-group
                // prefix and full-key lookup, which remains unchanged here.
                let radix_contract = if config.hash_contract.routing_is_full_key() {
                    config.hash_contract.with_full_key_lookup()
                } else {
                    config.hash_contract
                };
                Ok(Self::Radix(
                    RadixPartitionedAggregateHashTable::new_configured(
                        group_types,
                        aggregate_objects,
                        aggregate_inputs,
                        partition_bits,
                        allocator,
                        memory,
                        radix_contract,
                        config.capacity_hint,
                    )?,
                ))
            }
        }
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
        let hash_contract = AggregateHashContract::try_new(group_types.len(), group_types.len())?;
        Self::new_configured(
            group_types,
            aggregate_objects,
            aggregate_inputs,
            allocator,
            memory,
            AggregateHashTableConfig::new(
                AggregateHashTableLayout::Radix { partition_bits },
                hash_contract,
                HashTableCapacityHint::default(),
            ),
        )
    }

    pub fn hash_groups(&self, groups: &Chunk) -> Result<Vector> {
        match self {
            Self::Flat(table) => table.hash_routing_groups(groups),
            Self::Radix(table) => table.hash_groups(groups),
        }
    }

    pub(crate) fn routing_hash_contract(&self) -> RoutingHashContract {
        match self {
            Self::Flat(table) => table.routing_hash_contract(),
            Self::Radix(table) => table.hash_contract.routing(),
        }
    }

    pub(crate) fn take_hash_runtime_stats(&mut self) -> AggregateHashRuntimeStats {
        match self {
            Self::Flat(table) => table.take_hash_runtime_stats(),
            Self::Radix(table) => table.take_hash_runtime_stats(),
        }
    }

    #[cfg(test)]
    pub(crate) fn merge_hash_runtime_stats(&mut self, stats: AggregateHashRuntimeStats) {
        match self {
            Self::Flat(table) => table.merge_hash_runtime_stats(stats),
            Self::Radix(table) => table.hash_runtime_stats.merge(stats),
        }
    }

    pub(crate) fn find_or_create_groups_with_scratch(
        &mut self,
        groups: &Chunk,
        scratch: &mut GroupHashScratch,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<AggregateGroupLookup> {
        match self {
            Self::Flat(table) => {
                let hashes = table.hash_groups_with_scratch(groups, scratch)?;
                let new_group_count =
                    table.find_or_create_groups(groups, hashes, addresses, new_groups)?;
                Ok(AggregateGroupLookup::flat(new_group_count))
            }
            Self::Radix(table) => {
                let hashes = table.hash_groups_with_scratch(groups, scratch)?;
                table.find_or_create_groups_hashed(groups, hashes, addresses, new_groups)
            }
        }
    }

    /// Consume the route prepared by [`Self::growth_plan`]. Flat tables have
    /// no routing work to reuse and follow their ordinary scratch path.
    pub(crate) fn find_or_create_groups_with_growth_plan(
        &mut self,
        groups: &Chunk,
        hash_scratch: &mut GroupHashScratch,
        growth_plan: &mut AggregateHashTableGrowthPlan,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<AggregateGroupLookup> {
        match self {
            Self::Flat(table) => {
                if growth_plan.prepared_radix_route.is_some() {
                    return Err(paro_error::internal(
                        "flat aggregate received a radix growth capability",
                    ));
                }
                let hashes = table.hash_groups_with_scratch(groups, hash_scratch)?;
                let new_group_count =
                    table.find_or_create_groups(groups, hashes, addresses, new_groups)?;
                Ok(AggregateGroupLookup::flat(new_group_count))
            }
            Self::Radix(table) => {
                let prepared = growth_plan.prepared_radix_route.take().ok_or_else(|| {
                    paro_error::internal("radix aggregate growth plan has no prepared route")
                })?;
                table.find_or_create_groups_prepared(groups, prepared, addresses, new_groups)
            }
        }
    }

    /// Try a runtime-observed exact index for a compact integer group domain.
    /// Radix ownership still requires hashes, so only flat tables participate.
    pub(crate) fn try_find_or_create_adaptive_integer_groups(
        &mut self,
        groups: &Chunk,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<Option<AggregateGroupLookup>> {
        match self {
            Self::Flat(table) => {
                if table
                    .try_find_or_create_adaptive_integer_groups(groups, addresses, new_groups)?
                {
                    Ok(Some(AggregateGroupLookup::flat(new_groups.len())))
                } else {
                    Ok(None)
                }
            }
            Self::Radix(_) => Ok(None),
        }
    }

    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        match self {
            Self::Flat(table) => table
                .find_or_create_groups_with_routing_hashes(groups, hashes, addresses, new_groups),
            Self::Radix(table) => {
                table.find_or_create_groups(groups, hashes, addresses, new_groups)
            }
        }
    }

    /// Replay hashes are serialized under the immutable routing contract, not
    /// necessarily a flat table's current adaptive lookup contract.
    pub(crate) fn find_or_create_groups_with_routing_hashes(
        &mut self,
        groups: &Chunk,
        hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        match self {
            Self::Flat(table) => table
                .find_or_create_groups_with_routing_hashes(groups, hashes, addresses, new_groups),
            Self::Radix(table) => {
                table.find_or_create_groups(groups, hashes, addresses, new_groups)
            }
        }
    }

    /// Insert a complete DISTINCT key while preserving its explicit output
    /// group partitioning policy. The semantic wrapper prevents lookup and
    /// partition vectors from being exchanged at the call boundary.
    pub(crate) fn find_or_create_distinct_groups(
        &mut self,
        groups: &Chunk,
        hashes: DistinctHashVectors<'_>,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        match self {
            Self::Flat(table) => {
                table.find_or_create_groups(groups, hashes.lookup(), addresses, new_groups)
            }
            Self::Radix(table) => table.find_or_create_groups_partitioned(
                groups,
                hashes.lookup(),
                hashes.partition(),
                IncomingHashContract::Lookup(table.hash_contract.lookup()),
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

    /// Update the batch whose group lookup immediately preceded this call.
    ///
    /// Radix lookup owns the canonical row-to-partition permutation. Keeping
    /// that permutation live across the lookup/update boundary avoids routing
    /// the same hash vector twice while the consumable epoch prevents stale
    /// scratch from being reused by a later batch.
    pub(crate) fn update_aggregates_after_group_lookup(
        &mut self,
        lookup: AggregateGroupLookup,
        payload: &Chunk,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        match self {
            Self::Flat(table) => {
                lookup.consume_for_flat_custom_update()?;
                table.update_aggregates(payload, addresses, filter)
            }
            Self::Radix(table) => {
                let routing_epoch = lookup.into_radix_epoch(table.table_identity)?;
                table.update_aggregates_after_group_lookup(
                    routing_epoch,
                    payload,
                    addresses,
                    filter,
                )
            }
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

    pub(crate) fn try_update_direct_aggregates(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
    ) -> Result<bool> {
        match self {
            Self::Flat(table) => table.try_update_direct_aggregates(payload, addresses),
            // A radix table owns one state domain per partition. Its routed
            // direct path needs a partition-aware program rather than
            // borrowing an arbitrary child program.
            Self::Radix(_) => Ok(false),
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

    /// Consume several completed source tables in one bulk merge.
    ///
    /// Aggregate finalization owns every source fragment at this point. Making
    /// that ownership explicit lets flat partitions reserve once for the full
    /// merge frontier instead of repeatedly growing for individual workers.
    pub(crate) fn combine_sources(&mut self, sources: Vec<Self>) -> Result<()> {
        match self {
            Self::Flat(target) => {
                let mut flat_sources = Vec::with_capacity(sources.len());
                for source in sources {
                    let Self::Flat(source) = source else {
                        return Err(paro_error::internal(
                            "cannot bulk-combine radix aggregate source into flat target",
                        ));
                    };
                    flat_sources.push(source);
                }
                target.combine_owned(flat_sources)
            }
            Self::Radix(target) => {
                let mut radix_sources = Vec::with_capacity(sources.len());
                for source in sources {
                    let Self::Radix(source) = source else {
                        return Err(paro_error::internal(
                            "cannot bulk-combine flat aggregate source into radix target",
                        ));
                    };
                    radix_sources.push(source);
                }
                target.combine_sources(radix_sources)
            }
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
    hash_contract: AggregateHashContract,
    partition_bits: usize,
    partition_mask: usize,
    table_identity: RadixTableIdentity,
    partitions: Vec<GroupedAggregateHashTable>,
    scratch: RadixRoutingScratch,
    hash_runtime_stats: AggregateHashRuntimeStats,
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
    routing_epoch: RadixRoutingEpoch,
    group_lookup_epoch: Option<RadixRoutingEpoch>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RadixRoutingEpoch(u64);

impl RadixRoutingEpoch {
    fn try_advance(&mut self) -> Result<Self> {
        self.0 = self
            .0
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("radix aggregate routing epoch space exhausted"))?;
        Ok(*self)
    }
}

impl RadixPartitionedAggregateHashTable {
    fn into_partitions(self) -> Vec<GroupedAggregateHashTable> {
        self.partitions
    }

    fn new_configured(
        group_types: Vec<LogicalType>,
        aggregate_objects: Vec<AggregateObject>,
        aggregate_inputs: Vec<Vec<usize>>,
        partition_bits: usize,
        allocator: Arc<dyn Allocator>,
        memory: MemoryAccountingContext,
        hash_contract: AggregateHashContract,
        capacity_hint: HashTableCapacityHint,
    ) -> Result<Self> {
        if hash_contract.key_width() != group_types.len() {
            return Err(paro_error::internal(format!(
                "Radix aggregate hash contract/key mismatch: contract={}, groups={}",
                hash_contract.key_width(),
                group_types.len()
            )));
        }
        let partition_count = radix_partition_count(partition_bits)?;
        let partition_hint = capacity_hint.divided_across(partition_count);
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(GroupedAggregateHashTable::new_configured(
                group_types.clone(),
                aggregate_objects.clone(),
                aggregate_inputs.clone(),
                allocator.clone(),
                memory.clone(),
                GroupedAggregateHashTableConfig::estimated(hash_contract, partition_hint),
            )?);
        }
        Ok(Self {
            group_types,
            hash_contract,
            partition_bits,
            partition_mask: partition_count - 1,
            table_identity: RadixTableIdentity::try_new()?,
            partitions,
            scratch: RadixRoutingScratch::default(),
            hash_runtime_stats: AggregateHashRuntimeStats::default(),
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
        if groups.column_count() != self.group_types.len() {
            return Err(paro_error::internal(format!(
                "Radix aggregate group width mismatch: expected={}, actual={}",
                self.group_types.len(),
                groups.column_count()
            )));
        }
        hash_group_columns(groups)
    }

    fn hash_groups_with_scratch<'a>(
        &self,
        groups: &Chunk,
        scratch: &'a mut GroupHashScratch,
    ) -> Result<AggregateHashVectors<'a>> {
        if groups.column_count() != self.group_types.len() {
            return Err(paro_error::internal(format!(
                "Radix aggregate group width mismatch: expected={}, actual={}",
                self.group_types.len(),
                groups.column_count()
            )));
        }
        scratch.hash_aggregate(groups, self.hash_contract)
    }

    /// Route under the immutable ownership contract and retain the worst
    /// observed imbalance. Lookup fallback is deliberately independent:
    /// changing ownership after groups exist would require state migration and
    /// could diverge across independently built worker-local tables.
    fn route_hashes_and_observe(
        &mut self,
        groups: &Chunk,
        partition_hashes: &Vector,
        lookup_hashes: &Vector,
    ) -> Result<()> {
        self.scratch.route_hashes(
            self.partition_bits,
            self.partition_mask,
            self.partitions.len(),
            partition_hashes,
            lookup_hashes,
            groups.size(),
        )?;
        let row_count = groups.size();
        let partition_count = self.partitions.len();
        if row_count != 0 && partition_count != 0 {
            let peak = self.scratch.counts.iter().copied().max().unwrap_or(0);
            let skew_percent = peak
                .saturating_mul(partition_count)
                .saturating_mul(100)
                .div_ceil(row_count) as u64;
            self.hash_runtime_stats.max_radix_partition_skew_percent = self
                .hash_runtime_stats
                .max_radix_partition_skew_percent
                .max(skew_percent);
        }
        Ok(())
    }

    fn prepare_group_route(
        &mut self,
        groups: &Chunk,
        hashes: AggregateHashVectors<'_>,
    ) -> Result<PreparedRadixGroupRoute> {
        let (lookup_hashes, lookup_contract) = hashes.lookup();
        let (routing_hashes, _) = hashes.routing();
        self.route_hashes_and_observe(groups, routing_hashes, lookup_hashes)?;
        Ok(PreparedRadixGroupRoute {
            owner: self.table_identity,
            epoch: self.scratch.routing_epoch,
            row_count: groups.size(),
            lookup_contract: IncomingHashContract::Lookup(lookup_contract),
        })
    }

    fn take_hash_runtime_stats(&mut self) -> AggregateHashRuntimeStats {
        let mut stats = std::mem::take(&mut self.hash_runtime_stats);
        for partition in &mut self.partitions {
            stats.merge(partition.take_hash_runtime_stats());
        }
        stats
    }

    pub fn find_or_create_groups(
        &mut self,
        groups: &Chunk,
        routing_hashes: &Vector,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        if self.hash_contract.lookup_is_prefix() {
            let lookup_hashes =
                hash_group_columns_prefix(groups, self.hash_contract.lookup().width())?;
            self.find_or_create_groups_partitioned(
                groups,
                &lookup_hashes,
                routing_hashes,
                IncomingHashContract::Lookup(self.hash_contract.lookup()),
                addresses,
                new_groups,
            )
        } else {
            self.find_or_create_groups_partitioned(
                groups,
                routing_hashes,
                routing_hashes,
                IncomingHashContract::Lookup(self.hash_contract.lookup()),
                addresses,
                new_groups,
            )
        }
    }

    fn find_or_create_groups_hashed(
        &mut self,
        groups: &Chunk,
        hashes: AggregateHashVectors<'_>,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<AggregateGroupLookup> {
        let (lookup_hashes, lookup_contract) = hashes.lookup();
        let (routing_hashes, _) = hashes.routing();
        let new_group_count = self.find_or_create_groups_partitioned(
            groups,
            lookup_hashes,
            routing_hashes,
            IncomingHashContract::Lookup(lookup_contract),
            addresses,
            new_groups,
        )?;
        let routing_epoch = self.scratch.group_lookup_epoch.ok_or_else(|| {
            paro_error::internal("radix group lookup completed without a routing epoch")
        })?;
        Ok(AggregateGroupLookup::radix(
            new_group_count,
            self.table_identity,
            routing_epoch,
        ))
    }

    fn find_or_create_groups_prepared(
        &mut self,
        groups: &Chunk,
        prepared: PreparedRadixGroupRoute,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<AggregateGroupLookup> {
        if prepared.owner != self.table_identity
            || prepared.epoch != self.scratch.routing_epoch
            || prepared.row_count != groups.size()
            || self.scratch.group_lookup_epoch.is_some()
        {
            return Err(paro_error::internal(format!(
                "radix aggregate prepared route is stale: token_owner={:?}, table_owner={:?}, token_epoch={:?}, current_epoch={:?}, token_rows={}, groups={}, lookup_epoch={:?}",
                prepared.owner,
                self.table_identity,
                prepared.epoch,
                self.scratch.routing_epoch,
                prepared.row_count,
                groups.size(),
                self.scratch.group_lookup_epoch,
            )));
        }
        let new_group_count = self.find_or_create_groups_from_current_routing(
            groups,
            prepared.lookup_contract,
            addresses,
            new_groups,
        )?;
        Ok(AggregateGroupLookup::radix(
            new_group_count,
            self.table_identity,
            prepared.epoch,
        ))
    }

    fn find_or_create_groups_partitioned(
        &mut self,
        groups: &Chunk,
        lookup_hashes: &Vector,
        partition_hashes: &Vector,
        lookup_contract: IncomingHashContract,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        validate_hashes(lookup_hashes, groups.size())?;
        validate_hashes(partition_hashes, groups.size())?;
        validate_address_capacity(addresses, groups.size())?;

        self.route_hashes_and_observe(groups, partition_hashes, lookup_hashes)?;
        self.find_or_create_groups_from_current_routing(
            groups,
            lookup_contract,
            addresses,
            new_groups,
        )
    }

    fn find_or_create_groups_from_current_routing(
        &mut self,
        groups: &Chunk,
        lookup_contract: IncomingHashContract,
        addresses: &mut Vector,
        new_groups: &mut SelectionVector,
    ) -> Result<usize> {
        self.scratch.mark_group_lookup_route();

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
            if partition.lookup_hash_contract().width() != lookup_contract.width() {
                return Err(paro_error::internal(format!(
                    "radix child lookup contract diverged: child={:?}, incoming={lookup_contract:?}",
                    partition.lookup_hash_contract()
                )));
            }
            partition.find_or_create_groups_selected(
                groups,
                &scratch.rows_by_partition[start..end],
                &scratch.hashes_by_partition[start..end],
                lookup_contract,
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

    fn update_aggregates_after_group_lookup(
        &mut self,
        routing_epoch: RadixRoutingEpoch,
        payload: &Chunk,
        addresses: &Vector,
        filter: Option<&SelectionVector>,
    ) -> Result<()> {
        if filter.is_some() {
            return Err(paro_error::internal(
                "Radix partitioned aggregate hash table does not support filtered updates directly"
                    .to_string(),
            ));
        }
        if addresses.len() < payload.size() {
            return Err(paro_error::internal(format!(
                "Address vector too small for radix aggregate update: addresses={} payload_rows={}",
                addresses.len(),
                payload.size()
            )));
        }
        self.scratch
            .consume_group_lookup_route(routing_epoch, payload.size())?;
        if payload.size() == 0 {
            return Ok(());
        }
        self.update_aggregates_from_current_routing(payload, addresses)
    }

    fn update_aggregates_from_current_routing(
        &mut self,
        payload: &Chunk,
        addresses: &Vector,
    ) -> Result<()> {
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
            || self.hash_contract.routing() != other.hash_contract.routing()
            || self.group_types != other.group_types
            || self.partitions.len() != other.partitions.len()
        {
            return Err(paro_error::internal(format!(
                "Cannot combine radix aggregate hash tables with different layouts: \
bits {}/{} routing {:?}/{:?} partitions {}/{} group_types {:?}/{:?}",
                self.partition_bits,
                other.partition_bits,
                self.hash_contract.routing(),
                other.hash_contract.routing(),
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
        self.hash_runtime_stats
            .merge(std::mem::take(&mut other.hash_runtime_stats));
        Ok(())
    }

    fn combine_sources(&mut self, sources: Vec<Self>) -> Result<()> {
        for source in &sources {
            if self.partition_bits != source.partition_bits
                || self.hash_contract.routing() != source.hash_contract.routing()
                || self.group_types != source.group_types
                || self.partitions.len() != source.partitions.len()
            {
                return Err(paro_error::internal(format!(
                    "Cannot bulk-combine radix aggregate hash tables with different layouts: \
bits {}/{} routing {:?}/{:?} partitions {}/{} group_types {:?}/{:?}",
                    self.partition_bits,
                    source.partition_bits,
                    self.hash_contract.routing(),
                    source.hash_contract.routing(),
                    self.partitions.len(),
                    source.partitions.len(),
                    self.group_types,
                    source.group_types
                )));
            }
        }

        let mut source_runtime_stats = AggregateHashRuntimeStats::default();
        let mut sources_by_partition = (0..self.partitions.len())
            .map(|_| Vec::with_capacity(sources.len()))
            .collect::<Vec<_>>();
        for source in sources {
            source_runtime_stats.merge(source.hash_runtime_stats);
            for (partition_idx, partition) in source.partitions.into_iter().enumerate() {
                sources_by_partition[partition_idx].push(partition);
            }
        }
        for (target, sources) in self.partitions.iter_mut().zip(sources_by_partition) {
            target.combine_owned(sources)?;
        }
        self.hash_runtime_stats.merge(source_runtime_stats);
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
        self.routing_epoch.try_advance()?;
        self.group_lookup_epoch = None;
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

    fn mark_group_lookup_route(&mut self) {
        self.group_lookup_epoch = Some(self.routing_epoch);
    }

    fn consume_group_lookup_route(
        &mut self,
        expected_epoch: RadixRoutingEpoch,
        expected_rows: usize,
    ) -> Result<()> {
        let epoch = self.group_lookup_epoch.ok_or_else(|| {
            paro_error::internal(
                "radix aggregate update has no unconsumed group-lookup routing epoch",
            )
        })?;
        if epoch != expected_epoch
            || epoch != self.routing_epoch
            || self.rows_by_partition.len() != expected_rows
        {
            return Err(paro_error::internal(format!(
                "radix aggregate group-lookup routing is stale: token_epoch={expected_epoch:?}, lookup_epoch={epoch:?}, current_epoch={:?}, routed_rows={}, payload_rows={expected_rows}",
                self.routing_epoch,
                self.rows_by_partition.len()
            )));
        }
        self.group_lookup_epoch = None;
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

    #[test]
    fn radix_normalizes_correlated_prefix_hint_to_full_key_contract() {
        let allocator = test_allocator();
        let row_count = 512usize;
        let leading = vec![11i32; row_count];
        let suffix = (0..row_count).map(|row| row as i32).collect::<Vec<_>>();
        let groups = Chunk::from_vectors(
            vec![
                test_i32_vector_with_allocator(&leading, allocator.clone()),
                test_i32_vector_with_allocator(&suffix, allocator.clone()),
            ],
            allocator.clone(),
        );
        let hash_contract = AggregateHashContract::try_new(2, 1).expect("hash contract");
        let mut table = AggregateHashTable::new_configured(
            vec![LogicalType::Integer, LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            allocator,
            MemoryAccountingContext::detached(
                paro_common::allocator::MemoryTag::HashTable,
                MemoryAccountingClass::Revocable,
            ),
            AggregateHashTableConfig::new(
                AggregateHashTableLayout::Radix { partition_bits: 2 },
                hash_contract,
                HashTableCapacityHint::default(),
            ),
        )
        .expect("radix prefix table");

        let prefix_hashes = hash_group_columns_prefix(&groups, 1).expect("prefix hashes");
        assert!(prefix_hashes
            .as_slice::<u64>()
            .windows(2)
            .all(|pair| pair[0] == pair[1]));
        let routing_hashes = table.hash_groups(&groups).expect("routing hashes");
        assert!(routing_hashes
            .as_slice::<u64>()
            .windows(2)
            .any(|pair| pair[0] != pair[1]));
        let mut hash_scratch =
            GroupHashScratch::try_new(row_count, groups.allocator().clone()).expect("hash scratch");
        {
            let AggregateHashTable::Radix(radix) = &table else {
                panic!("expected radix table");
            };
            let hashes = radix
                .hash_groups_with_scratch(&groups, &mut hash_scratch)
                .expect("typed radix hashes");
            let (lookup, lookup_contract) = hashes.lookup();
            let (routing, routing_contract) = hashes.routing();
            assert_eq!(lookup_contract.width(), 2);
            assert_eq!(routing_contract.width(), 2);
            assert!(std::ptr::eq(lookup, routing), "full/full must not snapshot");
        }
        let mut addresses = test_vector_with_capacity(LogicalType::BigInt, row_count);
        let mut new_groups = test_selection_with_capacity(row_count);
        assert_eq!(
            table
                .find_or_create_groups_with_scratch(
                    &groups,
                    &mut hash_scratch,
                    &mut addresses,
                    &mut new_groups,
                )
                .expect("insert skewed groups")
                .new_group_count(),
            row_count
        );
        assert_eq!(table.count(), row_count);
        assert_eq!(table.routing_hash_contract().width(), 2);
        let AggregateHashTable::Radix(radix) = &table else {
            panic!("expected radix table");
        };
        assert!(radix
            .partitions
            .iter()
            .all(|partition| partition.lookup_hash_contract().width() == 2));

        let stats = table.take_hash_runtime_stats();
        assert_eq!(stats.full_key_fallback_count, 0);
        assert_eq!(stats.max_prefix_probe_distance, 0);
        assert!(stats.max_radix_partition_skew_percent < 200);

        // Ownership and lookup both use the stable full-key hash. The planned
        // prefix remains only a flat-table hint.
        let second_hashes = table.hash_groups(&groups).expect("stable route hashes");
        assert_eq!(
            second_hashes.as_slice::<u64>(),
            routing_hashes.as_slice::<u64>()
        );
        assert_eq!(
            table
                .find_or_create_groups(&groups, &second_hashes, &mut addresses, &mut new_groups,)
                .expect("probe locally promoted partition"),
            0
        );
        assert_eq!(table.count(), row_count);
    }

    #[test]
    fn growth_plan_route_is_consumed_without_a_second_route() {
        let allocator = test_allocator();
        let groups = Chunk::from_vectors(
            vec![test_i32_vector_with_allocator(
                &[1, 2, 3, 4, 5],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut table = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            allocator.clone(),
        )
        .expect("radix table");
        let mut hash_scratch =
            GroupHashScratch::try_new(groups.size(), allocator).expect("hash scratch");
        let (mut growth_plan, _) = table
            .growth_plan(&groups, &mut hash_scratch)
            .expect("growth plan");
        let planned_epoch = match &table {
            AggregateHashTable::Radix(radix) => radix.scratch.routing_epoch,
            AggregateHashTable::Flat(_) => panic!("expected radix table"),
        };
        let mut addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = test_selection_with_capacity(groups.size());
        let lookup = table
            .find_or_create_groups_with_growth_plan(
                &groups,
                &mut hash_scratch,
                &mut growth_plan,
                &mut addresses,
                &mut new_groups,
            )
            .expect("consume prepared route");
        let consumed_epoch = match &table {
            AggregateHashTable::Radix(radix) => radix.scratch.routing_epoch,
            AggregateHashTable::Flat(_) => panic!("expected radix table"),
        };
        assert_eq!(consumed_epoch, planned_epoch, "lookup must not reroute");
        assert!(growth_plan.prepared_radix_route.is_none());
        table
            .update_aggregates_after_group_lookup(lookup, &groups, &addresses, None)
            .expect("prepared lookup token remains consumable");
    }

    #[test]
    fn radix_group_lookup_token_rejects_stale_routing_epoch() {
        let allocator = test_allocator();
        let groups = Chunk::from_vectors(
            vec![test_i32_vector_with_allocator(
                &[1, 2, 3],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut table = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            allocator.clone(),
        )
        .expect("radix table");
        let mut hash_scratch =
            GroupHashScratch::try_new(groups.size(), allocator).expect("hash scratch");
        let mut addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut new_groups = test_selection_with_capacity(groups.size());

        let stale = table
            .find_or_create_groups_with_scratch(
                &groups,
                &mut hash_scratch,
                &mut addresses,
                &mut new_groups,
            )
            .expect("first lookup");
        let current = table
            .find_or_create_groups_with_scratch(
                &groups,
                &mut hash_scratch,
                &mut addresses,
                &mut new_groups,
            )
            .expect("second lookup");

        let error = table
            .update_aggregates_after_group_lookup(stale, &groups, &addresses, None)
            .expect_err("superseded lookup token must be rejected");
        assert!(error.to_string().contains("routing is stale"));
        table
            .update_aggregates_after_group_lookup(current, &groups, &addresses, None)
            .expect("current lookup token remains consumable");
    }

    #[test]
    fn radix_group_lookup_token_rejects_another_table_at_the_same_epoch() {
        let allocator = test_allocator();
        let groups = Chunk::from_vectors(
            vec![test_i32_vector_with_allocator(
                &[1, 2, 3],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut first = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            allocator.clone(),
        )
        .expect("first radix table");
        let mut second = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            allocator.clone(),
        )
        .expect("second radix table");
        let mut first_scratch =
            GroupHashScratch::try_new(groups.size(), allocator.clone()).expect("first scratch");
        let mut second_scratch =
            GroupHashScratch::try_new(groups.size(), allocator).expect("second scratch");
        let mut first_addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut second_addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut first_new_groups = test_selection_with_capacity(groups.size());
        let mut second_new_groups = test_selection_with_capacity(groups.size());

        let first_lookup = first
            .find_or_create_groups_with_scratch(
                &groups,
                &mut first_scratch,
                &mut first_addresses,
                &mut first_new_groups,
            )
            .expect("first lookup");
        let second_lookup = second
            .find_or_create_groups_with_scratch(
                &groups,
                &mut second_scratch,
                &mut second_addresses,
                &mut second_new_groups,
            )
            .expect("second lookup");
        assert_eq!(
            first_lookup.radix_routing_epoch, second_lookup.radix_routing_epoch,
            "the owner check, rather than a coincidentally different epoch, must reject the token"
        );
        assert_ne!(first_lookup.radix_owner, second_lookup.radix_owner);

        let error = first
            .update_aggregates_after_group_lookup(second_lookup, &groups, &first_addresses, None)
            .expect_err("a lookup token from another table must be rejected");
        assert!(error.to_string().contains("belongs to another table"));
        first
            .update_aggregates_after_group_lookup(first_lookup, &groups, &first_addresses, None)
            .expect("the owning table must still accept its token");
    }

    #[test]
    fn radix_group_lookup_token_rejects_previous_table_incarnation() {
        let allocator = test_allocator();
        let groups = Chunk::from_vectors(
            vec![test_i32_vector_with_allocator(
                &[1, 2, 3],
                allocator.clone(),
            )],
            allocator.clone(),
        );
        let mut table = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            allocator.clone(),
        )
        .expect("radix table");
        let mut hash_scratch =
            GroupHashScratch::try_new(groups.size(), allocator).expect("hash scratch");
        let mut stale_addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut current_addresses = test_vector_with_capacity(LogicalType::BigInt, groups.size());
        let mut stale_new_groups = test_selection_with_capacity(groups.size());
        let mut current_new_groups = test_selection_with_capacity(groups.size());

        let stale = table
            .find_or_create_groups_with_scratch(
                &groups,
                &mut hash_scratch,
                &mut stale_addresses,
                &mut stale_new_groups,
            )
            .expect("lookup before dismantling");
        let stale_epoch = stale.radix_routing_epoch;
        let stale_owner = stale.radix_owner;

        let build = ConcurrentRadixAggregateBuild::try_new(table)
            .expect("dismantle radix table for concurrent assembly");
        let mut table = build.finish().expect("reassemble radix table");
        let current = table
            .find_or_create_groups_with_scratch(
                &groups,
                &mut hash_scratch,
                &mut current_addresses,
                &mut current_new_groups,
            )
            .expect("lookup after reassembly");
        assert_eq!(
            stale_epoch, current.radix_routing_epoch,
            "a fresh routing scratch deliberately restarts its local epoch"
        );
        assert_ne!(
            stale_owner, current.radix_owner,
            "a fresh routing scratch must have a distinct capability owner"
        );

        let error = table
            .update_aggregates_after_group_lookup(stale, &groups, &stale_addresses, None)
            .expect_err("a lookup token must not survive table reassembly");
        assert!(error.to_string().contains("belongs to another table"));
        table
            .update_aggregates_after_group_lookup(current, &groups, &current_addresses, None)
            .expect("the reassembled table must accept its own lookup token");
    }

    #[test]
    fn radix_routing_epoch_rejects_exhaustion() {
        let mut epoch = RadixRoutingEpoch(u64::MAX);
        let error = epoch
            .try_advance()
            .expect_err("routing epochs must never wrap and become reusable");

        assert!(error.to_string().contains("routing epoch space exhausted"));
        assert_eq!(epoch, RadixRoutingEpoch(u64::MAX));
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
        assert!(
            table
                .take_hash_runtime_stats()
                .max_radix_partition_skew_percent
                > 0,
            "disassembling and reassembling the radix table must preserve wrapper observations"
        );
        assert_eq!(drain_integer_group_table(&mut table), 6);
    }

    #[test]
    fn radix_bulk_combine_transfers_source_wrapper_observations() {
        let mut target = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            test_allocator(),
        )
        .expect("target table");
        let mut source = AggregateHashTable::new_radix(
            vec![LogicalType::Integer],
            Vec::new(),
            Vec::new(),
            2,
            test_allocator(),
        )
        .expect("source table");
        insert_integer_groups(&mut source, &[1, 2, 3]);

        target
            .combine_sources(vec![source])
            .expect("bulk combine radix source");
        assert!(
            target
                .take_hash_runtime_stats()
                .max_radix_partition_skew_percent
                > 0,
            "source wrapper observations must move with its partitions"
        );
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
                AggregateHashContract::try_new(1, 1)
                    .expect("projection contract")
                    .routing(),
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
