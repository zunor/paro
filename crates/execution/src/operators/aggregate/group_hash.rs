// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reusable vector hashing for grouped execution operators.

use std::num::NonZeroUsize;
use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::hash::{combine_hash, NULL_HASH};
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorOperations};

/// Number of leading key columns used by a flat aggregate lookup index.
///
/// This token is deliberately distinct from [`RoutingHashContract`]. Callers
/// cannot accidentally feed a routing width into a lookup path merely because
/// both widths happen to be represented by `usize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LookupHashContract(NonZeroUsize);

impl LookupHashContract {
    pub(crate) fn width(self) -> usize {
        self.0.get()
    }

    fn try_new(width: usize, key_width: usize) -> Result<Self> {
        if width == 0 || width > key_width {
            return Err(paro_error::internal(format!(
                "Invalid aggregate lookup hash contract: keys={key_width}, hashed={width}"
            )));
        }
        Ok(Self(
            NonZeroUsize::new(width).expect("validated non-zero width"),
        ))
    }

    fn from_routing(routing: RoutingHashContract) -> Self {
        Self(routing.0)
    }
}

/// Number of key columns hashed for immutable ownership, spill, and replay.
/// Ordinary aggregate routing covers the complete key; DISTINCT uses its
/// output-group prefix as a separate exact ownership contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingHashContract(NonZeroUsize);

impl RoutingHashContract {
    pub(crate) fn width(self) -> usize {
        self.0.get()
    }

    fn full_key(key_width: usize) -> Result<Self> {
        NonZeroUsize::new(key_width).map(Self).ok_or_else(|| {
            paro_error::internal("aggregate hash contract requires at least one key column")
        })
    }

    fn try_prefix(width: usize, key_width: usize) -> Result<Self> {
        if width == 0 || width > key_width {
            return Err(paro_error::internal(format!(
                "Invalid aggregate routing hash contract: keys={key_width}, hashed={width}"
            )));
        }
        Ok(Self(
            NonZeroUsize::new(width).expect("validated non-zero width"),
        ))
    }
}

/// The complete hash policy owned by one aggregate table.
///
/// Ordinary routing is immutable and full-key. Its flat-table lookup starts
/// from the optimizer's prefix hint and may only move one way to the routing
/// contract. DISTINCT instead owns an explicit output-group routing prefix
/// and always uses full-key lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregateHashContract {
    /// Ordinary grouped aggregation owns rows by the complete key. Lookup may
    /// begin with a speculative prefix and can only promote to that full key.
    Ordinary {
        full_key: RoutingHashContract,
        lookup: LookupHashContract,
    },
    /// DISTINCT owns all inputs for one output group in the same partition,
    /// while equality lookup always covers the complete `(groups, inputs)` key.
    Distinct {
        routing_prefix: RoutingHashContract,
        full_key: LookupHashContract,
    },
}

impl AggregateHashContract {
    pub(crate) fn try_new(key_width: usize, lookup_width: usize) -> Result<Self> {
        Ok(Self::Ordinary {
            full_key: RoutingHashContract::full_key(key_width)?,
            lookup: LookupHashContract::try_new(lookup_width, key_width)?,
        })
    }

    /// DISTINCT tables have a separate, exact ownership invariant: every
    /// `(group..., input...)` key is looked up in full, while all inputs for an
    /// output group must remain in that output group's finalization partition.
    /// A zero-width output group (global DISTINCT) deliberately uses the full
    /// key so collection remains parallel.
    pub(crate) fn for_distinct(key_width: usize, output_group_width: usize) -> Result<Self> {
        let routing_width = if output_group_width == 0 {
            key_width
        } else {
            output_group_width
        };
        Ok(Self::Distinct {
            routing_prefix: RoutingHashContract::try_prefix(routing_width, key_width)?,
            full_key: LookupHashContract::try_new(key_width, key_width)?,
        })
    }

    pub(crate) fn routing(self) -> RoutingHashContract {
        match self {
            Self::Ordinary { full_key, .. } => full_key,
            Self::Distinct { routing_prefix, .. } => routing_prefix,
        }
    }

    pub(crate) fn lookup(self) -> LookupHashContract {
        match self {
            Self::Ordinary { lookup, .. } => lookup,
            Self::Distinct { full_key, .. } => full_key,
        }
    }

    pub(crate) fn lookup_is_prefix(self) -> bool {
        matches!(self, Self::Ordinary { full_key, lookup } if lookup.width() < full_key.width())
    }

    pub(crate) fn routing_is_full_key(self) -> bool {
        matches!(self, Self::Ordinary { .. })
    }

    pub(crate) fn key_width(self) -> usize {
        match self {
            Self::Ordinary { full_key, .. } => full_key.width(),
            Self::Distinct { full_key, .. } => full_key.width(),
        }
    }

    pub(crate) fn promote_lookup_to_full(&mut self) -> bool {
        match self {
            Self::Ordinary { full_key, lookup } if lookup.width() < full_key.width() => {
                *lookup = LookupHashContract::from_routing(*full_key);
                true
            }
            Self::Ordinary { .. } | Self::Distinct { .. } => false,
        }
    }

    pub(crate) fn with_full_key_lookup(self) -> Self {
        match self {
            Self::Ordinary { full_key, .. } => Self::Ordinary {
                full_key,
                lookup: LookupHashContract::from_routing(full_key),
            },
            distinct @ Self::Distinct { .. } => distinct,
        }
    }
}

/// Hash vectors derived together from one aggregate contract.
///
/// The named accessors retain the semantic token next to its vector, removing
/// the former pair of same-typed `(lookup, partition)` arguments.
pub(crate) struct AggregateHashVectors<'a> {
    lookup: &'a Vector,
    routing: &'a Vector,
    lookup_contract: LookupHashContract,
    routing_contract: RoutingHashContract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncomingHashContract {
    Lookup(LookupHashContract),
    Routing(RoutingHashContract),
}

impl IncomingHashContract {
    pub(crate) fn width(self) -> usize {
        match self {
            Self::Lookup(contract) => contract.width(),
            Self::Routing(contract) => contract.width(),
        }
    }
}

impl<'a> AggregateHashVectors<'a> {
    pub(crate) fn lookup(&self) -> (&'a Vector, LookupHashContract) {
        (self.lookup, self.lookup_contract)
    }

    pub(crate) fn routing(&self) -> (&'a Vector, RoutingHashContract) {
        (self.routing, self.routing_contract)
    }
}

pub(crate) struct DistinctHashVectors<'a> {
    lookup: &'a Vector,
    partition: &'a Vector,
}

struct FullAndPrefixHashes<'a> {
    full: &'a Vector,
    prefix: &'a Vector,
}

impl<'a> DistinctHashVectors<'a> {
    pub(crate) fn lookup(&self) -> &'a Vector {
        self.lookup
    }

    pub(crate) fn partition(&self) -> &'a Vector {
        self.partition
    }
}

/// Hash every column in a group-key batch.
///
/// This allocation-owning entry point is used by spill paths that do not own
/// operator-local scratch. Steady-state aggregation should retain a
/// [`GroupHashScratch`] instead.
pub(crate) fn hash_group_columns(groups: &Chunk) -> Result<Vector> {
    hash_group_columns_prefix(groups, groups.column_count())
}

/// Hash the leading `prefix_width` columns of a group-key batch.
///
/// Equality in the aggregate table still compares the complete group key. The
/// prefix is solely the table's lookup hash contract.
pub(crate) fn hash_group_columns_prefix(groups: &Chunk, prefix_width: usize) -> Result<Vector> {
    let mut scratch = GroupHashScratch::try_new(groups.size().max(1), groups.allocator().clone())?;
    scratch.hash_prefix(groups, prefix_width)?;
    Ok(scratch.hashes)
}

/// Reusable vectors for hashing a group-key batch.
///
/// DISTINCT aggregation can route a full `(groups..., inputs...)` lookup key
/// by the hash of its group prefix. That keeps all values for one output group
/// in the same radix partition while retaining exact full-key lookup hashes.
#[derive(Debug)]
pub(crate) struct GroupHashScratch {
    hashes: Vector,
    partition_hashes: Vector,
    column_hashes: Vector,
}

impl GroupHashScratch {
    pub(crate) fn try_new(capacity: usize, allocator: Arc<dyn Allocator>) -> Result<Self> {
        let capacity = capacity.max(1);
        Ok(Self {
            hashes: Vector::try_new(LogicalType::UBigInt, capacity, allocator.clone())?,
            partition_hashes: Vector::try_new(LogicalType::UBigInt, capacity, allocator.clone())?,
            column_hashes: Vector::try_new(LogicalType::UBigInt, capacity, allocator)?,
        })
    }

    pub(crate) fn hash<'a>(&'a mut self, groups: &Chunk) -> Result<&'a Vector> {
        self.hash_prefix(groups, groups.column_count())
    }

    /// Hash an aggregate batch under its complete routing contract and current
    /// lookup contract in one column pass. When lookup uses a prefix, the
    /// prefix state is snapshotted before hashing the remaining routing key.
    pub(crate) fn hash_aggregate<'a>(
        &'a mut self,
        groups: &Chunk,
        contract: AggregateHashContract,
    ) -> Result<AggregateHashVectors<'a>> {
        if !contract.routing_is_full_key() {
            return Err(paro_error::internal(
                "ordinary aggregate hashing requires full-key routing",
            ));
        }
        if groups.column_count() != contract.key_width() {
            return Err(paro_error::internal(format!(
                "Aggregate hash contract/key mismatch: contract={}, columns={}",
                contract.key_width(),
                groups.column_count()
            )));
        }
        let lookup_contract = contract.lookup();
        let routing_contract = contract.routing();
        if lookup_contract.width() == routing_contract.width() {
            let hashes = self.hash_prefix(groups, routing_contract.width())?;
            return Ok(AggregateHashVectors {
                lookup: hashes,
                routing: hashes,
                lookup_contract,
                routing_contract,
            });
        }

        let hashes = self.hash_with_partition_prefix(groups, lookup_contract.width())?;
        Ok(AggregateHashVectors {
            lookup: hashes.prefix,
            routing: hashes.full,
            lookup_contract,
            routing_contract,
        })
    }

    pub(crate) fn hash_distinct<'a>(
        &'a mut self,
        keys: &Chunk,
        output_group_prefix_width: usize,
    ) -> Result<DistinctHashVectors<'a>> {
        let hashes = self.hash_with_partition_prefix(keys, output_group_prefix_width)?;
        Ok(DistinctHashVectors {
            lookup: hashes.full,
            partition: hashes.prefix,
        })
    }

    /// Hash a leading subset while retaining exact full-key comparison in the
    /// aggregate table. A high-cardinality prefix often provides the complete
    /// 64-bit distribution entropy, so hashing wide dependent suffixes only
    /// burns CPU without reducing collisions.
    pub(crate) fn hash_prefix<'a>(
        &'a mut self,
        groups: &Chunk,
        prefix_width: usize,
    ) -> Result<&'a Vector> {
        if groups.column_count() > 0 && prefix_width == 0 {
            return Err(paro_error::internal(
                "group hash prefix cannot be empty for a non-empty key",
            ));
        }
        if prefix_width > groups.column_count() {
            return Err(paro_error::internal(format!(
                "Group hash prefix exceeds key width: prefix={prefix_width}, columns={}",
                groups.column_count()
            )));
        }

        let count = groups.size();
        self.ensure_capacity(count, groups.allocator().clone())?;
        self.hashes.try_set_count(count)?;
        if count == 0 {
            return Ok(&self.hashes);
        }
        if groups.column_count() == 0 {
            self.hashes.as_mut_slice::<u64>()[..count].fill(NULL_HASH);
            return Ok(&self.hashes);
        }

        let first = groups
            .column(0)
            .ok_or_else(|| paro_error::internal("Missing first group key column while hashing"))?;
        VectorOperations::hash(first.as_ref(), &mut self.hashes, count)?;
        for column_idx in 1..prefix_width {
            let column = groups.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group key column while hashing at index {column_idx}"
                ))
            })?;
            VectorOperations::hash(column.as_ref(), &mut self.column_hashes, count)?;
            let right = &self.column_hashes.as_slice::<u64>()[..count];
            for (left, right) in self.hashes.as_mut_slice::<u64>()[..count]
                .iter_mut()
                .zip(right)
            {
                *left = combine_hash(*left, *right);
            }
        }
        Ok(&self.hashes)
    }

    /// Return full-key hashes and hashes used for radix routing.
    ///
    /// A zero-width prefix intentionally routes by the full hash. This keeps
    /// ungrouped DISTINCT aggregation parallel instead of concentrating every
    /// key in one partition.
    fn hash_with_partition_prefix<'a>(
        &'a mut self,
        keys: &Chunk,
        prefix_column_count: usize,
    ) -> Result<FullAndPrefixHashes<'a>> {
        let column_count = keys.column_count();
        if prefix_column_count > column_count {
            return Err(paro_error::internal(format!(
                "Group hash prefix exceeds key width: prefix={prefix_column_count}, columns={column_count}"
            )));
        }

        let count = keys.size();
        self.ensure_capacity(count, keys.allocator().clone())?;
        self.hashes.try_set_count(count)?;
        if count == 0 {
            return Ok(FullAndPrefixHashes {
                full: &self.hashes,
                prefix: &self.hashes,
            });
        }
        if column_count == 0 {
            self.hashes.as_mut_slice::<u64>()[..count].fill(NULL_HASH);
            return Ok(FullAndPrefixHashes {
                full: &self.hashes,
                prefix: &self.hashes,
            });
        }

        let first = keys
            .column(0)
            .ok_or_else(|| paro_error::internal("Missing first group key column while hashing"))?;
        VectorOperations::hash(first.as_ref(), &mut self.hashes, count)?;
        if prefix_column_count == 1 && prefix_column_count < column_count {
            self.snapshot_partition_hashes(count)?;
        }

        for column_idx in 1..column_count {
            let column = keys.column(column_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing group key column while hashing at index {column_idx}"
                ))
            })?;
            VectorOperations::hash(column.as_ref(), &mut self.column_hashes, count)?;
            let right = &self.column_hashes.as_slice::<u64>()[..count];
            for (left, right) in self.hashes.as_mut_slice::<u64>()[..count]
                .iter_mut()
                .zip(right)
            {
                *left = combine_hash(*left, *right);
            }
            if column_idx + 1 == prefix_column_count && prefix_column_count < column_count {
                self.snapshot_partition_hashes(count)?;
            }
        }

        if prefix_column_count == 0 || prefix_column_count == column_count {
            Ok(FullAndPrefixHashes {
                full: &self.hashes,
                prefix: &self.hashes,
            })
        } else {
            Ok(FullAndPrefixHashes {
                full: &self.hashes,
                prefix: &self.partition_hashes,
            })
        }
    }

    fn snapshot_partition_hashes(&mut self, count: usize) -> Result<()> {
        self.partition_hashes.try_set_count(count)?;
        self.partition_hashes.as_mut_slice::<u64>()[..count]
            .copy_from_slice(&self.hashes.as_slice::<u64>()[..count]);
        Ok(())
    }

    fn ensure_capacity(&mut self, count: usize, allocator: Arc<dyn Allocator>) -> Result<()> {
        if self.hashes.capacity() < count {
            self.hashes = Vector::try_new(LogicalType::UBigInt, count, allocator.clone())?;
        }
        if self.partition_hashes.capacity() < count {
            self.partition_hashes =
                Vector::try_new(LogicalType::UBigInt, count, allocator.clone())?;
        }
        if self.column_hashes.capacity() < count {
            self.column_hashes = Vector::try_new(LogicalType::UBigInt, count, allocator)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use paro_common::chunk::Chunk;
    use paro_common::test_utils::{
        test_allocator, test_i32_vector_with_allocator, test_i64_vector_with_allocator,
    };

    use super::GroupHashScratch;

    #[test]
    fn partition_hash_uses_group_prefix_while_lookup_hash_uses_all_columns() {
        let allocator = test_allocator();
        let keys = Chunk::from_vectors(
            vec![
                test_i32_vector_with_allocator(&[7, 7, 8], allocator.clone()),
                test_i64_vector_with_allocator(&[10, 20, 10], allocator.clone()),
            ],
            allocator.clone(),
        );
        let mut scratch = GroupHashScratch::try_new(keys.size(), allocator).expect("scratch");
        let hashes = scratch
            .hash_with_partition_prefix(&keys, 1)
            .expect("hash keys");
        let lookup = hashes.full;
        let partition = hashes.prefix;

        assert_ne!(lookup.as_slice::<u64>()[0], lookup.as_slice::<u64>()[1]);
        assert_eq!(
            partition.as_slice::<u64>()[0],
            partition.as_slice::<u64>()[1]
        );
        assert_ne!(
            partition.as_slice::<u64>()[0],
            partition.as_slice::<u64>()[2]
        );
    }

    #[test]
    fn aggregate_hash_prefix_ignores_only_the_suffix() {
        let allocator = test_allocator();
        let keys = Chunk::from_vectors(
            vec![
                test_i32_vector_with_allocator(&[7, 7, 8], allocator.clone()),
                test_i64_vector_with_allocator(&[10, 20, 10], allocator.clone()),
            ],
            allocator.clone(),
        );
        let mut scratch = GroupHashScratch::try_new(keys.size(), allocator).expect("scratch");
        let hashes = scratch.hash_prefix(&keys, 1).expect("hash prefix");

        assert_eq!(hashes.as_slice::<u64>()[0], hashes.as_slice::<u64>()[1]);
        assert_ne!(hashes.as_slice::<u64>()[0], hashes.as_slice::<u64>()[2]);
    }
}
