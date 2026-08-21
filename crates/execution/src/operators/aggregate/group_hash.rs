// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Reusable vector hashing for grouped execution operators.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::hash::{combine_hash, NULL_HASH};
use paro_common::types::LogicalType;
use paro_common::vector::{Vector, VectorOperations};

/// Hash every column in a group-key batch.
///
/// This allocation-owning entry point is used by spill paths that do not own
/// operator-local scratch. Steady-state aggregation should retain a
/// [`GroupHashScratch`] instead.
pub(crate) fn hash_group_columns(groups: &Chunk) -> Result<Vector> {
    let mut scratch = GroupHashScratch::try_new(groups.size().max(1), groups.allocator().clone())?;
    scratch.hash(groups)?;
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
        let (hashes, _) = self.hash_with_partition_prefix(groups, groups.column_count())?;
        Ok(hashes)
    }

    /// Return full-key hashes and hashes used for radix routing.
    ///
    /// A zero-width prefix intentionally routes by the full hash. This keeps
    /// ungrouped DISTINCT aggregation parallel instead of concentrating every
    /// key in one partition.
    pub(crate) fn hash_with_partition_prefix<'a>(
        &'a mut self,
        keys: &Chunk,
        prefix_column_count: usize,
    ) -> Result<(&'a Vector, &'a Vector)> {
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
            return Ok((&self.hashes, &self.hashes));
        }
        if column_count == 0 {
            self.hashes.as_mut_slice::<u64>()[..count].fill(NULL_HASH);
            return Ok((&self.hashes, &self.hashes));
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
            Ok((&self.hashes, &self.hashes))
        } else {
            Ok((&self.hashes, &self.partition_hashes))
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
        let (lookup, partition) = scratch
            .hash_with_partition_prefix(&keys, 1)
            .expect("hash keys");

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
}
