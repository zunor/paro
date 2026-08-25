// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compact O(1)-common-case routing for contiguous row-id partitions.
//!
//! Generation search artifacts concatenate several segment-local domains.
//! Binary-searching their boundaries for every vector score or predicate
//! admission makes physical segmentation visible in the hottest loops. This
//! directory resolves blocks wholly owned by one partition directly; only a
//! block crossed by a partition boundary takes the exact binary-search path.

use paro_common::error::{self as paro_error, Result};

const PARTITION_BLOCK_SHIFT: u32 = 12;
const PARTITION_BLOCK_ROWS: u32 = 1 << PARTITION_BLOCK_SHIFT;
const MIXED_PARTITION: u32 = u32::MAX;

#[derive(Debug)]
pub(crate) struct PartitionDirectory {
    partition_ends: Box<[u32]>,
    blocks: Box<[u32]>,
    domain_len: u32,
}

impl PartitionDirectory {
    pub(crate) fn try_new(partition_ends: impl IntoIterator<Item = u32>) -> Result<Self> {
        let partition_ends = partition_ends.into_iter().collect::<Vec<_>>();
        if partition_ends.is_empty() {
            return Err(paro_error::invalid_input(
                "partition directory requires at least one partition",
            ));
        }
        let mut previous = 0u32;
        for &end in &partition_ends {
            if end <= previous {
                return Err(paro_error::invalid_input(
                    "partition directory ends must be strictly increasing",
                ));
            }
            previous = end;
        }
        let domain_len = previous;
        let block_count =
            usize::try_from(domain_len.div_ceil(PARTITION_BLOCK_ROWS)).map_err(|_| {
                paro_error::out_of_range("partition directory block count exceeds usize")
            })?;
        let mut blocks = Vec::with_capacity(block_count);
        for block in 0..block_count {
            let block_start = u32::try_from(block)
                .ok()
                .and_then(|block| block.checked_mul(PARTITION_BLOCK_ROWS))
                .ok_or_else(|| {
                    paro_error::out_of_range("partition directory block offset exceeds u32")
                })?;
            let block_end = block_start
                .saturating_add(PARTITION_BLOCK_ROWS)
                .min(domain_len);
            let first = partition_ends.partition_point(|&end| end <= block_start);
            let last = partition_ends.partition_point(|&end| end <= block_end.saturating_sub(1));
            blocks.push(if first == last {
                let direct = u32::try_from(first).map_err(|_| {
                    paro_error::configuration_limit_exceeded(
                        "partition directory partition count exceeds u32",
                    )
                })?;
                if direct == MIXED_PARTITION {
                    return Err(paro_error::configuration_limit_exceeded(
                        "partition directory exhausts its mixed-block sentinel",
                    ));
                }
                direct
            } else {
                MIXED_PARTITION
            });
        }
        Ok(Self {
            partition_ends: partition_ends.into_boxed_slice(),
            blocks: blocks.into_boxed_slice(),
            domain_len,
        })
    }

    #[inline(always)]
    pub(crate) fn part_for(&self, row_id: u32) -> Option<usize> {
        if row_id >= self.domain_len {
            return None;
        }
        let block = row_id as usize >> PARTITION_BLOCK_SHIFT;
        let directory_entry = *self.blocks.get(block)?;
        let position = if directory_entry == MIXED_PARTITION {
            self.partition_ends.partition_point(|&end| end <= row_id)
        } else {
            directory_entry as usize
        };
        let end = *self.partition_ends.get(position)?;
        let start = position
            .checked_sub(1)
            .map_or(0, |previous| self.partition_ends[previous]);
        debug_assert!((start..end).contains(&row_id));
        Some(position)
    }

    /// Heap allocations owned by the directory. The embedding object's
    /// retained-size calculation already includes this value's inline fields.
    pub(crate) fn allocated_bytes(&self) -> usize {
        self.partition_ends
            .len()
            .saturating_mul(std::mem::size_of::<u32>())
            .saturating_add(self.blocks.len().saturating_mul(std::mem::size_of::<u32>()))
    }

    pub(crate) fn matches_partition_ends(
        &self,
        partition_ends: impl IntoIterator<Item = u32>,
    ) -> bool {
        self.partition_ends.iter().copied().eq(partition_ends)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_blocks_and_boundary_blocks_resolve_the_same_partition_domain() {
        let directory = PartitionDirectory::try_new([6_000, 12_000]).unwrap();

        assert_eq!(directory.blocks.as_ref(), &[0, MIXED_PARTITION, 1]);
        assert_eq!(directory.part_for(4_095), Some(0));
        assert_eq!(directory.part_for(5_999), Some(0));
        assert_eq!(directory.part_for(6_000), Some(1));
        assert_eq!(directory.part_for(11_999), Some(1));
        assert_eq!(directory.part_for(12_000), None);
    }

    #[test]
    fn rejects_empty_duplicate_and_reversed_domains() {
        assert!(PartitionDirectory::try_new([]).is_err());
        assert!(PartitionDirectory::try_new([4, 4]).is_err());
        assert!(PartitionDirectory::try_new([8, 4]).is_err());
    }
}
