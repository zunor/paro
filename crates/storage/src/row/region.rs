// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::ops::Range;
use std::sync::{Arc, Mutex};

use paro_common::error::{self as paro_error, Result};

use crate::row::addr::{MAX_BLOCK_INDEX, MAX_ROW_WITHIN_BLOCK};
use crate::row::block::{BlockBacking, HeapBlock, RowBlock};
use crate::row::raw::{RawRowCollection, RawRowLocation};
use crate::row::RowAddr;

/// Physical row location inside one sealed store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowLocation {
    pub ordinal: u64,
    pub addr: RowAddr,
    pub region_index: usize,
    pub local_ordinal: usize,
    pub raw: RawRowLocation,
}

#[derive(Debug, Default)]
struct RowRegionReleaseState {
    row_block_prefix: usize,
    heap_block_prefix: usize,
}

/// Sealed append-time row region.
///
/// `RowRegion` is an allocator namespace fixed at seal time. It is not a reclaim
/// frontier by itself; prefix/reclaim semantics live on the store wrappers.
#[derive(Debug)]
pub struct RowRegion {
    index: u32,
    collection: RawRowCollection,
    row_count: u64,
    block_row_prefix: Vec<u32>,
    block_local_ordinals: Vec<Range<usize>>,
    heap_release_prefix_after_block: Vec<usize>,
    segment_row_block_prefix: Vec<usize>,
    segment_heap_block_prefix: Vec<usize>,
    row_blocks: Vec<RowBlock>,
    heap_blocks: Vec<HeapBlock>,
    release_state: Mutex<RowRegionReleaseState>,
}

impl RowRegion {
    pub(crate) fn from_collection(
        index: u32,
        ordinal_base: u64,
        collection: RawRowCollection,
        row_width: usize,
    ) -> Result<(Self, Vec<RowLocation>)> {
        let total_row_blocks = collection_row_block_count(&collection);
        if total_row_blocks == 0 && collection.count() != 0 {
            return Err(paro_error::internal(
                "non-empty row region has no row blocks",
            ));
        }
        if total_row_blocks > (MAX_BLOCK_INDEX as usize + 1) {
            return Err(paro_error::internal(format!(
                "row region {} has {} row blocks, exceeding {}",
                index,
                total_row_blocks,
                MAX_BLOCK_INDEX as usize + 1
            )));
        }

        let mut block_row_counts = vec![0u32; total_row_blocks];
        let mut block_heap_min_used = vec![None; total_row_blocks];
        let mut locations = Vec::with_capacity(collection.count());
        let mut local_ordinal = 0usize;
        let mut block_base = 0usize;
        let mut heap_block_base = 0usize;
        let mut segment_row_block_prefix = Vec::with_capacity(collection.segments().len() + 1);
        let mut segment_heap_block_prefix = Vec::with_capacity(collection.segments().len() + 1);
        segment_row_block_prefix.push(0);
        segment_heap_block_prefix.push(0);

        for (segment_index, segment) in collection.segments().iter().enumerate() {
            for chunk in segment.chunks() {
                if chunk.part_indices.is_empty() {
                    continue;
                }
                for part_idx in chunk.part_indices.start()..chunk.part_indices.end() {
                    let part = &segment.chunk_parts()[part_idx as usize];
                    let synthetic_block_index = block_base + part.row_block_index as usize;
                    if synthetic_block_index >= block_row_counts.len() {
                        return Err(paro_error::internal(format!(
                            "row block index {} out of region block range {}",
                            synthetic_block_index,
                            block_row_counts.len()
                        )));
                    }
                    if part.has_heap() {
                        let synthetic_heap_index = heap_block_base + part.heap_block_index as usize;
                        block_heap_min_used[synthetic_block_index] = Some(
                            block_heap_min_used[synthetic_block_index]
                                .map_or(synthetic_heap_index, |current: usize| {
                                    current.min(synthetic_heap_index)
                                }),
                        );
                    }
                    let first_row_within_block = part.row_block_offset as usize / row_width;
                    for row_in_part in 0..part.count as usize {
                        let row_within_block = first_row_within_block + row_in_part;
                        if row_within_block > MAX_ROW_WITHIN_BLOCK as usize {
                            return Err(paro_error::internal(format!(
                                "row offset {} exceeds RowAddr capacity",
                                row_within_block
                            )));
                        }
                        block_row_counts[synthetic_block_index] =
                            block_row_counts[synthetic_block_index].saturating_add(1);
                        let ordinal = ordinal_base + local_ordinal as u64;
                        let addr = RowAddr::new(
                            index,
                            synthetic_block_index as u32,
                            row_within_block as u32,
                        )?;
                        locations.push(RowLocation {
                            ordinal,
                            addr,
                            region_index: index as usize,
                            local_ordinal,
                            raw: RawRowLocation {
                                segment_index,
                                part_index: part_idx as usize,
                                row_in_part,
                            },
                        });
                        local_ordinal += 1;
                    }
                }
            }
            block_base += segment.allocator().row_block_count();
            heap_block_base += segment.allocator().heap_block_count();
            segment_row_block_prefix.push(block_base);
            segment_heap_block_prefix.push(heap_block_base);
        }

        let mut block_row_prefix = Vec::with_capacity(block_row_counts.len() + 1);
        block_row_prefix.push(0);
        for count in &block_row_counts {
            let next = block_row_prefix
                .last()
                .copied()
                .unwrap_or(0u32)
                .checked_add(*count)
                .ok_or_else(|| paro_error::internal("row block prefix overflow"))?;
            block_row_prefix.push(next);
        }

        let block_local_ordinals = block_row_counts
            .iter()
            .enumerate()
            .map(|(idx, count)| {
                let start = block_row_prefix[idx] as usize;
                start..(start + *count as usize)
            })
            .collect();

        let total_heap_blocks = collection_heap_block_count(&collection);
        let mut heap_release_prefix_after_block = vec![total_heap_blocks; total_row_blocks];
        let mut next_heap_prefix = total_heap_blocks;
        for block_idx in (0..total_row_blocks).rev() {
            heap_release_prefix_after_block[block_idx] = next_heap_prefix;
            if let Some(heap_idx) = block_heap_min_used[block_idx] {
                next_heap_prefix = next_heap_prefix.min(heap_idx);
            }
        }

        let row_blocks = block_row_counts
            .iter()
            .enumerate()
            .map(|(idx, count)| RowBlock::new(idx as u16, *count, BlockBacking::BufferPoolBacked))
            .collect();

        let heap_blocks = (0..collection_heap_block_count(&collection))
            .map(|idx| HeapBlock::new(idx as u32, BlockBacking::BufferPoolBacked))
            .collect();

        Ok((
            Self {
                index,
                row_count: collection.count() as u64,
                collection,
                block_row_prefix,
                block_local_ordinals,
                heap_release_prefix_after_block,
                segment_row_block_prefix,
                segment_heap_block_prefix,
                row_blocks,
                heap_blocks,
                release_state: Mutex::new(RowRegionReleaseState::default()),
            },
            locations,
        ))
    }

    #[inline]
    pub fn index(&self) -> u32 {
        self.index
    }

    #[inline]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    #[inline]
    pub fn block_row_prefix(&self) -> &[u32] {
        &self.block_row_prefix
    }

    #[inline]
    pub(crate) fn block_local_ordinals(&self, block_index: usize) -> Option<Range<usize>> {
        self.block_local_ordinals.get(block_index).cloned()
    }

    #[inline]
    pub(crate) fn heap_release_prefix_after_block(&self, block_index: usize) -> Option<usize> {
        self.heap_release_prefix_after_block
            .get(block_index)
            .copied()
    }

    #[inline]
    pub fn row_blocks(&self) -> &[RowBlock] {
        &self.row_blocks
    }

    #[inline]
    pub fn heap_blocks(&self) -> &[HeapBlock] {
        &self.heap_blocks
    }

    #[inline]
    pub(crate) fn collection(&self) -> &RawRowCollection {
        &self.collection
    }

    pub(crate) fn release_prefix_blocks(&self, row_block_end: usize, heap_block_end: usize) {
        let mut state = self
            .release_state
            .lock()
            .expect("row region release state poisoned");

        let target_row_block_end = row_block_end.min(self.row_blocks.len());
        let target_heap_block_end = heap_block_end.min(self.heap_blocks.len());
        if target_row_block_end <= state.row_block_prefix
            && target_heap_block_end <= state.heap_block_prefix
        {
            return;
        }

        for (segment_idx, segment) in self.collection.segments().iter().enumerate() {
            let row_global_start = self.segment_row_block_prefix[segment_idx];
            let row_global_end = self.segment_row_block_prefix[segment_idx + 1];
            let heap_global_start = self.segment_heap_block_prefix[segment_idx];
            let heap_global_end = self.segment_heap_block_prefix[segment_idx + 1];

            let row_release_start = state.row_block_prefix.max(row_global_start);
            let row_release_end = target_row_block_end.min(row_global_end);
            let heap_release_start = state.heap_block_prefix.max(heap_global_start);
            let heap_release_end = target_heap_block_end.min(heap_global_end);

            let allocator = unsafe {
                let ptr = Arc::as_ptr(segment.allocator()) as *mut crate::row::raw::RawRowAllocator;
                &mut *ptr
            };

            if row_release_end > row_release_start {
                allocator.release_row_blocks_range(
                    row_release_start - row_global_start,
                    row_release_end - row_global_start,
                );
            }
            if heap_release_end > heap_release_start {
                allocator.release_heap_blocks_range(
                    heap_release_start - heap_global_start,
                    heap_release_end - heap_global_start,
                );
            }
        }

        state.row_block_prefix = target_row_block_end.max(state.row_block_prefix);
        state.heap_block_prefix = target_heap_block_end.max(state.heap_block_prefix);
    }
}

pub(crate) fn collection_row_block_count(collection: &RawRowCollection) -> usize {
    collection
        .segments()
        .iter()
        .map(|segment| segment.allocator().row_block_count())
        .sum()
}

pub(crate) fn collection_heap_block_count(collection: &RawRowCollection) -> usize {
    collection
        .segments()
        .iter()
        .map(|segment| segment.allocator().heap_block_count())
        .sum()
}
