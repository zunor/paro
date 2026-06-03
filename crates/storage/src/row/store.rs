// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};

use crate::row::addr::MAX_REGION_INDEX;
use crate::row::pin::{PinSet, PrefixReleaseState};
use crate::row::pinned::PinnedRows;
use crate::row::raw::{RawRowCollection, RawRowLayout};
use crate::row::region::{RowLocation, RowRegion};
use crate::row::scan::{
    ReclaimTracker, ReclaimingRowScanCursor, RowParallelScanCursor, RowScanCursor, ScanChunkMeta,
};
use crate::row::{RowAddr, RowLayout};

/// Caller-declared ordinal ordering for batch pinning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    /// Ordinals are already in physical/sequential order.
    Sequential,
    /// Ordinals may be arbitrary; the store may reorder internally and restore output order.
    Arbitrary,
}

/// Addressable sealed row store.
#[derive(Debug)]
pub struct RowStore {
    layout: Arc<RowLayout>,
    regions: Vec<RowRegion>,
    count: u64,
    region_row_prefix: Vec<u64>,
    ordinal_locations: Vec<RowLocation>,
    addr_to_ordinal: HashMap<RowAddr, u64>,
    scan_chunks: Vec<ScanChunkMeta>,
}

impl RowStore {
    pub(crate) fn new(
        layout: Arc<RowLayout>,
        raw_layout: Arc<RawRowLayout>,
        raw_regions: Vec<RawRowCollection>,
    ) -> Result<Self> {
        if raw_regions.len() > (MAX_REGION_INDEX as usize + 1) {
            return Err(paro_error::internal(format!(
                "row store has {} regions, exceeding {}",
                raw_regions.len(),
                MAX_REGION_INDEX
            )));
        }

        let count: u64 = raw_regions.iter().map(|region| region.count() as u64).sum();
        let indexed_count = usize::try_from(count).map_err(|_| {
            paro_error::internal(format!(
                "row store count {} cannot be indexed on this platform",
                count
            ))
        })?;

        let mut regions = Vec::with_capacity(raw_regions.len());
        let mut region_row_prefix = Vec::with_capacity(raw_regions.len() + 1);
        let mut ordinal_locations = Vec::with_capacity(indexed_count);
        let mut addr_to_ordinal = HashMap::with_capacity(indexed_count);
        let mut scan_chunks = Vec::new();
        let mut ordinal_base = 0u64;
        region_row_prefix.push(0);

        for (region_index, raw_region) in raw_regions.into_iter().enumerate() {
            let (region, locations) = RowRegion::from_collection(
                region_index as u32,
                ordinal_base,
                raw_region,
                raw_layout.get_row_width(),
            )?;
            scan_chunks.extend(
                region
                    .row_blocks()
                    .iter()
                    .enumerate()
                    .map(|(block_idx, block)| {
                        let local_range = region
                            .block_local_ordinals(block_idx)
                            .expect("row block local ordinal range");
                        let ordinal_start = ordinal_base + local_range.start as u64;
                        let ordinal_end = ordinal_start + block.row_count() as u64;
                        ScanChunkMeta {
                            region_idx: region_index,
                            row_block_idx: block_idx,
                            ordinal_start,
                            ordinal_end,
                            row_count: block.row_count(),
                            local_ordinal_start: local_range.start as u32,
                            local_ordinal_end: local_range.end as u32,
                            heap_block_end: region
                                .heap_release_prefix_after_block(block_idx)
                                .unwrap_or(region.heap_blocks().len())
                                as u32,
                        }
                    }),
            );
            ordinal_base += region.row_count();
            region_row_prefix.push(ordinal_base);
            for location in &locations {
                if addr_to_ordinal
                    .insert(location.addr, location.ordinal)
                    .is_some()
                {
                    return Err(paro_error::internal(format!(
                        "duplicate row address while sealing: {}",
                        location.addr
                    )));
                }
            }
            ordinal_locations.extend(locations);
            regions.push(region);
        }

        Ok(Self {
            layout,
            regions,
            count,
            region_row_prefix,
            ordinal_locations,
            addr_to_ordinal,
            scan_chunks,
        })
    }

    #[inline]
    pub fn layout(&self) -> &RowLayout {
        &self.layout
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.regions
            .iter()
            .map(|region| region.collection().size_in_bytes())
            .sum()
    }

    #[inline]
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    #[inline]
    pub fn region_row_prefix(&self) -> &[u64] {
        &self.region_row_prefix
    }

    #[inline]
    pub fn regions(&self) -> &[RowRegion] {
        &self.regions
    }

    #[inline]
    pub(crate) fn region(&self, index: usize) -> &RowRegion {
        &self.regions[index]
    }

    #[inline]
    pub(crate) fn scan_chunks(&self) -> &[ScanChunkMeta] {
        &self.scan_chunks
    }

    #[inline]
    pub(crate) fn scan_chunk_count(&self) -> u32 {
        self.scan_chunks.len() as u32
    }

    pub fn addr_at_ordinal(&self, ordinal: u64) -> Result<RowAddr> {
        self.location_for_ordinal(ordinal)
            .map(|location| location.addr)
    }

    pub fn pin_rows(&self, addrs: &[RowAddr]) -> Result<PinnedRows<'_>> {
        let mut rows = Vec::with_capacity(addrs.len());
        for &addr in addrs {
            if addr.is_invalid() {
                return Err(paro_error::internal("cannot pin RowAddr::INVALID"));
            }
            let ordinal = self.addr_to_ordinal.get(&addr).copied().ok_or_else(|| {
                paro_error::internal(format!(
                    "row address {} does not belong to this store",
                    addr
                ))
            })?;
            rows.push(*self.location_for_ordinal(ordinal)?);
        }
        Ok(PinnedRows::new(
            self,
            rows,
            Ordering::Arbitrary,
            PinSet::none(),
        ))
    }

    pub fn pin_ordinals<O>(&self, ordinals: &[O], ordering: Ordering) -> Result<PinnedRows<'_>>
    where
        O: Copy + Into<u64>,
    {
        let mut rows = Vec::with_capacity(ordinals.len());
        for &ordinal in ordinals {
            rows.push(*self.location_for_ordinal(ordinal.into())?);
        }
        Ok(PinnedRows::new(self, rows, ordering, PinSet::none()))
    }

    pub fn pin_ordinal_range(&self, start: u64, len: u64) -> Result<PinnedRows<'_>> {
        let end = start
            .checked_add(len)
            .ok_or_else(|| paro_error::internal("row ordinal range overflow"))?;
        if end > self.count {
            return Err(paro_error::internal(format!(
                "row ordinal range [{}, {}) exceeds count {}",
                start, end, self.count
            )));
        }

        let start_idx = self.ordinal_to_index(start)?;
        let end_idx = self.ordinal_to_index(end)?;
        let rows = self.ordinal_locations[start_idx..end_idx].to_vec();
        Ok(PinnedRows::new(
            self,
            rows,
            Ordering::Sequential,
            PinSet::none(),
        ))
    }

    pub fn scanner(&self) -> RowScanCursor<'_> {
        RowScanCursor::new(self)
    }

    pub fn parallel_scanner(&self) -> RowParallelScanCursor<'_> {
        RowParallelScanCursor::new(self)
    }

    pub fn into_prefix_releasable(self) -> PrefixReleasableRowStore {
        PrefixReleasableRowStore {
            store: self,
            release_state: PrefixReleaseState::default(),
        }
    }

    pub fn into_reclaimable(self) -> ReclaimableRowStore {
        ReclaimableRowStore {
            store: self,
            tracker: ReclaimTracker::new(),
        }
    }

    pub(crate) fn validate_column(&self, column_idx: usize) -> Result<()> {
        if column_idx >= self.layout.column_count() {
            return Err(paro_error::internal(format!(
                "row column {} out of range {}",
                column_idx,
                self.layout.column_count()
            )));
        }
        Ok(())
    }

    fn location_for_ordinal(&self, ordinal: u64) -> Result<&RowLocation> {
        let index = self.ordinal_to_index(ordinal)?;
        self.ordinal_locations.get(index).ok_or_else(|| {
            paro_error::internal(format!(
                "row ordinal {} out of range for store count {}",
                ordinal, self.count
            ))
        })
    }

    fn ordinal_to_index(&self, ordinal: u64) -> Result<usize> {
        usize::try_from(ordinal).map_err(|_| {
            paro_error::internal(format!(
                "row ordinal {} cannot be indexed on this platform",
                ordinal
            ))
        })
    }

    pub(crate) fn scan_chunk_prefix_for_ordinal_frontier(&self, frontier: u64) -> u32 {
        self.scan_chunks
            .iter()
            .take_while(|meta| meta.ordinal_end <= frontier)
            .count() as u32
    }

    pub(crate) fn ordinal_frontier_for_scan_chunk_prefix(&self, prefix: u32) -> u64 {
        if prefix == 0 {
            0
        } else {
            self.scan_chunks
                .get(prefix as usize - 1)
                .map(|meta| meta.ordinal_end)
                .unwrap_or(self.count)
        }
    }

    pub(crate) fn release_scan_chunk_prefix(&self, current: u32, target: u32) {
        for meta in self
            .scan_chunks
            .iter()
            .skip(current as usize)
            .take(target.saturating_sub(current) as usize)
        {
            self.region(meta.region_idx)
                .release_prefix_blocks(meta.row_block_idx + 1, meta.heap_block_end as usize);
        }
    }
}

/// Prefix-releasable row store.
///
/// This wrapper intentionally does not expose `RowAddr`, `pin_rows`, or
/// `pin_ordinals`; callers can only pin live suffix ranges.
#[derive(Debug)]
pub struct PrefixReleasableRowStore {
    store: RowStore,
    release_state: PrefixReleaseState,
}

impl PrefixReleasableRowStore {
    #[inline]
    pub fn count(&self) -> u64 {
        self.store.count()
    }

    #[inline]
    pub fn layout(&self) -> &RowLayout {
        self.store.layout()
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.store.size_in_bytes()
    }

    pub fn pin_ordinal_range(&self, start: u64, len: u64) -> Result<PinnedRows<'_>> {
        if len == 0 {
            return Ok(PinnedRows::new(
                &self.store,
                Vec::new(),
                Ordering::Sequential,
                PinSet::none(),
            ));
        }

        let pin_set = PinSet::prefix(&self.store, &self.release_state);
        let physical_frontier = self.release_state.physical_release_frontier();
        if start < physical_frontier {
            return Err(paro_error::internal(format!(
                "row ordinal range starts before physical release frontier: start={}, physical_frontier={}",
                start, physical_frontier
            )));
        }

        let end = start
            .checked_add(len)
            .ok_or_else(|| paro_error::internal("row ordinal range overflow"))?;
        if end > self.store.count() {
            return Err(paro_error::internal(format!(
                "row ordinal range [{}, {}) exceeds count {}",
                start,
                end,
                self.store.count()
            )));
        }

        let start_idx = self.store.ordinal_to_index(start)?;
        let end_idx = self.store.ordinal_to_index(end)?;
        let rows = self.store.ordinal_locations[start_idx..end_idx].to_vec();
        Ok(PinnedRows::new(
            &self.store,
            rows,
            Ordering::Sequential,
            pin_set,
        ))
    }

    pub fn advance_release_frontier(&self, frontier: u64) -> Result<()> {
        self.release_state
            .advance_release_frontier(&self.store, frontier)
    }

    #[inline]
    pub fn logical_release_frontier(&self) -> u64 {
        self.release_state.logical_release_frontier()
    }

    #[inline]
    pub fn physical_release_frontier(&self) -> u64 {
        self.release_state.physical_release_frontier()
    }

    #[inline]
    pub fn outstanding_pins(&self) -> usize {
        self.release_state.outstanding_pins()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn scan_chunks(&self) -> &[ScanChunkMeta] {
        self.store.scan_chunks()
    }
}

/// Cursor-only reclaimable row store.
#[derive(Debug)]
pub struct ReclaimableRowStore {
    store: RowStore,
    tracker: ReclaimTracker,
}

impl ReclaimableRowStore {
    #[inline]
    pub fn count(&self) -> u64 {
        self.store.count()
    }

    #[inline]
    pub fn layout(&self) -> &RowLayout {
        self.store.layout()
    }

    pub fn scanner(&self) -> RowScanCursor<'_> {
        RowScanCursor::with_reclaim(&self.store, self.tracker.register_scanner(&self.store))
    }

    pub fn into_reclaiming_scanner(self) -> ReclaimingRowScanCursor {
        ReclaimingRowScanCursor::new(self.store, self.tracker)
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn reclaimed_scan_chunk_prefix(&self) -> u32 {
        self.tracker.released_scan_chunk_prefix()
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn scan_chunks(&self) -> &[ScanChunkMeta] {
        self.store.scan_chunks()
    }
}
