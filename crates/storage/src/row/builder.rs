// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

use crate::buffer::{BufferPool, MemoryTag, DEFAULT_BLOCK_SIZE};
use crate::row::block::{MAX_BLOCKS_PER_REGION, MAX_ROWS_PER_REGION};
use crate::row::raw::{RawRowAppendState, RawRowCollection, RawRowLayout, RawRowPinProperties};
use crate::row::region::collection_row_block_count;
use crate::row::{RowLayout, RowStore, RowValidityType};

/// Builder for execution-time row stores.
#[derive(Debug)]
pub struct RowStoreBuilder {
    layout: Arc<RowLayout>,
    raw_layout: Arc<RawRowLayout>,
    buffer_pool: Arc<BufferPool>,
    tag: MemoryTag,
    regions: Vec<RawRowCollection>,
    current: RawRowCollection,
    count: u64,
}

impl RowStoreBuilder {
    pub fn new(buffer_pool: Arc<BufferPool>, layout: Arc<RowLayout>, tag: MemoryTag) -> Self {
        let raw_layout = Arc::new(layout.to_raw_layout());
        let current = RawRowCollection::new(Arc::clone(&buffer_pool), Arc::clone(&raw_layout), tag);
        Self {
            layout,
            raw_layout,
            buffer_pool,
            tag,
            regions: Vec::new(),
            current,
            count: 0,
        }
    }

    pub fn from_types(
        buffer_pool: Arc<BufferPool>,
        types: Vec<LogicalType>,
        tag: MemoryTag,
    ) -> Self {
        Self::new(
            buffer_pool,
            Arc::new(RowLayout::from_types(
                types,
                RowValidityType::CanHaveNullValues,
            )),
            tag,
        )
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
    pub fn region_count(&self) -> usize {
        self.regions.len() + usize::from(!self.current.is_empty())
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.regions
            .iter()
            .map(RawRowCollection::size_in_bytes)
            .sum::<usize>()
            + self.current.size_in_bytes()
    }

    pub fn appender(&mut self) -> RowAppender<'_> {
        RowAppender {
            builder: self,
            state: RawRowAppendState::with_properties(RawRowPinProperties::UnpinAfterDone),
        }
    }

    pub fn append(&mut self, chunk: &Chunk) -> Result<usize> {
        let mut appender = self.appender();
        appender.append(chunk)
    }

    pub fn try_absorb(&mut self, other: RowStoreBuilder) -> Result<()> {
        self.ensure_compatible(&other)?;
        self.finish_current_region();
        for region in other.into_regions() {
            self.absorb_region(region)?;
        }
        Ok(())
    }

    /// Move another builder's regions into this builder.
    ///
    /// Panics only on incompatible layouts; use [`try_absorb`](Self::try_absorb)
    /// when the caller wants a recoverable error.
    pub fn absorb(&mut self, other: RowStoreBuilder) {
        self.try_absorb(other)
            .expect("cannot absorb row builders with incompatible layouts");
    }

    pub fn try_merge_builders(mut builders: Vec<RowStoreBuilder>) -> Result<RowStoreBuilder> {
        if builders.is_empty() {
            return Err(paro_error::internal(
                "cannot merge an empty RowStoreBuilder list",
            ));
        }

        let mut merged = builders.remove(0);
        for builder in builders {
            merged.try_absorb(builder)?;
        }
        Ok(merged)
    }

    /// Consume all builders and return one merged builder.
    pub fn merge_builders(builders: Vec<RowStoreBuilder>) -> RowStoreBuilder {
        Self::try_merge_builders(builders).expect("cannot merge row builders")
    }

    pub fn try_seal(mut self) -> Result<RowStore> {
        self.finish_current_region();
        let regions = coalesce_regions(self.regions);
        RowStore::new(self.layout, self.raw_layout, regions)
    }

    /// Seal the builder into an immutable [`RowStore`].
    pub fn seal(self) -> RowStore {
        self.try_seal().expect("failed to seal RowStoreBuilder")
    }

    fn append_with_state(&mut self, state: &mut RawRowAppendState, chunk: &Chunk) -> Result<usize> {
        let incoming_rows = chunk.size();
        if incoming_rows == 0 {
            return Ok(0);
        }
        self.rotate_if_needed(incoming_rows)?;

        self.current
            .initialize_append(state, RawRowPinProperties::UnpinAfterDone);
        let (count, _, _) = self.current.append(state, chunk)?;
        self.count += count as u64;

        if self.current.count() as u64 > MAX_ROWS_PER_REGION {
            return Err(paro_error::internal(format!(
                "row region has {} rows, exceeding {}",
                self.current.count(),
                MAX_ROWS_PER_REGION
            )));
        }
        if collection_row_block_count(&self.current) > MAX_BLOCKS_PER_REGION {
            return Err(paro_error::internal(format!(
                "row region has {} row blocks, exceeding {}",
                collection_row_block_count(&self.current),
                MAX_BLOCKS_PER_REGION
            )));
        }
        Ok(count)
    }

    fn rotate_if_needed(&mut self, incoming_rows: usize) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }

        let current_rows = self.current.count() as u64;
        if current_rows + incoming_rows as u64 > MAX_ROWS_PER_REGION {
            self.finish_current_region();
            return Ok(());
        }

        let current_blocks = collection_row_block_count(&self.current);
        let estimated_blocks = self.estimated_row_blocks(incoming_rows);
        if current_blocks + estimated_blocks > MAX_BLOCKS_PER_REGION {
            self.finish_current_region();
        }
        Ok(())
    }

    fn estimated_row_blocks(&self, incoming_rows: usize) -> usize {
        let row_width = self.raw_layout.get_row_width().max(1);
        let bytes = incoming_rows.saturating_mul(row_width);
        bytes.div_ceil(DEFAULT_BLOCK_SIZE).max(1)
    }

    fn absorb_region(&mut self, region: RawRowCollection) -> Result<()> {
        if region.count() == 0 {
            return Ok(());
        }
        self.count += region.count() as u64;
        self.regions.push(region);
        Ok(())
    }

    fn finish_current_region(&mut self) {
        let next = RawRowCollection::new(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.raw_layout),
            self.tag,
        );
        let current = std::mem::replace(&mut self.current, next);
        if current.count() > 0 {
            self.regions.push(current);
        }
    }

    fn into_regions(mut self) -> Vec<RawRowCollection> {
        self.finish_current_region();
        self.regions
    }

    fn ensure_compatible(&self, other: &RowStoreBuilder) -> Result<()> {
        if self.layout.types() != other.layout.types()
            || self.layout.validity() != other.layout.validity()
        {
            return Err(paro_error::internal(
                "cannot combine row builders with different layouts",
            ));
        }
        Ok(())
    }
}

fn coalesce_regions(regions: Vec<RawRowCollection>) -> Vec<RawRowCollection> {
    let mut coalesced: Vec<RawRowCollection> = Vec::with_capacity(regions.len());

    for region in regions {
        if let Some(last) = coalesced.last_mut() {
            if can_coalesce(last, &region) {
                last.combine(region);
                continue;
            }
        }
        coalesced.push(region);
    }

    coalesced
}

fn can_coalesce(left: &RawRowCollection, right: &RawRowCollection) -> bool {
    let combined_rows = left.count() as u64 + right.count() as u64;
    let combined_blocks = collection_row_block_count(left) + collection_row_block_count(right);
    combined_rows <= MAX_ROWS_PER_REGION && combined_blocks <= MAX_BLOCKS_PER_REGION
}

/// Append protocol object for a row-store builder.
#[derive(Debug)]
pub struct RowAppender<'a> {
    builder: &'a mut RowStoreBuilder,
    state: RawRowAppendState,
}

impl RowAppender<'_> {
    pub fn append(&mut self, chunk: &Chunk) -> Result<usize> {
        self.builder.append_with_state(&mut self.state, chunk)
    }
}
