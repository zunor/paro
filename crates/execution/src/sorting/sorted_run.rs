// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::{BufferAllocator, BufferManager};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{GrantAllocator, MemoryAccountingClass, MemoryAccountingContext};
use paro_common::sort_key::SortKeyEncoding;
use paro_common::vector::VECTOR_SIZE;
use paro_storage::buffer::{BufferPool, MemoryTag};
use paro_storage::row::{
    Ordering as RowOrdering, PrefixReleasableRowStore, RowLayout, RowStore, RowStoreBuilder,
};

use super::sort_key_store::SortKeyStore;
use super::sort_projection_column::SortProjectionColumn;

const GATHER_PLAN_CACHE_THRESHOLD: usize = VECTOR_SIZE * 2;

#[derive(Debug)]
pub struct RunBuilder {
    buffer_pool: Arc<BufferPool>,
    key_layout: Arc<RowLayout>,
    payload_layout: Arc<RowLayout>,
    memory: MemoryAccountingContext,
    key_store: SortKeyStore,
    key_rows: RowStoreBuilder,
    payload_rows: Option<RowStoreBuilder>,
    count: u32,
}

#[derive(Debug)]
struct GatherPlanCache {
    batch_plans: Vec<BatchGatherPlan>,
}

#[derive(Debug)]
struct BatchGatherPlan {
    locality_sorted_ordinals: Vec<u32>,
    physical_to_logical: Vec<u32>,
}

#[derive(Debug)]
struct CachedRangePlan {
    locality_sorted_ordinals: Vec<u32>,
    locality_output_positions: Vec<u32>,
}

#[derive(Debug)]
enum RunStorage {
    InMemory {
        key_store: SortKeyStore,
        key_rows: RowStore,
        payload_rows: Option<RowStore>,
        permutation: Vec<u32>,
        gather_plan_cache: Option<GatherPlanCache>,
    },
    External {
        key_store: SortKeyStore,
        key_rows: PrefixReleasableRowStore,
        payload_rows: Option<PrefixReleasableRowStore>,
    },
}

#[derive(Debug)]
pub struct SortedRun {
    key_layout: Arc<RowLayout>,
    payload_layout: Arc<RowLayout>,
    storage: RunStorage,
    count: u32,
}

#[derive(Debug)]
pub struct RunRowCursor<'a> {
    store: &'a PrefixReleasableRowStore,
    current_start: u32,
    current_len: u32,
    pinned: Option<paro_storage::row::PinnedRows<'a>>,
}

impl RunBuilder {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        key_layout: Arc<RowLayout>,
        payload_layout: Arc<RowLayout>,
        encoding: Arc<SortKeyEncoding>,
    ) -> Self {
        let memory =
            MemoryAccountingContext::detached(MemoryTag::OrderBy, MemoryAccountingClass::Revocable);
        Self::new_with_memory(buffer_pool, key_layout, payload_layout, encoding, memory)
    }

    pub fn new_with_grant_allocator(
        buffer_pool: Arc<BufferPool>,
        key_layout: Arc<RowLayout>,
        payload_layout: Arc<RowLayout>,
        encoding: Arc<SortKeyEncoding>,
        grant_allocator: GrantAllocator<'_>,
    ) -> Self {
        Self::new_with_memory(
            buffer_pool,
            key_layout,
            payload_layout,
            encoding,
            MemoryAccountingContext::from_grant_allocator(&grant_allocator),
        )
    }

    pub fn new_with_memory(
        buffer_pool: Arc<BufferPool>,
        key_layout: Arc<RowLayout>,
        payload_layout: Arc<RowLayout>,
        encoding: Arc<SortKeyEncoding>,
        memory: MemoryAccountingContext,
    ) -> Self {
        let key_store = SortKeyStore::new_with_memory(
            Arc::clone(&buffer_pool),
            Arc::clone(&encoding),
            memory.clone(),
        );
        let key_rows = RowStoreBuilder::new_with_memory(
            Arc::clone(&buffer_pool),
            Arc::clone(&key_layout),
            MemoryTag::OrderBy,
            memory.clone(),
        );
        let payload_rows = (payload_layout.column_count() > 0).then(|| {
            RowStoreBuilder::new_with_memory(
                Arc::clone(&buffer_pool),
                Arc::clone(&payload_layout),
                MemoryTag::OrderBy,
                memory.clone(),
            )
        });

        Self {
            buffer_pool,
            key_layout,
            payload_layout,
            memory,
            key_store,
            key_rows,
            payload_rows,
            count: 0,
        }
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.count as usize
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        self.key_store.size_in_bytes()
            + self.key_rows.size_in_bytes()
            + self
                .payload_rows
                .as_ref()
                .map(RowStoreBuilder::size_in_bytes)
                .unwrap_or(0)
    }

    pub fn sink(&mut self, key: &Chunk, payload: &Chunk) -> Result<()> {
        if key.size() == 0 {
            return Ok(());
        }
        if key.column_count() != self.key_layout.column_count() {
            return Err(paro_error::internal(format!(
                "sort key chunk column count mismatch: expected {}, got {}",
                self.key_layout.column_count(),
                key.column_count()
            )));
        }
        if payload.column_count() != self.payload_layout.column_count() {
            return Err(paro_error::internal(format!(
                "sort payload chunk column count mismatch: expected {}, got {}",
                self.payload_layout.column_count(),
                payload.column_count()
            )));
        }
        if key.size() != payload.size() && self.payload_rows.is_some() {
            return Err(paro_error::internal(format!(
                "sort key/payload row count mismatch: {} vs {}",
                key.size(),
                payload.size()
            )));
        }

        self.key_store.encode_batch(key)?;
        self.key_rows.append(key)?;
        if let Some(payload_rows) = self.payload_rows.as_mut() {
            payload_rows.append(payload)?;
        }
        self.count += key.size() as u32;
        Ok(())
    }

    pub fn finish(mut self, external: bool) -> Result<SortedRun> {
        self.key_store.finish_writing();

        let mut permutation: Vec<u32> = (0..self.count).collect();
        if self.count > 1 {
            let mut cursor = self.key_store.cursor_pinned()?;
            permutation.sort_unstable_by(|left, right| {
                cursor
                    .compare(*left, *right)
                    .expect("sort key comparison must succeed during finalize")
            });
        }

        let key_rows = self.key_rows.seal();
        let payload_rows = self.payload_rows.map(RowStoreBuilder::seal);

        let storage = if external {
            let reordered_key_store = self.key_store.reorder_by_permutation(&permutation)?;
            let reordered_key_rows = reorder_row_store(
                Arc::clone(&self.buffer_pool),
                self.memory.clone(),
                &key_rows,
                &permutation,
            )?
            .into_prefix_releasable();
            let reordered_payload_rows = payload_rows
                .as_ref()
                .map(|rows| {
                    reorder_row_store(
                        Arc::clone(&self.buffer_pool),
                        self.memory.clone(),
                        rows,
                        &permutation,
                    )
                })
                .transpose()?
                .map(RowStore::into_prefix_releasable);
            RunStorage::External {
                key_store: reordered_key_store,
                key_rows: reordered_key_rows,
                payload_rows: reordered_payload_rows,
            }
        } else {
            let gather_plan_cache = GatherPlanCache::build(&permutation);
            RunStorage::InMemory {
                key_store: self.key_store,
                key_rows,
                payload_rows,
                permutation,
                gather_plan_cache,
            }
        };

        Ok(SortedRun {
            key_layout: self.key_layout,
            payload_layout: self.payload_layout,
            storage,
            count: self.count,
        })
    }
}

impl SortedRun {
    #[inline]
    pub fn count(&self) -> usize {
        self.count as usize
    }

    #[inline]
    pub fn size_in_bytes(&self) -> usize {
        match &self.storage {
            RunStorage::InMemory {
                key_store,
                key_rows,
                payload_rows,
                permutation,
                ..
            } => key_store
                .size_in_bytes()
                .saturating_add(key_rows.size_in_bytes())
                .saturating_add(
                    payload_rows
                        .as_ref()
                        .map(RowStore::size_in_bytes)
                        .unwrap_or(0),
                )
                .saturating_add(permutation.capacity() * std::mem::size_of::<u32>()),
            RunStorage::External {
                key_store,
                key_rows,
                payload_rows,
            } => key_store
                .size_in_bytes()
                .saturating_add(key_rows.size_in_bytes())
                .saturating_add(
                    payload_rows
                        .as_ref()
                        .map(PrefixReleasableRowStore::size_in_bytes)
                        .unwrap_or(0),
                ),
        }
    }

    #[inline]
    pub fn is_external(&self) -> bool {
        matches!(self.storage, RunStorage::External { .. })
    }

    pub fn into_external(
        self,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<(Self, usize)> {
        let before = self.size_in_bytes();
        let Self {
            key_layout,
            payload_layout,
            storage,
            count,
        } = self;

        let storage = match storage {
            RunStorage::External { .. } => {
                return Ok((
                    Self {
                        key_layout,
                        payload_layout,
                        storage,
                        count,
                    },
                    0,
                ));
            }
            RunStorage::InMemory {
                key_store,
                key_rows,
                payload_rows,
                permutation,
                ..
            } => {
                let reordered_key_store = key_store.reorder_by_permutation(&permutation)?;
                let reordered_key_rows = reorder_row_store(
                    buffer_pool.clone(),
                    memory.clone(),
                    &key_rows,
                    &permutation,
                )?
                .into_prefix_releasable();
                let reordered_payload_rows = payload_rows
                    .as_ref()
                    .map(|rows| {
                        reorder_row_store(buffer_pool.clone(), memory.clone(), rows, &permutation)
                    })
                    .transpose()?
                    .map(RowStore::into_prefix_releasable);
                RunStorage::External {
                    key_store: reordered_key_store,
                    key_rows: reordered_key_rows,
                    payload_rows: reordered_payload_rows,
                }
            }
        };

        let run = Self {
            key_layout,
            payload_layout,
            storage,
            count,
        };
        let after = run.size_in_bytes();
        Ok((run, before.saturating_sub(after)))
    }

    #[inline]
    pub fn key_layout(&self) -> &Arc<RowLayout> {
        &self.key_layout
    }

    #[inline]
    pub fn payload_layout(&self) -> &Arc<RowLayout> {
        &self.payload_layout
    }

    #[inline]
    pub fn sort_indices(&self) -> Option<&Vec<u32>> {
        match &self.storage {
            RunStorage::InMemory { permutation, .. } => Some(permutation),
            RunStorage::External { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn has_gather_plan_cache(&self) -> bool {
        matches!(
            &self.storage,
            RunStorage::InMemory {
                gather_plan_cache: Some(_),
                ..
            }
        )
    }

    #[inline]
    pub(crate) fn key_store(&self) -> &SortKeyStore {
        match &self.storage {
            RunStorage::InMemory { key_store, .. } | RunStorage::External { key_store, .. } => {
                key_store
            }
        }
    }

    pub(crate) fn external_key_cursor(&self) -> Option<RunRowCursor<'_>> {
        match &self.storage {
            RunStorage::External { key_rows, .. } => Some(RunRowCursor::new(key_rows)),
            RunStorage::InMemory { .. } => None,
        }
    }

    pub(crate) fn external_payload_cursor(&self) -> Option<RunRowCursor<'_>> {
        match &self.storage {
            RunStorage::External {
                payload_rows: Some(payload_rows),
                ..
            } => Some(RunRowCursor::new(payload_rows)),
            _ => None,
        }
    }

    pub(crate) fn source_ordinal_at_sorted_position(&self, position: u32) -> Result<u32> {
        if position >= self.count {
            return Err(paro_error::internal(format!(
                "sorted position {} out of bounds {}",
                position, self.count
            )));
        }
        Ok(match &self.storage {
            RunStorage::InMemory { permutation, .. } => permutation[position as usize],
            RunStorage::External { .. } => position,
        })
    }

    pub fn read_sort_key_at(&self, position: usize) -> Result<Vec<u8>> {
        let ordinal = self.source_ordinal_at_sorted_position(position as u32)?;
        self.key_store().read_key(ordinal)
    }

    pub fn scan(
        &self,
        chunk: &mut Chunk,
        sorted_position: usize,
        output_projection_columns: &[SortProjectionColumn],
    ) -> Result<()> {
        let remaining = self.count().saturating_sub(sorted_position);
        let count = remaining.min(VECTOR_SIZE);
        if count == 0 {
            chunk.set_cardinality(0);
            return Ok(());
        }

        chunk.try_reset(chunk.allocator().clone())?;
        let output_positions: Vec<u32> = (0..count as u32).collect();
        match &self.storage {
            RunStorage::External { .. } => {
                let mut key_cursor = self
                    .external_key_cursor()
                    .expect("external run must have key row cursor");
                let mut payload_cursor = self.external_payload_cursor();
                self.gather_sorted_range_projected(
                    sorted_position as u32,
                    count as u32,
                    chunk,
                    &output_positions,
                    output_projection_columns,
                    Some(&mut key_cursor),
                    payload_cursor.as_mut(),
                )?;
            }
            RunStorage::InMemory { .. } => {
                self.gather_sorted_range_projected(
                    sorted_position as u32,
                    count as u32,
                    chunk,
                    &output_positions,
                    output_projection_columns,
                    None,
                    None,
                )?;
            }
        }
        chunk.set_cardinality(count);
        Ok(())
    }

    pub(crate) fn gather_sorted_range_projected(
        &self,
        sorted_start: u32,
        len: u32,
        output: &mut Chunk,
        output_positions: &[u32],
        output_projection_columns: &[SortProjectionColumn],
        key_cursor: Option<&mut RunRowCursor<'_>>,
        payload_cursor: Option<&mut RunRowCursor<'_>>,
    ) -> Result<()> {
        let key_projection: Vec<(usize, usize)> = output_projection_columns
            .iter()
            .filter(|projection| !projection.is_payload)
            .map(|projection| (projection.layout_col_idx, projection.output_col_idx))
            .collect();
        let payload_projection: Vec<(usize, usize)> = output_projection_columns
            .iter()
            .filter(|projection| projection.is_payload)
            .map(|projection| (projection.layout_col_idx, projection.output_col_idx))
            .collect();

        match &self.storage {
            RunStorage::InMemory {
                key_rows,
                payload_rows,
                permutation,
                gather_plan_cache,
                ..
            } => {
                let ordinals = &permutation[sorted_start as usize..(sorted_start + len) as usize];
                let cached_range = gather_plan_cache
                    .as_ref()
                    .and_then(|cache| cache.range_plan(sorted_start, len, output_positions));
                if !key_projection.is_empty() {
                    if let Some(plan) = cached_range.as_ref() {
                        let pinned = key_rows.pin_ordinals(
                            &plan.locality_sorted_ordinals,
                            RowOrdering::Sequential,
                        )?;
                        pinned.gather_columns_projected(
                            &key_projection,
                            output,
                            &plan.locality_output_positions,
                        )?;
                    } else {
                        let pinned = key_rows.pin_ordinals(ordinals, RowOrdering::Arbitrary)?;
                        pinned.gather_columns_projected(
                            &key_projection,
                            output,
                            output_positions,
                        )?;
                    }
                }
                if !payload_projection.is_empty() {
                    let payload_rows = payload_rows.as_ref().ok_or_else(|| {
                        paro_error::internal("payload rows missing for payload projection")
                    })?;
                    if let Some(plan) = cached_range.as_ref() {
                        let pinned = payload_rows.pin_ordinals(
                            &plan.locality_sorted_ordinals,
                            RowOrdering::Sequential,
                        )?;
                        pinned.gather_columns_projected(
                            &payload_projection,
                            output,
                            &plan.locality_output_positions,
                        )?;
                    } else {
                        let pinned = payload_rows.pin_ordinals(ordinals, RowOrdering::Arbitrary)?;
                        pinned.gather_columns_projected(
                            &payload_projection,
                            output,
                            output_positions,
                        )?;
                    }
                }
            }
            RunStorage::External {
                key_rows: _,
                payload_rows,
                ..
            } => {
                if !key_projection.is_empty() {
                    let key_cursor = key_cursor.ok_or_else(|| {
                        paro_error::internal("missing external key row cursor for sort run")
                    })?;
                    let pinned = key_cursor.pin_range(sorted_start, len)?;
                    pinned.gather_columns_projected(&key_projection, output, output_positions)?;
                }
                if !payload_projection.is_empty() {
                    payload_rows.as_ref().ok_or_else(|| {
                        paro_error::internal("payload rows missing for payload projection")
                    })?;
                    let payload_cursor = payload_cursor.ok_or_else(|| {
                        paro_error::internal("missing external payload row cursor for sort run")
                    })?;
                    let pinned = payload_cursor.pin_range(sorted_start, len)?;
                    pinned.gather_columns_projected(
                        &payload_projection,
                        output,
                        output_positions,
                    )?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn advance_release_frontier(&self, frontier: u32) -> Result<()> {
        if let RunStorage::External {
            key_store,
            key_rows,
            payload_rows,
        } = &self.storage
        {
            key_store.advance_release_frontier(frontier as u64)?;
            key_rows.advance_release_frontier(frontier as u64)?;
            if let Some(payload_rows) = payload_rows {
                payload_rows.advance_release_frontier(frontier as u64)?;
            }
        }
        Ok(())
    }
}

impl GatherPlanCache {
    fn build(permutation: &[u32]) -> Option<Self> {
        if permutation.len() <= GATHER_PLAN_CACHE_THRESHOLD {
            return None;
        }

        let mut batch_plans = Vec::with_capacity(permutation.len().div_ceil(VECTOR_SIZE));
        for batch in permutation.chunks(VECTOR_SIZE) {
            let mut indexed = batch
                .iter()
                .copied()
                .enumerate()
                .map(|(logical_idx, ordinal)| (logical_idx as u32, ordinal))
                .collect::<Vec<_>>();
            indexed.sort_unstable_by_key(|&(_, ordinal)| ordinal);
            batch_plans.push(BatchGatherPlan {
                locality_sorted_ordinals: indexed.iter().map(|&(_, ordinal)| ordinal).collect(),
                physical_to_logical: indexed
                    .into_iter()
                    .map(|(logical_idx, _)| logical_idx)
                    .collect(),
            });
        }

        Some(Self { batch_plans })
    }

    fn range_plan(
        &self,
        sorted_start: u32,
        len: u32,
        output_positions: &[u32],
    ) -> Option<CachedRangePlan> {
        if len == 0 {
            return Some(CachedRangePlan {
                locality_sorted_ordinals: Vec::new(),
                locality_output_positions: Vec::new(),
            });
        }

        let start = sorted_start as usize;
        let end = start.checked_add(len as usize)?;
        let batch_idx = start / VECTOR_SIZE;
        let batch_start = batch_idx * VECTOR_SIZE;
        let batch_end = batch_start + self.batch_plans.get(batch_idx)?.physical_to_logical.len();
        if end > batch_end {
            return None;
        }

        let logical_begin = (start - batch_start) as u32;
        let logical_end = logical_begin + len;
        let batch_plan = &self.batch_plans[batch_idx];
        let mut locality_sorted_ordinals = Vec::with_capacity(len as usize);
        let mut locality_output_positions = Vec::with_capacity(len as usize);

        for (physical_idx, &logical_idx) in batch_plan.physical_to_logical.iter().enumerate() {
            if logical_idx >= logical_begin && logical_idx < logical_end {
                locality_sorted_ordinals.push(batch_plan.locality_sorted_ordinals[physical_idx]);
                locality_output_positions
                    .push(output_positions[(logical_idx - logical_begin) as usize]);
            }
        }

        Some(CachedRangePlan {
            locality_sorted_ordinals,
            locality_output_positions,
        })
    }
}

impl<'a> RunRowCursor<'a> {
    pub fn new(store: &'a PrefixReleasableRowStore) -> Self {
        Self {
            store,
            current_start: 0,
            current_len: 0,
            pinned: None,
        }
    }

    pub fn pin_range(
        &mut self,
        start: u32,
        len: u32,
    ) -> Result<&paro_storage::row::PinnedRows<'a>> {
        let requested_end = start
            .checked_add(len)
            .ok_or_else(|| paro_error::internal("run row cursor range overflow"))?;
        let current_end = self.current_start + self.current_len;

        let reuse =
            self.pinned.is_some() && start == self.current_start && requested_end == current_end;
        if !reuse {
            self.pinned = Some(self.store.pin_ordinal_range(start, len)?);
            self.current_start = start;
            self.current_len = len;
        }

        self.pinned
            .as_ref()
            .ok_or_else(|| paro_error::internal("run row cursor failed to keep range pinned"))
    }
}

fn reorder_row_store(
    buffer_pool: Arc<BufferPool>,
    memory: MemoryAccountingContext,
    source: &RowStore,
    permutation: &[u32],
) -> Result<RowStore> {
    let allocator = Arc::new(BufferAllocator::new(
        Arc::clone(&buffer_pool) as Arc<dyn BufferManager>,
        MemoryTag::OrderBy,
    ));
    let mut builder = RowStoreBuilder::new_with_memory(
        buffer_pool,
        Arc::new(source.layout().clone()),
        MemoryTag::OrderBy,
        memory,
    );
    let column_ids: Vec<usize> = (0..source.layout().column_count()).collect();
    let mut gathered = Chunk::try_initialize(source.layout().types(), VECTOR_SIZE, allocator)?;

    for batch in permutation.chunks(VECTOR_SIZE) {
        gathered.try_reset(gathered.allocator().clone())?;
        let pinned = source.pin_ordinals(batch, RowOrdering::Arbitrary)?;
        pinned.gather_columns(&column_ids, &mut gathered, 0)?;
        gathered.set_cardinality(batch.len());
        builder.append(&gathered)?;
    }

    Ok(builder.seal())
}
