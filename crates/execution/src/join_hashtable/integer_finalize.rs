// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact integer-index finalization for hash joins.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::error::{self as paro_error, ErrorClass, Result};

use super::super::build_store::BuildBlockRange;
use super::super::integer_index::{
    ConcurrentDirectIntegerIndexBuilder, ExactIntegerJoinIndex, StagedRankedIntegerIndexBuilder,
};
use super::{IntegerIndexBuildStats, JoinHashTable};

pub(super) struct BuiltIntegerIndex {
    pub(super) index: ExactIntegerJoinIndex,
    pub(super) has_long_chains: bool,
}

/// Scheduler-facing direct-index build prepared after all build stores have
/// been merged. A bounded set of scheduler workers dynamically claims
/// immutable row slabs; the final successful worker publishes the index
/// through [`Self::complete`].
pub(crate) struct ParallelDirectIntegerIndexBuild {
    table: Arc<JoinHashTable>,
    kind: super::super::integer_index::IntegerKeyKind,
    blocks: Box<[BuildBlockRange]>,
    builder: Mutex<Option<Arc<ConcurrentDirectIntegerIndexBuilder>>>,
    has_long_chains: AtomicBool,
    next_block: AtomicUsize,
}

impl std::fmt::Debug for ParallelDirectIntegerIndexBuild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelDirectIntegerIndexBuild")
            .field("block_count", &self.blocks.len())
            .finish_non_exhaustive()
    }
}

impl ParallelDirectIntegerIndexBuild {
    pub(crate) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub(crate) fn claim_block(&self) -> Option<usize> {
        let block_idx = self.next_block.fetch_add(1, Ordering::Relaxed);
        (block_idx < self.blocks.len()).then_some(block_idx)
    }

    pub(crate) fn build_block(&self, block_idx: usize) -> Result<()> {
        let block = *self.blocks.get(block_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "direct join index task is out of bounds: index={block_idx}, count={}",
                self.blocks.len()
            ))
        })?;
        let builder = self
            .builder
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| paro_error::internal("direct join index was already completed"))?;
        if self
            .table
            .build_direct_integer_block(self.kind, builder.as_ref(), block)?
        {
            self.has_long_chains.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(crate) fn complete(&self) -> Result<()> {
        let builder =
            self.builder.lock().unwrap().take().ok_or_else(|| {
                paro_error::internal("direct join index completed more than once")
            })?;
        let builder = Arc::try_unwrap(builder)
            .map_err(|_| paro_error::internal("direct join index retained a task reference"))?;
        self.table.publish_integer_index(BuiltIntegerIndex {
            index: builder.finish()?,
            has_long_chains: self.has_long_chains.load(Ordering::Relaxed),
        })
    }
}

impl JoinHashTable {
    fn prepare_direct_integer_index_builder(
        &self,
    ) -> Result<
        Option<(
            super::super::integer_index::IntegerKeyKind,
            Arc<ConcurrentDirectIntegerIndexBuilder>,
            Box<[BuildBlockRange]>,
        )>,
    > {
        let IntegerIndexBuildStats::Bounded {
            kind,
            minimum: min_ordinal,
            maximum: max_ordinal,
            count: measured_count,
        } = *self.integer_index_build_stats.lock().unwrap()
        else {
            return Ok(None);
        };
        if measured_count == 0 || measured_count != self.count() {
            return Ok(None);
        }
        let builder = match ConcurrentDirectIntegerIndexBuilder::try_new(
            kind,
            min_ordinal,
            max_ordinal,
            measured_count,
            self.allocator.clone(),
            &self.pointer_memory,
        ) {
            Ok(Some(builder)) => Arc::new(builder),
            Ok(None) => return Ok(None),
            Err(error) if error.error_class() == ErrorClass::Resource => return Ok(None),
            Err(error) => return Err(error),
        };

        let blocks = self
            .build_store
            .lock()
            .unwrap()
            .block_ranges()
            .into_boxed_slice();
        Ok(Some((kind, builder, blocks)))
    }

    pub(crate) fn prepare_parallel_direct_integer_index(
        self: &Arc<Self>,
    ) -> Result<Option<ParallelDirectIntegerIndexBuild>> {
        let Some((kind, builder, blocks)) = self.prepare_direct_integer_index_builder()? else {
            return Ok(None);
        };
        Ok(Some(ParallelDirectIntegerIndexBuild {
            table: Arc::clone(self),
            kind,
            blocks,
            builder: Mutex::new(Some(builder)),
            has_long_chains: AtomicBool::new(false),
            next_block: AtomicUsize::new(0),
        }))
    }

    pub(super) fn try_build_direct_integer_index(&self) -> Result<Option<BuiltIntegerIndex>> {
        let Some((kind, builder, blocks)) = self.prepare_direct_integer_index_builder()? else {
            return Ok(None);
        };
        let mut has_long_chains = false;
        for block in &blocks {
            has_long_chains |= self.build_direct_integer_block(kind, builder.as_ref(), *block)?;
        }
        let builder = Arc::try_unwrap(builder).map_err(|_| {
            paro_error::internal("direct join index retained a temporary reference")
        })?;
        Ok(Some(BuiltIntegerIndex {
            index: builder.finish()?,
            has_long_chains,
        }))
    }

    pub(super) fn try_build_ranked_integer_index(&self) -> Result<Option<BuiltIntegerIndex>> {
        let IntegerIndexBuildStats::Bounded {
            kind,
            minimum: min_ordinal,
            maximum: max_ordinal,
            count: measured_count,
        } = *self.integer_index_build_stats.lock().unwrap()
        else {
            return Ok(None);
        };
        if measured_count == 0 || measured_count != self.count() {
            return Ok(None);
        }
        let builder = match StagedRankedIntegerIndexBuilder::try_new(
            kind,
            min_ordinal,
            max_ordinal,
            measured_count,
            self.allocator.clone(),
            &self.pointer_memory,
        ) {
            Ok(Some(builder)) => builder,
            Ok(None) => return Ok(None),
            Err(error) if error.error_class() == ErrorClass::Resource => return Ok(None),
            Err(error) => return Err(error),
        };

        let store = self.build_store.lock().unwrap();
        let blocks = store.block_ranges();
        let mut builder = builder;
        for block in &blocks {
            self.visit_integer_build_block(kind, *block, |row_idx, _, ordinal| {
                builder.record_at(row_idx, ordinal)
            })?;
        }
        let mut scatter = builder.prepare_scatter()?;
        let mut has_long_chains = false;
        for block in &blocks {
            for row_idx in 0..block.row_count() {
                let row_ptr = unsafe { block.row_ptr(row_idx) };
                if let Some(previous) = scatter.insert_at(block.row_offset() + row_idx, row_ptr)? {
                    self.build_row_layout
                        .set_next(row_ptr as *mut u8, previous as *const u8);
                    has_long_chains = true;
                }
            }
        }
        drop(store);
        Ok(Some(BuiltIntegerIndex {
            index: scatter.finish()?,
            has_long_chains,
        }))
    }

    fn build_direct_integer_block(
        &self,
        kind: super::super::integer_index::IntegerKeyKind,
        builder: &ConcurrentDirectIntegerIndexBuilder,
        block: BuildBlockRange,
    ) -> Result<bool> {
        let mut has_long_chains = false;
        self.visit_integer_build_block(kind, block, |_, row_ptr, ordinal| {
            if let Some(previous) = builder.insert(ordinal, row_ptr)? {
                self.build_row_layout
                    .set_next(row_ptr as *mut u8, previous as *const u8);
                has_long_chains = true;
            }
            Ok(())
        })?;
        Ok(has_long_chains)
    }

    fn visit_integer_build_block(
        &self,
        kind: super::super::integer_index::IntegerKeyKind,
        block: BuildBlockRange,
        mut visit: impl FnMut(usize, usize, u128) -> Result<()>,
    ) -> Result<()> {
        for row_idx in 0..block.row_count() {
            // SAFETY: finalization seals build reclaim before retaining block
            // ranges, so the table owns this immutable slab for the visit.
            let row_ptr = unsafe { block.row_ptr(row_idx) };
            let ordinal = kind
                .row_ordinal(self.build_row_layout.base(), row_ptr as *const u8, 0)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "integer join build key does not match declared type {:?}",
                        self.equality_types[0]
                    ))
                })?;
            visit(block.row_offset() + row_idx, row_ptr, ordinal)?;
        }
        Ok(())
    }
}
