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
        for row_idx in 0..block.row_count() {
            // SAFETY: build completion freezes the store, `table` retains its
            // slabs, and this task stays within its assigned block.
            let row_ptr = unsafe { block.row_ptr(row_idx) };
            let ordinal = self
                .kind
                .row_ordinal(self.table.build_row_layout.base(), row_ptr as *const u8, 0)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "integer join build key does not match declared type {:?}",
                        self.table.equality_types[0]
                    ))
                })?;
            if let Some(previous) = builder.insert(ordinal, row_ptr)? {
                self.table
                    .build_row_layout
                    .set_next(row_ptr as *mut u8, previous as *const u8);
                self.has_long_chains.store(true, Ordering::Relaxed);
            }
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
        self.table.chains_longer_than_one.store(
            self.has_long_chains.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.table
            .install_finalized_integer_index(builder.finish()?)
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

    pub(super) fn try_build_direct_integer_index(&self) -> Result<Option<ExactIntegerJoinIndex>> {
        let Some((kind, builder, blocks)) = self.prepare_direct_integer_index_builder()? else {
            return Ok(None);
        };
        let has_long_chains = AtomicBool::new(false);
        for block in &blocks {
            for row_idx in 0..block.row_count() {
                // SAFETY: the build store is immutable throughout finalization.
                let row_ptr = unsafe { block.row_ptr(row_idx) };
                let ordinal = kind
                    .row_ordinal(self.build_row_layout.base(), row_ptr as *const u8, 0)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "integer join build key does not match declared type {:?}",
                            self.equality_types[0]
                        ))
                    })?;
                if let Some(previous) = builder.insert(ordinal, row_ptr)? {
                    self.build_row_layout
                        .set_next(row_ptr as *mut u8, previous as *const u8);
                    has_long_chains.store(true, Ordering::Relaxed);
                }
            }
        }
        let builder = Arc::try_unwrap(builder).map_err(|_| {
            paro_error::internal("direct join index retained a temporary reference")
        })?;
        let index = builder.finish()?;
        self.chains_longer_than_one
            .store(has_long_chains.load(Ordering::Relaxed), Ordering::Relaxed);
        Ok(Some(index))
    }

    pub(super) fn try_build_ranked_integer_index(&self) -> Result<Option<ExactIntegerJoinIndex>> {
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
            for row_idx in 0..block.row_count() {
                let row_ptr = unsafe { block.row_ptr(row_idx) };
                let ordinal = kind
                    .row_ordinal(self.build_row_layout.base(), row_ptr as *const u8, 0)
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "integer join build key does not match declared type {:?}",
                            self.equality_types[0]
                        ))
                    })?;
                builder.record_at(block.row_offset() + row_idx, ordinal)?;
            }
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
        let index = scatter.finish()?;
        self.chains_longer_than_one
            .store(has_long_chains, Ordering::Relaxed);
        Ok(Some(index))
    }
}
