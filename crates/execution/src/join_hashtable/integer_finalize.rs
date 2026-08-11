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

impl JoinHashTable {
    pub(super) fn try_build_direct_integer_index_parallel(
        &self,
        parallelism: usize,
    ) -> Result<Option<ExactIntegerJoinIndex>> {
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

        let store = self.build_store.lock().unwrap();
        let blocks = store.block_ranges();
        let has_long_chains = AtomicBool::new(false);
        self.parallel_visit_build_rows(&blocks, parallelism, |_, row_ptr| {
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
            Ok(())
        })?;
        drop(store);
        let builder = Arc::try_unwrap(builder).map_err(|_| {
            paro_error::internal("parallel direct join index retained a worker reference")
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

    fn parallel_visit_build_rows<F>(
        &self,
        blocks: &[BuildBlockRange],
        parallelism: usize,
        visit: F,
    ) -> Result<()>
    where
        F: Fn(usize, usize) -> Result<()> + Sync,
    {
        let block_count = blocks.len();
        if block_count == 0 {
            return Ok(());
        }
        let worker_count = parallelism.min(block_count).max(1);
        if worker_count == 1 {
            for block in blocks {
                for row_idx in 0..block.row_count() {
                    let row_ptr = unsafe { block.row_ptr(row_idx) };
                    visit(block.row_offset() + row_idx, row_ptr)?;
                }
            }
            return Ok(());
        }
        let next_block = AtomicUsize::new(0);
        let error = Mutex::new(None::<paro_common::error::ParoError>);
        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                let next_block = &next_block;
                let error = &error;
                let visit = &visit;
                scope.spawn(move || loop {
                    if error.lock().unwrap().is_some() {
                        return;
                    }
                    let block_idx = next_block.fetch_add(1, Ordering::Relaxed);
                    if block_idx >= block_count {
                        return;
                    }
                    let block = blocks[block_idx];
                    let result = (0..block.row_count()).try_for_each(|row_idx| {
                        // SAFETY: the caller keeps the build store locked and
                        // alive for the complete scoped traversal.
                        let row_ptr = unsafe { block.row_ptr(row_idx) };
                        visit(block.row_offset() + row_idx, row_ptr)
                    });
                    if let Err(failure) = result {
                        let mut slot = error.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(failure);
                        }
                        return;
                    }
                });
            }
        });
        match error.into_inner().unwrap() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
