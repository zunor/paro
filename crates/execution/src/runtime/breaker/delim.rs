// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Delim/correlated-subquery breaker handle.
//!
//! Delim capture is not a generic materialization handle: it owns a distinct
//! key set for correlated values, a sealed delim-value stream for dependent
//! scans, and an optional cached outer stream for the wrapped join.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;

use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct DelimHandle {
    metadata: BreakerHandleMetadata,
    seen: Mutex<HashSet<Box<[Value]>>>,
    pending_values: Mutex<Vec<Chunk>>,
    pending_cached_outer: Mutex<Vec<Chunk>>,
    sealed_values: OnceLock<Arc<[Chunk]>>,
    sealed_cached_outer: OnceLock<Arc<[Chunk]>>,
    capture_sealed: AtomicBool,
    cleanup: CleanupState,
}

impl DelimHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            seen: Mutex::new(HashSet::new()),
            pending_values: Mutex::new(Vec::new()),
            pending_cached_outer: Mutex::new(Vec::new()),
            sealed_values: OnceLock::new(),
            sealed_cached_outer: OnceLock::new(),
            capture_sealed: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn select_new_keys(&self, keys: &Chunk) -> Result<Vec<u32>> {
        let mut seen = self.seen.lock();
        let mut selection = Vec::with_capacity(keys.size());
        for row_idx in 0..keys.size() {
            let mut key = Vec::with_capacity(keys.column_count());
            for col_idx in 0..keys.column_count() {
                key.push(
                    keys.get_value(col_idx, row_idx).ok_or_else(|| {
                        paro_error::internal("delim key chunk value out of bounds")
                    })?,
                );
            }
            if seen.insert(key.into_boxed_slice()) {
                selection.push(row_idx as u32);
            }
        }
        Ok(selection)
    }

    pub fn append_capture(
        &self,
        values: &mut Vec<Chunk>,
        cached_outer: &mut Vec<Chunk>,
    ) -> Result<()> {
        if values.is_empty() && cached_outer.is_empty() {
            return Ok(());
        }
        if self.is_capture_sealed() {
            return Err(paro_error::internal(
                "cannot append to a sealed delim capture handle",
            ));
        }
        if !values.is_empty() {
            self.pending_values.lock().extend(values.drain(..));
        }
        if !cached_outer.is_empty() {
            self.pending_cached_outer
                .lock()
                .extend(cached_outer.drain(..));
        }
        Ok(())
    }

    pub fn seal_capture(&self) -> Result<()> {
        if self.is_capture_sealed() {
            return Ok(());
        }
        let values = {
            let mut pending = self.pending_values.lock();
            std::mem::take(&mut *pending)
        };
        self.sealed_values
            .set(Arc::from(values.into_boxed_slice()))
            .map_err(|_| paro_error::internal("delim values were sealed twice"))?;

        let cached_outer = {
            let mut pending = self.pending_cached_outer.lock();
            std::mem::take(&mut *pending)
        };
        self.sealed_cached_outer
            .set(Arc::from(cached_outer.into_boxed_slice()))
            .map_err(|_| paro_error::internal("delim cached outer was sealed twice"))?;

        self.capture_sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_capture_sealed(&self) -> bool {
        self.capture_sealed.load(Ordering::Acquire)
    }

    pub fn sealed_values(&self) -> Result<Arc<[Chunk]>> {
        self.sealed_values
            .get()
            .map(Arc::clone)
            .ok_or_else(|| paro_error::internal("delim scan polled before capture was sealed"))
    }

    pub fn sealed_cached_outer(&self) -> Result<Arc<[Chunk]>> {
        self.sealed_cached_outer
            .get()
            .map(Arc::clone)
            .ok_or_else(|| {
                paro_error::internal("delim cached outer read before capture was sealed")
            })
    }

    #[inline]
    pub fn distinct_key_count(&self) -> usize {
        self.seen.lock().len()
    }

    #[inline]
    pub fn pending_value_chunk_count(&self) -> usize {
        self.pending_values.lock().len()
    }

    #[inline]
    pub fn sealed_value_chunk_count(&self) -> usize {
        self.sealed_values
            .get()
            .map(|chunks| chunks.len())
            .unwrap_or(0)
    }

    #[inline]
    pub fn sealed_cached_outer_chunk_count(&self) -> usize {
        self.sealed_cached_outer
            .get()
            .map(|chunks| chunks.len())
            .unwrap_or(0)
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for DelimHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.pending_values.lock().clear();
        self.pending_cached_outer.lock().clear();
        self.seen.lock().clear();
        self.cleanup.mark(reason);
        Ok(())
    }
}
