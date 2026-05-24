// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime window breaker handle.
//!
//! Window build tasks retain input chunks task-locally, merge them into this
//! handle once, then `finish()` seals immutable output chunks. Emit source
//! locals cache the sealed chunk array, so scan batches are lock-free.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::operators::window::runtime::build_window_output_chunks;
use crate::physical::specs::WindowSpec;
use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct WindowHandle {
    metadata: BreakerHandleMetadata,
    pending_chunks: Mutex<Vec<Chunk>>,
    sealed_chunks: OnceLock<Arc<[Chunk]>>,
    sealed: AtomicBool,
    cleanup: CleanupState,
}

impl WindowHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            pending_chunks: Mutex::new(Vec::new()),
            sealed_chunks: OnceLock::new(),
            sealed: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn append_chunks(&self, chunks: &mut Vec<Chunk>) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot append to a sealed window breaker handle",
            ));
        }
        self.pending_chunks.lock().extend(chunks.drain(..));
        Ok(())
    }

    pub fn seal(&self, spec: &WindowSpec, allocator: Arc<dyn Allocator>) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }

        let input_chunks = {
            let mut pending = self.pending_chunks.lock();
            std::mem::take(&mut *pending)
        };
        let output_chunks = build_window_output_chunks(spec, &input_chunks, allocator)?;
        self.sealed_chunks
            .set(Arc::from(output_chunks.into_boxed_slice()))
            .map_err(|_| paro_error::internal("window handle was sealed twice"))?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    pub fn sealed_chunks(&self) -> Result<Arc<[Chunk]>> {
        self.sealed_chunks.get().map(Arc::clone).ok_or_else(|| {
            paro_error::internal("window emit source polled before handle was sealed")
        })
    }

    #[inline]
    pub fn pending_chunk_count(&self) -> usize {
        self.pending_chunks.lock().len()
    }

    #[inline]
    pub fn sealed_chunk_count(&self) -> usize {
        self.sealed_chunks
            .get()
            .map(|chunks| chunks.len())
            .unwrap_or(0)
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for WindowHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.pending_chunks.lock().clear();
        self.cleanup.mark(reason);
        Ok(())
    }
}
