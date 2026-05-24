// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime CTE materialization handle.
//!
//! CTE producers append task-local chunks during `merge_local()` and seal an
//! immutable chunk array at finish. Each `CteScanSource` keeps its own
//! source-local cursor over the sealed array, so multiple CTE consumers can
//! rescan the same materialized rows without contending on a shared cursor.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};

use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct CteHandle {
    metadata: BreakerHandleMetadata,
    pending_chunks: Mutex<Vec<Chunk>>,
    sealed_chunks: OnceLock<Arc<[Chunk]>>,
    sealed: AtomicBool,
    cleanup: CleanupState,
}

impl CteHandle {
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
                "cannot append to a sealed CTE breaker handle",
            ));
        }
        self.pending_chunks.lock().extend(chunks.drain(..));
        Ok(())
    }

    pub fn seal(&self) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }

        let chunks = {
            let mut pending = self.pending_chunks.lock();
            std::mem::take(&mut *pending)
        };
        self.sealed_chunks
            .set(Arc::from(chunks.into_boxed_slice()))
            .map_err(|_| paro_error::internal("CTE handle was sealed twice"))?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    pub fn sealed_chunks(&self) -> Result<Arc<[Chunk]>> {
        self.sealed_chunks
            .get()
            .map(Arc::clone)
            .ok_or_else(|| paro_error::internal("CTE scan source polled before handle was sealed"))
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

impl RuntimeCleanup for CteHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.pending_chunks.lock().clear();
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};
    use crate::runtime::breaker::BreakerHandleMetadata;
    use crate::{physical::properties::PipelineProperties, physical::row_type::RowType};
    use paro_common::test_utils::test_allocator;
    use paro_common::types::LogicalType;

    fn metadata() -> BreakerHandleMetadata {
        BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind: BreakerHandleKind::Cte,
            row_type: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Box::new([]),
            properties: PipelineProperties::default(),
        }
    }

    #[test]
    fn cte_handle_seals_chunks_for_independent_consumer_cursors() {
        let handle = CteHandle::new(metadata());
        let mut chunks = vec![Chunk::try_new(test_allocator()).expect("chunk")];
        handle.append_chunks(&mut chunks).expect("append");
        assert_eq!(handle.pending_chunk_count(), 1);

        handle.seal().expect("seal");
        let first_reader = handle.sealed_chunks().expect("first reader");
        let second_reader = handle.sealed_chunks().expect("second reader");

        assert_eq!(first_reader.len(), 1);
        assert_eq!(second_reader.len(), 1);
        assert_eq!(handle.sealed_chunk_count(), 1);
        assert!(handle.is_sealed());
    }
}
