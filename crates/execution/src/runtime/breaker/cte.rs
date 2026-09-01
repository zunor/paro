// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime CTE materialization handle.
//!
//! CTE producers publish the same immutable, spill-capable snapshot used by
//! generic materialization. Each CTE reference creates an independent reader,
//! while tasks belonging to that reader claim disjoint chunks or row stores.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_storage::row::RowStore;

use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::materialized::MaterializedHandle;
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct CteHandle {
    materialized: Arc<MaterializedHandle>,
    external_selected: AtomicBool,
    staged: Mutex<CteStagedSnapshot>,
    cleanup: CleanupState,
}

#[derive(Debug, Default)]
pub(crate) struct CteStagedSnapshot {
    pub(crate) chunks: Vec<Chunk>,
    pub(crate) stores: Vec<RowStore>,
    prepared: bool,
}

impl CteHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            materialized: Arc::new(MaterializedHandle::new(metadata)),
            external_selected: AtomicBool::new(false),
            staged: Mutex::new(CteStagedSnapshot::default()),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        self.materialized.metadata()
    }

    #[inline]
    pub fn materialized(&self) -> Arc<MaterializedHandle> {
        Arc::clone(&self.materialized)
    }

    #[inline]
    pub(crate) fn external_selected(&self) -> bool {
        self.external_selected.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn select_external(&self) {
        self.external_selected.store(true, Ordering::Release);
    }

    pub(crate) fn stage_chunks(&self, chunks: &mut Vec<Chunk>) -> Result<()> {
        let mut staged = self.staged.lock();
        if staged.prepared {
            return Err(paro_error::internal(
                "cannot stage CTE chunks after snapshot preparation",
            ));
        }
        staged.chunks.append(chunks);
        Ok(())
    }

    pub(crate) fn stage_row_store(&self, store: RowStore) -> Result<()> {
        let mut staged = self.staged.lock();
        if staged.prepared {
            return Err(paro_error::internal(
                "cannot stage a CTE row store after snapshot preparation",
            ));
        }
        staged.stores.push(store);
        Ok(())
    }

    pub(crate) fn take_staged_snapshot(&self) -> Option<CteStagedSnapshot> {
        let mut staged = self.staged.lock();
        if staged.prepared {
            return None;
        }
        staged.prepared = true;
        Some(CteStagedSnapshot {
            chunks: std::mem::take(&mut staged.chunks),
            stores: std::mem::take(&mut staged.stores),
            prepared: true,
        })
    }

    pub fn append_chunks(&self, chunks: &mut Vec<Chunk>) -> Result<()> {
        self.materialized.append_chunks(chunks)
    }

    pub fn append_row_store(&self, store: RowStore) -> Result<()> {
        self.materialized.append_row_store(store)
    }

    pub fn seal(&self) -> Result<()> {
        self.materialized.seal()
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.materialized.is_sealed()
    }

    #[inline]
    pub fn pending_chunk_count(&self) -> usize {
        self.materialized.pending_chunk_count()
    }

    #[inline]
    pub fn sealed_chunk_count(&self) -> usize {
        self.materialized.sealed_chunk_count()
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for CteHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        *self.staged.lock() = CteStagedSnapshot::default();
        self.materialized.cleanup(ctx, reason)?;
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
        let first = super::super::MaterializedReader::new(handle.materialized(), "first reader");
        let second = super::super::MaterializedReader::new(handle.materialized(), "second reader");
        let first_reader = first.sealed_chunks().expect("first reader");
        let second_reader = second.sealed_chunks().expect("second reader");

        assert_eq!(first_reader.len(), 1);
        assert_eq!(second_reader.len(), 1);
        assert_eq!(handle.sealed_chunk_count(), 1);
        assert!(handle.is_sealed());
    }
}
