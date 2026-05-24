// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::vector::SelectionVector;

use crate::runtime::context::OperatorCleanupContext;

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct RecursiveTableHandle {
    metadata: BreakerHandleMetadata,
    chunks: Mutex<Vec<Chunk>>,
    dedup: Mutex<Option<Arc<RecursiveDedupSet>>>,
    epoch: AtomicU64,
    cleanup: CleanupState,
}

impl RecursiveTableHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            chunks: Mutex::new(Vec::new()),
            dedup: Mutex::new(None),
            epoch: AtomicU64::new(0),
            cleanup: CleanupState::default(),
        }
    }

    pub fn set_dedup(&self, dedup: Arc<RecursiveDedupSet>) {
        *self.dedup.lock() = Some(dedup);
    }

    pub fn dedup(&self) -> Option<Arc<RecursiveDedupSet>> {
        self.dedup.lock().clone()
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    #[inline]
    pub fn advance_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn append_chunks(&self, chunks: &mut Vec<Chunk>) {
        if chunks.is_empty() {
            return;
        }
        self.chunks.lock().extend(chunks.drain(..));
        self.advance_epoch();
    }

    pub fn append_distinct(
        &self,
        dedup: &RecursiveDedupSet,
        chunks: &mut Vec<Chunk>,
    ) -> Result<usize> {
        if chunks.is_empty() {
            return Ok(0);
        }

        let mut distinct_chunks = Vec::with_capacity(chunks.len());
        let mut row_count = 0usize;
        for chunk in chunks.drain(..) {
            if chunk.is_empty() {
                continue;
            }
            let size = chunk.size();
            let selected = {
                let mut seen = dedup.seen.lock();
                let mut selected = Vec::with_capacity(size);
                for row in 0..size {
                    if seen.insert(row_key(&chunk, row)) {
                        selected.push(row as u32);
                    }
                }
                selected
            };
            match selected.len() {
                0 => {}
                len if len == size => {
                    row_count += len;
                    distinct_chunks.push(chunk);
                }
                len => {
                    row_count += len;
                    let allocator = chunk.allocator().clone();
                    let sel = SelectionVector::try_from_indices(selected, allocator)?;
                    let mut filtered = chunk;
                    filtered.try_slice(&sel, len)?;
                    distinct_chunks.push(filtered);
                }
            }
        }

        if !distinct_chunks.is_empty() {
            self.chunks.lock().extend(distinct_chunks);
            self.advance_epoch();
        }
        Ok(row_count)
    }

    pub fn append_snapshot(&self, chunks: &[Chunk]) {
        if chunks.is_empty() {
            return;
        }
        self.chunks.lock().extend(chunks.iter().cloned());
        self.advance_epoch();
    }

    pub fn replace_chunks(&self, chunks: Vec<Chunk>) {
        *self.chunks.lock() = chunks;
        self.advance_epoch();
    }

    pub fn take_chunks(&self) -> Vec<Chunk> {
        let chunks = std::mem::take(&mut *self.chunks.lock());
        self.advance_epoch();
        chunks
    }

    pub fn clear(&self) {
        self.chunks.lock().clear();
        self.advance_epoch();
    }

    pub fn snapshot_chunks(&self) -> Vec<Chunk> {
        self.chunks.lock().clone()
    }

    pub fn move_all_from(&self, source: &Self) {
        self.replace_chunks(source.take_chunks());
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.chunks.lock().is_empty()
    }

    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.lock().len()
    }

    pub fn row_count(&self) -> usize {
        self.chunks.lock().iter().map(Chunk::size).sum()
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for RecursiveTableHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.chunks.lock().clear();
        if let Some(dedup) = self.dedup.lock().take() {
            dedup.clear();
        }
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug)]
pub struct RecursiveDedupSet {
    seen: Mutex<HashSet<Box<[Value]>>>,
}

impl RecursiveDedupSet {
    pub fn new() -> Self {
        Self {
            seen: Mutex::new(HashSet::new()),
        }
    }

    pub fn clear(&self) {
        self.seen.lock().clear();
    }
}

fn row_key(chunk: &Chunk, row: usize) -> Box<[Value]> {
    chunk
        .data
        .iter()
        .map(|column| column.get_value(row))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_context::{test_support::TestStatementContextBuilder, StatementCancelReason};

    use crate::explain::profiler::OperatorProfiler;
    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};
    use crate::runtime::breaker::cleanup::{CleanupReason, CleanupStatus, RuntimeCleanup};
    use crate::runtime::context::{OperatorCleanupContext, QueryRuntimeContext};
    use crate::runtime::parameter::ParameterBindings;
    use crate::runtime::scratch::TaskMemoryGrants;
    use crate::runtime::QueryOutputPort;
    use crate::thread_context::ThreadContext;

    use super::*;

    fn handle(id: usize) -> RecursiveTableHandle {
        RecursiveTableHandle::new(BreakerHandleMetadata {
            id: BreakerHandleId::new(id),
            kind: BreakerHandleKind::RecursiveTable,
            row_type: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Vec::new().into_boxed_slice(),
            properties: Default::default(),
        })
    }

    fn chunk(values: &[i32]) -> Chunk {
        paro_common::test_utils::test_chunk_from_vectors(vec![Vector::try_from_i32(
            values,
            paro_common::test_utils::test_allocator(),
        )
        .expect("vector")])
    }

    fn query_context() -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        )
    }

    fn with_cleanup_context<R>(
        query: &QueryRuntimeContext,
        f: impl FnOnce(&mut OperatorCleanupContext<'_>) -> R,
    ) -> R {
        let thread = ThreadContext::single_threaded();
        let memory = TaskMemoryGrants::detached(paro_common::test_utils::test_allocator());
        let mut profiler = OperatorProfiler::disabled();
        let mut ctx = OperatorCleanupContext {
            query,
            pipeline: None,
            operator: None,
            thread: &thread,
            memory: memory.call_scope(),
            cancel: &query.cancellation,
            profiler: &mut profiler,
        };
        f(&mut ctx)
    }

    #[test]
    fn append_distinct_filters_seen_rows() {
        let target = handle(0);
        let dedup = RecursiveDedupSet::new();
        let mut first = vec![chunk(&[1, 1, 2])];
        let inserted = target
            .append_distinct(&dedup, &mut first)
            .expect("first append");

        assert_eq!(inserted, 2);
        assert_eq!(target.row_count(), 2);

        let mut second = vec![chunk(&[2, 3])];
        let inserted = target
            .append_distinct(&dedup, &mut second)
            .expect("second append");

        assert_eq!(inserted, 1);
        assert_eq!(target.row_count(), 3);
        let values = target
            .snapshot_chunks()
            .into_iter()
            .flat_map(|chunk| (0..chunk.size()).map(move |row| chunk.data[0].get_value(row)))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]
        );
    }

    #[test]
    fn cancellation_clears_unsealed_iteration_chunks() {
        let target = handle(0);
        let dedup = Arc::new(RecursiveDedupSet::new());
        target.set_dedup(dedup.clone());

        let mut chunks = vec![chunk(&[1, 2, 3])];
        target
            .append_distinct(&dedup, &mut chunks)
            .expect("append iteration chunk");
        assert_eq!(target.row_count(), 3);
        assert_eq!(target.cleanup_status(), CleanupStatus::Live);

        let query = query_context();
        with_cleanup_context(&query, |ctx| {
            target
                .cleanup(
                    ctx,
                    CleanupReason::Cancelled(StatementCancelReason::UserRequest),
                )
                .expect("cancel cleanup");
        });

        assert_eq!(target.row_count(), 0);
        assert_eq!(target.cleanup_status(), CleanupStatus::Cancelled);
        assert!(target.dedup().is_none());
    }
}
