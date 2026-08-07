// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime sort and TopN breaker handles.
//!
//! Build sinks keep sort runs / TopN heaps task-local on the chunk hot path.
//! Handles are only touched at local merge, finish, cleanup, and the emit
//! source's first poll. Emit sources cache the sealed state in source-local
//! state, so scan batches do not lock the shared handle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{
    MemoryAccountingClass, MemoryAccountingContext, MemoryDomain, MemoryResult,
};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_storage::buffer::BufferPool;

use crate::memory_runtime::{ReclaimStats, Reclaimer, SpillCost};
use crate::operators::sort::topn_heap::{TopNBoundaryValue, TopNHeap};
use crate::runtime::context::OperatorCleanupContext;
use crate::sorting::sort_descriptor::Sort;
use crate::sorting::sorted_run::SortedRun;
use crate::sorting::sorted_run_merger::{SortedRunMerger, SortedRunMergerGlobalState};

use super::cleanup::{CleanupReason, CleanupState, CleanupStatus, RuntimeCleanup};
use super::registry::BreakerHandleMetadata;

#[derive(Debug)]
pub struct SortHandle {
    metadata: BreakerHandleMetadata,
    sort: OnceLock<Arc<Sort>>,
    output_types: OnceLock<Box<[LogicalType]>>,
    pending_runs: Mutex<Vec<SortedRun>>,
    sealed: OnceLock<Arc<SortSealedState>>,
    external: AtomicBool,
    cleanup: CleanupState,
}

impl SortHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            sort: OnceLock::new(),
            output_types: OnceLock::new(),
            pending_runs: Mutex::new(Vec::new()),
            sealed: OnceLock::new(),
            external: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn initialize(
        &self,
        sort: Arc<Sort>,
        output_types: Box<[LogicalType]>,
        external: bool,
    ) -> Result<()> {
        if self.sort.set(sort).is_err() {
            if self.output_types()? != output_types.as_ref() {
                return Err(paro_error::internal(
                    "sort handle output types changed across shared producers",
                ));
            }
            self.external.fetch_or(external, Ordering::AcqRel);
            return Ok(());
        }
        self.output_types
            .set(output_types)
            .map_err(|_| paro_error::internal("sort handle output types initialized twice"))?;
        self.external.store(external, Ordering::Release);
        Ok(())
    }

    pub fn sort(&self) -> Result<Arc<Sort>> {
        self.sort
            .get()
            .map(Arc::clone)
            .ok_or_else(|| paro_error::internal("sort handle has no initialized sort descriptor"))
    }

    pub fn output_types(&self) -> Result<&[LogicalType]> {
        self.output_types
            .get()
            .map(|types| types.as_ref())
            .ok_or_else(|| paro_error::internal("sort handle has no initialized output types"))
    }

    #[inline]
    pub fn mark_external(&self) {
        self.external.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_external(&self) -> bool {
        self.external.load(Ordering::Acquire)
    }

    pub fn add_run(&self, run: SortedRun) -> Result<()> {
        if self.is_sealed() {
            return Err(paro_error::internal(
                "cannot add sorted run after sort handle was sealed",
            ));
        }
        self.pending_runs.lock().push(run);
        Ok(())
    }

    pub fn prepare_parallel_materialization(
        &self,
        num_threads: usize,
        memory_budget_bytes: usize,
    ) -> Result<Option<SortMaterializationBuild>> {
        if self.is_sealed() || self.is_external() || num_threads <= 1 {
            return Ok(None);
        }

        let mut pending = self.pending_runs.lock();
        if pending.len() <= 1 || pending.iter().any(SortedRun::is_external) {
            return Ok(None);
        }
        let total_count = checked_sorted_row_count(&pending)?;
        let task_count = num_threads.min(total_count.div_ceil(VECTOR_SIZE)).max(1);
        if task_count <= 1 {
            return Ok(None);
        }
        let materialized_bytes = checked_sorted_run_bytes(&pending)?;
        if materialized_bytes > memory_budget_bytes {
            return Ok(None);
        }

        let runs = std::mem::take(&mut *pending);
        drop(pending);
        let merger = Arc::new(SortedRunMerger::new(self.sort()?, runs));
        let partition_size = total_count.div_ceil(task_count);
        Ok(Some(SortMaterializationBuild {
            merger,
            output_types: self.output_types()?.to_vec().into_boxed_slice(),
            partition_size,
            task_count,
            total_count,
        }))
    }

    pub fn seal_streaming(&self) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }

        let sort = self.sort()?;
        let output_types = self.output_types()?.to_vec().into_boxed_slice();
        let runs = {
            let mut pending = self.pending_runs.lock();
            std::mem::take(&mut *pending)
        };
        let total_count = checked_sorted_row_count(&runs)?;
        let external = self.is_external();

        let output = if total_count == 0 {
            SortOutputState::Empty
        } else if !external && runs.len() == 1 {
            SortOutputState::SingleRun(Arc::new(runs.into_iter().next().expect("single run")))
        } else {
            let merger = Arc::new(SortedRunMerger::new(Arc::clone(&sort), runs));
            let merger_gstate = Arc::new(SortedRunMergerGlobalState::new(
                total_count,
                total_count.max(1),
                external,
                1,
            ));
            SortOutputState::StreamingMerge {
                merger,
                global: merger_gstate,
            }
        };

        self.install_sealed(SortSealedState {
            sort,
            output_types,
            output,
            total_count,
        })
    }

    pub(crate) fn install_materialized(
        &self,
        chunks: Vec<Chunk>,
        expected_count: usize,
    ) -> Result<()> {
        let output_types = self.output_types()?.to_vec().into_boxed_slice();
        let total_count = chunks.iter().try_fold(0usize, |count, chunk| {
            if chunk.types() != output_types.as_ref() {
                return Err(paro_error::internal(format!(
                    "materialized sort chunk schema mismatch: expected={:?}, actual={:?}",
                    output_types,
                    chunk.types()
                )));
            }
            count
                .checked_add(chunk.size())
                .ok_or_else(|| paro_error::internal("materialized sort row count overflow"))
        })?;
        if total_count != expected_count {
            return Err(paro_error::internal(format!(
                "materialized sort row count mismatch: expected={expected_count}, actual={total_count}"
            )));
        }
        self.install_sealed(SortSealedState {
            sort: self.sort()?,
            output_types,
            output: SortOutputState::Materialized(Arc::from(chunks.into_boxed_slice())),
            total_count,
        })
    }

    fn install_sealed(&self, state: SortSealedState) -> Result<()> {
        self.sealed
            .set(Arc::new(state))
            .map_err(|_| paro_error::internal("sort handle was sealed more than once"))
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.get().is_some()
    }

    pub fn sealed_state(&self) -> Result<Arc<SortSealedState>> {
        self.sealed
            .get()
            .map(Arc::clone)
            .ok_or_else(|| paro_error::internal("sort emit source polled before handle was sealed"))
    }

    #[inline]
    pub fn pending_run_count(&self) -> usize {
        self.pending_runs.lock().len()
    }

    pub fn pending_reclaimable_bytes(&self) -> usize {
        if self.is_sealed() {
            return 0;
        }
        self.pending_runs
            .lock()
            .iter()
            .filter(|run| !run.is_external())
            .map(SortedRun::size_in_bytes)
            .sum()
    }

    pub fn spill_pending_runs(
        &self,
        target_bytes: usize,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Result<ReclaimStats> {
        if target_bytes == 0 || self.is_sealed() {
            return Ok(ReclaimStats::empty(target_bytes));
        }

        let runs = {
            let mut pending = self.pending_runs.lock();
            if pending.iter().all(SortedRun::is_external) {
                return Ok(ReclaimStats::empty(target_bytes));
            }
            self.mark_external();
            std::mem::take(&mut *pending)
        };

        let mut reclaimed = 0usize;
        let mut spilled = 0usize;
        let mut converted = Vec::with_capacity(runs.len());
        for run in runs {
            if reclaimed < target_bytes && !run.is_external() {
                let before = run.size_in_bytes();
                let (external_run, reclaimed_bytes) =
                    run.into_external(Arc::clone(&buffer_pool), memory.clone())?;
                reclaimed = reclaimed.saturating_add(reclaimed_bytes);
                spilled = spilled.saturating_add(before);
                converted.push(external_run);
            } else {
                converted.push(run);
            }
        }

        let mut pending = self.pending_runs.lock();
        if pending.is_empty() {
            *pending = converted;
        } else {
            converted.extend(std::mem::take(&mut *pending));
            *pending = converted;
        }
        Ok(ReclaimStats::new(target_bytes, reclaimed, spilled))
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for SortHandle {
    fn cleanup(&self, ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        self.pending_runs.lock().clear();
        ctx.query
            .memory
            .unregister_reclaimer_by_name(&SortPendingRunsReclaimer::name_for(self));
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug)]
pub struct SortPendingRunsReclaimer {
    name: String,
    handle: Arc<SortHandle>,
    buffer_pool: Arc<BufferPool>,
    memory: MemoryAccountingContext,
}

impl SortPendingRunsReclaimer {
    pub fn new(
        handle: Arc<SortHandle>,
        buffer_pool: Arc<BufferPool>,
        memory: MemoryAccountingContext,
    ) -> Self {
        Self {
            name: Self::name_for(&handle),
            handle,
            buffer_pool,
            memory,
        }
    }

    pub fn for_query(
        handle: Arc<SortHandle>,
        buffer_pool: Arc<BufferPool>,
        query_memory: Arc<crate::memory_runtime::QueryMemoryPool>,
    ) -> Self {
        let memory = MemoryAccountingContext::from_owner(
            query_memory,
            MemoryDomain::Host,
            MemoryTag::OrderBy,
            MemoryAccountingClass::Revocable,
        );
        Self::new(handle, buffer_pool, memory)
    }

    pub fn name_for(handle: &SortHandle) -> String {
        format!("sort_pending_runs:{}", handle.metadata().id.index())
    }
}

impl Reclaimer for SortPendingRunsReclaimer {
    fn name(&self) -> &str {
        &self.name
    }

    fn reclaimable_bytes(&self) -> usize {
        self.handle.pending_reclaimable_bytes()
    }

    fn reclaim_sync(&self, target_bytes: usize) -> MemoryResult<ReclaimStats> {
        self.handle
            .spill_pending_runs(
                target_bytes,
                Arc::clone(&self.buffer_pool),
                self.memory.clone(),
            )
            .map_err(|err| paro_common::memory::MemoryError::reclaim_failed(err.to_string()))
    }

    fn spill_cost(&self) -> SpillCost {
        SpillCost::SpillToDisk
    }
}

#[derive(Debug)]
pub struct SortSealedState {
    pub sort: Arc<Sort>,
    pub output_types: Box<[LogicalType]>,
    pub output: SortOutputState,
    pub total_count: usize,
}

#[derive(Debug)]
pub enum SortOutputState {
    Empty,
    SingleRun(Arc<SortedRun>),
    Materialized(Arc<[Chunk]>),
    StreamingMerge {
        merger: Arc<SortedRunMerger>,
        global: Arc<SortedRunMergerGlobalState>,
    },
}

#[derive(Debug)]
pub struct SortMaterializationBuild {
    pub merger: Arc<SortedRunMerger>,
    pub output_types: Box<[LogicalType]>,
    pub partition_size: usize,
    pub task_count: usize,
    pub total_count: usize,
}

fn checked_sorted_row_count(runs: &[SortedRun]) -> Result<usize> {
    runs.iter().try_fold(0usize, |count, run| {
        count
            .checked_add(run.count())
            .ok_or_else(|| paro_error::internal("sorted row count overflow"))
    })
}

fn checked_sorted_run_bytes(runs: &[SortedRun]) -> Result<usize> {
    runs.iter().try_fold(0usize, |bytes, run| {
        bytes
            .checked_add(run.size_in_bytes())
            .ok_or_else(|| paro_error::internal("sorted run byte count overflow"))
    })
}

#[derive(Debug)]
pub struct TopNHandle {
    metadata: BreakerHandleMetadata,
    state: OnceLock<Mutex<Option<TopNRuntimeState>>>,
    sealed_chunks: OnceLock<Arc<[Chunk]>>,
    sealed: AtomicBool,
    cleanup: CleanupState,
}

impl TopNHandle {
    pub fn new(metadata: BreakerHandleMetadata) -> Self {
        Self {
            metadata,
            state: OnceLock::new(),
            sealed_chunks: OnceLock::new(),
            sealed: AtomicBool::new(false),
            cleanup: CleanupState::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &BreakerHandleMetadata {
        &self.metadata
    }

    pub fn initialize(&self, state: TopNRuntimeState) -> Result<()> {
        match self.state.set(Mutex::new(Some(state))) {
            Ok(()) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    pub fn boundary(&self) -> Result<Arc<TopNBoundaryValue>> {
        let state = self
            .state
            .get()
            .ok_or_else(|| paro_error::internal("topn handle has no initialized state"))?;
        let guard = state.lock();
        guard
            .as_ref()
            .map(|state| Arc::clone(&state.boundary))
            .ok_or_else(|| paro_error::internal("topn state was already sealed"))
    }

    pub fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut TopNRuntimeState) -> Result<R>,
    ) -> Result<R> {
        let state = self
            .state
            .get()
            .ok_or_else(|| paro_error::internal("topn handle has no initialized state"))?;
        let mut guard = state.lock();
        let state = guard
            .as_mut()
            .ok_or_else(|| paro_error::internal("topn state was already sealed"))?;
        f(state)
    }

    pub fn seal(&self) -> Result<()> {
        if self.sealed.load(Ordering::Acquire) {
            return Ok(());
        }
        let state = self
            .state
            .get()
            .ok_or_else(|| paro_error::internal("topn handle has no initialized state"))?;
        let mut state = state
            .lock()
            .take()
            .ok_or_else(|| paro_error::internal("topn state was already sealed"))?;
        let chunks = state.heap.extract_results()?;
        self.sealed_chunks
            .set(Arc::from(chunks.into_boxed_slice()))
            .map_err(|_| paro_error::internal("topn handle result chunks sealed twice"))?;
        self.sealed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn sealed_chunks(&self) -> Result<Arc<[Chunk]>> {
        self.sealed_chunks
            .get()
            .map(Arc::clone)
            .ok_or_else(|| paro_error::internal("topn emit source polled before handle was sealed"))
    }

    #[inline]
    pub fn cleanup_status(&self) -> CleanupStatus {
        self.cleanup.status()
    }
}

impl RuntimeCleanup for TopNHandle {
    fn cleanup(&self, _ctx: &mut OperatorCleanupContext, reason: CleanupReason) -> Result<()> {
        if let Some(state) = self.state.get() {
            let _ = state.lock().take();
        }
        self.cleanup.mark(reason);
        Ok(())
    }
}

#[derive(Debug)]
pub struct TopNRuntimeState {
    pub heap: TopNHeap,
    pub boundary: Arc<TopNBoundaryValue>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::test_utils::{
        test_allocator, test_chunk_from_vectors, test_vector_with_capacity,
    };
    use paro_common::types::LogicalType;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_planner::binder::ir::OrderByNode;
    use paro_planner::expression::{Expression, ReferenceExpression};
    use paro_storage::buffer::BufferPool;

    use crate::explain::profiler::OperatorProfiler;
    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::pipeline::handles::{BreakerHandleId, BreakerHandleKind};
    use crate::runtime::context::{OperatorCleanupContext, QueryRuntimeContext};
    use crate::runtime::parameter::ParameterBindings;
    use crate::runtime::scratch::TaskMemoryGrants;
    use crate::runtime::QueryOutputPort;
    use crate::sorting::sorted_run::RunBuilder;
    use crate::thread_context::ThreadContext;

    use super::*;

    fn metadata(kind: BreakerHandleKind) -> BreakerHandleMetadata {
        BreakerHandleMetadata {
            id: BreakerHandleId::new(0),
            kind,
            row_type: RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
            producer: None,
            consumers: Box::new([]),
            properties: PipelineProperties::default(),
        }
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
        let memory = TaskMemoryGrants::detached(test_allocator());
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

    fn int_sort() -> Arc<Sort> {
        Arc::new(
            Sort::new(
                vec![OrderByNode {
                    expression: Expression::Reference(ReferenceExpression {
                        index: 0,
                        return_type: LogicalType::Integer,
                    }),
                    ascending: true,
                    nulls_first: false,
                }],
                vec![LogicalType::Integer],
                vec![],
                false,
            )
            .expect("sort descriptor"),
        )
    }

    fn sorted_run(sort: &Arc<Sort>, external: bool) -> SortedRun {
        let buffer_pool = Arc::new(BufferPool::new(16 * 1024 * 1024));
        let mut builder = RunBuilder::new(
            buffer_pool,
            Arc::clone(sort.key_layout()),
            Arc::clone(sort.payload_layout()),
            Arc::clone(sort.sort_key_encoding()),
        );
        let mut keys = test_vector_with_capacity(LogicalType::Integer, 2);
        keys.set_i32(0, 2);
        keys.set_i32(1, 1);
        keys.set_count(2);
        let key_chunk = test_chunk_from_vectors(vec![keys]);
        let payload_chunk = Chunk::try_new(test_allocator()).expect("empty payload chunk");
        builder.sink(&key_chunk, &payload_chunk).expect("sink run");
        builder.finish(external).expect("finish run")
    }

    fn buffer_pool_with_temp_dir() -> Arc<BufferPool> {
        let pool = BufferPool::new_arc(16 * 1024 * 1024);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!(
            "paro_sort_reclaimer_{}_{:?}_{}",
            std::process::id(),
            std::thread::current().id(),
            now
        ));
        pool.set_temporary_directory(temp_dir.to_string_lossy().to_string())
            .expect("set temp directory");
        pool
    }

    #[test]
    fn sort_pending_runs_reclaimer_spills_in_memory_runs() {
        let handle = Arc::new(SortHandle::new(metadata(BreakerHandleKind::Sort)));
        let sort = int_sort();
        handle
            .initialize(
                Arc::clone(&sort),
                vec![LogicalType::Integer].into_boxed_slice(),
                false,
            )
            .expect("initialize sort");
        handle
            .add_run(sorted_run(&sort, false))
            .expect("add in-memory run");

        let reclaimable = handle.pending_reclaimable_bytes();
        assert!(reclaimable > 0);
        let reclaimer = SortPendingRunsReclaimer::new(
            Arc::clone(&handle),
            buffer_pool_with_temp_dir(),
            MemoryAccountingContext::detached(MemoryTag::OrderBy, MemoryAccountingClass::Revocable),
        );

        let stats = reclaimer.reclaim_sync(1).expect("reclaim pending run");
        assert_eq!(stats.requested_bytes, 1);
        assert!(stats.reclaimed_bytes > 0);
        assert!(stats.spilled_bytes >= stats.reclaimed_bytes);
        assert!(handle.is_external());
        assert_eq!(handle.pending_run_count(), 1);
        assert_eq!(handle.pending_reclaimable_bytes(), 0);
        assert_eq!(
            reclaimer.reclaim_sync(1).expect("idempotent reclaim"),
            ReclaimStats::empty(1)
        );

        handle.seal_streaming().expect("seal sort");
        let state = handle.sealed_state().expect("sealed state");
        let SortOutputState::StreamingMerge { merger, .. } = &state.output else {
            panic!("external sort should seal as a streaming merge");
        };
        assert!(merger.sorted_runs.iter().all(SortedRun::is_external));
    }

    #[test]
    fn sort_cleanup_releases_pending_external_runs_without_sealing() {
        let handle = SortHandle::new(metadata(BreakerHandleKind::Sort));
        let sort = int_sort();
        handle
            .initialize(
                Arc::clone(&sort),
                vec![LogicalType::Integer].into_boxed_slice(),
                true,
            )
            .expect("initialize sort");
        handle
            .add_run(sorted_run(&sort, true))
            .expect("add external run");
        assert_eq!(handle.pending_run_count(), 1);
        assert!(handle.is_external());

        let query = query_context();
        with_cleanup_context(&query, |ctx| {
            handle
                .cleanup(
                    ctx,
                    CleanupReason::Failed(crate::runtime::context::QueryErrorId::new(17)),
                )
                .expect("cleanup sort");
        });

        assert_eq!(handle.pending_run_count(), 0);
        assert_eq!(handle.cleanup_status(), CleanupStatus::Failed);
        assert!(!handle.is_sealed());
    }

    #[test]
    fn sort_initialize_is_idempotent_for_shared_producers() {
        let handle = SortHandle::new(metadata(BreakerHandleKind::Sort));
        let sort = int_sort();
        handle
            .initialize(
                Arc::clone(&sort),
                vec![LogicalType::Integer].into_boxed_slice(),
                false,
            )
            .expect("first initialize");
        handle
            .initialize(
                Arc::clone(&sort),
                vec![LogicalType::Integer].into_boxed_slice(),
                true,
            )
            .expect("shared producer initialize");

        assert_eq!(handle.output_types().unwrap(), [LogicalType::Integer]);
        assert!(handle.is_external());
    }

    #[test]
    fn topn_cleanup_drops_unsealed_heap_state() {
        let handle = TopNHandle::new(metadata(BreakerHandleKind::TopN));
        let boundary = Arc::new(TopNBoundaryValue::new());
        handle
            .initialize(TopNRuntimeState {
                heap: TopNHeap::new(vec![LogicalType::Integer], &[], 1, 0),
                boundary,
            })
            .expect("initialize topn");

        let query = query_context();
        with_cleanup_context(&query, |ctx| {
            handle
                .cleanup(
                    ctx,
                    CleanupReason::Cancelled(paro_context::StatementCancelReason::UserRequest),
                )
                .expect("cleanup topn");
        });

        assert_eq!(handle.cleanup_status(), CleanupStatus::Cancelled);
        assert!(handle.boundary().is_err());
    }
}
