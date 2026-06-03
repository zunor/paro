// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime contexts shared by role factories and operator calls.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::allocator::{Allocator, BufferAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{ParoError, Result};
use paro_context::{StatementCancellation, StatementContext, TransactionView};
use paro_function::scalar::FunctionExecContext;

use crate::explain::profiler::{ExplainProfiler, OperatorProfiler};
use crate::memory_runtime::{OperatorMemoryScope, QueryMemoryPool};
use crate::physical::properties::PipelineProperties;
use crate::pipeline::graph::PipelineId;
use crate::thread_context::ThreadContext;

use super::breaker::BreakerHandleRegistry;
use super::ids::RuntimeOperatorId;
use super::parameter::ParameterBindings;
use super::scratch::{ExpressionScratchArena, ExpressionScratchLease, PipelineScratch};

pub struct PipelineInitContext<'a> {
    pub query: &'a QueryRuntimeContext,
    pub pipeline: PipelineId,
    pub operator: RuntimeOperatorId,
    pub params: &'a ParameterBindings,
    pub handles: &'a BreakerHandleRegistry,
    pub properties: &'a PipelineProperties,
}

pub struct OperatorCallContext<'a> {
    pub query: &'a QueryRuntimeContext,
    pub pipeline: PipelineId,
    pub operator: RuntimeOperatorId,
    pub thread: &'a ThreadContext,
    pub memory: OperatorMemoryScope<'a>,
    pub scratch: OperatorScratchScope<'a>,
    pub cancel: &'a StatementCancellation,
    pub wake: &'a OperatorWakeScope,
    pub profiler: &'a mut OperatorProfiler,
}

#[derive(Debug)]
pub struct OperatorCallContextCell {
    pipeline: PipelineId,
    operator: RuntimeOperatorId,
    scratch_generation: u64,
}

impl OperatorCallContextCell {
    pub fn new(pipeline: PipelineId, operator: RuntimeOperatorId) -> Self {
        Self {
            pipeline,
            operator,
            scratch_generation: 0,
        }
    }

    #[inline(always)]
    pub fn context<'a>(
        &'a mut self,
        query: &'a QueryRuntimeContext,
        operator: RuntimeOperatorId,
        thread: &'a ThreadContext,
        memory: OperatorMemoryScope<'a>,
        scratch: OperatorScratchScope<'a>,
        wake: &'a OperatorWakeScope,
        profiler: &'a mut OperatorProfiler,
    ) -> OperatorCallContext<'a> {
        self.operator = operator;
        self.scratch_generation = scratch.generation();
        OperatorCallContext {
            query,
            pipeline: self.pipeline,
            operator,
            thread,
            memory,
            scratch,
            cancel: &query.cancellation,
            wake,
            profiler,
        }
    }

    #[inline]
    pub fn operator(&self) -> RuntimeOperatorId {
        self.operator
    }

    #[inline]
    pub fn scratch_generation(&self) -> u64 {
        self.scratch_generation
    }
}

pub struct OperatorFinishContext<'a> {
    pub query: &'a QueryRuntimeContext,
    pub pipeline: PipelineId,
    pub operator: RuntimeOperatorId,
    pub finish_task: Option<FinishTaskId>,
    pub thread: &'a ThreadContext,
    pub memory: OperatorMemoryScope<'a>,
    pub cancel: &'a StatementCancellation,
    pub wake: &'a OperatorWakeScope,
    pub profiler: &'a mut OperatorProfiler,
}

pub struct OperatorCleanupContext<'a> {
    pub query: &'a QueryRuntimeContext,
    pub pipeline: Option<PipelineId>,
    pub operator: Option<RuntimeOperatorId>,
    pub thread: &'a ThreadContext,
    pub memory: OperatorMemoryScope<'a>,
    pub cancel: &'a StatementCancellation,
    pub profiler: &'a mut OperatorProfiler,
}

pub struct UtilityContext<'a> {
    pub session: &'a StatementContext,
    pub catalog: &'a CatalogSnapshot,
    pub transaction: &'a TransactionView,
    pub params: &'a ParameterBindings,
    pub cancel: &'a StatementCancellation,
    pub errors: &'a QueryErrorRegistry,
}

#[derive(Clone)]
pub struct QueryRuntimeContext {
    pub session: Arc<StatementContext>,
    pub catalog: CatalogSnapshot,
    pub transaction: TransactionView,
    pub params: Arc<ParameterBindings>,
    pub memory: Arc<QueryMemoryPool>,
    pub output: QueryOutputPort,
    pub cancellation: StatementCancellation,
    pub errors: QueryErrorRegistry,
    pub wake_events: QueryWakeRegistry,
    pub profiler: QueryProfilerRegistry,
    pub explain_profiler: Option<Arc<ExplainProfiler>>,
}

impl QueryRuntimeContext {
    pub fn new(
        session: Arc<StatementContext>,
        params: Arc<ParameterBindings>,
        memory: Arc<QueryMemoryPool>,
        output: QueryOutputPort,
    ) -> Self {
        Self {
            catalog: session.catalog_txn_view(),
            transaction: session.txn.transaction.clone(),
            cancellation: session.cancellation.child_execution_attempt(),
            session,
            params,
            memory,
            output,
            errors: QueryErrorRegistry::default(),
            wake_events: QueryWakeRegistry::default(),
            profiler: QueryProfilerRegistry::default(),
            explain_profiler: None,
        }
    }

    pub fn with_explain_profiler(mut self, profiler: Arc<ExplainProfiler>) -> Self {
        self.explain_profiler = Some(profiler);
        self
    }

    pub fn record_operator_error(&self, error: ParoError) -> QueryErrorId {
        self.errors.record_root(error)
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryWakeRegistry {
    inner: Arc<Mutex<QueryWakeRegistryState>>,
}

#[derive(Debug, Default)]
struct QueryWakeRegistryState {
    ready: HashSet<WakeKey>,
    coalesced: HashMap<WakeKey, u64>,
}

impl QueryWakeRegistry {
    pub fn wake(&self, key: WakeKey) {
        let mut inner = self
            .inner
            .lock()
            .expect("query wake registry lock poisoned");
        if !inner.ready.insert(key) {
            *inner.coalesced.entry(key).or_insert(0) += 1;
        }
    }

    pub fn wake_registration(&self, registration: PendingWakeRegistration) {
        self.wake(registration.key());
    }

    pub fn is_ready(&self, key: WakeKey) -> bool {
        self.inner
            .lock()
            .expect("query wake registry lock poisoned")
            .ready
            .contains(&key)
    }

    pub fn take_ready(&self, key: WakeKey) -> bool {
        self.take_ready_with_coalesced(key).is_some()
    }

    pub fn take_ready_with_coalesced(&self, key: WakeKey) -> Option<u64> {
        let mut inner = self
            .inner
            .lock()
            .expect("query wake registry lock poisoned");
        if !inner.ready.remove(&key) {
            return None;
        }
        Some(inner.coalesced.remove(&key).unwrap_or(0))
    }
}

impl FunctionExecContext for QueryRuntimeContext {
    fn current_database(&self) -> Option<&str> {
        Some(self.session.current_database())
    }

    fn current_schema(&self) -> Option<&str> {
        Some(self.session.current_schema())
    }

    fn current_user(&self) -> Option<&str> {
        Some(self.session.current_user())
    }

    fn current_setting(&self, key: &str) -> Option<String> {
        let normalized_key = key
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_ascii_lowercase();
        self.session
            .get_setting(&normalized_key)
            .map(|value| paro_common::config::format_setting_value(&normalized_key, value))
    }

    fn is_interrupted(&self) -> bool {
        self.session.is_interrupted()
    }

    fn allocator(&self, tag: MemoryTag) -> Arc<dyn Allocator> {
        if tag == MemoryTag::Allocator {
            self.session.buffer_allocator()
        } else {
            Arc::new(BufferAllocator::new(
                self.session.buffer_manager().clone()
                    as Arc<dyn paro_common::allocator::BufferManager>,
                tag,
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryOutputPort {
    inner: Arc<QueryOutputPortInner>,
}

impl Default for QueryOutputPort {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl QueryOutputPort {
    pub fn unbounded() -> Self {
        Self::with_mode(
            usize::MAX,
            QueryOutputPortMode::Buffered {
                collect_stats: false,
            },
        )
    }

    pub fn bounded(capacity: usize) -> Self {
        Self::with_mode(
            capacity,
            QueryOutputPortMode::Buffered {
                collect_stats: false,
            },
        )
    }

    pub fn unbounded_with_stats() -> Self {
        Self::with_mode(
            usize::MAX,
            QueryOutputPortMode::Buffered {
                collect_stats: true,
            },
        )
    }

    pub fn bounded_with_stats(capacity: usize) -> Self {
        Self::with_mode(
            capacity,
            QueryOutputPortMode::Buffered {
                collect_stats: true,
            },
        )
    }

    pub fn with_blocking_writes(port: &Self) -> Self {
        match port.inner.mode {
            QueryOutputPortMode::Buffered { collect_stats }
            | QueryOutputPortMode::BlockingBuffered { collect_stats } => Self::with_mode(
                port.inner.capacity.max(1),
                QueryOutputPortMode::BlockingBuffered { collect_stats },
            ),
            QueryOutputPortMode::Discarding => Self::discarding(),
        }
    }

    pub fn discarding() -> Self {
        Self::with_mode(0, QueryOutputPortMode::Discarding)
    }

    fn with_mode(capacity: usize, mode: QueryOutputPortMode) -> Self {
        Self {
            inner: Arc::new(QueryOutputPortInner {
                capacity,
                mode,
                chunks: Mutex::new(VecDeque::new()),
                stats: Mutex::new(QueryOutputPortStats::default()),
                generation: AtomicU64::new(0),
                closed: AtomicBool::new(false),
                cv: Condvar::new(),
            }),
        }
    }

    #[inline]
    pub fn try_push(&self, chunk: Chunk) -> QueryOutputWrite {
        match self.inner.mode {
            QueryOutputPortMode::Buffered {
                collect_stats: false,
            } => self.try_push_buffered(chunk),
            QueryOutputPortMode::Buffered {
                collect_stats: true,
            } => self.try_push_buffered_with_stats(chunk),
            QueryOutputPortMode::BlockingBuffered {
                collect_stats: false,
            } => self.push_blocking_buffered(chunk),
            QueryOutputPortMode::BlockingBuffered {
                collect_stats: true,
            } => self.push_blocking_buffered_with_stats(chunk),
            QueryOutputPortMode::Discarding => self.try_push_discarding(chunk),
        }
    }

    #[inline]
    fn try_push_buffered(&self, chunk: Chunk) -> QueryOutputWrite {
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputWrite::Blocked(chunk);
        }
        if chunks.len() >= self.inner.capacity {
            return QueryOutputWrite::Blocked(chunk);
        }
        chunks.push_back(chunk);
        self.inner.cv.notify_all();
        QueryOutputWrite::Written
    }

    fn try_push_buffered_with_stats(&self, chunk: Chunk) -> QueryOutputWrite {
        let rows = chunk.size();
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputWrite::Blocked(chunk);
        }
        if chunks.len() >= self.inner.capacity {
            self.inner
                .stats
                .lock()
                .expect("query output port stats lock poisoned")
                .blocked_pushes += 1;
            return QueryOutputWrite::Blocked(chunk);
        }
        chunks.push_back(chunk);
        let queue_len = chunks.len();
        drop(chunks);

        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned");
        stats.pushed_chunks += 1;
        stats.pushed_rows += rows;
        stats.peak_queue_chunks = stats.peak_queue_chunks.max(queue_len);
        self.inner.cv.notify_all();
        QueryOutputWrite::Written
    }

    fn push_blocking_buffered(&self, chunk: Chunk) -> QueryOutputWrite {
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        while chunks.len() >= self.inner.capacity && !self.inner.closed.load(Ordering::Acquire) {
            chunks = self
                .inner
                .cv
                .wait(chunks)
                .expect("query output port condvar poisoned");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputWrite::Blocked(chunk);
        }
        chunks.push_back(chunk);
        self.inner.cv.notify_all();
        QueryOutputWrite::Written
    }

    fn push_blocking_buffered_with_stats(&self, chunk: Chunk) -> QueryOutputWrite {
        let rows = chunk.size();
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        while chunks.len() >= self.inner.capacity && !self.inner.closed.load(Ordering::Acquire) {
            self.inner
                .stats
                .lock()
                .expect("query output port stats lock poisoned")
                .blocked_pushes += 1;
            chunks = self
                .inner
                .cv
                .wait(chunks)
                .expect("query output port condvar poisoned");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputWrite::Blocked(chunk);
        }
        chunks.push_back(chunk);
        let queue_len = chunks.len();
        drop(chunks);

        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned");
        stats.pushed_chunks += 1;
        stats.pushed_rows += rows;
        stats.peak_queue_chunks = stats.peak_queue_chunks.max(queue_len);
        self.inner.cv.notify_all();
        QueryOutputWrite::Written
    }

    fn try_push_discarding(&self, chunk: Chunk) -> QueryOutputWrite {
        let rows = chunk.size();
        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned");
        stats.pushed_chunks += 1;
        stats.pushed_rows += rows;
        QueryOutputWrite::Written
    }

    #[inline]
    pub fn try_push_reference(&self, chunk: &Chunk) -> QueryOutputReferenceWrite {
        match self.inner.mode {
            QueryOutputPortMode::Buffered {
                collect_stats: false,
            } => self.try_push_reference_buffered(chunk),
            QueryOutputPortMode::Buffered {
                collect_stats: true,
            } => self.try_push_reference_buffered_with_stats(chunk),
            QueryOutputPortMode::BlockingBuffered {
                collect_stats: false,
            } => self.push_reference_blocking_buffered(chunk),
            QueryOutputPortMode::BlockingBuffered {
                collect_stats: true,
            } => self.push_reference_blocking_buffered_with_stats(chunk),
            QueryOutputPortMode::Discarding => self.try_push_reference_discarding(chunk),
        }
    }

    #[inline]
    fn try_push_reference_buffered(&self, chunk: &Chunk) -> QueryOutputReferenceWrite {
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputReferenceWrite::Blocked;
        }
        if chunks.len() >= self.inner.capacity {
            return QueryOutputReferenceWrite::Blocked;
        }
        chunks.push_back(chunk.clone_referencing_vectors());
        self.inner.cv.notify_all();
        QueryOutputReferenceWrite::Written
    }

    fn try_push_reference_buffered_with_stats(&self, chunk: &Chunk) -> QueryOutputReferenceWrite {
        let rows = chunk.size();
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputReferenceWrite::Blocked;
        }
        if chunks.len() >= self.inner.capacity {
            self.inner
                .stats
                .lock()
                .expect("query output port stats lock poisoned")
                .blocked_pushes += 1;
            return QueryOutputReferenceWrite::Blocked;
        }
        chunks.push_back(chunk.clone_referencing_vectors());
        let queue_len = chunks.len();
        drop(chunks);

        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned");
        stats.pushed_chunks += 1;
        stats.pushed_rows += rows;
        stats.peak_queue_chunks = stats.peak_queue_chunks.max(queue_len);
        self.inner.cv.notify_all();
        QueryOutputReferenceWrite::Written
    }

    fn push_reference_blocking_buffered(&self, chunk: &Chunk) -> QueryOutputReferenceWrite {
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        while chunks.len() >= self.inner.capacity && !self.inner.closed.load(Ordering::Acquire) {
            chunks = self
                .inner
                .cv
                .wait(chunks)
                .expect("query output port condvar poisoned");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputReferenceWrite::Blocked;
        }
        chunks.push_back(chunk.clone_referencing_vectors());
        self.inner.cv.notify_all();
        QueryOutputReferenceWrite::Written
    }

    fn push_reference_blocking_buffered_with_stats(
        &self,
        chunk: &Chunk,
    ) -> QueryOutputReferenceWrite {
        let rows = chunk.size();
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        while chunks.len() >= self.inner.capacity && !self.inner.closed.load(Ordering::Acquire) {
            self.inner
                .stats
                .lock()
                .expect("query output port stats lock poisoned")
                .blocked_pushes += 1;
            chunks = self
                .inner
                .cv
                .wait(chunks)
                .expect("query output port condvar poisoned");
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return QueryOutputReferenceWrite::Blocked;
        }
        chunks.push_back(chunk.clone_referencing_vectors());
        let queue_len = chunks.len();
        drop(chunks);

        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned");
        stats.pushed_chunks += 1;
        stats.pushed_rows += rows;
        stats.peak_queue_chunks = stats.peak_queue_chunks.max(queue_len);
        self.inner.cv.notify_all();
        QueryOutputReferenceWrite::Written
    }

    fn try_push_reference_discarding(&self, chunk: &Chunk) -> QueryOutputReferenceWrite {
        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned");
        stats.pushed_chunks += 1;
        stats.pushed_rows += chunk.size();
        QueryOutputReferenceWrite::Written
    }

    #[inline]
    pub fn pop_front(&self) -> Option<Chunk> {
        match self.inner.mode {
            QueryOutputPortMode::Buffered {
                collect_stats: false,
            }
            | QueryOutputPortMode::BlockingBuffered {
                collect_stats: false,
            } => self.pop_front_buffered(),
            QueryOutputPortMode::Buffered {
                collect_stats: true,
            }
            | QueryOutputPortMode::BlockingBuffered {
                collect_stats: true,
            } => self.pop_front_buffered_with_stats(),
            QueryOutputPortMode::Discarding => None,
        }
    }

    #[inline]
    fn pop_front_buffered(&self) -> Option<Chunk> {
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        let chunk = chunks.pop_front();
        if chunk.is_some() {
            self.inner.generation.fetch_add(1, Ordering::AcqRel);
            self.inner.cv.notify_all();
        }
        chunk
    }

    fn pop_front_buffered_with_stats(&self) -> Option<Chunk> {
        let mut chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        let chunk = chunks.pop_front();
        let rows = chunk.as_ref().map(Chunk::size);
        drop(chunks);

        if let Some(rows) = rows {
            let mut stats = self
                .inner
                .stats
                .lock()
                .expect("query output port stats lock poisoned");
            stats.popped_chunks += 1;
            stats.popped_rows += rows;
            self.inner.generation.fetch_add(1, Ordering::AcqRel);
            self.inner.cv.notify_all();
        }
        chunk
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.cv.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    pub fn wait_for_change_timeout(&self, timeout: Duration) {
        let chunks = self
            .inner
            .chunks
            .lock()
            .expect("query output port lock poisoned");
        if !chunks.is_empty() || self.is_closed() {
            return;
        }
        let _ = self
            .inner
            .cv
            .wait_timeout(chunks, timeout)
            .expect("query output port condvar poisoned");
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn len(&self) -> usize {
        if matches!(self.inner.mode, QueryOutputPortMode::Discarding) {
            return 0;
        }
        self.inner
            .chunks
            .lock()
            .expect("query output port lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn wake_generation(&self) -> WakeGeneration {
        WakeGeneration(self.inner.generation.load(Ordering::Acquire))
    }

    pub fn wake_token(&self) -> WakeToken {
        WakeToken(0)
    }

    pub fn stats(&self) -> QueryOutputPortStats {
        self.inner
            .stats
            .lock()
            .expect("query output port stats lock poisoned")
            .to_owned()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryOutputPortStats {
    pub pushed_chunks: usize,
    pub pushed_rows: usize,
    pub popped_chunks: usize,
    pub popped_rows: usize,
    pub blocked_pushes: usize,
    pub peak_queue_chunks: usize,
}

#[derive(Debug)]
pub enum QueryOutputWrite {
    Written,
    Blocked(Chunk),
}

#[derive(Debug)]
pub enum QueryOutputReferenceWrite {
    Written,
    Blocked,
}

#[derive(Debug)]
struct QueryOutputPortInner {
    capacity: usize,
    mode: QueryOutputPortMode,
    // Current fetch-driven execution has one driver and one client fetcher, so
    // this small mutexed queue keeps the root output path simple. A future
    // worker-pool driver with concurrent root producers should replace it with
    // a scheduler-aware MPSC/ring buffer instead of extending this lock.
    chunks: Mutex<VecDeque<Chunk>>,
    stats: Mutex<QueryOutputPortStats>,
    generation: AtomicU64,
    closed: AtomicBool,
    cv: Condvar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryOutputPortMode {
    Buffered { collect_stats: bool },
    BlockingBuffered { collect_stats: bool },
    Discarding,
}

#[derive(Debug, Clone, Default)]
pub struct QueryErrorRegistry {
    inner: Arc<Mutex<QueryErrorRegistryInner>>,
}

impl QueryErrorRegistry {
    pub fn record_root(&self, error: ParoError) -> QueryErrorId {
        let mut inner = self
            .inner
            .lock()
            .expect("query error registry lock poisoned");
        if let Some((id, _)) = &inner.root {
            return *id;
        }
        let id = QueryErrorId::new(inner.next_id);
        inner.next_id += 1;
        inner.root = Some((id, error));
        id
    }

    pub fn record_secondary(&self, error: ParoError) {
        self.inner
            .lock()
            .expect("query error registry lock poisoned")
            .secondary
            .push(error);
    }

    pub fn root_error_id(&self) -> Option<QueryErrorId> {
        self.inner
            .lock()
            .expect("query error registry lock poisoned")
            .root
            .as_ref()
            .map(|(id, _)| *id)
    }

    pub fn root_error(&self) -> Option<ParoError> {
        self.inner
            .lock()
            .expect("query error registry lock poisoned")
            .root
            .as_ref()
            .map(|(_, error)| error.clone())
    }

    pub fn secondary_count(&self) -> usize {
        self.inner
            .lock()
            .expect("query error registry lock poisoned")
            .secondary
            .len()
    }
}

#[derive(Debug, Default)]
struct QueryErrorRegistryInner {
    next_id: u64,
    root: Option<(QueryErrorId, ParoError)>,
    secondary: Vec<ParoError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryErrorId(u64);

impl QueryErrorId {
    pub const UNKNOWN: Self = Self(u64::MAX);

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryProfilerRegistry;

pub struct OperatorScratchScope<'a> {
    expression: &'a mut ExpressionScratchArena,
    generation: u64,
}

impl<'a> OperatorScratchScope<'a> {
    #[inline]
    pub fn new(task: &'a mut PipelineScratch) -> Self {
        let generation = task.expression.begin_call();
        Self {
            expression: &mut task.expression,
            generation,
        }
    }

    #[inline(always)]
    pub fn from_expression(expression: &'a mut ExpressionScratchArena) -> Self {
        let generation = expression.begin_call();
        Self {
            expression,
            generation,
        }
    }

    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[inline(always)]
    pub fn expr(&mut self) -> ExpressionScratchLease<'_> {
        self.expression.lease()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorWakeScope {
    pub task_id: PipelineTaskId,
    pub generation: WakeGeneration,
}

impl OperatorWakeScope {
    pub fn register(&self, source: WakeSource, token: WakeToken) -> PendingWakeRegistration {
        PendingWakeRegistration {
            task_id: self.task_id,
            source,
            token,
            generation: self.generation,
        }
    }
}

impl PendingWakeRegistration {
    pub fn key(&self) -> WakeKey {
        WakeKey {
            source: self.source,
            token: self.token,
            generation: self.generation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WakeKey {
    pub source: WakeSource,
    pub token: WakeToken,
    pub generation: WakeGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineTaskId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FinishTaskId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WakeGeneration(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WakeToken(pub u64);

impl WakeToken {
    const NAMESPACE_SHIFT: u64 = 56;
    const PAYLOAD_MASK: u64 = (1u64 << Self::NAMESPACE_SHIFT) - 1;
    const EXTERNAL_TABLE_NAMESPACE: u8 = 1;
    const EXTERNAL_OPERATOR_BATCH_NAMESPACE: u8 = 2;

    #[inline]
    fn namespaced(namespace: u8, payload: u64) -> Self {
        Self(((namespace as u64) << Self::NAMESPACE_SHIFT) | (payload & Self::PAYLOAD_MASK))
    }

    #[inline]
    pub fn external_table(handle_id: usize) -> Self {
        Self::namespaced(Self::EXTERNAL_TABLE_NAMESPACE, handle_id as u64)
    }

    #[inline]
    pub fn external_operator_batch(operator: RuntimeOperatorId, batch_id: u64) -> Self {
        let operator_id = (operator.index() as u64 & 0x00ff_ffff) << 32;
        Self::namespaced(
            Self::EXTERNAL_OPERATOR_BATCH_NAMESPACE,
            operator_id | (batch_id & 0xffff_ffff),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WakeSource {
    Memory,
    Spill,
    ExternalRuntime,
    DerivedIndex,
    OutputBuffer,
    Cancellation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingWakeRegistration {
    pub task_id: PipelineTaskId,
    pub source: WakeSource,
    pub token: WakeToken,
    pub generation: WakeGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocker {
    pub reason: BlockReason,
    pub wake: Option<PendingWakeRegistration>,
    pub retained_memory: RetainedMemorySnapshot,
}

impl Blocker {
    pub fn new(reason: BlockReason) -> Self {
        Self {
            reason,
            wake: None,
            retained_memory: RetainedMemorySnapshot::default(),
        }
    }

    pub fn with_wake(mut self, wake: PendingWakeRegistration) -> Self {
        self.wake = Some(wake);
        self
    }

    pub fn output_backpressure(wake: &OperatorWakeScope, port: &QueryOutputPort) -> Self {
        Self::new(BlockReason::OutputBackpressure).with_wake(PendingWakeRegistration {
            task_id: wake.task_id,
            source: WakeSource::OutputBuffer,
            token: port.wake_token(),
            generation: port.wake_generation(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockReason {
    Memory,
    Spill,
    ExternalRuntime,
    DerivedIndex,
    OutputBackpressure,
    CancelCheck,
    Other(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetainedMemorySnapshot {
    pub bytes: usize,
}

pub fn check_cancelled(cancel: &StatementCancellation) -> Result<()> {
    cancel.check()
}

#[cfg(test)]
mod tests {
    use paro_common::error as paro_error;

    use super::*;

    #[test]
    fn output_port_reports_backpressure_and_wakes_on_pop() {
        let port = QueryOutputPort::bounded(1);
        let first = paro_common::test_utils::test_chunk(&[]);
        let second = paro_common::test_utils::test_chunk(&[]);
        let initial_generation = port.wake_generation();

        assert!(matches!(port.try_push(first), QueryOutputWrite::Written));
        assert!(matches!(
            port.try_push(second),
            QueryOutputWrite::Blocked(_)
        ));
        assert_eq!(port.len(), 1);

        assert!(port.pop_front().is_some());
        assert_ne!(port.wake_generation(), initial_generation);
    }

    #[test]
    fn discarding_output_port_counts_rows_without_buffering_chunks() {
        let port = QueryOutputPort::discarding();
        let mut first =
            Chunk::try_initialize(&[], 3, paro_common::test_utils::test_allocator()).unwrap();
        first.try_set_cardinality(3).unwrap();
        let mut second =
            Chunk::try_initialize(&[], 2, paro_common::test_utils::test_allocator()).unwrap();
        second.try_set_cardinality(2).unwrap();

        assert!(matches!(port.try_push(first), QueryOutputWrite::Written));
        assert!(matches!(port.try_push(second), QueryOutputWrite::Written));

        assert_eq!(port.capacity(), 0);
        assert_eq!(port.len(), 0);
        assert!(port.pop_front().is_none());
        assert_eq!(
            port.stats(),
            QueryOutputPortStats {
                pushed_chunks: 2,
                pushed_rows: 5,
                popped_chunks: 0,
                popped_rows: 0,
                blocked_pushes: 0,
                peak_queue_chunks: 0,
            }
        );
    }

    #[test]
    fn external_runtime_wake_tokens_are_namespaced() {
        let table = WakeToken::external_table(7);
        let batch = WakeToken::external_operator_batch(RuntimeOperatorId::new(7), 7);
        let next_batch = WakeToken::external_operator_batch(RuntimeOperatorId::new(7), 8);
        let next_operator = WakeToken::external_operator_batch(RuntimeOperatorId::new(8), 7);

        assert_ne!(table, batch);
        assert_ne!(batch, next_batch);
        assert_ne!(batch, next_operator);
        assert_eq!(
            batch,
            WakeToken::external_operator_batch(RuntimeOperatorId::new(7), 7)
        );
    }

    #[test]
    fn query_wake_registry_tracks_ready_wake_keys() {
        let registry = QueryWakeRegistry::default();
        let key = WakeKey {
            source: WakeSource::Memory,
            token: WakeToken(42),
            generation: WakeGeneration(7),
        };

        assert!(!registry.is_ready(key));
        registry.wake(key);
        assert!(registry.is_ready(key));
        assert!(registry.take_ready(key));
        assert!(!registry.take_ready(key));
    }

    #[test]
    fn query_wake_registry_counts_duplicate_ready_wakes() {
        let registry = QueryWakeRegistry::default();
        let key = WakeKey {
            source: WakeSource::Memory,
            token: WakeToken(42),
            generation: WakeGeneration(7),
        };

        registry.wake(key);
        registry.wake(key);
        registry.wake(key);

        assert_eq!(registry.take_ready_with_coalesced(key), Some(2));
        assert_eq!(registry.take_ready_with_coalesced(key), None);
    }

    #[test]
    fn error_registry_keeps_first_error_and_records_secondary_errors() {
        let registry = QueryErrorRegistry::default();
        let first = registry.record_root(paro_error::internal("first"));
        let second = registry.record_root(paro_error::internal("second"));
        registry.record_secondary(paro_error::internal("cleanup"));

        assert_eq!(first, second);
        assert_eq!(registry.root_error_id(), Some(first));
        assert_eq!(registry.secondary_count(), 1);
        assert!(registry
            .root_error()
            .expect("root error")
            .to_string()
            .contains("first"));
    }

    #[test]
    fn scratch_scope_advances_generation_once_per_operator_call() {
        let mut arena = ExpressionScratchArena::default();

        {
            let mut first = OperatorScratchScope::from_expression(&mut arena);
            assert_eq!(first.generation(), 1);
            assert_eq!(first.expr().generation(), 1);
            assert_eq!(first.expr().generation(), 1);
        }

        let mut second = OperatorScratchScope::from_expression(&mut arena);
        assert_eq!(second.generation(), 2);
        assert_eq!(second.expr().generation(), 2);
    }
}
