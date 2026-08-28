// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::publish_frontier::{ApplyFrontier, PublishFrontier};
use crate::runtime::JournalFrontierSnapshot;
use crate::waiter::WaitMode;
use paro_common::error as paro_error;
use paro_common::error::{ParoError, Result};
use paro_common::journal::{JournalPublicationWatermarks, RecoverySummary};
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};
use std::thread;
use std::time::Instant;

type ApplyWork = Box<dyn FnOnce() -> Result<()> + Send + 'static>;
type PublishedHook = Box<dyn FnOnce() -> Result<()> + Send + 'static>;

/// Database-owned observer for the exact prefix published by the ordered
/// journal apply runtime. Checkpointing must consume this source rather than
/// independently reconstructing LSN order from transaction callbacks.
pub trait JournalPublicationObserver: Send + Sync + std::fmt::Debug + 'static {
    fn record_durable(&self, durable_lsn: u64);

    fn record_published(&self, lsn: u64, watermarks: JournalPublicationWatermarks) -> Result<()>;
}

pub type ApplyCompletion =
    Box<dyn FnOnce(std::result::Result<(), JournalApplyError>) + Send + 'static>;
pub type ApplyCompletionFallbackAck = Box<dyn FnOnce(&JournalApplyError) + Send + 'static>;
pub type ApplyFatalSink = Box<dyn FnOnce(&JournalApplyError) + Send + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplyPhase {
    Runtime,
    CatalogPre,
    TabletParts,
    Descriptor,
    CatalogPost,
    Published,
    Completion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplyErrorSource {
    ApplyClosure,
    PublishedHook,
    WorkerPanic,
    CompletionCallback,
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalApplyError {
    pub phase: ApplyPhase,
    pub source: ApplyErrorSource,
    pub lsn: u64,
    pub commit_id: Option<u64>,
    pub error_code: u32,
    pub message: Arc<str>,
}

impl JournalApplyError {
    pub fn apply_failed(
        phase: ApplyPhase,
        source: ApplyErrorSource,
        lsn: u64,
        commit_id: Option<u64>,
        err: &ParoError,
    ) -> Self {
        Self::new(phase, source, lsn, commit_id, err.to_string())
    }

    pub fn worker_panic(
        phase: ApplyPhase,
        lsn: u64,
        commit_id: Option<u64>,
        panic: Box<dyn Any + Send + 'static>,
    ) -> Self {
        Self::new(
            phase,
            ApplyErrorSource::WorkerPanic,
            lsn,
            commit_id,
            panic_payload_message(panic),
        )
    }

    pub fn completion_panic(
        lsn: u64,
        commit_id: Option<u64>,
        panic: Box<dyn Any + Send + 'static>,
    ) -> Self {
        Self::new(
            ApplyPhase::Completion,
            ApplyErrorSource::CompletionCallback,
            lsn,
            commit_id,
            panic_payload_message(panic),
        )
    }

    pub fn runtime_unavailable(
        lsn: u64,
        commit_id: Option<u64>,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self::new(
            ApplyPhase::Runtime,
            ApplyErrorSource::Runtime,
            lsn,
            commit_id,
            message,
        )
    }

    pub fn new(
        phase: ApplyPhase,
        source: ApplyErrorSource,
        lsn: u64,
        commit_id: Option<u64>,
        message: impl Into<Arc<str>>,
    ) -> Self {
        let error_code = apply_error_code(phase, source);
        Self {
            phase,
            source,
            lsn,
            commit_id,
            error_code,
            message: message.into(),
        }
    }

    pub fn to_paro_error(&self) -> ParoError {
        paro_error::internal(self.to_string())
    }
}

impl std::fmt::Display for JournalApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "journal apply {:?}/{:?} failed at lsn {} commit_id {:?}: {}",
            self.phase, self.source, self.lsn, self.commit_id, self.message
        )
    }
}

impl std::error::Error for JournalApplyError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyRuntimeError {
    RuntimeUnavailable { message: Arc<str> },
    Fatal { message: Arc<str> },
}

impl ApplyRuntimeError {
    fn runtime_unavailable(message: impl Into<Arc<str>>) -> Self {
        Self::RuntimeUnavailable {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ApplyRuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeUnavailable { message } => {
                write!(f, "journal apply runtime unavailable: {message}")
            }
            Self::Fatal { message } => write!(f, "journal apply runtime fatal: {message}"),
        }
    }
}

impl std::error::Error for ApplyRuntimeError {}

#[derive(Clone, Default)]
struct SharedApplyFailure {
    inner: Arc<Mutex<Option<JournalApplyError>>>,
}

impl SharedApplyFailure {
    fn record(&self, error: JournalApplyError) {
        let mut guard = self.inner.lock().unwrap();
        if guard.is_none() {
            *guard = Some(error);
        }
    }

    fn take_or_runtime(
        &self,
        lsn: u64,
        commit_id: Option<u64>,
        err: &ParoError,
    ) -> JournalApplyError {
        self.inner.lock().unwrap().clone().unwrap_or_else(|| {
            JournalApplyError::runtime_unavailable(lsn, commit_id, err.to_string())
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JournalApplyMetricsSnapshot {
    pub queue_depth: u64,
    pub queue_depth_peak: u64,
    pub async_submit_queue_depth: u64,
    pub async_submit_queue_depth_peak: u64,
    pub active_workers: u64,
    pub active_workers_peak: u64,
    pub mailbox_count: u64,
    pub durable_lsn: u64,
    pub applied_lsn: u64,
    pub published_lsn: u64,
    pub applied_lag: u64,
    pub published_lag: u64,
    pub durable_wait_count: u64,
    pub durable_wait_micros: u64,
    pub applied_wait_count: u64,
    pub applied_wait_micros: u64,
    pub published_wait_count: u64,
    pub published_wait_micros: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryPlaceholderRecordKind {
    Maintenance,
    CheckpointFence,
    Other,
}

impl std::fmt::Display for RecoveryPlaceholderRecordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Maintenance => write!(f, "maintenance"),
            Self::CheckpointFence => write!(f, "checkpoint_fence"),
            Self::Other => write!(f, "other"),
        }
    }
}

#[derive(Debug)]
pub struct ApplySubmitResult<R> {
    pub value: R,
    pub wait_micros: u64,
}

pub struct TabletApplyPart {
    pub tablet_id: u64,
    pub apply: ApplyWork,
}

pub struct ApplyRequest<R> {
    pub lsn: u64,
    pub durable_batch_lsn: u64,
    pub commit_id: Option<u64>,
    pub publication_watermarks: JournalPublicationWatermarks,
    pub wait_mode: WaitMode,
    pub catalog_serial: bool,
    pub catalog_pre: ApplyWork,
    pub tablet_parts: Vec<TabletApplyPart>,
    pub descriptor_phase: ApplyWork,
    pub catalog_post: Box<dyn FnOnce() -> Result<R> + Send + 'static>,
    pub on_published: PublishedHook,
}

#[derive(Debug, Clone, Copy)]
struct ApplyRecordMetadata {
    raw_lsn: u64,
    durable_batch_lsn: u64,
    commit_id: Option<u64>,
    publication_watermarks: JournalPublicationWatermarks,
}

pub struct JournalApplyRuntime {
    inner: Arc<JournalApplyRuntimeInner>,
}

impl std::fmt::Debug for JournalApplyRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalApplyRuntime")
            .field("frontiers", &self.frontiers())
            .finish()
    }
}

impl Default for JournalApplyRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl JournalApplyRuntime {
    pub fn new() -> Self {
        let inner = Arc::new(JournalApplyRuntimeInner {
            state: Mutex::new(ApplyRuntimeState::default()),
            catalog_lane: Mutex::new(()),
            dispatch_wake: Condvar::new(),
            async_submit: Mutex::new(AsyncSubmitState::default()),
            async_submit_wake: Condvar::new(),
            tablet_dispatch: Mutex::new(TabletDispatchState::default()),
            tablet_wake: Condvar::new(),
            metrics: ApplyRuntimeMetrics::default(),
            publication_observer: RwLock::new(None),
        });
        let runtime = Self {
            inner: Arc::clone(&inner),
        };
        for worker_id in 0..default_apply_worker_count() {
            let worker_inner = Arc::downgrade(&inner);
            thread::Builder::new()
                .name(format!("paro-tablet-apply-{worker_id}"))
                .spawn(move || run_tablet_worker(worker_inner))
                .expect("spawn tablet apply worker");
        }
        let submit_inner = Arc::downgrade(&inner);
        thread::Builder::new()
            .name("paro-journal-apply-submit".to_string())
            .spawn(move || run_async_submit_worker(submit_inner))
            .expect("spawn journal apply submit worker");
        runtime
    }

    pub fn frontiers(&self) -> JournalFrontierSnapshot {
        self.inner.state.lock().unwrap().frontiers
    }

    pub fn bind_publication_observer(&self, observer: Arc<dyn JournalPublicationObserver>) {
        *self.inner.publication_observer.write().unwrap() = Some(observer);
    }

    pub fn metrics(&self) -> JournalApplyMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    pub fn next_dispatch_lsn(&self) -> u64 {
        self.inner.state.lock().unwrap().next_dispatch_lsn
    }

    pub fn bootstrap_frontiers(&self, summary: RecoverySummary) {
        let mut state = self.inner.state.lock().unwrap();
        state.next_dispatch_lsn = summary.max_lsn.saturating_add(1).max(1);
        state.next_ephemeral_lsn = summary.max_lsn.saturating_add(1).max(1);
        state.frontiers.durable_lsn = summary.max_lsn;
        state.frontiers.applied_lsn = summary.max_lsn;
        state.frontiers.published_lsn = summary.max_lsn;
        state.apply_frontier.bootstrap(summary.max_lsn);
        state.publish_frontier.bootstrap(summary.max_lsn);
    }

    pub fn note_durable_append(&self, durable_lsn: u64) {
        let mut state = self.inner.state.lock().unwrap();
        state.frontiers.durable_lsn = state.frontiers.durable_lsn.max(durable_lsn);
    }

    pub fn advance_dispatch_past_placeholder(
        &self,
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
    ) -> Result<()> {
        self.inner
            .advance_dispatch_past_placeholder(lsn, record_kind)
    }

    pub fn advance_dispatch_gap_before(
        &self,
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
    ) -> Result<()> {
        self.inner.advance_dispatch_gap_before(lsn, record_kind)
    }

    pub fn submit<R>(&self, request: ApplyRequest<R>) -> Result<R> {
        self.submit_observed(request).map(|observed| observed.value)
    }

    pub fn submit_async(&self, request: ApplyRequest<()>) -> Result<()> {
        self.inner
            .enqueue_async_submit(AsyncSubmitWork::Legacy(request))
    }

    pub fn submit_async_with_completion(
        &self,
        mut request: ApplyRequest<()>,
        on_complete: ApplyCompletion,
        fallback_ack: ApplyCompletionFallbackAck,
        fatal_sink: ApplyFatalSink,
    ) -> std::result::Result<(), ApplyRuntimeError> {
        request.wait_mode = WaitMode::AsyncCompletion;
        self.inner
            .enqueue_async_submit(AsyncSubmitWork::WithCompletion {
                request,
                on_complete,
                fallback_ack,
                fatal_sink,
            })
            .map_err(|err| ApplyRuntimeError::runtime_unavailable(err.to_string()))
    }

    pub fn submit_observed<R>(&self, request: ApplyRequest<R>) -> Result<ApplySubmitResult<R>> {
        self.submit_observed_tracked(request, None)
    }

    fn submit_observed_tracked<R>(
        &self,
        request: ApplyRequest<R>,
        failure: Option<SharedApplyFailure>,
    ) -> Result<ApplySubmitResult<R>> {
        let started_at = Instant::now();
        let ApplyRequest {
            lsn,
            durable_batch_lsn,
            commit_id,
            publication_watermarks,
            wait_mode,
            catalog_serial,
            catalog_pre,
            tablet_parts,
            descriptor_phase,
            catalog_post,
            on_published,
        } = request;

        let ticket = self.inner.register_record(
            ApplyRecordMetadata {
                raw_lsn: lsn,
                durable_batch_lsn,
                commit_id,
                publication_watermarks,
            },
            tablet_parts.len(),
            on_published,
            failure.clone(),
        )?;

        let record_wait_metrics = |result: Result<R>| {
            let wait_micros = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
            self.inner.record_wait(wait_mode, wait_micros);
            result.map(|value| ApplySubmitResult { value, wait_micros })
        };

        if let Err(err) = self.inner.wait_for_dispatch_turn(ticket.lsn) {
            self.inner.fail_record(&ticket, err.clone());
            return record_wait_metrics(Err(err));
        }

        let _catalog_lane = if catalog_serial {
            Some(self.inner.catalog_lane.lock().unwrap())
        } else {
            None
        };

        if let Err(err) = call_apply_work(
            catalog_pre,
            ApplyPhase::CatalogPre,
            ApplyErrorSource::ApplyClosure,
            lsn,
            commit_id,
            failure.as_ref(),
        ) {
            self.inner.fail_record(&ticket, err.clone());
            return record_wait_metrics(Err(err));
        }

        if let Err(err) = self
            .inner
            .enqueue_tablet_parts(&ticket, tablet_parts, failure.clone())
        {
            self.inner.fail_record(&ticket, err.clone());
            return record_wait_metrics(Err(err));
        }
        self.inner.finish_dispatch_turn(ticket.lsn);

        if let Err(err) = ticket.wait_for_tablet_phase(&self.inner) {
            self.inner.fail_record(&ticket, err.clone());
            return record_wait_metrics(Err(err));
        }

        if let Err(err) = call_apply_work(
            descriptor_phase,
            ApplyPhase::Descriptor,
            ApplyErrorSource::ApplyClosure,
            lsn,
            commit_id,
            failure.as_ref(),
        ) {
            self.inner.fail_record(&ticket, err.clone());
            return record_wait_metrics(Err(err));
        }

        let result = match call_catalog_post(
            catalog_post,
            ApplyPhase::CatalogPost,
            ApplyErrorSource::ApplyClosure,
            lsn,
            commit_id,
            failure.as_ref(),
        ) {
            Ok(result) => result,
            Err(err) => {
                self.inner.fail_record(&ticket, err.clone());
                return record_wait_metrics(Err(err));
            }
        };

        self.inner.mark_applied(&ticket)?;
        if matches!(wait_mode, WaitMode::Published | WaitMode::AsyncCompletion) {
            ticket.wait_for_published(&self.inner)?;
        }

        record_wait_metrics(Ok(result))
    }
}

impl Clone for JournalApplyRuntime {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for JournalApplyRuntime {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let mut state = self.inner.tablet_dispatch.lock().unwrap();
            state.shutdown = true;
            self.inner.tablet_wake.notify_all();
            let mut state = self.inner.async_submit.lock().unwrap();
            state.shutdown = true;
            self.inner.async_submit_wake.notify_all();
        }
    }
}

struct ApplyRuntimeState {
    next_dispatch_lsn: u64,
    next_ephemeral_lsn: u64,
    frontiers: JournalFrontierSnapshot,
    apply_frontier: ApplyFrontier,
    publish_frontier: PublishFrontier,
    records: HashMap<u64, Arc<RecordTicket>>,
    poisoned: Option<ParoError>,
}

impl Default for ApplyRuntimeState {
    fn default() -> Self {
        Self {
            next_dispatch_lsn: 1,
            next_ephemeral_lsn: 1,
            frontiers: JournalFrontierSnapshot::default(),
            apply_frontier: ApplyFrontier::default(),
            publish_frontier: PublishFrontier::default(),
            records: HashMap::new(),
            poisoned: None,
        }
    }
}

struct JournalApplyRuntimeInner {
    state: Mutex<ApplyRuntimeState>,
    catalog_lane: Mutex<()>,
    dispatch_wake: Condvar,
    async_submit: Mutex<AsyncSubmitState>,
    async_submit_wake: Condvar,
    tablet_dispatch: Mutex<TabletDispatchState>,
    tablet_wake: Condvar,
    metrics: ApplyRuntimeMetrics,
    publication_observer: RwLock<Option<Arc<dyn JournalPublicationObserver>>>,
}

impl JournalApplyRuntimeInner {
    fn metrics_snapshot(&self) -> JournalApplyMetricsSnapshot {
        let frontiers = self.state.lock().unwrap().frontiers;
        JournalApplyMetricsSnapshot {
            queue_depth: self.metrics.queue_depth.load(Ordering::Relaxed),
            queue_depth_peak: self.metrics.queue_depth_peak.load(Ordering::Relaxed),
            async_submit_queue_depth: self
                .metrics
                .async_submit_queue_depth
                .load(Ordering::Relaxed),
            async_submit_queue_depth_peak: self
                .metrics
                .async_submit_queue_depth_peak
                .load(Ordering::Relaxed),
            active_workers: self.metrics.active_workers.load(Ordering::Relaxed),
            active_workers_peak: self.metrics.active_workers_peak.load(Ordering::Relaxed),
            mailbox_count: self.metrics.mailbox_count.load(Ordering::Relaxed),
            durable_lsn: frontiers.durable_lsn,
            applied_lsn: frontiers.applied_lsn,
            published_lsn: frontiers.published_lsn,
            applied_lag: frontiers.durable_lsn.saturating_sub(frontiers.applied_lsn),
            published_lag: frontiers
                .durable_lsn
                .saturating_sub(frontiers.published_lsn),
            durable_wait_count: self.metrics.durable_wait_count.load(Ordering::Relaxed),
            durable_wait_micros: self.metrics.durable_wait_micros.load(Ordering::Relaxed),
            applied_wait_count: self.metrics.applied_wait_count.load(Ordering::Relaxed),
            applied_wait_micros: self.metrics.applied_wait_micros.load(Ordering::Relaxed),
            published_wait_count: self.metrics.published_wait_count.load(Ordering::Relaxed),
            published_wait_micros: self.metrics.published_wait_micros.load(Ordering::Relaxed),
        }
    }

    fn register_record(
        &self,
        metadata: ApplyRecordMetadata,
        tablet_parts: usize,
        on_published: PublishedHook,
        failure: Option<SharedApplyFailure>,
    ) -> Result<Arc<RecordTicket>> {
        let ApplyRecordMetadata {
            raw_lsn,
            durable_batch_lsn,
            commit_id,
            publication_watermarks,
        } = metadata;
        let mut state = self.state.lock().unwrap();
        if let Some(err) = state.poisoned.clone() {
            return Err(err);
        }
        let lsn = if raw_lsn == 0 {
            let lsn = state.next_ephemeral_lsn;
            state.next_ephemeral_lsn = state.next_ephemeral_lsn.saturating_add(1);
            lsn
        } else {
            state.next_ephemeral_lsn = state.next_ephemeral_lsn.max(raw_lsn.saturating_add(1));
            raw_lsn
        };
        if lsn < state.next_dispatch_lsn {
            return Err(paro_error::internal(format!(
                "journal apply lsn {} is below next dispatch frontier {}",
                lsn, state.next_dispatch_lsn
            )));
        }
        let ticket = Arc::new(RecordTicket::new(
            lsn,
            commit_id,
            publication_watermarks,
            tablet_parts as u32,
            on_published,
            failure,
        ));
        state.frontiers.durable_lsn = state.frontiers.durable_lsn.max(durable_batch_lsn);
        state.records.insert(lsn, Arc::clone(&ticket));
        Ok(ticket)
    }

    fn wait_for_dispatch_turn(&self, lsn: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(err) = state.poisoned.clone() {
                return Err(err);
            }
            if state.next_dispatch_lsn == lsn {
                return Ok(());
            }
            state = self.dispatch_wake.wait(state).unwrap();
        }
    }

    fn finish_dispatch_turn(&self, lsn: u64) {
        let mut state = self.state.lock().unwrap();
        if state.next_dispatch_lsn == lsn {
            state.next_dispatch_lsn = state.next_dispatch_lsn.saturating_add(1);
        }
        self.dispatch_wake.notify_all();
    }

    fn enqueue_tablet_parts(
        self: &Arc<Self>,
        ticket: &Arc<RecordTicket>,
        tablet_parts: Vec<TabletApplyPart>,
        failure: Option<SharedApplyFailure>,
    ) -> Result<()> {
        if tablet_parts.is_empty() {
            ticket.notify_zero_tablet_parts();
            return Ok(());
        }

        self.metrics
            .increment_queue_depth(tablet_parts.len() as u64);
        for part in tablet_parts {
            self.enqueue_tablet_part(
                part.tablet_id,
                TabletApplyWork {
                    apply: Some(part.apply),
                    runtime: Arc::downgrade(self),
                    ticket: Arc::clone(ticket),
                    failure: failure.clone(),
                },
            )?;
        }
        Ok(())
    }

    fn enqueue_async_submit(&self, request: AsyncSubmitWork) -> Result<()> {
        let mut state = self.async_submit.lock().unwrap();
        if state.shutdown {
            return Err(paro_error::internal(
                "journal apply runtime is shutting down",
            ));
        }
        state.queue.push_back(request);
        self.metrics.increment_async_submit_queue_depth();
        self.async_submit_wake.notify_one();
        Ok(())
    }

    fn dequeue_async_submit(&self) -> Option<AsyncSubmitWork> {
        let mut state = self.async_submit.lock().unwrap();
        loop {
            if let Some(request) = state.queue.pop_front() {
                self.metrics.decrement_async_submit_queue_depth();
                return Some(request);
            }
            if state.shutdown {
                return None;
            }
            state = self.async_submit_wake.wait(state).unwrap();
        }
    }

    fn enqueue_tablet_part(&self, tablet_id: u64, work: TabletApplyWork) -> Result<()> {
        let mut state = self.tablet_dispatch.lock().unwrap();
        let mut created = false;
        let mailbox = state.mailboxes.entry(tablet_id).or_insert_with(|| {
            created = true;
            TabletMailbox::default()
        });
        if created {
            self.metrics.increment_mailbox_count();
        }
        mailbox.queue.push_back(work);
        if !mailbox.scheduled_or_running {
            mailbox.scheduled_or_running = true;
            state.ready_tablets.push_back(tablet_id);
            self.tablet_wake.notify_one();
        }
        Ok(())
    }

    fn complete_tablet_part(&self, ticket: &Arc<RecordTicket>, result: Result<()>) {
        self.metrics.decrement_queue_depth(1);
        if let Err(err) = result {
            self.fail_record(ticket, err);
            return;
        }
        ticket.complete_tablet_part();
    }

    fn fail_record(&self, ticket: &Arc<RecordTicket>, err: ParoError) {
        tracing::error!(
            lsn = ticket.lsn,
            commit_id = ?ticket.commit_id,
            error = %err,
            error_class = %err.error_class(),
            "journal apply runtime entered fail-stop after durable apply failure"
        );
        let normalized = normalize_durable_apply_failure(&err);
        let waiters = {
            let mut state = self.state.lock().unwrap();
            if state.poisoned.is_none() {
                state.poisoned = Some(normalized.clone());
            }
            state.records.values().cloned().collect::<Vec<_>>()
        };
        ticket.fail(normalized);
        self.dispatch_wake.notify_all();
        for waiter in waiters {
            waiter.notify_poison();
        }
    }

    fn advance_dispatch_past_placeholder(
        &self,
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
    ) -> Result<()> {
        if lsn == 0 {
            return Err(paro_error::internal(format!(
                "journal recovery placeholder {record_kind} cannot use synthetic lsn 0"
            )));
        }

        let (published_waiters, published_lsns) = {
            let mut state = self.state.lock().unwrap();
            if let Some(err) = state.poisoned.clone() {
                return Err(err);
            }
            if lsn < state.next_dispatch_lsn {
                return Ok(());
            }
            if lsn != state.next_dispatch_lsn {
                return Err(paro_error::internal(format!(
                    "journal recovery placeholder {record_kind} at lsn {lsn} cannot skip next dispatch lsn {}",
                    state.next_dispatch_lsn
                )));
            }

            state.frontiers.durable_lsn = state.frontiers.durable_lsn.max(lsn);
            state.next_ephemeral_lsn = state.next_ephemeral_lsn.max(lsn.saturating_add(1));
            state.next_dispatch_lsn = state.next_dispatch_lsn.saturating_add(1);
            let advanced = advance_publish_frontiers_locked(&mut state, lsn);
            self.dispatch_wake.notify_all();
            advanced
        };

        self.run_published_hooks(published_waiters, published_lsns)
    }

    fn advance_dispatch_gap_before(
        &self,
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
    ) -> Result<()> {
        if lsn == 0 {
            return Err(paro_error::internal(format!(
                "journal recovery gap before {record_kind} cannot target synthetic lsn 0"
            )));
        }

        let mut state = self.state.lock().unwrap();
        if let Some(err) = state.poisoned.clone() {
            return Err(err);
        }
        if lsn <= state.next_dispatch_lsn {
            return Ok(());
        }

        if let Some(pending_lsn) = state
            .records
            .keys()
            .copied()
            .filter(|pending| *pending < lsn)
            .min()
        {
            return Err(paro_error::internal(format!(
                "journal recovery gap before {record_kind} at lsn {lsn} cannot skip pending record {pending_lsn}"
            )));
        }

        let gap_end = lsn.saturating_sub(1);
        state.frontiers.durable_lsn = state.frontiers.durable_lsn.max(gap_end);
        state.frontiers.applied_lsn = state.frontiers.applied_lsn.max(gap_end);
        state.frontiers.published_lsn = state.frontiers.published_lsn.max(gap_end);
        state.next_ephemeral_lsn = state.next_ephemeral_lsn.max(lsn);
        state.next_dispatch_lsn = lsn;
        state.apply_frontier.skip_through(gap_end);
        state.publish_frontier.skip_through(gap_end);
        self.dispatch_wake.notify_all();
        Ok(())
    }

    fn record_wait(&self, mode: WaitMode, latency_micros: u64) {
        match mode {
            WaitMode::Durable => {
                self.metrics
                    .durable_wait_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .durable_wait_micros
                    .fetch_add(latency_micros, Ordering::Relaxed);
            }
            WaitMode::Applied => {
                self.metrics
                    .applied_wait_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .applied_wait_micros
                    .fetch_add(latency_micros, Ordering::Relaxed);
            }
            WaitMode::Published | WaitMode::AsyncCompletion => {
                self.metrics
                    .published_wait_count
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .published_wait_micros
                    .fetch_add(latency_micros, Ordering::Relaxed);
            }
        }
    }

    fn mark_applied(&self, ticket: &Arc<RecordTicket>) -> Result<()> {
        let (published_waiters, published_lsns) = {
            let mut state = self.state.lock().unwrap();
            if let Some(err) = state.poisoned.clone() {
                return Err(err);
            }

            ticket.mark_applied();
            advance_publish_frontiers_locked(&mut state, ticket.lsn)
        };

        self.run_published_hooks(published_waiters, published_lsns)
    }

    fn run_published_hooks(
        &self,
        published_waiters: Vec<Arc<RecordTicket>>,
        published_lsns: Vec<u64>,
    ) -> Result<()> {
        for (record, published_lsn) in published_waiters.into_iter().zip(published_lsns) {
            if let Err(err) = record.run_published_hook() {
                self.fail_record(
                    &record,
                    paro_error::internal(format!(
                        "published hook failed at lsn {}: {}",
                        published_lsn, err
                    )),
                );
                return Err(paro_error::internal(format!(
                    "published hook failed at lsn {}: {}",
                    published_lsn, err
                )));
            }
            if let Some(observer) = self.publication_observer.read().unwrap().as_ref() {
                if let Err(err) =
                    observer.record_published(published_lsn, record.publication_watermarks)
                {
                    self.fail_record(
                        &record,
                        paro_error::internal(format!(
                            "journal publication observer failed at lsn {}: {}",
                            published_lsn, err
                        )),
                    );
                    return Err(paro_error::internal(format!(
                        "journal publication observer failed at lsn {}: {}",
                        published_lsn, err
                    )));
                }
            }
            record.mark_published();
        }

        Ok(())
    }

    fn dequeue_tablet_work(&self) -> Option<(u64, TabletApplyWork)> {
        let mut state = self.tablet_dispatch.lock().unwrap();
        loop {
            if state.shutdown && state.ready_tablets.is_empty() {
                return None;
            }
            if let Some(tablet_id) = state.ready_tablets.pop_front() {
                if let Some(mailbox) = state.mailboxes.get_mut(&tablet_id) {
                    if let Some(work) = mailbox.queue.pop_front() {
                        return Some((tablet_id, work));
                    }
                    mailbox.scheduled_or_running = false;
                    state.mailboxes.remove(&tablet_id);
                    self.metrics.decrement_mailbox_count();
                }
                continue;
            }
            state = self.tablet_wake.wait(state).unwrap();
        }
    }

    fn finish_tablet_dispatch(&self, tablet_id: u64) {
        let mut state = self.tablet_dispatch.lock().unwrap();
        let Some(mailbox) = state.mailboxes.get_mut(&tablet_id) else {
            return;
        };
        if mailbox.queue.is_empty() {
            mailbox.scheduled_or_running = false;
            state.mailboxes.remove(&tablet_id);
            self.metrics.decrement_mailbox_count();
        } else {
            state.ready_tablets.push_back(tablet_id);
            self.tablet_wake.notify_one();
        }
    }
}

#[derive(Default)]
struct ApplyRuntimeMetrics {
    queue_depth: AtomicU64,
    queue_depth_peak: AtomicU64,
    async_submit_queue_depth: AtomicU64,
    async_submit_queue_depth_peak: AtomicU64,
    active_workers: AtomicU64,
    active_workers_peak: AtomicU64,
    mailbox_count: AtomicU64,
    durable_wait_count: AtomicU64,
    durable_wait_micros: AtomicU64,
    applied_wait_count: AtomicU64,
    applied_wait_micros: AtomicU64,
    published_wait_count: AtomicU64,
    published_wait_micros: AtomicU64,
}

impl ApplyRuntimeMetrics {
    fn increment_queue_depth(&self, amount: u64) {
        let new_depth = self.queue_depth.fetch_add(amount, Ordering::Relaxed) + amount;
        let mut current_peak = self.queue_depth_peak.load(Ordering::Relaxed);
        while new_depth > current_peak {
            match self.queue_depth_peak.compare_exchange_weak(
                current_peak,
                new_depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current_peak = observed,
            }
        }
    }

    fn decrement_queue_depth(&self, amount: u64) {
        self.queue_depth.fetch_sub(amount, Ordering::Relaxed);
    }

    fn increment_async_submit_queue_depth(&self) {
        let new_depth = self
            .async_submit_queue_depth
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let mut current_peak = self.async_submit_queue_depth_peak.load(Ordering::Relaxed);
        while new_depth > current_peak {
            match self.async_submit_queue_depth_peak.compare_exchange_weak(
                current_peak,
                new_depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current_peak = observed,
            }
        }
    }

    fn decrement_async_submit_queue_depth(&self) {
        self.async_submit_queue_depth
            .fetch_sub(1, Ordering::Relaxed);
    }

    fn increment_active_workers(&self) {
        let new_count = self.active_workers.fetch_add(1, Ordering::Relaxed) + 1;
        let mut current_peak = self.active_workers_peak.load(Ordering::Relaxed);
        while new_count > current_peak {
            match self.active_workers_peak.compare_exchange_weak(
                current_peak,
                new_count,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current_peak = observed,
            }
        }
    }

    fn decrement_active_workers(&self) {
        self.active_workers.fetch_sub(1, Ordering::Relaxed);
    }

    fn increment_mailbox_count(&self) {
        self.mailbox_count.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_mailbox_count(&self) {
        self.mailbox_count.fetch_sub(1, Ordering::Relaxed);
    }
}

fn normalize_durable_apply_failure(err: &ParoError) -> ParoError {
    paro_error::internal(format!("journal apply failed after durable append: {err}"))
        .detail(err.to_string())
        .context(format!(
            "original durable apply failure class: {}",
            err.error_class()
        ))
}

fn advance_publish_frontiers_locked(
    state: &mut ApplyRuntimeState,
    applied_ready_lsn: u64,
) -> (Vec<Arc<RecordTicket>>, Vec<u64>) {
    let mut published_waiters = Vec::new();
    let mut published_lsns = Vec::new();
    for applied_lsn in state.apply_frontier.mark_ready(applied_ready_lsn) {
        state.frontiers.applied_lsn = applied_lsn;
        for published_lsn in state.publish_frontier.mark_ready(applied_lsn) {
            state.frontiers.published_lsn = published_lsn;
            if let Some(record) = state.records.remove(&published_lsn) {
                published_waiters.push(record);
                published_lsns.push(published_lsn);
            }
        }
    }
    (published_waiters, published_lsns)
}

struct RecordTicket {
    lsn: u64,
    commit_id: Option<u64>,
    publication_watermarks: JournalPublicationWatermarks,
    on_published: Mutex<Option<PublishedHook>>,
    failure: Option<SharedApplyFailure>,
    progress: Mutex<RecordProgress>,
    wake: Condvar,
}

#[derive(Default)]
struct RecordProgress {
    remaining_tablet_parts: u32,
    part_error: Option<ParoError>,
    applied: bool,
    published: bool,
}

impl RecordTicket {
    fn new(
        lsn: u64,
        commit_id: Option<u64>,
        publication_watermarks: JournalPublicationWatermarks,
        remaining_tablet_parts: u32,
        on_published: PublishedHook,
        failure: Option<SharedApplyFailure>,
    ) -> Self {
        Self {
            lsn,
            commit_id,
            publication_watermarks,
            on_published: Mutex::new(Some(on_published)),
            failure,
            progress: Mutex::new(RecordProgress {
                remaining_tablet_parts,
                part_error: None,
                applied: false,
                published: false,
            }),
            wake: Condvar::new(),
        }
    }

    fn notify_zero_tablet_parts(&self) {
        let mut progress = self.progress.lock().unwrap();
        progress.remaining_tablet_parts = 0;
        self.wake.notify_all();
    }

    fn complete_tablet_part(&self) {
        let mut progress = self.progress.lock().unwrap();
        progress.remaining_tablet_parts = progress.remaining_tablet_parts.saturating_sub(1);
        self.wake.notify_all();
    }

    fn mark_applied(&self) {
        let mut progress = self.progress.lock().unwrap();
        progress.applied = true;
        self.wake.notify_all();
    }

    fn mark_published(&self) {
        let mut progress = self.progress.lock().unwrap();
        progress.published = true;
        self.wake.notify_all();
    }

    fn fail(&self, err: ParoError) {
        let mut progress = self.progress.lock().unwrap();
        if progress.part_error.is_none() {
            progress.part_error = Some(err);
        }
        self.wake.notify_all();
    }

    fn notify_poison(&self) {
        self.wake.notify_all();
    }

    fn wait_for_tablet_phase(&self, runtime: &JournalApplyRuntimeInner) -> Result<()> {
        let mut progress = self.progress.lock().unwrap();
        loop {
            if let Some(err) = progress.part_error.clone() {
                return Err(err);
            }
            if progress.remaining_tablet_parts == 0 {
                return Ok(());
            }
            if let Some(err) = runtime.state.lock().unwrap().poisoned.clone() {
                return Err(err);
            }
            progress = self.wake.wait(progress).unwrap();
        }
    }

    fn wait_for_published(&self, runtime: &JournalApplyRuntimeInner) -> Result<()> {
        let mut progress = self.progress.lock().unwrap();
        loop {
            if let Some(err) = progress.part_error.clone() {
                return Err(err);
            }
            if progress.published {
                return Ok(());
            }
            if let Some(err) = runtime.state.lock().unwrap().poisoned.clone() {
                return Err(err);
            }
            progress = self.wake.wait(progress).unwrap();
        }
    }

    fn run_published_hook(&self) -> Result<()> {
        let Some(hook) = self.on_published.lock().unwrap().take() else {
            return Ok(());
        };
        call_apply_work(
            hook,
            ApplyPhase::Published,
            ApplyErrorSource::PublishedHook,
            self.lsn,
            self.commit_id,
            self.failure.as_ref(),
        )
    }
}

#[derive(Default)]
struct AsyncSubmitState {
    queue: VecDeque<AsyncSubmitWork>,
    shutdown: bool,
}

enum AsyncSubmitWork {
    Legacy(ApplyRequest<()>),
    WithCompletion {
        request: ApplyRequest<()>,
        on_complete: ApplyCompletion,
        fallback_ack: ApplyCompletionFallbackAck,
        fatal_sink: ApplyFatalSink,
    },
}

#[derive(Default)]
struct TabletDispatchState {
    mailboxes: HashMap<u64, TabletMailbox>,
    ready_tablets: VecDeque<u64>,
    shutdown: bool,
}

#[derive(Default)]
struct TabletMailbox {
    queue: VecDeque<TabletApplyWork>,
    scheduled_or_running: bool,
}

struct TabletApplyWork {
    apply: Option<ApplyWork>,
    runtime: Weak<JournalApplyRuntimeInner>,
    ticket: Arc<RecordTicket>,
    failure: Option<SharedApplyFailure>,
}

fn default_apply_worker_count() -> usize {
    // JournalApplyRuntime is instantiated per database today, so a "one worker per core"
    // policy quickly overcommits thread resources when tests or deployments keep several
    // databases open at once. Keep the default pool conservative until we move to a shared
    // executor or expose an explicit tuning knob.
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4)
        .clamp(1, 4)
}

fn run_tablet_worker(runtime: Weak<JournalApplyRuntimeInner>) {
    loop {
        let Some(inner) = runtime.upgrade() else {
            return;
        };
        let Some((tablet_id, work)) = inner.dequeue_tablet_work() else {
            return;
        };

        inner.metrics.increment_active_workers();
        let result = match work.apply {
            Some(apply) => call_apply_work(
                apply,
                ApplyPhase::TabletParts,
                ApplyErrorSource::ApplyClosure,
                work.ticket.lsn,
                work.ticket.commit_id,
                work.failure.as_ref(),
            ),
            None => Ok(()),
        };
        inner.metrics.decrement_active_workers();
        inner.finish_tablet_dispatch(tablet_id);

        let Some(runtime) = work.runtime.upgrade().or_else(|| Some(Arc::clone(&inner))) else {
            return;
        };
        runtime.complete_tablet_part(&work.ticket, result);
    }
}

fn run_async_submit_worker(runtime: Weak<JournalApplyRuntimeInner>) {
    loop {
        let Some(inner) = runtime.upgrade() else {
            return;
        };
        let Some(request) = inner.dequeue_async_submit() else {
            return;
        };
        let apply_runtime = JournalApplyRuntime {
            inner: Arc::clone(&inner),
        };
        match request {
            AsyncSubmitWork::Legacy(request) => {
                if let Err(err) = apply_runtime.submit(request) {
                    tracing::error!(
                        error = %err,
                        "asynchronous journal apply request failed after durable append"
                    );
                }
            }
            AsyncSubmitWork::WithCompletion {
                request,
                on_complete,
                fallback_ack,
                fatal_sink,
            } => {
                run_async_submit_with_completion(
                    apply_runtime,
                    request,
                    on_complete,
                    fallback_ack,
                    fatal_sink,
                );
            }
        }
    }
}

fn run_async_submit_with_completion(
    apply_runtime: JournalApplyRuntime,
    request: ApplyRequest<()>,
    on_complete: ApplyCompletion,
    fallback_ack: ApplyCompletionFallbackAck,
    fatal_sink: ApplyFatalSink,
) {
    let lsn = request.lsn;
    let commit_id = request.commit_id;
    let failure = SharedApplyFailure::default();
    let submit_result = catch_unwind(AssertUnwindSafe(|| {
        apply_runtime.submit_observed_tracked(request, Some(failure.clone()))
    }));
    let completion_result = match submit_result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(failure.take_or_runtime(lsn, commit_id, &err)),
        Err(panic) => Err(JournalApplyError::worker_panic(
            ApplyPhase::Runtime,
            lsn,
            commit_id,
            panic,
        )),
    };
    complete_async_request(
        completion_result,
        lsn,
        commit_id,
        on_complete,
        fallback_ack,
        fatal_sink,
    );
}

fn complete_async_request(
    result: std::result::Result<(), JournalApplyError>,
    lsn: u64,
    commit_id: Option<u64>,
    on_complete: ApplyCompletion,
    fallback_ack: ApplyCompletionFallbackAck,
    fatal_sink: ApplyFatalSink,
) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| on_complete(result))) {
        let completion_error = JournalApplyError::completion_panic(lsn, commit_id, panic);
        if catch_unwind(AssertUnwindSafe(|| fallback_ack(&completion_error))).is_err() {
            std::process::abort();
        }
        if catch_unwind(AssertUnwindSafe(|| fatal_sink(&completion_error))).is_err() {
            std::process::abort();
        }
    }
}

fn call_apply_work(
    work: ApplyWork,
    phase: ApplyPhase,
    source: ApplyErrorSource,
    lsn: u64,
    commit_id: Option<u64>,
    failure: Option<&SharedApplyFailure>,
) -> Result<()> {
    match catch_unwind(AssertUnwindSafe(work)) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            if let Some(failure) = failure {
                failure.record(JournalApplyError::apply_failed(
                    phase, source, lsn, commit_id, &err,
                ));
            }
            Err(err)
        }
        Err(panic) => {
            let error = JournalApplyError::worker_panic(phase, lsn, commit_id, panic);
            if let Some(failure) = failure {
                failure.record(error.clone());
            }
            Err(error.to_paro_error())
        }
    }
}

fn call_catalog_post<R>(
    work: Box<dyn FnOnce() -> Result<R> + Send + 'static>,
    phase: ApplyPhase,
    source: ApplyErrorSource,
    lsn: u64,
    commit_id: Option<u64>,
    failure: Option<&SharedApplyFailure>,
) -> Result<R> {
    match catch_unwind(AssertUnwindSafe(work)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => {
            if let Some(failure) = failure {
                failure.record(JournalApplyError::apply_failed(
                    phase, source, lsn, commit_id, &err,
                ));
            }
            Err(err)
        }
        Err(panic) => {
            let error = JournalApplyError::worker_panic(phase, lsn, commit_id, panic);
            if let Some(failure) = failure {
                failure.record(error.clone());
            }
            Err(error.to_paro_error())
        }
    }
}

const fn apply_error_code(phase: ApplyPhase, source: ApplyErrorSource) -> u32 {
    ((phase as u32) << 8) | source as u32
}

fn panic_payload_message(panic: Box<dyn Any + Send + 'static>) -> Arc<str> {
    if let Some(message) = panic.downcast_ref::<&'static str>() {
        return Arc::from(*message);
    }
    if let Some(message) = panic.downcast_ref::<String>() {
        return Arc::from(message.as_str());
    }
    Arc::from("panic payload is not a string")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::thread;
    use std::time::Duration;

    fn empty_request(
        lsn: u64,
        commit_id: Option<u64>,
        wait_mode: WaitMode,
        tablet_parts: Vec<TabletApplyPart>,
    ) -> ApplyRequest<()> {
        ApplyRequest {
            lsn,
            durable_batch_lsn: lsn,
            commit_id,
            publication_watermarks: JournalPublicationWatermarks::default(),
            wait_mode,
            catalog_serial: false,
            catalog_pre: Box::new(|| Ok(())),
            tablet_parts,
            descriptor_phase: Box::new(|| Ok(())),
            catalog_post: Box::new(|| Ok(())),
            on_published: Box::new(|| Ok(())),
        }
    }

    #[test]
    fn tablet_queue_dispatch_respects_lsn_order_even_if_submit_arrives_out_of_order() {
        let runtime = JournalApplyRuntime::new();
        let order = Arc::new(StdMutex::new(Vec::new()));
        let second_finished = Arc::new(AtomicBool::new(false));

        let runtime_second = runtime.clone();
        let order_second = Arc::clone(&order);
        let second_flag = Arc::clone(&second_finished);
        let second = thread::spawn(move || {
            runtime_second
                .submit(empty_request(
                    2,
                    Some(2),
                    WaitMode::Published,
                    vec![TabletApplyPart {
                        tablet_id: 7,
                        apply: Box::new(move || {
                            order_second.lock().unwrap().push(2);
                            second_flag.store(true, Ordering::Release);
                            Ok(())
                        }),
                    }],
                ))
                .unwrap();
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!second_finished.load(Ordering::Acquire));

        runtime
            .submit(empty_request(
                1,
                Some(1),
                WaitMode::Published,
                vec![TabletApplyPart {
                    tablet_id: 7,
                    apply: Box::new({
                        let order = Arc::clone(&order);
                        move || {
                            order.lock().unwrap().push(1);
                            Ok(())
                        }
                    }),
                }],
            ))
            .unwrap();
        second.join().unwrap();

        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.published_lsn, 2);
    }

    #[test]
    fn recovery_gap_skip_unblocks_sparse_journal_record() {
        let runtime = JournalApplyRuntime::new();
        runtime
            .advance_dispatch_gap_before(3, RecoveryPlaceholderRecordKind::Other)
            .unwrap();

        runtime
            .submit(empty_request(3, Some(3), WaitMode::Published, Vec::new()))
            .unwrap();

        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.durable_lsn, 3);
        assert_eq!(frontiers.applied_lsn, 3);
        assert_eq!(frontiers.published_lsn, 3);
    }

    #[test]
    fn async_submit_queues_apply_without_waiting_for_publish() {
        let runtime = JournalApplyRuntime::new();
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let release_apply = Arc::clone(&release);
        let started_apply = Arc::clone(&started);

        runtime
            .submit_async(empty_request(
                1,
                Some(1),
                WaitMode::Published,
                vec![TabletApplyPart {
                    tablet_id: 11,
                    apply: Box::new(move || {
                        started_apply.store(true, Ordering::Release);
                        let (lock, wake) = &*release_apply;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = wake.wait(released).unwrap();
                        }
                        Ok(())
                    }),
                }],
            ))
            .unwrap();

        for _ in 0..20 {
            if started.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(started.load(Ordering::Acquire));
        assert_eq!(runtime.frontiers().published_lsn, 0);

        let (lock, wake) = &*release;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        for _ in 0..50 {
            if runtime.frontiers().published_lsn == 1 {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("async apply did not publish commit");
    }

    #[test]
    fn async_completion_runs_after_published_hook() {
        let runtime = JournalApplyRuntime::new();
        let published = Arc::new(AtomicBool::new(false));
        let (complete_tx, complete_rx) = mpsc::channel();
        let mut request = empty_request(1, Some(10), WaitMode::Published, Vec::new());
        let published_hook = Arc::clone(&published);
        request.on_published = Box::new(move || {
            published_hook.store(true, Ordering::Release);
            Ok(())
        });

        runtime
            .submit_async_with_completion(
                request,
                Box::new(move |result| complete_tx.send(result).unwrap()),
                Box::new(|_| {}),
                Box::new(|_| {}),
            )
            .unwrap();

        let result = complete_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(result.is_ok());
        assert!(published.load(Ordering::Acquire));
        assert_eq!(runtime.frontiers().published_lsn, 1);
    }

    #[test]
    fn async_completion_reports_apply_phase_error() {
        let runtime = JournalApplyRuntime::new();
        let (complete_tx, complete_rx) = mpsc::channel();
        let mut request = empty_request(1, Some(11), WaitMode::Published, Vec::new());
        request.catalog_pre = Box::new(|| Err(paro_error::internal("catalog pre failed")));

        runtime
            .submit_async_with_completion(
                request,
                Box::new(move |result| complete_tx.send(result).unwrap()),
                Box::new(|_| {}),
                Box::new(|_| {}),
            )
            .unwrap();

        let error = complete_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err();
        assert_eq!(error.phase, ApplyPhase::CatalogPre);
        assert_eq!(error.source, ApplyErrorSource::ApplyClosure);
        assert_eq!(error.lsn, 1);
        assert_eq!(error.commit_id, Some(11));
    }

    #[test]
    fn completion_panic_uses_fallback_ack_and_fatal_sink() {
        let runtime = JournalApplyRuntime::new();
        let fallback_seen = Arc::new(AtomicBool::new(false));
        let fatal_seen = Arc::new(AtomicBool::new(false));
        let (done_tx, done_rx) = mpsc::channel();

        runtime
            .submit_async_with_completion(
                empty_request(1, Some(12), WaitMode::Published, Vec::new()),
                Box::new(|_| panic!("completion panic")),
                {
                    let fallback_seen = Arc::clone(&fallback_seen);
                    Box::new(move |error| {
                        assert_eq!(error.phase, ApplyPhase::Completion);
                        fallback_seen.store(true, Ordering::Release);
                    })
                },
                {
                    let fatal_seen = Arc::clone(&fatal_seen);
                    Box::new(move |error| {
                        assert_eq!(error.phase, ApplyPhase::Completion);
                        fatal_seen.store(true, Ordering::Release);
                        done_tx.send(()).unwrap();
                    })
                },
            )
            .unwrap();

        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(fallback_seen.load(Ordering::Acquire));
        assert!(fatal_seen.load(Ordering::Acquire));
    }

    #[test]
    fn maintenance_publish_advances_lsn_frontier() {
        let runtime = JournalApplyRuntime::new();
        runtime
            .submit(empty_request(1, Some(7), WaitMode::Published, Vec::new()))
            .unwrap();
        runtime
            .submit(empty_request(2, None, WaitMode::Published, Vec::new()))
            .unwrap();

        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.applied_lsn, 2);
        assert_eq!(frontiers.published_lsn, 2);
    }

    #[test]
    fn recovery_placeholder_unblocks_later_commit_lsn() {
        let runtime = JournalApplyRuntime::new();
        let finished = Arc::new(AtomicBool::new(false));
        let runtime_commit = runtime.clone();
        let finished_commit = Arc::clone(&finished);
        let commit = thread::spawn(move || {
            runtime_commit
                .submit(empty_request(2, Some(9), WaitMode::Published, Vec::new()))
                .unwrap();
            finished_commit.store(true, Ordering::Release);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!finished.load(Ordering::Acquire));
        assert_eq!(runtime.frontiers().published_lsn, 0);

        runtime
            .advance_dispatch_past_placeholder(1, RecoveryPlaceholderRecordKind::Maintenance)
            .unwrap();

        commit.join().unwrap();
        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.published_lsn, 2);
    }

    #[test]
    fn recovery_placeholder_rejects_lsn_gap() {
        let runtime = JournalApplyRuntime::new();
        let err = runtime
            .advance_dispatch_past_placeholder(2, RecoveryPlaceholderRecordKind::CheckpointFence)
            .expect_err("placeholder must not skip an earlier WAL lsn");
        assert!(err.to_string().contains("cannot skip next dispatch lsn"));
    }

    #[test]
    fn synthetic_lsn_zero_submits_publish_without_deadlock() {
        let runtime = JournalApplyRuntime::new();
        runtime
            .submit(empty_request(0, Some(9), WaitMode::Published, Vec::new()))
            .unwrap();
        runtime
            .submit(empty_request(0, Some(10), WaitMode::Published, Vec::new()))
            .unwrap();

        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.applied_lsn, 2);
        assert_eq!(frontiers.published_lsn, 2);
    }

    #[test]
    fn bootstrap_frontiers_resumes_from_recovered_lsn() {
        let runtime = JournalApplyRuntime::new();
        runtime.bootstrap_frontiers(RecoverySummary {
            max_lsn: 7,
            max_commit_id: 4,
            ..RecoverySummary::default()
        });

        runtime
            .submit(empty_request(8, Some(5), WaitMode::Published, Vec::new()))
            .unwrap();

        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.durable_lsn, 8);
        assert_eq!(frontiers.published_lsn, 8);
    }

    #[test]
    fn bootstrap_frontiers_rejects_stale_live_lsn() {
        let runtime = JournalApplyRuntime::new();
        runtime.bootstrap_frontiers(RecoverySummary {
            max_lsn: 7,
            max_commit_id: 4,
            ..RecoverySummary::default()
        });

        let err = runtime
            .submit(empty_request(1, Some(5), WaitMode::Published, Vec::new()))
            .expect_err("stale post-recovery lsn must fail instead of waiting forever");
        assert!(err.to_string().contains("below next dispatch frontier"));
    }

    #[test]
    fn descriptor_phase_waits_for_all_tablet_parts() {
        let runtime = JournalApplyRuntime::new();
        let fast_part_done = Arc::new(AtomicBool::new(false));
        let descriptor_ran = Arc::new(AtomicBool::new(false));
        let release_slow_part = Arc::new((StdMutex::new(false), Condvar::new()));

        let runtime_worker = runtime.clone();
        let fast_part_done_worker = Arc::clone(&fast_part_done);
        let descriptor_ran_worker = Arc::clone(&descriptor_ran);
        let release_slow_part_worker = Arc::clone(&release_slow_part);
        let handle = thread::spawn(move || {
            runtime_worker
                .submit(ApplyRequest {
                    lsn: 1,
                    durable_batch_lsn: 1,
                    commit_id: Some(1),
                    publication_watermarks: JournalPublicationWatermarks::default(),
                    wait_mode: WaitMode::Published,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: vec![
                        TabletApplyPart {
                            tablet_id: 11,
                            apply: Box::new(move || {
                                fast_part_done_worker.store(true, Ordering::Release);
                                Ok(())
                            }),
                        },
                        TabletApplyPart {
                            tablet_id: 22,
                            apply: Box::new(move || {
                                let (lock, wake) = &*release_slow_part_worker;
                                let mut released = lock.lock().unwrap();
                                while !*released {
                                    released = wake.wait(released).unwrap();
                                }
                                Ok(())
                            }),
                        },
                    ],
                    descriptor_phase: Box::new(move || {
                        descriptor_ran_worker.store(true, Ordering::Release);
                        Ok(())
                    }),
                    catalog_post: Box::new(|| Ok(())),
                    on_published: Box::new(|| Ok(())),
                })
                .unwrap();
        });

        for _ in 0..20 {
            if fast_part_done.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(fast_part_done.load(Ordering::Acquire));
        assert!(!descriptor_ran.load(Ordering::Acquire));

        let (lock, wake) = &*release_slow_part;
        *lock.lock().unwrap() = true;
        wake.notify_all();

        handle.join().unwrap();
        assert!(descriptor_ran.load(Ordering::Acquire));
    }

    #[test]
    fn worker_pool_keeps_active_workers_bounded() {
        let runtime = JournalApplyRuntime::new();
        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let tablet_parts = (0..32u64)
            .map(|tablet_id| {
                let release = Arc::clone(&release);
                TabletApplyPart {
                    tablet_id,
                    apply: Box::new(move || {
                        let (lock, wake) = &*release;
                        let mut ready = lock.lock().unwrap();
                        while !*ready {
                            ready = wake.wait(ready).unwrap();
                        }
                        Ok(())
                    }),
                }
            })
            .collect::<Vec<_>>();

        let runtime_submit = runtime.clone();
        let handle = thread::spawn(move || {
            runtime_submit
                .submit(empty_request(1, Some(1), WaitMode::Published, tablet_parts))
                .unwrap();
        });

        thread::sleep(Duration::from_millis(100));
        let metrics = runtime.metrics();
        assert!(metrics.active_workers_peak <= default_apply_worker_count() as u64);
        assert!(metrics.mailbox_count <= 32);

        let (lock, wake) = &*release;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        handle.join().unwrap();
    }

    #[test]
    fn published_frontier_waits_for_earlier_multi_tablet_record_before_releasing_later_commit() {
        let runtime = JournalApplyRuntime::new();
        let fast_part_done = Arc::new(AtomicBool::new(false));
        let second_finished = Arc::new(AtomicBool::new(false));
        let release_slow_part = Arc::new((StdMutex::new(false), Condvar::new()));

        let runtime_first = runtime.clone();
        let fast_part_done_first = Arc::clone(&fast_part_done);
        let release_slow_part_first = Arc::clone(&release_slow_part);
        let first = thread::spawn(move || {
            runtime_first
                .submit(ApplyRequest {
                    lsn: 1,
                    durable_batch_lsn: 1,
                    commit_id: Some(1),
                    publication_watermarks: JournalPublicationWatermarks::default(),
                    wait_mode: WaitMode::Applied,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: vec![
                        TabletApplyPart {
                            tablet_id: 11,
                            apply: Box::new(move || {
                                fast_part_done_first.store(true, Ordering::Release);
                                Ok(())
                            }),
                        },
                        TabletApplyPart {
                            tablet_id: 22,
                            apply: Box::new(move || {
                                let (lock, wake) = &*release_slow_part_first;
                                let mut released = lock.lock().unwrap();
                                while !*released {
                                    released = wake.wait(released).unwrap();
                                }
                                Ok(())
                            }),
                        },
                    ],
                    descriptor_phase: Box::new(|| Ok(())),
                    catalog_post: Box::new(|| Ok(())),
                    on_published: Box::new(|| Ok(())),
                })
                .unwrap();
        });

        for _ in 0..20 {
            if fast_part_done.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(fast_part_done.load(Ordering::Acquire));

        let runtime_second = runtime.clone();
        let second_finished_flag = Arc::clone(&second_finished);
        let second = thread::spawn(move || {
            runtime_second
                .submit(empty_request(2, Some(2), WaitMode::Published, Vec::new()))
                .unwrap();
            second_finished_flag.store(true, Ordering::Release);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!second_finished.load(Ordering::Acquire));

        let stalled = runtime.frontiers();
        assert_eq!(stalled.durable_lsn, 2);
        assert_eq!(stalled.applied_lsn, 0);
        assert_eq!(stalled.published_lsn, 0);

        let (lock, wake) = &*release_slow_part;
        *lock.lock().unwrap() = true;
        wake.notify_all();

        first.join().unwrap();
        second.join().unwrap();

        assert!(second_finished.load(Ordering::Acquire));
        let frontiers = runtime.frontiers();
        assert_eq!(frontiers.applied_lsn, 2);
        assert_eq!(frontiers.published_lsn, 2);
    }

    #[test]
    fn durable_apply_failure_is_normalized_to_internal_poison() {
        let runtime = JournalApplyRuntime::new();
        let err = runtime
            .submit(ApplyRequest {
                lsn: 1,
                durable_batch_lsn: 1,
                commit_id: Some(1),
                publication_watermarks: JournalPublicationWatermarks::default(),
                wait_mode: WaitMode::Published,
                catalog_serial: false,
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: vec![TabletApplyPart {
                    tablet_id: 7,
                    apply: Box::new(|| {
                        Err(paro_error::serialization_failure(
                            "stale delete patch should poison runtime",
                        ))
                    }),
                }],
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            })
            .unwrap_err();

        assert!(err.is_internal_error());
        assert!(
            err.to_string()
                .contains("journal apply failed after durable append"),
            "normalized error message should avoid surfacing retryable business conflicts"
        );

        let next = runtime
            .submit(empty_request(2, Some(2), WaitMode::Published, Vec::new()))
            .unwrap_err();
        assert!(next.is_internal_error());
    }

    #[test]
    fn metrics_track_queue_depth_frontiers_and_wait_modes() {
        let runtime = JournalApplyRuntime::new();
        let release = Arc::new((StdMutex::new(false), Condvar::new()));
        let started = Arc::new((StdMutex::new(false), Condvar::new()));
        let release_worker = Arc::clone(&release);
        let started_worker = Arc::clone(&started);
        let runtime_worker = runtime.clone();

        let worker = thread::spawn(move || {
            runtime_worker
                .submit(ApplyRequest {
                    lsn: 1,
                    durable_batch_lsn: 5,
                    commit_id: Some(8),
                    publication_watermarks: JournalPublicationWatermarks::default(),
                    wait_mode: WaitMode::Published,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: vec![TabletApplyPart {
                        tablet_id: 99,
                        apply: Box::new(move || {
                            let (started_lock, started_wake) = &*started_worker;
                            *started_lock.lock().unwrap() = true;
                            started_wake.notify_all();
                            let (lock, wake) = &*release_worker;
                            let mut released = lock.lock().unwrap();
                            while !*released {
                                released = wake.wait(released).unwrap();
                            }
                            Ok(())
                        }),
                    }],
                    descriptor_phase: Box::new(|| Ok(())),
                    catalog_post: Box::new(|| Ok(())),
                    on_published: Box::new(|| Ok(())),
                })
                .unwrap();
        });

        let (started_lock, started_wake) = &*started;
        let mut observed_started = started_lock.lock().unwrap();
        while !*observed_started {
            observed_started = started_wake.wait(observed_started).unwrap();
        }
        drop(observed_started);

        let inflight = runtime.metrics();
        assert_eq!(inflight.queue_depth, 1);
        assert!(inflight.queue_depth_peak >= 1);
        assert_eq!(inflight.durable_lsn, 5);
        assert_eq!(inflight.applied_lag, 5);
        assert_eq!(inflight.published_lag, 5);

        let (lock, wake) = &*release;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        worker.join().unwrap();

        let metrics = runtime.metrics();
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(metrics.applied_lsn, 1);
        assert_eq!(metrics.published_lsn, 1);
        assert_eq!(metrics.applied_lag, 4);
        assert_eq!(metrics.published_lag, 4);
        assert_eq!(metrics.published_wait_count, 1);
        assert!(metrics.published_wait_micros > 0);
    }
}
