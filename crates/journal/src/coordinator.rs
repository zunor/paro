// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::appender::{AppendResult, JournalAppender};
use crate::apply_queue::JournalApplyRuntime;
use paro_common::durability::{PreparedCommitPlan, PreparedMaintenancePlan};
use paro_common::error as paro_error;
use paro_common::error::{ParoError, Result};
use paro_common::journal::{CommitRecord, JournalRecord, MaintenanceRecord};
use paro_common::logging::targets;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitExecutionContext {
    pub commit_id: u64,
    pub lsn: u64,
    pub durable_batch_lsn: u64,
    pub durable_batch_size: u64,
    pub durable_batch_bytes: u64,
    pub sync_latency_micros: u64,
    pub record: CommitRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceExecutionContext {
    pub maintenance_id: u64,
    pub lsn: u64,
    pub durable_batch_lsn: u64,
    pub durable_batch_size: u64,
    pub durable_batch_bytes: u64,
    pub sync_latency_micros: u64,
    pub record: MaintenanceRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JournalFrontierSnapshot {
    pub durable_lsn: u64,
    pub applied_lsn: u64,
    pub published_lsn: u64,
    pub durable_commit_id: u64,
    pub published_commit_id: u64,
}

pub struct JournalCoordinator {
    inner: Arc<JournalCoordinatorInner>,
}

impl std::fmt::Debug for JournalCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalCoordinator")
            .field("frontiers", &self.frontiers())
            .finish()
    }
}

struct JournalCoordinatorInner {
    appender: Option<Arc<JournalAppender>>,
    apply_runtime: Mutex<Option<Arc<JournalApplyRuntime>>>,
    next_commit_id: AtomicU64,
    next_maintenance_id: AtomicU64,
    state: Mutex<CoordinatorState>,
    wake_worker: Condvar,
}

#[derive(Default)]
struct CoordinatorState {
    queue: VecDeque<PendingRequest>,
    fallback_frontiers: JournalFrontierSnapshot,
    poisoned: Option<ParoError>,
    shutdown: bool,
}

enum PendingRequest {
    Commit(PendingCommitRequest),
    Maintenance(PendingMaintenanceRequest),
}

type CommitValidator = Box<dyn FnOnce(&PreparedCommitPlan) -> Result<()> + Send + 'static>;
type CommitFinish =
    Box<dyn FnOnce(std::result::Result<CommitExecutionContext, ParoError>) -> Result<bool> + Send>;
type MaintenanceValidator =
    Box<dyn FnOnce(&PreparedMaintenancePlan) -> Result<()> + Send + 'static>;
type MaintenanceFinish = Box<
    dyn FnOnce(std::result::Result<MaintenanceExecutionContext, ParoError>) -> Result<bool> + Send,
>;

struct PendingCommitRequest {
    plan: PreparedCommitPlan,
    validate: Option<CommitValidator>,
    finish: Option<CommitFinish>,
    queued_at: Instant,
}

struct PendingMaintenanceRequest {
    plan: PreparedMaintenancePlan,
    validate: Option<MaintenanceValidator>,
    finish: Option<MaintenanceFinish>,
    queued_at: Instant,
}

enum AcceptedRequest {
    Commit {
        commit_id: u64,
        record: CommitRecord,
        finish: CommitFinish,
        queued_at: Instant,
    },
    Maintenance {
        maintenance_id: u64,
        record: MaintenanceRecord,
        finish: MaintenanceFinish,
        queued_at: Instant,
    },
}

impl JournalCoordinator {
    pub fn new(appender: Option<Arc<JournalAppender>>) -> Self {
        let inner = Arc::new(JournalCoordinatorInner {
            appender,
            apply_runtime: Mutex::new(None),
            next_commit_id: AtomicU64::new(1),
            next_maintenance_id: AtomicU64::new(1),
            state: Mutex::new(CoordinatorState::default()),
            wake_worker: Condvar::new(),
        });

        let worker_inner = Arc::downgrade(&inner);
        thread::Builder::new()
            .name("paro-journal-coordinator".to_string())
            .spawn(move || run_worker(worker_inner))
            .expect("spawn journal coordinator worker");

        Self { inner }
    }

    pub fn bind_apply_runtime(&self, runtime: Arc<JournalApplyRuntime>) {
        *self.inner.apply_runtime.lock().unwrap() = Some(runtime);
    }

    pub fn frontiers(&self) -> JournalFrontierSnapshot {
        self.inner
            .apply_runtime
            .lock()
            .unwrap()
            .as_ref()
            .map(|runtime| runtime.frontiers())
            .unwrap_or_else(|| self.inner.state.lock().unwrap().fallback_frontiers)
    }

    pub fn sync_commit_id_with(&self, min_committed_version: u64) {
        let next = min_committed_version.saturating_add(1);
        bump_atomic_min(&self.inner.next_commit_id, next);

        if let Some(runtime) = self.inner.apply_runtime.lock().unwrap().as_ref() {
            runtime.sync_commit_frontier_with(min_committed_version);
            return;
        }

        let mut state = self.inner.state.lock().unwrap();
        state.fallback_frontiers.durable_commit_id = state
            .fallback_frontiers
            .durable_commit_id
            .max(min_committed_version);
        state.fallback_frontiers.published_commit_id = state
            .fallback_frontiers
            .published_commit_id
            .max(min_committed_version);
    }

    pub fn sync_maintenance_id_with(&self, min_maintenance_id: u64) {
        let next = min_maintenance_id.saturating_add(1);
        bump_atomic_min(&self.inner.next_maintenance_id, next);
    }

    pub fn submit_commit<R, V, A>(
        &self,
        plan: PreparedCommitPlan,
        validate: V,
        apply: A,
    ) -> Result<R>
    where
        R: Send + 'static,
        V: FnOnce(&PreparedCommitPlan) -> Result<()> + Send + 'static,
        A: FnOnce(CommitExecutionContext) -> Result<R> + Send + 'static,
    {
        let (tx, rx) = sync_channel(1);
        let finish = Box::new(
            move |result: std::result::Result<CommitExecutionContext, ParoError>| -> Result<bool> {
                match result {
                    Ok(ctx) => {
                        let apply_result = apply(ctx);
                        let coordinator_result =
                            apply_result.as_ref().map(|_| true).map_err(Clone::clone);
                        let _ = tx.send(apply_result);
                        coordinator_result
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err.clone()));
                        Err(err)
                    }
                }
            },
        );

        self.enqueue(PendingRequest::Commit(PendingCommitRequest {
            plan,
            validate: Some(Box::new(validate)),
            finish: Some(finish),
            queued_at: Instant::now(),
        }))?;

        rx.recv().map_err(|_| {
            paro_error::internal("journal coordinator worker exited before commit response")
        })?
    }

    pub fn submit_commit_context<V>(
        &self,
        plan: PreparedCommitPlan,
        validate: V,
    ) -> Result<CommitExecutionContext>
    where
        V: FnOnce(&PreparedCommitPlan) -> Result<()> + Send + 'static,
    {
        let (tx, rx) = sync_channel(1);
        let finish = Box::new(
            move |result: std::result::Result<CommitExecutionContext, ParoError>| -> Result<bool> {
                match result {
                    Ok(ctx) => {
                        let _ = tx.send(Ok(ctx));
                        Ok(false)
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err.clone()));
                        Err(err)
                    }
                }
            },
        );

        self.enqueue(PendingRequest::Commit(PendingCommitRequest {
            plan,
            validate: Some(Box::new(validate)),
            finish: Some(finish),
            queued_at: Instant::now(),
        }))?;

        rx.recv().map_err(|_| {
            paro_error::internal("journal coordinator worker exited before commit context response")
        })?
    }

    pub fn submit_maintenance<R, V, A>(
        &self,
        plan: PreparedMaintenancePlan,
        validate: V,
        apply: A,
    ) -> Result<R>
    where
        R: Send + 'static,
        V: FnOnce(&PreparedMaintenancePlan) -> Result<()> + Send + 'static,
        A: FnOnce(MaintenanceExecutionContext) -> Result<R> + Send + 'static,
    {
        let (tx, rx) = sync_channel(1);
        let finish = Box::new(
            move |result: std::result::Result<MaintenanceExecutionContext, ParoError>| -> Result<bool> {
                match result {
                    Ok(ctx) => {
                        let apply_result = apply(ctx);
                        let coordinator_result =
                            apply_result.as_ref().map(|_| true).map_err(Clone::clone);
                        let _ = tx.send(apply_result);
                        coordinator_result
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err.clone()));
                        Err(err)
                    }
                }
            },
        );

        self.enqueue(PendingRequest::Maintenance(PendingMaintenanceRequest {
            plan,
            validate: Some(Box::new(validate)),
            finish: Some(finish),
            queued_at: Instant::now(),
        }))?;

        rx.recv().map_err(|_| {
            paro_error::internal("journal coordinator worker exited before maintenance response")
        })?
    }

    pub fn submit_maintenance_context<V>(
        &self,
        plan: PreparedMaintenancePlan,
        validate: V,
    ) -> Result<MaintenanceExecutionContext>
    where
        V: FnOnce(&PreparedMaintenancePlan) -> Result<()> + Send + 'static,
    {
        let (tx, rx) = sync_channel(1);
        let finish = Box::new(
            move |result: std::result::Result<MaintenanceExecutionContext, ParoError>| -> Result<bool> {
                match result {
                    Ok(ctx) => {
                        let _ = tx.send(Ok(ctx));
                        Ok(false)
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err.clone()));
                        Err(err)
                    }
                }
            },
        );

        self.enqueue(PendingRequest::Maintenance(PendingMaintenanceRequest {
            plan,
            validate: Some(Box::new(validate)),
            finish: Some(finish),
            queued_at: Instant::now(),
        }))?;

        rx.recv().map_err(|_| {
            paro_error::internal(
                "journal coordinator worker exited before maintenance context response",
            )
        })?
    }

    fn enqueue(&self, request: PendingRequest) -> Result<()> {
        let mut state = self.inner.state.lock().unwrap();
        if let Some(err) = state.poisoned.clone() {
            return Err(err);
        }
        if state.shutdown {
            return Err(paro_error::internal(
                "journal coordinator is shutting down and cannot accept new work",
            ));
        }
        state.queue.push_back(request);
        drop(state);
        self.inner.wake_worker.notify_one();
        Ok(())
    }
}

impl Clone for JournalCoordinator {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for JournalCoordinator {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let mut state = self.inner.state.lock().unwrap();
            state.shutdown = true;
            self.inner.wake_worker.notify_all();
        }
    }
}

fn run_worker(inner: Weak<JournalCoordinatorInner>) {
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let (batch, poison) = {
            let mut state = inner.state.lock().unwrap();
            while state.queue.is_empty() && !state.shutdown {
                state = inner.wake_worker.wait(state).unwrap();
            }

            if state.queue.is_empty() && state.shutdown {
                return;
            }

            let mut batch = Vec::with_capacity(state.queue.len());
            while let Some(next) = state.queue.pop_front() {
                batch.push(next);
            }
            (batch, state.poisoned.clone())
        };

        if let Some(err) = poison {
            reject_batch(batch, err);
            continue;
        }

        process_mixed_batch(&inner, batch);
    }
}

fn process_mixed_batch(inner: &Arc<JournalCoordinatorInner>, batch: Vec<PendingRequest>) {
    let mut accepted = Vec::new();
    let batch_started_at = Instant::now();

    for request in batch {
        match request {
            PendingRequest::Commit(mut request) => {
                let Some(validate) = request.validate.take() else {
                    continue;
                };
                let Some(finish) = request.finish.take() else {
                    continue;
                };
                match validate(&request.plan) {
                    Ok(()) => {
                        let commit_id = inner.next_commit_id.fetch_add(1, Ordering::SeqCst);
                        accepted.push(AcceptedRequest::Commit {
                            commit_id,
                            record: request.plan.into_record(commit_id),
                            finish,
                            queued_at: request.queued_at,
                        });
                    }
                    Err(err) => {
                        let _ = finish(Err(err));
                    }
                }
            }
            PendingRequest::Maintenance(mut request) => {
                let Some(validate) = request.validate.take() else {
                    continue;
                };
                let Some(finish) = request.finish.take() else {
                    continue;
                };
                match validate(&request.plan) {
                    Ok(()) => {
                        let maintenance_id =
                            inner.next_maintenance_id.fetch_add(1, Ordering::SeqCst);
                        accepted.push(AcceptedRequest::Maintenance {
                            maintenance_id,
                            record: request.plan.into_record(maintenance_id),
                            finish,
                            queued_at: request.queued_at,
                        });
                    }
                    Err(err) => {
                        let _ = finish(Err(err));
                    }
                }
            }
        }
    }

    if accepted.is_empty() {
        return;
    }

    let records: Vec<JournalRecord> = accepted
        .iter()
        .map(|request| match request {
            AcceptedRequest::Commit { record, .. } => JournalRecord::Commit(record.clone()),
            AcceptedRequest::Maintenance { record, .. } => {
                JournalRecord::Maintenance(record.clone())
            }
        })
        .collect();

    let append_results = match inner.appender.as_ref() {
        Some(appender) => appender.append_records(&records),
        None => Ok(vec![
            AppendResult {
                lsn: 0,
                durable_batch_lsn: 0,
                durable_batch_size: accepted.len() as u64,
                durable_batch_bytes: 0,
                sync_latency_micros: 0,
            };
            accepted.len()
        ]),
    };

    match append_results {
        Ok(results) => {
            let batch_wait_micros = accepted
                .iter()
                .map(|request| match request {
                    AcceptedRequest::Commit { queued_at, .. }
                    | AcceptedRequest::Maintenance { queued_at, .. } => batch_started_at
                        .saturating_duration_since(*queued_at)
                        .as_micros()
                        .min(u64::MAX as u128)
                        as u64,
                })
                .max()
                .unwrap_or(0);
            if let Some(last) = results.last().copied() {
                let last_commit_id = accepted
                    .iter()
                    .filter_map(|request| match request {
                        AcceptedRequest::Commit { commit_id, .. } => Some(*commit_id),
                        AcceptedRequest::Maintenance { .. } => None,
                    })
                    .next_back()
                    .unwrap_or(0);
                update_durable_frontier(inner, last.durable_batch_lsn, last_commit_id);
                tracing::info!(
                    target: targets::WAL,
                    first_lsn = results.first().map(|result| result.lsn).unwrap_or(0),
                    durable_batch_lsn = last.durable_batch_lsn,
                    group_size = last.durable_batch_size,
                    batch_bytes = last.durable_batch_bytes,
                    sync_latency_micros = last.sync_latency_micros,
                    batch_wait_micros = batch_wait_micros,
                    commit_records = accepted
                        .iter()
                        .filter(|request| matches!(request, AcceptedRequest::Commit { .. }))
                        .count(),
                    maintenance_records = accepted
                        .iter()
                        .filter(|request| matches!(request, AcceptedRequest::Maintenance { .. }))
                        .count(),
                    "Durable journal batch appended"
                );
            }

            for (request, append_result) in accepted.into_iter().zip(results.into_iter()) {
                match request {
                    AcceptedRequest::Commit {
                        commit_id,
                        record,
                        finish,
                        ..
                    } => {
                        let context = CommitExecutionContext {
                            commit_id,
                            lsn: append_result.lsn,
                            durable_batch_lsn: append_result.durable_batch_lsn,
                            durable_batch_size: append_result.durable_batch_size,
                            durable_batch_bytes: append_result.durable_batch_bytes,
                            sync_latency_micros: append_result.sync_latency_micros,
                            record,
                        };
                        match finish(Ok(context)) {
                            Ok(true) => {
                                update_published_frontier(inner, append_result.lsn, commit_id)
                            }
                            Ok(false) => {}
                            Err(err) => {
                                poison_coordinator(inner, err);
                                return;
                            }
                        }
                    }
                    AcceptedRequest::Maintenance {
                        maintenance_id,
                        record,
                        finish,
                        ..
                    } => {
                        let context = MaintenanceExecutionContext {
                            maintenance_id,
                            lsn: append_result.lsn,
                            durable_batch_lsn: append_result.durable_batch_lsn,
                            durable_batch_size: append_result.durable_batch_size,
                            durable_batch_bytes: append_result.durable_batch_bytes,
                            sync_latency_micros: append_result.sync_latency_micros,
                            record,
                        };
                        match finish(Ok(context)) {
                            Ok(true) => update_published_frontier(inner, append_result.lsn, 0),
                            Ok(false) => {}
                            Err(err) => {
                                poison_coordinator(inner, err);
                                return;
                            }
                        }
                    }
                }
            }
        }
        Err(err) => {
            for request in accepted {
                match request {
                    AcceptedRequest::Commit { finish, .. } => {
                        let _ = finish(Err(err.clone()));
                    }
                    AcceptedRequest::Maintenance { finish, .. } => {
                        let _ = finish(Err(err.clone()));
                    }
                }
            }
        }
    }
}

fn reject_batch(batch: Vec<PendingRequest>, err: ParoError) {
    for request in batch {
        match request {
            PendingRequest::Commit(mut request) => {
                if let Some(finish) = request.finish.take() {
                    let _ = finish(Err(err.clone()));
                }
            }
            PendingRequest::Maintenance(mut request) => {
                if let Some(finish) = request.finish.take() {
                    let _ = finish(Err(err.clone()));
                }
            }
        }
    }
}

fn update_durable_frontier(
    inner: &Arc<JournalCoordinatorInner>,
    durable_lsn: u64,
    durable_commit_id: u64,
) {
    if let Some(runtime) = inner.apply_runtime.lock().unwrap().as_ref() {
        runtime.note_durable_append(
            durable_lsn,
            (durable_commit_id != 0).then_some(durable_commit_id),
        );
        return;
    }

    let mut state = inner.state.lock().unwrap();
    state.fallback_frontiers.durable_lsn = state.fallback_frontiers.durable_lsn.max(durable_lsn);
    state.fallback_frontiers.durable_commit_id = state
        .fallback_frontiers
        .durable_commit_id
        .max(durable_commit_id);
}

fn update_published_frontier(inner: &Arc<JournalCoordinatorInner>, lsn: u64, commit_id: u64) {
    if inner.apply_runtime.lock().unwrap().is_some() {
        return;
    }

    let mut state = inner.state.lock().unwrap();
    state.fallback_frontiers.applied_lsn = state.fallback_frontiers.applied_lsn.max(lsn);
    state.fallback_frontiers.published_lsn = state.fallback_frontiers.published_lsn.max(lsn);
    if commit_id != 0 {
        state.fallback_frontiers.published_commit_id =
            state.fallback_frontiers.published_commit_id.max(commit_id);
    }
}

fn poison_coordinator(inner: &Arc<JournalCoordinatorInner>, err: ParoError) {
    let mut state = inner.state.lock().unwrap();
    if state.poisoned.is_none() {
        state.poisoned = Some(paro_error::internal(format!(
            "journal coordinator halted after durable apply failure: {}",
            err
        )));
    }
}

fn bump_atomic_min(atomic: &AtomicU64, min_value: u64) -> u64 {
    loop {
        let current = atomic.load(Ordering::SeqCst);
        if current >= min_value {
            return current;
        }
        if atomic
            .compare_exchange(current, min_value, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return min_value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appender::JournalSink;
    use crate::{ApplyRequest, JournalApplyRuntime, TabletApplyPart, WaitMode};
    use parking_lot::Mutex as ParkingMutex;
    use paro_common::journal::MaintenanceKind;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct RecordingSink {
        batch_sizes: ParkingMutex<Vec<usize>>,
    }

    impl JournalSink for RecordingSink {
        fn append_batch(&self, frames: &[Vec<u8>]) -> Result<()> {
            self.batch_sizes.lock().push(frames.len());
            Ok(())
        }
    }

    fn empty_commit_plan(txn_id: u64) -> PreparedCommitPlan {
        PreparedCommitPlan {
            txn_id,
            start_time: txn_id,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        }
    }

    fn empty_maintenance_plan() -> PreparedMaintenancePlan {
        PreparedMaintenancePlan {
            kind: MaintenanceKind::Compaction,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        }
    }

    #[test]
    fn mixed_batch_shares_single_append_for_commit_and_maintenance() {
        let sink = Arc::new(RecordingSink::default());
        let appender = Arc::new(JournalAppender::new(sink.clone()));
        let inner = Arc::new(JournalCoordinatorInner {
            appender: Some(appender),
            apply_runtime: Mutex::new(None),
            next_commit_id: AtomicU64::new(1),
            next_maintenance_id: AtomicU64::new(1),
            state: Mutex::new(CoordinatorState::default()),
            wake_worker: Condvar::new(),
        });

        let (commit_tx, commit_rx) = sync_channel(1);
        let (maintenance_tx, maintenance_rx) = sync_channel(1);
        process_mixed_batch(
            &inner,
            vec![
                PendingRequest::Commit(PendingCommitRequest {
                    plan: empty_commit_plan(7),
                    validate: Some(Box::new(|_| Ok(()))),
                    finish: Some(Box::new(move |result| {
                        commit_tx.send(result).unwrap();
                        Ok(false)
                    })),
                    queued_at: Instant::now(),
                }),
                PendingRequest::Maintenance(PendingMaintenanceRequest {
                    plan: empty_maintenance_plan(),
                    validate: Some(Box::new(|_| Ok(()))),
                    finish: Some(Box::new(move |result| {
                        maintenance_tx.send(result).unwrap();
                        Ok(false)
                    })),
                    queued_at: Instant::now(),
                }),
            ],
        );

        let commit = commit_rx.recv().unwrap().unwrap();
        let maintenance = maintenance_rx.recv().unwrap().unwrap();
        assert_eq!(*sink.batch_sizes.lock(), vec![2]);
        assert_eq!(commit.commit_id, 1);
        assert_eq!(maintenance.maintenance_id, 1);
        assert_eq!(commit.durable_batch_lsn, maintenance.durable_batch_lsn);
        assert_eq!(commit.lsn, 1);
        assert_eq!(maintenance.lsn, 2);
    }

    #[test]
    fn maintenance_id_bootstraps_from_floor() {
        let coordinator = JournalCoordinator::new(None);
        coordinator.sync_maintenance_id_with(9);
        let result = coordinator
            .submit_maintenance_context(empty_maintenance_plan(), |_| Ok(()))
            .unwrap();
        assert_eq!(result.maintenance_id, 10);
    }

    #[test]
    fn frontiers_proxy_bound_apply_runtime_without_crossing_apply_gap() {
        let sink = Arc::new(RecordingSink::default());
        let appender = Arc::new(JournalAppender::new(sink));
        let coordinator = JournalCoordinator::new(Some(appender));
        let runtime = Arc::new(JournalApplyRuntime::new());
        coordinator.bind_apply_runtime(Arc::clone(&runtime));

        let slow_part_started = Arc::new(AtomicBool::new(false));
        let release_slow_part = Arc::new((StdMutex::new(false), Condvar::new()));
        let first_ctx = coordinator
            .submit_commit_context(empty_commit_plan(1), |_| Ok(()))
            .unwrap();
        let runtime_first = Arc::clone(&runtime);
        let slow_part_started_first = Arc::clone(&slow_part_started);
        let release_slow_part_first = Arc::clone(&release_slow_part);

        let first = thread::spawn(move || {
            runtime_first
                .submit(ApplyRequest {
                    lsn: first_ctx.lsn,
                    durable_batch_lsn: first_ctx.durable_batch_lsn,
                    commit_id: Some(first_ctx.commit_id),
                    wait_mode: WaitMode::Published,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: vec![TabletApplyPart {
                        tablet_id: 41,
                        apply: Box::new(move || {
                            slow_part_started_first.store(true, Ordering::Release);
                            let (lock, wake) = &*release_slow_part_first;
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

        for _ in 0..20 {
            if slow_part_started.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(slow_part_started.load(Ordering::Acquire));

        let second = coordinator
            .submit_commit_context(empty_commit_plan(2), |_| Ok(()))
            .unwrap();
        let stalled = coordinator.frontiers();
        assert_eq!(stalled.durable_lsn, second.lsn);
        assert_eq!(stalled.durable_commit_id, second.commit_id);
        assert_eq!(stalled.applied_lsn, 0);
        assert_eq!(stalled.published_lsn, 0);
        assert_eq!(stalled.published_commit_id, 0);

        let (lock, wake) = &*release_slow_part;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        first.join().unwrap();

        runtime
            .submit(ApplyRequest {
                lsn: second.lsn,
                durable_batch_lsn: second.durable_batch_lsn,
                commit_id: Some(second.commit_id),
                wait_mode: WaitMode::Published,
                catalog_serial: false,
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: Vec::new(),
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            })
            .unwrap();

        let frontiers = coordinator.frontiers();
        assert_eq!(frontiers.applied_lsn, second.lsn);
        assert_eq!(frontiers.published_lsn, second.lsn);
        assert_eq!(frontiers.published_commit_id, second.commit_id);
    }
}
