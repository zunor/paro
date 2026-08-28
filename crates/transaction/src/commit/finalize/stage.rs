// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered commit-finalize stage.

use super::super::{
    CommitAckPolicy, CommitCompletionHandle, CommitDurableBatch, DurableCommitHandleError,
    DurableCommitJob, JournalApplyError, PublishCompletion, PublishCompletionFallbackAck,
    PublishFatalSink, PublishSubmission, PublishSubmitError, RegistrationGate,
};
use crate::sync::{Condvar, Mutex};
use crate::types::CommitTs;
use paro_journal::JournalApplyRuntime;
use std::collections::VecDeque;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_FINALIZE_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

type SubmissionHook = Arc<dyn Fn(PublishSubmission, CommitAckPolicy) + Send + Sync + 'static>;
type CompletionHook = Arc<dyn Fn(CommitTs, Result<(), JournalApplyError>) + Send + Sync + 'static>;
type ApplyErrorHook = Arc<dyn Fn(CommitTs, &JournalApplyError) + Send + Sync + 'static>;
type SubmitErrorHook = Arc<dyn Fn(CommitTs, &PublishSubmitError) + Send + Sync + 'static>;
type StageErrorHook = Arc<dyn Fn(&CommitFinalizeStageError) + Send + Sync + 'static>;
type DurableAmbiguousHook = Arc<dyn Fn(CommitCompletionHandle, Arc<str>) + Send + Sync + 'static>;
type RegisteredHook = Arc<dyn Fn(CommitTs) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitFinalizeStageOptions {
    pub queue_capacity: usize,
    pub graceful_shutdown_timeout: Duration,
}

impl Default for CommitFinalizeStageOptions {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_FINALIZE_QUEUE_CAPACITY,
            graceful_shutdown_timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
        }
    }
}

#[derive(Clone)]
pub struct CommitFinalizeStageHooks {
    pub on_submission: SubmissionHook,
    pub on_registered: RegisteredHook,
    pub on_complete: CompletionHook,
    pub fallback_ack: ApplyErrorHook,
    pub fatal_sink: ApplyErrorHook,
    pub on_submit_error: SubmitErrorHook,
    pub on_stage_error: StageErrorHook,
    pub on_durable_ambiguous: DurableAmbiguousHook,
}

impl Default for CommitFinalizeStageHooks {
    fn default() -> Self {
        Self {
            on_submission: Arc::new(|_, _| {}),
            on_registered: Arc::new(|_| {}),
            on_complete: Arc::new(|_, _| {}),
            fallback_ack: Arc::new(|_, _| {}),
            fatal_sink: Arc::new(|_, _| {}),
            on_submit_error: Arc::new(|_, _| {}),
            on_stage_error: Arc::new(|_| {}),
            on_durable_ambiguous: Arc::new(|_, _| {}),
        }
    }
}

impl fmt::Debug for CommitFinalizeStageHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitFinalizeStageHooks")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitFinalizeStageError {
    Phase1 {
        commit_ts: CommitTs,
        durable_lsn: u64,
        message: Arc<str>,
    },
    DurableHandle {
        offset: usize,
        message: Arc<str>,
    },
    BuildRequest {
        commit_ts: CommitTs,
        durable_lsn: u64,
        message: Arc<str>,
    },
    Submit {
        commit_ts: CommitTs,
        durable_lsn: u64,
        error: PublishSubmitError,
    },
}

impl fmt::Display for CommitFinalizeStageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Phase1 {
                commit_ts,
                durable_lsn,
                message,
            } => write!(
                f,
                "commit-finalize phase1 failed at {} lsn {}: {}",
                commit_ts, durable_lsn, message
            ),
            Self::DurableHandle { offset, message } => {
                write!(
                    f,
                    "commit-finalize durable handle failed at offset {offset}: {message}"
                )
            }
            Self::BuildRequest {
                commit_ts,
                durable_lsn,
                message,
            } => write!(
                f,
                "commit-finalize build request failed at {} lsn {}: {}",
                commit_ts, durable_lsn, message
            ),
            Self::Submit {
                commit_ts,
                durable_lsn,
                error,
            } => write!(
                f,
                "commit-finalize submit failed at {} lsn {}: {}",
                commit_ts, durable_lsn, error
            ),
        }
    }
}

impl std::error::Error for CommitFinalizeStageError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitFinalizeStageScheduleError {
    ForcedShutdown,
    Poisoned(CommitFinalizeStageError),
}

impl fmt::Display for CommitFinalizeStageScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForcedShutdown => write!(f, "commit-finalize stage is forced shut down"),
            Self::Poisoned(error) => write!(f, "commit-finalize stage poisoned: {error}"),
        }
    }
}

impl std::error::Error for CommitFinalizeStageScheduleError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitFinalizeWaitError {
    Poisoned(CommitFinalizeStageError),
}

impl fmt::Display for CommitFinalizeWaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Poisoned(error) => write!(f, "commit-finalize wait poisoned: {error}"),
        }
    }
}

impl std::error::Error for CommitFinalizeWaitError {}

#[derive(Clone)]
pub struct CommitFinalizeStage {
    inner: Arc<CommitFinalizeStageInner>,
}

impl CommitFinalizeStage {
    pub fn new_inline(
        apply_runtime: Arc<JournalApplyRuntime>,
        options: CommitFinalizeStageOptions,
        hooks: CommitFinalizeStageHooks,
    ) -> Self {
        assert!(
            options.queue_capacity > 0,
            "finalize queue capacity must be nonzero"
        );
        let inner = Arc::new(CommitFinalizeStageInner {
            state: Mutex::new(FinalizeStageState {
                queue: VecDeque::with_capacity(options.queue_capacity),
                shutdown: CommitFinalizeShutdownMode::Running,
                poison: None,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            gate: RegistrationGate::default(),
            options,
            apply_runtime,
            hooks,
        });
        let worker_inner = Arc::clone(&inner);
        thread::Builder::new()
            .name("paro-commit-finalize".to_string())
            .spawn(move || run_finalize_worker(worker_inner))
            .expect("spawn commit finalize worker");
        Self { inner }
    }

    pub fn schedule(
        &self,
        accepted: Vec<DurableCommitJob>,
        batch: Arc<CommitDurableBatch>,
    ) -> Result<(), CommitFinalizeStageScheduleError> {
        self.inner
            .schedule(ScheduledFinalizeBatch { accepted, batch })
    }

    pub fn wait_until_registered(&self, floor: CommitTs) -> Result<(), CommitFinalizeWaitError> {
        self.inner
            .gate
            .wait_until_registered(floor, || self.inner.poison_snapshot())
    }

    pub fn registered_commit_ts(&self) -> CommitTs {
        self.inner.gate.registered_commit_ts()
    }

    pub(crate) fn mark_recovered_through(&self, commit_ts: CommitTs) {
        self.inner.gate.mark_registered(commit_ts);
    }

    pub fn queue_depth(&self) -> usize {
        self.inner.state.lock().queue.len()
    }

    pub fn start_graceful_shutdown(&self, timeout: Option<Duration>) {
        let deadline =
            Instant::now() + timeout.unwrap_or(self.inner.options.graceful_shutdown_timeout);
        let mut state = self.inner.state.lock();
        if matches!(state.shutdown, CommitFinalizeShutdownMode::Running) {
            state.shutdown = CommitFinalizeShutdownMode::GracefulDraining { deadline };
            self.inner.not_empty.notify_all();
            self.inner.not_full.notify_all();
        }
    }

    pub fn force_shutdown(&self) {
        self.inner.force_shutdown();
    }
}

impl fmt::Debug for CommitFinalizeStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitFinalizeStage")
            .field("registered_commit_ts", &self.registered_commit_ts())
            .finish_non_exhaustive()
    }
}

impl Drop for CommitFinalizeStage {
    fn drop(&mut self) {
        // The worker owns one strong reference while it is blocked on the queue.
        // When the last external handle is dropped, wake it so poisoned database
        // close/reopen does not leave a parked finalize thread behind.
        if Arc::strong_count(&self.inner) <= 2 {
            self.inner.force_shutdown();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFinalizeShutdownMode {
    Running,
    GracefulDraining { deadline: Instant },
    Forced,
}

struct CommitFinalizeStageInner {
    state: Mutex<FinalizeStageState>,
    not_empty: Condvar,
    not_full: Condvar,
    gate: RegistrationGate,
    options: CommitFinalizeStageOptions,
    apply_runtime: Arc<JournalApplyRuntime>,
    hooks: CommitFinalizeStageHooks,
}

struct FinalizeStageState {
    queue: VecDeque<ScheduledFinalizeBatch>,
    shutdown: CommitFinalizeShutdownMode,
    poison: Option<CommitFinalizeStageError>,
}

struct ScheduledFinalizeBatch {
    accepted: Vec<DurableCommitJob>,
    batch: Arc<CommitDurableBatch>,
}

impl CommitFinalizeStageInner {
    fn schedule(
        &self,
        batch: ScheduledFinalizeBatch,
    ) -> Result<(), CommitFinalizeStageScheduleError> {
        let mut state = self.state.lock();
        loop {
            if let Some(error) = state.poison.clone() {
                return Err(CommitFinalizeStageScheduleError::Poisoned(error));
            }
            match state.shutdown {
                CommitFinalizeShutdownMode::Forced => {
                    return Err(CommitFinalizeStageScheduleError::ForcedShutdown);
                }
                CommitFinalizeShutdownMode::GracefulDraining { deadline }
                    if Instant::now() >= deadline =>
                {
                    state.shutdown = CommitFinalizeShutdownMode::Forced;
                    self.not_empty.notify_all();
                    self.not_full.notify_all();
                    return Err(CommitFinalizeStageScheduleError::ForcedShutdown);
                }
                _ => {}
            }
            if state.queue.len() < self.options.queue_capacity {
                state.queue.push_back(batch);
                self.not_empty.notify_one();
                return Ok(());
            }
            state = match state.shutdown {
                CommitFinalizeShutdownMode::GracefulDraining { deadline } => {
                    let now = Instant::now();
                    if now >= deadline {
                        state.shutdown = CommitFinalizeShutdownMode::Forced;
                        self.not_empty.notify_all();
                        self.not_full.notify_all();
                        return Err(CommitFinalizeStageScheduleError::ForcedShutdown);
                    }
                    self.not_full.wait_timeout(state, deadline - now).0
                }
                _ => self.not_full.wait(state),
            };
        }
    }

    fn force_shutdown(&self) {
        let mut state = self.state.lock();
        state.shutdown = CommitFinalizeShutdownMode::Forced;
        self.not_empty.notify_all();
        self.not_full.notify_all();
        self.gate.notify_all();
    }

    fn poison(&self, error: CommitFinalizeStageError) {
        let mut state = self.state.lock();
        if state.poison.is_none() {
            state.poison = Some(error);
        }
        state.shutdown = CommitFinalizeShutdownMode::Forced;
        self.not_empty.notify_all();
        self.not_full.notify_all();
        self.gate.notify_all();
    }

    fn poison_snapshot(&self) -> Option<CommitFinalizeStageError> {
        self.state.lock().poison.clone()
    }

    fn drain_queued_jobs(&self) -> Vec<CommitCompletionHandle> {
        let mut state = self.state.lock();
        state
            .queue
            .drain(..)
            .flat_map(|batch| batch.accepted)
            .map(release_unprocessed_job)
            .collect()
    }

    fn pop_batch(&self) -> Option<ScheduledFinalizeBatch> {
        let mut state = self.state.lock();
        loop {
            if let Some(batch) = state.queue.pop_front() {
                self.not_full.notify_one();
                return Some(batch);
            }
            if matches!(state.shutdown, CommitFinalizeShutdownMode::Forced)
                || state.poison.is_some()
            {
                return None;
            }
            state = self.not_empty.wait(state);
        }
    }
}

fn run_finalize_worker(inner: Arc<CommitFinalizeStageInner>) {
    while let Some(batch) = inner.pop_batch() {
        if let Err(failure) = process_finalize_batch(&inner, batch) {
            let message = Arc::from(failure.error.to_string());
            for completion in failure
                .ambiguous_jobs
                .into_iter()
                .chain(inner.drain_queued_jobs())
            {
                (inner.hooks.on_durable_ambiguous)(completion, Arc::clone(&message));
            }
            (inner.hooks.on_stage_error)(&failure.error);
            inner.poison(failure.error);
            return;
        }
    }
}

struct ProcessFinalizeBatchFailure {
    error: CommitFinalizeStageError,
    ambiguous_jobs: Vec<CommitCompletionHandle>,
}

fn process_finalize_batch(
    inner: &Arc<CommitFinalizeStageInner>,
    batch: ScheduledFinalizeBatch,
) -> Result<(), ProcessFinalizeBatchFailure> {
    debug_assert_eq!(
        batch.accepted.len(),
        batch.batch.record_count(),
        "finalize jobs must preserve durable-batch offset order"
    );
    let mut accepted = VecDeque::from(batch.accepted);
    let mut offset = 0_usize;
    while let Some(job) = accepted.pop_front() {
        let current_completion = job.completion;
        let handle = match batch.batch.handle_at(offset as u32) {
            Ok(handle) => handle,
            Err(err) => {
                return Err(finalize_failure_unprocessed(
                    durable_handle_error(offset, err),
                    Some(job),
                    accepted,
                ));
            }
        };
        let commit_ts = job.commit_ts;
        let durable_lsn = handle.durable_lsn();

        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
            job.finalize_reservation.apply();
            job.lock_release_plan.apply();
            job.pre_publish_release_plan.apply();
        })) {
            return Err(finalize_failure_after_current(
                CommitFinalizeStageError::Phase1 {
                    commit_ts,
                    durable_lsn,
                    message: panic_message(panic),
                },
                current_completion,
                accepted,
            ));
        }
        inner.gate.mark_registered(commit_ts);
        (inner.hooks.on_registered)(commit_ts);

        let request = match catch_unwind(AssertUnwindSafe(|| {
            (job.required_publish.build_apply_request)(handle.clone())
        })) {
            Ok(request) => request,
            Err(panic) => {
                return Err(finalize_failure_after_current(
                    CommitFinalizeStageError::BuildRequest {
                        commit_ts,
                        durable_lsn,
                        message: panic_message(panic),
                    },
                    current_completion,
                    accepted,
                ));
            }
        };
        let completion = completion_callback(commit_ts, Arc::clone(&inner.hooks.on_complete));
        let fallback_ack = fallback_ack_callback(commit_ts, Arc::clone(&inner.hooks.fallback_ack));
        let fatal_sink = fatal_sink_callback(commit_ts, Arc::clone(&inner.hooks.fatal_sink));
        if let Err(error) = inner.apply_runtime.submit_async_with_completion(
            request,
            completion,
            fallback_ack,
            fatal_sink,
        ) {
            let error = PublishSubmitError::from(error);
            (inner.hooks.on_submit_error)(commit_ts, &error);
            return Err(finalize_failure_after_current(
                CommitFinalizeStageError::Submit {
                    commit_ts,
                    durable_lsn,
                    error,
                },
                current_completion,
                accepted,
            ));
        }

        (inner.hooks.on_submission)(PublishSubmission { commit_ts }, job.ack_policy);
        offset += 1;
    }
    Ok(())
}

fn finalize_failure_after_current(
    error: CommitFinalizeStageError,
    current: CommitCompletionHandle,
    suffix: VecDeque<DurableCommitJob>,
) -> ProcessFinalizeBatchFailure {
    let mut suffix = release_unprocessed_jobs(suffix);
    let mut ambiguous_jobs = Vec::with_capacity(suffix.len() + 1);
    ambiguous_jobs.push(current);
    ambiguous_jobs.append(&mut suffix);
    ProcessFinalizeBatchFailure {
        error,
        ambiguous_jobs,
    }
}

fn finalize_failure_unprocessed(
    error: CommitFinalizeStageError,
    current: Option<DurableCommitJob>,
    suffix: VecDeque<DurableCommitJob>,
) -> ProcessFinalizeBatchFailure {
    let current = current.into_iter().map(release_unprocessed_job);
    let suffix = release_unprocessed_jobs(suffix);
    let ambiguous_jobs = current.chain(suffix).collect();
    ProcessFinalizeBatchFailure {
        error,
        ambiguous_jobs,
    }
}

fn release_unprocessed_jobs(accepted: VecDeque<DurableCommitJob>) -> Vec<CommitCompletionHandle> {
    accepted.into_iter().map(release_unprocessed_job).collect()
}

fn release_unprocessed_job(job: DurableCommitJob) -> CommitCompletionHandle {
    job.finalize_reservation.release();
    job.lock_release_plan.apply();
    job.pre_publish_release_plan.apply();
    job.completion
}

fn durable_handle_error(offset: usize, err: DurableCommitHandleError) -> CommitFinalizeStageError {
    CommitFinalizeStageError::DurableHandle {
        offset,
        message: Arc::from(err.to_string()),
    }
}

fn completion_callback(commit_ts: CommitTs, hook: CompletionHook) -> PublishCompletion {
    Box::new(move |result| hook(commit_ts, result))
}

fn fallback_ack_callback(
    commit_ts: CommitTs,
    hook: ApplyErrorHook,
) -> PublishCompletionFallbackAck {
    Box::new(move |error| hook(commit_ts, error))
}

fn fatal_sink_callback(commit_ts: CommitTs, hook: ApplyErrorHook) -> PublishFatalSink {
    Box::new(move |error| hook(commit_ts, error))
}

fn panic_message(panic: Box<dyn std::any::Any + Send + 'static>) -> Arc<str> {
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
    use crate::commit::RequiredPublishPlan;
    use crate::{CommitAckPolicy, CommitDurableBatch};
    use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    fn durable_batch(count: usize) -> Arc<CommitDurableBatch> {
        Arc::new(
            CommitDurableBatch::new(
                1,
                1,
                count as u64,
                (count as u64) * 64,
                Arc::from(vec![64_u32; count]),
                10,
                CommitTs::new(1),
                CommitTs::new(count as u64),
            )
            .unwrap(),
        )
    }

    fn durable_job(commit_ts: CommitTs, published: Arc<AtomicBool>) -> DurableCommitJob {
        durable_job_with_plan(
            commit_ts,
            RequiredPublishPlan::new(
                Box::new(move |handle| {
                    let published = Arc::clone(&published);
                    ApplyRequest {
                        lsn: handle.durable_lsn(),
                        durable_batch_lsn: handle.durable_batch_lsn(),
                        commit_id: Some(handle.commit_ts().into_raw()),
                        publication_watermarks:
                            paro_common::journal::JournalPublicationWatermarks::default(),
                        wait_mode: WaitMode::Published,
                        catalog_serial: false,
                        catalog_pre: Box::new(|| Ok(())),
                        tablet_parts: Vec::<TabletApplyPart>::new(),
                        descriptor_phase: Box::new(|| Ok(())),
                        catalog_post: Box::new(|| Ok(())),
                        on_published: Box::new(move || {
                            published.store(true, Ordering::Release);
                            Ok(())
                        }),
                    }
                }),
                Arc::from([]),
            ),
        )
    }

    fn durable_job_with_plan(
        commit_ts: CommitTs,
        required_publish: RequiredPublishPlan,
    ) -> DurableCommitJob {
        durable_job_with_completion(
            commit_ts,
            required_publish,
            crate::commit::CommitCompletionHandle::default(),
        )
    }

    fn durable_job_with_completion(
        commit_ts: CommitTs,
        required_publish: RequiredPublishPlan,
        completion: crate::commit::CommitCompletionHandle,
    ) -> DurableCommitJob {
        DurableCommitJob {
            commit_ts,
            retained_bytes: 1,
            finalize_reservation: crate::commit::CommitFinalizeReservation::default(),
            lock_release_plan: crate::commit::LockReleasePlan::noop(),
            pre_publish_release_plan: crate::commit::PrePublishReleasePlan::noop(),
            required_publish,
            ack_policy: CommitAckPolicy::RequiredPublished,
            completion,
        }
    }

    #[test]
    fn finalize_stage_registers_before_async_completion() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let published = Arc::new(AtomicBool::new(false));
        let (registered_tx, registered_rx) = mpsc::channel();
        let (submitted_tx, submitted_rx) = mpsc::channel();
        let (complete_tx, complete_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let hooks = CommitFinalizeStageHooks {
            on_registered: Arc::new(move |commit_ts| {
                registered_tx.send(commit_ts).unwrap();
            }),
            on_submission: Arc::new(move |submission, _| {
                submitted_tx.send(submission.commit_ts).unwrap();
            }),
            on_complete: Arc::new(move |commit_ts, result| {
                result.unwrap();
                complete_tx.send(commit_ts).unwrap();
            }),
            ..CommitFinalizeStageHooks::default()
        };
        let stage =
            CommitFinalizeStage::new_inline(runtime, CommitFinalizeStageOptions::default(), hooks);
        let published_for_request = Arc::clone(&published);

        stage
            .schedule(
                vec![durable_job_with_plan(
                    CommitTs::new(1),
                    RequiredPublishPlan::new(
                        Box::new(move |handle| {
                            release_rx
                                .recv_timeout(Duration::from_secs(2))
                                .expect("test must release apply request after registration");
                            let published = Arc::clone(&published_for_request);
                            ApplyRequest {
                                lsn: handle.durable_lsn(),
                                durable_batch_lsn: handle.durable_batch_lsn(),
                                commit_id: Some(handle.commit_ts().into_raw()),
                                publication_watermarks:
                                    paro_common::journal::JournalPublicationWatermarks::default(),
                                wait_mode: WaitMode::Published,
                                catalog_serial: false,
                                catalog_pre: Box::new(|| Ok(())),
                                tablet_parts: Vec::<TabletApplyPart>::new(),
                                descriptor_phase: Box::new(|| Ok(())),
                                catalog_post: Box::new(|| Ok(())),
                                on_published: Box::new(move || {
                                    published.store(true, Ordering::Release);
                                    Ok(())
                                }),
                            }
                        }),
                        Arc::from([]),
                    ),
                )],
                durable_batch(1),
            )
            .unwrap();
        assert_eq!(
            registered_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            CommitTs::new(1)
        );

        assert_eq!(stage.registered_commit_ts(), CommitTs::new(1));
        assert!(matches!(
            complete_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        assert_eq!(
            submitted_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            CommitTs::new(1)
        );
        assert_eq!(
            complete_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            CommitTs::new(1)
        );
        assert!(published.load(Ordering::Acquire));
        stage.force_shutdown();
    }

    #[test]
    fn build_apply_request_panic_poisons_stage() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let stage = CommitFinalizeStage::new_inline(
            runtime,
            CommitFinalizeStageOptions::default(),
            CommitFinalizeStageHooks::default(),
        );

        stage
            .schedule(
                vec![durable_job_with_plan(
                    CommitTs::new(1),
                    RequiredPublishPlan::new(
                        Box::new(|_| panic!("build request panic")),
                        Arc::from([]),
                    ),
                )],
                durable_batch(1),
            )
            .unwrap();
        stage.wait_until_registered(CommitTs::new(1)).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(error) = stage.inner.poison_snapshot() {
                match error {
                    CommitFinalizeStageError::BuildRequest { commit_ts, .. } => {
                        assert_eq!(commit_ts, CommitTs::new(1));
                    }
                    other => panic!("unexpected stage poison: {other:?}"),
                }
                match stage.schedule(
                    vec![durable_job(
                        CommitTs::new(1),
                        Arc::new(AtomicBool::new(false)),
                    )],
                    durable_batch(1),
                ) {
                    Err(CommitFinalizeStageScheduleError::Poisoned(
                        CommitFinalizeStageError::BuildRequest { commit_ts, .. },
                    )) => {
                        assert_eq!(commit_ts, CommitTs::new(1));
                    }
                    other => panic!("unexpected schedule result: {other:?}"),
                }
                break;
            }
            if Instant::now() >= deadline {
                panic!("stage was not poisoned after build request panic");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn build_apply_request_panic_marks_batch_suffix_ambiguous() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let (ambiguous_tx, ambiguous_rx) = mpsc::channel();
        let hooks = CommitFinalizeStageHooks {
            on_durable_ambiguous: Arc::new(move |completion, _| {
                ambiguous_tx.send(completion.slot_id).unwrap();
            }),
            ..CommitFinalizeStageHooks::default()
        };
        let stage =
            CommitFinalizeStage::new_inline(runtime, CommitFinalizeStageOptions::default(), hooks);

        stage
            .schedule(
                vec![
                    durable_job_with_completion(
                        CommitTs::new(1),
                        RequiredPublishPlan::new(
                            Box::new(|_| panic!("build request panic")),
                            Arc::from([]),
                        ),
                        crate::commit::CommitCompletionHandle { slot_id: 11 },
                    ),
                    durable_job_with_completion(
                        CommitTs::new(2),
                        RequiredPublishPlan::noop_for_tests(),
                        crate::commit::CommitCompletionHandle { slot_id: 12 },
                    ),
                ],
                durable_batch(2),
            )
            .unwrap();

        let first = ambiguous_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = ambiguous_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!([first, second], [11, 12]);
        stage.force_shutdown();
    }

    #[test]
    fn explicit_force_shutdown_rejects_future_schedule() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let stage = CommitFinalizeStage::new_inline(
            runtime,
            CommitFinalizeStageOptions::default(),
            CommitFinalizeStageHooks::default(),
        );
        let clone = stage.clone();

        drop(stage);
        clone
            .schedule(
                vec![durable_job(
                    CommitTs::new(1),
                    Arc::new(AtomicBool::new(false)),
                )],
                durable_batch(1),
            )
            .unwrap();
        clone.force_shutdown();
        let err = clone
            .schedule(
                vec![durable_job(
                    CommitTs::new(1),
                    Arc::new(AtomicBool::new(false)),
                )],
                durable_batch(1),
            )
            .expect_err("explicit forced shutdown should reject schedule");
        assert_eq!(err, CommitFinalizeStageScheduleError::ForcedShutdown);
    }

    #[test]
    fn graceful_schedule_timeout_escalates_to_forced() {
        let runtime = Arc::new(JournalApplyRuntime::new());
        let stage = CommitFinalizeStage::new_inline(
            runtime,
            CommitFinalizeStageOptions {
                queue_capacity: 1,
                graceful_shutdown_timeout: Duration::from_millis(1),
            },
            CommitFinalizeStageHooks::default(),
        );
        stage.start_graceful_shutdown(Some(Duration::from_millis(0)));
        let err = stage
            .schedule(Vec::new(), durable_batch(1))
            .expect_err("expired graceful shutdown should reject schedule");
        assert_eq!(err, CommitFinalizeStageScheduleError::ForcedShutdown);
        stage.force_shutdown();
    }
}
