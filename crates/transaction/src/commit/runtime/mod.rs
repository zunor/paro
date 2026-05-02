// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit runtime queue drain, durable append, and completion-slot handoff.

mod assembly;
mod completion;
mod error;
mod poison;

use super::{
    append_durable_commit_batch, AppendCommitError, AppendFailureCleanupBundle,
    CleanupBackpressureSnapshot, CommitAppendBatch, CommitBatchPolicy,
    CommitDrainBackpressureInput, CommitDrainWakeHandle, CommitDrainWakePoolMetrics,
    CommitFinalizeStage, CommitFinalizeStageError, CommitFinalizeStageHooks,
    CommitFinalizeStageOptions, CommitFinalizeStageScheduleError, CommitFrontier,
    CommitFrontierSnapshot, CommitJournal, CommitQueue, CommitQueueEntry, CommitQueueSnapshot,
    CommitQueueTicket, CommitSequencer, CommitSequencerAppendError, CommitSequencerOrderedBatch,
    CommitSequencerOrderedError, DrainInlinePolicy, DrainSignalReason,
    DurableAmbiguousCleanupBundle, DurableCommitHandle, IsolationLevel, OrderedCommitPlan,
    PreparedCommitJob, PublishFailureCause, RecoveryReplayError, RecoveryReplayEvent,
    RecoveryReplaySummary, SequencedCommitJob,
};
use crate::sync::Mutex;
use crate::types::CommitTs;
pub use assembly::{
    CommitFinalFence, CommitFinalizeReservationFactory, CommitRuntimeAssembly,
    CommitRuntimeHealthSink,
};
use completion::CommitCompletionRegistry;
pub use error::{
    CommitCompletionError, CommitRuntimeError, CommitRuntimeFailure, CommitRuntimePoison,
    CommitRuntimeRejection,
};
use paro_journal::JournalApplyRuntime;
use poison::RuntimePoisonCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

#[derive(Clone)]
pub struct CommitRuntime {
    inner: Arc<CommitRuntimeInner>,
}

struct CommitRuntimeInner {
    journal: Arc<dyn CommitJournal>,
    policy: CommitBatchPolicy,
    sequencer: Arc<CommitSequencer>,
    queue: Arc<CommitQueue>,
    frontier: Arc<CommitFrontier>,
    apply_runtime: Arc<JournalApplyRuntime>,
    finalize_stage: CommitFinalizeStage,
    completions: Arc<CommitCompletionRegistry>,
    reservation_factory: CommitFinalizeReservationFactory,
    final_fence: CommitFinalFence,
    cleanup_snapshot: Arc<dyn Fn() -> CleanupBackpressureSnapshot + Send + Sync + 'static>,
    wake_handle: Option<CommitDrainWakeHandle>,
    health_sink: Option<CommitRuntimeHealthSink>,
    admission_open: AtomicBool,
    poison: Mutex<Option<CommitRuntimePoison>>,
}

impl CommitRuntime {
    pub fn new(assembly: CommitRuntimeAssembly) -> Self {
        let queue = CommitQueue::new();
        let completions = Arc::new(CommitCompletionRegistry::default());
        let frontier = Arc::clone(&assembly.frontier);
        let poison_cell = Arc::new(RuntimePoisonCell::default());
        let drain_cell = Arc::new(Mutex::<Option<Weak<CommitRuntimeInner>>>::new(None));
        let wake_handle = assembly.wake_handle.clone();

        let finalize_hooks = CommitFinalizeStageHooks {
            on_submission: {
                let completions = Arc::clone(&completions);
                Arc::new(move |submission, ack_policy| {
                    completions.mark_publish_submitted(submission.commit_ts, ack_policy);
                })
            },
            on_registered: {
                let queue = Arc::clone(&queue);
                let wake_handle = wake_handle.clone();
                let drain_cell = Arc::clone(&drain_cell);
                Arc::new(move |commit_ts| {
                    if let Some(wake) = &wake_handle {
                        queue
                            .signal_pending_fence_ready(commit_ts, DrainInlinePolicy::WakePoolOnly);
                        wake.signal(DrainSignalReason::PendingFenceReady);
                        return;
                    }

                    if queue.signal_pending_fence_ready(commit_ts, DrainInlinePolicy::AllowInline) {
                        let Some(runtime) = drain_cell.lock().as_ref().and_then(Weak::upgrade)
                        else {
                            return;
                        };
                        runtime.drain_inline(None);
                    }
                })
            },
            on_complete: {
                let completions = Arc::clone(&completions);
                let frontier = Arc::clone(&frontier);
                let poison_cell = Arc::clone(&poison_cell);
                Arc::new(move |commit_ts, result| match result {
                    Ok(()) => completions.mark_published(commit_ts),
                    Err(error) => {
                        frontier.mark_publish_failed(
                            commit_ts,
                            PublishFailureCause::apply_with_diagnostics(
                                error.lsn,
                                error.error_code,
                                error.message.clone(),
                            ),
                        );
                        poison_cell.poison(CommitRuntimePoison::Apply {
                            commit_ts,
                            message: Arc::from(error.to_string()),
                        });
                        completions.mark_failed(commit_ts, CommitRuntimeFailure::Apply(error));
                    }
                })
            },
            fallback_ack: {
                let completions = Arc::clone(&completions);
                Arc::new(move |commit_ts, error| {
                    completions.mark_failed(
                        commit_ts,
                        CommitRuntimeFailure::CompletionPanic(error.clone()),
                    );
                })
            },
            fatal_sink: {
                let frontier = Arc::clone(&assembly.frontier);
                let poison_cell = Arc::clone(&poison_cell);
                Arc::new(move |commit_ts, error| {
                    frontier.mark_publish_failed(
                        commit_ts,
                        PublishFailureCause::apply_with_diagnostics(
                            error.lsn,
                            error.error_code,
                            error.message.clone(),
                        ),
                    );
                    poison_cell.poison(CommitRuntimePoison::CompletionPanic {
                        commit_ts,
                        message: Arc::from(error.to_string()),
                    });
                })
            },
            on_submit_error: Arc::new(|_, _| {}),
            on_stage_error: {
                let frontier = Arc::clone(&assembly.frontier);
                let completions = Arc::clone(&completions);
                let poison_cell = Arc::clone(&poison_cell);
                Arc::new(move |error| match error {
                    CommitFinalizeStageError::Phase1 {
                        commit_ts,
                        durable_lsn,
                        message,
                    }
                    | CommitFinalizeStageError::BuildRequest {
                        commit_ts,
                        durable_lsn,
                        message,
                    } => {
                        frontier.mark_publish_failed(
                            *commit_ts,
                            PublishFailureCause::phase1_with_diagnostics(
                                *durable_lsn,
                                0,
                                message.clone(),
                            ),
                        );
                        completions.mark_ambiguous_commit_ts(
                            *commit_ts,
                            CommitRuntimeFailure::Ambiguous(Arc::from(error.to_string())),
                        );
                        poison_cell.poison(CommitRuntimePoison::FinalizeStage {
                            commit_ts: Some(*commit_ts),
                            message: Arc::from(error.to_string()),
                        });
                    }
                    CommitFinalizeStageError::Submit {
                        commit_ts,
                        durable_lsn,
                        error: submit_error,
                    } => {
                        frontier.mark_publish_failed(
                            *commit_ts,
                            PublishFailureCause::submit_with_diagnostics(
                                *durable_lsn,
                                0,
                                submit_error.message(),
                            ),
                        );
                        completions.mark_ambiguous_commit_ts(
                            *commit_ts,
                            CommitRuntimeFailure::Ambiguous(Arc::from(error.to_string())),
                        );
                        poison_cell.poison(CommitRuntimePoison::Submit {
                            commit_ts: *commit_ts,
                            message: Arc::from(submit_error.to_string()),
                        });
                    }
                    CommitFinalizeStageError::DurableHandle { .. } => {
                        poison_cell.poison(CommitRuntimePoison::FinalizeStage {
                            commit_ts: None,
                            message: Arc::from(error.to_string()),
                        });
                    }
                })
            },
            on_durable_ambiguous: {
                let completions = Arc::clone(&completions);
                Arc::new(move |completion, message| {
                    completions
                        .mark_ambiguous(completion, CommitRuntimeFailure::Ambiguous(message));
                })
            },
        };

        let finalize_stage = CommitFinalizeStage::new_inline(
            Arc::clone(&assembly.apply_runtime),
            CommitFinalizeStageOptions {
                queue_capacity: assembly.policy.max_commit_finalize_pipeline_depth.max(1),
                graceful_shutdown_timeout: assembly.policy.graceful_shutdown_timeout(),
            },
            finalize_hooks,
        );

        let inner = Arc::new(CommitRuntimeInner {
            journal: assembly.journal,
            policy: assembly.policy,
            sequencer: assembly.sequencer,
            queue,
            frontier: assembly.frontier,
            apply_runtime: assembly.apply_runtime,
            finalize_stage,
            completions,
            reservation_factory: assembly.reservation_factory,
            final_fence: assembly.final_fence,
            cleanup_snapshot: assembly.cleanup_snapshot,
            wake_handle,
            health_sink: assembly.health_sink,
            admission_open: AtomicBool::new(true),
            poison: Mutex::new(None),
        });
        *drain_cell.lock() = Some(Arc::downgrade(&inner));
        inner
            .finalize_stage
            .mark_recovered_through(inner.frontier.published_commit_id());
        poison_cell.bind(&inner);
        Self { inner }
    }

    pub fn submit_commit(
        &self,
        job: PreparedCommitJob,
    ) -> Result<CommitQueueTicket, CommitRuntimeError> {
        self.inner.ensure_healthy()?;
        let completion = self.inner.completions.allocate();
        let ticket = self
            .inner
            .queue
            .enqueue(job, completion, self.inner.policy)
            .map_err(CommitRuntimeError::Queue)?;
        if self
            .inner
            .queue
            .signal_drain_needed(DrainSignalReason::Enqueued, DrainInlinePolicy::AllowInline)
        {
            self.drain_inline();
        } else if let Some(wake) = &self.inner.wake_handle {
            wake.signal(DrainSignalReason::Enqueued);
        }
        Ok(ticket)
    }

    pub fn commit_blocking(
        &self,
        job: PreparedCommitJob,
    ) -> Result<CommitRuntimeCommitOutcome, CommitRuntimeError> {
        let ticket = self.submit_commit(job)?;
        self.drain_inline();
        self.inner
            .completions
            .wait(ticket.completion)
            .map_err(CommitRuntimeError::Completion)
    }

    pub fn drain_inline(&self) {
        self.inner.drain_inline(None);
    }

    pub fn drain_inline_with_batch_budget(&self, max_batches: usize) {
        self.inner.drain_inline(Some(max_batches.max(1)));
    }

    pub fn frontier(&self) -> &Arc<CommitFrontier> {
        &self.inner.frontier
    }

    pub fn queue(&self) -> &Arc<CommitQueue> {
        &self.inner.queue
    }

    pub fn finalize_stage(&self) -> &CommitFinalizeStage {
        &self.inner.finalize_stage
    }

    pub fn poison_snapshot(&self) -> Option<CommitRuntimePoison> {
        self.inner.poison.lock().clone()
    }

    pub fn is_admission_open(&self) -> bool {
        self.inner.admission_open.load(Ordering::Acquire)
    }

    pub fn recovery_replay<I>(
        &self,
        events: I,
    ) -> Result<RecoveryReplaySummary, RecoveryReplayError>
    where
        I: IntoIterator<Item = RecoveryReplayEvent>,
    {
        self.inner.recovery_replay(events)
    }

    pub fn snapshot(&self) -> CommitRuntimeSnapshot {
        CommitRuntimeSnapshot {
            admission_open: self.is_admission_open(),
            poison: self.poison_snapshot(),
            frontier: self.inner.frontier.snapshot(),
            queue: self.inner.queue.snapshot(),
            wake_pool: self
                .inner
                .wake_handle
                .as_ref()
                .and_then(CommitDrainWakeHandle::metrics),
            finalize_queue_depth: self.inner.finalize_stage.queue_depth(),
            registered_commit_ts: self.inner.finalize_stage.registered_commit_ts(),
        }
    }
}

impl CommitRuntimeInner {
    fn ensure_healthy(&self) -> Result<(), CommitRuntimeError> {
        if !self.admission_open.load(Ordering::Acquire) {
            return Err(CommitRuntimeError::AdmissionClosed);
        }
        if let Some(poison) = self.poison.lock().clone() {
            return Err(CommitRuntimeError::Poisoned(poison));
        }
        Ok(())
    }

    fn recovery_replay<I>(&self, events: I) -> Result<RecoveryReplaySummary, RecoveryReplayError>
    where
        I: IntoIterator<Item = RecoveryReplayEvent>,
    {
        if let Some(poison) = self.poison.lock().clone() {
            return Err(RecoveryReplayError::RuntimePoisoned(poison));
        }
        self.admission_open.store(false, Ordering::Release);

        let mut summary = RecoveryReplaySummary {
            max_commit_ts_seen: self.frontier.durable_commit_id(),
            ..RecoveryReplaySummary::default()
        };
        for event in events {
            if let Some(poison) = self.poison.lock().clone() {
                return Err(RecoveryReplayError::RuntimePoisoned(poison));
            }
            match event {
                RecoveryReplayEvent::Placeholder { lsn, record_kind } => {
                    if let Err(err) = self
                        .apply_runtime
                        .advance_dispatch_past_placeholder(lsn, record_kind)
                    {
                        let error = RecoveryReplayError::Placeholder {
                            lsn,
                            record_kind,
                            message: Arc::from(err.to_string()),
                        };
                        self.poison(CommitRuntimePoison::Recovery {
                            message: Arc::from(error.to_string()),
                        });
                        return Err(error);
                    }
                    summary.placeholders = summary.placeholders.saturating_add(1);
                    summary.max_lsn_seen = summary.max_lsn_seen.max(lsn);
                }
                RecoveryReplayEvent::Commit(commit) => {
                    let handle = commit.handle;
                    let commit_ts = handle.commit_ts();
                    let durable_lsn = handle.durable_lsn();
                    if commit_ts <= summary.max_commit_ts_seen {
                        let error = RecoveryReplayError::CommitOrder {
                            previous_commit_ts: summary.max_commit_ts_seen,
                            current_commit_ts: commit_ts,
                        };
                        self.poison(CommitRuntimePoison::Recovery {
                            message: Arc::from(error.to_string()),
                        });
                        return Err(error);
                    }
                    let next_dispatch_lsn = self.apply_runtime.next_dispatch_lsn();
                    if durable_lsn != next_dispatch_lsn {
                        let error = RecoveryReplayError::CommitLsnGap {
                            commit_ts,
                            durable_lsn,
                            next_dispatch_lsn,
                        };
                        self.poison(CommitRuntimePoison::Recovery {
                            message: Arc::from(error.to_string()),
                        });
                        return Err(error);
                    }

                    self.frontier.mark_durable(&handle);
                    let request = catch_unwind(AssertUnwindSafe(|| {
                        (commit.required_publish.build_apply_request)(handle.clone())
                    }))
                    .map_err(|panic| RecoveryReplayError::BuildRequest {
                        commit_ts,
                        durable_lsn,
                        message: panic_message(panic),
                    });
                    let request = match request {
                        Ok(request) => request,
                        Err(error) => {
                            self.frontier.mark_publish_failed(
                                commit_ts,
                                PublishFailureCause::phase1_with_diagnostics(
                                    durable_lsn,
                                    0,
                                    error.to_string(),
                                ),
                            );
                            self.poison(CommitRuntimePoison::Recovery {
                                message: Arc::from(error.to_string()),
                            });
                            return Err(error);
                        }
                    };

                    if let Err(err) = self.apply_runtime.submit_observed(request) {
                        let error = RecoveryReplayError::Apply {
                            commit_ts,
                            durable_lsn,
                            message: Arc::from(err.to_string()),
                        };
                        self.frontier.mark_publish_failed(
                            commit_ts,
                            PublishFailureCause::apply_with_diagnostics(
                                durable_lsn,
                                0,
                                error.to_string(),
                            ),
                        );
                        self.poison(CommitRuntimePoison::Recovery {
                            message: Arc::from(error.to_string()),
                        });
                        return Err(error);
                    }

                    summary.commits = summary.commits.saturating_add(1);
                    summary.max_lsn_seen = summary.max_lsn_seen.max(durable_lsn);
                    summary.max_commit_ts_seen = commit_ts;
                }
            }
        }

        let frontier = self.frontier.snapshot();
        if frontier.published_commit_id != frontier.durable_commit_id {
            let error = RecoveryReplayError::IncompleteFrontier {
                durable_commit_id: frontier.durable_commit_id,
                published_commit_id: frontier.published_commit_id,
            };
            self.poison(CommitRuntimePoison::Recovery {
                message: Arc::from(error.to_string()),
            });
            return Err(error);
        }

        self.sequencer
            .sync_next_commit_ts_with(summary.max_commit_ts_seen);
        self.finalize_stage
            .mark_recovered_through(summary.max_commit_ts_seen);
        self.admission_open.store(true, Ordering::Release);
        Ok(summary)
    }

    fn drain_inline(&self, max_batches: Option<usize>) {
        let Some(owner) = self.queue.try_acquire_drain_owner() else {
            return;
        };
        self.drain_owned(owner, max_batches);
    }

    fn drain_owned(&self, owner: super::CommitDrainOwner, max_batches: Option<usize>) {
        let mut owner = Some(owner);
        let mut scheduled_batches = 0_usize;
        let turn_started_at = Instant::now();
        while let Some(active_owner) = owner.take() {
            if self.ensure_healthy().is_err() {
                active_owner.release();
                return;
            }

            if let Some(entries) = active_owner
                .take_ready_pending_fence(self.finalize_stage.registered_commit_ts(), self.policy)
            {
                scheduled_batches =
                    scheduled_batches.saturating_add(self.process_entries(&active_owner, entries));
                if self.turn_batch_budget_exhausted(scheduled_batches, max_batches) {
                    active_owner.release();
                    self.signal_drain_continuation();
                    return;
                }
                owner = Some(active_owner);
                continue;
            }

            let local = active_owner.take_local_buffer(self.policy.effective_target_batch_size());
            if local.is_empty() {
                active_owner.release();
                return;
            }
            scheduled_batches =
                scheduled_batches.saturating_add(self.process_entries(&active_owner, local));
            if self.turn_batch_budget_exhausted(scheduled_batches, max_batches) {
                active_owner.release();
                self.signal_drain_continuation();
                return;
            }
            let elapsed_us = turn_started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
            if elapsed_us >= self.policy.drain_owner_coalesce_budget_us
                && self.queue.has_ready_work()
            {
                active_owner.release();
                self.signal_drain_continuation();
                return;
            }
            owner = Some(active_owner);
        }
    }

    fn turn_batch_budget_exhausted(
        &self,
        scheduled_batches: usize,
        max_batches: Option<usize>,
    ) -> bool {
        max_batches.is_some_and(|max_batches| scheduled_batches >= max_batches)
            && self.queue.has_ready_work()
    }

    fn signal_drain_continuation(&self) {
        if let Some(wake) = &self.wake_handle {
            wake.signal(DrainSignalReason::DeferredEntries);
        } else if self.queue.signal_drain_needed(
            DrainSignalReason::DeferredEntries,
            DrainInlinePolicy::AllowInline,
        ) {
            self.drain_inline(None);
        }
    }

    fn process_entries(
        &self,
        owner: &super::CommitDrainOwner,
        entries: Vec<CommitQueueEntry>,
    ) -> usize {
        let mut scheduled_batches = 0_usize;
        let mut local: std::collections::VecDeque<_> = entries.into();
        while !local.is_empty() {
            let frontier = self.frontier.snapshot();
            let input = CommitDrainBackpressureInput {
                frontier: &frontier,
                commit_finalize_queue_depth: self.finalize_stage.queue_depth(),
                pending_fence_retained_bytes: self.queue.pending_fence_retained_bytes(),
                cleanup: (self.cleanup_snapshot)(),
            };
            if let Err(error) = self.policy.check_drain(&input) {
                let entry = local.pop_front().expect("local not empty");
                self.completions.mark_rejected(
                    entry.completion,
                    CommitRuntimeRejection::DrainBackpressure(error),
                );
                continue;
            }

            let Some(front) = local.pop_front() else {
                break;
            };
            if self.should_block_on_registration(&front) {
                let required = self.predecessor_commit_ts();
                owner.block_on_fence(required, vec![front], self.policy);
                if !owner.can_bypass_snapshot(self.policy, Instant::now()) {
                    owner.defer_entries(local.into());
                    return scheduled_batches;
                }
                continue;
            }

            if front.is_snapshot_only() && self.queue.has_pending_fence() {
                if owner.snapshot_bypass_conflicts_with_pending_head(&front) {
                    let mut suffix = Vec::with_capacity(local.len() + 1);
                    suffix.push(front);
                    suffix.extend(local.into_iter());
                    owner.defer_entries(suffix);
                    return scheduled_batches;
                }
                if !owner.can_bypass_snapshot(self.policy, Instant::now()) {
                    let mut suffix = Vec::with_capacity(local.len() + 1);
                    suffix.push(front);
                    suffix.extend(local.into_iter());
                    owner.defer_entries(suffix);
                    return scheduled_batches;
                }
                owner.record_snapshot_bypass(self.policy);
            }

            let mut batch = vec![front];
            let mut batch_bytes = batch[0].estimated_record_bytes() as u64;
            while batch.len() < self.policy.effective_target_batch_size()
                && batch_bytes < self.policy.effective_target_batch_bytes()
            {
                let Some(candidate) = local.pop_front() else {
                    break;
                };
                if self.should_block_on_registration(&candidate) {
                    owner.block_on_fence(
                        self.predecessor_commit_ts(),
                        vec![candidate],
                        self.policy,
                    );
                    break;
                }
                if candidate.is_snapshot_only() && self.queue.has_pending_fence() {
                    if owner.snapshot_bypass_conflicts_with_pending_head(&candidate) {
                        local.push_front(candidate);
                        break;
                    }
                    if !owner.can_bypass_snapshot(self.policy, Instant::now()) {
                        local.push_front(candidate);
                        break;
                    }
                    owner.record_snapshot_bypass(self.policy);
                }
                batch_bytes = batch_bytes.saturating_add(candidate.estimated_record_bytes() as u64);
                batch.push(candidate);
            }

            self.sequence_append_schedule(batch);
            scheduled_batches = scheduled_batches.saturating_add(1);
        }
        if !local.is_empty() {
            owner.defer_entries(local.into());
        }
        scheduled_batches
    }

    fn should_block_on_registration(&self, entry: &CommitQueueEntry) -> bool {
        if entry.job.sequencing_plan.plan.isolation != IsolationLevel::Serializable {
            return false;
        }
        let predecessor = self.predecessor_commit_ts();
        predecessor.into_raw() > 0
            && self.finalize_stage.registered_commit_ts().into_raw() < predecessor.into_raw()
    }

    fn predecessor_commit_ts(&self) -> CommitTs {
        CommitTs::new(self.sequencer.next_commit_ts().into_raw().saturating_sub(1))
    }

    fn sequence_append_schedule(&self, entries: Vec<CommitQueueEntry>) {
        let ordered = entries
            .into_iter()
            .map(|entry| OrderedCommitPlan {
                sequencing_plan: entry.job.sequencing_plan.clone(),
                payload: entry,
            })
            .collect::<Vec<_>>();
        let reservation_factory = Arc::clone(&self.reservation_factory);
        let final_fence = Arc::clone(&self.final_fence);
        let result = self.sequencer.sequence_ordered_batch(
            ordered,
            |plan, in_flight| final_fence(plan, in_flight),
            move |commit_ts, ordered| {
                let completion = ordered.payload.completion;
                let reservation =
                    reservation_factory(commit_ts, &ordered.payload.job.reservation_input);
                ordered
                    .payload
                    .job
                    .into_sequenced(commit_ts, reservation, completion)
            },
            |mut accepted: Vec<SequencedCommitJob>| {
                if accepted.is_empty() {
                    return Err(CommitSequencerAppendError::append_failed(
                        AppendCommitError::AppendFailed {
                            inner: paro_common::error::internal("empty commit runtime append"),
                        },
                        accepted,
                    ));
                }
                match append_durable_commit_batch(self.journal.as_ref(), &mut accepted) {
                    Ok(batch) => Ok(batch),
                    Err(error @ AppendCommitError::AppendFailed { .. }) => {
                        Err(CommitSequencerAppendError::append_failed(error, accepted))
                    }
                    Err(error @ AppendCommitError::DurableProtocolViolation { .. }) => Err(
                        CommitSequencerAppendError::durable_committed(error, accepted),
                    ),
                }
            },
        );

        match result {
            Ok(batch) => self.handle_append_success(batch),
            Err(error) => self.handle_sequence_or_append_error(error),
        }
    }

    fn handle_append_success(
        &self,
        batch: CommitSequencerOrderedBatch<CommitAppendBatch, CommitQueueEntry>,
    ) {
        for rejected in batch.rejected {
            self.completions.mark_rejected(
                rejected.payload.completion,
                CommitRuntimeRejection::Fence(rejected.reason),
            );
        }
        let Some(append_output) = batch.append_output else {
            return;
        };
        for (handle, job) in append_output
            .handles
            .iter()
            .cloned()
            .zip(append_output.durable_jobs.iter())
        {
            self.frontier.mark_durable(&handle);
            self.completions
                .mark_durable(job.completion, handle.clone(), job.ack_policy);
        }
        let schedule_failure_slots = append_output
            .durable_jobs
            .iter()
            .map(|job| job.completion)
            .collect::<Vec<_>>();
        if let Err(error) = self
            .finalize_stage
            .schedule(append_output.durable_jobs, Arc::clone(&append_output.batch))
        {
            for completion in schedule_failure_slots {
                self.completions.mark_ambiguous(
                    completion,
                    CommitRuntimeFailure::Ambiguous(Arc::from(error.to_string())),
                );
            }
            self.poison_from_finalize_schedule(error);
        }
    }

    fn handle_sequence_or_append_error(
        &self,
        error: CommitSequencerOrderedError<AppendCommitError, SequencedCommitJob, CommitQueueEntry>,
    ) {
        match error {
            CommitSequencerOrderedError::Append {
                error,
                durable_committed,
                accepted,
                rejected,
                ..
            } => {
                for rejected in rejected {
                    self.completions.mark_rejected(
                        rejected.payload.completion,
                        CommitRuntimeRejection::Fence(rejected.reason),
                    );
                }
                if durable_committed {
                    self.poison(CommitRuntimePoison::DurableProtocol {
                        message: Arc::from(error.to_string()),
                    });
                    for job in accepted {
                        match job.cleanup_after_durable_ambiguous() {
                            Ok(bundle) => {
                                self.cleanup_durable_ambiguous(bundle, error.clone());
                            }
                            Err(err) => {
                                self.poison(CommitRuntimePoison::AppendCleanup {
                                    message: Arc::from(err.to_string()),
                                });
                            }
                        }
                    }
                } else {
                    for job in accepted {
                        match job.cleanup_after_append_failure() {
                            Ok(bundle) => self.cleanup_append_failure(bundle, error.clone()),
                            Err(err) => self.poison(CommitRuntimePoison::AppendCleanup {
                                message: Arc::from(err.to_string()),
                            }),
                        }
                    }
                }
            }
        }
    }

    fn cleanup_append_failure(&self, bundle: AppendFailureCleanupBundle, error: AppendCommitError) {
        bundle.reservation.release();
        let rollback = bundle.append_failure_rollback_plan.apply();
        bundle.lock_release_plan.apply();
        bundle.pre_publish_release_plan.apply();
        match rollback {
            Ok(()) => self
                .completions
                .mark_failed_handle(bundle.completion, CommitRuntimeFailure::Append(error)),
            Err(err) => {
                self.completions.mark_failed_handle(
                    bundle.completion,
                    CommitRuntimeFailure::AppendCleanup(Arc::from(err.to_string())),
                );
                self.poison(CommitRuntimePoison::AppendCleanup {
                    message: Arc::from(err.to_string()),
                });
            }
        }
    }

    fn cleanup_durable_ambiguous(
        &self,
        bundle: DurableAmbiguousCleanupBundle,
        error: AppendCommitError,
    ) {
        bundle.reservation.release();
        bundle.lock_release_plan.apply();
        bundle.pre_publish_release_plan.apply();
        self.completions
            .mark_ambiguous(bundle.completion, CommitRuntimeFailure::Append(error));
    }

    fn poison_from_finalize_schedule(&self, error: CommitFinalizeStageScheduleError) {
        self.poison(CommitRuntimePoison::FinalizeSchedule {
            message: Arc::from(error.to_string()),
        });
    }

    fn poison(&self, poison: CommitRuntimePoison) {
        self.admission_open.store(false, Ordering::Release);
        let (first_poison, effective_poison) = {
            let mut guard = self.poison.lock();
            match guard.clone() {
                Some(existing) => (None, existing),
                None => {
                    *guard = Some(poison.clone());
                    (Some(poison.clone()), poison)
                }
            }
        };
        if let (Some(sink), Some(poison)) = (&self.health_sink, first_poison) {
            sink(poison);
        }
        let pending = self.queue.close_and_drain_pending();
        let rejection =
            CommitRuntimeRejection::RuntimePoisoned(Arc::from(effective_poison.to_string()));
        for completion in pending {
            self.completions
                .mark_rejected(completion, rejection.clone());
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRuntimeCommitOutcome {
    pub commit_ts: CommitTs,
    pub handle: DurableCommitHandle,
    pub ack: CommitRuntimeAck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitRuntimeAck {
    DurableOnly,
    Published,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRuntimeSnapshot {
    pub admission_open: bool,
    pub poison: Option<CommitRuntimePoison>,
    pub frontier: CommitFrontierSnapshot,
    pub queue: CommitQueueSnapshot,
    pub wake_pool: Option<CommitDrainWakePoolMetrics>,
    pub finalize_queue_depth: usize,
    pub registered_commit_ts: CommitTs,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        AppendFailureRollbackPlan, CommitAckPolicy, CommitFinalizeReservation, LockReleasePlan,
        PrePublishReleasePlan, RecoveryPlaceholderRecordKind, RecoveryReplayCommit,
        RecoveryReplayError, RecoveryReplayEvent, RequiredPublishPlan,
    };
    use crate::{
        CommitDurableBatch, CommitRequest, CommitSequencingPlan, DatabaseId, FrozenLockSet,
        TransactionView, TxnId,
    };
    use paro_common::durability::PreparedCommitPlan;
    use paro_common::journal::JournalRecord;
    use paro_journal::{AppendResult, ApplyRequest, TabletApplyPart, WaitMode};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingCommitJournal {
        appended: Mutex<Vec<Vec<JournalRecord>>>,
    }

    impl CommitJournal for RecordingCommitJournal {
        fn append_records(
            &self,
            records: &[JournalRecord],
        ) -> paro_common::error::Result<Vec<AppendResult>> {
            self.appended.lock().unwrap().push(records.to_vec());
            Ok((0..records.len())
                .map(|offset| AppendResult {
                    lsn: 1 + offset as u64,
                    durable_batch_lsn: records.len() as u64,
                    durable_batch_size: records.len() as u64,
                    durable_batch_bytes: 1024,
                    sync_latency_micros: 10,
                })
                .collect())
        }
    }

    fn empty_plan(txn_id: u64) -> PreparedCommitPlan {
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

    fn prepared_job(
        txn_id: u64,
        ack_policy: CommitAckPolicy,
        required_publish: RequiredPublishPlan,
    ) -> PreparedCommitJob {
        let request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(txn_id),
            TransactionView::autocommit(crate::ReadTs::new(1)),
            ack_policy,
            FrozenLockSet::empty(),
            Vec::new(),
        );
        PreparedCommitJob {
            sequencing_plan: CommitSequencingPlan::from_commit_plan(request.commit_plan()),
            durable_plan: empty_plan(txn_id),
            reservation_input: request.commit_plan().into(),
            lock_release_plan: LockReleasePlan::noop(),
            pre_publish_release_plan: PrePublishReleasePlan::noop(),
            append_failure_rollback_plan: AppendFailureRollbackPlan::noop(),
            required_publish,
            deferred_publish: Vec::new(),
            ack_policy,
            estimated_record_bytes: 64,
            retained_bytes: 128,
            created_at: Instant::now(),
        }
    }

    fn publish_plan(frontier: Arc<CommitFrontier>) -> RequiredPublishPlan {
        RequiredPublishPlan::new(
            Box::new(move |handle| {
                let frontier = Arc::clone(&frontier);
                ApplyRequest {
                    lsn: handle.durable_lsn(),
                    durable_batch_lsn: handle.durable_batch_lsn(),
                    commit_id: Some(handle.commit_ts().into_raw()),
                    wait_mode: WaitMode::Published,
                    catalog_serial: false,
                    catalog_pre: Box::new(|| Ok(())),
                    tablet_parts: Vec::<TabletApplyPart>::new(),
                    descriptor_phase: Box::new(|| Ok(())),
                    catalog_post: Box::new(|| Ok(())),
                    on_published: Box::new(move || {
                        frontier.mark_published(&handle);
                        Ok(())
                    }),
                }
            }),
            Arc::from([]),
        )
    }

    fn recovery_handle(
        first_lsn: u64,
        commit_ts: CommitTs,
        record_bytes: u32,
    ) -> DurableCommitHandle {
        Arc::new(
            CommitDurableBatch::new(
                first_lsn,
                first_lsn,
                1,
                record_bytes as u64,
                Arc::from([record_bytes]),
                0,
                commit_ts,
                commit_ts,
            )
            .unwrap(),
        )
        .handle_at(0)
        .unwrap()
    }

    #[test]
    fn runtime_durable_only_ack_happens_after_publish_submit() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let mut assembly = CommitRuntimeAssembly::for_tests(journal.clone(), apply_runtime);
        assembly.frontier = Arc::clone(&frontier);
        let runtime = CommitRuntime::new(assembly);

        let outcome = runtime
            .commit_blocking(prepared_job(
                1,
                CommitAckPolicy::DurableOnlyAsync,
                publish_plan(Arc::clone(&frontier)),
            ))
            .unwrap();

        assert_eq!(outcome.commit_ts, CommitTs::new(1));
        assert_eq!(outcome.ack, CommitRuntimeAck::DurableOnly);
        assert_eq!(frontier.durable_commit_id(), CommitTs::new(1));
        assert_eq!(journal.appended.lock().unwrap()[0].len(), 1);
        assert_eq!(runtime.inner.completions.slot_count(), 0);
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn runtime_required_published_waits_for_apply_completion() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, apply_runtime);
        assembly.frontier = Arc::clone(&frontier);
        let runtime = CommitRuntime::new(assembly);

        let outcome = runtime
            .commit_blocking(prepared_job(
                1,
                CommitAckPolicy::RequiredPublished,
                publish_plan(Arc::clone(&frontier)),
            ))
            .unwrap();

        assert_eq!(outcome.ack, CommitRuntimeAck::Published);
        assert_eq!(frontier.published_commit_id(), CommitTs::new(1));
        assert_eq!(runtime.inner.completions.slot_count(), 0);
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn runtime_poison_rejects_pending_queue_completions() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, apply_runtime);
        assembly.frontier = Arc::clone(&frontier);
        let runtime = CommitRuntime::new(assembly);
        let completion = runtime.inner.completions.allocate();

        runtime
            .inner
            .queue
            .enqueue(
                prepared_job(
                    1,
                    CommitAckPolicy::RequiredPublished,
                    publish_plan(Arc::clone(&frontier)),
                ),
                completion,
                runtime.inner.policy,
            )
            .unwrap();
        runtime.inner.poison(CommitRuntimePoison::Recovery {
            message: Arc::from("test poison"),
        });

        let err = runtime.inner.completions.wait(completion).unwrap_err();
        assert!(matches!(
            err,
            CommitCompletionError::Rejected(CommitRuntimeRejection::RuntimePoisoned(_))
        ));
        assert_eq!(runtime.inner.queue.snapshot().entries, 0);
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn runtime_reservation_factory_runs_before_lock_release() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let order = Arc::new(AtomicUsize::new(0));
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, apply_runtime);
        assembly.frontier = Arc::clone(&frontier);
        assembly.reservation_factory = {
            let order = Arc::clone(&order);
            Arc::new(move |_, _| {
                CommitFinalizeReservation::new(
                    super::super::WriteConflictReservation { slot_id: 1 },
                    super::super::SummaryReservation { slot_id: 1 },
                    {
                        let order = Arc::clone(&order);
                        move || {
                            assert_eq!(order.fetch_add(1, Ordering::AcqRel), 0);
                        }
                    },
                    || {},
                )
            })
        };
        let runtime = CommitRuntime::new(assembly);
        let request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(1),
            TransactionView::autocommit(crate::ReadTs::new(1)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );
        let job = PreparedCommitJob {
            sequencing_plan: CommitSequencingPlan::from_commit_plan(request.commit_plan()),
            durable_plan: empty_plan(1),
            reservation_input: request.commit_plan().into(),
            lock_release_plan: LockReleasePlan::new({
                let order = Arc::clone(&order);
                move || {
                    assert_eq!(order.fetch_add(1, Ordering::AcqRel), 1);
                }
            }),
            pre_publish_release_plan: PrePublishReleasePlan::noop(),
            append_failure_rollback_plan: AppendFailureRollbackPlan::noop(),
            required_publish: publish_plan(Arc::clone(&frontier)),
            deferred_publish: Vec::new(),
            ack_policy: CommitAckPolicy::RequiredPublished,
            estimated_record_bytes: 64,
            retained_bytes: 128,
            created_at: Instant::now(),
        };

        runtime.commit_blocking(job).unwrap();
        assert_eq!(order.load(Ordering::Acquire), 2);
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn recovery_replay_requires_ordered_placeholder_before_commit_lsn_gap() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, Arc::clone(&apply_runtime));
        assembly.frontier = Arc::clone(&frontier);
        let runtime = CommitRuntime::new(assembly);

        let err = runtime
            .recovery_replay(vec![RecoveryReplayEvent::Commit(
                RecoveryReplayCommit::new(
                    recovery_handle(2, CommitTs::new(1), 64),
                    publish_plan(Arc::clone(&frontier)),
                ),
            )])
            .expect_err("commit-only recovery stream must not skip non-commit WAL lsn");

        assert!(matches!(
            err,
            RecoveryReplayError::CommitLsnGap {
                durable_lsn: 2,
                next_dispatch_lsn: 1,
                ..
            }
        ));
        assert!(!runtime.is_admission_open());
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn recovery_replay_interleaves_placeholder_and_commit_events() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, Arc::clone(&apply_runtime));
        assembly.frontier = Arc::clone(&frontier);
        let runtime = CommitRuntime::new(assembly);

        let summary = runtime
            .recovery_replay(vec![
                RecoveryReplayEvent::Placeholder {
                    lsn: 1,
                    record_kind: RecoveryPlaceholderRecordKind::Maintenance,
                },
                RecoveryReplayEvent::Commit(RecoveryReplayCommit::new(
                    recovery_handle(2, CommitTs::new(1), 64),
                    publish_plan(Arc::clone(&frontier)),
                )),
                RecoveryReplayEvent::Placeholder {
                    lsn: 3,
                    record_kind: RecoveryPlaceholderRecordKind::CheckpointFence,
                },
                RecoveryReplayEvent::Commit(RecoveryReplayCommit::new(
                    recovery_handle(4, CommitTs::new(2), 80),
                    publish_plan(Arc::clone(&frontier)),
                )),
            ])
            .unwrap();

        assert_eq!(summary.commits, 2);
        assert_eq!(summary.placeholders, 2);
        assert_eq!(summary.max_lsn_seen, 4);
        assert_eq!(summary.max_commit_ts_seen, CommitTs::new(2));
        assert_eq!(frontier.durable_commit_id(), CommitTs::new(2));
        assert_eq!(frontier.published_commit_id(), CommitTs::new(2));
        assert_eq!(apply_runtime.frontiers().published_lsn, 4);
        assert_eq!(runtime.inner.sequencer.next_commit_ts(), CommitTs::new(3));
        assert_eq!(
            runtime.finalize_stage().registered_commit_ts(),
            CommitTs::new(2)
        );
        assert!(runtime.is_admission_open());
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn runtime_initializes_registration_gate_from_published_frontier() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        frontier.sync_commit_ids(CommitTs::new(5), CommitTs::new(5));
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, apply_runtime);
        assembly.frontier = frontier;

        let runtime = CommitRuntime::new(assembly);

        assert_eq!(
            runtime.finalize_stage().registered_commit_ts(),
            CommitTs::new(5)
        );
        runtime.finalize_stage().force_shutdown();
    }

    #[test]
    fn finalize_stage_failure_poisons_runtime_and_wakes_durable_waiter() {
        let journal = Arc::new(RecordingCommitJournal::default());
        let apply_runtime = Arc::new(JournalApplyRuntime::new());
        let frontier = Arc::new(CommitFrontier::new());
        let mut assembly = CommitRuntimeAssembly::for_tests(journal, apply_runtime);
        assembly.frontier = Arc::clone(&frontier);
        let runtime = CommitRuntime::new(assembly);

        let err = runtime
            .commit_blocking(prepared_job(
                1,
                CommitAckPolicy::RequiredPublished,
                RequiredPublishPlan::new(
                    Box::new(|_| panic!("build request failed")),
                    Arc::from([]),
                ),
            ))
            .expect_err("durable commit with finalize failure must not leave waiter parked");

        assert!(matches!(
            err,
            CommitRuntimeError::Completion(CommitCompletionError::AmbiguousCommitted(_))
        ));
        assert!(matches!(
            runtime.poison_snapshot(),
            Some(CommitRuntimePoison::FinalizeStage {
                commit_ts: Some(commit_ts),
                ..
            }) if commit_ts == CommitTs::new(1)
        ));
        assert_eq!(frontier.publish_failure_watermark(), Some(CommitTs::new(1)));
        assert!(!runtime.is_admission_open());
        runtime.finalize_stage().force_shutdown();
    }
}
