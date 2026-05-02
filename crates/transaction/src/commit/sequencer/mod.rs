// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered commit timestamp assignment and in-batch fencing.

mod backpressure;

use super::{
    CommitPlan, FrozenLockSet, IsolationLevel, DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE,
    DEFAULT_MAX_GROUP_COMMIT_FENCE_US,
};
use crate::sync::Mutex;
use crate::types::{CommitTs, ReadTs, TxnId};
use crate::{LockMode, LockRequest, LockResource};
use std::time::Instant;

pub use backpressure::{
    CommitBackpressureController, CommitBackpressureError, CommitBackpressureOptions,
    CommitBackpressureSnapshot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitSequencerOptions {
    pub max_group_commit_batch_size: usize,
    pub max_group_commit_fence_us: u64,
    pub adaptive_batch_sizing: bool,
    pub parallel_fence_groups: bool,
}

impl Default for CommitSequencerOptions {
    fn default() -> Self {
        Self {
            max_group_commit_batch_size: DEFAULT_MAX_GROUP_COMMIT_BATCH_SIZE,
            max_group_commit_fence_us: DEFAULT_MAX_GROUP_COMMIT_FENCE_US,
            adaptive_batch_sizing: false,
            parallel_fence_groups: false,
        }
    }
}

impl CommitSequencerOptions {
    #[inline]
    fn effective_batch_size(self) -> usize {
        self.max_group_commit_batch_size.max(1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSequencingPlan {
    pub plan: CommitPlan,
    pub write_set: Vec<LockResource>,
    pub validation_epoch: u64,
    pub ssi_effect_epoch: u64,
    pub estimated_bytes: usize,
}

impl CommitSequencingPlan {
    #[inline]
    pub fn new(plan: CommitPlan, write_set: Vec<LockResource>) -> Self {
        Self {
            plan,
            write_set,
            validation_epoch: 0,
            ssi_effect_epoch: 0,
            estimated_bytes: 0,
        }
    }

    #[inline]
    pub fn from_commit_plan(plan: CommitPlan) -> Self {
        let write_set = write_set_from_lock_set(&plan.lock_set);
        Self::new(plan, write_set)
    }

    #[inline]
    pub const fn with_validation_epoch(mut self, validation_epoch: u64) -> Self {
        self.validation_epoch = validation_epoch;
        self
    }

    #[inline]
    pub const fn with_ssi_effect_epoch(mut self, ssi_effect_epoch: u64) -> Self {
        self.ssi_effect_epoch = ssi_effect_epoch;
        self
    }

    #[inline]
    pub const fn with_estimated_bytes(mut self, estimated_bytes: usize) -> Self {
        self.estimated_bytes = estimated_bytes;
        self
    }
}

fn write_set_from_lock_set(lock_set: &FrozenLockSet) -> Vec<LockResource> {
    lock_set
        .locks()
        .iter()
        .filter(|request| request.mode.is_write_intent())
        .filter(|request| !is_shadowed_table_intent(lock_set, request))
        .map(|request| request.resource.clone())
        .collect()
}

fn is_shadowed_table_intent(lock_set: &FrozenLockSet, request: &LockRequest) -> bool {
    if request.mode != LockMode::IX {
        return false;
    }
    let LockResource::Table {
        namespace,
        table_id,
    } = request.resource
    else {
        return false;
    };

    lock_set.locks().iter().any(|other| {
        other.mode.is_write_intent()
            && !matches!(other.resource, LockResource::Table { .. })
            && other.resource.namespace() == namespace
            && other.resource.table_id() == Some(table_id)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFenceRejectReason {
    BatchSizeLimit,
    FenceBudgetExceeded {
        elapsed_us: u64,
        limit_us: u64,
    },
    InBatchWriteConflict,
    SsiEpochAdvanced {
        validation_epoch: u64,
        batch_effect_epoch: u64,
    },
    SsiStateEpochAdvanced {
        validation_epoch: u64,
        current_epoch: u64,
    },
    CommitTimestampExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedCommitPlan {
    pub plan: CommitSequencingPlan,
    pub reason: CommitFenceRejectReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightAcceptedPlan {
    pub txn_id: TxnId,
    pub read_ts: ReadTs,
    pub commit_ts: CommitTs,
    pub write_set: Vec<LockResource>,
    pub ssi_effect_epoch: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InFlightCommitBatch {
    accepted: Vec<InFlightAcceptedPlan>,
    write_set: Vec<LockResource>,
    max_ssi_effect_epoch: u64,
}

impl InFlightCommitBatch {
    #[inline]
    pub fn accepted(&self) -> &[InFlightAcceptedPlan] {
        &self.accepted
    }

    #[inline]
    pub fn write_set(&self) -> &[LockResource] {
        &self.write_set
    }

    #[inline]
    pub const fn max_ssi_effect_epoch(&self) -> u64 {
        self.max_ssi_effect_epoch
    }

    pub fn reject_reason_for(
        &self,
        plan: &CommitSequencingPlan,
    ) -> Option<CommitFenceRejectReason> {
        if plan.plan.isolation == IsolationLevel::Serializable
            && plan.validation_epoch < self.max_ssi_effect_epoch
        {
            return Some(CommitFenceRejectReason::SsiEpochAdvanced {
                validation_epoch: plan.validation_epoch,
                batch_effect_epoch: self.max_ssi_effect_epoch,
            });
        }
        if self.conflicts_with_write_set(&plan.write_set) {
            return Some(CommitFenceRejectReason::InBatchWriteConflict);
        }
        None
    }

    #[inline]
    pub fn conflicts_with_write_set(&self, write_set: &[LockResource]) -> bool {
        write_set.iter().any(|resource| {
            self.write_set
                .iter()
                .any(|seen| seen.conflicts_with(resource))
        })
    }

    fn accept(&mut self, plan: &CommitSequencingPlan, commit_ts: CommitTs) {
        self.write_set.extend(plan.write_set.iter().cloned());
        self.max_ssi_effect_epoch = self.max_ssi_effect_epoch.max(plan.ssi_effect_epoch);
        self.accepted.push(InFlightAcceptedPlan {
            txn_id: plan.plan.txn_id,
            read_ts: plan.plan.read_ts,
            commit_ts,
            write_set: plan.write_set.clone(),
            ssi_effect_epoch: plan.ssi_effect_epoch,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedCommit {
    pub commit_ts: CommitTs,
    pub plan: CommitSequencingPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedCommitBatch {
    pub accepted: Vec<SequencedCommit>,
    pub rejected: Vec<RejectedCommitPlan>,
    pub fence_duration_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedCommitPlan<T> {
    pub sequencing_plan: CommitSequencingPlan,
    pub payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedOrderedCommit<T> {
    pub plan: CommitSequencingPlan,
    pub payload: T,
    pub reason: CommitFenceRejectReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSequencerAppendError<E, T> {
    AppendFailed { error: E, accepted: Vec<T> },
    DurableCommitted { error: E, accepted: Vec<T> },
}

impl<E, T> CommitSequencerAppendError<E, T> {
    #[inline]
    pub fn append_failed(error: E, accepted: Vec<T>) -> Self {
        Self::AppendFailed { error, accepted }
    }

    #[inline]
    pub fn durable_committed(error: E, accepted: Vec<T>) -> Self {
        Self::DurableCommitted { error, accepted }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSequencerOrderedBatch<O, R> {
    pub append_output: Option<O>,
    pub rejected: Vec<RejectedOrderedCommit<R>>,
    pub fence_duration_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSequencerError<E> {
    Append {
        error: E,
        provisional_start: CommitTs,
        provisional_count: usize,
        accepted: Vec<SequencedCommit>,
        rejected: Vec<RejectedCommitPlan>,
        fence_duration_us: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSequencerOrderedError<E, A, R> {
    Append {
        error: E,
        durable_committed: bool,
        provisional_start: CommitTs,
        provisional_count: usize,
        accepted: Vec<A>,
        rejected: Vec<RejectedOrderedCommit<R>>,
        fence_duration_us: u64,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitSequencerMetrics {
    pub batches: u64,
    pub accepted_plans: u64,
    pub rejected_plans: u64,
    pub append_failures: u64,
    pub fence_duration_us_total: u64,
    pub fence_duration_us_peak: u64,
    pub reject_batch_size_limit: u64,
    pub reject_fence_budget_exceeded: u64,
    pub reject_in_batch_write_conflict: u64,
    pub reject_ssi_epoch_advanced: u64,
    pub reject_commit_timestamp_exhausted: u64,
}

impl CommitSequencerMetrics {
    fn observe_batch(&mut self, accepted: usize, rejected: &[RejectedCommitPlan], fence_us: u64) {
        self.batches = self.batches.saturating_add(1);
        self.accepted_plans = self.accepted_plans.saturating_add(accepted as u64);
        self.rejected_plans = self.rejected_plans.saturating_add(rejected.len() as u64);
        self.fence_duration_us_total = self.fence_duration_us_total.saturating_add(fence_us);
        self.fence_duration_us_peak = self.fence_duration_us_peak.max(fence_us);
        for rejected in rejected {
            self.observe_reject(rejected.reason);
        }
    }

    fn observe_ordered_batch<T>(
        &mut self,
        accepted: usize,
        rejected: &[RejectedOrderedCommit<T>],
        fence_us: u64,
    ) {
        self.batches = self.batches.saturating_add(1);
        self.accepted_plans = self.accepted_plans.saturating_add(accepted as u64);
        self.rejected_plans = self.rejected_plans.saturating_add(rejected.len() as u64);
        self.fence_duration_us_total = self.fence_duration_us_total.saturating_add(fence_us);
        self.fence_duration_us_peak = self.fence_duration_us_peak.max(fence_us);
        for rejected in rejected {
            self.observe_reject(rejected.reason);
        }
    }

    fn observe_reject(&mut self, reason: CommitFenceRejectReason) {
        match reason {
            CommitFenceRejectReason::BatchSizeLimit => {
                self.reject_batch_size_limit = self.reject_batch_size_limit.saturating_add(1)
            }
            CommitFenceRejectReason::FenceBudgetExceeded { .. } => {
                self.reject_fence_budget_exceeded =
                    self.reject_fence_budget_exceeded.saturating_add(1)
            }
            CommitFenceRejectReason::InBatchWriteConflict => {
                self.reject_in_batch_write_conflict =
                    self.reject_in_batch_write_conflict.saturating_add(1)
            }
            CommitFenceRejectReason::SsiEpochAdvanced { .. }
            | CommitFenceRejectReason::SsiStateEpochAdvanced { .. } => {
                self.reject_ssi_epoch_advanced = self.reject_ssi_epoch_advanced.saturating_add(1)
            }
            CommitFenceRejectReason::CommitTimestampExhausted => {
                self.reject_commit_timestamp_exhausted =
                    self.reject_commit_timestamp_exhausted.saturating_add(1)
            }
        }
    }
}

#[derive(Debug)]
pub struct CommitSequencer {
    options: CommitSequencerOptions,
    state: Mutex<CommitSequencerState>,
}

#[derive(Debug)]
struct CommitSequencerState {
    next_commit_ts: CommitTs,
    metrics: CommitSequencerMetrics,
}

impl CommitSequencer {
    pub fn new(next_commit_ts: CommitTs, options: CommitSequencerOptions) -> Self {
        Self {
            options,
            state: Mutex::new(CommitSequencerState {
                next_commit_ts,
                metrics: CommitSequencerMetrics::default(),
            }),
        }
    }

    #[inline]
    pub fn with_next_commit_ts(next_commit_ts: CommitTs) -> Self {
        Self::new(next_commit_ts, CommitSequencerOptions::default())
    }

    #[inline]
    pub fn next_commit_ts(&self) -> CommitTs {
        self.state.lock().next_commit_ts
    }

    #[inline]
    pub fn metrics_snapshot(&self) -> CommitSequencerMetrics {
        self.state.lock().metrics
    }

    pub fn sync_next_commit_ts_with(&self, min_committed_version: CommitTs) {
        let mut state = self.state.lock();
        if let Some(next) = commit_ts_at(min_committed_version, 1) {
            state.next_commit_ts = state.next_commit_ts.max(next);
        } else {
            state.next_commit_ts = CommitTs::new(u64::MAX);
        }
    }

    pub fn sequence_batch<E>(
        &self,
        plans: impl IntoIterator<Item = CommitSequencingPlan>,
        append: impl FnOnce(&[SequencedCommit]) -> std::result::Result<(), E>,
    ) -> std::result::Result<SequencedCommitBatch, CommitSequencerError<E>> {
        self.sequence_batch_with_fence(plans, |_, _| None, append)
    }

    pub fn sequence_batch_with_fence<E>(
        &self,
        plans: impl IntoIterator<Item = CommitSequencingPlan>,
        mut final_fence: impl FnMut(
            &CommitSequencingPlan,
            &InFlightCommitBatch,
        ) -> Option<CommitFenceRejectReason>,
        append: impl FnOnce(&[SequencedCommit]) -> std::result::Result<(), E>,
    ) -> std::result::Result<SequencedCommitBatch, CommitSequencerError<E>> {
        let mut state = self.state.lock();
        let base_commit_ts = state.next_commit_ts;
        let fence_started_at = Instant::now();
        let mut in_flight = InFlightCommitBatch::default();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let max_batch = self.options.effective_batch_size();

        for plan in plans {
            if accepted.len() >= max_batch {
                rejected.push(RejectedCommitPlan {
                    plan,
                    reason: CommitFenceRejectReason::BatchSizeLimit,
                });
                continue;
            }

            let elapsed_us = elapsed_us_since(fence_started_at);
            if elapsed_us >= self.options.max_group_commit_fence_us {
                rejected.push(RejectedCommitPlan {
                    plan,
                    reason: CommitFenceRejectReason::FenceBudgetExceeded {
                        elapsed_us,
                        limit_us: self.options.max_group_commit_fence_us,
                    },
                });
                continue;
            }

            if let Some(reason) = in_flight.reject_reason_for(&plan) {
                rejected.push(RejectedCommitPlan { plan, reason });
                continue;
            }

            if let Some(reason) = final_fence(&plan, &in_flight) {
                rejected.push(RejectedCommitPlan { plan, reason });
                continue;
            }

            let Some(commit_ts) = commit_ts_at(base_commit_ts, accepted.len()) else {
                rejected.push(RejectedCommitPlan {
                    plan,
                    reason: CommitFenceRejectReason::CommitTimestampExhausted,
                });
                continue;
            };

            in_flight.accept(&plan, commit_ts);
            accepted.push(SequencedCommit { commit_ts, plan });
        }

        let fence_duration_us = elapsed_us_since(fence_started_at);
        if accepted.is_empty() {
            state
                .metrics
                .observe_batch(accepted.len(), &rejected, fence_duration_us);
            return Ok(SequencedCommitBatch {
                accepted,
                rejected,
                fence_duration_us,
            });
        }

        match append(&accepted) {
            Ok(()) => {
                if let Some(next) = commit_ts_at(base_commit_ts, accepted.len()) {
                    state.next_commit_ts = next;
                } else {
                    state.next_commit_ts = CommitTs::new(u64::MAX);
                }
                state
                    .metrics
                    .observe_batch(accepted.len(), &rejected, fence_duration_us);
                Ok(SequencedCommitBatch {
                    accepted,
                    rejected,
                    fence_duration_us,
                })
            }
            Err(error) => {
                state.metrics.append_failures = state.metrics.append_failures.saturating_add(1);
                state
                    .metrics
                    .observe_batch(accepted.len(), &rejected, fence_duration_us);
                Err(CommitSequencerError::Append {
                    error,
                    provisional_start: base_commit_ts,
                    provisional_count: accepted.len(),
                    accepted,
                    rejected,
                    fence_duration_us,
                })
            }
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn sequence_ordered_batch<R, A, O, E>(
        &self,
        plans: impl IntoIterator<Item = OrderedCommitPlan<R>>,
        mut final_fence: impl FnMut(
            &CommitSequencingPlan,
            &InFlightCommitBatch,
        ) -> Option<CommitFenceRejectReason>,
        mut accept: impl FnMut(CommitTs, OrderedCommitPlan<R>) -> A,
        append: impl FnOnce(Vec<A>) -> std::result::Result<O, CommitSequencerAppendError<E, A>>,
    ) -> std::result::Result<CommitSequencerOrderedBatch<O, R>, CommitSequencerOrderedError<E, A, R>>
    {
        let base_commit_ts = self.state.lock().next_commit_ts;
        let fence_started_at = Instant::now();
        let mut in_flight = InFlightCommitBatch::default();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let max_batch = self.options.effective_batch_size();

        for ordered in plans {
            if accepted.len() >= max_batch {
                rejected.push(RejectedOrderedCommit {
                    plan: ordered.sequencing_plan,
                    payload: ordered.payload,
                    reason: CommitFenceRejectReason::BatchSizeLimit,
                });
                continue;
            }

            let elapsed_us = elapsed_us_since(fence_started_at);
            if elapsed_us >= self.options.max_group_commit_fence_us {
                rejected.push(RejectedOrderedCommit {
                    plan: ordered.sequencing_plan,
                    payload: ordered.payload,
                    reason: CommitFenceRejectReason::FenceBudgetExceeded {
                        elapsed_us,
                        limit_us: self.options.max_group_commit_fence_us,
                    },
                });
                continue;
            }

            if let Some(reason) = in_flight.reject_reason_for(&ordered.sequencing_plan) {
                rejected.push(RejectedOrderedCommit {
                    plan: ordered.sequencing_plan,
                    payload: ordered.payload,
                    reason,
                });
                continue;
            }

            if let Some(reason) = final_fence(&ordered.sequencing_plan, &in_flight) {
                rejected.push(RejectedOrderedCommit {
                    plan: ordered.sequencing_plan,
                    payload: ordered.payload,
                    reason,
                });
                continue;
            }

            let Some(commit_ts) = commit_ts_at(base_commit_ts, accepted.len()) else {
                rejected.push(RejectedOrderedCommit {
                    plan: ordered.sequencing_plan,
                    payload: ordered.payload,
                    reason: CommitFenceRejectReason::CommitTimestampExhausted,
                });
                continue;
            };

            in_flight.accept(&ordered.sequencing_plan, commit_ts);
            accepted.push(accept(commit_ts, ordered));
        }

        let fence_duration_us = elapsed_us_since(fence_started_at);
        if accepted.is_empty() {
            let mut state = self.state.lock();
            state
                .metrics
                .observe_ordered_batch(0, &rejected, fence_duration_us);
            return Ok(CommitSequencerOrderedBatch {
                append_output: None,
                rejected,
                fence_duration_us,
            });
        }

        let provisional_count = accepted.len();
        match append(accepted) {
            Ok(append_output) => {
                let mut state = self.state.lock();
                advance_shadow_clock(&mut state.next_commit_ts, base_commit_ts, provisional_count);
                state.metrics.observe_ordered_batch(
                    provisional_count,
                    &rejected,
                    fence_duration_us,
                );
                Ok(CommitSequencerOrderedBatch {
                    append_output: Some(append_output),
                    rejected,
                    fence_duration_us,
                })
            }
            Err(CommitSequencerAppendError::AppendFailed { error, accepted }) => {
                let mut state = self.state.lock();
                state.metrics.append_failures = state.metrics.append_failures.saturating_add(1);
                state.metrics.observe_ordered_batch(
                    provisional_count,
                    &rejected,
                    fence_duration_us,
                );
                Err(CommitSequencerOrderedError::Append {
                    error,
                    durable_committed: false,
                    provisional_start: base_commit_ts,
                    provisional_count,
                    accepted,
                    rejected,
                    fence_duration_us,
                })
            }
            Err(CommitSequencerAppendError::DurableCommitted { error, accepted }) => {
                let mut state = self.state.lock();
                advance_shadow_clock(&mut state.next_commit_ts, base_commit_ts, provisional_count);
                state.metrics.append_failures = state.metrics.append_failures.saturating_add(1);
                state.metrics.observe_ordered_batch(
                    provisional_count,
                    &rejected,
                    fence_duration_us,
                );
                Err(CommitSequencerOrderedError::Append {
                    error,
                    durable_committed: true,
                    provisional_start: base_commit_ts,
                    provisional_count,
                    accepted,
                    rejected,
                    fence_duration_us,
                })
            }
        }
    }
}

impl Default for CommitSequencer {
    fn default() -> Self {
        Self::with_next_commit_ts(CommitTs::new(1))
    }
}

#[inline]
fn commit_ts_at(base: CommitTs, offset: usize) -> Option<CommitTs> {
    base.into_raw()
        .checked_add(u64::try_from(offset).ok()?)
        .map(CommitTs::new)
}

fn advance_shadow_clock(next_commit_ts: &mut CommitTs, base: CommitTs, count: usize) {
    debug_assert_eq!(
        *next_commit_ts, base,
        "single drain owner must complete the shadow commit range before another batch advances it"
    );
    if let Some(next) = commit_ts_at(base, count) {
        *next_commit_ts = (*next_commit_ts).max(next);
    } else {
        *next_commit_ts = CommitTs::new(u64::MAX);
    }
}

#[inline]
fn elapsed_us_since(started_at: Instant) -> u64 {
    started_at.elapsed().as_micros().min(u64::MAX as u128) as u64
}
