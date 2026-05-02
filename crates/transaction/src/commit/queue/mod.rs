// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit queue synchronization and pending-fence ownership.

mod policy;

use super::{CommitCompletionHandle, IsolationLevel, PreparedCommitJob};
use crate::sync::Mutex;
use crate::types::CommitTs;
pub use policy::{
    CleanupBackpressureSnapshot, CommitBatchPolicy, CommitDrainBackpressure,
    CommitDrainBackpressureInput, CommitQueueBackpressure,
};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainInlinePolicy {
    AllowInline,
    WakePoolOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainSignalReason {
    Enqueued,
    DeferredEntries,
    PendingFenceReady,
    BackpressureReleased,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitQueueTicket {
    pub queue_seq: u64,
    pub completion: CommitCompletionHandle,
}

#[derive(Debug)]
pub struct CommitQueueEntry {
    pub queue_seq: u64,
    pub completion: CommitCompletionHandle,
    pub enqueued_at: Instant,
    pub job: PreparedCommitJob,
}

impl CommitQueueEntry {
    #[inline]
    pub fn retained_bytes(&self) -> u64 {
        self.job.retained_bytes
    }

    #[inline]
    pub fn estimated_record_bytes(&self) -> u32 {
        self.job.estimated_record_bytes
    }

    #[inline]
    pub fn is_serializable(&self) -> bool {
        self.job.sequencing_plan.plan.isolation == IsolationLevel::Serializable
    }

    #[inline]
    pub fn is_snapshot_only(&self) -> bool {
        !self.is_serializable()
    }
}

#[derive(Debug)]
pub struct FenceBlockedBatch {
    pub required_registered_ts: CommitTs,
    pub entries: Vec<CommitQueueEntry>,
    pub original_head_seq: u64,
    pub enqueued_at: Instant,
    pub bypass_epoch_at_block: u64,
    pub bypass_batch_budget: u32,
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingFenceGuard {
    pub batch_count: usize,
    pub retained_bytes: u64,
    pub min_remaining_bypass_batches: u32,
    pub oldest_enqueued_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitQueueMetrics {
    pub enqueue_count: u64,
    pub enqueue_reject_count: u64,
    pub drain_owner_acquire_count: u64,
    pub deferred_entry_count: u64,
    pub pending_fence_block_count: u64,
    pub pending_fence_ready_count: u64,
    pub snapshot_bypass_count: u64,
    pub snapshot_bypass_budget_block_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitQueueSnapshot {
    pub entries: usize,
    pub deferred_entries: usize,
    pub pending_fence_batches: usize,
    pub queue_retained_bytes: u64,
    pub deferred_entries_bytes: u64,
    pub pending_fence_bytes: u64,
    pub next_enqueue_seq: u64,
    pub published_tail_seq: u64,
    pub pending_fence_ready_epoch: u64,
    pub drain_owner_active: bool,
    pub ready_queued: bool,
    pub metrics: CommitQueueMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitQueueError {
    Backpressure(CommitQueueBackpressure),
    Closed,
}

impl fmt::Display for CommitQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backpressure(error) => write!(f, "commit queue backpressure: {error}"),
            Self::Closed => write!(f, "commit queue is closed"),
        }
    }
}

impl std::error::Error for CommitQueueError {}

#[derive(Debug)]
pub struct CommitQueue {
    state: Mutex<QueueState>,
    published_tail_seq: AtomicU64,
    pending_fence_ready_epoch: AtomicU64,
    drain_owner_active: AtomicBool,
}

#[derive(Debug)]
struct QueueState {
    entries: VecDeque<CommitQueueEntry>,
    deferred_entries: VecDeque<CommitQueueEntry>,
    deferred_entries_bytes: u64,
    pending_fence: VecDeque<FenceBlockedBatch>,
    pending_fence_guard: PendingFenceGuard,
    next_enqueue_seq: u64,
    queue_retained_bytes: u64,
    snapshot_bypass_epoch: u64,
    ready_queued: bool,
    closed: bool,
    metrics: CommitQueueMetrics,
}

impl Default for QueueState {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            deferred_entries: VecDeque::new(),
            deferred_entries_bytes: 0,
            pending_fence: VecDeque::new(),
            pending_fence_guard: PendingFenceGuard {
                min_remaining_bypass_batches: u32::MAX,
                ..PendingFenceGuard::default()
            },
            next_enqueue_seq: 1,
            queue_retained_bytes: 0,
            snapshot_bypass_epoch: 0,
            ready_queued: false,
            closed: false,
            metrics: CommitQueueMetrics::default(),
        }
    }
}

impl CommitQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(QueueState::default()),
            published_tail_seq: AtomicU64::new(0),
            pending_fence_ready_epoch: AtomicU64::new(0),
            drain_owner_active: AtomicBool::new(false),
        })
    }

    pub fn enqueue(
        self: &Arc<Self>,
        job: PreparedCommitJob,
        completion: CommitCompletionHandle,
        policy: CommitBatchPolicy,
    ) -> Result<CommitQueueTicket, CommitQueueError> {
        let mut state = self.state.lock();
        if state.closed {
            state.metrics.enqueue_reject_count =
                state.metrics.enqueue_reject_count.saturating_add(1);
            return Err(CommitQueueError::Closed);
        }
        policy
            .check_enqueue(
                state.visible_depth(),
                state.queue_retained_bytes,
                job.retained_bytes,
            )
            .map_err(|error| {
                state.metrics.enqueue_reject_count =
                    state.metrics.enqueue_reject_count.saturating_add(1);
                CommitQueueError::Backpressure(error)
            })?;

        let queue_seq = state.next_enqueue_seq;
        state.next_enqueue_seq = state.next_enqueue_seq.saturating_add(1);
        let retained_bytes = job.retained_bytes;
        state.entries.push_back(CommitQueueEntry {
            queue_seq,
            completion,
            enqueued_at: Instant::now(),
            job,
        });
        state.queue_retained_bytes = state.queue_retained_bytes.saturating_add(retained_bytes);
        state.metrics.enqueue_count = state.metrics.enqueue_count.saturating_add(1);
        self.published_tail_seq.store(queue_seq, Ordering::Release);
        Ok(CommitQueueTicket {
            queue_seq,
            completion,
        })
    }

    pub fn try_acquire_drain_owner(self: &Arc<Self>) -> Option<CommitDrainOwner> {
        self.drain_owner_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        let mut state = self.state.lock();
        state.metrics.drain_owner_acquire_count =
            state.metrics.drain_owner_acquire_count.saturating_add(1);
        drop(state);
        Some(CommitDrainOwner {
            queue: Arc::clone(self),
            released: false,
        })
    }

    pub fn signal_drain_needed(
        self: &Arc<Self>,
        _reason: DrainSignalReason,
        inline: DrainInlinePolicy,
    ) -> bool {
        let mut state = self.state.lock();
        state.ready_queued = true;
        matches!(inline, DrainInlinePolicy::AllowInline)
            && !self.drain_owner_active.load(Ordering::Acquire)
    }

    pub fn signal_pending_fence_ready(
        self: &Arc<Self>,
        _registered_ts: CommitTs,
        inline: DrainInlinePolicy,
    ) -> bool {
        self.pending_fence_ready_epoch
            .fetch_add(1, Ordering::AcqRel);
        self.signal_drain_needed(DrainSignalReason::PendingFenceReady, inline)
    }

    pub fn close_and_drain_pending(&self) -> Vec<CommitCompletionHandle> {
        let mut state = self.state.lock();
        state.closed = true;
        state.ready_queued = false;

        let pending_count = state
            .entries
            .len()
            .saturating_add(state.deferred_entries.len())
            .saturating_add(
                state
                    .pending_fence
                    .iter()
                    .map(|batch| batch.entries.len())
                    .sum::<usize>(),
            );
        let mut completions = Vec::with_capacity(pending_count);
        completions.extend(state.entries.drain(..).map(|entry| entry.completion));
        completions.extend(
            state
                .deferred_entries
                .drain(..)
                .map(|entry| entry.completion),
        );
        for batch in state.pending_fence.drain(..) {
            completions.extend(batch.entries.into_iter().map(|entry| entry.completion));
        }

        state.queue_retained_bytes = 0;
        state.deferred_entries_bytes = 0;
        state.pending_fence_guard = PendingFenceGuard {
            min_remaining_bypass_batches: u32::MAX,
            ..PendingFenceGuard::default()
        };
        completions
    }

    pub fn snapshot(&self) -> CommitQueueSnapshot {
        let state = self.state.lock();
        CommitQueueSnapshot {
            entries: state.entries.len(),
            deferred_entries: state.deferred_entries.len(),
            pending_fence_batches: state.pending_fence.len(),
            queue_retained_bytes: state.queue_retained_bytes,
            deferred_entries_bytes: state.deferred_entries_bytes,
            pending_fence_bytes: state.pending_fence_guard.retained_bytes,
            next_enqueue_seq: state.next_enqueue_seq,
            published_tail_seq: self.published_tail_seq.load(Ordering::Acquire),
            pending_fence_ready_epoch: self.pending_fence_ready_epoch.load(Ordering::Acquire),
            drain_owner_active: self.drain_owner_active.load(Ordering::Acquire),
            ready_queued: state.ready_queued,
            metrics: state.metrics,
        }
    }

    pub fn pending_fence_retained_bytes(&self) -> u64 {
        self.state.lock().pending_fence_guard.retained_bytes
    }

    pub fn has_pending_fence(&self) -> bool {
        !self.state.lock().pending_fence.is_empty()
    }

    pub fn has_ready_work(&self) -> bool {
        let state = self.state.lock();
        !state.deferred_entries.is_empty()
            || !state.entries.is_empty()
            || !state.pending_fence.is_empty()
    }

    pub fn pending_fence_ready_epoch(&self) -> u64 {
        self.pending_fence_ready_epoch.load(Ordering::Acquire)
    }
}

impl QueueState {
    fn visible_depth(&self) -> usize {
        self.entries
            .len()
            .saturating_add(self.deferred_entries.len())
            .saturating_add(
                self.pending_fence
                    .iter()
                    .map(|batch| batch.entries.len())
                    .sum::<usize>(),
            )
    }

    fn recompute_pending_guard(&mut self, policy: CommitBatchPolicy) {
        let retained_bytes = self
            .pending_fence
            .iter()
            .map(|batch| batch.retained_bytes)
            .sum();
        let oldest_enqueued_at = self
            .pending_fence
            .iter()
            .map(|batch| batch.enqueued_at)
            .min();
        let min_remaining_bypass_batches = self
            .pending_fence
            .iter()
            .map(|batch| {
                let spent = self
                    .snapshot_bypass_epoch
                    .saturating_sub(batch.bypass_epoch_at_block);
                batch.bypass_batch_budget.saturating_sub(spent as u32)
            })
            .min()
            .unwrap_or(u32::MAX)
            .min(policy.serializable_bypass_batch_budget);

        self.pending_fence_guard = PendingFenceGuard {
            batch_count: self.pending_fence.len(),
            retained_bytes,
            min_remaining_bypass_batches,
            oldest_enqueued_at,
        };
    }
}

pub struct CommitDrainOwner {
    queue: Arc<CommitQueue>,
    released: bool,
}

impl CommitDrainOwner {
    pub fn take_local_buffer(&self, max_entries: usize) -> Vec<CommitQueueEntry> {
        let mut state = self.queue.state.lock();
        state.ready_queued = false;
        let tail_seq = self.queue.published_tail_seq.load(Ordering::Acquire);
        let mut local = Vec::with_capacity(max_entries);

        while local.len() < max_entries {
            let Some(entry) = state.deferred_entries.pop_front() else {
                break;
            };
            state.deferred_entries_bytes = state
                .deferred_entries_bytes
                .saturating_sub(entry.retained_bytes());
            state.queue_retained_bytes = state
                .queue_retained_bytes
                .saturating_sub(entry.retained_bytes());
            local.push(entry);
        }

        while local.len() < max_entries {
            let Some(front) = state.entries.front() else {
                break;
            };
            if front.queue_seq > tail_seq {
                break;
            }
            let entry = state.entries.pop_front().expect("front exists");
            state.queue_retained_bytes = state
                .queue_retained_bytes
                .saturating_sub(entry.retained_bytes());
            local.push(entry);
        }

        local
    }

    pub fn take_ready_pending_fence(
        &self,
        registered_ts: CommitTs,
        policy: CommitBatchPolicy,
    ) -> Option<Vec<CommitQueueEntry>> {
        let mut state = self.queue.state.lock();
        let ready = state
            .pending_fence
            .front()
            .is_some_and(|batch| batch.required_registered_ts <= registered_ts);
        if !ready {
            return None;
        }
        let batch = state.pending_fence.pop_front().expect("front checked");
        state.queue_retained_bytes = state
            .queue_retained_bytes
            .saturating_sub(batch.retained_bytes);
        state.metrics.pending_fence_ready_count =
            state.metrics.pending_fence_ready_count.saturating_add(1);
        state.recompute_pending_guard(policy);
        Some(batch.entries)
    }

    pub fn block_on_fence(
        &self,
        required_registered_ts: CommitTs,
        entries: Vec<CommitQueueEntry>,
        policy: CommitBatchPolicy,
    ) {
        if entries.is_empty() {
            return;
        }
        let retained_bytes = entries.iter().fold(0_u64, |sum, entry| {
            sum.saturating_add(entry.retained_bytes())
        });
        let original_head_seq = entries[0].queue_seq;
        let enqueued_at = entries[0].enqueued_at;
        let mut state = self.queue.state.lock();
        let bypass_epoch_at_block = state.snapshot_bypass_epoch;
        state.queue_retained_bytes = state.queue_retained_bytes.saturating_add(retained_bytes);
        state.pending_fence.push_back(FenceBlockedBatch {
            required_registered_ts,
            entries,
            original_head_seq,
            enqueued_at,
            bypass_epoch_at_block,
            bypass_batch_budget: policy.serializable_bypass_batch_budget,
            retained_bytes,
        });
        state.metrics.pending_fence_block_count =
            state.metrics.pending_fence_block_count.saturating_add(1);
        state.recompute_pending_guard(policy);
    }

    pub fn can_bypass_snapshot(&self, policy: CommitBatchPolicy, now: Instant) -> bool {
        let mut state = self.queue.state.lock();
        if state.pending_fence.is_empty() {
            return true;
        }
        let guard = state.pending_fence_guard;
        if guard.min_remaining_bypass_batches == 0 {
            state.metrics.snapshot_bypass_budget_block_count = state
                .metrics
                .snapshot_bypass_budget_block_count
                .saturating_add(1);
            return false;
        }
        if let Some(oldest) = guard.oldest_enqueued_at {
            let waited_us = now.duration_since(oldest).as_micros().min(u64::MAX as u128) as u64;
            if waited_us >= policy.serializable_bypass_wait_budget_us {
                state.metrics.snapshot_bypass_budget_block_count = state
                    .metrics
                    .snapshot_bypass_budget_block_count
                    .saturating_add(1);
                return false;
            }
        }
        true
    }

    pub fn snapshot_bypass_conflicts_with_pending_head(&self, entry: &CommitQueueEntry) -> bool {
        if !entry.is_snapshot_only() {
            return false;
        }
        let state = self.queue.state.lock();
        let Some(head) = state.pending_fence.front() else {
            return false;
        };
        head.entries.iter().any(|pending| {
            pending
                .job
                .sequencing_plan
                .write_set
                .iter()
                .any(|pending_write| {
                    entry
                        .job
                        .sequencing_plan
                        .write_set
                        .iter()
                        .any(|candidate_write| pending_write.conflicts_with(candidate_write))
                })
        })
    }

    pub fn record_snapshot_bypass(&self, policy: CommitBatchPolicy) {
        let mut state = self.queue.state.lock();
        if state.pending_fence.is_empty() {
            return;
        }
        state.snapshot_bypass_epoch = state.snapshot_bypass_epoch.saturating_add(1);
        state.metrics.snapshot_bypass_count = state.metrics.snapshot_bypass_count.saturating_add(1);
        state.pending_fence_guard.min_remaining_bypass_batches = state
            .pending_fence_guard
            .min_remaining_bypass_batches
            .saturating_sub(1);
        state.recompute_pending_guard(policy);
    }

    pub fn defer_entries(&self, entries: Vec<CommitQueueEntry>) {
        if entries.is_empty() {
            return;
        }
        let mut state = self.queue.state.lock();
        for entry in entries {
            state.deferred_entries_bytes = state
                .deferred_entries_bytes
                .saturating_add(entry.retained_bytes());
            state.queue_retained_bytes = state
                .queue_retained_bytes
                .saturating_add(entry.retained_bytes());
            state.deferred_entries.push_back(entry);
            state.metrics.deferred_entry_count =
                state.metrics.deferred_entry_count.saturating_add(1);
        }
    }

    pub fn release(mut self) {
        self.released = true;
        self.queue
            .drain_owner_active
            .store(false, Ordering::Release);
    }
}

impl Drop for CommitDrainOwner {
    fn drop(&mut self) {
        if !self.released {
            self.queue
                .drain_owner_active
                .store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{
        AppendFailureRollbackPlan, CommitSequencingPlan, LockReleasePlan, PrePublishReleasePlan,
        RequiredPublishPlan,
    };
    use crate::{
        CommitAckPolicy, CommitRequest, DatabaseId, FrozenLockSet, LockNamespace, LockResource,
        ReadTs, TableId, TransactionView, TxnId,
    };
    use paro_common::durability::PreparedCommitPlan;

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

    fn job(txn_id: u64, isolation: IsolationLevel, retained_bytes: u64) -> PreparedCommitJob {
        let mut request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(txn_id),
            TransactionView::autocommit(ReadTs::new(1)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );
        request.isolation = isolation;
        PreparedCommitJob {
            sequencing_plan: CommitSequencingPlan::from_commit_plan(request.commit_plan()),
            durable_plan: empty_plan(txn_id),
            reservation_input: request.commit_plan().into(),
            lock_release_plan: LockReleasePlan::noop(),
            pre_publish_release_plan: PrePublishReleasePlan::noop(),
            append_failure_rollback_plan: AppendFailureRollbackPlan::noop(),
            required_publish: RequiredPublishPlan::noop_for_tests(),
            deferred_publish: Vec::new(),
            ack_policy: CommitAckPolicy::RequiredPublished,
            estimated_record_bytes: 64,
            retained_bytes,
            created_at: Instant::now(),
        }
    }

    fn job_with_write_set(
        txn_id: u64,
        isolation: IsolationLevel,
        write_set: Vec<LockResource>,
    ) -> PreparedCommitJob {
        let mut job = job(txn_id, isolation, 50);
        job.sequencing_plan.write_set = write_set;
        job
    }

    fn key_resource(key_hash: u64) -> LockResource {
        LockResource::primary_key(
            LockNamespace::single_tenant(DatabaseId::new(1)),
            TableId::new(10),
            1,
            key_hash,
        )
    }

    #[test]
    fn enqueue_uses_retained_bytes_not_wal_estimate() {
        let queue = CommitQueue::new();
        let policy = CommitBatchPolicy {
            max_commit_queue_bytes: 100,
            ..CommitBatchPolicy::default()
        };

        let err = queue
            .enqueue(
                job(1, IsolationLevel::Snapshot, 101),
                CommitCompletionHandle { slot_id: 1 },
                policy,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            CommitQueueError::Backpressure(CommitQueueBackpressure::QueueRetainedBytes {
                bytes: 101,
                limit: 100
            })
        ));
    }

    #[test]
    fn drain_owner_moves_unprocessed_suffix_to_deferred_home() {
        let queue = CommitQueue::new();
        let policy = CommitBatchPolicy::default();
        queue
            .enqueue(
                job(1, IsolationLevel::Snapshot, 10),
                CommitCompletionHandle { slot_id: 1 },
                policy,
            )
            .unwrap();
        queue
            .enqueue(
                job(2, IsolationLevel::Snapshot, 20),
                CommitCompletionHandle { slot_id: 2 },
                policy,
            )
            .unwrap();

        let owner = queue.try_acquire_drain_owner().unwrap();
        let mut local = owner.take_local_buffer(8);
        assert_eq!(local.len(), 2);
        let suffix = vec![local.pop().unwrap()];
        owner.defer_entries(suffix);
        owner.release();

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.deferred_entries, 1);
        assert_eq!(snapshot.deferred_entries_bytes, 20);
        assert_eq!(snapshot.queue_retained_bytes, 20);
    }

    #[test]
    fn close_and_drain_pending_returns_every_queued_completion() {
        let queue = CommitQueue::new();
        let policy = CommitBatchPolicy::default();
        queue
            .enqueue(
                job(1, IsolationLevel::Snapshot, 10),
                CommitCompletionHandle { slot_id: 1 },
                policy,
            )
            .unwrap();

        let owner = queue.try_acquire_drain_owner().unwrap();
        owner.defer_entries(vec![CommitQueueEntry {
            queue_seq: 2,
            completion: CommitCompletionHandle { slot_id: 2 },
            enqueued_at: Instant::now(),
            job: job(2, IsolationLevel::Snapshot, 20),
        }]);
        owner.block_on_fence(
            CommitTs::new(7),
            vec![CommitQueueEntry {
                queue_seq: 3,
                completion: CommitCompletionHandle { slot_id: 3 },
                enqueued_at: Instant::now(),
                job: job(3, IsolationLevel::Serializable, 30),
            }],
            policy,
        );
        owner.release();

        let mut slots = queue
            .close_and_drain_pending()
            .into_iter()
            .map(|handle| handle.slot_id)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        assert_eq!(slots, vec![1, 2, 3]);

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.entries, 0);
        assert_eq!(snapshot.deferred_entries, 0);
        assert_eq!(snapshot.pending_fence_batches, 0);
        assert_eq!(snapshot.queue_retained_bytes, 0);
        assert_eq!(snapshot.deferred_entries_bytes, 0);
        assert_eq!(snapshot.pending_fence_bytes, 0);
    }

    #[test]
    fn pending_fence_guard_blocks_snapshot_bypass_after_budget() {
        let queue = CommitQueue::new();
        let policy = CommitBatchPolicy {
            serializable_bypass_batch_budget: 1,
            serializable_bypass_wait_budget_us: 10_000,
            ..CommitBatchPolicy::default()
        };
        let owner = queue.try_acquire_drain_owner().unwrap();
        owner.block_on_fence(
            CommitTs::new(7),
            vec![CommitQueueEntry {
                queue_seq: 1,
                completion: CommitCompletionHandle { slot_id: 1 },
                enqueued_at: Instant::now(),
                job: job(1, IsolationLevel::Serializable, 50),
            }],
            policy,
        );

        assert!(owner.can_bypass_snapshot(policy, Instant::now()));
        owner.record_snapshot_bypass(policy);
        assert!(!owner.can_bypass_snapshot(policy, Instant::now()));

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.metrics.snapshot_bypass_count, 1);
        assert_eq!(snapshot.pending_fence_bytes, 50);
        owner.release();
    }

    #[test]
    fn snapshot_bypass_checks_pending_head_write_conflict() {
        let queue = CommitQueue::new();
        let policy = CommitBatchPolicy::default();
        let owner = queue.try_acquire_drain_owner().unwrap();
        owner.block_on_fence(
            CommitTs::new(7),
            vec![CommitQueueEntry {
                queue_seq: 1,
                completion: CommitCompletionHandle { slot_id: 1 },
                enqueued_at: Instant::now(),
                job: job_with_write_set(1, IsolationLevel::Serializable, vec![key_resource(44)]),
            }],
            policy,
        );

        let conflicting = CommitQueueEntry {
            queue_seq: 2,
            completion: CommitCompletionHandle { slot_id: 2 },
            enqueued_at: Instant::now(),
            job: job_with_write_set(2, IsolationLevel::Snapshot, vec![key_resource(44)]),
        };
        let independent = CommitQueueEntry {
            queue_seq: 3,
            completion: CommitCompletionHandle { slot_id: 3 },
            enqueued_at: Instant::now(),
            job: job_with_write_set(3, IsolationLevel::Snapshot, vec![key_resource(45)]),
        };

        assert!(owner.snapshot_bypass_conflicts_with_pending_head(&conflicting));
        assert!(!owner.snapshot_bypass_conflicts_with_pending_head(&independent));
        owner.release();
    }
}
