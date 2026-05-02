// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit job DTOs that move through prepare, append, and finalize stages.

mod reservation;

use super::{
    AppendFailureRollbackPlan, CommitAckPolicy, CommitSequencingPlan, DurableCommitHandle,
    LockReleasePlan, PrePublishReleasePlan, RequiredPublishPlan,
};
use crate::types::CommitTs;
use paro_common::durability::PreparedCommitPlan;
use paro_common::journal::CommitRecord;
pub use reservation::{
    CommitFinalizeReservation, CommitFinalizeReservationInput, SummaryReservation,
    WriteConflictPlacementInput, WriteConflictReservation,
};
use std::fmt;
use std::time::Instant;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeferredPublishPlan {
    pub retained_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitCompletionHandle {
    pub slot_id: u64,
}

pub struct PreparedCommitJob {
    pub sequencing_plan: CommitSequencingPlan,
    pub durable_plan: PreparedCommitPlan,
    pub reservation_input: CommitFinalizeReservationInput,
    pub lock_release_plan: LockReleasePlan,
    pub pre_publish_release_plan: PrePublishReleasePlan,
    pub append_failure_rollback_plan: AppendFailureRollbackPlan,
    pub required_publish: RequiredPublishPlan,
    pub deferred_publish: Vec<DeferredPublishPlan>,
    pub ack_policy: CommitAckPolicy,
    pub estimated_record_bytes: u32,
    pub retained_bytes: u64,
    pub created_at: Instant,
}

impl fmt::Debug for PreparedCommitJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedCommitJob")
            .field("sequencing_plan", &self.sequencing_plan)
            .field("reservation_input", &self.reservation_input)
            .field("deferred_publish", &self.deferred_publish)
            .field("ack_policy", &self.ack_policy)
            .field("estimated_record_bytes", &self.estimated_record_bytes)
            .field("retained_bytes", &self.retained_bytes)
            .field("created_at", &self.created_at)
            .finish_non_exhaustive()
    }
}

impl PreparedCommitJob {
    pub fn into_sequenced(
        self,
        commit_ts: CommitTs,
        finalize_reservation: CommitFinalizeReservation,
        completion: CommitCompletionHandle,
    ) -> SequencedCommitJob {
        let record = self.durable_plan.into_record(commit_ts.into_raw());
        SequencedCommitJob {
            commit_ts,
            record: Some(record),
            estimated_record_bytes: self.estimated_record_bytes,
            retained_bytes: self.retained_bytes,
            post_append: Some(SequencedCommitPostAppend {
                finalize_reservation,
                lock_release_plan: self.lock_release_plan,
                pre_publish_release_plan: self.pre_publish_release_plan,
                append_failure_rollback_plan: self.append_failure_rollback_plan,
                required_publish: self.required_publish,
                ack_policy: self.ack_policy,
                completion,
            }),
        }
    }
}

pub struct SequencedCommitJob {
    pub commit_ts: CommitTs,
    record: Option<CommitRecord>,
    pub estimated_record_bytes: u32,
    pub retained_bytes: u64,
    post_append: Option<SequencedCommitPostAppend>,
}

impl SequencedCommitJob {
    pub fn new(
        commit_ts: CommitTs,
        record: CommitRecord,
        estimated_record_bytes: u32,
        retained_bytes: u64,
        post_append: SequencedCommitPostAppend,
    ) -> Self {
        Self {
            commit_ts,
            record: Some(record),
            estimated_record_bytes,
            retained_bytes,
            post_append: Some(post_append),
        }
    }

    pub fn has_record(&self) -> bool {
        self.record.is_some()
    }

    pub fn record_commit_id(&self) -> Option<u64> {
        self.record.as_ref().map(|record| record.commit_id)
    }

    pub fn take_record(&mut self) -> Result<CommitRecord, SequencedCommitJobStateError> {
        self.record
            .take()
            .ok_or(SequencedCommitJobStateError::RecordAlreadyTaken {
                commit_ts: self.commit_ts,
            })
    }

    pub fn take_durable_job(
        &mut self,
        handle: &DurableCommitHandle,
    ) -> Result<DurableCommitJob, SequencedCommitJobStateError> {
        if self.record.is_some() {
            return Err(SequencedCommitJobStateError::RecordNotAppended {
                commit_ts: self.commit_ts,
            });
        }
        if handle.commit_ts() != self.commit_ts {
            return Err(SequencedCommitJobStateError::HandleCommitTsMismatch {
                job_commit_ts: self.commit_ts,
                handle_commit_ts: handle.commit_ts(),
            });
        }
        let post_append = self.post_append.take().ok_or(
            SequencedCommitJobStateError::PostAppendAlreadyTaken {
                commit_ts: self.commit_ts,
            },
        )?;
        Ok(post_append.into_durable_job(self.commit_ts, self.retained_bytes))
    }

    pub fn take_append_failure_cleanup(
        &mut self,
    ) -> Result<AppendFailureCleanupBundle, SequencedCommitJobStateError> {
        let post_append = self.post_append.take().ok_or(
            SequencedCommitJobStateError::PostAppendAlreadyTaken {
                commit_ts: self.commit_ts,
            },
        )?;
        Ok(post_append.into_append_failure_cleanup())
    }

    pub fn cleanup_after_append_failure(
        mut self,
    ) -> Result<AppendFailureCleanupBundle, SequencedCommitJobStateError> {
        self.take_append_failure_cleanup()
    }

    pub fn cleanup_after_durable_ambiguous(
        mut self,
    ) -> Result<DurableAmbiguousCleanupBundle, SequencedCommitJobStateError> {
        let post_append = self.post_append.take().ok_or(
            SequencedCommitJobStateError::PostAppendAlreadyTaken {
                commit_ts: self.commit_ts,
            },
        )?;
        Ok(post_append.into_durable_ambiguous_cleanup())
    }
}

impl fmt::Debug for SequencedCommitJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequencedCommitJob")
            .field("commit_ts", &self.commit_ts)
            .field("has_record", &self.record.is_some())
            .field("estimated_record_bytes", &self.estimated_record_bytes)
            .field("retained_bytes", &self.retained_bytes)
            .field("has_post_append", &self.post_append.is_some())
            .finish()
    }
}

pub struct SequencedCommitPostAppend {
    pub finalize_reservation: CommitFinalizeReservation,
    pub lock_release_plan: LockReleasePlan,
    pub pre_publish_release_plan: PrePublishReleasePlan,
    pub append_failure_rollback_plan: AppendFailureRollbackPlan,
    pub required_publish: RequiredPublishPlan,
    pub ack_policy: CommitAckPolicy,
    pub completion: CommitCompletionHandle,
}

impl SequencedCommitPostAppend {
    fn into_durable_job(self, commit_ts: CommitTs, retained_bytes: u64) -> DurableCommitJob {
        DurableCommitJob {
            commit_ts,
            retained_bytes,
            finalize_reservation: self.finalize_reservation,
            lock_release_plan: self.lock_release_plan,
            pre_publish_release_plan: self.pre_publish_release_plan,
            required_publish: self.required_publish,
            ack_policy: self.ack_policy,
            completion: self.completion,
        }
    }

    fn into_append_failure_cleanup(self) -> AppendFailureCleanupBundle {
        AppendFailureCleanupBundle {
            completion: self.completion,
            reservation: self.finalize_reservation,
            append_failure_rollback_plan: self.append_failure_rollback_plan,
            lock_release_plan: self.lock_release_plan,
            pre_publish_release_plan: self.pre_publish_release_plan,
        }
    }

    fn into_durable_ambiguous_cleanup(self) -> DurableAmbiguousCleanupBundle {
        DurableAmbiguousCleanupBundle {
            completion: self.completion,
            reservation: self.finalize_reservation,
            lock_release_plan: self.lock_release_plan,
            pre_publish_release_plan: self.pre_publish_release_plan,
        }
    }
}

impl fmt::Debug for SequencedCommitPostAppend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequencedCommitPostAppend")
            .field("finalize_reservation", &self.finalize_reservation)
            .field("ack_policy", &self.ack_policy)
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

pub struct DurableCommitJob {
    pub commit_ts: CommitTs,
    pub retained_bytes: u64,
    pub finalize_reservation: CommitFinalizeReservation,
    pub lock_release_plan: LockReleasePlan,
    pub pre_publish_release_plan: PrePublishReleasePlan,
    pub required_publish: RequiredPublishPlan,
    pub ack_policy: CommitAckPolicy,
    pub completion: CommitCompletionHandle,
}

impl fmt::Debug for DurableCommitJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableCommitJob")
            .field("commit_ts", &self.commit_ts)
            .field("retained_bytes", &self.retained_bytes)
            .field("finalize_reservation", &self.finalize_reservation)
            .field("ack_policy", &self.ack_policy)
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct AppendFailureCleanupBundle {
    pub completion: CommitCompletionHandle,
    pub reservation: CommitFinalizeReservation,
    pub append_failure_rollback_plan: AppendFailureRollbackPlan,
    pub lock_release_plan: LockReleasePlan,
    pub pre_publish_release_plan: PrePublishReleasePlan,
}

#[derive(Debug)]
pub struct DurableAmbiguousCleanupBundle {
    pub completion: CommitCompletionHandle,
    pub reservation: CommitFinalizeReservation,
    pub lock_release_plan: LockReleasePlan,
    pub pre_publish_release_plan: PrePublishReleasePlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequencedCommitJobStateError {
    RecordAlreadyTaken {
        commit_ts: CommitTs,
    },
    RecordNotAppended {
        commit_ts: CommitTs,
    },
    PostAppendAlreadyTaken {
        commit_ts: CommitTs,
    },
    HandleCommitTsMismatch {
        job_commit_ts: CommitTs,
        handle_commit_ts: CommitTs,
    },
}

impl fmt::Display for SequencedCommitJobStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecordAlreadyTaken { commit_ts } => {
                write!(f, "commit record for {} was already taken", commit_ts)
            }
            Self::RecordNotAppended { commit_ts } => {
                write!(f, "commit record for {} has not been appended", commit_ts)
            }
            Self::PostAppendAlreadyTaken { commit_ts } => {
                write!(f, "post-append state for {} was already taken", commit_ts)
            }
            Self::HandleCommitTsMismatch {
                job_commit_ts,
                handle_commit_ts,
            } => write!(
                f,
                "durable handle commit_ts {} does not match job commit_ts {}",
                handle_commit_ts, job_commit_ts
            ),
        }
    }
}

impl std::error::Error for SequencedCommitJobStateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommitAckPolicy, CommitRequest, DatabaseId, FrozenLockSet, ReadTs, TxnId};
    use paro_common::durability::PreparedCommitPlan;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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

    fn post_append() -> SequencedCommitPostAppend {
        SequencedCommitPostAppend {
            finalize_reservation: CommitFinalizeReservation::default(),
            lock_release_plan: LockReleasePlan::noop(),
            pre_publish_release_plan: PrePublishReleasePlan::noop(),
            append_failure_rollback_plan: AppendFailureRollbackPlan::noop(),
            required_publish: RequiredPublishPlan::noop_for_tests(),
            ack_policy: CommitAckPolicy::RequiredPublished,
            completion: CommitCompletionHandle::default(),
        }
    }

    #[test]
    fn prepared_job_moves_payload_into_sequenced_record() {
        let request = CommitRequest::new(
            DatabaseId::new(1),
            TxnId::new(7),
            crate::TransactionView::autocommit(ReadTs::new(3)),
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );
        let job = PreparedCommitJob {
            sequencing_plan: CommitSequencingPlan::from_commit_plan(request.commit_plan()),
            durable_plan: empty_plan(7),
            reservation_input: request.commit_plan().into(),
            lock_release_plan: LockReleasePlan::noop(),
            pre_publish_release_plan: PrePublishReleasePlan::noop(),
            append_failure_rollback_plan: AppendFailureRollbackPlan::noop(),
            required_publish: RequiredPublishPlan::noop_for_tests(),
            deferred_publish: Vec::new(),
            ack_policy: CommitAckPolicy::RequiredPublished,
            estimated_record_bytes: 123,
            retained_bytes: 456,
            created_at: Instant::now(),
        };

        let sequenced = job.into_sequenced(
            CommitTs::new(11),
            CommitFinalizeReservation::default(),
            CommitCompletionHandle::default(),
        );

        assert_eq!(sequenced.commit_ts, CommitTs::new(11));
        assert_eq!(sequenced.estimated_record_bytes, 123);
        assert_eq!(sequenced.retained_bytes, 456);
        assert_eq!(sequenced.record.as_ref().unwrap().commit_id, 11);
    }

    #[test]
    fn sequenced_job_takes_record_before_durable_conversion() {
        let mut job = SequencedCommitJob::new(
            CommitTs::new(5),
            empty_plan(5).into_record(5),
            88,
            99,
            post_append(),
        );

        let record = job.take_record().unwrap();
        assert_eq!(record.commit_id, 5);

        let batch = Arc::new(
            super::super::CommitDurableBatch::new(
                1,
                1,
                1,
                88,
                Arc::from([88_u32]),
                10,
                CommitTs::new(5),
                CommitTs::new(5),
            )
            .unwrap(),
        );
        let handle = batch.handle_at(0).unwrap();
        let durable = job.take_durable_job(&handle).unwrap();

        assert_eq!(durable.commit_ts, CommitTs::new(5));
        assert_eq!(durable.retained_bytes, 99);
        assert!(!job.has_record());
    }

    #[test]
    fn finalize_reservation_consumes_exactly_one_lifecycle_path() {
        let registered = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let reservation = CommitFinalizeReservation::new(
            WriteConflictReservation { slot_id: 1 },
            SummaryReservation { slot_id: 2 },
            {
                let registered = Arc::clone(&registered);
                move || {
                    registered.fetch_add(1, Ordering::Release);
                }
            },
            {
                let released = Arc::clone(&released);
                move || {
                    released.fetch_add(1, Ordering::Release);
                }
            },
        );

        reservation.apply();
        assert_eq!(registered.load(Ordering::Acquire), 1);
        assert_eq!(released.load(Ordering::Acquire), 0);

        let reservation = CommitFinalizeReservation::new(
            WriteConflictReservation { slot_id: 1 },
            SummaryReservation { slot_id: 2 },
            {
                let registered = Arc::clone(&registered);
                move || {
                    registered.fetch_add(1, Ordering::Release);
                }
            },
            {
                let released = Arc::clone(&released);
                move || {
                    released.fetch_add(1, Ordering::Release);
                }
            },
        );

        reservation.release();
        assert_eq!(registered.load(Ordering::Acquire), 1);
        assert_eq!(released.load(Ordering::Acquire), 1);
    }
}
