// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Serializable Snapshot Isolation validation.

use crate::{
    ActiveReadConflict, CommitPlan, CommittedTxnConflict, CommittedTxnSummaryIndex, IsolationLevel,
    LockResource, ReadDependencyIndex, SsiTxnState, TxnId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsiValidationOutcome {
    pub validation_epoch: u64,
    pub ssi_effect_epoch: u64,
    pub rw_conflict_in: bool,
    pub rw_conflict_out: bool,
    pub active_conflict_count: usize,
    pub coarse_scan_marker_conflict: bool,
    pub committed_write_conflict: Option<CommittedTxnConflict>,
    pub committed_read_conflict: Option<CommittedTxnConflict>,
    pub active_read_conflict: Option<ActiveReadConflict>,
}

impl SsiValidationOutcome {
    #[inline]
    pub fn snapshot(validation_epoch: u64) -> Self {
        Self {
            validation_epoch,
            ssi_effect_epoch: 0,
            rw_conflict_in: false,
            rw_conflict_out: false,
            active_conflict_count: 0,
            coarse_scan_marker_conflict: false,
            committed_write_conflict: None,
            committed_read_conflict: None,
            active_read_conflict: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsiValidationError {
    DangerousStructure {
        txn_id: TxnId,
        state: SsiTxnState,
        committed_write_conflict: Option<CommittedTxnConflict>,
        committed_read_conflict: Option<CommittedTxnConflict>,
        active_read_conflict: Option<ActiveReadConflict>,
        coarse_scan_marker_conflict: bool,
    },
}

impl SsiValidationError {
    #[inline]
    pub const fn coarse_scan_marker_conflict(&self) -> bool {
        match self {
            Self::DangerousStructure {
                coarse_scan_marker_conflict,
                ..
            } => *coarse_scan_marker_conflict,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SsiValidator<'a> {
    read_dependency_index: &'a ReadDependencyIndex,
    committed_summaries: &'a CommittedTxnSummaryIndex,
}

impl<'a> SsiValidator<'a> {
    #[inline]
    pub const fn new(
        read_dependency_index: &'a ReadDependencyIndex,
        committed_summaries: &'a CommittedTxnSummaryIndex,
    ) -> Self {
        Self {
            read_dependency_index,
            committed_summaries,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn validate_commit(
        &self,
        plan: &CommitPlan,
        write_set: &[LockResource],
    ) -> Result<SsiValidationOutcome, SsiValidationError> {
        let validation_start_epoch = self.read_dependency_index.state_epoch();
        if plan.isolation != IsolationLevel::Serializable {
            return Ok(SsiValidationOutcome::snapshot(validation_start_epoch));
        }

        let committed_write_conflict = self
            .committed_summaries
            .first_write_conflict_for_reads(plan.read_ts, plan.frozen_read_set.dependencies());
        let committed_write_coarse = committed_write_conflict
            .as_ref()
            .is_some_and(CommittedTxnConflict::is_coarse_scan_marker);
        if committed_write_conflict.is_some() {
            self.read_dependency_index
                .mark_txn_conflict_out(plan.txn_id);
            if committed_write_coarse {
                self.read_dependency_index
                    .mark_txn_coarse_scan_conflict(plan.txn_id);
            }
        }

        let committed_read_conflict = self
            .committed_summaries
            .first_read_conflict_for_writes(plan.read_ts, write_set);
        let committed_read_coarse = committed_read_conflict
            .as_ref()
            .is_some_and(CommittedTxnConflict::is_coarse_scan_marker);
        if committed_read_conflict.is_some() {
            self.read_dependency_index.mark_txn_conflict_in(plan.txn_id);
            if committed_read_coarse {
                self.read_dependency_index
                    .mark_txn_coarse_scan_conflict(plan.txn_id);
            }
        }

        let active_effects = self
            .read_dependency_index
            .mark_write_conflicts(plan.txn_id, write_set);
        if active_effects.writer_has_conflict_in {
            self.read_dependency_index.mark_txn_conflict_in(plan.txn_id);
            if active_effects.coarse_scan_marker_conflict {
                self.read_dependency_index
                    .mark_txn_coarse_scan_conflict(plan.txn_id);
            }
        }

        let mut state = self.read_dependency_index.ssi_state(plan.txn_id);
        if state.rw_conflict_in && state.rw_conflict_out {
            state = self.read_dependency_index.mark_txn_dangerous(plan.txn_id);
        }

        let validation_epoch = self
            .read_dependency_index
            .state_epoch()
            .max(validation_start_epoch);
        let ssi_effect_epoch = validation_epoch.max(active_effects.ssi_effect_epoch);
        let coarse_scan_marker_conflict = state.coarse_scan_marker_conflict
            || committed_write_coarse
            || committed_read_coarse
            || active_effects.coarse_scan_marker_conflict;

        if state.dangerous_structure {
            return Err(SsiValidationError::DangerousStructure {
                txn_id: plan.txn_id,
                state,
                committed_write_conflict,
                committed_read_conflict,
                active_read_conflict: active_effects.first_conflict,
                coarse_scan_marker_conflict,
            });
        }

        Ok(SsiValidationOutcome {
            validation_epoch,
            ssi_effect_epoch,
            rw_conflict_in: state.rw_conflict_in,
            rw_conflict_out: state.rw_conflict_out,
            active_conflict_count: active_effects.matched_txn_count,
            coarse_scan_marker_conflict,
            committed_write_conflict,
            committed_read_conflict,
            active_read_conflict: active_effects.first_conflict,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CommitRequest, CommitTs, FrozenLockSet, LockMode, LockNamespace, LockRequest,
        ParticipantStateSet, ReadSnapshot, ReadTrackerHandle, ReadTs, TableId, TransactionView,
        WriterId,
    };
    use std::sync::Arc;

    fn ns() -> LockNamespace {
        LockNamespace::single_tenant(crate::DatabaseId::new(1))
    }

    fn table_resource(table_id: u64) -> LockResource {
        LockResource::Table {
            namespace: ns(),
            table_id: TableId::new(table_id),
        }
    }

    fn serializable_plan(
        txn_id: u64,
        read_ts: u64,
        read_index: Arc<ReadDependencyIndex>,
        read_table: Option<u64>,
        write_table: Option<u64>,
    ) -> (CommitPlan, Vec<LockResource>) {
        serializable_plan_with_policy(
            txn_id,
            read_ts,
            read_index,
            read_table,
            write_table,
            crate::ReadTrackingPolicy::Serializable,
        )
    }

    fn serializable_plan_with_policy(
        txn_id: u64,
        read_ts: u64,
        read_index: Arc<ReadDependencyIndex>,
        read_table: Option<u64>,
        write_table: Option<u64>,
        policy: crate::ReadTrackingPolicy,
    ) -> (CommitPlan, Vec<LockResource>) {
        let tracker = ReadTrackerHandle::serializable_with_policy(
            read_index,
            TxnId::new(txn_id),
            ReadTs::new(read_ts),
            policy,
        );
        if let Some(table_id) = read_table {
            tracker.record_table_read(TableId::new(table_id));
        }
        let locks = write_table
            .into_iter()
            .map(|table_id| LockRequest::new(table_resource(table_id), LockMode::X))
            .collect::<Vec<_>>();
        let view = TransactionView::new(
            WriterId::new(txn_id),
            ReadTs::new(read_ts),
            ReadSnapshot::without_lease(ReadTs::new(read_ts)),
            IsolationLevel::Serializable,
            crate::CommandId::new(0),
            tracker,
            ParticipantStateSet::empty(),
        );
        let request = CommitRequest::new(
            crate::DatabaseId::new(1),
            TxnId::new(txn_id),
            view,
            crate::CommitAckPolicy::RequiredPublished,
            FrozenLockSet::from_locks(locks),
            Vec::new(),
        );
        let plan = request.commit_plan();
        let writes = plan
            .lock_set
            .locks()
            .iter()
            .map(|lock| lock.resource.clone())
            .collect();
        (plan, writes)
    }

    #[test]
    fn validator_detects_committed_write_after_read() {
        let read_index = Arc::new(ReadDependencyIndex::with_shards(4));
        let summaries = CommittedTxnSummaryIndex::with_shards(4);
        summaries
            .register_commit(crate::CommittedTxnSummary::new(
                TxnId::new(200),
                ReadTs::new(4),
                CommitTs::new(8),
                [table_resource(10)],
                &crate::FrozenReadSet::empty(),
            ))
            .unwrap();
        let (plan, writes) = serializable_plan(100, 5, Arc::clone(&read_index), Some(10), None);

        let outcome = SsiValidator::new(&read_index, &summaries)
            .validate_commit(&plan, &writes)
            .unwrap();

        assert!(outcome.rw_conflict_out);
        assert!(outcome.committed_write_conflict.is_some());
    }

    #[test]
    fn snapshot_validation_does_not_publish_ssi_effect_epoch() {
        let read_index = ReadDependencyIndex::with_shards(4);
        let summaries = CommittedTxnSummaryIndex::with_shards(4);
        let request = CommitRequest::new(
            crate::DatabaseId::new(1),
            TxnId::new(100),
            TransactionView::autocommit(ReadTs::new(5)),
            crate::CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            Vec::new(),
        );

        let outcome = SsiValidator::new(&read_index, &summaries)
            .validate_commit(&request.commit_plan(), &[])
            .unwrap();

        assert_eq!(outcome.ssi_effect_epoch, 0);
    }

    #[test]
    fn validator_updates_active_reader_flags_for_writer() {
        let read_index = Arc::new(ReadDependencyIndex::with_shards(4));
        let summaries = CommittedTxnSummaryIndex::with_shards(4);
        let (_reader_plan, _reader_writes) =
            serializable_plan(100, 5, Arc::clone(&read_index), Some(10), None);
        let (writer_plan, writer_writes) =
            serializable_plan(101, 5, Arc::clone(&read_index), None, Some(10));

        let outcome = SsiValidator::new(&read_index, &summaries)
            .validate_commit(&writer_plan, &writer_writes)
            .unwrap();

        assert!(outcome.rw_conflict_in);
        assert_eq!(outcome.active_conflict_count, 1);
        assert!(read_index.ssi_state(TxnId::new(100)).rw_conflict_out);
    }

    #[test]
    fn validator_rejects_dangerous_structure() {
        let read_index = Arc::new(ReadDependencyIndex::with_shards(4));
        let summaries = CommittedTxnSummaryIndex::with_shards(4);
        summaries
            .register_commit(crate::CommittedTxnSummary::new(
                TxnId::new(200),
                ReadTs::new(4),
                CommitTs::new(8),
                [table_resource(10)],
                &crate::FrozenReadSet::empty(),
            ))
            .unwrap();
        let (_reader_plan, _reader_writes) =
            serializable_plan(101, 5, Arc::clone(&read_index), Some(11), None);
        let (pivot_plan, pivot_writes) =
            serializable_plan(100, 5, Arc::clone(&read_index), Some(10), Some(11));

        let error = SsiValidator::new(&read_index, &summaries)
            .validate_commit(&pivot_plan, &pivot_writes)
            .unwrap_err();

        assert!(matches!(
            error,
            SsiValidationError::DangerousStructure { .. }
        ));
    }

    #[test]
    fn validator_marks_coarse_scan_marker_conflict() {
        let read_index = Arc::new(ReadDependencyIndex::with_shards(4));
        let summaries = CommittedTxnSummaryIndex::with_shards(4);
        summaries
            .register_commit(crate::CommittedTxnSummary::new(
                TxnId::new(200),
                ReadTs::new(4),
                CommitTs::new(8),
                [table_resource(10)],
                &crate::FrozenReadSet::empty(),
            ))
            .unwrap();
        let (_reader_plan, _reader_writes) =
            serializable_plan(101, 5, Arc::clone(&read_index), Some(11), None);
        let (pivot_plan, pivot_writes) = serializable_plan_with_policy(
            100,
            5,
            Arc::clone(&read_index),
            Some(10),
            Some(11),
            crate::ReadTrackingPolicy::AnalyticalScan,
        );

        let error = SsiValidator::new(&read_index, &summaries)
            .validate_commit(&pivot_plan, &pivot_writes)
            .unwrap_err();

        assert!(error.coarse_scan_marker_conflict());
    }
}
