// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage commit participant and transaction-record applier.

use crate::transaction::manager::TransactionManager;
use crate::transaction::txn::{PreparedStorageCommit, Transaction};
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_transaction::{
    AbortReason, CommitParticipant, CommitPlan, CommittedRecordApplier, CommittedTxnRecord,
    DatabaseId, ParticipantDescriptor, ParticipantId, ParticipantKind, PreparedCommitPart,
    PreparedParticipant, PublishResult, TransactionView, TxnResourceKey, ValidationContext,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StoragePreparedCommitPart {
    participant: PreparedParticipant,
    commit: PreparedStorageCommit,
}

impl StoragePreparedCommitPart {
    #[inline]
    pub fn new(participant: PreparedParticipant, commit: PreparedStorageCommit) -> Self {
        Self {
            participant,
            commit,
        }
    }

    #[inline]
    pub fn commit(&self) -> &PreparedStorageCommit {
        &self.commit
    }
}

impl PreparedCommitPart for StoragePreparedCommitPart {
    #[inline]
    fn prepared_participant(&self) -> &PreparedParticipant {
        &self.participant
    }
}

#[derive(Debug, Clone)]
pub struct StorageCommitParticipant {
    database_id: DatabaseId,
    transaction: Arc<Transaction>,
}

impl StorageCommitParticipant {
    pub const PARTICIPANT_ID: ParticipantId = ParticipantId::new(1);

    #[inline]
    pub fn new(database_id: DatabaseId, transaction: Arc<Transaction>) -> Self {
        Self {
            database_id,
            transaction,
        }
    }

    #[inline]
    pub fn descriptor(&self) -> ParticipantDescriptor {
        ParticipantDescriptor::new(
            Self::PARTICIPANT_ID,
            ParticipantKind::Storage,
            TxnResourceKey::database(ParticipantKind::Storage, self.database_id),
        )
    }
}

impl CommitParticipant for StorageCommitParticipant {
    type Prepared = StoragePreparedCommitPart;
    type Error = ParoError;

    fn prepare(&self, _view: &TransactionView) -> Result<Self::Prepared> {
        let commit = self.transaction.prepare_commit()?;
        let write_count = commit
            .storage_ops
            .iter()
            .map(|op| match op {
                paro_common::effect::StorageCommitOp::Tablet(tablet) => tablet.mutations.len(),
            })
            .sum();
        let prepared_bytes = commit
            .storage_ops
            .iter()
            .map(std::mem::size_of_val)
            .sum::<usize>();
        Ok(StoragePreparedCommitPart::new(
            PreparedParticipant::new(self.descriptor(), true, prepared_bytes, write_count),
            commit,
        ))
    }

    fn validate(&self, plan: &CommitPlan, ctx: &ValidationContext) -> Result<()> {
        let descriptor = self.descriptor();
        if plan.database_id != self.database_id {
            return Err(paro_error::invalid_transaction_state(format!(
                "storage participant database mismatch: plan={} participant={}",
                plan.database_id, self.database_id
            )));
        }
        if plan.txn_id != self.transaction.txn_id() {
            return Err(paro_error::invalid_transaction_state(format!(
                "storage participant txn mismatch: plan={} participant={}",
                plan.txn_id,
                self.transaction.txn_id()
            )));
        }
        let visible_read_ts =
            paro_transaction::ReadTs::new(self.transaction.visible_commit_ts().into_raw());
        if ctx.read_ts != visible_read_ts {
            return Err(paro_error::invalid_transaction_state(format!(
                "storage participant read_ts mismatch: ctx={} participant={}",
                ctx.read_ts, visible_read_ts
            )));
        }
        if !plan.contains_participant(&descriptor) && self.transaction.has_pending_storage_work() {
            return Err(paro_error::invalid_transaction_state(
                "storage participant missing from commit plan",
            ));
        }
        Ok(())
    }

    fn descriptor(&self, prepared: &Self::Prepared) -> Result<ParticipantDescriptor> {
        Ok(prepared.descriptor().clone())
    }

    fn abort(&self, _reason: AbortReason) -> Result<()> {
        self.transaction.abort_prepared_storage();
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StorageCommittedRecordApplier {
    transaction: Arc<Transaction>,
}

impl StorageCommittedRecordApplier {
    #[inline]
    pub fn new(_manager: Arc<TransactionManager>, transaction: Arc<Transaction>) -> Self {
        Self { transaction }
    }
}

impl CommittedRecordApplier for StorageCommittedRecordApplier {
    type Error = ParoError;

    fn applies_to(&self, descriptor: &ParticipantDescriptor) -> bool {
        descriptor.kind == ParticipantKind::Storage
    }

    fn apply_required(
        &self,
        record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> Result<PublishResult> {
        if !self.applies_to(descriptor) {
            return Err(paro_error::invalid_transaction_state(
                "storage applier received non-storage descriptor",
            ));
        }
        record.validate_versions().map_err(|err| {
            paro_error::invalid_transaction_state(format!(
                "storage applier rejected committed record: {err}"
            ))
        })?;
        if record.txn_id != self.transaction.txn_id() {
            return Err(paro_error::invalid_transaction_state(format!(
                "storage applier txn mismatch: record={} transaction={}",
                record.txn_id,
                self.transaction.txn_id()
            )));
        }
        // Live-path idempotency: once the prepared transaction has been published,
        // a duplicate delivery for the same in-memory transaction is a no-op.
        // Recovery from durable records cannot rely on this live transaction handle;
        // T077b/T132a track mutation-identity replay for that path.
        if self.transaction.is_awaiting_cleanup() {
            return Ok(PublishResult::required(record.commit_ts));
        }
        self.transaction
            .apply_prepared_storage_for_commit(record.commit_ts.into_raw())?;
        Ok(PublishResult::required(record.commit_ts))
    }

    fn apply_deferred(
        &self,
        _record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> Result<PublishResult> {
        if self.applies_to(descriptor) {
            return Err(paro_error::invalid_transaction_state(
                "storage participant is required and cannot be deferred",
            ));
        }
        Ok(PublishResult::deferred())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_transaction::{
        CommitAckPolicy, CommitRequest, FrozenLockSet, ReadSnapshot, ReadTrackerHandle, ReadTs,
        TxnId,
    };

    #[test]
    fn storage_participant_prepares_and_applies_empty_transaction() {
        let manager = Arc::new(TransactionManager::new_for_database_id(7));
        let transaction = manager.begin_transaction().unwrap();
        let database_id = DatabaseId::new(7);
        let participant = StorageCommitParticipant::new(database_id, Arc::clone(&transaction));

        let view = TransactionView::new(
            transaction.writer_id(),
            transaction.read_ts(),
            ReadSnapshot::without_lease(ReadTs::new(transaction.visible_commit_ts().into_raw())),
            paro_transaction::IsolationLevel::Snapshot,
            paro_transaction::CommandId::new(0),
            ReadTrackerHandle::noop(),
            paro_transaction::ParticipantStateSet::from_vec(vec![
                transaction.storage_participant_state()
            ]),
        );
        let request = CommitRequest::new(
            database_id,
            TxnId::new(transaction.id),
            view,
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![participant.descriptor()],
        );
        let plan = request.commit_plan();
        let ctx = request.validation_context();

        let prepared = participant.prepare(&request.transaction_view).unwrap();
        participant.validate(&plan, &ctx).unwrap();
        assert_eq!(prepared.write_count(), 0);

        let record = request.committed_record(paro_transaction::CommitTs::new(3));
        let applier =
            StorageCommittedRecordApplier::new(Arc::clone(&manager), Arc::clone(&transaction));
        let result = applier
            .apply_required(&record, prepared.descriptor())
            .unwrap();
        assert_eq!(
            result.published_ts,
            Some(paro_transaction::CommitTs::new(3))
        );
    }
}
