// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bulk-load staged artifact commit participant.

use paro_common::effect::{
    ApplyDescriptor, BulkLoadRowsetArtifact, StorageCommitOp, TabletApplyOp, TabletMutation,
    VersionSpan,
};
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_transaction::{
    AbortReason, CommitParticipant, CommitPlan, DatabaseId, LockMode, LockNamespace, LockRequest,
    LockResource, ParticipantDescriptor, ParticipantId, ParticipantKind, PreparedCommitPart,
    PreparedParticipant, TableId, TransactionView, TxnResourceKey, ValidationContext,
};

#[derive(Debug, Clone)]
pub struct BulkLoadPreparedCommitPart {
    participant: PreparedParticipant,
    artifact: BulkLoadRowsetArtifact,
    storage_op: StorageCommitOp,
    apply_descriptor: ApplyDescriptor,
}

impl BulkLoadPreparedCommitPart {
    #[inline]
    fn new(
        participant: PreparedParticipant,
        artifact: BulkLoadRowsetArtifact,
        storage_op: StorageCommitOp,
        apply_descriptor: ApplyDescriptor,
    ) -> Self {
        Self {
            participant,
            artifact,
            storage_op,
            apply_descriptor,
        }
    }

    #[inline]
    pub fn artifact(&self) -> &BulkLoadRowsetArtifact {
        &self.artifact
    }

    #[inline]
    pub fn storage_op(&self) -> &StorageCommitOp {
        &self.storage_op
    }

    #[inline]
    pub fn apply_descriptor(&self) -> &ApplyDescriptor {
        &self.apply_descriptor
    }
}

impl PreparedCommitPart for BulkLoadPreparedCommitPart {
    #[inline]
    fn prepared_participant(&self) -> &PreparedParticipant {
        &self.participant
    }
}

#[derive(Debug, Clone)]
pub struct BulkLoadParticipant {
    database_id: DatabaseId,
    table_id: TableId,
    artifact: BulkLoadRowsetArtifact,
}

impl BulkLoadParticipant {
    pub const PARTICIPANT_ID: ParticipantId = ParticipantId::new(6);

    pub fn new(
        database_id: DatabaseId,
        table_id: TableId,
        artifact: BulkLoadRowsetArtifact,
    ) -> Result<Self> {
        if artifact.table_id != table_id.into_raw() {
            return Err(paro_error::invalid_input(format!(
                "bulk-load artifact table mismatch: descriptor={} participant={}",
                artifact.table_id, table_id
            )));
        }
        if artifact.row_count > 0 && artifact.byte_size == 0 {
            return Err(paro_error::invalid_input(
                "bulk-load artifact with rows must report non-zero bytes",
            ));
        }
        Ok(Self {
            database_id,
            table_id,
            artifact,
        })
    }

    #[inline]
    pub fn descriptor(&self) -> ParticipantDescriptor {
        ParticipantDescriptor::new(
            Self::PARTICIPANT_ID,
            ParticipantKind::BulkLoad,
            TxnResourceKey::table(ParticipantKind::BulkLoad, self.database_id, self.table_id),
        )
    }

    #[inline]
    pub fn required_locks(&self) -> Vec<LockRequest> {
        let namespace = LockNamespace::single_tenant(self.database_id);
        let mut locks = vec![
            LockRequest::new(
                LockResource::Table {
                    namespace,
                    table_id: self.table_id,
                },
                LockMode::IX,
            ),
            LockRequest::new(
                LockResource::Table {
                    namespace,
                    table_id: self.table_id,
                },
                LockMode::SchemaStability,
            ),
        ];
        if let Some((start_hash, end_hash)) = self.artifact.unique_summary.key_hash_range() {
            locks.push(LockRequest::new(
                LockResource::Range {
                    namespace,
                    table_id: self.table_id,
                    tablet_id: self.artifact.tablet_id,
                    start_hash,
                    end_hash,
                },
                LockMode::RangeX,
            ));
        }
        locks
    }

    fn storage_op(&self) -> StorageCommitOp {
        StorageCommitOp::Tablet(TabletApplyOp {
            tablet_id: self.artifact.tablet_id,
            mutations: vec![TabletMutation::PublishRowset {
                rowset_id: self.artifact.rowset_id,
                version_span: VersionSpan { start: 0, end: 0 },
                rowset_ref: self.artifact.rowset_ref.clone(),
            }],
        })
    }

    fn apply_descriptor(&self) -> ApplyDescriptor {
        ApplyDescriptor::PublishStagedArtifact(
            paro_common::effect::StagedArtifactDescriptor::BulkLoadRowset(self.artifact.clone()),
        )
    }

    fn validate_locks(&self, plan: &CommitPlan) -> Result<()> {
        let namespace = LockNamespace::single_tenant(self.database_id);
        let has_table_write = plan.lock_set.locks().iter().any(|lock| {
            matches!(
                &lock.resource,
                LockResource::Table {
                    namespace: lock_namespace,
                    table_id,
                } if *lock_namespace == namespace && *table_id == self.table_id
            ) && lock.mode.is_write_intent()
        });
        if !has_table_write {
            return Err(paro_error::invalid_transaction_state(
                "bulk-load participant requires a table write-intent lock",
            ));
        }

        let has_schema_guard = plan.lock_set.locks().iter().any(|lock| {
            matches!(
                &lock.resource,
                LockResource::Table {
                    namespace: lock_namespace,
                    table_id,
                } if *lock_namespace == namespace && *table_id == self.table_id
            ) && matches!(
                lock.mode,
                LockMode::SchemaStability | LockMode::SchemaModification | LockMode::X
            )
        });
        if !has_schema_guard {
            return Err(paro_error::invalid_transaction_state(
                "bulk-load participant requires a table schema-stability lock",
            ));
        }

        let Some((required_start, required_end)) = self.artifact.unique_summary.key_hash_range()
        else {
            return Ok(());
        };
        let has_conflict_guard = plan
            .lock_set
            .locks()
            .iter()
            .any(|lock| match &lock.resource {
                LockResource::Table {
                    namespace: lock_namespace,
                    table_id,
                } => {
                    *lock_namespace == namespace
                        && *table_id == self.table_id
                        && matches!(lock.mode, LockMode::X | LockMode::SchemaModification)
                }
                LockResource::Range {
                    namespace: lock_namespace,
                    table_id,
                    tablet_id,
                    start_hash,
                    end_hash,
                } => {
                    *lock_namespace == namespace
                        && *table_id == self.table_id
                        && *tablet_id == self.artifact.tablet_id
                        && *start_hash <= required_start
                        && *end_hash >= required_end
                        && matches!(lock.mode, LockMode::RangeX | LockMode::X)
                }
                _ => false,
            });
        if !has_conflict_guard {
            return Err(paro_error::invalid_transaction_state(
                "bulk-load participant requires a unique-summary conflict guard",
            ));
        }
        Ok(())
    }
}

impl CommitParticipant for BulkLoadParticipant {
    type Prepared = BulkLoadPreparedCommitPart;
    type Error = ParoError;

    fn prepare(&self, _view: &TransactionView) -> Result<Self::Prepared> {
        let storage_op = self.storage_op();
        let apply_descriptor = self.apply_descriptor();
        let prepared_bytes = self
            .artifact
            .byte_size
            .try_into()
            .unwrap_or(usize::MAX)
            .saturating_add(std::mem::size_of_val(&self.artifact));
        Ok(BulkLoadPreparedCommitPart::new(
            PreparedParticipant::new(self.descriptor(), true, prepared_bytes, 1),
            self.artifact.clone(),
            storage_op,
            apply_descriptor,
        ))
    }

    fn validate(&self, plan: &CommitPlan, _ctx: &ValidationContext) -> Result<()> {
        if plan.database_id != self.database_id {
            return Err(paro_error::invalid_transaction_state(format!(
                "bulk-load participant database mismatch: plan={} participant={}",
                plan.database_id, self.database_id
            )));
        }
        if !plan.contains_participant(&self.descriptor()) {
            return Err(paro_error::invalid_transaction_state(
                "bulk-load participant missing from commit plan",
            ));
        }
        if !self.artifact.unique_summary.is_conflict_free() {
            return Err(paro_error::unique_violation("bulk_load_unique_summary"));
        }
        if self.artifact.schema_epoch.is_none() {
            return Err(paro_error::invalid_transaction_state(
                "bulk-load artifact must carry schema epoch",
            ));
        }
        self.validate_locks(plan)
    }

    fn descriptor(&self, prepared: &Self::Prepared) -> Result<ParticipantDescriptor> {
        Ok(prepared.descriptor().clone())
    }

    fn abort(&self, _reason: AbortReason) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
    use paro_common::effect::{
        ArtifactNamespace, ArtifactRef, BulkLoadUniqueSummary, StagedArtifactDescriptor,
        StagingArtifactId,
    };
    use paro_transaction::{
        CommitAckPolicy, CommitRequest, FrozenLockSet, IsolationLevel, ParticipantStateSet,
        ReadSnapshot, ReadTrackerHandle, ReadTs, TxnId, WriterId,
    };

    fn artifact(unique_summary: BulkLoadUniqueSummary) -> BulkLoadRowsetArtifact {
        BulkLoadRowsetArtifact {
            table_object: DdlObjectKey::new("db", Some("public"), "items", DdlObjectKind::Table),
            table_id: 11,
            tablet_id: 42,
            rowset_id: 9001,
            staging: StagingArtifactId::new(
                77,
                vec!["bulk".to_string(), "rowset_9001".to_string()],
            ),
            rowset_ref: ArtifactRef {
                namespace: ArtifactNamespace::Staged,
                locator: vec!["bulk".to_string(), "rowset_9001".to_string()],
            },
            row_count: 4,
            byte_size: 4096,
            schema_epoch: Some(3),
            physical_schema_token: Some(8),
            unique_summary,
        }
    }

    fn request_with_locks(
        participant: &BulkLoadParticipant,
        locks: Vec<LockRequest>,
    ) -> CommitRequest {
        let view = TransactionView::new(
            WriterId::new(77),
            ReadTs::new(12),
            ReadSnapshot::without_lease(ReadTs::new(12)),
            IsolationLevel::Serializable,
            paro_transaction::CommandId::new(1),
            ReadTrackerHandle::noop(),
            ParticipantStateSet::empty(),
        );
        CommitRequest::new(
            DatabaseId::new(5),
            TxnId::new(77),
            view,
            CommitAckPolicy::RequiredPublished,
            FrozenLockSet::from_locks(locks),
            vec![participant.descriptor()],
        )
    }

    #[test]
    fn bulk_load_prepares_staged_rowset_descriptor() {
        let participant = BulkLoadParticipant::new(
            DatabaseId::new(5),
            TableId::new(11),
            artifact(BulkLoadUniqueSummary {
                key_count: 4,
                duplicate_key_count: 0,
                checksum_crc32c: 123,
                min_key_hash: Some(10),
                max_key_hash: Some(99),
            }),
        )
        .expect("participant");
        let request = request_with_locks(&participant, participant.required_locks());
        let prepared = participant
            .prepare(&request.transaction_view)
            .expect("prepare");

        assert_eq!(prepared.descriptor(), &participant.descriptor());
        match prepared.storage_op() {
            StorageCommitOp::Tablet(op) => {
                assert_eq!(op.tablet_id, 42);
                assert_eq!(op.mutations.len(), 1);
                assert!(matches!(
                    op.mutations[0],
                    TabletMutation::PublishRowset {
                        rowset_id: 9001,
                        ..
                    }
                ));
            }
        }
        assert!(matches!(
            prepared.apply_descriptor(),
            ApplyDescriptor::PublishStagedArtifact(StagedArtifactDescriptor::BulkLoadRowset(_))
        ));
        participant
            .validate(&request.commit_plan(), &request.validation_context())
            .expect("validate");
    }

    #[test]
    fn bulk_load_rejects_duplicates_and_missing_conflict_guard() {
        let participant = BulkLoadParticipant::new(
            DatabaseId::new(5),
            TableId::new(11),
            artifact(BulkLoadUniqueSummary {
                key_count: 4,
                duplicate_key_count: 1,
                checksum_crc32c: 123,
                min_key_hash: Some(10),
                max_key_hash: Some(99),
            }),
        )
        .expect("participant");
        let request = request_with_locks(&participant, participant.required_locks());
        let err = participant
            .validate(&request.commit_plan(), &request.validation_context())
            .expect_err("duplicate summary should fail");
        assert!(err.to_string().contains("bulk_load_unique_summary"));

        let participant = BulkLoadParticipant::new(
            DatabaseId::new(5),
            TableId::new(11),
            artifact(BulkLoadUniqueSummary {
                key_count: 4,
                duplicate_key_count: 0,
                checksum_crc32c: 123,
                min_key_hash: Some(10),
                max_key_hash: Some(99),
            }),
        )
        .expect("participant");
        let locks = participant
            .required_locks()
            .into_iter()
            .filter(|lock| !matches!(lock.resource, LockResource::Range { .. }))
            .collect();
        let request = request_with_locks(&participant, locks);
        let err = participant
            .validate(&request.commit_plan(), &request.validation_context())
            .expect_err("missing range lock should fail");
        assert!(err.to_string().contains("conflict guard"));
    }
}
