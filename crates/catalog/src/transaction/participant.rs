// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Live catalog commit participant.

use crate::collection::StagedCatalogMutation;
use crate::dependency::{DependencyDelta, DependencyGraph};
use paro_common::ddl::{DdlChangeRecord, DdlObjectKey};
use paro_common::effect::CatalogTxnOp;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_transaction::{
    AbortReason, CommitParticipant, CommitPlan, DatabaseId, ParticipantDescriptor, ParticipantId,
    ParticipantKind, PreparedCommitPart, PreparedParticipant, TxnResourceKey, ValidationContext,
};
use std::collections::HashSet;
use std::sync::Mutex;

pub const CATALOG_PARTICIPANT_ID: ParticipantId = ParticipantId::new(2);

#[derive(Debug)]
pub struct CatalogPreparedChange {
    pub record: DdlChangeRecord,
    pub catalog: Option<StagedCatalogMutation>,
    pub dependencies: Option<DependencyDelta>,
}

impl CatalogPreparedChange {
    #[inline]
    pub fn new(
        record: DdlChangeRecord,
        catalog: Option<StagedCatalogMutation>,
        dependencies: Option<DependencyDelta>,
    ) -> Self {
        Self {
            record,
            catalog,
            dependencies,
        }
    }

    #[inline]
    pub fn catalog_op(&self) -> CatalogTxnOp {
        CatalogTxnOp {
            change: self.record.clone(),
        }
    }

    pub fn publish(&mut self, commit_id: u64, graph: &DependencyGraph) -> Result<()> {
        if let Some(handle) = self.catalog.take() {
            handle.publish(commit_id)?;
        }
        if let Some(delta) = self.dependencies.take() {
            delta.publish(graph)?;
        }
        Ok(())
    }

    pub fn discard(mut self) -> Result<()> {
        if let Some(delta) = self.dependencies.take() {
            delta.discard();
        }
        if let Some(handle) = self.catalog.take() {
            handle.discard()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CatalogPreparedCommitPart {
    participant: PreparedParticipant,
    catalog_ops: Vec<CatalogTxnOp>,
    changes: Vec<CatalogPreparedChange>,
}

impl CatalogPreparedCommitPart {
    #[inline]
    fn new(participant: PreparedParticipant, changes: Vec<CatalogPreparedChange>) -> Self {
        let catalog_ops = changes
            .iter()
            .map(CatalogPreparedChange::catalog_op)
            .collect();
        Self {
            participant,
            catalog_ops,
            changes,
        }
    }

    #[inline]
    pub fn catalog_ops(&self) -> &[CatalogTxnOp] {
        &self.catalog_ops
    }

    #[inline]
    pub fn into_changes(self) -> Vec<CatalogPreparedChange> {
        self.changes
    }

    #[inline]
    pub fn changes_mut(&mut self) -> &mut [CatalogPreparedChange] {
        &mut self.changes
    }
}

impl PreparedCommitPart for CatalogPreparedCommitPart {
    #[inline]
    fn prepared_participant(&self) -> &PreparedParticipant {
        &self.participant
    }
}

#[derive(Debug)]
pub struct CatalogCommitParticipant {
    database_id: DatabaseId,
    change_count: usize,
    changes: Mutex<Option<Vec<CatalogPreparedChange>>>,
}

impl CatalogCommitParticipant {
    #[inline]
    pub fn new(database_id: DatabaseId, changes: Vec<CatalogPreparedChange>) -> Self {
        Self {
            database_id,
            change_count: changes.len(),
            changes: Mutex::new(Some(changes)),
        }
    }

    #[inline]
    pub fn participant_descriptor(database_id: DatabaseId) -> ParticipantDescriptor {
        ParticipantDescriptor::new(
            CATALOG_PARTICIPANT_ID,
            ParticipantKind::Catalog,
            TxnResourceKey::database(ParticipantKind::Catalog, database_id),
        )
    }

    #[inline]
    pub fn descriptor(&self) -> ParticipantDescriptor {
        Self::participant_descriptor(self.database_id)
    }

    fn validate_unique_keys(changes: &[CatalogPreparedChange]) -> Result<()> {
        let mut seen: HashSet<&DdlObjectKey> = HashSet::with_capacity(changes.len());
        for change in changes {
            if !seen.insert(&change.record.key) {
                return Err(paro_error::invalid_transaction_state(format!(
                    "catalog participant has duplicate change key: {:?}",
                    change.record.key
                )));
            }
        }
        Ok(())
    }
}

impl CommitParticipant for CatalogCommitParticipant {
    type Prepared = CatalogPreparedCommitPart;
    type Error = ParoError;

    fn prepare(&self, _view: &paro_transaction::TransactionView) -> Result<Self::Prepared> {
        let mut guard = self.changes.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock catalog participant changes: {e}"))
        })?;
        if let Some(changes) = guard.as_ref() {
            Self::validate_unique_keys(changes)?;
        }
        let changes = guard.take().ok_or_else(|| {
            paro_error::invalid_transaction_state("catalog participant already prepared or aborted")
        })?;

        let prepared_bytes =
            changes.len() * (std::mem::size_of::<DdlChangeRecord>() + std::mem::size_of::<usize>());
        let participant =
            PreparedParticipant::new(self.descriptor(), true, prepared_bytes, changes.len());
        Ok(CatalogPreparedCommitPart::new(participant, changes))
    }

    fn validate(&self, plan: &CommitPlan, _ctx: &ValidationContext) -> Result<()> {
        let descriptor = self.descriptor();
        if plan.database_id != self.database_id {
            return Err(paro_error::invalid_transaction_state(format!(
                "catalog participant database mismatch: plan={} participant={}",
                plan.database_id, self.database_id
            )));
        }
        if self.change_count > 0 && !plan.contains_participant(&descriptor) {
            return Err(paro_error::invalid_transaction_state(
                "catalog participant missing from commit plan",
            ));
        }
        Ok(())
    }

    fn descriptor(&self, prepared: &Self::Prepared) -> Result<ParticipantDescriptor> {
        Ok(prepared.descriptor().clone())
    }

    fn abort(&self, _reason: AbortReason) -> Result<()> {
        let mut guard = self.changes.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock catalog participant changes: {e}"))
        })?;
        if let Some(changes) = guard.take() {
            for change in changes.into_iter().rev() {
                change.discard()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::ddl::{CreateSchemaPayload, DdlChange, DdlObjectKind};
    use paro_transaction::{CommitRequest, FrozenLockSet, ReadTs, TransactionView, TxnId};

    fn change(name: &str, object_id: u64) -> CatalogPreparedChange {
        CatalogPreparedChange::new(
            DdlChangeRecord {
                key: DdlObjectKey::new("main", None::<String>, name, DdlObjectKind::Schema),
                change: DdlChange::CreateSchema(CreateSchemaPayload {
                    object_id,
                    if_not_exists: false,
                }),
            },
            None,
            None,
        )
    }

    #[test]
    fn catalog_participant_prepares_descriptor_and_ops_without_commit_ts() {
        let database_id = DatabaseId::new(7);
        let participant = CatalogCommitParticipant::new(database_id, vec![change("s1", 10)]);
        let request = CommitRequest::new(
            database_id,
            TxnId::new(11),
            TransactionView::autocommit(ReadTs::new(5)),
            paro_transaction::CommitAckPolicy::RequiredPublished,
            FrozenLockSet::empty(),
            vec![participant.descriptor()],
        );

        let plan = request.commit_plan();
        let ctx = request.validation_context();
        participant.validate(&plan, &ctx).unwrap();
        let prepared = participant.prepare(&request.transaction_view).unwrap();

        assert_eq!(prepared.descriptor().kind, ParticipantKind::Catalog);
        assert_eq!(prepared.write_count(), 1);
        assert_eq!(prepared.catalog_ops().len(), 1);
    }

    #[test]
    fn catalog_participant_rejects_duplicate_keys() {
        let participant = CatalogCommitParticipant::new(
            DatabaseId::new(7),
            vec![change("s1", 10), change("s1", 11)],
        );

        let error = participant
            .prepare(&TransactionView::autocommit(ReadTs::new(5)))
            .unwrap_err();
        assert!(error.to_string().contains("duplicate change key"));
    }
}
