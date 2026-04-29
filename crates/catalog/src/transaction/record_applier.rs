// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable catalog committed-record applier.

use super::participant::CatalogPreparedCommitPart;
use crate::database_catalog::ParoCatalog;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_transaction::{
    CommittedRecordApplier, CommittedTxnRecord, DatabaseId, ParticipantDescriptor, ParticipantKind,
    PublishResult,
};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct CatalogCommittedRecordApplier {
    database_id: DatabaseId,
    catalog: Arc<ParoCatalog>,
    prepared: Mutex<Option<CatalogPreparedCommitPart>>,
}

impl CatalogCommittedRecordApplier {
    #[inline]
    pub fn new(
        database_id: DatabaseId,
        catalog: Arc<ParoCatalog>,
        prepared: CatalogPreparedCommitPart,
    ) -> Self {
        Self {
            database_id,
            catalog,
            prepared: Mutex::new(Some(prepared)),
        }
    }

    pub fn abort_prepared(&self) -> Result<()> {
        let mut guard = self.prepared.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock prepared catalog part: {e}"))
        })?;
        if let Some(prepared) = guard.take() {
            for change in prepared.into_changes().into_iter().rev() {
                change.discard()?;
            }
        }
        Ok(())
    }
}

impl CommittedRecordApplier for CatalogCommittedRecordApplier {
    type Error = ParoError;

    fn applies_to(&self, descriptor: &ParticipantDescriptor) -> bool {
        descriptor.kind == ParticipantKind::Catalog
    }

    fn apply_required(
        &self,
        record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> Result<PublishResult> {
        if !self.applies_to(descriptor) {
            return Err(paro_error::invalid_transaction_state(
                "catalog applier received non-catalog descriptor",
            ));
        }
        record.validate_versions().map_err(|err| {
            paro_error::invalid_transaction_state(format!(
                "catalog applier rejected committed record: {err}"
            ))
        })?;
        if record.database_id != self.database_id {
            return Err(paro_error::invalid_transaction_state(format!(
                "catalog applier database mismatch: record={} applier={}",
                record.database_id, self.database_id
            )));
        }

        let mut guard = self.prepared.lock().map_err(|e| {
            paro_error::internal(format!("failed to lock prepared catalog part: {e}"))
        })?;
        // Idempotency: `prepared` is cleared only after every catalog change has
        // published successfully. If a publish attempt fails after consuming part
        // of a change, retry resumes from the remaining staged handles instead of
        // treating the record as already applied.
        let Some(prepared) = guard.as_mut() else {
            return Ok(PublishResult::required(record.commit_ts));
        };
        for change in prepared.changes_mut() {
            change.publish(record.commit_ts.into_raw(), self.catalog.dependency_graph())?;
        }
        *guard = None;
        Ok(PublishResult::required(record.commit_ts))
    }

    fn apply_deferred(
        &self,
        _record: &CommittedTxnRecord,
        descriptor: &ParticipantDescriptor,
    ) -> Result<PublishResult> {
        if self.applies_to(descriptor) {
            return Err(paro_error::invalid_transaction_state(
                "catalog participant is required and cannot be deferred",
            ));
        }
        Ok(PublishResult::deferred())
    }
}
