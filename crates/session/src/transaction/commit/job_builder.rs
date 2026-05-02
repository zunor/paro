// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit request and prepared-job construction helpers.

use super::super::ddl_changes::PreparedCatalogOp;
use crate::session::Session;
use paro_catalog::transaction::CatalogCommitParticipant;
use paro_storage::transaction::txn::Transaction;
use paro_transaction::{CommitRequest, DatabaseId, TransactionView};
use std::sync::Arc;

pub(super) fn build_commit_request(
    session: &Session,
    active: &Arc<Transaction>,
    ddl_changes: &[PreparedCatalogOp],
    transaction_view: TransactionView,
) -> CommitRequest {
    let database_id = DatabaseId::new(session.current_database.id());
    let mut participants = active.participant_descriptors();
    if !ddl_changes.is_empty() {
        participants.push(CatalogCommitParticipant::participant_descriptor(
            database_id,
        ));
    }
    CommitRequest::new(
        database_id,
        active.txn_id(),
        transaction_view,
        session.commit_ack_policy(),
        active.frozen_lock_set(),
        participants,
    )
}
