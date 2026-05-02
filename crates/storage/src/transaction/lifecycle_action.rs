// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Storage-owned lifecycle actions handed to the commit runtime.

use crate::transaction::manager::TransactionManager;
use crate::transaction::txn::Transaction;
use paro_transaction::{
    AppendFailureRollbackPlan, ApplyErrorSource, ApplyPhase, DurableCommitHandle,
    JournalApplyError, LockReleasePlan, PostApplyFinalizePlan, PrePublishReleasePlan,
};
use std::sync::Arc;

pub fn lock_release_plan(transaction: Arc<Transaction>) -> LockReleasePlan {
    LockReleasePlan::new(move || transaction.release_transaction_locks())
}

pub fn pre_publish_release_plan(
    manager: Arc<TransactionManager>,
    transaction: Arc<Transaction>,
) -> PrePublishReleasePlan {
    PrePublishReleasePlan::new(move || manager.release_pre_publish_lifecycle(&transaction))
}

pub fn post_apply_finalize_plan(
    manager: Arc<TransactionManager>,
    transaction: Arc<Transaction>,
) -> PostApplyFinalizePlan {
    PostApplyFinalizePlan::new(move |handle| finalize_and_enqueue(&manager, &transaction, handle))
}

pub fn append_failure_rollback_plan(transaction: Arc<Transaction>) -> AppendFailureRollbackPlan {
    AppendFailureRollbackPlan::new(move || {
        transaction.rollback_prepared_storage_only();
        Ok(())
    })
}

fn finalize_and_enqueue(
    manager: &TransactionManager,
    transaction: &Arc<Transaction>,
    handle: &DurableCommitHandle,
) -> Result<(), JournalApplyError> {
    let commit_id = handle.commit_ts().into_raw();
    transaction
        .finalize_applied_commit(commit_id)
        .map_err(|error| {
            JournalApplyError::apply_failed(
                ApplyPhase::Published,
                ApplyErrorSource::PublishedHook,
                handle.durable_lsn(),
                Some(commit_id),
                &error,
            )
        })?;
    manager.enqueue_finalized_transaction_cleanup(transaction, transaction.changes_made());
    Ok(())
}
