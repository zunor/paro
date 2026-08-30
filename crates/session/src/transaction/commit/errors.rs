// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! SQL-facing commit error mapping.

use paro_common::error::ParoError;
use paro_transaction::{
    CommitCompletionError, CommitRuntimeError, CommitRuntimeFailure, CommitRuntimeRejection,
};

#[derive(Debug)]
pub struct CommitFailure {
    pub error: ParoError,
    pub rollback_succeeded: bool,
}

pub(super) fn commit_runtime_error_to_paro(error: CommitRuntimeError) -> ParoError {
    match error {
        CommitRuntimeError::AdmissionClosed => paro_common::error::cannot_connect_now()
            .detail("commit runtime admission is closed for this database"),
        CommitRuntimeError::Poisoned(poison) => {
            paro_common::error::system_error("database commit runtime is poisoned")
                .detail(poison.to_string())
                .hint("inspect paro_commit_poison() and reopen the database after recovery")
        }
        CommitRuntimeError::Queue(error) => {
            paro_common::error::invalid_transaction_state(error.to_string())
        }
        CommitRuntimeError::Completion(error) => commit_completion_error_to_paro(error),
    }
}

/// Whether the runtime has proved that no durable commit record exists.
///
/// Callers may destroy pre-commit artifact sources only for these outcomes.
/// Apply failures, ambiguous outcomes, and unknown completion slots retain
/// their sources for recovery even when that means a temporary disk orphan.
pub(super) fn commit_runtime_error_is_definitely_nondurable(error: &CommitRuntimeError) -> bool {
    match error {
        CommitRuntimeError::AdmissionClosed
        | CommitRuntimeError::Queue(_)
        | CommitRuntimeError::Poisoned(_) => true,
        CommitRuntimeError::Completion(CommitCompletionError::Rejected(_)) => true,
        CommitRuntimeError::Completion(CommitCompletionError::Failed(
            CommitRuntimeFailure::Append(_) | CommitRuntimeFailure::AppendCleanup(_),
        )) => true,
        CommitRuntimeError::Completion(
            CommitCompletionError::Failed(_)
            | CommitCompletionError::AmbiguousCommitted(_)
            | CommitCompletionError::UnknownSlot(_),
        ) => false,
    }
}

fn commit_completion_error_to_paro(error: CommitCompletionError) -> ParoError {
    match error {
        CommitCompletionError::Rejected(CommitRuntimeRejection::Fence(reason)) => {
            paro_common::error::serialization_failure(format!(
                "commit rejected at ordered final fence: {reason:?}"
            ))
        }
        CommitCompletionError::Rejected(CommitRuntimeRejection::DrainBackpressure(reason)) => {
            paro_common::error::invalid_transaction_state(reason.to_string())
        }
        CommitCompletionError::Rejected(CommitRuntimeRejection::RuntimePoisoned(message)) => {
            paro_common::error::system_error("database commit runtime is poisoned")
                .detail(message.to_string())
                .hint("inspect paro_commit_poison() and reopen the database after recovery")
        }
        CommitCompletionError::Failed(CommitRuntimeFailure::Append(error)) => {
            paro_common::error::system_error("commit append failed").detail(error.to_string())
        }
        CommitCompletionError::Failed(error) => {
            paro_common::error::system_error("commit failed").detail(error.to_string())
        }
        CommitCompletionError::AmbiguousCommitted(error) => {
            paro_common::error::system_error("commit outcome is ambiguous after durable append")
                .detail(error.to_string())
                .hint(
                    "inspect paro_commit_poison() and recover the database before retrying writes",
                )
        }
        CommitCompletionError::UnknownSlot(handle) => paro_common::error::internal(format!(
            "unknown commit completion slot {}",
            handle.slot_id
        )),
    }
}
