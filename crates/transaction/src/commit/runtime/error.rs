// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit runtime failure, poison, and completion error types.

use super::super::{
    AppendCommitError, CommitDrainBackpressure, CommitFenceRejectReason, CommitQueueError,
    JournalApplyError,
};
use crate::types::CommitTs;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CommitRuntimeError {
    AdmissionClosed,
    Queue(CommitQueueError),
    Completion(CommitCompletionError),
    Poisoned(CommitRuntimePoison),
}

impl fmt::Display for CommitRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdmissionClosed => write!(f, "commit runtime admission is closed"),
            Self::Queue(error) => write!(f, "commit queue error: {error}"),
            Self::Completion(error) => write!(f, "commit completion error: {error}"),
            Self::Poisoned(poison) => write!(f, "commit runtime poisoned: {poison}"),
        }
    }
}

impl std::error::Error for CommitRuntimeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRuntimePoison {
    DurableProtocol {
        message: Arc<str>,
    },
    FinalizeSchedule {
        message: Arc<str>,
    },
    FinalizeStage {
        commit_ts: Option<CommitTs>,
        message: Arc<str>,
    },
    Submit {
        commit_ts: CommitTs,
        message: Arc<str>,
    },
    Apply {
        commit_ts: CommitTs,
        message: Arc<str>,
    },
    CompletionPanic {
        commit_ts: CommitTs,
        message: Arc<str>,
    },
    AppendCleanup {
        message: Arc<str>,
    },
    Recovery {
        message: Arc<str>,
    },
}

impl fmt::Display for CommitRuntimePoison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurableProtocol { message } => write!(f, "durable protocol violation: {message}"),
            Self::FinalizeSchedule { message } => write!(f, "finalize schedule failed: {message}"),
            Self::FinalizeStage { commit_ts, message } => match commit_ts {
                Some(commit_ts) => {
                    write!(f, "commit-finalize stage failed at {commit_ts}: {message}")
                }
                None => write!(f, "commit-finalize stage failed: {message}"),
            },
            Self::Submit { commit_ts, message } => {
                write!(f, "publish submit failed at {commit_ts}: {message}")
            }
            Self::Apply { commit_ts, message } => {
                write!(f, "publish apply failed at {commit_ts}: {message}")
            }
            Self::CompletionPanic { commit_ts, message } => {
                write!(f, "completion callback panic at {commit_ts}: {message}")
            }
            Self::AppendCleanup { message } => write!(f, "append cleanup failed: {message}"),
            Self::Recovery { message } => write!(f, "recovery replay failed: {message}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRuntimeRejection {
    Fence(CommitFenceRejectReason),
    DrainBackpressure(CommitDrainBackpressure),
    RuntimePoisoned(Arc<str>),
}

impl fmt::Display for CommitRuntimeRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fence(reason) => write!(f, "commit fence rejected: {reason:?}"),
            Self::DrainBackpressure(error) => write!(f, "commit drain backpressure: {error}"),
            Self::RuntimePoisoned(message) => write!(f, "commit runtime poisoned: {message}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CommitRuntimeFailure {
    Append(AppendCommitError),
    AppendCleanup(Arc<str>),
    Apply(JournalApplyError),
    CompletionPanic(JournalApplyError),
    Ambiguous(Arc<str>),
}

impl fmt::Display for CommitRuntimeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Append(error) => write!(f, "append failed: {error}"),
            Self::AppendCleanup(message) => write!(f, "append cleanup failed: {message}"),
            Self::Apply(error) => write!(f, "apply failed: {error}"),
            Self::CompletionPanic(error) => write!(f, "completion callback panic: {error}"),
            Self::Ambiguous(message) => write!(f, "ambiguous committed: {message}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CommitCompletionError {
    Rejected(CommitRuntimeRejection),
    Failed(CommitRuntimeFailure),
    AmbiguousCommitted(CommitRuntimeFailure),
    UnknownSlot(super::super::CommitCompletionHandle),
}

impl fmt::Display for CommitCompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(reason) => write!(f, "commit rejected: {reason}"),
            Self::Failed(error) => write!(f, "commit failed: {error}"),
            Self::AmbiguousCommitted(error) => write!(f, "commit ambiguous: {error}"),
            Self::UnknownSlot(handle) => write!(f, "unknown commit completion slot {:?}", handle),
        }
    }
}

impl std::error::Error for CommitCompletionError {}
