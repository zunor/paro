// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ordered WAL recovery replay contracts for commit runtime bootstrap.

use super::{DurableCommitHandle, RequiredPublishPlan};
use crate::types::CommitTs;
pub use paro_journal::RecoveryPlaceholderRecordKind;
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
pub enum RecoveryReplayEvent {
    Commit(RecoveryReplayCommit),
    Placeholder {
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
    },
}

#[derive(Debug)]
pub struct RecoveryReplayCommit {
    pub handle: DurableCommitHandle,
    pub required_publish: RequiredPublishPlan,
}

impl RecoveryReplayCommit {
    pub fn new(handle: DurableCommitHandle, required_publish: RequiredPublishPlan) -> Self {
        Self {
            handle,
            required_publish,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReplaySummary {
    pub commits: u64,
    pub placeholders: u64,
    pub max_lsn_seen: u64,
    pub max_commit_ts_seen: CommitTs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryReplayError {
    RuntimePoisoned(super::CommitRuntimePoison),
    CommitOrder {
        previous_commit_ts: CommitTs,
        current_commit_ts: CommitTs,
    },
    CommitLsnGap {
        commit_ts: CommitTs,
        durable_lsn: u64,
        next_dispatch_lsn: u64,
    },
    BuildRequest {
        commit_ts: CommitTs,
        durable_lsn: u64,
        message: Arc<str>,
    },
    Placeholder {
        lsn: u64,
        record_kind: RecoveryPlaceholderRecordKind,
        message: Arc<str>,
    },
    Apply {
        commit_ts: CommitTs,
        durable_lsn: u64,
        message: Arc<str>,
    },
    IncompleteFrontier {
        durable_commit_id: CommitTs,
        published_commit_id: CommitTs,
    },
}

impl fmt::Display for RecoveryReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimePoisoned(poison) => write!(f, "commit runtime poisoned: {poison}"),
            Self::CommitOrder {
                previous_commit_ts,
                current_commit_ts,
            } => write!(
                f,
                "recovery commit order regressed: previous={previous_commit_ts} current={current_commit_ts}"
            ),
            Self::CommitLsnGap {
                commit_ts,
                durable_lsn,
                next_dispatch_lsn,
            } => write!(
                f,
                "recovery commit {commit_ts} at lsn {durable_lsn} cannot skip next dispatch lsn {next_dispatch_lsn}"
            ),
            Self::BuildRequest {
                commit_ts,
                durable_lsn,
                message,
            } => write!(
                f,
                "recovery build apply request failed at {commit_ts} lsn {durable_lsn}: {message}"
            ),
            Self::Placeholder {
                lsn,
                record_kind,
                message,
            } => write!(
                f,
                "recovery placeholder {record_kind} at lsn {lsn} failed: {message}"
            ),
            Self::Apply {
                commit_ts,
                durable_lsn,
                message,
            } => write!(
                f,
                "recovery apply failed at {commit_ts} lsn {durable_lsn}: {message}"
            ),
            Self::IncompleteFrontier {
                durable_commit_id,
                published_commit_id,
            } => write!(
                f,
                "recovery ended before publish caught durable frontier: durable={durable_commit_id} published={published_commit_id}"
            ),
        }
    }
}

impl std::error::Error for RecoveryReplayError {}
