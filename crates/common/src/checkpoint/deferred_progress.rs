// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

/// Stable durable identity for one deferred task family.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeferredTaskKey {
    pub task_kind: DeferredTaskKind,
    pub scope: DeferredTaskScope,
}

/// Durable task families currently known to checkpoint recovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeferredTaskKind {
    FinalizeIndexState,
}

/// Scope attached to one durable deferred task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeferredTaskScope {
    Object(u64),
    Tablet(u64),
    Global(String),
}

/// Checkpointed progress state for deferred work.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeferredTaskProgress {
    pub task_key: DeferredTaskKey,
    pub visible_lsn: u64,
    pub completed_lsn: Option<u64>,
    pub failed_lsn: Option<u64>,
    pub last_error: Option<String>,
}
