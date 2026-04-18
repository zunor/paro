// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable journal record schema shared across runtime append and recovery.

use crate::effect::{ApplyDescriptor, CatalogTxnOp, DeferredTask, StorageCommitOp};
use serde::{Deserialize, Serialize};

/// Journal frame schema version used by the binary codec.
pub const JOURNAL_FORMAT_VERSION: u16 = 3;

/// One durable journal record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JournalRecord {
    Commit(CommitRecord),
    Maintenance(MaintenanceRecord),
    CheckpointFence(CheckpointFence),
}

/// Durable transaction commit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub txn_id: u64,
    pub start_time: u64,
    pub commit_id: u64,
    pub catalog_ops: Vec<CatalogTxnOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub apply_descriptors: Vec<ApplyDescriptor>,
    pub deferred_tasks: Vec<DeferredTask>,
}

/// Durable maintenance record.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceRecord {
    pub maintenance_id: u64,
    pub kind: MaintenanceKind,
    pub catalog_ops: Vec<CatalogTxnOp>,
    pub storage_ops: Vec<StorageCommitOp>,
    pub apply_descriptors: Vec<ApplyDescriptor>,
    pub deferred_tasks: Vec<DeferredTask>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceKind {
    Compaction,
    IndexBackfill,
    MaterializedViewRefresh,
}

/// Checkpoint fence carried by the journal stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointFence {
    pub checkpoint_marker: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoverySummary {
    pub max_lsn: u64,
    pub max_commit_id: u64,
    pub max_maintenance_id: u64,
    pub max_catalog_commit_id: u64,
    pub max_seen_object_id: u64,
}
