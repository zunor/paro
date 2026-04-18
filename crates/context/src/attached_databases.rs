// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::database_catalog::ParoCatalog;
use paro_common::identity::DatabaseType;
use paro_storage::meta::TabletMetaManager;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSnapshotIdentity {
    pub id: u64,
    pub name: String,
    pub path: String,
    pub db_type: DatabaseType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachedDatabaseWalMetricsSnapshot {
    pub checkpoint_success_total: u64,
    pub checkpoint_failure_total: u64,
    pub wal_health_check_total: u64,
    pub wal_keep_from: u64,
    pub recovery_mode: String,
    pub main_wal_needs_truncation: bool,
    pub checkpoint_wal_needs_truncation: bool,
    pub recovery_wal_needs_truncation: bool,
    pub journal_apply_queue_depth: u64,
    pub journal_apply_queue_depth_peak: u64,
    pub journal_apply_active_workers: u64,
    pub journal_apply_active_workers_peak: u64,
    pub journal_apply_mailbox_count: u64,
    pub journal_apply_applied_lag: u64,
    pub journal_apply_published_lag: u64,
    pub journal_apply_durable_wait_count: u64,
    pub journal_apply_durable_wait_micros: u64,
    pub journal_apply_applied_wait_count: u64,
    pub journal_apply_applied_wait_micros: u64,
    pub journal_apply_published_wait_count: u64,
    pub journal_apply_published_wait_micros: u64,
    pub journal_commit_bytes_total: u64,
    pub journal_group_count: u64,
    pub journal_group_size_last: u64,
    pub journal_group_size_peak: u64,
    pub journal_sync_latency_micros_total: u64,
    pub journal_sync_latency_micros_peak: u64,
    pub journal_replay_rowsets_total: u64,
    pub journal_replay_delete_patches_total: u64,
    pub journal_inline_delete_patch_count: u64,
    pub journal_delete_patch_count: u64,
}

#[derive(Debug, Clone)]
pub struct AttachedDatabaseSnapshot {
    pub identity: DatabaseSnapshotIdentity,
    pub catalog: Arc<ParoCatalog>,
    pub tablet_meta: Option<Arc<TabletMetaManager>>,
    pub wal_metrics: AttachedDatabaseWalMetricsSnapshot,
}

impl AttachedDatabaseSnapshot {
    pub fn id(&self) -> u64 {
        self.identity.id
    }

    pub fn name(&self) -> &str {
        &self.identity.name
    }

    pub fn path(&self) -> &str {
        &self.identity.path
    }

    pub fn db_type(&self) -> DatabaseType {
        self.identity.db_type
    }

    pub fn catalog(&self) -> &Arc<ParoCatalog> {
        &self.catalog
    }

    pub fn tablet_meta_manager(&self) -> Option<Arc<TabletMetaManager>> {
        self.tablet_meta.clone()
    }

    pub fn wal_metrics(&self) -> &AttachedDatabaseWalMetricsSnapshot {
        &self.wal_metrics
    }
}

#[derive(Debug, Clone, Default)]
pub struct AttachedDatabaseDirectory {
    pub visible_generation: u64,
    ordered: Arc<[AttachedDatabaseSnapshot]>,
    by_name: HashMap<String, usize>,
    current_database: Option<String>,
}

impl AttachedDatabaseDirectory {
    pub fn new(
        visible_generation: u64,
        current_database: Option<String>,
        ordered: Vec<AttachedDatabaseSnapshot>,
    ) -> Self {
        let mut by_name = HashMap::with_capacity(ordered.len());
        for (index, database) in ordered.iter().enumerate() {
            by_name.insert(database.identity.name.to_ascii_lowercase(), index);
        }
        Self {
            visible_generation,
            ordered: ordered.into(),
            by_name,
            current_database,
        }
    }

    pub fn get(&self, name: &str) -> Option<&AttachedDatabaseSnapshot> {
        let index = self.by_name.get(&name.to_ascii_lowercase())?;
        self.ordered.get(*index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &AttachedDatabaseSnapshot> {
        self.ordered.iter()
    }

    pub fn current_database_snapshot(&self) -> Option<&AttachedDatabaseSnapshot> {
        self.current_database
            .as_deref()
            .and_then(|name| self.get(name))
    }
}
