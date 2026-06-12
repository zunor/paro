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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachedDatabaseTransactionMetricsSnapshot {
    pub txn_begin_count: u64,
    pub txn_begin_latency_us_total: u64,
    pub txn_begin_latency_us_peak: u64,
    pub txn_commit_count: u64,
    pub txn_commit_latency_us_total: u64,
    pub txn_commit_latency_us_peak: u64,
    pub txn_commit_prepare_latency_us_total: u64,
    pub txn_commit_prepare_latency_us_peak: u64,
    pub txn_commit_validate_latency_us_total: u64,
    pub txn_commit_validate_latency_us_peak: u64,
    pub group_commit_fence_us_total: u64,
    pub group_commit_fence_us_peak: u64,
    pub txn_commit_durable_latency_us_total: u64,
    pub txn_commit_durable_latency_us_peak: u64,
    pub commit_required_publish_wait_us_total: u64,
    pub commit_required_publish_wait_us_peak: u64,
    pub txn_commit_publish_latency_us_total: u64,
    pub txn_commit_publish_latency_us_peak: u64,
    pub commit_ack_mode: String,
    pub write_conflict_index_size: u64,
    pub write_conflict_index_fine_entries: u64,
    pub write_conflict_index_fine_summary_entries: u64,
    pub write_conflict_index_coarse_entries: u64,
    pub lock_wait_count: u64,
    pub lock_wait_duration_us: u64,
    pub lock_wound_wait_abort_count: u64,
    pub lock_deadlock_abort_count: u64,
    pub durable_published_lag_commits: u64,
    pub durable_published_lag_ms: u64,
    pub backpressure_throttle_count: u64,
    pub ssi_validation_abort_count: u64,
    pub ssi_abort_due_to_coarse_scan_marker: u64,
    pub read_tracker_record_count: u64,
    pub read_tracker_coarsened_count: u64,
    pub read_tracking_hint_count: u64,
    pub read_tracking_policy_escalation_count: u64,
    pub read_tracking_point_critical_count: u64,
    pub read_tracking_range_critical_count: u64,
    pub read_tracking_analytical_scan_count: u64,
    pub read_tracking_safe_snapshot_preferred_count: u64,
    pub derived_index_lag_ts: u64,
    pub tail_exact_merge_cost: u64,
    pub commit_participant_count: u64,
    pub inflight_batch_conflict_reject_count: u64,
    pub retention_watermark_lag_ms: u64,
    pub oldest_active_rw_lag_ms: u64,
    pub read_snapshot_lease_count: u64,
    pub active_rw_txn_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachedDatabaseCommitFrontierSnapshot {
    pub durable_commit_id: u64,
    pub published_commit_id: u64,
    pub durable_commit_bytes: u64,
    pub published_commit_bytes: u64,
    pub durable_to_published_bytes_lag: Option<u64>,
    pub stale_bytes_at_poison: Option<u64>,
    pub publish_failure_watermark: Option<u64>,
    pub publish_failure_cause: Option<String>,
    pub wait_count: u64,
    pub wait_wake_count: u64,
    pub notify_all_count: u64,
    pub notify_suppressed_count: u64,
    pub publish_failure_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedDatabaseCommitPoisonSnapshot {
    pub admission_state: String,
    pub admission_open: bool,
    pub poisoned: bool,
    pub poison_cause: Option<String>,
    pub first_blocked_commit_ts: Option<u64>,
}

impl Default for AttachedDatabaseCommitPoisonSnapshot {
    fn default() -> Self {
        Self {
            admission_state: "open".to_string(),
            admission_open: true,
            poisoned: false,
            poison_cause: None,
            first_blocked_commit_ts: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AttachedDatabaseSnapshot {
    pub identity: DatabaseSnapshotIdentity,
    pub catalog: Arc<ParoCatalog>,
    pub tablet_meta: Option<Arc<TabletMetaManager>>,
    pub wal_metrics: AttachedDatabaseWalMetricsSnapshot,
    pub transaction_metrics: AttachedDatabaseTransactionMetricsSnapshot,
    pub commit_frontier: AttachedDatabaseCommitFrontierSnapshot,
    pub commit_poison: AttachedDatabaseCommitPoisonSnapshot,
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

    pub fn transaction_metrics(&self) -> &AttachedDatabaseTransactionMetricsSnapshot {
        &self.transaction_metrics
    }

    pub fn commit_frontier(&self) -> &AttachedDatabaseCommitFrontierSnapshot {
        &self.commit_frontier
    }

    pub fn commit_poison(&self) -> &AttachedDatabaseCommitPoisonSnapshot {
        &self.commit_poison
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
