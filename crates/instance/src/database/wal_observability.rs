// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::checkpoint::coordinator::CheckpointCoordinator;
use crate::storage_manager::StorageManager;
use parking_lot::RwLock;
use paro_catalog::database_catalog::ParoCatalog;
use paro_common::logging::targets;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Per-database WAL lifecycle observability snapshot for instance-level aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalLifecycleMetricsSnapshot {
    pub checkpoint_success_total: u64,
    pub checkpoint_failure_total: u64,
    pub wal_health_check_total: u64,
    pub wal_keep_from: u64,
    pub recovery_mode: paro_journal::wal::recovery::WalRecoveryMode,
    pub main_wal_needs_truncation: bool,
}

fn wal_recovery_mode_from_metric(value: u64) -> paro_journal::wal::recovery::WalRecoveryMode {
    match value {
        1 => paro_journal::wal::recovery::WalRecoveryMode::NoWal,
        2 => paro_journal::wal::recovery::WalRecoveryMode::MainWalOnly,
        _ => paro_journal::wal::recovery::WalRecoveryMode::Unknown,
    }
}

/// WAL-facing diagnostics and recovery observability.
pub struct WalObservability {
    wal_keep_from: AtomicU64,
    wal_health_check_total: AtomicU64,
    wal_recovery_mode_metric: AtomicU64,
    main_wal_needs_truncation: AtomicBool,
    last_recovery_report:
        RwLock<Option<crate::recovery::consistency_report::RecoveryConsistencyReport>>,
}

impl WalObservability {
    pub fn new() -> Self {
        Self {
            wal_keep_from: AtomicU64::new(u64::MAX),
            wal_health_check_total: AtomicU64::new(0),
            wal_recovery_mode_metric: AtomicU64::new(
                paro_journal::wal::recovery::WalRecoveryMode::Unknown.as_metric_value(),
            ),
            main_wal_needs_truncation: AtomicBool::new(false),
            last_recovery_report: RwLock::new(None),
        }
    }

    pub fn snapshot(&self, coordinator: &CheckpointCoordinator) -> WalLifecycleMetricsSnapshot {
        WalLifecycleMetricsSnapshot {
            checkpoint_success_total: coordinator.checkpoint_success_total(),
            checkpoint_failure_total: coordinator.checkpoint_failure_total(),
            wal_health_check_total: self.wal_health_check_total.load(Ordering::Relaxed),
            wal_keep_from: self.wal_keep_from.load(Ordering::Acquire),
            recovery_mode: wal_recovery_mode_from_metric(
                self.wal_recovery_mode_metric.load(Ordering::Relaxed),
            ),
            main_wal_needs_truncation: self.main_wal_needs_truncation.load(Ordering::Relaxed),
        }
    }

    pub fn wal_keep_from(&self) -> u64 {
        self.wal_keep_from.load(Ordering::Acquire)
    }

    pub fn set_wal_keep_from(&self, keep_from: u64) {
        self.wal_keep_from.store(keep_from, Ordering::Release);
    }

    pub fn update_from_health_check(
        &self,
        report: &paro_journal::wal::recovery::WalHealthCheckReport,
    ) {
        self.wal_health_check_total.fetch_add(1, Ordering::Relaxed);
        self.wal_recovery_mode_metric
            .store(report.recovery_mode.as_metric_value(), Ordering::Relaxed);
        self.main_wal_needs_truncation
            .store(report.main_wal.needs_truncation, Ordering::Relaxed);
    }

    pub fn check_wal_health(
        &self,
        storage: Option<&dyn StorageManager>,
        db_name: &str,
        db_path: &str,
    ) -> anyhow::Result<paro_journal::wal::recovery::WalHealthCheckReport> {
        let wal_path = storage
            .map(|sm| sm.get_wal_path())
            .unwrap_or_else(|| format!("{}.wal", db_path));
        let report = paro_journal::wal::recovery::segment_catalog_health_check_read_only(
            Path::new(&wal_path),
        );
        self.update_from_health_check(&report);
        tracing::info!(
            target: targets::WAL,
            db = %db_name,
            wal_path = %wal_path,
            recovery_mode = report.recovery_mode.as_str(),
            healthy = report.healthy,
            "DatabaseHandle WAL health check completed (read-only)"
        );
        Ok(report)
    }

    pub fn refresh_for_path(&self, wal_path: &Path) {
        let report = paro_journal::wal::recovery::segment_catalog_health_check_read_only(wal_path);
        self.update_from_health_check(&report);
    }

    pub fn store_recovery_report(
        &self,
        report: crate::recovery::consistency_report::RecoveryConsistencyReport,
    ) {
        *self.last_recovery_report.write() = Some(report);
    }

    pub fn build_and_cache_recovery_report(
        &self,
        catalog: &Arc<ParoCatalog>,
    ) -> crate::recovery::consistency_report::RecoveryConsistencyReport {
        let report =
            crate::recovery::consistency_report::build_recovery_consistency_report(catalog);
        self.store_recovery_report(report.clone());
        report
    }

    pub fn last_recovery_report(
        &self,
    ) -> Option<crate::recovery::consistency_report::RecoveryConsistencyReport> {
        self.last_recovery_report.read().clone()
    }
}

impl Default for WalObservability {
    fn default() -> Self {
        Self::new()
    }
}
