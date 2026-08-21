// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::identity::DatabaseType;
use parking_lot::RwLock;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::logging::targets;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::BufferPool;
use paro_storage::compaction::compaction_manager::{
    CompactionAdmissionPolicy, CompactionManager, CompactionObservability,
};
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::TabletRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const COMPACTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct CompactionDriver {
    manager: RwLock<Option<Arc<CompactionManager>>>,
    scheduler: RwLock<Option<Arc<TaskScheduler>>>,
    buffer_pool: Arc<BufferPool>,
    max_concurrency: usize,
    admission_policy: CompactionAdmissionPolicy,
}

pub struct CompactionSuspendGuard {
    manager: Arc<CompactionManager>,
    reason: &'static str,
}

pub struct ForegroundMaintenanceGuard {
    manager: Arc<CompactionManager>,
}

impl Drop for ForegroundMaintenanceGuard {
    fn drop(&mut self) {
        self.manager.finish_foreground_statement();
    }
}

impl Drop for CompactionSuspendGuard {
    fn drop(&mut self) {
        self.manager.resume(self.reason);
    }
}

impl CompactionDriver {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        max_concurrency: usize,
        admission_policy: CompactionAdmissionPolicy,
    ) -> Self {
        Self {
            manager: RwLock::new(None),
            scheduler: RwLock::new(None),
            buffer_pool,
            max_concurrency: max_concurrency.max(1),
            admission_policy,
        }
    }

    pub fn has_manager(&self) -> bool {
        self.manager.read().is_some()
    }

    pub fn observability(&self) -> Option<CompactionObservability> {
        self.manager
            .read()
            .as_ref()
            .map(|manager| manager.observability())
    }

    pub fn bind_scheduler(&self, scheduler: Arc<TaskScheduler>) {
        *self.scheduler.write() = Some(scheduler);
    }

    pub fn ensure_started(&self, db_name: &str, db_type: DatabaseType) {
        if !Self::should_enable(db_type) {
            return;
        }

        if self.manager.read().is_some() {
            return;
        }

        let scheduler = self.scheduler.read().clone();
        let manager = match scheduler {
            Some(scheduler) => Arc::new(
                CompactionManager::new_with_buffer_pool_scheduler_and_admission_policy(
                    self.max_concurrency,
                    self.buffer_pool.clone(),
                    scheduler,
                    self.admission_policy,
                ),
            ),
            None => Arc::new(
                CompactionManager::new_with_buffer_pool_and_admission_policy(
                    self.max_concurrency,
                    self.buffer_pool.clone(),
                    self.admission_policy,
                ),
            ),
        };
        manager.clone().start();

        let mut slot = self.manager.write();
        if slot.is_none() {
            *slot = Some(manager);
            tracing::info!(
                target: targets::INSTANCE,
                db = %db_name,
                "Compaction manager initialized for attached database"
            );
        }
    }

    pub fn suspend(&self, db_name: &str, reason: &'static str) -> Option<CompactionSuspendGuard> {
        let manager = self.manager.read().as_ref()?.clone();
        manager.suspend(reason);
        if !manager.wait_for_idle(COMPACTION_DRAIN_TIMEOUT) {
            tracing::warn!(
                target: targets::INSTANCE,
                db = %db_name,
                reason = reason,
                timeout_secs = COMPACTION_DRAIN_TIMEOUT.as_secs(),
                running = manager.running_task_count(),
                "Compaction did not fully drain before lifecycle critical section"
            );
        }
        Some(CompactionSuspendGuard { manager, reason })
    }

    /// Defer new maintenance admission while a foreground statement is active.
    /// Accepted work is never canceled; the manager provides bounded
    /// starvation relief during sustained foreground traffic.
    pub fn enter_foreground(&self) -> Option<ForegroundMaintenanceGuard> {
        let manager = self.manager.read().as_ref()?.clone();
        manager.begin_foreground_statement();
        Some(ForegroundMaintenanceGuard { manager })
    }

    pub fn shutdown(&self) {
        let manager = self.manager.write().take();
        if let Some(manager) = manager {
            manager.stop();
            if let Err(err) = manager.sync_tablets(HashMap::new()) {
                tracing::warn!(
                    target: targets::INSTANCE,
                    error = %err,
                    "Failed to drain compaction tablets during shutdown"
                );
            }
        }
    }

    pub fn sync_tablets(
        &self,
        catalog: &ParoCatalog,
        db_name: &str,
        db_type: DatabaseType,
    ) -> anyhow::Result<()> {
        if !Self::should_enable(db_type) {
            return Ok(());
        }

        self.ensure_started(db_name, db_type);
        let manager = match self.manager.read().as_ref() {
            Some(manager) => manager.clone(),
            None => return Ok(()),
        };

        let desired = Self::collect_runtime_tables(catalog, &manager)?;
        let stats = manager.sync_tablets(desired)?;
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db_name,
            registered = stats.registered,
            unregistered = stats.unregistered,
            total = stats.total_registered,
            "Compaction tablet registry synchronized"
        );
        Ok(())
    }

    pub fn register_tablet(
        &self,
        storage: &Arc<TableHandle>,
        db_name: &str,
        db_type: DatabaseType,
    ) -> anyhow::Result<()> {
        if !Self::should_enable(db_type) {
            return Ok(());
        }

        self.ensure_started(db_name, db_type);
        let manager = match self.manager.read().as_ref() {
            Some(manager) => Arc::clone(manager),
            None => return Ok(()),
        };
        storage.bind_compaction_manager(&manager);
        manager.register_tablet(storage.tablet());
        Ok(())
    }

    pub fn unregister_tablet(
        &self,
        tablet_id: u64,
        db_name: &str,
        db_type: DatabaseType,
    ) -> anyhow::Result<()> {
        if !Self::should_enable(db_type) {
            return Ok(());
        }

        let manager = match self.manager.read().as_ref() {
            Some(manager) => Arc::clone(manager),
            None => {
                tracing::debug!(
                    target: targets::INSTANCE,
                    db = %db_name,
                    tablet_id,
                    "Compaction manager not started; skipping tablet unregister"
                );
                return Ok(());
            }
        };
        manager.unregister_tablet(tablet_id)?;
        Ok(())
    }

    pub fn collect_runtime_tables(
        catalog: &ParoCatalog,
        manager: &Arc<CompactionManager>,
    ) -> anyhow::Result<HashMap<u64, TabletRef>> {
        let txn = CatalogSnapshot::read_only(u64::MAX);
        let mut tablets = HashMap::new();

        for schema_entry in catalog
            .get_schema_collection()
            .scan(txn.transaction_id, txn.start_time)
        {
            let CatalogEntryEnum::Schema(schema) = schema_entry.as_ref() else {
                continue;
            };

            for table_entry in schema
                .collection(CatalogType::Table)
                .expect("table collection")
                .scan(txn.transaction_id, txn.start_time)
            {
                let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
                    continue;
                };
                if let Some(storage) = table.get_storage() {
                    storage.bind_compaction_manager(manager);
                    tablets.insert(storage.tablet_id(), storage.tablet());
                }
            }
        }

        Ok(tablets)
    }

    fn should_enable(db_type: DatabaseType) -> bool {
        !db_type.is_system() && !db_type.is_temporary() && !db_type.is_read_only()
    }
}
