// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::config::BootConfig;
use crate::lifecycle::startup_report::StartupPolicy;
use crate::metadata::instance_catalog::{DatabaseRecord, DatabaseRecordState, InstanceCatalog};
use crate::metadata::InstanceMetadata;
use crate::{Instance, InstanceDdlOwner};
use paro_common::logging::targets;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::index::graph::GraphProjectionIndexManager;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod closer;
pub mod commit_health;
pub mod compaction_driver;
pub mod handle;
pub mod hooks;
pub mod identity;
pub mod opener;
pub mod registry;
pub mod state;
pub mod storage;
pub mod storage_identity;
pub mod wal_observability;

use crate::database::handle::{AttachOptions, DatabaseCloseAction, DatabaseHandle};
use crate::database::hooks::{
    DeferredTaskRecoveryHook, GraphProjectionRecoveryHook, RecoveryHook, RecoveryHookContext,
    RecoveryHookResult,
};
use crate::database::opener::{
    DatabaseOpenContext, DatabaseOpenError, DatabaseOpenIntent, DatabaseOpenRequest,
    DatabaseOpenResult, DatabaseOpener,
};
use crate::database::registry::{AttachInfo, DatabaseRegistry, OnEntryNotFound};
use crate::recovery::consistency_report::RecoveryConsistencyReport;
use paro_common::effect::DeferredTask;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InstanceWalLifecycleMetrics {
    pub database_count: usize,
    pub checkpoint_success_total: u64,
    pub checkpoint_failure_total: u64,
    pub wal_health_check_total: u64,
    pub wal_keep_from_pinned_dbs: usize,
    pub wal_keep_from_keep_all_dbs: usize,
    pub recovery_mode_unknown: usize,
    pub recovery_mode_no_wal: usize,
    pub recovery_mode_main_wal_only: usize,
    pub main_wal_needs_truncation_dbs: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct RecoveryHookExecutionError {
    pub(crate) detail: String,
    pub(crate) hook_results: Vec<RecoveryHookResult>,
}

pub fn default_recovery_hooks() -> Vec<Arc<dyn RecoveryHook>> {
    vec![Arc::new(GraphProjectionRecoveryHook)]
}

/// Narrow context for managed database DDL orchestration.
pub struct DatabaseDdlContext<'a> {
    pub boot_config: &'a BootConfig,
    pub metadata: &'a InstanceMetadata,
    pub open_ctx: DatabaseOpenContext,
}

impl DatabaseDdlContext<'_> {
    fn is_in_memory(&self) -> bool {
        self.boot_config.is_in_memory()
    }

    fn managed_storage_dir(&self, database_id: u64) -> paro_common::error::Result<String> {
        if self.is_in_memory() {
            return Ok(":memory:".to_string());
        }

        let layout = self.metadata.layout().ok_or_else(|| {
            paro_common::error::internal("Persistent instance layout is unavailable")
        })?;
        Ok(layout
            .managed_database_dir(database_id)
            .to_string_lossy()
            .to_string())
    }
}

/// Service that owns runtime-visible managed database orchestration.
pub struct ManagedDatabaseService {
    registry: Arc<DatabaseRegistry>,
    graph_manager: Arc<GraphProjectionIndexManager>,
    scheduler: Arc<TaskScheduler>,
    recovery_hooks: Vec<Arc<dyn RecoveryHook>>,
}

impl ManagedDatabaseService {
    pub(crate) fn new(
        registry: Arc<DatabaseRegistry>,
        graph_manager: Arc<GraphProjectionIndexManager>,
        scheduler: Arc<TaskScheduler>,
        recovery_hooks: Vec<Arc<dyn RecoveryHook>>,
    ) -> Self {
        Self {
            registry,
            graph_manager,
            scheduler,
            recovery_hooks,
        }
    }

    pub(crate) fn new_with_boot_config(
        boot_config: &BootConfig,
        scheduler: Arc<TaskScheduler>,
        recovery_hooks: Vec<Arc<dyn RecoveryHook>>,
    ) -> Self {
        let registry = match &boot_config.path_manager {
            Some(path_manager) => {
                Arc::new(DatabaseRegistry::with_path_manager(path_manager.clone()))
            }
            None => Arc::new(DatabaseRegistry::new()),
        };
        Self::new(
            registry,
            Arc::new(GraphProjectionIndexManager::new()),
            scheduler,
            recovery_hooks,
        )
    }

    pub fn registry(&self) -> &Arc<DatabaseRegistry> {
        &self.registry
    }

    pub fn graph_manager(&self) -> &Arc<GraphProjectionIndexManager> {
        &self.graph_manager
    }

    pub fn checkpoint_all(&self) -> paro_common::error::Result<()> {
        for db in self.registry.get_databases() {
            db.force_checkpoint().map_err(|e| {
                paro_common::error::internal(format!(
                    "Failed to checkpoint database \"{}\": {}",
                    db.name(),
                    e
                ))
            })?;
        }
        Ok(())
    }

    pub fn create_database(
        &self,
        ctx: &DatabaseDdlContext<'_>,
        name: &str,
    ) -> paro_common::error::Result<Arc<DatabaseHandle>> {
        let mut catalog = ctx.metadata.load_catalog()?;
        self.provision_managed_database(ctx, &mut catalog, name, false, true)
    }

    pub fn drop_database(
        &self,
        ctx: &DatabaseDdlContext<'_>,
        name: &str,
    ) -> paro_common::error::Result<()> {
        let mut catalog = ctx.metadata.load_catalog()?;
        let record = catalog
            .find_database_by_name(name)
            .cloned()
            .ok_or_else(|| {
                paro_common::error::catalog(format!(
                    "Failed to detach database with name \"{}\": database not found",
                    name
                ))
            })?;

        if !record.state.can_drop() {
            return Err(paro_common::error::catalog(format!(
                "Cannot drop database \"{}\" while it is in {:?} state",
                name, record.state
            )));
        }

        if catalog.default_database_id == Some(record.database_id) {
            return Err(paro_common::error::catalog(format!(
                "Cannot drop database \"{}\" because it is the default database",
                name
            )));
        }

        {
            let record_state = catalog
                .find_database_mut_by_id(record.database_id)
                .ok_or_else(|| {
                    paro_common::error::internal("instance catalog record disappeared")
                })?;
            record_state.state = DatabaseRecordState::Dropping;
            record_state.last_error = None;
        }
        ctx.metadata.persist_catalog(&mut catalog)?;

        if let Err(err) = self
            .registry
            .detach_database_full(name, OnEntryNotFound::ReturnNull)
            .map_err(|e| paro_common::error::internal(e.to_string()))
        {
            self.persist_database_transition_failure(
                ctx.metadata,
                &mut catalog,
                record.database_id,
                DatabaseRecordState::Dropping,
                err.to_string(),
            )?;
            return Err(err);
        }

        if let Err(err) = self.cleanup_database_storage_dir(ctx, &record) {
            self.persist_database_transition_failure(
                ctx.metadata,
                &mut catalog,
                record.database_id,
                DatabaseRecordState::Dropping,
                err.to_string(),
            )?;
            return Err(err);
        }

        catalog.remove_database_by_id(record.database_id);
        ctx.metadata.persist_catalog(&mut catalog)?;
        Ok(())
    }

    pub fn rename_database(
        &self,
        ctx: &DatabaseDdlContext<'_>,
        old_name: &str,
        new_name: &str,
    ) -> paro_common::error::Result<()> {
        if DatabaseHandle::name_is_reserved(new_name) {
            return Err(paro_common::error::catalog(format!(
                "database \"{}\" is a reserved name",
                new_name
            )));
        }

        let mut catalog = ctx.metadata.load_catalog()?;
        let record = catalog
            .find_database_by_name(old_name)
            .cloned()
            .ok_or_else(|| {
                paro_common::error::catalog(format!(
                    "Failed to rename database \"{}\": database not found",
                    old_name
                ))
            })?;

        if !record.state.can_rename() {
            return Err(paro_common::error::catalog(format!(
                "Cannot rename database \"{}\" while it is in {:?} state",
                old_name, record.state
            )));
        }

        if old_name.eq_ignore_ascii_case(new_name) {
            return Ok(());
        }

        if catalog.find_database_by_name(new_name).is_some() {
            return Err(paro_common::error::catalog(format!(
                "database \"{}\" already exists",
                new_name
            )));
        }

        catalog
            .rename_database(record.database_id, new_name.to_string())
            .map_err(|e| paro_common::error::catalog(e.to_string()))?;
        ctx.metadata.persist_catalog(&mut catalog)?;

        if record.state.allows_runtime_open() {
            self.registry
                .rename_database(old_name, new_name, OnEntryNotFound::ThrowException)
                .map_err(|e| paro_common::error::internal(e.to_string()))?;
        }

        tracing::info!(
            target: targets::INSTANCE,
            old_database = %old_name,
            database = %new_name,
            database_id = record.database_id,
            "Managed database renamed via instance catalog"
        );

        Ok(())
    }

    pub fn open_managed_database(
        &self,
        record: &DatabaseRecord,
        intent: DatabaseOpenIntent,
        open_ctx: &DatabaseOpenContext,
        startup_policy: StartupPolicy,
        run_hooks: bool,
    ) -> Result<DatabaseOpenResult, DatabaseOpenError> {
        let request = DatabaseOpenRequest {
            record: record.clone(),
            intent,
            object_id_allocator: Arc::clone(self.registry.object_id_allocator()),
        };
        let mut result = match intent {
            DatabaseOpenIntent::CreateNew => DatabaseOpener::bootstrap_new(open_ctx, request),
            DatabaseOpenIntent::OpenExisting => DatabaseOpener::open_existing(open_ctx, request),
            DatabaseOpenIntent::AttachExternal => {
                return Err(DatabaseOpenError::new(paro_common::error::not_implemented(
                    "AttachExternal database open is not implemented",
                )));
            }
        }
        .map_err(|e| {
            DatabaseOpenError::new(paro_common::error::internal(format!(
                "Failed to open database: {}",
                e
            )))
        })?;

        if run_hooks {
            result.recovery_report.hook_results = match self.run_recovery_hooks(
                &result.handle,
                &result.recovery_report.consistency,
                startup_policy,
                &result.replayed_deferred_tasks,
            ) {
                Ok(hook_results) => hook_results,
                Err(hook_error) => {
                    result.recovery_report.hook_results = hook_error.hook_results.clone();
                    return Err(DatabaseOpenError::with_recovery_report(
                        paro_common::error::internal(hook_error.detail),
                        result.recovery_report,
                    ));
                }
            };
        }

        Ok(result)
    }

    pub fn publish(
        &self,
        record: &DatabaseRecord,
        db: &Arc<DatabaseHandle>,
    ) -> paro_common::error::Result<()> {
        let attach_info = AttachInfo::new(record.name.clone(), record.storage_dir.clone());
        let attach_options = AttachOptions::default();
        self.registry
            .attach_database(&attach_info, &attach_options, Arc::clone(db))
            .map_err(|e| paro_common::error::internal(e.to_string()))?;
        Ok(())
    }

    pub fn reopen_after_commit_poison(
        &self,
        ctx: &DatabaseDdlContext<'_>,
        name: &str,
    ) -> paro_common::error::Result<Arc<DatabaseHandle>> {
        let catalog = ctx.metadata.load_catalog()?;
        let record = catalog
            .find_database_by_name(name)
            .cloned()
            .ok_or_else(|| paro_common::error::database_not_found(name))?;
        if !record.state.allows_runtime_open() {
            return Err(paro_common::error::cannot_connect_now().detail(format!(
                "database \"{}\" is in {:?} state",
                name, record.state
            )));
        }
        let old = self.registry.get_database(name).ok_or_else(|| {
            paro_common::error::cannot_connect_now()
                .detail(format!("database \"{}\" is not currently open", name))
        })?;
        let old_poison = old.commit_poison_snapshot();
        if !old_poison.poisoned {
            return Err(paro_common::error::invalid_transaction_state(format!(
                "database \"{}\" is not commit-poisoned",
                name
            )));
        }

        old.close(DatabaseCloseAction::TryCheckpoint)
            .map_err(|error| paro_common::error::internal(error.to_string()))?;
        let open_result = self
            .open_managed_database(
                &record,
                DatabaseOpenIntent::OpenExisting,
                &ctx.open_ctx,
                StartupPolicy::default(),
                true,
            )
            .map_err(|error| error.error)?;
        self.registry
            .replace_runtime_database(name, Arc::clone(&open_result.handle))
            .map_err(|error| paro_common::error::internal(error.to_string()))?;
        Ok(open_result.handle)
    }

    pub(crate) fn provision_managed_database(
        &self,
        ctx: &DatabaseDdlContext<'_>,
        catalog: &mut InstanceCatalog,
        name: &str,
        set_as_default: bool,
        publish_runtime: bool,
    ) -> paro_common::error::Result<Arc<DatabaseHandle>> {
        if DatabaseHandle::name_is_reserved(name) {
            return Err(paro_common::error::catalog(format!(
                "database \"{}\" is a reserved name",
                name
            )));
        }
        if catalog.find_database_by_name(name).is_some()
            || self.registry.get_database(name).is_some()
        {
            return Err(paro_common::error::catalog(format!(
                "database \"{}\" already exists",
                name
            )));
        }

        let storage_dir = ctx.managed_storage_dir(catalog.next_database_id)?;
        let record = catalog
            .allocate_database(name.to_string(), storage_dir.clone())
            .map_err(|e| paro_common::error::catalog(e.to_string()))?;

        if set_as_default {
            catalog
                .set_default_database(Some(record.database_id))
                .map_err(|e| paro_common::error::internal(e.to_string()))?;
        }
        ctx.metadata.persist_catalog(catalog)?;

        let open_result = match self.open_managed_database(
            &record,
            DatabaseOpenIntent::CreateNew,
            &ctx.open_ctx,
            ctx.boot_config.startup_policy,
            false,
        ) {
            Ok(result) => result,
            Err(err) => match self.cleanup_database_storage_dir(ctx, &record) {
                Ok(()) => {
                    catalog.remove_database_by_id(record.database_id);
                    let _ = ctx.metadata.persist_catalog(catalog);
                    return Err(err.error);
                }
                Err(cleanup_err) => {
                    let combined_error = format!(
                        "{}; cleanup for provisioning database \"{}\" also failed: {}",
                        err, name, cleanup_err
                    );
                    let _ = self.persist_database_transition_failure(
                        ctx.metadata,
                        catalog,
                        record.database_id,
                        DatabaseRecordState::Provisioning,
                        combined_error.clone(),
                    );
                    return Err(paro_common::error::internal(combined_error));
                }
            },
        };

        if let Some(record_state) = catalog.find_database_mut_by_id(record.database_id) {
            record_state.state = DatabaseRecordState::Ready;
            record_state.last_error = None;
        }
        ctx.metadata.persist_catalog(catalog)?;

        if publish_runtime {
            self.publish(&record, &open_result.handle)?;
        }

        if set_as_default && publish_runtime {
            self.registry
                .set_default_database(record.database_id)
                .map_err(|e| paro_common::error::internal(e.to_string()))?;
        }

        tracing::info!(
            target: targets::INSTANCE,
            database = %name,
            database_id = record.database_id,
            path = %record.storage_dir,
            "Managed database created via instance catalog"
        );

        Ok(open_result.handle)
    }

    pub(crate) fn managed_runtime_databases(&self) -> Vec<Arc<DatabaseHandle>> {
        let mut databases: Vec<_> = self
            .registry
            .get_databases()
            .into_iter()
            .filter(|db| db.id() != 0)
            .collect();
        databases.sort_by_key(|db| db.id());
        databases
    }

    pub(crate) fn system_runtime_databases(&self) -> Vec<Arc<DatabaseHandle>> {
        self.registry
            .get_databases()
            .into_iter()
            .filter(|db| db.id() == 0)
            .collect()
    }

    pub fn wal_lifecycle_metrics(&self) -> InstanceWalLifecycleMetrics {
        let databases = self.registry.get_databases();
        let mut metrics = InstanceWalLifecycleMetrics {
            database_count: databases.len(),
            ..Default::default()
        };

        for db in databases {
            let db_metrics = db.wal_lifecycle_metrics();
            metrics.checkpoint_success_total += db_metrics.checkpoint_success_total;
            metrics.checkpoint_failure_total += db_metrics.checkpoint_failure_total;
            metrics.wal_health_check_total += db_metrics.wal_health_check_total;
            if db_metrics.wal_keep_from != u64::MAX {
                metrics.wal_keep_from_pinned_dbs += 1;
                if db_metrics.wal_keep_from == 0 {
                    metrics.wal_keep_from_keep_all_dbs += 1;
                }
            }

            match db_metrics.recovery_mode {
                ::paro_journal::wal::recovery::WalRecoveryMode::Unknown => {
                    metrics.recovery_mode_unknown += 1
                }
                ::paro_journal::wal::recovery::WalRecoveryMode::NoWal => {
                    metrics.recovery_mode_no_wal += 1
                }
                ::paro_journal::wal::recovery::WalRecoveryMode::MainWalOnly => {
                    metrics.recovery_mode_main_wal_only += 1
                }
            }

            if db_metrics.main_wal_needs_truncation {
                metrics.main_wal_needs_truncation_dbs += 1;
            }
        }

        metrics
    }

    fn run_recovery_hooks(
        &self,
        db: &Arc<DatabaseHandle>,
        consistency: &RecoveryConsistencyReport,
        startup_policy: StartupPolicy,
        replayed_deferred_tasks: &[DeferredTask],
    ) -> Result<Vec<RecoveryHookResult>, RecoveryHookExecutionError> {
        let mut hook_results = Vec::with_capacity(self.recovery_hooks.len());
        let context = RecoveryHookContext {
            database_root: PathBuf::from(db.path()),
            recovery_report: consistency.clone(),
            startup_policy,
            graph_registry: self.graph_manager.clone(),
            scheduler: Arc::clone(&self.scheduler),
            replayed_deferred_tasks: replayed_deferred_tasks.to_vec(),
        };

        for hook in &self.recovery_hooks {
            match hook.run(db, &context) {
                Ok(result) => hook_results.push(result),
                Err(err) => {
                    let hook_error = format!(
                        "recovery hook {} failed for database {}: {}",
                        hook.name(),
                        db.name(),
                        err
                    );
                    hook_results.push(RecoveryHookResult::Failed {
                        error: hook_error.clone(),
                        issues: Vec::new(),
                    });
                    return Err(RecoveryHookExecutionError {
                        detail: hook_error,
                        hook_results,
                    });
                }
            }
        }

        let deferred_task_hook = DeferredTaskRecoveryHook;
        let deferred_task_hook_configured = self
            .recovery_hooks
            .iter()
            .any(|hook| hook.name() == deferred_task_hook.name());
        if !context.replayed_deferred_tasks.is_empty() && !deferred_task_hook_configured {
            match deferred_task_hook.run(db, &context) {
                Ok(result) => hook_results.push(result),
                Err(err) => {
                    let hook_error = format!(
                        "recovery hook {} failed for database {}: {}",
                        deferred_task_hook.name(),
                        db.name(),
                        err
                    );
                    hook_results.push(RecoveryHookResult::Failed {
                        error: hook_error.clone(),
                        issues: Vec::new(),
                    });
                    return Err(RecoveryHookExecutionError {
                        detail: hook_error,
                        hook_results,
                    });
                }
            }
        }

        Ok(hook_results)
    }

    fn cleanup_database_storage_dir(
        &self,
        ctx: &DatabaseDdlContext<'_>,
        record: &DatabaseRecord,
    ) -> paro_common::error::Result<()> {
        if ctx.is_in_memory() {
            return Ok(());
        }

        let storage_path = Path::new(&record.storage_dir);
        if !storage_path.exists() {
            return Ok(());
        }

        std::fs::remove_dir_all(storage_path).map_err(|e| {
            paro_common::error::internal(format!(
                "Failed to remove database storage directory {}: {}",
                storage_path.display(),
                e
            ))
        })
    }

    fn persist_database_transition_failure(
        &self,
        metadata: &InstanceMetadata,
        catalog: &mut InstanceCatalog,
        database_id: u64,
        state: DatabaseRecordState,
        error: String,
    ) -> paro_common::error::Result<()> {
        let record = catalog
            .find_database_mut_by_id(database_id)
            .ok_or_else(|| {
                paro_common::error::internal(format!(
                    "instance catalog record {} disappeared while persisting failure",
                    database_id
                ))
            })?;
        record.state = state;
        record.last_error = Some(error);
        metadata.persist_catalog(catalog)
    }
}

impl Instance {
    pub(crate) fn database_ddl_context(&self) -> DatabaseDdlContext<'_> {
        DatabaseDdlContext {
            boot_config: self.boot_config.as_ref(),
            metadata: &self.metadata,
            open_ctx: self
                .runtime
                .database_open_context(self.boot_config.checkpoint, self.boot_config.compaction),
        }
    }

    pub fn checkpoint(&self) -> paro_common::error::Result<()> {
        self.lifecycle.admission.check(None)?;
        self.database_service.checkpoint_all()
    }

    pub fn create_database(&self, name: &str) -> paro_common::error::Result<Arc<DatabaseHandle>> {
        let _ddl_guard = self.lock_ddl(InstanceDdlOwner::CreateDatabase)?;
        self.database_service
            .create_database(&self.database_ddl_context(), name)
    }

    pub fn drop_database(&self, name: &str) -> paro_common::error::Result<()> {
        let _ddl_guard = self.lock_ddl(InstanceDdlOwner::DropDatabase)?;
        self.database_service
            .drop_database(&self.database_ddl_context(), name)
    }

    pub fn rename_database(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> paro_common::error::Result<()> {
        let _ddl_guard = self.lock_ddl(InstanceDdlOwner::RenameDatabase)?;
        self.database_service
            .rename_database(&self.database_ddl_context(), old_name, new_name)
    }

    pub fn reopen_database_after_commit_poison(
        &self,
        name: &str,
    ) -> paro_common::error::Result<Arc<DatabaseHandle>> {
        let _ddl_guard = self.lock_ddl(InstanceDdlOwner::ReopenDatabase)?;
        self.database_service
            .reopen_after_commit_poison(&self.database_ddl_context(), name)
    }

    pub fn database_registry(&self) -> &Arc<DatabaseRegistry> {
        self.database_service.registry()
    }

    pub fn graph_manager(&self) -> &Arc<GraphProjectionIndexManager> {
        self.database_service.graph_manager()
    }

    /// Aggregate per-database WAL lifecycle metrics at instance scope.
    pub fn wal_lifecycle_metrics(&self) -> InstanceWalLifecycleMetrics {
        self.database_service.wal_lifecycle_metrics()
    }
}
