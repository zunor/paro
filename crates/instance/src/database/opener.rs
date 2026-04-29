// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::handle::{AttachOptions, DatabaseHandle, RecoveryMode};
use super::hooks::RecoveryHookResult;
use super::storage::{DatabaseStorage, InMemoryDatabaseStorage};
use crate::checkpoint::recovery::{CheckpointBaseState, CheckpointRecovery};
use crate::checkpoint::RetentionCoordinator;
use crate::config::CheckpointConfigOptions;
use crate::metadata::instance_catalog::DatabaseRecord;
use crate::storage_manager::StorageManager;
use paro_catalog::catalog::Catalog;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::checkpoint::RecoverySummary;
use paro_common::effect::DeferredTask;
use paro_common::logging::targets;
use paro_journal::wal::wal_entry::WalHeaderMetadata;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferManager, BufferPool};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseOpenIntent {
    CreateNew,
    OpenExisting,
    AttachExternal,
}

#[derive(Clone)]
pub struct DatabaseOpenContext {
    pub buffer_pool: Arc<BufferPool>,
    pub buffer_manager: Arc<dyn BufferManager>,
    pub scheduler: Arc<TaskScheduler>,
    pub checkpoint: CheckpointConfigOptions,
}

#[derive(Debug, Clone)]
pub struct DatabaseOpenRequest {
    pub record: DatabaseRecord,
    pub intent: DatabaseOpenIntent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub consistency: crate::recovery::consistency_report::RecoveryConsistencyReport,
    pub hook_results: Vec<RecoveryHookResult>,
}

#[derive(Debug, Clone)]
pub struct DatabaseOpenError {
    pub error: paro_common::error::ParoError,
    pub recovery_report: Option<RecoveryReport>,
}

impl DatabaseOpenError {
    pub fn new(error: paro_common::error::ParoError) -> Self {
        Self {
            error,
            recovery_report: None,
        }
    }

    pub fn with_recovery_report(
        error: paro_common::error::ParoError,
        recovery_report: RecoveryReport,
    ) -> Self {
        Self {
            error,
            recovery_report: Some(recovery_report),
        }
    }
}

impl std::fmt::Display for DatabaseOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for DatabaseOpenError {}

#[derive(Debug)]
pub struct DatabaseOpenResult {
    pub handle: Arc<DatabaseHandle>,
    pub recovery_report: RecoveryReport,
    pub(crate) replayed_deferred_tasks: Vec<DeferredTask>,
}

#[derive(Debug)]
struct FinalizeLoadInputs {
    wal_path: PathBuf,
    checkpoint_base: CheckpointBaseState,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: u64,
}

#[derive(Debug)]
struct FinalizeLoadOutcome {
    consistency: crate::recovery::consistency_report::RecoveryConsistencyReport,
    replayed_deferred_tasks: Vec<DeferredTask>,
}

#[derive(Debug)]
struct WalReplayOutcome {
    summary: RecoverySummary,
    replayed_deferred_tasks: Vec<DeferredTask>,
}

pub struct DatabaseOpener;

impl DatabaseOpener {
    pub fn bootstrap_new(
        context: &DatabaseOpenContext,
        request: DatabaseOpenRequest,
    ) -> anyhow::Result<DatabaseOpenResult> {
        Self::open_database(context, request, DatabaseOpenIntent::CreateNew)
    }

    pub fn open_existing(
        context: &DatabaseOpenContext,
        request: DatabaseOpenRequest,
    ) -> anyhow::Result<DatabaseOpenResult> {
        Self::open_database(context, request, DatabaseOpenIntent::OpenExisting)
    }

    pub fn initialize_catalog(catalog: &ParoCatalog) -> anyhow::Result<()> {
        catalog.initialize(false);
        Ok(())
    }

    pub fn finalize_load(db: &DatabaseHandle) -> anyhow::Result<()> {
        Self::finalize_load_outcome(db, CheckpointConfigOptions::default()).map(|_| ())
    }

    pub fn finalize_load_with_report(
        db: &DatabaseHandle,
        checkpoint: CheckpointConfigOptions,
    ) -> anyhow::Result<crate::recovery::consistency_report::RecoveryConsistencyReport> {
        Self::finalize_load_outcome(db, checkpoint).map(|outcome| outcome.consistency)
    }

    fn finalize_load_outcome(
        db: &DatabaseHandle,
        checkpoint: CheckpointConfigOptions,
    ) -> anyhow::Result<FinalizeLoadOutcome> {
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            stage = "prepare_recovery",
            "Finalizing database load"
        );

        let _compaction_guard = db.compaction().suspend(db.name(), "recovery");
        db.transaction_manager().block_recovery_admission();
        let inputs = Self::collect_finalize_inputs(db)?;
        let apply_runtime = db.journal_apply_runtime();
        apply_runtime.bootstrap_frontiers(Self::journal_recovery_summary(
            &inputs.checkpoint_base.bootstrap,
        ));

        let (recovery_summary, replayed_deferred_tasks, replayed_wal) =
            if crate::recovery::replay_handler::needs_recovery(&inputs.wal_path) {
                let replay = Self::replay_wal(db, &inputs, Some(Arc::clone(&apply_runtime)))?;
                (replay.summary, replay.replayed_deferred_tasks, true)
            } else {
                tracing::debug!(
                    target: targets::WAL,
                    db = %db.name(),
                    wal_path = %inputs.wal_path.display(),
                    stage = "wal_probe",
                    "No WAL recovery needed"
                );
                (inputs.checkpoint_base.bootstrap.clone(), Vec::new(), false)
            };

        apply_runtime.bootstrap_frontiers(Self::journal_recovery_summary(&recovery_summary));
        db.bootstrap_checkpoint_runtime(recovery_summary.clone());
        if !replayed_wal {
            CheckpointRecovery::redeliver_deferred_tasks(
                db.catalog(),
                &inputs.checkpoint_base.deferred_tasks,
            );
        }
        let report = Self::refresh_recovery_report(db, &inputs.wal_path);
        Self::sweep_checkpoint_artifacts(db, checkpoint)?;
        Self::ensure_recovered_commits_published(&apply_runtime, recovery_summary.max_commit_id)?;
        let runtime_commit_floor =
            Self::reconcile_runtime_state(db, &recovery_summary, checkpoint)?;
        db.sync_commit_runtime_with(runtime_commit_floor);
        db.transaction_manager()
            .complete_recovery_admission(runtime_commit_floor);
        Self::mark_ready(db);
        db.maybe_gc_catalog();
        Ok(FinalizeLoadOutcome {
            consistency: report,
            replayed_deferred_tasks,
        })
    }

    fn open_database(
        context: &DatabaseOpenContext,
        request: DatabaseOpenRequest,
        expected_intent: DatabaseOpenIntent,
    ) -> anyhow::Result<DatabaseOpenResult> {
        if request.intent != expected_intent {
            anyhow::bail!(
                "database open intent mismatch: expected {:?}, got {:?}",
                expected_intent,
                request.intent
            );
        }

        if request.intent == DatabaseOpenIntent::AttachExternal {
            anyhow::bail!("AttachExternal database open is not implemented");
        }

        let record = request.record;
        tracing::info!(
            target: targets::INSTANCE,
            db = %record.name,
            database_id = record.database_id,
            path = %record.storage_dir,
            intent = ?request.intent,
            "Opening managed database"
        );

        let db = Arc::new(DatabaseHandle::with_options(
            record.database_id,
            record.name.clone(),
            record.storage_dir.clone(),
            context.buffer_pool.clone(),
            AttachOptions::default(),
        ));
        db.bind_task_scheduler(context.scheduler.clone());

        Self::initialize_catalog(db.catalog().as_ref())?;
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            database_id = db.id(),
            path = %db.path(),
            stage = "catalog_initialize",
            "Initialized database catalog skeleton"
        );

        if db.path() == ":memory:" {
            Self::initialize_in_memory_storage(&db)?;
        } else {
            match request.intent {
                DatabaseOpenIntent::CreateNew => {
                    Self::create_storage_layout(&db, context.buffer_manager.clone())?;
                }
                DatabaseOpenIntent::OpenExisting => {
                    Self::open_storage_layout(&db, context.buffer_manager.clone())?;
                }
                DatabaseOpenIntent::AttachExternal => {
                    anyhow::bail!("AttachExternal database open is not implemented");
                }
            }
        }

        let finalize = Self::finalize_load_outcome(&db, context.checkpoint)?;
        Ok(DatabaseOpenResult {
            handle: db,
            recovery_report: RecoveryReport {
                consistency: finalize.consistency,
                hook_results: Vec::new(),
            },
            replayed_deferred_tasks: finalize.replayed_deferred_tasks,
        })
    }

    fn initialize_in_memory_storage(db: &Arc<DatabaseHandle>) -> anyhow::Result<()> {
        let mut storage = InMemoryDatabaseStorage::new();
        storage.initialize().map_err(|e| anyhow::anyhow!(e))?;
        db.attach_storage(Box::new(storage));
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            stage = "open_storage_layout",
            "Initialized in-memory storage"
        );
        Ok(())
    }

    fn create_storage_layout(
        db: &Arc<DatabaseHandle>,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> anyhow::Result<()> {
        let mut storage = DatabaseStorage::new(db.path().to_string(), buffer_manager);
        storage.create_new().map_err(|e| anyhow::anyhow!(e))?;
        storage
            .bootstrap_storage_identity(db.id())
            .map_err(|e| anyhow::anyhow!(e))?;

        if db.recovery_mode() != RecoveryMode::NoWalWrites {
            storage.initialize_wal().map_err(|e| anyhow::anyhow!(e))?;
        }

        db.attach_storage(Box::new(storage));
        tracing::info!(
            target: targets::INSTANCE,
            db = %db.name(),
            path = %db.path(),
            stage = "create_storage_layout",
            "Created managed database storage"
        );
        Ok(())
    }

    fn open_storage_layout(
        db: &Arc<DatabaseHandle>,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> anyhow::Result<()> {
        let mut storage = DatabaseStorage::new(db.path().to_string(), buffer_manager);
        storage.load_existing().map_err(|e| anyhow::anyhow!(e))?;
        storage
            .validate_storage_identity(db.id())
            .map_err(|e| anyhow::anyhow!(e))?;

        if db.recovery_mode() != RecoveryMode::NoWalWrites {
            storage.load_wal().map_err(|e| anyhow::anyhow!(e))?;
            if storage.get_wal().is_none() {
                storage.initialize_wal().map_err(|e| anyhow::anyhow!(e))?;
            } else {
                storage
                    .validate_loaded_wal_identity()
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }

        db.attach_storage(Box::new(storage));
        tracing::info!(
            target: targets::INSTANCE,
            db = %db.name(),
            path = %db.path(),
            stage = "open_storage_layout",
            "Opened managed database storage"
        );
        Ok(())
    }

    fn collect_finalize_inputs(db: &DatabaseHandle) -> anyhow::Result<FinalizeLoadInputs> {
        let storage = db.storage_lock().read();
        let sm = storage.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Storage manager must be attached before finalize_load")
        })?;

        Ok(FinalizeLoadInputs {
            wal_path: PathBuf::from(sm.get_wal_path()),
            checkpoint_base: CheckpointRecovery::load_base_from_storage(
                db.catalog().as_ref(),
                sm.as_ref(),
                db.tablet_meta_manager(),
            )?,
            wal_header_metadata: sm.wal_header_metadata(),
            wal_keep_from: sm.wal_keep_from(),
        })
    }

    fn replay_wal(
        db: &DatabaseHandle,
        inputs: &FinalizeLoadInputs,
        apply_runtime: Option<Arc<paro_journal::JournalApplyRuntime>>,
    ) -> anyhow::Result<WalReplayOutcome> {
        tracing::info!(
            target: targets::WAL,
            db = %db.name(),
            wal_path = %inputs.wal_path.display(),
            stage = "wal_replay",
            "WAL recovery needed, replaying entries"
        );

        let replay = crate::recovery::replay_handler::recover_database_with_checkpoint_bootstrap(
            &inputs.wal_path,
            db.catalog(),
            db.tablet_meta_manager(),
            inputs.checkpoint_base.journal_tail.clone(),
            inputs.wal_header_metadata,
            (inputs.wal_keep_from != u64::MAX).then_some(inputs.wal_keep_from),
            apply_runtime,
            inputs.checkpoint_base.bootstrap.clone(),
        )?;

        tracing::info!(
            target: targets::WAL,
            db = %db.name(),
            entries_replayed = replay.replay_result.entries_replayed,
            all_succeeded = replay.replay_result.all_succeeded,
            replayed_deferred_tasks = replay.replayed_deferred_tasks.len(),
            stage = "wal_replay",
            "WAL recovery complete"
        );

        let mut storage = db.storage_lock().write();
        let sm = storage
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("StorageManager disappeared during finalize_load"))?;
        sm.replace_wal(Arc::new(replay.wal))
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(WalReplayOutcome {
            summary: replay.summary,
            replayed_deferred_tasks: replay.replayed_deferred_tasks,
        })
    }

    fn refresh_recovery_report(
        db: &DatabaseHandle,
        wal_path: &Path,
    ) -> crate::recovery::consistency_report::RecoveryConsistencyReport {
        let report = db
            .wal_observability()
            .build_and_cache_recovery_report(db.catalog());
        db.wal_observability().refresh_for_path(wal_path);
        tracing::info!(
            target: targets::INSTANCE,
            db = %db.name(),
            table_count = report.table_count,
            consistent_tables = report.consistent_tables,
            inconsistent_tables = report.inconsistent_tables,
            all_consistent = report.all_consistent,
            stage = "reconcile",
            "Recovery consistency report refreshed"
        );
        report
    }

    fn reconcile_runtime_state(
        db: &DatabaseHandle,
        recovery_summary: &RecoverySummary,
        checkpoint: CheckpointConfigOptions,
    ) -> anyhow::Result<u64> {
        let runtime_commit_floor = Self::sync_transaction_clock(db, recovery_summary.max_commit_id);
        db.configure_checkpoint_runtime(checkpoint);
        db.compaction()
            .sync_tablets(db.catalog().as_ref(), db.name(), db.db_type())?;
        Ok(runtime_commit_floor)
    }

    fn ensure_recovered_commits_published(
        runtime: &paro_journal::JournalApplyRuntime,
        durable_commit_id: u64,
    ) -> anyhow::Result<()> {
        let frontiers = runtime.frontiers();
        if frontiers.published_commit_id < durable_commit_id {
            anyhow::bail!(
                "recovery apply stopped below durable commit frontier: published={} durable={}",
                frontiers.published_commit_id,
                durable_commit_id
            );
        }
        Ok(())
    }

    fn journal_recovery_summary(
        summary: &RecoverySummary,
    ) -> paro_common::journal::RecoverySummary {
        paro_common::journal::RecoverySummary {
            max_lsn: summary.max_lsn,
            max_commit_id: summary.max_commit_id,
            max_maintenance_id: summary.max_maintenance_id,
            max_catalog_commit_id: summary.max_catalog_commit_id,
            max_seen_object_id: summary.max_seen_object_id,
        }
    }

    fn mark_ready(db: &DatabaseHandle) {
        db.state_handle().set_ready();
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            "Database runtime finalized and marked ready"
        );
    }

    fn sweep_checkpoint_artifacts(
        db: &DatabaseHandle,
        checkpoint: CheckpointConfigOptions,
    ) -> anyhow::Result<()> {
        let storage = db.storage_lock().read();
        let Some(sm) = storage.as_ref() else {
            return Ok(());
        };
        let report = RetentionCoordinator::sweep_startup_orphans(
            db.catalog().as_ref(),
            sm.as_ref(),
            db.tablet_meta_manager(),
            checkpoint,
        )?;
        if report != crate::checkpoint::artifact_gc::ArtifactGcReport::default() {
            tracing::info!(
                target: targets::CHECKPOINT,
                db = %db.name(),
                removed_graph_dirs = report.removed_graph_dirs,
                removed_staging_entries = report.removed_staging_entries,
                removed_compaction_dirs = report.removed_compaction_dirs,
                removed_txn_spill_artifacts = report.removed_txn_spill_artifacts,
                removed_txn_spill_manifest_dirs = report.removed_txn_spill_manifest_dirs,
                "Removed orphan checkpoint-related artifacts during startup"
            );
        }
        Ok(())
    }

    fn sync_transaction_clock(db: &DatabaseHandle, recovery_commit_floor: u64) -> u64 {
        let txn = CatalogSnapshot::default();
        let mut max_committed_version = 0u64;

        for schema_entry in db
            .catalog()
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
                let Some(storage) = table.get_storage() else {
                    continue;
                };
                let version = storage.max_version();
                if version >= 0 {
                    max_committed_version = max_committed_version.max(version as u64);
                }
            }
        }

        let runtime_commit_floor = max_committed_version.max(recovery_commit_floor);
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            max_committed_version,
            recovery_commit_floor,
            runtime_commit_floor,
            stage = "reconcile",
            "Synchronized transaction clock with recovered storage version"
        );
        runtime_commit_floor
    }
}
