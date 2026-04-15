use super::catalog_checkpoint::CatalogCheckpoint;
use super::handle::{AttachOptions, DatabaseHandle, RecoveryMode};
use super::hooks::RecoveryHookResult;
use super::storage::{DatabaseStorage, InMemoryDatabaseStorage};
use crate::metadata::instance_catalog::DatabaseRecord;
use crate::storage_manager::StorageManager;
use paro_catalog::catalog::Catalog;
use paro_catalog::collection::CatalogReplaySummary;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::logging::targets;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::{BufferManager, BufferPool};
use paro_storage::wal::wal_entry::WalHeaderMetadata;
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
    pub checkpoint_wal_size: u64,
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
}

#[derive(Debug)]
struct FinalizeLoadInputs {
    wal_path: PathBuf,
    checkpoint_marker: Option<u64>,
    wal_header_metadata: Option<WalHeaderMetadata>,
    wal_keep_from: u64,
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
        Self::finalize_load_with_report(db, 0).map(|_| ())
    }

    pub fn finalize_load_with_report(
        db: &DatabaseHandle,
        checkpoint_wal_size: u64,
    ) -> anyhow::Result<crate::recovery::consistency_report::RecoveryConsistencyReport> {
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            stage = "prepare_recovery",
            "Finalizing database load"
        );

        let _compaction_guard = db.compaction().suspend(db.name(), "recovery");
        let inputs = Self::collect_finalize_inputs(db)?;

        Self::load_catalog_checkpoint(db);

        let replay_summary = if crate::recovery::replay_handler::needs_recovery(&inputs.wal_path) {
            Self::replay_wal(db, &inputs)?
        } else {
            tracing::debug!(
                target: targets::WAL,
                db = %db.name(),
                wal_path = %inputs.wal_path.display(),
                stage = "wal_probe",
                "No WAL recovery needed"
            );
            CatalogReplaySummary::default()
        };

        let report = Self::refresh_recovery_report(db, &inputs.wal_path);
        Self::reconcile_runtime_state(
            db,
            replay_summary.max_catalog_commit_id,
            checkpoint_wal_size,
        )?;
        db.maybe_gc_catalog();
        Ok(report)
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

        let consistency = Self::finalize_load_with_report(&db, context.checkpoint_wal_size)?;
        Ok(DatabaseOpenResult {
            handle: db,
            recovery_report: RecoveryReport {
                consistency,
                hook_results: Vec::new(),
            },
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
            checkpoint_marker: CatalogCheckpoint::marker_from_storage(sm.as_ref())?,
            wal_header_metadata: sm.wal_header_metadata(),
            wal_keep_from: sm.wal_keep_from(),
        })
    }

    fn load_catalog_checkpoint(db: &DatabaseHandle) {
        let metadata_store = {
            let storage = db.storage_lock().read();
            storage.as_ref().and_then(|sm| sm.get_metadata_store_arc())
        };

        let Some(store) = metadata_store else {
            return;
        };

        if let Err(err) = CatalogCheckpoint::load_from_store(
            db.catalog().as_ref(),
            store.as_ref(),
            db.tablet_meta_manager(),
        ) {
            tracing::error!(
                target: targets::CHECKPOINT,
                db = %db.name(),
                err = %err,
                stage = "checkpoint_overlay",
                "Failed to load catalog checkpoint during finalize_load; proceeding to WAL-only recovery"
            );
        }
    }

    fn replay_wal(
        db: &DatabaseHandle,
        inputs: &FinalizeLoadInputs,
    ) -> anyhow::Result<CatalogReplaySummary> {
        tracing::info!(
            target: targets::WAL,
            db = %db.name(),
            wal_path = %inputs.wal_path.display(),
            stage = "wal_replay",
            "WAL recovery needed, replaying entries"
        );

        let (recovered_wal, result, summary) =
            crate::recovery::replay_handler::recover_database_with_checkpoint(
                &inputs.wal_path,
                db.catalog(),
                db.tablet_meta_manager(),
                inputs.checkpoint_marker,
                inputs.wal_header_metadata,
                (inputs.wal_keep_from != u64::MAX).then_some(inputs.wal_keep_from),
            )?;

        tracing::info!(
            target: targets::WAL,
            db = %db.name(),
            entries_replayed = result.entries_replayed,
            all_succeeded = result.all_succeeded,
            stage = "wal_replay",
            "WAL recovery complete"
        );

        let mut storage = db.storage_lock().write();
        let sm = storage
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("StorageManager disappeared during finalize_load"))?;
        sm.replace_wal(Arc::new(recovered_wal))
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(summary)
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
        recovery_catalog_commit_floor: u64,
        checkpoint_wal_size: u64,
    ) -> anyhow::Result<()> {
        Self::sync_transaction_clock(db, recovery_catalog_commit_floor);
        if checkpoint_wal_size != 0 {
            db.set_checkpoint_wal_size(checkpoint_wal_size);
        }
        db.compaction()
            .sync_tablets(db.catalog().as_ref(), db.name(), db.db_type())?;
        Self::mark_ready(db);
        Ok(())
    }

    fn mark_ready(db: &DatabaseHandle) {
        db.state_handle().set_ready();
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            "Database runtime finalized and marked ready"
        );
    }

    fn sync_transaction_clock(db: &DatabaseHandle, recovery_catalog_commit_floor: u64) {
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

        let runtime_commit_floor = max_committed_version.max(recovery_catalog_commit_floor);
        db.transaction_manager()
            .sync_commit_id_with(runtime_commit_floor);
        tracing::debug!(
            target: targets::INSTANCE,
            db = %db.name(),
            max_committed_version,
            recovery_catalog_commit_floor,
            runtime_commit_floor,
            stage = "reconcile",
            "Synchronized transaction clock with recovered storage version"
        );
    }
}
