use crate::database::catalog_checkpoint::CatalogCheckpoint;
use crate::database::compaction_driver::{CompactionDriver, CompactionSuspendGuard};
use crate::database::storage::DEFAULT_CHECKPOINT_WAL_SIZE;
use crate::storage_manager::StorageManager;
use parking_lot::{RwLock, RwLockReadGuard};
use paro_catalog::database_catalog::ParoCatalog;
use paro_common::logging::targets;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub struct Checkpointer {
    checkpoint_wal_size: AtomicU64,
    checkpoint_in_progress: AtomicBool,
    checkpoint_success_total: AtomicU64,
    checkpoint_failure_total: AtomicU64,
}

pub struct CheckpointInProgressGuard<'a> {
    flag: &'a AtomicBool,
}

pub struct CheckpointContext<'a> {
    _compaction_guard: Option<CompactionSuspendGuard>,
    _checkpoint_guard: CheckpointInProgressGuard<'a>,
    storage: RwLockReadGuard<'a, Option<Box<dyn StorageManager>>>,
}

impl Drop for CheckpointInProgressGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

impl<'a> CheckpointContext<'a> {
    pub fn acquire(
        checkpointer: &'a Checkpointer,
        compaction: &CompactionDriver,
        storage_lock: &'a RwLock<Option<Box<dyn StorageManager>>>,
        db_name: &str,
    ) -> Option<Self> {
        let compaction_guard = compaction.suspend(db_name, "checkpoint");
        let checkpoint_guard = checkpointer.try_acquire_in_progress()?;
        let storage = storage_lock.read();

        Some(Self {
            _compaction_guard: compaction_guard,
            _checkpoint_guard: checkpoint_guard,
            storage,
        })
    }

    pub fn storage(&self) -> Option<&dyn StorageManager> {
        self.storage.as_ref().map(|storage| storage.as_ref())
    }
}

impl Checkpointer {
    pub fn new() -> Self {
        Self {
            checkpoint_wal_size: AtomicU64::new(DEFAULT_CHECKPOINT_WAL_SIZE),
            checkpoint_in_progress: AtomicBool::new(false),
            checkpoint_success_total: AtomicU64::new(0),
            checkpoint_failure_total: AtomicU64::new(0),
        }
    }

    pub fn should_checkpoint(
        &self,
        has_wal: bool,
        wal_size: u64,
        estimated_wal_bytes: u64,
    ) -> bool {
        if self.checkpoint_in_progress.load(Ordering::Acquire) {
            return false;
        }

        if !has_wal {
            return false;
        }

        wal_size + estimated_wal_bytes >= self.checkpoint_wal_size.load(Ordering::Acquire)
    }

    pub fn is_in_progress(&self) -> bool {
        self.checkpoint_in_progress.load(Ordering::Acquire)
    }

    pub fn set_checkpoint_wal_size(&self, size: u64) {
        self.checkpoint_wal_size.store(size, Ordering::Release);
    }

    pub fn checkpoint_wal_size(&self) -> u64 {
        self.checkpoint_wal_size.load(Ordering::Acquire)
    }

    pub fn checkpoint_success_total(&self) -> u64 {
        self.checkpoint_success_total.load(Ordering::Relaxed)
    }

    pub fn checkpoint_failure_total(&self) -> u64 {
        self.checkpoint_failure_total.load(Ordering::Relaxed)
    }

    pub fn try_acquire_in_progress(&self) -> Option<CheckpointInProgressGuard<'_>> {
        self.checkpoint_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| CheckpointInProgressGuard {
                flag: &self.checkpoint_in_progress,
            })
    }

    pub fn checkpoint_if_needed(
        &self,
        storage_lock: &RwLock<Option<Box<dyn StorageManager>>>,
        compaction: &CompactionDriver,
        catalog: &ParoCatalog,
        db_name: &str,
        force: bool,
    ) -> anyhow::Result<bool> {
        {
            let storage = storage_lock.read();
            let has_wal = storage.as_ref().map(|sm| sm.has_wal()).unwrap_or(false);
            let wal_size = storage.as_ref().map(|sm| sm.wal_size()).unwrap_or(0);
            if !force && !self.should_checkpoint(has_wal, wal_size, 0) {
                return Ok(false);
            }
        }

        let Some(ctx) = CheckpointContext::acquire(self, compaction, storage_lock, db_name) else {
            tracing::debug!(
                target: targets::CHECKPOINT,
                db = %db_name,
                "Checkpoint already in progress, skipping"
            );
            return Ok(false);
        };

        let result = self.execute(ctx, catalog, db_name);
        self.record_checkpoint_outcome(&result);
        result.map(|_| true)
    }

    pub(crate) fn record_checkpoint_outcome(&self, result: &anyhow::Result<()>) {
        if result.is_ok() {
            self.checkpoint_success_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.checkpoint_failure_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn execute(
        &self,
        ctx: CheckpointContext<'_>,
        catalog: &ParoCatalog,
        db_name: &str,
    ) -> anyhow::Result<()> {
        let sm = match ctx.storage() {
            Some(storage) => storage,
            None => return Ok(()),
        };

        let metadata_store = sm
            .get_metadata_store_arc()
            .ok_or_else(|| anyhow::anyhow!("MetadataStore not available for checkpoint"))?;

        tracing::info!(
            target: targets::CHECKPOINT,
            db = %db_name,
            "Starting database checkpoint with WAL coordination"
        );

        let catalog_bytes = CatalogCheckpoint::serialize(catalog)?;
        tracing::info!(
            target: targets::CHECKPOINT,
            db = %db_name,
            bytes = catalog_bytes.len(),
            "Catalog serialized for checkpoint"
        );

        let checkpoint_marker =
            CatalogCheckpoint::write_metadata_batch(metadata_store.as_ref(), catalog_bytes)?;

        let wal_had_content = if let Some(wal) = sm.get_wal_arc() {
            wal.start_checkpoint(checkpoint_marker)
                .map_err(|e| anyhow::anyhow!(e))?
        } else {
            false
        };

        if let Some(tablet_meta_manager) = sm.get_tablet_meta_manager() {
            tablet_meta_manager
                .rebuild_storage_manifest()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        if wal_had_content {
            if let Some(wal) = sm.get_wal_arc() {
                wal.finish_checkpoint().map_err(|e| anyhow::anyhow!(e))?;
            }
        }

        tracing::info!(
            target: targets::CHECKPOINT,
            db = %db_name,
            checkpoint_marker = checkpoint_marker,
            "Database checkpointed with WAL coordination"
        );
        Ok(())
    }
}

impl Default for Checkpointer {
    fn default() -> Self {
        Self::new()
    }
}
