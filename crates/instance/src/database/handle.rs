// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Represents a single database attached to the Paro instance.

use crate::database::checkpointer::{CheckpointContext, Checkpointer};
use crate::database::closer::DatabaseCloser;
use crate::database::compaction_driver::CompactionDriver;
use crate::database::identity::{DatabaseIdentity, DatabaseType};
use crate::database::opener::DatabaseOpener;
use crate::database::state::DatabaseState;
use crate::database::wal_observability::WalLifecycleMetricsSnapshot;
use crate::database::wal_observability::WalObservability;
use crate::storage_manager::StorageManager;
use parking_lot::RwLock;
use paro_catalog::database_catalog::ParoCatalog;
use paro_common::logging::targets;
use paro_scheduler::scheduler::TaskScheduler;
use paro_storage::buffer::BufferPool;
use paro_storage::compaction::compaction_manager::CompactionObservability;
use paro_storage::meta::TabletMetaManager;
use paro_storage::transaction::manager::TransactionManager;
use paro_storage::wal::write_ahead_log::WriteAheadLog;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// State of an attached database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbState {
    /// Being initialized or loaded.
    Opening,
    /// Ready for queries.
    Ready,
    /// Marking for deletion, no new sessions allowed.
    Dropping,
    /// Physically deleted or unmounted.
    Dropped,
}

/// Access mode for database operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessMode {
    /// Automatic mode (determined by context).
    #[default]
    Automatic,
    /// Read-only access.
    ReadOnly,
    /// Read-write access.
    ReadWrite,
}

/// Recovery mode for database durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryMode {
    /// Standard ACID crash recovery mode (default).
    #[default]
    Default,
    /// Disables WAL writes, disabling the D in ACID.
    /// Use with caution as it disables recovery from crashes.
    NoWalWrites,
}

/// Action to take when closing a database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatabaseCloseAction {
    /// Throws if checkpoint fails. Always cleans up.
    #[default]
    Checkpoint,
    /// Does not throw when failing a checkpoint. Always cleans up.
    TryCheckpoint,
}

/// Visibility of an attached database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AttachVisibility {
    /// Database is shown in listings.
    #[default]
    Shown,
    /// Database is hidden from listings.
    Hidden,
}

/// Options for attaching a database.
///
#[derive(Debug, Clone)]
pub struct AttachOptions {
    /// Access mode for the database.
    pub access_mode: AccessMode,
    /// Recovery mode for durability.
    pub recovery_mode: RecoveryMode,
    /// Database type (e.g., "paro", "sqlite").
    pub db_type: String,
    /// Whether this is the main database.
    pub is_main_database: bool,
    /// Visibility of the attached database.
    pub visibility: AttachVisibility,
    /// Additional key-value options.
    pub options: HashMap<String, String>,
}

impl Default for AttachOptions {
    fn default() -> Self {
        Self {
            access_mode: AccessMode::Automatic,
            recovery_mode: RecoveryMode::Default,
            db_type: "paro".to_string(),
            is_main_database: false,
            visibility: AttachVisibility::Shown,
            options: HashMap::new(),
        }
    }
}

impl AttachOptions {
    /// Create new attach options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create attach options for a read-only database.
    pub fn read_only() -> Self {
        Self {
            access_mode: AccessMode::ReadOnly,
            ..Default::default()
        }
    }

    /// Create attach options for a read-write database.
    pub fn read_write() -> Self {
        Self {
            access_mode: AccessMode::ReadWrite,
            ..Default::default()
        }
    }

    /// Set the access mode.
    pub fn with_access_mode(mut self, mode: AccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    /// Set the recovery mode.
    pub fn with_recovery_mode(mut self, mode: RecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }

    /// Set the database type.
    pub fn with_db_type(mut self, db_type: impl Into<String>) -> Self {
        self.db_type = db_type.into();
        self
    }

    /// Mark as main database.
    pub fn as_main_database(mut self) -> Self {
        self.is_main_database = true;
        self
    }

    /// Set visibility.
    pub fn with_visibility(mut self, visibility: AttachVisibility) -> Self {
        self.visibility = visibility;
        self
    }

    /// Add a custom option.
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Parse options from a key-value map.
    pub fn from_options(options: HashMap<String, String>, default_access_mode: AccessMode) -> Self {
        let mut result = Self {
            access_mode: default_access_mode,
            ..Default::default()
        };

        for (key, value) in &options {
            match key.to_lowercase().as_str() {
                "readonly" | "read_only" => {
                    if value.to_lowercase() == "true" || value == "1" {
                        result.access_mode = AccessMode::ReadOnly;
                    } else {
                        result.access_mode = AccessMode::ReadWrite;
                    }
                }
                "readwrite" | "read_write" => {
                    if value.to_lowercase() == "true" || value == "1" {
                        result.access_mode = AccessMode::ReadWrite;
                    } else {
                        result.access_mode = AccessMode::ReadOnly;
                    }
                }
                "recovery_mode" => {
                    result.recovery_mode = match value.to_lowercase().as_str() {
                        "no_wal_writes" | "nowalwrites" => RecoveryMode::NoWalWrites,
                        _ => RecoveryMode::Default,
                    };
                }
                "type" => {
                    result.db_type = value.clone();
                }
                _ => {
                    result.options.insert(key.clone(), value.clone());
                }
            }
        }

        result
    }
}

/// An attached database instance.
///
/// This is the primary boundary for SQL queries. A Session points to a
/// specific DatabaseHandle to execute its commands, or can access multiple
/// via the DatabaseRegistry.
///
pub struct DatabaseHandle {
    identity: DatabaseIdentity,
    state: DatabaseState,
    catalog: Arc<ParoCatalog>,
    transaction_manager: Arc<TransactionManager>,
    buffer_pool: Arc<BufferPool>,
    storage_manager: RwLock<Option<Box<dyn StorageManager>>>,
    compaction: CompactionDriver,
    checkpointer: Checkpointer,
    wal_observability: WalObservability,
    last_gc_epoch: AtomicU64,
    last_gc_watermark: AtomicU64,
}

impl std::fmt::Debug for DatabaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseHandle")
            .field("id", &self.id())
            .field("name", &self.name())
            .field("path", &self.path())
            .field("state", &self.state())
            .field("db_type", &self.db_type())
            .field("recovery_mode", &self.recovery_mode())
            .field("wal_keep_from", &self.wal_keep_from())
            .field("has_storage", &self.has_storage_manager())
            .field("has_compaction", &self.has_compaction_manager())
            .field("has_wal", &self.has_wal())
            .finish()
    }
}

impl DatabaseHandle {
    fn catalog_for_path(name: &str, path: &str) -> Arc<ParoCatalog> {
        if path.is_empty() || path == ":memory:" {
            Arc::new(ParoCatalog::new(name.to_string()))
        } else {
            Arc::new(ParoCatalog::with_path(name.to_string(), path.to_string()))
        }
    }

    fn base(
        identity: DatabaseIdentity,
        buffer_pool: Arc<BufferPool>,
        initial_state: DbState,
    ) -> Self {
        let catalog = Self::catalog_for_path(&identity.name, &identity.path);
        Self {
            identity,
            state: DatabaseState::new(initial_state),
            catalog,
            transaction_manager: Arc::new(TransactionManager::new()),
            buffer_pool: buffer_pool.clone(),
            storage_manager: RwLock::new(None),
            compaction: CompactionDriver::new(buffer_pool),
            checkpointer: Checkpointer::new(),
            wal_observability: WalObservability::new(),
            last_gc_epoch: AtomicU64::new(0),
            last_gc_watermark: AtomicU64::new(0),
        }
    }

    /// Create a new attached database with options.
    pub fn new(id: u64, name: String, path: String, buffer_pool: Arc<BufferPool>) -> Self {
        Self::base(
            DatabaseIdentity::new(
                id,
                name,
                path,
                DatabaseType::ReadWrite,
                RecoveryMode::Default,
                AttachVisibility::Shown,
                false,
                HashMap::new(),
            ),
            buffer_pool,
            DbState::Opening,
        )
    }

    /// Create a new attached database with options.
    pub fn with_options(
        id: u64,
        name: String,
        path: String,
        buffer_pool: Arc<BufferPool>,
        options: AttachOptions,
    ) -> Self {
        let db_type = match options.access_mode {
            AccessMode::ReadOnly => DatabaseType::ReadOnly,
            _ => DatabaseType::ReadWrite,
        };

        Self::base(
            DatabaseIdentity::new(
                id,
                name,
                path,
                db_type,
                options.recovery_mode,
                options.visibility,
                options.is_main_database,
                options.options,
            ),
            buffer_pool,
            DbState::Opening,
        )
    }

    /// Create a system database (no storage).
    pub fn new_system(id: u64, buffer_pool: Arc<BufferPool>) -> Self {
        Self::base(
            DatabaseIdentity::new(
                id,
                "system".to_string(),
                ":memory:".to_string(),
                DatabaseType::System,
                RecoveryMode::Default,
                AttachVisibility::Shown,
                false,
                HashMap::new(),
            ),
            buffer_pool,
            DbState::Ready,
        )
    }

    /// Create a temporary database (in-memory, session-scoped).
    pub fn new_temp(id: u64, buffer_pool: Arc<BufferPool>) -> Self {
        Self::base(
            DatabaseIdentity::new(
                id,
                "temp".to_string(),
                ":memory:".to_string(),
                DatabaseType::Temp,
                RecoveryMode::Default,
                AttachVisibility::Shown,
                false,
                HashMap::new(),
            ),
            buffer_pool,
            DbState::Ready,
        )
    }

    // --- State Management ---

    /// Set the database state to Dropping.
    pub fn set_dropping(&self) {
        self.state.set_dropping();
    }

    /// Try to mark the database as Dropping.
    /// Returns true if the state was transitioned from Ready.
    pub fn try_mark_dropping(&self) -> bool {
        self.state.try_mark_dropping()
    }

    /// Check if the database is ready for queries.
    pub fn is_ready(&self) -> bool {
        self.state.is_ready()
    }

    /// Get the current state of the database.
    pub fn state(&self) -> DbState {
        self.state.get()
    }

    // --- Type Queries ---

    /// Check if this is a system database.
    pub fn is_system(&self) -> bool {
        self.identity.db_type.is_system()
    }

    /// Check if this is a temporary database.
    pub fn is_temporary(&self) -> bool {
        self.identity.db_type.is_temporary()
    }

    /// Check if this is a read-only database.
    pub fn is_read_only(&self) -> bool {
        self.identity.db_type.is_read_only()
    }

    /// Check if this is the initial (main) database.
    pub fn is_initial_database(&self) -> bool {
        self.identity.is_initial_database()
    }

    /// Mark this as the initial database.
    pub fn set_initial_database(&self) {
        self.identity.set_initial_database();
    }

    /// Get the database type.
    pub fn db_type(&self) -> DatabaseType {
        self.identity.db_type
    }

    /// Get the recovery mode.
    pub fn recovery_mode(&self) -> RecoveryMode {
        self.identity.recovery_mode
    }

    /// Get the visibility.
    pub fn visibility(&self) -> AttachVisibility {
        self.identity.visibility
    }

    /// Get the attach options.
    pub fn attach_options(&self) -> &HashMap<String, String> {
        &self.identity.attach_options
    }

    /// Check if the database is closed.
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    // --- Storage Management ---

    /// Check if this database has a storage manager.
    pub fn has_storage_manager(&self) -> bool {
        self.storage_manager.read().is_some()
    }

    /// Check if this database has an active compaction manager.
    pub fn has_compaction_manager(&self) -> bool {
        self.compaction.has_manager()
    }

    /// Get compaction observability snapshot for control-plane diagnostics.
    pub fn compaction_observability(&self) -> Option<CompactionObservability> {
        self.compaction.observability()
    }

    /// Bind the instance task scheduler for background maintenance tasks.
    pub fn bind_task_scheduler(&self, scheduler: Arc<TaskScheduler>) {
        self.compaction.bind_scheduler(scheduler);
    }

    /// Sync compaction tablet registry with the currently visible catalog tables.
    pub fn sync_compaction_tablets(&self) -> anyhow::Result<()> {
        self.compaction
            .sync_tablets(&self.catalog, self.name(), self.db_type())
    }

    /// Set the storage manager for persistence.
    ///
    /// The storage manager now contains metadata/tablet manager and WAL.
    pub fn attach_storage(&self, manager: Box<dyn StorageManager>) {
        manager.set_wal_keep_from(self.wal_keep_from());
        let mut storage = self.storage_manager.write();
        *storage = Some(manager);
        drop(storage);

        self.compaction.ensure_started(self.name(), self.db_type());
        if let Err(err) = self.sync_compaction_tablets() {
            tracing::warn!(
                target: targets::INSTANCE,
                db = %self.name(),
                err = %err,
                "Failed to synchronize compaction tablets after storage manager update"
            );
        }
    }

    /// Get metadata store (metadata abstraction entry point).
    pub fn metadata_store(&self) -> Option<Arc<dyn paro_storage::meta::MetadataStore>> {
        let storage = self.storage_manager.read();
        storage.as_ref().and_then(|s| s.get_metadata_store_arc())
    }

    /// Get tablet metadata manager.
    pub fn tablet_meta_manager(&self) -> Option<Arc<TabletMetaManager>> {
        let storage = self.storage_manager.read();
        storage.as_ref().and_then(|s| s.get_tablet_meta_manager())
    }

    /// Get the catalog.
    pub fn catalog(&self) -> &Arc<ParoCatalog> {
        &self.catalog
    }

    pub fn maybe_gc_catalog(&self) -> paro_catalog::collection::CatalogGcStats {
        let current_epoch = self.catalog.gc_epoch();
        let last_epoch = self.last_gc_epoch.load(Ordering::Relaxed);
        if current_epoch == last_epoch {
            return paro_catalog::collection::CatalogGcStats::default();
        }

        let watermark = self.transaction_manager.get_min_active_start_time();
        let last_watermark = self.last_gc_watermark.load(Ordering::SeqCst);
        if watermark <= last_watermark {
            return paro_catalog::collection::CatalogGcStats::default();
        }

        let stats = self.catalog.gc(watermark);
        self.last_gc_epoch
            .store(self.catalog.gc_epoch(), Ordering::Relaxed);
        self.last_gc_watermark.store(watermark, Ordering::SeqCst);
        stats
    }

    /// Get the transaction manager.
    pub fn transaction_manager(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    /// Buffer pool shared with the rest of the instance.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    // --- WAL Management (via StorageManager) ---

    /// Initialize the WAL for this database.
    ///
    /// The WAL lifecycle is managed by the active StorageManager implementation.
    pub fn initialize_wal(&self) -> anyhow::Result<()> {
        // Skip WAL initialization if recovery mode is NoWalWrites
        if self.recovery_mode() == RecoveryMode::NoWalWrites {
            tracing::info!(
                target: targets::WAL,
                db=%self.name(),
                "WAL disabled (NoWalWrites mode)"
            );
            return Ok(());
        }

        let mut storage = self.storage_manager.write();
        if let Some(ref mut sm) = *storage {
            sm.initialize().map_err(|e| anyhow::anyhow!(e))?;
        }

        tracing::info!(
            target: targets::WAL,
            db=%self.name(),
            "WAL initialized via StorageManager"
        );
        Ok(())
    }

    /// Get the WAL for this database.
    ///
    /// WAL is now accessed through the StorageManager.
    pub fn wal(&self) -> Option<Arc<WriteAheadLog>> {
        let storage = self.storage_manager.read();
        storage.as_ref().and_then(|s| s.get_wal_arc())
    }

    /// Run a read-only WAL health check.
    ///
    /// This inspects main/checkpoint/recovery WAL files without mutating state.
    pub fn check_wal_health(
        &self,
    ) -> anyhow::Result<paro_storage::wal::recovery::WalHealthCheckReport> {
        let storage = self.storage_manager.read();
        self.wal_observability.check_wal_health(
            storage.as_ref().map(|sm| sm.as_ref()),
            self.name(),
            self.path(),
        )
    }

    /// Build and cache the latest recovery consistency report for management diagnostics.
    pub fn recovery_consistency_report(
        &self,
    ) -> crate::recovery::consistency_report::RecoveryConsistencyReport {
        self.wal_observability
            .build_and_cache_recovery_report(&self.catalog)
    }

    /// Return the last cached recovery consistency report.
    pub fn last_recovery_consistency_report(
        &self,
    ) -> Option<crate::recovery::consistency_report::RecoveryConsistencyReport> {
        self.wal_observability.last_recovery_report()
    }

    /// Return WAL lifecycle observability snapshot for instance aggregation.
    pub fn wal_lifecycle_metrics(&self) -> WalLifecycleMetricsSnapshot {
        self.wal_observability.snapshot(&self.checkpointer)
    }

    /// Check if this database has a WAL.
    pub fn has_wal(&self) -> bool {
        let storage = self.storage_manager.read();
        storage.as_ref().map(|s| s.has_wal()).unwrap_or(false)
    }

    /// Get the WAL size in bytes.
    pub fn wal_size(&self) -> u64 {
        let storage = self.storage_manager.read();
        storage.as_ref().map(|s| s.wal_size()).unwrap_or(0)
    }

    /// Set WAL keep-from retention threshold.
    pub fn set_wal_keep_from(&self, keep_from: u64) {
        self.wal_observability.set_wal_keep_from(keep_from);

        let storage = self.storage_manager.read();
        if let Some(sm) = storage.as_ref() {
            sm.set_wal_keep_from(keep_from);
        }
    }

    /// Get WAL keep-from retention threshold.
    pub fn wal_keep_from(&self) -> u64 {
        self.wal_observability.wal_keep_from()
    }

    /// Set the checkpoint WAL size threshold.
    ///
    /// When the WAL size exceeds this threshold, an automatic checkpoint
    /// will be triggered after transaction commit.
    pub fn set_checkpoint_wal_size(&self, size: u64) {
        self.checkpointer.set_checkpoint_wal_size(size);
    }

    /// Get the checkpoint WAL size threshold.
    pub fn checkpoint_wal_size(&self) -> u64 {
        self.checkpointer.checkpoint_wal_size()
    }

    /// Get a reference to the storage manager.
    ///
    pub fn storage_manager(
        &self,
    ) -> Option<impl std::ops::Deref<Target = Option<Box<dyn StorageManager>>> + '_> {
        let guard = self.storage_manager.read();
        if guard.is_some() {
            Some(guard)
        } else {
            None
        }
    }

    // --- Checkpoint Management ---

    /// Check if automatic checkpoint should be triggered.
    ///
    /// This method checks if the WAL size has exceeded the configured threshold.
    /// It also considers any estimated additional bytes that will be written.
    ///
    /// # Arguments
    /// * `estimated_wal_bytes` - Estimated additional bytes to be written to WAL
    ///
    /// # Returns
    /// `true` if checkpoint should be triggered, `false` otherwise
    pub fn should_checkpoint(&self, estimated_wal_bytes: u64) -> bool {
        self.checkpointer
            .should_checkpoint(self.has_wal(), self.wal_size(), estimated_wal_bytes)
    }

    /// Check if a checkpoint is currently in progress.
    pub fn is_checkpoint_in_progress(&self) -> bool {
        self.checkpointer.is_in_progress()
    }

    /// Perform automatic checkpoint if needed.
    ///
    /// This method checks if checkpoint should be triggered and performs it
    /// if necessary. It's designed to be called after transaction commit.
    ///
    /// # Arguments
    /// * `force` - If true, force checkpoint regardless of WAL size
    ///
    /// # Returns
    /// * `Ok(true)` - Checkpoint was performed
    /// * `Ok(false)` - Checkpoint was not needed or already in progress
    /// * `Err(...)` - Checkpoint failed
    pub fn checkpoint_if_needed(&self, force: bool) -> anyhow::Result<bool> {
        self.checkpointer.checkpoint_if_needed(
            &self.storage_manager,
            &self.compaction,
            &self.catalog,
            self.name(),
            force,
        )
    }

    /// Checkpoint the database to disk.
    /// This saves the catalog and updates the database header.
    ///
    /// This method always goes through WAL coordination to guarantee a single
    /// checkpoint path (`start_checkpoint -> metadata update -> finish_checkpoint`).
    pub fn checkpoint(&self) -> anyhow::Result<()> {
        let Some(ctx) = CheckpointContext::acquire(
            &self.checkpointer,
            &self.compaction,
            &self.storage_manager,
            self.name(),
        ) else {
            return Err(anyhow::anyhow!("Checkpoint already in progress"));
        };
        let result = self
            .checkpointer
            .execute(ctx, self.catalog.as_ref(), self.name());
        self.checkpointer.record_checkpoint_outcome(&result);
        result
    }

    /// Force a checkpoint regardless of WAL size.
    ///
    /// This is equivalent to the `FORCE CHECKPOINT` SQL command.
    pub fn force_checkpoint(&self) -> anyhow::Result<()> {
        self.checkpoint()
    }

    // --- Lifecycle Management ---

    /// Initialize the database catalog and storage.
    ///
    /// This should be called after creating the database but before using it.
    ///
    ///
    /// 1. Materialize the default schema skeleton
    /// 2. Storage initialization is handled separately via `attach_storage`
    /// 3. WAL recovery/reconciliation is triggered via `finalize_load`
    pub fn initialize(&self) -> anyhow::Result<()> {
        DatabaseOpener::initialize_catalog(&self.catalog)?;
        tracing::debug!(
            target: targets::INSTANCE,
            db=%self.name(),
            "Initialized database catalog skeleton"
        );
        Ok(())
    }

    /// Finalize loading after recovery.
    ///
    /// This should be called after WAL replay to finalize the database state.
    ///
    ///
    /// The finalization flow:
    /// 1. Restore the last catalog checkpoint, if any
    /// 2. Replay WAL only when recovery is needed
    /// 3. Reconcile transaction clock / compaction / observability
    /// 4. Transition the database to `Ready`
    pub fn finalize_load(&self) -> anyhow::Result<()> {
        DatabaseOpener::finalize_load(self)
    }

    /// Close the database.
    ///
    /// This method performs cleanup and optionally checkpoints the database.
    pub fn close(&self, action: DatabaseCloseAction) -> anyhow::Result<()> {
        DatabaseCloser::close(self, action)
    }

    /// Called when the database is detached.
    pub fn on_detach(&self) -> anyhow::Result<()> {
        DatabaseCloser::on_detach(self)
    }

    // --- Name Utilities ---

    /// Check if a name is reserved.
    pub fn name_is_reserved(name: &str) -> bool {
        DatabaseIdentity::name_is_reserved(name)
    }

    /// Extract database name from a path.
    pub fn extract_database_name(dbpath: &str) -> String {
        DatabaseIdentity::extract_database_name(dbpath)
    }

    /// Get the stored path (for path manager integration).
    pub fn stored_path(&self) -> &str {
        &self.identity.path
    }

    /// Returns the database path.
    ///
    /// This is the preferred method in Rust code.
    pub fn path(&self) -> &str {
        &self.identity.path
    }

    /// Returns the database ID.
    ///
    /// This is the preferred method in Rust code.
    pub fn id(&self) -> u64 {
        self.identity.id
    }

    /// Returns the database name.
    ///
    /// This is the preferred method in Rust code.
    pub fn name(&self) -> &str {
        &self.identity.name
    }

    pub(crate) fn state_handle(&self) -> &DatabaseState {
        &self.state
    }

    pub(crate) fn storage_lock(&self) -> &RwLock<Option<Box<dyn StorageManager>>> {
        &self.storage_manager
    }

    pub(crate) fn compaction(&self) -> &CompactionDriver {
        &self.compaction
    }

    pub(crate) fn wal_observability(&self) -> &WalObservability {
        &self.wal_observability
    }

    #[cfg(test)]
    pub(crate) fn checkpointer(&self) -> &Checkpointer {
        &self.checkpointer
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        // Try to close gracefully on drop
        if let Err(e) = self.close(DatabaseCloseAction::TryCheckpoint) {
            tracing::warn!(
                target: targets::INSTANCE,
                db=%self.name(),
                err=%e,
                "Error during database drop"
            );
        }
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
