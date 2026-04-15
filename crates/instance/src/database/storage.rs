// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Database storage backend for a managed database.
//!
//! Concrete `StorageManager` implementation backed by TabletMetaManager + WAL.

use crate::database::storage_identity::{DatabaseStorageIdentity, DATABASE_STORAGE_IDENTITY_KEY};
use crate::storage_manager::{
    wal_path_with_suffix, CheckpointOptions, DatabaseSize, MetadataBlockInfo, StorageCommitState,
    StorageManager, CHECKPOINT_WAL_SUFFIX, MAIN_WAL_SUFFIX, RECOVERY_WAL_SUFFIX,
};
use paro_common::error::{self as paro_error, Result};
use paro_storage::buffer::BufferManager;
use paro_storage::meta::{
    FileMetadataStore, MetadataOp, MetadataStore, StorageManifest, TabletMetaManager,
};
use paro_storage::wal::wal_entry::WalHeaderMetadata;
use paro_storage::wal::write_ahead_log::WriteAheadLog;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_CHECKPOINT_WAL_SIZE: u64 = 1 << 24;

const DEFAULT_PARALLEL_TABLET_LOAD: usize = 4;
const DEFAULT_WAL_BASENAME: &str = "db";

/// Concrete storage backend for one managed database.
pub struct DatabaseStorage {
    /// Database root path.
    path: String,
    /// Whether the storage is in-memory.
    in_memory: bool,
    /// Read-only mode.
    read_only: bool,
    /// Whether metadata/storage is loaded.
    loaded: bool,
    /// Optional WAL handle.
    wal: Option<Arc<WriteAheadLog>>,
    /// WAL mutex for synchronization.
    wal_lock: Mutex<()>,
    /// Metadata store.
    metadata_store: Option<Arc<dyn MetadataStore>>,
    /// Tablet metadata manager.
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    /// Buffer manager reserved for future storage integrations.
    buffer_manager: Option<Arc<dyn BufferManager>>,
    /// Estimated WAL size in bytes.
    wal_size: AtomicU64,
    /// WAL keep-from retention threshold.
    wal_keep_from: AtomicU64,
    /// Checkpoint WAL threshold.
    checkpoint_wal_size: u64,
    /// Storage identity persisted in metadata and mirrored into WAL headers.
    storage_identity: Option<DatabaseStorageIdentity>,
    /// WAL header metadata used for new WAL creation.
    wal_header_metadata: WalHeaderMetadata,
}

impl std::fmt::Debug for DatabaseStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseStorage")
            .field("path", &self.path)
            .field("in_memory", &self.in_memory)
            .field("read_only", &self.read_only)
            .field("loaded", &self.loaded)
            .field("has_wal", &self.wal.is_some())
            .field("has_metadata_store", &self.metadata_store.is_some())
            .field(
                "has_tablet_meta_manager",
                &self.tablet_meta_manager.is_some(),
            )
            .field("has_buffer_manager", &self.buffer_manager.is_some())
            .field("wal_size", &self.wal_size.load(Ordering::Relaxed))
            .field("wal_keep_from", &self.wal_keep_from.load(Ordering::Acquire))
            .field("checkpoint_wal_size", &self.checkpoint_wal_size)
            .field("storage_identity", &self.storage_identity)
            .field("wal_header_metadata", &self.wal_header_metadata)
            .finish()
    }
}

impl DatabaseStorage {
    /// Create a new persistent database storage backend.
    pub fn new(path: String, buffer_manager: Arc<dyn BufferManager>) -> Self {
        Self {
            path,
            in_memory: false,
            read_only: false,
            loaded: false,
            wal: None,
            wal_lock: Mutex::new(()),
            metadata_store: None,
            tablet_meta_manager: None,
            buffer_manager: Some(buffer_manager),
            wal_size: AtomicU64::new(0),
            wal_keep_from: AtomicU64::new(u64::MAX),
            checkpoint_wal_size: DEFAULT_CHECKPOINT_WAL_SIZE,
            storage_identity: None,
            wal_header_metadata: WalHeaderMetadata::default(),
        }
    }

    pub(crate) fn new_in_memory() -> Self {
        Self {
            path: ":memory:".to_string(),
            in_memory: true,
            read_only: false,
            loaded: false,
            wal: None,
            wal_lock: Mutex::new(()),
            metadata_store: None,
            tablet_meta_manager: None,
            buffer_manager: None,
            wal_size: AtomicU64::new(0),
            wal_keep_from: AtomicU64::new(u64::MAX),
            checkpoint_wal_size: DEFAULT_CHECKPOINT_WAL_SIZE,
            storage_identity: None,
            wal_header_metadata: WalHeaderMetadata::default(),
        }
    }

    /// Set read-only mode.
    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    /// Set the checkpoint WAL size threshold.
    pub fn set_checkpoint_wal_size(&mut self, size: u64) {
        self.checkpoint_wal_size = size;
    }

    fn storage_root_path(&self) -> Option<PathBuf> {
        if self.in_memory {
            return None;
        }
        let base_path = self.path.split('?').next().unwrap_or(&self.path);
        if base_path.is_empty() || base_path == ":memory:" {
            None
        } else {
            Some(PathBuf::from(base_path))
        }
    }

    fn tablets_dir(&self) -> Option<PathBuf> {
        self.storage_root_path().map(|root| root.join("tablets"))
    }

    fn meta_dir(&self) -> Option<PathBuf> {
        self.storage_root_path().map(|root| root.join("meta"))
    }

    fn wal_dir(&self) -> Option<PathBuf> {
        self.storage_root_path().map(|root| root.join("wal"))
    }

    fn wal_basename(&self) -> Option<String> {
        self.storage_root_path()
            .and_then(|root| {
                root.file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .filter(|name| !name.is_empty())
    }

    fn wal_path_in_dir_with_suffix(&self, suffix: &str) -> Option<PathBuf> {
        let wal_dir = self.wal_dir()?;
        let wal_basename = self
            .wal_basename()
            .unwrap_or_else(|| DEFAULT_WAL_BASENAME.to_string());
        Some(wal_dir.join(format!("{}{}", wal_basename, suffix)))
    }

    fn wal_path_with_suffix(&self, suffix: &str) -> String {
        if self.in_memory {
            return wal_path_with_suffix(self.get_path(), suffix);
        }

        self.wal_path_in_dir_with_suffix(suffix)
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| wal_path_with_suffix(self.get_path(), suffix))
    }

    fn ensure_storage_layout(&self) -> Result<()> {
        let Some(root) = self.storage_root_path() else {
            return Ok(());
        };

        if root.exists() && root.is_file() {
            return Err(paro_error::invalid_input(format!(
                "Paro storage path must be a directory, got file: {}",
                root.display()
            )));
        }

        std::fs::create_dir_all(&root).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create storage root directory {}: {}",
                root.display(),
                e
            ))
        })?;

        for dir in [self.tablets_dir(), self.meta_dir(), self.wal_dir()]
            .into_iter()
            .flatten()
        {
            std::fs::create_dir_all(&dir).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create storage sub-directory {}: {}",
                    dir.display(),
                    e
                ))
            })?;
        }

        Ok(())
    }

    fn ensure_metadata_components(&mut self) -> Result<()> {
        if self.metadata_store.is_some() && self.tablet_meta_manager.is_some() {
            return Ok(());
        }

        if self.in_memory {
            let store: Arc<dyn MetadataStore> = Arc::new(InMemoryMetadataStore::default());
            self.metadata_store = Some(store.clone());
            self.tablet_meta_manager = Some(Arc::new(TabletMetaManager::with_store(store)));
            return Ok(());
        }

        self.ensure_storage_layout()?;
        let meta_root = self
            .meta_dir()
            .ok_or_else(|| paro_error::internal("Missing storage metadata root"))?;
        let data_root = self
            .tablets_dir()
            .ok_or_else(|| paro_error::internal("Missing storage tablets root"))?;
        let store: Arc<dyn MetadataStore> = Arc::new(FileMetadataStore::new(meta_root)?);
        let tablet_meta_manager = Arc::new(TabletMetaManager::with_store_and_data_root(
            store.clone(),
            data_root,
        ));
        self.metadata_store = Some(store);
        self.tablet_meta_manager = Some(tablet_meta_manager);
        Ok(())
    }

    fn ensure_manifest_initialized(&self) -> Result<()> {
        let Some(tablet_meta_manager) = self.tablet_meta_manager.as_ref() else {
            return Ok(());
        };

        if tablet_meta_manager.load_storage_manifest()?.is_none() {
            tablet_meta_manager.rebuild_storage_manifest()?;
        }
        Ok(())
    }

    fn validate_manifest_integrity(&self) -> Result<()> {
        let Some(tablet_meta_manager) = self.tablet_meta_manager.as_ref() else {
            return Ok(());
        };

        let startup_tablets =
            tablet_meta_manager.load_startup_tablets(DEFAULT_PARALLEL_TABLET_LOAD)?;
        for tablet_meta in startup_tablets {
            let data_dir = PathBuf::from(tablet_meta.data_dir());
            if !data_dir.exists() {
                return Err(paro_error::internal(format!(
                    "Tablet {} data directory missing: {}",
                    tablet_meta.tablet_id(),
                    data_dir.display()
                )));
            }
        }
        Ok(())
    }

    fn refresh_wal_size_from_disk(&self) {
        let wal_path = PathBuf::from(self.get_wal_path());
        let size = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
        self.set_wal_size(size);
    }

    fn storage_root_display(&self) -> String {
        self.storage_root_path()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| self.path.clone())
    }

    fn identity_repair_guidance(&self) -> String {
        format!(
            "Repair guidance: remove the orphan directory or repair the storage metadata under {} before retrying startup.",
            self.storage_root_display()
        )
    }

    fn load_storage_identity_from_store(
        &self,
        store: &dyn MetadataStore,
    ) -> Result<DatabaseStorageIdentity> {
        let Some(payload) = store.get(DATABASE_STORAGE_IDENTITY_KEY)? else {
            return Err(paro_error::invalid_input(format!(
                "Storage identity missing for managed database storage {}. {}",
                self.storage_root_display(),
                self.identity_repair_guidance()
            )));
        };

        let identity: DatabaseStorageIdentity = serde_json::from_slice(&payload).map_err(|e| {
            paro_error::invalid_input(format!(
                "Storage identity invalid for managed database storage {}: {}. {}",
                self.storage_root_display(),
                e,
                self.identity_repair_guidance()
            ))
        })?;
        identity.validate().map_err(|e| {
            paro_error::invalid_input(format!(
                "Storage identity invalid for managed database storage {}: {}. {}",
                self.storage_root_display(),
                e,
                self.identity_repair_guidance()
            ))
        })?;
        Ok(identity)
    }

    pub fn bootstrap_storage_identity(
        &mut self,
        database_id: u64,
    ) -> Result<DatabaseStorageIdentity> {
        if self.in_memory {
            return Err(paro_error::invalid_input(
                "Storage identity bootstrap is not supported for in-memory storage",
            ));
        }

        self.ensure_metadata_components()?;
        let store = self
            .metadata_store
            .as_ref()
            .ok_or_else(|| paro_error::internal("Metadata store unavailable for storage identity"))?
            .clone();

        if store.exists(DATABASE_STORAGE_IDENTITY_KEY)? {
            return Err(paro_error::invalid_input(format!(
                "Storage identity already exists for managed database storage {}",
                self.storage_root_display()
            )));
        }

        let identity = DatabaseStorageIdentity::new(database_id);
        let payload = serde_json::to_vec_pretty(&identity).map_err(|e| {
            paro_error::internal(format!(
                "serialize storage identity for {}: {}",
                database_id, e
            ))
        })?;
        store.durable_put(DATABASE_STORAGE_IDENTITY_KEY, &payload)?;
        self.wal_header_metadata = identity.wal_header_metadata().map_err(|e| {
            paro_error::internal(format!(
                "build WAL header metadata from storage identity for {}: {}",
                database_id, e
            ))
        })?;
        self.storage_identity = Some(identity.clone());
        Ok(identity)
    }

    pub fn load_storage_identity(&mut self) -> Result<DatabaseStorageIdentity> {
        if self.in_memory {
            return Err(paro_error::invalid_input(
                "Storage identity is not available for in-memory storage",
            ));
        }

        self.ensure_metadata_components()?;
        let store = self
            .metadata_store
            .as_ref()
            .ok_or_else(|| paro_error::internal("Metadata store unavailable for storage identity"))?
            .clone();
        let identity = self.load_storage_identity_from_store(store.as_ref())?;
        self.wal_header_metadata = identity.wal_header_metadata().map_err(|e| {
            paro_error::internal(format!(
                "build WAL header metadata from storage identity for {}: {}",
                identity.database_id, e
            ))
        })?;
        self.storage_identity = Some(identity.clone());
        Ok(identity)
    }

    pub fn validate_storage_identity(
        &mut self,
        expected_database_id: u64,
    ) -> Result<DatabaseStorageIdentity> {
        let identity = self.load_storage_identity()?;
        if identity.database_id != expected_database_id {
            return Err(paro_error::invalid_input(format!(
                "Storage identity mismatch for managed database storage {}: instance catalog expects database_id {}, but storage metadata belongs to database_id {}. {}",
                self.storage_root_display(),
                expected_database_id,
                identity.database_id,
                self.identity_repair_guidance()
            )));
        }
        Ok(identity)
    }

    pub fn validate_loaded_wal_identity(&self) -> Result<()> {
        let Some(identity) = self.storage_identity.as_ref() else {
            return Ok(());
        };
        if self.wal.is_none() {
            return Ok(());
        }
        let actual = self.wal_header_metadata;
        let expected = identity.db_identifier;
        if actual.db_identifier != expected {
            return Err(paro_error::invalid_input(format!(
                "Storage identity mismatch for managed database storage {}: WAL header db_identifier {} does not match storage metadata {}. {}",
                self.storage_root_display(),
                format_db_identifier(actual.db_identifier),
                format_db_identifier(expected),
                self.identity_repair_guidance()
            )));
        }
        Ok(())
    }

    /// Initialize WAL for this storage manager.
    pub fn initialize_wal(&mut self) -> Result<()> {
        if self.read_only || self.in_memory {
            return Ok(());
        }

        let wal_path = self.get_wal_path();
        if let Some(parent) = PathBuf::from(&wal_path).parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create WAL parent directory {}: {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let wal = WriteAheadLog::new_with_header_metadata(&wal_path, self.wal_header_metadata)?;
        self.wal = Some(Arc::new(wal));
        self.refresh_wal_size_from_disk();
        Ok(())
    }

    /// Load an existing WAL.
    pub fn load_wal(&mut self) -> Result<()> {
        let wal_path = self.get_wal_path();
        let wal_path_buf = PathBuf::from(&wal_path);
        if wal_path_buf.exists() {
            let wal = WriteAheadLog::new(&wal_path)?;
            if let Some(mut reader) = paro_storage::wal::wal_reader::WalReader::open(&wal_path)? {
                reader.ensure_header_read()?;
                self.wal_header_metadata = reader.header_metadata();
            } else {
                self.wal_header_metadata = WalHeaderMetadata::default();
            }
            self.wal = Some(Arc::new(wal));
            self.refresh_wal_size_from_disk();
        } else {
            self.wal = None;
            self.set_wal_size(0);
        }
        Ok(())
    }

    /// Create a new empty Paro storage.
    pub fn create_new(&mut self) -> Result<()> {
        if !self.in_memory {
            let root = self
                .storage_root_path()
                .ok_or_else(|| paro_error::internal("Storage root path is unavailable"))?;
            if root.exists() {
                return Err(paro_error::invalid_input(format!(
                    "Storage directory already exists: {}",
                    root.display()
                )));
            }
        }

        self.ensure_metadata_components()?;
        self.ensure_manifest_initialized()?;
        self.loaded = true;
        Ok(())
    }

    /// Load an existing Paro storage.
    pub fn load_existing(&mut self) -> Result<()> {
        if !self.in_memory {
            let root = self
                .storage_root_path()
                .ok_or_else(|| paro_error::internal("Storage root path is unavailable"))?;
            if !root.exists() {
                return Err(paro_error::invalid_input(format!(
                    "Storage directory does not exist: {}",
                    root.display()
                )));
            }
            if !root.is_dir() {
                return Err(paro_error::invalid_input(format!(
                    "Storage path is not a directory: {}",
                    root.display()
                )));
            }
        }

        self.ensure_metadata_components()?;
        self.ensure_manifest_initialized()?;
        self.validate_manifest_integrity()?;
        self.loaded = true;
        Ok(())
    }
}

impl StorageManager for DatabaseStorage {
    fn get_path(&self) -> &str {
        &self.path
    }

    fn in_memory(&self) -> bool {
        self.in_memory
    }

    fn is_read_only(&self) -> bool {
        self.read_only
    }

    fn is_loaded(&self) -> bool {
        self.loaded
    }

    fn get_wal(&self) -> Option<&WriteAheadLog> {
        self.wal.as_ref().map(|w| w.as_ref())
    }

    fn get_wal_mut(&mut self) -> Option<&mut WriteAheadLog> {
        self.wal.as_mut().and_then(Arc::get_mut)
    }

    fn get_wal_arc(&self) -> Option<Arc<WriteAheadLog>> {
        self.wal.clone()
    }

    fn wal_header_metadata(&self) -> Option<WalHeaderMetadata> {
        self.wal.as_ref().map(|_| self.wal_header_metadata)
    }

    fn replace_wal(&mut self, wal: Arc<WriteAheadLog>) -> Result<()> {
        self.wal_header_metadata = wal.header_metadata();
        self.wal = Some(wal);
        self.refresh_wal_size_from_disk();
        Ok(())
    }

    fn wal_size(&self) -> u64 {
        self.wal_size.load(Ordering::Relaxed)
    }

    fn add_wal_size(&self, size: u64) {
        self.wal_size.fetch_add(size, Ordering::Relaxed);
    }

    fn set_wal_size(&self, size: u64) {
        self.wal_size.store(size, Ordering::Relaxed);
    }

    fn wal_keep_from(&self) -> u64 {
        self.wal_keep_from.load(Ordering::Acquire)
    }

    fn set_wal_keep_from(&self, keep_from: u64) {
        self.wal_keep_from.store(keep_from, Ordering::Release);
    }

    fn get_wal_path(&self) -> String {
        self.wal_path_with_suffix(MAIN_WAL_SUFFIX)
    }

    fn get_checkpoint_wal_path(&self) -> String {
        self.wal_path_with_suffix(CHECKPOINT_WAL_SUFFIX)
    }

    fn get_recovery_wal_path(&self) -> String {
        self.wal_path_with_suffix(RECOVERY_WAL_SUFFIX)
    }

    fn get_metadata_store(&self) -> Option<&dyn MetadataStore> {
        self.metadata_store.as_deref()
    }

    fn get_metadata_store_arc(&self) -> Option<Arc<dyn MetadataStore>> {
        self.metadata_store.clone()
    }

    fn get_tablet_meta_manager(&self) -> Option<Arc<TabletMetaManager>> {
        self.tablet_meta_manager.clone()
    }

    fn create_checkpoint(&self, options: CheckpointOptions) -> Result<()> {
        let _lock = self
            .wal_lock
            .lock()
            .map_err(|_| paro_error::internal("WAL lock poisoned"))?;

        if !options.force && self.wal_size() == 0 {
            return Ok(());
        }

        if let Some(tablet_meta_manager) = &self.tablet_meta_manager {
            tablet_meta_manager.rebuild_storage_manifest()?;
        }

        if let Some(wal) = self.wal.as_ref() {
            let had_content = wal.start_checkpoint(0)?;
            if had_content {
                wal.finish_checkpoint()?;
            }
        }

        self.set_wal_size(0);
        Ok(())
    }

    fn automatic_checkpoint(&self, estimated_wal_bytes: u64) -> bool {
        if self.read_only {
            return false;
        }
        self.wal_size() + estimated_wal_bytes >= self.checkpoint_wal_size
    }

    fn gen_storage_commit_state(&self, transaction_id: u64) -> Box<dyn StorageCommitState> {
        Box::new(crate::storage_manager::SingleFileStorageCommitState::new(
            self.wal.clone(),
            transaction_id,
        ))
    }

    fn get_database_size(&self) -> DatabaseSize {
        let mut size = DatabaseSize::default();
        if self.in_memory {
            size.wal_size = self.wal_size();
            return size;
        }

        if let Some(tablets_dir) = self.tablets_dir() {
            size.bytes = size
                .bytes
                .saturating_add(recursive_dir_size(&tablets_dir).unwrap_or(0));
        }
        if let Some(meta_dir) = self.meta_dir() {
            size.bytes = size
                .bytes
                .saturating_add(recursive_dir_size(&meta_dir).unwrap_or(0));
        }

        size.wal_size = self.wal_size();
        size
    }

    fn get_metadata_info(&self) -> Vec<MetadataBlockInfo> {
        let mut info = Vec::new();
        if let Some(store) = self.metadata_store.as_ref() {
            if let Ok(entries) = store.scan_prefix("tablet/") {
                if !entries.is_empty() {
                    info.push(MetadataBlockInfo {
                        block_id: 0,
                        block_type: "tablet_metadata".to_string(),
                        entry_count: u64::try_from(entries.len()).unwrap_or(u64::MAX),
                    });
                }
            }
            if let Ok(Some(manifest)) = store.get(TabletMetaManager::manifest_key()) {
                if let Ok(decoded) = StorageManifest::from_json_bytes(&manifest) {
                    info.push(MetadataBlockInfo {
                        block_id: 1,
                        block_type: "manifest".to_string(),
                        entry_count: u64::try_from(decoded.tablets.len()).unwrap_or(u64::MAX),
                    });
                }
            }
        }
        info
    }

    fn initialize(&mut self) -> Result<()> {
        if self.in_memory {
            self.create_new()?;
            return Ok(());
        }

        let path = self
            .storage_root_path()
            .ok_or_else(|| paro_error::internal("Storage root path is unavailable"))?;

        if path.exists() {
            self.load_existing()?;
            self.load_wal()?;
        } else {
            self.create_new()?;
            self.initialize_wal()?;
        }

        Ok(())
    }

    fn destroy(&mut self) {
        self.wal = None;
        self.metadata_store = None;
        self.tablet_meta_manager = None;
        self.loaded = false;
        self.storage_identity = None;
        self.wal_header_metadata = WalHeaderMetadata::default();
    }
}

/// In-memory Paro storage manager.
#[derive(Debug)]
pub struct InMemoryDatabaseStorage {
    inner: DatabaseStorage,
}

impl InMemoryDatabaseStorage {
    pub fn new() -> Self {
        Self {
            inner: DatabaseStorage::new_in_memory(),
        }
    }
}

impl Default for InMemoryDatabaseStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageManager for InMemoryDatabaseStorage {
    fn get_path(&self) -> &str {
        self.inner.get_path()
    }

    fn in_memory(&self) -> bool {
        true
    }

    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    fn is_loaded(&self) -> bool {
        self.inner.is_loaded()
    }

    fn get_wal(&self) -> Option<&WriteAheadLog> {
        self.inner.get_wal()
    }

    fn get_wal_mut(&mut self) -> Option<&mut WriteAheadLog> {
        self.inner.get_wal_mut()
    }

    fn get_wal_arc(&self) -> Option<Arc<WriteAheadLog>> {
        self.inner.get_wal_arc()
    }

    fn wal_header_metadata(&self) -> Option<WalHeaderMetadata> {
        self.inner.wal_header_metadata()
    }

    fn replace_wal(&mut self, wal: Arc<WriteAheadLog>) -> Result<()> {
        self.inner.replace_wal(wal)
    }

    fn wal_size(&self) -> u64 {
        self.inner.wal_size()
    }

    fn add_wal_size(&self, size: u64) {
        self.inner.add_wal_size(size)
    }

    fn set_wal_size(&self, size: u64) {
        self.inner.set_wal_size(size)
    }

    fn wal_keep_from(&self) -> u64 {
        self.inner.wal_keep_from()
    }

    fn set_wal_keep_from(&self, keep_from: u64) {
        self.inner.set_wal_keep_from(keep_from)
    }

    fn get_wal_path(&self) -> String {
        self.inner.get_wal_path()
    }

    fn get_checkpoint_wal_path(&self) -> String {
        self.inner.get_checkpoint_wal_path()
    }

    fn get_recovery_wal_path(&self) -> String {
        self.inner.get_recovery_wal_path()
    }

    fn get_metadata_store(&self) -> Option<&dyn MetadataStore> {
        self.inner.get_metadata_store()
    }

    fn get_metadata_store_arc(&self) -> Option<Arc<dyn MetadataStore>> {
        self.inner.get_metadata_store_arc()
    }

    fn get_tablet_meta_manager(&self) -> Option<Arc<TabletMetaManager>> {
        self.inner.get_tablet_meta_manager()
    }

    fn create_checkpoint(&self, options: CheckpointOptions) -> Result<()> {
        self.inner.create_checkpoint(options)
    }

    fn automatic_checkpoint(&self, estimated_wal_bytes: u64) -> bool {
        self.inner.automatic_checkpoint(estimated_wal_bytes)
    }

    fn gen_storage_commit_state(&self, transaction_id: u64) -> Box<dyn StorageCommitState> {
        self.inner.gen_storage_commit_state(transaction_id)
    }

    fn get_database_size(&self) -> DatabaseSize {
        self.inner.get_database_size()
    }

    fn get_metadata_info(&self) -> Vec<MetadataBlockInfo> {
        self.inner.get_metadata_info()
    }

    fn initialize(&mut self) -> Result<()> {
        self.inner.initialize()
    }

    fn destroy(&mut self) {
        self.inner.destroy();
    }
}

#[derive(Debug, Default)]
struct InMemoryMetadataStore {
    data: parking_lot::RwLock<HashMap<String, Vec<u8>>>,
}

impl MetadataStore for InMemoryMetadataStore {
    fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(paro_error::invalid_input("metadata key cannot be empty"));
        }
        self.data.write().insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.data.read().get(key).cloned())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.data.write().remove(key);
        Ok(())
    }

    fn write_batch(&self, ops: &[MetadataOp]) -> Result<()> {
        let mut guard = self.data.write();
        for op in ops {
            match op {
                MetadataOp::Put { key, value } => {
                    guard.insert(key.clone(), value.clone());
                }
                MetadataOp::Delete { key } => {
                    guard.remove(key);
                }
            }
        }
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut rows: Vec<(String, Vec<u8>)> = self
            .data
            .read()
            .iter()
            .filter_map(|(k, v)| {
                if k.starts_with(prefix) {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(rows)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.data.read().contains_key(key))
    }
}

fn recursive_dir_size(root: &Path) -> std::io::Result<u64> {
    if !root.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(recursive_dir_size(&path)?);
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn format_db_identifier(identifier: [u8; 16]) -> String {
    identifier
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_buffer_manager() -> Arc<dyn BufferManager> {
        Arc::new(paro_storage::buffer::StandardBufferManager::new(
            8 * 1024 * 1024,
            paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE,
            8,
        ))
    }

    #[test]
    fn create_new_initializes_storage_layout() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db");
        let buffer_manager = test_buffer_manager();
        let mut storage =
            DatabaseStorage::new(db_path.to_string_lossy().to_string(), buffer_manager);

        storage.create_new().unwrap();

        assert!(db_path.exists());
        assert!(db_path.join("tablets").exists());
        assert!(db_path.join("meta").exists());
        assert!(db_path.join("wal").exists());
        assert!(storage.get_metadata_store().is_some());
        assert!(storage.get_tablet_meta_manager().is_some());
        assert!(storage.is_loaded());
    }

    #[test]
    fn create_new_rejects_existing_storage_root() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db");
        std::fs::create_dir_all(&db_path).unwrap();

        let buffer_manager = test_buffer_manager();
        let mut storage =
            DatabaseStorage::new(db_path.to_string_lossy().to_string(), buffer_manager);

        let err = storage
            .create_new()
            .expect_err("existing storage root should fail create_new");
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn in_memory_storage_initializes_metadata_store() {
        let mut storage = InMemoryDatabaseStorage::new();
        storage.initialize().unwrap();
        assert!(storage.in_memory());
        assert!(storage.get_metadata_store().is_some());
        assert!(storage.get_tablet_meta_manager().is_some());
    }

    #[test]
    fn persistent_storage_defaults_match_expected_state() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db");
        let storage =
            DatabaseStorage::new(db_path.to_string_lossy().to_string(), test_buffer_manager());

        assert_eq!(storage.get_path(), db_path.to_string_lossy().as_ref());
        assert!(!storage.in_memory());
        assert!(!storage.is_read_only());
        assert!(!storage.is_loaded());
        assert!(!storage.has_wal());
    }

    #[test]
    fn wal_size_and_keep_from_operations_work() {
        let storage = InMemoryDatabaseStorage::new();
        assert_eq!(storage.wal_size(), 0);

        storage.add_wal_size(100);
        assert_eq!(storage.wal_size(), 100);
        storage.add_wal_size(50);
        assert_eq!(storage.wal_size(), 150);
        storage.set_wal_size(0);
        assert_eq!(storage.wal_size(), 0);

        assert_eq!(storage.wal_keep_from(), u64::MAX);
        storage.set_wal_keep_from(0);
        assert_eq!(storage.wal_keep_from(), 0);
        storage.set_wal_keep_from(42);
        assert_eq!(storage.wal_keep_from(), 42);
    }

    #[test]
    fn automatic_checkpoint_respects_threshold_and_read_only_mode() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db");
        let mut storage =
            DatabaseStorage::new(db_path.to_string_lossy().to_string(), test_buffer_manager());
        storage.set_checkpoint_wal_size(1000);

        storage.set_wal_size(500);
        assert!(!storage.automatic_checkpoint(499));
        assert!(storage.automatic_checkpoint(500));

        storage.set_read_only(true);
        assert!(!storage.automatic_checkpoint(u64::MAX));
    }

    #[test]
    fn wal_paths_match_storage_manager_suffixes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db");
        let storage =
            DatabaseStorage::new(db_path.to_string_lossy().to_string(), test_buffer_manager());

        let wal_dir = db_path.join("wal");
        assert_eq!(
            storage.get_wal_path(),
            wal_dir.join("db.wal").to_string_lossy()
        );
        assert_eq!(
            storage.get_checkpoint_wal_path(),
            wal_dir.join("db.checkpoint.wal").to_string_lossy()
        );
        assert_eq!(
            storage.get_recovery_wal_path(),
            wal_dir.join("db.recovery.wal").to_string_lossy()
        );
    }
}
