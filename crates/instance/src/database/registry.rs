// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime database registry for the current process.
//!
//! Durable database membership and lifecycle state live in the instance
//! catalog; this type only tracks runtime-visible handles and allocators.

use crate::builtin::functions::BuiltinFunctions;
use crate::database::handle::{AttachOptions, DatabaseHandle};
use parking_lot::{Mutex, RwLock};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::SchemaEntry;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::logging::targets;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Reserved database names that cannot be used for user databases.
pub const SYSTEM_CATALOG: &str = "system";
pub const TEMP_CATALOG: &str = "temp";

/// Information required to attach a database.
///
#[derive(Debug, Clone)]
pub struct AttachInfo {
    /// Name of the database to attach.
    pub name: String,
    /// Path to the database file or directory.
    pub path: String,
    /// What to do if the database already exists.
    pub on_conflict: OnCreateConflict,
}

impl AttachInfo {
    /// Create a new AttachInfo.
    pub fn new(name: String, path: String) -> Self {
        Self {
            name,
            path,
            on_conflict: OnCreateConflict::Error,
        }
    }

    /// Create AttachInfo with a specific conflict resolution.
    pub fn with_conflict(name: String, path: String, on_conflict: OnCreateConflict) -> Self {
        Self {
            name,
            path,
            on_conflict,
        }
    }
}

/// What to do when attaching a database that already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnCreateConflict {
    /// Throw an error if the database already exists.
    #[default]
    Error,
    /// Ignore if the database already exists.
    Ignore,
    /// Replace the existing database.
    Replace,
}

/// Types of ALTER DATABASE operations.
#[derive(Debug, Clone)]
pub enum AlterDatabaseType {
    /// Rename the database.
    Rename { new_name: String },
}

/// Information for ALTER DATABASE operations.
#[derive(Debug, Clone)]
pub struct AlterDatabaseInfo {
    /// Name of the database to alter.
    pub name: String,
    /// The type of alteration.
    pub alter_type: AlterDatabaseType,
    /// What to do if the database is not found.
    pub if_not_found: OnEntryNotFound,
}

impl AlterDatabaseInfo {
    /// Create a rename operation.
    pub fn rename(name: String, new_name: String) -> Self {
        Self {
            name,
            alter_type: AlterDatabaseType::Rename { new_name },
            if_not_found: OnEntryNotFound::ThrowException,
        }
    }
}

/// Result of attempting to insert a database path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertDatabasePathResult {
    /// Path was successfully inserted.
    Success,
    /// Path already exists in the registry.
    AlreadyExists,
}

/// Action to take when an entry is not found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnEntryNotFound {
    /// Throw an exception if the entry is not found.
    #[default]
    ThrowException,
    /// Return null/None if the entry is not found.
    ReturnNull,
}

/// Manages database file paths to prevent opening the same file twice.
///
#[derive(Debug, Default)]
pub struct DatabaseFilePathManager {
    /// Map of absolute paths to database names.
    paths: RwLock<HashMap<String, String>>,
}

impl DatabaseFilePathManager {
    /// Create a new path manager.
    pub fn new() -> Self {
        Self {
            paths: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a database path.
    ///
    /// Returns Success if the path was inserted, AlreadyExists if it's already registered.
    pub fn insert_path(
        &self,
        path: &str,
        name: &str,
        on_conflict: OnCreateConflict,
    ) -> InsertDatabasePathResult {
        // Skip for in-memory databases
        if path.is_empty() || path == ":memory:" {
            return InsertDatabasePathResult::Success;
        }

        let mut paths = self.paths.write();

        // Normalize path (basic normalization)
        let normalized = Self::normalize_path(path);

        if let Some(existing_name) = paths.get(&normalized) {
            if existing_name != name && on_conflict != OnCreateConflict::Replace {
                return InsertDatabasePathResult::AlreadyExists;
            }
        }

        paths.insert(normalized, name.to_string());
        InsertDatabasePathResult::Success
    }

    /// Remove a database path.
    pub fn remove_path(&self, path: &str) {
        if path.is_empty() || path == ":memory:" {
            return;
        }

        let mut paths = self.paths.write();
        let normalized = Self::normalize_path(path);
        paths.remove(&normalized);
    }

    /// Get all registered paths.
    pub fn get_paths(&self) -> Vec<String> {
        let paths = self.paths.read();
        paths.keys().cloned().collect()
    }

    /// Get the approximate count of registered paths.
    pub fn approx_database_count(&self) -> usize {
        self.paths.read().len()
    }

    /// Normalize a path for comparison.
    fn normalize_path(path: &str) -> String {
        // Basic normalization - in production, this should use std::fs::canonicalize
        path.trim_end_matches('/').to_string()
    }
}

/// Runtime-only registry of attached databases.
///
/// The DatabaseRegistry sits at the root of all attached databases and manages:
/// - The system database (holds built-in functions, types, etc.)
/// - Runtime-visible user databases that have already been recovered/opened
/// - Transaction and query number allocation
/// - OID allocation for catalog entries
/// - Path management to prevent duplicate file opens
///
/// Durable database membership and state live in `InstanceCatalogStore`; this type only tracks the
/// runtime-published handles and name/path lookup tables.
///
#[derive(Debug)]
pub struct DatabaseRegistry {
    /// The system database holds system entries (builtin functions, types, etc.)
    pub system: RwLock<Option<Arc<DatabaseHandle>>>,
    /// Lock for database operations (attach/detach).
    databases_lock: Mutex<()>,
    /// Map of database name (case-insensitive) to DatabaseHandle instance.
    databases: RwLock<HashMap<String, Arc<DatabaseHandle>>>,
    /// Runtime-visible logical database names keyed by durable database_id.
    runtime_names_by_id: RwLock<HashMap<u64, String>>,
    /// The next object id handed out by the next_oid method.
    next_oid: AtomicU64,
    /// The current query number.
    current_query_number: AtomicU64,
    /// The current transaction number.
    current_transaction_id: AtomicU64,
    /// Runtime default database pointer keyed by durable database_id.
    default_database_id: RwLock<Option<u64>>,
    /// Monotonic generation for the visible attached-database set.
    attached_generation: AtomicU64,
    /// Manager for ensuring we never open the same database file twice.
    path_manager: Arc<DatabaseFilePathManager>,
}

impl Default for DatabaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseRegistry {
    /// Create a new DatabaseRegistry.
    pub fn new() -> Self {
        Self {
            system: RwLock::new(None),
            databases_lock: Mutex::new(()),
            databases: RwLock::new(HashMap::new()),
            runtime_names_by_id: RwLock::new(HashMap::new()),
            next_oid: AtomicU64::new(1),
            current_query_number: AtomicU64::new(1),
            current_transaction_id: AtomicU64::new(0),
            default_database_id: RwLock::new(None),
            attached_generation: AtomicU64::new(1),
            path_manager: Arc::new(DatabaseFilePathManager::new()),
        }
    }

    /// Create a DatabaseRegistry with a shared path manager.
    pub fn with_path_manager(path_manager: Arc<DatabaseFilePathManager>) -> Self {
        Self {
            system: RwLock::new(None),
            databases_lock: Mutex::new(()),
            databases: RwLock::new(HashMap::new()),
            runtime_names_by_id: RwLock::new(HashMap::new()),
            next_oid: AtomicU64::new(1),
            current_query_number: AtomicU64::new(1),
            current_transaction_id: AtomicU64::new(0),
            default_database_id: RwLock::new(None),
            attached_generation: AtomicU64::new(1),
            path_manager,
        }
    }

    fn bump_visible_generation(&self) -> u64 {
        self.attached_generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn visible_generation(&self) -> u64 {
        self.attached_generation.load(Ordering::SeqCst)
    }

    // --- System Catalog Initialization ---

    /// Initialize the system catalog of the attached SYSTEM_DATABASE.
    ///
    /// This method registers all built-in functions into the system catalog.
    /// The system database has no persistent storage and holds built-in
    /// functions, types, and other system entries.
    ///
    /// Functions are registered to pg_catalog schema for PostgreSQL compatibility.
    ///
    pub fn initialize_system_catalog(&self) {
        let system = self.system.read();
        if let Some(system_db) = system.as_ref() {
            // Get pg_catalog schema for PostgreSQL compatibility
            // Falls back to public schema if pg_catalog doesn't exist
            let target_schema = self.get_pg_catalog_or_public(system_db);

            if let Some(schema) = target_schema {
                // Built-in functions live in the system catalog.
                BuiltinFunctions::register_all(&schema);
                tracing::info!(
                    target: targets::CATALOG,
                    schema = %schema.base.name,
                    "Registered built-in functions to system catalog"
                );
            }
        }
    }

    /// Get pg_catalog schema, falling back to public if not available.
    fn get_pg_catalog_or_public(&self, db: &Arc<DatabaseHandle>) -> Option<Arc<SchemaEntry>> {
        let txn = CatalogSnapshot::read_only(u64::MAX);
        // Try pg_catalog first (PostgreSQL compatibility)
        if let Ok(pg_catalog) = db.catalog().get_schema(&txn, "pg_catalog") {
            return Some(pg_catalog);
        }
        // Fall back to public schema
        db.catalog().get_schema(&txn, "public").ok()
    }

    // --- Database DDL Operations ---

    /// Attach a new database.
    ///
    /// This is the main entry point for attaching databases via SQL:
    /// `ATTACH DATABASE 'path' AS name`
    ///
    ///
    /// # Arguments
    /// * `info` - Information about the database to attach
    /// * `options` - Options for the attach operation
    /// * `db` - The pre-created DatabaseHandle instance
    ///
    /// # Returns
    /// The attached database on success.
    pub fn attach_database(
        &self,
        info: &AttachInfo,
        options: &AttachOptions,
        db: Arc<DatabaseHandle>,
    ) -> anyhow::Result<Arc<DatabaseHandle>> {
        // Check for reserved names
        if DatabaseHandle::name_is_reserved(&info.name) {
            anyhow::bail!(
                "Attached database name \"{}\" cannot be used because it is a reserved name",
                info.name
            );
        }

        // Check for duplicate paths
        let path_result = self.insert_database_path(info, options);
        if path_result == InsertDatabasePathResult::AlreadyExists {
            match info.on_conflict {
                OnCreateConflict::Error => {
                    anyhow::bail!("Database with path \"{}\" is already attached", info.path);
                }
                OnCreateConflict::Ignore => {
                    // Return existing database
                    if let Some(existing) = self.get_database(&info.name) {
                        return Ok(existing);
                    }
                }
                OnCreateConflict::Replace => {
                    // Will replace below
                }
            }
        }

        // Finalize the attach
        self.finalize_attach(info, db.clone())?;

        Ok(db)
    }

    /// Finalize attaching a database.
    fn finalize_attach(&self, info: &AttachInfo, db: Arc<DatabaseHandle>) -> anyhow::Result<()> {
        let _lock = self.databases_lock.lock();
        let mut dbs = self.databases.write();
        let name = info.name.to_lowercase();

        // Check for existing database
        if let Some(existing) = dbs.get(&name) {
            match info.on_conflict {
                OnCreateConflict::Error => {
                    anyhow::bail!(
                        "Failed to attach database: database with name \"{}\" already exists",
                        info.name
                    );
                }
                OnCreateConflict::Ignore => {
                    return Ok(());
                }
                OnCreateConflict::Replace => {
                    // Remove old database
                    let old_db = existing.clone();
                    self.path_manager.remove_path(old_db.path());
                    // Will be replaced below
                }
            }
        }

        self.install_default_database_if_missing(db.id());

        self.runtime_names_by_id
            .write()
            .insert(db.id(), info.name.clone());
        dbs.insert(name, db);
        self.bump_visible_generation();
        Ok(())
    }

    /// Insert a database path into the path manager.
    fn insert_database_path(
        &self,
        info: &AttachInfo,
        _options: &AttachOptions,
    ) -> InsertDatabasePathResult {
        self.path_manager
            .insert_path(&info.path, &info.name, info.on_conflict)
    }

    /// Detach an existing database.
    ///
    ///
    /// Note: This is similar to unregister_database but also handles path cleanup.
    pub fn detach_database_full(
        &self,
        name: &str,
        if_not_found: OnEntryNotFound,
    ) -> anyhow::Result<Option<Arc<DatabaseHandle>>> {
        // Cannot detach system database
        if name.eq_ignore_ascii_case(SYSTEM_CATALOG) {
            anyhow::bail!("Cannot detach system database");
        }

        if let Some(db) = self.get_database(name) {
            self.ensure_not_runtime_default(&db, name)?;
        }

        // Detach internal
        let db = self.detach_internal(name);
        if db.is_none() {
            return match if_not_found {
                OnEntryNotFound::ThrowException => Err(anyhow::anyhow!(
                    "Failed to detach database with name \"{}\": database not found",
                    name
                )),
                OnEntryNotFound::ReturnNull => Ok(None),
            };
        }

        // Clean up path
        if let Some(ref d) = db {
            self.path_manager.remove_path(d.path());
            // Call on_detach
            let _ = d.on_detach();
        }

        Ok(db)
    }

    /// Internal detach - removes from map only.
    fn detach_internal(&self, name: &str) -> Option<Arc<DatabaseHandle>> {
        let _lock = self.databases_lock.lock();
        let mut dbs = self.databases.write();
        let lower_name = name.to_lowercase();

        let result = dbs.remove(&lower_name);

        if let Some(db) = result.as_ref() {
            self.runtime_names_by_id.write().remove(&db.id());
            self.reassign_default_database_after_removal(db.id(), &dbs);
        }

        if result.is_some() {
            self.bump_visible_generation();
        }

        result
    }

    /// Alter operation dispatcher.
    ///
    /// Handles ALTER DATABASE operations like RENAME.
    ///
    pub fn alter(&self, info: AlterDatabaseInfo) -> anyhow::Result<()> {
        match info.alter_type {
            AlterDatabaseType::Rename { new_name } => {
                self.rename_database(&info.name, &new_name, info.if_not_found)
            }
        }
    }

    // --- Database Lookup ---

    /// Get an attached database by its name.
    ///
    /// # Arguments
    /// * `name` - The name of the database (case-insensitive)
    ///
    /// # Returns
    /// The attached database if found, None otherwise.
    pub fn get_database(&self, name: &str) -> Option<Arc<DatabaseHandle>> {
        let lower_name = name.to_lowercase();

        // Check for system database
        if lower_name == SYSTEM_CATALOG {
            return self.system.read().clone();
        }

        let dbs = self.databases.read();
        dbs.get(&lower_name).cloned()
    }

    pub fn get_database_by_id(&self, database_id: u64) -> Option<Arc<DatabaseHandle>> {
        if let Some(system) = self.system.read().clone() {
            if system.id() == database_id {
                return Some(system);
            }
        }

        let dbs = self.databases.read();
        dbs.values().find(|db| db.id() == database_id).cloned()
    }

    /// Get an attached database by name, with error handling.
    ///
    /// # Arguments
    /// * `name` - The name of the database
    /// * `if_not_found` - Action to take if database is not found
    ///
    /// # Returns
    /// The attached database, or an error if not found and if_not_found is ThrowException.
    pub fn get_database_or_error(
        &self,
        name: &str,
        if_not_found: OnEntryNotFound,
    ) -> anyhow::Result<Option<Arc<DatabaseHandle>>> {
        match self.get_database(name) {
            Some(db) => Ok(Some(db)),
            None => match if_not_found {
                OnEntryNotFound::ThrowException => {
                    Err(anyhow::anyhow!("Database \"{}\" not found", name))
                }
                OnEntryNotFound::ReturnNull => Ok(None),
            },
        }
    }

    /// Register a new database in the registry.
    ///
    /// # Arguments
    /// * `db` - The database to register
    ///
    /// # Returns
    /// Ok(()) if successful, Err if a database with the same name already exists.
    pub fn register_database(&self, db: Arc<DatabaseHandle>) -> anyhow::Result<()> {
        let _lock = self.databases_lock.lock();
        let mut dbs = self.databases.write();
        let name = db.name().to_lowercase();

        if dbs.contains_key(&name) {
            anyhow::bail!("Database already exists: {}", db.name());
        }

        self.install_default_database_if_missing(db.id());

        self.runtime_names_by_id
            .write()
            .insert(db.id(), db.name().to_string());
        dbs.insert(name, db);
        self.bump_visible_generation();
        Ok(())
    }

    /// Unregister a database from the registry.
    ///
    /// # Arguments
    /// * `name` - The name of the database to unregister
    ///
    /// # Returns
    /// The unregistered database if found, None otherwise.
    pub fn unregister_database(&self, name: &str) -> Option<Arc<DatabaseHandle>> {
        let _lock = self.databases_lock.lock();
        let mut dbs = self.databases.write();
        let lower_name = name.to_lowercase();

        let result = dbs.remove(&lower_name);

        if let Some(db) = result.as_ref() {
            self.runtime_names_by_id.write().remove(&db.id());
            self.reassign_default_database_after_removal(db.id(), &dbs);
        }

        if result.is_some() {
            self.bump_visible_generation();
        }

        result
    }

    /// Detach a database from the registry.
    ///
    /// This is similar to unregister_database but with additional validation.
    ///
    /// # Arguments
    /// * `name` - The name of the database to detach
    /// * `if_not_found` - Action to take if database is not found
    pub fn detach_database(
        &self,
        name: &str,
        if_not_found: OnEntryNotFound,
    ) -> anyhow::Result<Option<Arc<DatabaseHandle>>> {
        // Cannot detach system database
        if name.eq_ignore_ascii_case(SYSTEM_CATALOG) {
            anyhow::bail!("Cannot detach system database");
        }

        if let Some(db) = self.get_database(name) {
            self.ensure_not_runtime_default(&db, name)?;
        }

        match self.unregister_database(name) {
            Some(db) => Ok(Some(db)),
            None => match if_not_found {
                OnEntryNotFound::ThrowException => Err(anyhow::anyhow!(
                    "Failed to detach database with name \"{}\": database not found",
                    name
                )),
                OnEntryNotFound::ReturnNull => Ok(None),
            },
        }
    }

    /// Get all registered database names.
    pub fn get_database_names(&self) -> Vec<String> {
        let dbs = self.databases.read();
        dbs.keys().cloned().collect()
    }

    /// Get all attached databases.
    ///
    /// Returns a vector of all databases including the system database.
    pub fn get_databases(&self) -> Vec<Arc<DatabaseHandle>> {
        let mut result = Vec::new();

        let dbs = self.databases.read();
        for db in dbs.values() {
            result.push(db.clone());
        }

        // Add system database
        if let Some(system) = self.system.read().as_ref() {
            result.push(system.clone());
        }

        result
    }

    /// Get the approximate count of attached databases.
    pub fn approx_database_count(&self) -> usize {
        let dbs = self.databases.read();
        dbs.len() + 1 // +1 for system database
    }

    /// Get the runtime default database id.
    pub fn default_database_id(&self) -> Option<u64> {
        *self.default_database_id.read()
    }

    /// Get the runtime default database handle.
    pub fn default_database(&self) -> Option<Arc<DatabaseHandle>> {
        self.default_database_id()
            .and_then(|database_id| self.get_database_by_id(database_id))
    }

    /// Get the runtime default database name.
    pub fn default_database_name(&self) -> Option<String> {
        let default_database_id = self.default_database_id()?;
        if default_database_id == 0 {
            return self
                .system
                .read()
                .as_ref()
                .filter(|database| database.id() == default_database_id)
                .map(|_| SYSTEM_CATALOG.to_string());
        }

        self.runtime_names_by_id
            .read()
            .get(&default_database_id)
            .cloned()
    }

    /// Set the runtime default database.
    pub fn set_default_database(&self, database_id: u64) -> anyhow::Result<()> {
        let db = self
            .get_database_by_id(database_id)
            .ok_or_else(|| anyhow::anyhow!("Database with id {} not found", database_id))?;

        if db.id() == 0 || db.name().eq_ignore_ascii_case(SYSTEM_CATALOG) {
            anyhow::bail!("Cannot set the default database to a system database");
        }
        if db.name().eq_ignore_ascii_case(TEMP_CATALOG) {
            anyhow::bail!("Cannot set the default database to a temporary database");
        }

        *self.default_database_id.write() = Some(database_id);
        Ok(())
    }

    /// Check if a default database has been set.
    pub fn has_default_database(&self) -> bool {
        self.default_database_id().is_some()
    }

    /// Returns a reference to the system catalog.
    ///
    /// # Panics
    /// Panics if the system database has not been initialized.
    pub fn get_system_catalog(&self) -> Arc<ParoCatalog> {
        let system = self.system.read();
        system
            .as_ref()
            .expect("System database not initialized")
            .catalog()
            .clone()
    }

    /// Get a new query number.
    ///
    /// Query numbers track execution order inside the instance.
    pub fn get_new_query_number(&self) -> u64 {
        self.current_query_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current active query number.
    pub fn active_query_number(&self) -> u64 {
        self.current_query_number.load(Ordering::SeqCst)
    }

    /// Get a new transaction number.
    ///
    /// Transaction numbers are used for MVCC versioning and transaction
    /// ordering.
    pub fn get_new_transaction_number(&self) -> u64 {
        self.current_transaction_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current active transaction number.
    pub fn active_transaction_number(&self) -> u64 {
        self.current_transaction_id.load(Ordering::SeqCst)
    }

    /// Get the next OID for catalog entries.
    ///
    /// OIDs are globally unique identifiers for catalog entries (tables,
    /// schemas, functions, etc.).
    pub fn next_oid(&self) -> u64 {
        self.next_oid.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current OID value (without incrementing).
    pub fn current_oid(&self) -> u64 {
        self.next_oid.load(Ordering::SeqCst)
    }

    /// Set the next OID value.
    ///
    /// This is used during recovery to restore the OID counter.
    pub fn set_next_oid(&self, oid: u64) {
        self.next_oid.store(oid, Ordering::SeqCst);
    }

    /// Reset all databases.
    ///
    /// This removes all databases from the catalog set. This is necessary
    /// for the database instance's destructor, as the database manager has
    /// to be alive when destroying the catalog set objects.
    pub fn reset_databases(&self) {
        let _lock = self.databases_lock.lock();

        // Close all databases
        let dbs = self.get_databases();
        for db in dbs {
            db.set_dropping();
        }

        // Clear the databases map
        let mut databases = self.databases.write();
        databases.clear();
        self.runtime_names_by_id.write().clear();

        *self.default_database_id.write() = None;
        self.bump_visible_generation();
    }

    /// Check if a database name is reserved.
    ///
    /// Reserved names cannot be used for user databases.
    pub fn name_is_reserved(name: &str) -> bool {
        let lower = name.to_lowercase();
        lower == SYSTEM_CATALOG || lower == TEMP_CATALOG
    }

    /// Rename an existing database.
    ///
    /// # Arguments
    /// * `old_name` - The current name of the database
    /// * `new_name` - The new name for the database
    /// * `if_not_found` - Action to take if database is not found
    pub fn rename_database(
        &self,
        old_name: &str,
        new_name: &str,
        if_not_found: OnEntryNotFound,
    ) -> anyhow::Result<()> {
        // Check if new name is reserved
        if Self::name_is_reserved(new_name) {
            anyhow::bail!(
                "Database name \"{}\" cannot be used because it is a reserved name",
                new_name
            );
        }

        let _lock = self.databases_lock.lock();
        let mut dbs = self.databases.write();

        let old_lower = old_name.to_lowercase();
        let new_lower = new_name.to_lowercase();

        if old_lower == new_lower {
            return Ok(());
        }

        // Check if old database exists
        if !dbs.contains_key(&old_lower) {
            return match if_not_found {
                OnEntryNotFound::ThrowException => Err(anyhow::anyhow!(
                    "Failed to rename database \"{}\": database not found",
                    old_name
                )),
                OnEntryNotFound::ReturnNull => Ok(()),
            };
        }

        // Check if new name already exists
        if dbs.contains_key(&new_lower) {
            anyhow::bail!(
                "Failed to rename database \"{}\" to \"{}\": database with new name already exists",
                old_name,
                new_name
            );
        }

        // Perform the rename
        if let Some(db) = dbs.remove(&old_lower) {
            self.path_manager.remove_path(db.path());
            let _ = self
                .path_manager
                .insert_path(db.path(), new_name, OnCreateConflict::Replace);
            self.runtime_names_by_id
                .write()
                .insert(db.id(), new_name.to_string());
            dbs.insert(new_lower.clone(), db);
            self.bump_visible_generation();
        }

        Ok(())
    }

    fn install_default_database_if_missing(&self, database_id: u64) {
        let mut default = self.default_database_id.write();
        if default.is_none() {
            *default = Some(database_id);
        }
    }

    fn ensure_not_runtime_default(
        &self,
        database: &Arc<DatabaseHandle>,
        requested_name: &str,
    ) -> anyhow::Result<()> {
        if self.default_database_id() == Some(database.id()) {
            anyhow::bail!(
                "Cannot detach database \"{}\" because it is the default database. \
                 Select a different database using `USE` to allow detaching this database",
                requested_name
            );
        }
        Ok(())
    }

    fn reassign_default_database_after_removal(
        &self,
        removed_database_id: u64,
        databases: &HashMap<String, Arc<DatabaseHandle>>,
    ) {
        let mut default = self.default_database_id.write();
        if *default == Some(removed_database_id) {
            *default = databases.values().next().map(|db| db.id());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_storage::buffer::BufferPool;

    fn create_test_db(id: u64, name: &str) -> Arc<DatabaseHandle> {
        let buffer_pool = Arc::new(BufferPool::new(1024));
        Arc::new(DatabaseHandle::new(
            id,
            name.to_string(),
            format!("/tmp/{}", name),
            buffer_pool,
        ))
    }

    #[test]
    fn test_database_registry_new() {
        let manager = DatabaseRegistry::new();
        assert!(manager.get_database("test").is_none());
        assert!(!manager.has_default_database());
        assert_eq!(manager.approx_database_count(), 1); // system db
    }

    #[test]
    fn test_register_and_get_database() {
        let manager = DatabaseRegistry::new();
        let db = create_test_db(1, "test_db");

        manager.register_database(db.clone()).unwrap();

        // Case-insensitive lookup
        assert!(manager.get_database("test_db").is_some());
        assert!(manager.get_database("TEST_DB").is_some());
        assert!(manager.get_database("Test_Db").is_some());

        // First registered database becomes default
        assert_eq!(manager.default_database_id(), Some(1));
        assert_eq!(manager.default_database_name().as_deref(), Some("test_db"));
    }

    #[test]
    fn test_register_duplicate_database() {
        let manager = DatabaseRegistry::new();
        let db1 = create_test_db(1, "test_db");
        let db2 = create_test_db(2, "TEST_DB"); // Same name, different case

        manager.register_database(db1).unwrap();
        let result = manager.register_database(db2);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_unregister_database() {
        let manager = DatabaseRegistry::new();
        let db = create_test_db(1, "test_db");

        manager.register_database(db).unwrap();
        assert!(manager.get_database("test_db").is_some());

        let removed = manager.unregister_database("test_db");
        assert!(removed.is_some());
        assert!(manager.get_database("test_db").is_none());
    }

    #[test]
    fn test_detach_database() {
        let manager = DatabaseRegistry::new();
        let db1 = create_test_db(1, "db1");
        let db2 = create_test_db(2, "db2");

        manager.register_database(db1).unwrap();
        manager.register_database(db2).unwrap();

        // Set db2 as default so we can detach db1
        manager.set_default_database(2).unwrap();

        // Detach db1
        let result = manager.detach_database("db1", OnEntryNotFound::ThrowException);
        assert!(result.is_ok());
        assert!(manager.get_database("db1").is_none());
    }

    #[test]
    fn test_detach_default_database_fails() {
        let manager = DatabaseRegistry::new();
        let db = create_test_db(1, "test_db");

        manager.register_database(db).unwrap();

        // Cannot detach default database
        let result = manager.detach_database("test_db", OnEntryNotFound::ThrowException);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("default database"));
    }

    #[test]
    fn test_set_default_database() {
        let manager = DatabaseRegistry::new();
        let db1 = create_test_db(1, "db1");
        let db2 = create_test_db(2, "db2");

        manager.register_database(db1).unwrap();
        manager.register_database(db2).unwrap();

        assert_eq!(manager.default_database_id(), Some(1));
        assert_eq!(manager.default_database_name().as_deref(), Some("db1"));

        manager.set_default_database(2).unwrap();
        assert_eq!(manager.default_database_id(), Some(2));
        assert_eq!(manager.default_database_name().as_deref(), Some("db2"));
    }

    #[test]
    fn test_set_default_database_not_found() {
        let manager = DatabaseRegistry::new();
        let result = manager.set_default_database(404);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_number_allocation() {
        let manager = DatabaseRegistry::new();

        let q1 = manager.get_new_query_number();
        let q2 = manager.get_new_query_number();
        let q3 = manager.get_new_query_number();

        assert_eq!(q1, 1);
        assert_eq!(q2, 2);
        assert_eq!(q3, 3);
        assert_eq!(manager.active_query_number(), 4);
    }

    #[test]
    fn test_transaction_number_allocation() {
        let manager = DatabaseRegistry::new();

        let t1 = manager.get_new_transaction_number();
        let t2 = manager.get_new_transaction_number();
        let t3 = manager.get_new_transaction_number();

        assert_eq!(t1, 0);
        assert_eq!(t2, 1);
        assert_eq!(t3, 2);
        assert_eq!(manager.active_transaction_number(), 3);
    }

    #[test]
    fn test_oid_allocation() {
        let manager = DatabaseRegistry::new();

        let o1 = manager.next_oid();
        let o2 = manager.next_oid();
        let o3 = manager.next_oid();

        assert_eq!(o1, 1);
        assert_eq!(o2, 2);
        assert_eq!(o3, 3);
        assert_eq!(manager.current_oid(), 4);

        // Test set_next_oid
        manager.set_next_oid(100);
        assert_eq!(manager.current_oid(), 100);
        assert_eq!(manager.next_oid(), 100);
        assert_eq!(manager.current_oid(), 101);
    }

    #[test]
    fn test_get_databases() {
        let manager = DatabaseRegistry::new();

        // Initialize system database
        let buffer_pool = Arc::new(BufferPool::new(1024));
        let system_db = Arc::new(DatabaseHandle::new(
            0,
            "system".to_string(),
            ":memory:".to_string(),
            buffer_pool.clone(),
        ));
        {
            let mut system = manager.system.write();
            *system = Some(system_db);
        }

        let db1 = create_test_db(1, "db1");
        let db2 = create_test_db(2, "db2");

        manager.register_database(db1).unwrap();
        manager.register_database(db2).unwrap();

        let dbs = manager.get_databases();
        assert_eq!(dbs.len(), 3); // db1, db2, system
    }

    #[test]
    fn test_visible_generation_tracks_attached_database_changes() {
        let manager = DatabaseRegistry::new();
        let initial = manager.visible_generation();

        let db1 = create_test_db(1, "db1");
        manager.register_database(db1).unwrap();
        let after_register = manager.visible_generation();
        assert!(after_register > initial);

        manager.unregister_database("db1");
        let after_unregister = manager.visible_generation();
        assert!(after_unregister > after_register);
    }

    #[test]
    fn test_name_is_reserved() {
        assert!(DatabaseRegistry::name_is_reserved("system"));
        assert!(DatabaseRegistry::name_is_reserved("SYSTEM"));
        assert!(DatabaseRegistry::name_is_reserved("temp"));
        assert!(DatabaseRegistry::name_is_reserved("TEMP"));
        assert!(!DatabaseRegistry::name_is_reserved("mydb"));
        assert!(!DatabaseRegistry::name_is_reserved("test"));
    }

    #[test]
    fn test_rename_database() {
        let manager = DatabaseRegistry::new();
        let db1 = create_test_db(1, "old_name");
        let db2 = create_test_db(2, "other_db");

        manager.register_database(db1).unwrap();
        manager.register_database(db2).unwrap();

        // Rename old_name to new_name
        manager
            .rename_database("old_name", "new_name", OnEntryNotFound::ThrowException)
            .unwrap();

        assert!(manager.get_database("old_name").is_none());
        assert!(manager.get_database("new_name").is_some());
    }

    #[test]
    fn test_rename_default_database_updates_visible_default_name() {
        let manager = DatabaseRegistry::new();
        let db = create_test_db(1, "old_name");

        manager.register_database(db).unwrap();
        assert_eq!(manager.default_database_name().as_deref(), Some("old_name"));

        manager
            .rename_database("old_name", "new_name", OnEntryNotFound::ThrowException)
            .unwrap();

        assert_eq!(manager.default_database_id(), Some(1));
        assert_eq!(manager.default_database_name().as_deref(), Some("new_name"));
    }

    #[test]
    fn test_rename_to_reserved_name_fails() {
        let manager = DatabaseRegistry::new();
        let db = create_test_db(1, "mydb");

        manager.register_database(db).unwrap();

        let result = manager.rename_database("mydb", "system", OnEntryNotFound::ThrowException);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("reserved name"));
    }

    #[test]
    fn test_rename_to_existing_name_fails() {
        let manager = DatabaseRegistry::new();
        let db1 = create_test_db(1, "db1");
        let db2 = create_test_db(2, "db2");

        manager.register_database(db1).unwrap();
        manager.register_database(db2).unwrap();

        let result = manager.rename_database("db1", "db2", OnEntryNotFound::ThrowException);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_reset_databases() {
        let manager = DatabaseRegistry::new();
        let db1 = create_test_db(1, "db1");
        let db2 = create_test_db(2, "db2");

        manager.register_database(db1).unwrap();
        manager.register_database(db2).unwrap();

        assert_eq!(manager.get_database_names().len(), 2);

        manager.reset_databases();

        assert_eq!(manager.get_database_names().len(), 0);
        assert!(!manager.has_default_database());
    }

    #[test]
    fn test_get_database_or_error() {
        let manager = DatabaseRegistry::new();
        let db = create_test_db(1, "test_db");

        manager.register_database(db).unwrap();

        // Found case
        let result = manager.get_database_or_error("test_db", OnEntryNotFound::ThrowException);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Not found with ThrowException
        let result = manager.get_database_or_error("nonexistent", OnEntryNotFound::ThrowException);
        assert!(result.is_err());

        // Not found with ReturnNull
        let result = manager.get_database_or_error("nonexistent", OnEntryNotFound::ReturnNull);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_initialize_system_catalog() {
        let manager = DatabaseRegistry::new();

        // Initialize system database
        let buffer_pool = Arc::new(BufferPool::new(1024));
        let system_db = Arc::new(DatabaseHandle::new_system(0, buffer_pool));
        system_db.initialize().unwrap();
        {
            let mut system = manager.system.write();
            *system = Some(system_db);
        }

        manager.initialize_system_catalog();

        // System database should be ready
        let system = manager.system.read();
        assert!(system.as_ref().unwrap().is_ready());
    }
}
