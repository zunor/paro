// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Catalog Collection - MVCC-aware entry collection
//!
//!
//! CatalogCollection implements Multi-Version Concurrency Control for catalog entries:
//! - Each entry has a version chain (newer -> older via child/parent pointers)
//! - Uncommitted entries have timestamp >= TRANSACTION_ID_START
//! - Committed entries have timestamp < TRANSACTION_ID_START
//! - Visibility is determined by comparing timestamps with transaction context

mod gc;
mod lock_key;
mod staged_mutation;

use crate::entry::{CatalogEntryEnum, DependencyList};
use crate::mvcc::{self, CatalogSnapshot, VersionedEntry};
use paro_common::error::{self as paro_error, Result};
use paro_transaction::TRANSACTION_ID_START;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(test)]
use crate::entry::CatalogObjectId;

pub use gc::{CatalogGcStats, CatalogReplaySummary};
use lock_key::{ordered_collection_pair, with_ordered_collection_maps, OrderedCollectionPair};
pub(crate) use lock_key::{CollectionFamily, CollectionLockKey};
pub use staged_mutation::StagedCatalogMutation;

// ============================================================================
// EntryLookup Result Types
// ============================================================================

/// Result of looking up an entry with detailed failure reason.
///
#[derive(Debug)]
pub struct EntryLookup {
    /// The found entry, if any
    pub result: Option<Arc<CatalogEntryEnum>>,
    /// The reason for failure, if lookup failed
    pub reason: EntryLookupFailure,
}

/// Reason for entry lookup failure.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryLookupFailure {
    /// Entry was found successfully
    Success,
    /// Entry exists but is marked as deleted
    Deleted,
    /// Entry does not exist in the catalog
    NotPresent,
    /// Entry exists but is not visible to this transaction
    Invisible,
}

/// Similar entry result for fuzzy matching (error messages).
///
#[derive(Debug, Clone)]
pub struct SimilarCatalogEntry {
    /// The similar entry name
    pub name: String,
    /// Similarity score (0.0 - 1.0, higher is more similar)
    pub score: f64,
}

impl Default for SimilarCatalogEntry {
    fn default() -> Self {
        Self {
            name: String::new(),
            score: 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallMode {
    RejectExisting,
    ReplaceExisting,
}

/// Internal map for storing catalog entries.
///
/// Provides case-insensitive key lookup and version-chain head management.
#[derive(Debug, Default)]
struct CatalogEntryMap {
    /// Mapping of lowercase name to version-chain head.
    entries: HashMap<String, Arc<VersionedEntry>>,
}

impl CatalogEntryMap {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Add a new entry to the map.
    #[cfg(test)]
    fn add_entry(&mut self, name: String, entry: Arc<VersionedEntry>) {
        self.entries.insert(name.to_lowercase(), entry);
    }

    /// Update an existing entry (push new version onto chain).
    fn update_entry(&mut self, name: &str, new_entry: Arc<VersionedEntry>) {
        self.entries.insert(name.to_lowercase(), new_entry);
    }

    /// Drop an entry from the map.
    fn drop_entry(&mut self, name: &str) {
        let key = name.to_lowercase();
        self.entries.remove(&key);
    }

    /// Get an entry by name (case-insensitive).
    fn get_entry(&self, name: &str) -> Option<Arc<VersionedEntry>> {
        let key = name.to_lowercase();
        self.entries.get(&key).cloned()
    }

    /// Check if an entry exists.
    fn contains(&self, name: &str) -> bool {
        let key = name.to_lowercase();
        self.entries.contains_key(&key)
    }

    /// Get all entries (for scanning).
    fn entries(&self) -> impl Iterator<Item = (&String, &Arc<VersionedEntry>)> {
        self.entries.iter()
    }

    /// Get the number of entries.
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the map is empty.
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================================
// CatalogCollection
// ============================================================================

/// CatalogCollection manages a versioned set of CatalogEntry objects with MVCC support.
///
///
/// Key features:
/// - Case-insensitive entry lookup
/// - MVCC version chains for transactional DDL
/// - Write-write conflict detection
/// - Transaction visibility checks
pub struct CatalogCollection {
    /// Reference to parent catalog name (for debugging/error messages)
    catalog_name: String,
    /// Entry map (case-insensitive key -> version chain head)
    map: RwLock<CatalogEntryMap>,
    /// Catalog-level write lock (for serializing modifications)
    catalog_lock: Mutex<()>,
    /// Stable lock identity for cross-collection ordering.
    lock_key: CollectionLockKey,
    /// Shared catalog dirty epoch.
    gc_epoch: Arc<AtomicU64>,
}

impl std::fmt::Debug for CatalogCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogCollection")
            .field("catalog_name", &self.catalog_name)
            .field("lock_key", &self.lock_key)
            .field("map", &self.map)
            .finish()
    }
}

impl CatalogCollection {
    fn visible_node(
        &self,
        head: &Arc<VersionedEntry>,
        transaction_id: u64,
        start_time: u64,
    ) -> Option<Arc<VersionedEntry>> {
        let mut current = Some(Arc::clone(head));
        while let Some(node) = current {
            if self.use_timestamp(transaction_id, start_time, node.timestamp()) {
                return Some(node);
            }
            current = node.child();
        }
        None
    }

    fn set_entry_mvcc_state(&self, entry: &Arc<CatalogEntryEnum>, timestamp: u64, deleted: bool) {
        self.set_entry_timestamp(entry, timestamp);
        self.set_entry_deleted(entry, deleted);
    }

    /// Create a new CatalogCollection.
    pub(crate) fn new(
        catalog_name: String,
        lock_key: CollectionLockKey,
        gc_epoch: Arc<AtomicU64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            catalog_name,
            map: RwLock::new(CatalogEntryMap::new()),
            catalog_lock: Mutex::new(()),
            lock_key,
            gc_epoch,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(
        catalog_name: impl Into<String>,
        schema_id: u64,
        family: CollectionFamily,
    ) -> Arc<Self> {
        let lock_key = match family {
            CollectionFamily::Schemas => CollectionLockKey::database_schemas(),
            _ => CollectionLockKey::schema_family(CatalogObjectId::from_raw(schema_id), family),
        };
        Self::new(catalog_name.into(), lock_key, Arc::new(AtomicU64::new(0)))
    }

    pub(crate) fn lock_key(&self) -> CollectionLockKey {
        self.lock_key
    }

    pub(crate) fn mark_gc_dirty(&self) {
        self.gc_epoch.fetch_add(1, Ordering::Relaxed);
    }

    // =========================================================================
    // Entry Creation
    // =========================================================================

    pub fn stage_create(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        name: &str,
        entry: Arc<CatalogEntryEnum>,
    ) -> Result<Option<StagedCatalogMutation>> {
        self.stage_create_internal(snapshot, name.to_string(), entry, DependencyList::new())
    }

    pub fn stage_replace(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        name: &str,
        entry: Arc<CatalogEntryEnum>,
    ) -> Result<Option<StagedCatalogMutation>> {
        self.stage_replace_internal(snapshot, name, entry)
    }

    pub fn stage_drop(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        name: &str,
    ) -> Result<Option<StagedCatalogMutation>> {
        self.stage_drop_internal(snapshot, name, false)
    }

    pub fn stage_rename(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        from: &str,
        to: &str,
        entry: Arc<CatalogEntryEnum>,
    ) -> Result<Option<StagedCatalogMutation>> {
        self.stage_move_internal(snapshot, from, self, to, entry)
    }

    pub fn stage_move(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        from: &str,
        target: &Arc<CatalogCollection>,
        to: &str,
        entry: Arc<CatalogEntryEnum>,
    ) -> Result<Option<StagedCatalogMutation>> {
        self.stage_move_internal(snapshot, from, target, to, entry)
    }

    pub fn install_committed(&self, entry: Arc<CatalogEntryEnum>, mode: InstallMode) -> Result<()> {
        self.install_entry_with_timestamp(0, entry, mode)
    }

    pub fn install_replayed(
        &self,
        commit_ts: u64,
        entry: Arc<CatalogEntryEnum>,
        mode: InstallMode,
    ) -> Result<()> {
        if commit_ts == 0 || commit_ts >= TRANSACTION_ID_START {
            return Err(paro_error::invalid_input(format!(
                "replayed catalog commit timestamp must be in committed range, got {}",
                commit_ts
            )));
        }
        self.install_entry_with_timestamp(commit_ts, entry, mode)
    }

    /// Create a new entry in the catalog set.
    ///
    ///
    /// Returns `Ok(true)` if the entry was created successfully.
    /// Returns `Ok(false)` if an entry with the same name already exists.
    /// Returns `Err` if there's a write-write conflict.
    ///
    /// # Arguments
    /// * `transaction_id` - The transaction ID
    /// * `name` - Entry name
    /// * `entry` - The entry to create
    /// * `dependencies` - List of dependencies (required parameter per spec)
    #[cfg(test)]
    pub fn create_entry(
        &self,
        transaction_id: u64,
        name: String,
        entry: Arc<CatalogEntryEnum>,
        dependencies: DependencyList,
    ) -> Result<bool> {
        // Mark this entry as being created by the current active transaction
        self.set_entry_mvcc_state(&entry, transaction_id, false);

        // Lock for writing
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let key = name.to_lowercase();

        // Check for existing entry
        if let Some(existing) = map.get_entry(&key) {
            let timestamp = existing.timestamp();

            // Check for conflicts
            if self.created_by_other_active_transaction(transaction_id, timestamp) {
                return Err(paro_error::serialization_failure(format!(
                    "Catalog write-write conflict on create with \"{}\"",
                    name
                )));
            }

            let Some(visible) = self.visible_node(&existing, transaction_id, TRANSACTION_ID_START)
            else {
                return Err(paro_error::serialization_failure(format!(
                    "Catalog write-write conflict on create with \"{}\"",
                    name
                )));
            };

            if !visible.is_deleted() {
                return Ok(false);
            }
        }

        // Store dependencies on the entry if it's a StandardEntry
        // Dependencies are stored on the entry itself, not in CatalogCollection.
        let _ = dependencies; // Dependencies are already set on the entry

        let previous = map.get_entry(&key);
        let new_head = VersionedEntry::new(Some(entry), transaction_id, false, previous);
        if map.contains(&key) {
            map.update_entry(&key, new_head);
        } else {
            map.add_entry(key.clone(), new_head);
        }

        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();

        Ok(true)
    }

    fn stage_create_internal(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        name: String,
        entry: Arc<CatalogEntryEnum>,
        dependencies: DependencyList,
    ) -> Result<Option<StagedCatalogMutation>> {
        let transaction_id = snapshot.write_timestamp()?;
        let start_time = snapshot.start_time;
        self.set_entry_mvcc_state(&entry, transaction_id, false);

        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let key = name.to_lowercase();
        if let Some(existing) = map.get_entry(&key) {
            let timestamp = existing.timestamp();
            if self.created_by_other_active_transaction(transaction_id, timestamp) {
                return Err(paro_error::serialization_failure(format!(
                    "Catalog write-write conflict on create with \"{}\"",
                    name
                )));
            }

            let Some(visible) = self.visible_node(&existing, transaction_id, start_time) else {
                return Err(paro_error::serialization_failure(format!(
                    "Catalog write-write conflict on create with \"{}\"",
                    name
                )));
            };

            if !visible.is_deleted() {
                return Ok(None);
            }
        }

        let _ = dependencies;

        let previous = map.get_entry(&key);
        let new_head = VersionedEntry::new(Some(entry), transaction_id, false, previous.clone());
        map.update_entry(&key, new_head.clone());
        Ok(Some(StagedCatalogMutation::replace(
            Arc::clone(self),
            key,
            new_head,
            previous,
        )))
    }

    /// Set the timestamp on an entry.
    ///
    fn set_entry_timestamp(&self, entry: &Arc<CatalogEntryEnum>, timestamp: u64) {
        match entry.as_ref() {
            CatalogEntryEnum::Table(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::View(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::Index(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::PropertyGraph(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::Sequence(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::Routine(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::ScalarFunction(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::AggregateFunction(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::TableFunction(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::CopyFunction(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::Type(e) => e.base.base.set_timestamp(timestamp),
            CatalogEntryEnum::Schema(e) => e.base.set_timestamp(timestamp),
        }
    }

    fn set_entry_deleted(&self, entry: &Arc<CatalogEntryEnum>, deleted: bool) {
        match entry.as_ref() {
            CatalogEntryEnum::Table(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::View(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::Index(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::PropertyGraph(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::Sequence(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::Routine(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::ScalarFunction(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::AggregateFunction(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::TableFunction(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::CopyFunction(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::Type(e) => e.base.base.set_deleted(deleted),
            CatalogEntryEnum::Schema(e) => e.base.set_deleted(deleted),
        }
    }

    fn install_entry_with_timestamp(
        &self,
        timestamp: u64,
        entry: Arc<CatalogEntryEnum>,
        mode: InstallMode,
    ) -> Result<()> {
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let key = entry.name().to_lowercase();
        if mode == InstallMode::RejectExisting && map.contains(&key) {
            return Err(paro_error::object_exists(
                entry.entry_type().as_str(),
                entry.name(),
            ));
        }

        self.set_entry_mvcc_state(&entry, timestamp, false);
        map.update_entry(
            &key,
            VersionedEntry::new(Some(entry), timestamp, false, None),
        );
        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();
        Ok(())
    }

    /// Create a committed entry (for bootstrap/recovery).
    ///
    ///
    /// This bypasses MVCC and creates an entry visible to all transactions.
    pub fn create_committed_entry(
        &self,
        entry: Arc<CatalogEntryEnum>,
    ) -> Option<Arc<CatalogEntryEnum>> {
        let name = entry.name().to_string();
        self.create_committed_entry_lazy(&name, || Some(entry))
            .ok()
            .flatten()
    }

    /// Lazily create a committed entry after winning the install race on `name`.
    pub fn create_committed_entry_lazy<F>(
        &self,
        name: &str,
        build: F,
    ) -> Result<Option<Arc<CatalogEntryEnum>>>
    where
        F: FnOnce() -> Option<Arc<CatalogEntryEnum>>,
    {
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let key = name.to_lowercase();
        if map.contains(&key) {
            return Ok(None);
        }

        let Some(entry) = build() else {
            return Ok(None);
        };
        self.set_entry_mvcc_state(&entry, 0, false);
        map.update_entry(
            &key,
            VersionedEntry::new(Some(Arc::clone(&entry)), 0, false, None),
        );
        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();
        Ok(Some(entry))
    }

    // =========================================================================
    // Entry Deletion
    // =========================================================================

    /// Drop an entry from the catalog set.
    ///
    ///
    /// This creates a tombstone entry for MVCC purposes.
    #[cfg(test)]
    pub fn drop_entry(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
        _cascade: bool,
    ) -> Result<bool> {
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        // First check if entry exists and is visible
        let entry = self.get_entry_internal(transaction_id, start_time, name)?;
        if entry.is_none() {
            return Ok(false);
        }

        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        let key = name.to_lowercase();
        let previous = map.get_entry(&key);
        let tombstone = VersionedEntry::new(entry, transaction_id, true, previous);
        map.update_entry(&key, tombstone);

        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();

        Ok(true)
    }

    fn stage_drop_internal(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        name: &str,
        cascade: bool,
    ) -> Result<Option<StagedCatalogMutation>> {
        let transaction_id = snapshot.write_timestamp()?;
        let start_time = snapshot.start_time;
        let _ = cascade;
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let entry = self.get_entry_internal(transaction_id, start_time, name)?;
        if entry.is_none() {
            return Ok(None);
        }

        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        let key = name.to_lowercase();
        let previous = map.get_entry(&key);
        let tombstone = VersionedEntry::new(entry, transaction_id, true, previous.clone());
        map.update_entry(&key, tombstone.clone());
        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();
        Ok(Some(StagedCatalogMutation::replace(
            Arc::clone(self),
            key,
            tombstone,
            previous,
        )))
    }

    /// Remove an entry (for rollback).
    pub fn remove_entry(&self, name: &str) {
        if let Ok(_write_lock) = self.catalog_lock.lock() {
            if let Ok(mut map) = self.map.write() {
                if let Some(head) = map.get_entry(name) {
                    if let Some(child) = head.child() {
                        map.update_entry(name, child);
                    } else {
                        map.drop_entry(name);
                    }
                }
            }
        }
        self.mark_gc_dirty();
    }

    // =========================================================================
    // Entry Lookup
    // =========================================================================

    /// Get entry by name with visibility check.
    ///
    ///
    /// Returns the entry if it exists and is visible to the transaction.
    pub fn get_entry(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Option<Arc<CatalogEntryEnum>> {
        // First, try to find in the map
        {
            if let Ok(map) = self.map.read() {
                if let Some(head) = map.get_entry(name) {
                    if let Some(node) = self.visible_node(&head, transaction_id, start_time) {
                        if !node.is_deleted() {
                            return node.entry.clone();
                        }
                    }
                }
            }
        }
        None
    }

    /// Get entry with detailed lookup result.
    ///
    pub fn get_entry_detailed(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> EntryLookup {
        let map = match self.map.read() {
            Ok(m) => m,
            Err(_) => {
                return EntryLookup {
                    result: None,
                    reason: EntryLookupFailure::NotPresent,
                }
            }
        };

        match map.get_entry(name) {
            Some(head) => {
                if let Some(node) = self.visible_node(&head, transaction_id, start_time) {
                    if node.is_deleted() {
                        EntryLookup {
                            result: None,
                            reason: EntryLookupFailure::Deleted,
                        }
                    } else {
                        EntryLookup {
                            result: node.entry.clone(),
                            reason: EntryLookupFailure::Success,
                        }
                    }
                } else {
                    EntryLookup {
                        result: None,
                        reason: EntryLookupFailure::Invisible,
                    }
                }
            }
            None => EntryLookup {
                result: None,
                reason: EntryLookupFailure::NotPresent,
            },
        }
    }

    /// Look up an entry using committed-only visibility (no active transaction).
    ///
    /// Equivalent to `get_entry(0, TRANSACTION_ID_START, name)` — for tests and
    /// diagnostics that need the catalog state without a writer snapshot.
    pub fn get_committed_entry(&self, name: &str) -> Option<Arc<CatalogEntryEnum>> {
        self.get_entry(0, TRANSACTION_ID_START, name)
    }

    /// Internal entry lookup for modification operations.
    fn get_entry_internal(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntryEnum>>> {
        let map = self
            .map
            .read()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        match map.get_entry(name) {
            Some(head) => {
                let timestamp = head.timestamp();

                // Check for write-write conflict
                if self.has_conflict(transaction_id, start_time, timestamp) {
                    return Err(paro_error::serialization_failure(format!(
                        "Catalog write-write conflict on alter with \"{}\"",
                        name
                    )));
                }

                Ok(self
                    .visible_node(&head, transaction_id, start_time)
                    .and_then(|node| {
                        if node.is_deleted() {
                            None
                        } else {
                            node.entry.clone()
                        }
                    }))
            }
            None => Ok(None),
        }
    }

    // =========================================================================
    // Entry Alteration
    // =========================================================================

    /// Alter an existing entry.
    ///
    #[cfg(test)]
    pub fn alter_entry(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
        new_entry: Arc<CatalogEntryEnum>,
    ) -> Result<bool> {
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let existing = self.get_entry_internal(transaction_id, start_time, name)?;
        if existing.is_none() {
            return Ok(false);
        }

        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        self.set_entry_mvcc_state(&new_entry, transaction_id, false);
        let previous = map.get_entry(name);
        map.update_entry(
            name,
            VersionedEntry::new(Some(new_entry), transaction_id, false, previous),
        );
        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();

        Ok(true)
    }

    fn stage_replace_internal(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        name: &str,
        new_entry: Arc<CatalogEntryEnum>,
    ) -> Result<Option<StagedCatalogMutation>> {
        let transaction_id = snapshot.write_timestamp()?;
        let start_time = snapshot.start_time;
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let existing = self.get_entry_internal(transaction_id, start_time, name)?;
        if existing.is_none() {
            return Ok(None);
        }

        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        self.set_entry_mvcc_state(&new_entry, transaction_id, false);
        let previous = map.get_entry(name);
        let head = VersionedEntry::new(Some(new_entry), transaction_id, false, previous.clone());
        map.update_entry(name, head.clone());
        Ok(Some(StagedCatalogMutation::replace(
            Arc::clone(self),
            name.to_lowercase(),
            head,
            previous,
        )))
    }

    #[cfg(test)]
    pub fn prepare_rename_entry(
        self: &Arc<Self>,
        transaction_id: u64,
        start_time: u64,
        old_name: &str,
        new_name: &str,
    ) -> Result<Option<StagedCatalogMutation>> {
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        let old_entry = self.get_entry_internal(transaction_id, start_time, old_name)?;
        let Some(old_entry) = old_entry else {
            return Ok(None);
        };

        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        if let Some(existing) = map.get_entry(new_name) {
            if let Some(node) = self.visible_node(&existing, transaction_id, start_time) {
                if !node.is_deleted() {
                    return Err(paro_error::duplicate_object(format!(
                        "Could not rename \"{}\" to \"{}\": another entry with this name already exists!",
                        old_name, new_name
                    )));
                }
            }
        }

        let new_entry = self.clone_entry_with_new_name(&old_entry, new_name)?;
        self.set_entry_mvcc_state(&new_entry, transaction_id, false);

        let old_key = old_name.to_lowercase();
        let new_key = new_name.to_lowercase();
        let old_previous = map.get_entry(&old_key);
        let new_previous = map.get_entry(&new_key);
        let old_head = VersionedEntry::new(None, transaction_id, true, old_previous.clone());
        let new_head =
            VersionedEntry::new(Some(new_entry), transaction_id, false, new_previous.clone());
        map.update_entry(&old_key, old_head.clone());
        map.update_entry(&new_key, new_head.clone());

        Ok(Some(StagedCatalogMutation::rename(
            Arc::clone(self),
            old_key,
            old_head,
            old_previous,
            new_key,
            new_head,
            new_previous,
        )))
    }

    fn stage_move_internal(
        self: &Arc<Self>,
        snapshot: &CatalogSnapshot,
        old_name: &str,
        target_set: &Arc<CatalogCollection>,
        new_name: &str,
        new_entry: Arc<CatalogEntryEnum>,
    ) -> Result<Option<StagedCatalogMutation>> {
        let transaction_id = snapshot.write_timestamp()?;
        let start_time = snapshot.start_time;
        let source_set = Arc::clone(self);
        let old_key = old_name.to_lowercase();
        let new_key = new_name.to_lowercase();

        if matches!(
            ordered_collection_pair(&source_set, target_set)?,
            OrderedCollectionPair::One
        ) {
            // Same-collection move is normalized to a rename so we only take one lock.
            let _write_lock = self
                .catalog_lock
                .lock()
                .map_err(|_| paro_error::internal("lock poisoned"))?;
            let mut map = self
                .map
                .write()
                .map_err(|_| paro_error::internal("lock poisoned"))?;

            let Some(existing) = map.get_entry(&old_key) else {
                return Ok(None);
            };
            let timestamp = existing.timestamp();
            if self.created_by_other_active_transaction(transaction_id, timestamp) {
                return Err(paro_error::serialization_failure(format!(
                    "Catalog write-write conflict on rename with \"{}\"",
                    old_name
                )));
            }
            let Some(visible) = self.visible_node(&existing, transaction_id, start_time) else {
                return Err(paro_error::serialization_failure(format!(
                    "Catalog write-write conflict on rename with \"{}\"",
                    old_name
                )));
            };
            if visible.is_deleted() {
                return Ok(None);
            }

            if let Some(existing) = map.get_entry(&new_key) {
                if let Some(node) = self.visible_node(&existing, transaction_id, start_time) {
                    if !node.is_deleted() {
                        return Err(paro_error::duplicate_object(format!(
                            "Could not rename \"{}\" to \"{}\": another entry with this name already exists!",
                            old_name, new_name
                        )));
                    }
                }
            }

            self.set_entry_mvcc_state(&new_entry, transaction_id, false);
            let old_previous = map.get_entry(&old_key);
            let new_previous = map.get_entry(&new_key);
            let old_head = VersionedEntry::new(None, transaction_id, true, old_previous.clone());
            let new_head =
                VersionedEntry::new(Some(new_entry), transaction_id, false, new_previous.clone());
            map.update_entry(&old_key, old_head.clone());
            map.update_entry(&new_key, new_head.clone());

            return Ok(Some(StagedCatalogMutation::rename(
                source_set,
                old_key,
                old_head,
                old_previous,
                new_key,
                new_head,
                new_previous,
            )));
        }

        let result = with_ordered_collection_maps(
            &source_set,
            target_set,
            |source_map, target_map| {
                let Some(existing) = source_map.get_entry(&old_key) else {
                    return Ok(None);
                };
                let timestamp = existing.timestamp();
                if self.created_by_other_active_transaction(transaction_id, timestamp) {
                    return Err(paro_error::serialization_failure(format!(
                        "Catalog write-write conflict on move with \"{}\"",
                        old_name
                    )));
                }
                let Some(visible) = self.visible_node(&existing, transaction_id, start_time) else {
                    return Err(paro_error::serialization_failure(format!(
                        "Catalog write-write conflict on move with \"{}\"",
                        old_name
                    )));
                };
                if visible.is_deleted() {
                    return Ok(None);
                }

                if let Some(existing) = target_map.get_entry(&new_key) {
                    if let Some(node) =
                        target_set.visible_node(&existing, transaction_id, start_time)
                    {
                        if !node.is_deleted() {
                            return Err(paro_error::duplicate_object(format!(
                                "Could not move \"{}\" to \"{}\": another entry with this name already exists!",
                                old_name, new_name
                            )));
                        }
                    }
                }

                target_set.set_entry_mvcc_state(&new_entry, transaction_id, false);
                let source_previous = source_map.get_entry(&old_key);
                let target_previous = target_map.get_entry(&new_key);
                let source_head =
                    VersionedEntry::new(None, transaction_id, true, source_previous.clone());
                let target_head = VersionedEntry::new(
                    Some(Arc::clone(&new_entry)),
                    transaction_id,
                    false,
                    target_previous.clone(),
                );
                source_map.update_entry(&old_key, source_head.clone());
                target_map.update_entry(&new_key, target_head.clone());

                Ok(Some(StagedCatalogMutation::move_between_sets(
                    Arc::clone(&source_set),
                    old_key.clone(),
                    source_head,
                    source_previous,
                    Arc::clone(target_set),
                    new_key.clone(),
                    target_head,
                    target_previous,
                )))
            },
        );
        result
    }

    /// Rename an entry from old_name to new_name.
    ///
    ///
    /// This method:
    /// 1. Checks if the new name already exists
    /// 2. Removes the entry from the old name
    /// 3. Adds the entry with the new name
    ///
    /// Returns `Ok(true)` if the rename was successful.
    /// Returns `Ok(false)` if the old entry doesn't exist.
    /// Returns `Err` if the new name already exists or there's a conflict.
    #[cfg(test)]
    pub fn rename_entry(
        &self,
        transaction_id: u64,
        start_time: u64,
        old_name: &str,
        new_name: &str,
    ) -> Result<bool> {
        let _write_lock = self
            .catalog_lock
            .lock()
            .map_err(|_| paro_error::internal("lock poisoned"))?;

        // Check if the old entry exists
        let old_entry = self.get_entry_internal(transaction_id, start_time, old_name)?;
        if old_entry.is_none() {
            return Ok(false);
        }
        let old_entry = old_entry.unwrap();

        // Check if the new name already exists
        let mut map = self
            .map
            .write()
            .map_err(|_| paro_error::internal("lock poisoned"))?;
        if let Some(existing) = map.get_entry(new_name) {
            if let Some(node) = self.visible_node(&existing, transaction_id, start_time) {
                if !node.is_deleted() {
                    return Err(paro_error::duplicate_object(format!(
                        "Could not rename \"{}\" to \"{}\": another entry with this name already exists!",
                        old_name, new_name
                    )));
                }
            }
        }

        // Remove from old name
        map.drop_entry(old_name);

        // Create a new entry with the new name
        // We need to clone the entry and update its name
        let new_entry = self.clone_entry_with_new_name(&old_entry, new_name)?;

        self.set_entry_mvcc_state(&new_entry, transaction_id, false);

        // Add with new name
        map.add_entry(
            new_name.to_string(),
            VersionedEntry::new(Some(new_entry), transaction_id, false, None),
        );
        drop(map);
        drop(_write_lock);
        self.mark_gc_dirty();

        Ok(true)
    }

    /// Clone an entry with a new name.
    ///
    /// This is a helper method for rename operations.
    #[cfg(test)]
    fn clone_entry_with_new_name(
        &self,
        entry: &Arc<CatalogEntryEnum>,
        new_name: &str,
    ) -> Result<Arc<CatalogEntryEnum>> {
        // For rename operations, we create a new entry with the updated name
        // The actual implementation depends on the entry type
        match entry.as_ref() {
            CatalogEntryEnum::Schema(e) => {
                let mut new_schema = e.copy()?;
                new_schema.base.name = new_name.to_string();
                new_schema.base.set_timestamp(e.base.timestamp());
                Ok(Arc::new(CatalogEntryEnum::Schema(Arc::new(new_schema))))
            }
            CatalogEntryEnum::Table(e) => Ok(Arc::new(CatalogEntryEnum::Table(Arc::new(
                e.clone_with_new_name(new_name.to_string(), e.base.base.timestamp()),
            )))),
            CatalogEntryEnum::View(_) => Err(paro_error::not_implemented(
                "Renaming views is not yet fully implemented",
            )),
            CatalogEntryEnum::Index(_) => Err(paro_error::not_implemented(
                "Renaming indexes is not yet fully implemented",
            )),
            CatalogEntryEnum::Sequence(_) => Err(paro_error::not_implemented(
                "Renaming sequences is not yet fully implemented",
            )),
            CatalogEntryEnum::ScalarFunction(_) => Err(paro_error::not_implemented(
                "Renaming scalar functions is not yet fully implemented",
            )),
            CatalogEntryEnum::AggregateFunction(_) => Err(paro_error::not_implemented(
                "Renaming aggregate functions is not yet fully implemented",
            )),
            CatalogEntryEnum::TableFunction(_) => Err(paro_error::not_implemented(
                "Renaming table functions is not yet fully implemented",
            )),
            CatalogEntryEnum::CopyFunction(_) => Err(paro_error::not_implemented(
                "Renaming copy functions is not yet fully implemented",
            )),
            CatalogEntryEnum::Type(_) => Err(paro_error::not_implemented(
                "Renaming types is not yet fully implemented",
            )),
            CatalogEntryEnum::PropertyGraph(_) => Err(paro_error::not_implemented(
                "Renaming property graphs is not yet fully implemented",
            )),
            CatalogEntryEnum::Routine(_) => Err(paro_error::not_implemented(
                "Renaming routines is not yet fully implemented",
            )),
        }
    }

    // =========================================================================
    // Scanning
    // =========================================================================

    /// Scan all entries visible to the transaction.
    ///
    ///
    pub fn scan(&self, transaction_id: u64, start_time: u64) -> Vec<Arc<CatalogEntryEnum>> {
        let map = match self.map.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };

        let mut result = Vec::new();
        for (_, head) in map.entries() {
            if let Some(node) = self.visible_node(head, transaction_id, start_time) {
                if !node.is_deleted() {
                    if let Some(entry) = node.entry.as_ref() {
                        result.push(Arc::clone(entry));
                    }
                }
            }
        }

        result
    }

    /// Scan all entries with a callback.
    pub fn scan_with_callback<F>(&self, transaction_id: u64, start_time: u64, mut callback: F)
    where
        F: FnMut(&CatalogEntryEnum),
    {
        if let Ok(map) = self.map.read() {
            for (_, head) in map.entries() {
                if let Some(node) = self.visible_node(head, transaction_id, start_time) {
                    if !node.is_deleted() {
                        if let Some(entry) = node.entry.as_ref() {
                            callback(entry);
                        }
                    }
                }
            }
        }
    }

    /// Scan all committed entries (no transaction context).
    pub fn scan_committed(&self) -> Vec<Arc<CatalogEntryEnum>> {
        let map = match self.map.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };

        let mut result = Vec::new();
        for (_, head) in map.entries() {
            if let Some(node) = self.visible_node(head, 0, TRANSACTION_ID_START) {
                if Self::is_committed(node.timestamp()) && !node.is_deleted() {
                    if let Some(entry) = node.entry.as_ref() {
                        result.push(Arc::clone(entry));
                    }
                }
            }
        }

        result
    }

    // =========================================================================
    // Similar Entry (Fuzzy Matching)
    // =========================================================================

    /// Find the most similar entry name (for error messages).
    ///
    pub fn similar_entry(
        &self,
        transaction_id: u64,
        start_time: u64,
        name: &str,
    ) -> SimilarCatalogEntry {
        let map = match self.map.read() {
            Ok(m) => m,
            Err(_) => return SimilarCatalogEntry::default(),
        };

        let mut result = SimilarCatalogEntry::default();

        for (entry_name, head) in map.entries() {
            let Some(node) = self.visible_node(head, transaction_id, start_time) else {
                continue;
            };
            if node.is_deleted() {
                continue;
            }

            let score = Self::similarity_score(entry_name, name);
            if score > result.score {
                result.score = score;
                result.name = entry_name.clone();
            }
        }

        result
    }

    /// Calculate similarity score between two strings.
    fn similarity_score(a: &str, b: &str) -> f64 {
        let a_lower = a.to_lowercase();
        let b_lower = b.to_lowercase();

        if a_lower == b_lower {
            return 1.0;
        }

        let max_len = a_lower.len().max(b_lower.len());
        if max_len == 0 {
            return 1.0;
        }

        let distance = Self::levenshtein_distance(&a_lower, &b_lower);
        1.0 - (distance as f64 / max_len as f64)
    }

    /// Calculate Levenshtein distance between two strings.
    pub fn levenshtein_distance(a: &str, b: &str) -> usize {
        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        let m = a_chars.len();
        let n = b_chars.len();

        if m == 0 {
            return n;
        }
        if n == 0 {
            return m;
        }

        let mut dp = vec![vec![0usize; n + 1]; m + 1];

        for i in 0..=m {
            dp[i][0] = i;
        }
        for j in 0..=n {
            dp[0][j] = j;
        }

        for i in 1..=m {
            for j in 1..=n {
                let cost = if a_chars[i - 1] == b_chars[j - 1] {
                    0
                } else {
                    1
                };
                dp[i][j] = (dp[i - 1][j] + 1)
                    .min(dp[i][j - 1] + 1)
                    .min(dp[i - 1][j - 1] + cost);
            }
        }

        dp[m][n]
    }

    // =========================================================================
    // MVCC Visibility
    // =========================================================================

    /// Check if we should use this timestamp for visibility.
    ///
    fn use_timestamp(&self, transaction_id: u64, start_time: u64, timestamp: u64) -> bool {
        let writer_id = (transaction_id >= TRANSACTION_ID_START).then_some(transaction_id);
        mvcc::is_visible(timestamp, writer_id, start_time)
    }

    /// Check if a timestamp represents a committed entry.
    ///
    pub fn is_committed(timestamp: u64) -> bool {
        mvcc::is_committed(timestamp)
    }

    /// Check if entry was created by another active transaction.
    ///
    fn created_by_other_active_transaction(&self, transaction_id: u64, timestamp: u64) -> bool {
        mvcc::is_provisional(timestamp) && timestamp != transaction_id
    }

    /// Check if there's a conflict with the given timestamp.
    ///
    pub fn has_conflict(&self, transaction_id: u64, start_time: u64, timestamp: u64) -> bool {
        let writer_id = (transaction_id >= TRANSACTION_ID_START).then_some(transaction_id);
        mvcc::has_conflict(timestamp, writer_id, start_time)
    }

    // =========================================================================
    // Transaction Support
    // =========================================================================

    /// Verify that a dependency still exists at commit time.
    ///
    pub fn verify_existence_of_dependency(&self, commit_id: u64, entry: &CatalogEntryEnum) -> bool {
        let map = match self.map.read() {
            Ok(m) => m,
            Err(_) => return false,
        };

        if let Some(current) = map.get_entry(entry.name()) {
            if let Some(node) = self.visible_node(&current, 0, commit_id.saturating_add(1)) {
                let timestamp = node.timestamp();
                if timestamp < TRANSACTION_ID_START && timestamp <= commit_id && !node.is_deleted()
                {
                    return true;
                }
            }
        }

        false
    }

    /// Verify we can commit a drop operation.
    ///
    pub fn commit_drop(&self, commit_id: u64, start_time: u64, entry: &CatalogEntryEnum) -> bool {
        let map = match self.map.read() {
            Ok(m) => m,
            Err(_) => return true,
        };

        if let Some(current) = map.get_entry(entry.name()) {
            let timestamp = current.timestamp();
            if timestamp > start_time && timestamp < commit_id {
                return false;
            }
        }

        true
    }

    /// Undo a catalog entry modification (for rollback).
    ///
    pub fn undo(&self, entry: &CatalogEntryEnum) {
        if let Ok(_write_lock) = self.catalog_lock.lock() {
            if let Ok(mut map) = self.map.write() {
                if let Some(head) = map.get_entry(entry.name()) {
                    if let Some(child) = head.child() {
                        map.update_entry(entry.name(), child);
                    } else {
                        map.drop_entry(entry.name());
                    }
                }
            }
        }
        self.mark_gc_dirty();
    }

    /// Update the timestamp of an entry (for commit).
    ///
    pub fn update_timestamp(&self, name: &str, timestamp: u64) {
        if let Ok(map) = self.map.read() {
            if let Some(node) = map.get_entry(name) {
                node.set_timestamp(timestamp);
                if let Some(entry) = node.entry.as_ref() {
                    self.set_entry_timestamp(entry, timestamp);
                }
            }
        }
        self.mark_gc_dirty();
    }

    pub(crate) fn for_each_chain_head<F>(&self, mut f: F)
    where
        F: FnMut(&str, &Arc<VersionedEntry>),
    {
        let Ok(map) = self.map.read() else {
            return;
        };

        for (name, head) in map.entries() {
            f(name, head);
        }
    }

    pub fn gc(&self, watermark: u64) -> CatalogGcStats {
        gc::run_collection_gc(self, watermark)
    }

    // =========================================================================
    // Utility
    // =========================================================================

    /// Get the number of entries (for debugging).
    pub fn len(&self) -> usize {
        self.map.read().map(|m| m.len()).unwrap_or(0)
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.map.read().map(|m| m.is_empty()).unwrap_or(true)
    }

    /// Get the catalog name.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{ColumnDefinition, SchemaEntry, TableCatalogEntry};
    use paro_common::types::LogicalType;
    use paro_storage::table::table_factory::TableFactory;
    use std::sync::{mpsc, Arc, Barrier};
    use std::time::Duration;

    fn make_schema_entry(name: &str, timestamp: u64) -> Arc<CatalogEntryEnum> {
        let schema = Arc::new(SchemaEntry::new(
            "test_catalog".to_string(),
            name.to_string(),
            Arc::new(AtomicU64::new(0)),
            timestamp,
        ));
        Arc::new(CatalogEntryEnum::Schema(schema))
    }

    fn make_table_entry(name: &str, timestamp: u64) -> Arc<CatalogEntryEnum> {
        let storage = Arc::new(
            TableFactory::default()
                .create_table(&[LogicalType::Integer])
                .expect("create test table"),
        );
        let table = Arc::new(TableCatalogEntry::new(
            "test_catalog".to_string(),
            "public".to_string(),
            name.to_string(),
            vec![ColumnDefinition::new(
                "id".to_string(),
                LogicalType::Integer,
            )],
            storage,
            timestamp,
        ));
        Arc::new(CatalogEntryEnum::Table(table))
    }

    fn test_schema_set() -> Arc<CatalogCollection> {
        CatalogCollection::new_for_tests("test", 1, CollectionFamily::Schemas)
    }

    fn test_table_set(schema_id: u64) -> Arc<CatalogCollection> {
        CatalogCollection::new_for_tests("test", schema_id, CollectionFamily::Tables)
    }

    #[test]
    fn test_catalog_collection_basic() {
        let set = test_schema_set();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_create_committed_entry() {
        let set = test_schema_set();
        let entry = make_schema_entry("schema1", 0);

        let result = set.create_committed_entry(entry.clone());
        assert!(result.is_some());
        assert_eq!(set.len(), 1);

        // Duplicate should fail
        let entry2 = make_schema_entry("schema1", 0);
        let result2 = set.create_committed_entry(entry2);
        assert!(result2.is_none());
    }

    #[test]
    fn test_create_entry_with_dependencies() {
        let set = test_schema_set();
        let entry = make_schema_entry("schema1", 0); // Initial timestamp

        let deps = DependencyList::new();
        let result = set.create_entry(TRANSACTION_ID_START + 1, "schema1".to_string(), entry, deps);
        assert!(result.is_ok());
        assert!(result.unwrap());
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_case_insensitive() {
        let set = test_schema_set();

        set.create_committed_entry(make_schema_entry("MySchema", 0));

        assert!(set.get_committed_entry("myschema").is_some());
        assert!(set.get_committed_entry("MYSCHEMA").is_some());
        assert!(set.get_committed_entry("MySchema").is_some());
    }

    #[test]
    fn test_mvcc_visibility() {
        let set = test_schema_set();

        let t1_id = TRANSACTION_ID_START + 1; // Complete transaction ID
        let t1_start = 10;

        let t2_id = TRANSACTION_ID_START + 2; // Complete transaction ID
        let t2_start = 20;

        let entry = make_schema_entry("schema1", 0); // Initial timestamp, will be set by create_entry
        set.create_entry(t1_id, "schema1".to_string(), entry, DependencyList::new())
            .unwrap();

        // T1 should see it (created by T1)
        assert!(set.get_entry(t1_id, t1_start, "schema1").is_some());

        // T2 should NOT see it (created by another active transaction)
        assert!(set.get_entry(t2_id, t2_start, "schema1").is_none());
    }

    #[test]
    fn test_write_write_conflict() {
        let set = test_schema_set();

        let t1_id = TRANSACTION_ID_START + 1; // Complete transaction ID
        let t2_id = TRANSACTION_ID_START + 2; // Complete transaction ID

        let entry1 = make_schema_entry("schema1", 0); // Initial timestamp
        set.create_entry(t1_id, "schema1".to_string(), entry1, DependencyList::new())
            .unwrap();

        let entry2 = make_schema_entry("schema1", 0); // Initial timestamp
        let result = set.create_entry(t2_id, "schema1".to_string(), entry2, DependencyList::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_has_conflict() {
        let set = test_schema_set();

        let t1_id = TRANSACTION_ID_START + 1; // Complete transaction ID
        let t1_start = 10;

        // Another active transaction
        assert!(set.has_conflict(t1_id, t1_start, TRANSACTION_ID_START + 2));

        // Same transaction - no conflict
        assert!(!set.has_conflict(t1_id, t1_start, t1_id));

        // Committed before start - no conflict
        assert!(!set.has_conflict(t1_id, t1_start, 5));

        // Committed after start - conflict
        assert!(set.has_conflict(t1_id, t1_start, 15));

        // Committed exactly at start - also conflict
        assert!(set.has_conflict(t1_id, t1_start, t1_start));
    }

    #[test]
    fn test_scan() {
        let set = test_schema_set();

        set.create_committed_entry(make_schema_entry("schema1", 0));
        set.create_committed_entry(make_schema_entry("schema2", 0));
        set.create_committed_entry(make_schema_entry("schema3", 0));

        let entries = set.scan(0, TRANSACTION_ID_START);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(CatalogCollection::levenshtein_distance("", ""), 0);
        assert_eq!(CatalogCollection::levenshtein_distance("abc", ""), 3);
        assert_eq!(CatalogCollection::levenshtein_distance("abc", "abc"), 0);
        assert_eq!(CatalogCollection::levenshtein_distance("abc", "abd"), 1);
    }

    #[test]
    fn test_stage_create_rejects_read_only_snapshot() {
        let set = test_schema_set();
        let snapshot = CatalogSnapshot::read_only(10);

        let err = set
            .stage_create(&snapshot, "schema1", make_schema_entry("schema1", 0))
            .expect_err("read-only snapshot must reject staged writes");

        assert!(err.to_string().contains("writer snapshot"));
    }

    #[test]
    fn test_install_replayed_uses_commit_visibility_boundary() {
        let set = test_schema_set();
        let commit_ts = 42;
        set.install_replayed(
            commit_ts,
            make_schema_entry("schema1", 0),
            InstallMode::RejectExisting,
        )
        .expect("replayed install should succeed");

        let at_commit = CatalogSnapshot::read_only(commit_ts);
        assert!(set
            .get_entry(at_commit.transaction_id, at_commit.start_time, "schema1")
            .is_none());

        let after_commit = CatalogSnapshot::read_only(commit_ts + 1);
        assert!(set
            .get_entry(
                after_commit.transaction_id,
                after_commit.start_time,
                "schema1"
            )
            .is_some());
    }

    #[test]
    fn test_rename_entry() {
        let set = test_schema_set();

        // Create an entry
        let entry = make_schema_entry("old_name", 0);
        set.create_committed_entry(entry);

        // Verify it exists
        assert!(set.get_committed_entry("old_name").is_some());
        assert!(set.get_committed_entry("new_name").is_none());

        // Rename it
        let result = set.rename_entry(0, u64::MAX, "old_name", "new_name");
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Verify the rename
        assert!(set.get_committed_entry("old_name").is_none());
        assert!(set.get_committed_entry("new_name").is_some());
    }

    #[test]
    fn test_rename_entry_conflict() {
        let set = test_schema_set();

        // Create two entries
        set.create_committed_entry(make_schema_entry("entry1", 0));
        set.create_committed_entry(make_schema_entry("entry2", 0));

        // Try to rename entry1 to entry2 (should fail)
        let result = set.rename_entry(0, u64::MAX, "entry1", "entry2");
        assert!(result.is_err());

        // Verify both entries still exist
        assert!(set.get_committed_entry("entry1").is_some());
        assert!(set.get_committed_entry("entry2").is_some());
    }

    #[test]
    fn test_rename_nonexistent_entry() {
        let set = test_table_set(1);

        // Try to rename a non-existent entry
        let result = set.rename_entry(0, u64::MAX, "nonexistent", "new_name");
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should return false

        // Verify nothing was created
        assert!(set.get_committed_entry("nonexistent").is_none());
        assert!(set.get_committed_entry("new_name").is_none());
    }

    #[test]
    fn test_stage_rename_for_table_stages_visibility_and_discard() {
        let set = test_table_set(1);
        set.create_committed_entry(make_table_entry("users", 0));

        let txn_id = TRANSACTION_ID_START + 1;
        let other_txn_id = TRANSACTION_ID_START + 2;
        let snapshot = CatalogSnapshot::writer(txn_id, 10);
        let source_entry = set.get_entry(txn_id, 10, "users").expect("source table");
        let CatalogEntryEnum::Table(table) = source_entry.as_ref() else {
            panic!("expected table entry");
        };
        let renamed_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            table.clone_with_new_schema_and_name(
                table.base.schema_name.clone(),
                "users_v2".to_string(),
                0,
            ),
        )));
        let handle = set
            .stage_rename(&snapshot, "users", "users_v2", renamed_entry)
            .expect("stage rename")
            .expect("rename handle");

        assert!(set.get_entry(txn_id, 10, "users").is_none());
        assert_eq!(
            set.get_entry(txn_id, 10, "users_v2")
                .expect("renamed entry")
                .name(),
            "users_v2"
        );
        assert!(set.get_entry(other_txn_id, 20, "users").is_some());
        assert!(set.get_entry(other_txn_id, 20, "users_v2").is_none());

        handle.discard().expect("discard rename");
        assert!(set.get_committed_entry("users").is_some());
        assert!(set.get_committed_entry("users_v2").is_none());
    }

    #[test]
    fn test_stage_move_same_collection_is_normalized_to_rename() {
        let set = test_table_set(1);
        set.create_committed_entry(make_table_entry("users", 0));

        let txn_id = TRANSACTION_ID_START + 1;
        let other_txn_id = TRANSACTION_ID_START + 2;
        let snapshot = CatalogSnapshot::writer(txn_id, 10);
        let source_entry = set.get_entry(txn_id, 10, "users").expect("source table");
        let CatalogEntryEnum::Table(table) = source_entry.as_ref() else {
            panic!("expected table entry");
        };
        let moved_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            table.clone_with_new_schema_and_name(
                table.base.schema_name.clone(),
                "users_v2".to_string(),
                0,
            ),
        )));

        let handle = set
            .stage_move(&snapshot, "users", &set, "users_v2", moved_entry)
            .expect("stage move")
            .expect("same-set move should stage a rename");

        assert!(set.get_entry(txn_id, 10, "users").is_none());
        assert_eq!(
            set.get_entry(txn_id, 10, "users_v2")
                .expect("renamed entry")
                .name(),
            "users_v2"
        );
        assert!(set.get_entry(other_txn_id, 20, "users").is_some());
        assert!(set.get_entry(other_txn_id, 20, "users_v2").is_none());

        handle.discard().expect("discard rename");
        assert!(set.get_committed_entry("users").is_some());
        assert!(set.get_committed_entry("users_v2").is_none());
    }

    #[test]
    fn test_stage_move_across_sets_stages_visibility_and_discard() {
        let source_set = test_table_set(1);
        let target_set = test_table_set(2);
        source_set.create_committed_entry(make_table_entry("users", 0));

        let txn_id = TRANSACTION_ID_START + 1;
        let other_txn_id = TRANSACTION_ID_START + 2;
        let snapshot = CatalogSnapshot::writer(txn_id, 10);
        let source_entry = source_set
            .get_entry(txn_id, 10, "users")
            .expect("source table");
        let CatalogEntryEnum::Table(table) = source_entry.as_ref() else {
            panic!("expected table entry");
        };
        let moved_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
            table.clone_with_new_schema_and_name("archive".to_string(), "users_v2".to_string(), 0),
        )));

        let handle = source_set
            .stage_move(&snapshot, "users", &target_set, "users_v2", moved_entry)
            .expect("stage move")
            .expect("move handle");

        assert!(source_set.get_entry(txn_id, 10, "users").is_none());
        assert!(source_set.get_entry(other_txn_id, 20, "users").is_some());
        let moved = target_set
            .get_entry(txn_id, 10, "users_v2")
            .expect("moved entry");
        let CatalogEntryEnum::Table(table) = moved.as_ref() else {
            panic!("expected moved table");
        };
        assert_eq!(table.base.schema_name, "archive");
        assert!(target_set.get_entry(other_txn_id, 20, "users_v2").is_none());

        handle.discard().expect("discard move");
        assert!(source_set.get_committed_entry("users").is_some());
        assert!(target_set.get_committed_entry("users_v2").is_none());
    }

    #[test]
    fn test_concurrent_cross_schema_moves_and_discards_finish_without_deadlock() {
        let left = test_table_set(1);
        let right = test_table_set(2);
        left.create_committed_entry(make_table_entry("left_users", 0));
        right.create_committed_entry(make_table_entry("right_users", 0));

        let barrier = Arc::new(Barrier::new(2));
        let (done_tx, done_rx) = mpsc::channel();

        {
            let left = Arc::clone(&left);
            let right = Arc::clone(&right);
            let barrier = Arc::clone(&barrier);
            let done_tx = done_tx.clone();
            std::thread::spawn(move || {
                let snapshot = CatalogSnapshot::writer(TRANSACTION_ID_START + 10, 10);
                barrier.wait();
                let source_entry = left
                    .get_entry(snapshot.transaction_id, snapshot.start_time, "left_users")
                    .expect("left source entry");
                let CatalogEntryEnum::Table(table) = source_entry.as_ref() else {
                    panic!("expected left table entry");
                };
                let moved_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table.clone_with_new_schema_and_name(
                        "schema_b".to_string(),
                        "left_users_moved".to_string(),
                        0,
                    ),
                )));
                let handle = left
                    .stage_move(
                        &snapshot,
                        "left_users",
                        &right,
                        "left_users_moved",
                        moved_entry,
                    )
                    .expect("stage left->right move")
                    .expect("left->right move handle");
                handle.discard().expect("discard left->right move");
                done_tx.send(()).expect("signal left thread");
            });
        }

        {
            let left = Arc::clone(&left);
            let right = Arc::clone(&right);
            let barrier = Arc::clone(&barrier);
            let done_tx = done_tx.clone();
            std::thread::spawn(move || {
                let snapshot = CatalogSnapshot::writer(TRANSACTION_ID_START + 11, 10);
                barrier.wait();
                let source_entry = right
                    .get_entry(snapshot.transaction_id, snapshot.start_time, "right_users")
                    .expect("right source entry");
                let CatalogEntryEnum::Table(table) = source_entry.as_ref() else {
                    panic!("expected right table entry");
                };
                let moved_entry = Arc::new(CatalogEntryEnum::Table(Arc::new(
                    table.clone_with_new_schema_and_name(
                        "schema_a".to_string(),
                        "right_users_moved".to_string(),
                        0,
                    ),
                )));
                let handle = right
                    .stage_move(
                        &snapshot,
                        "right_users",
                        &left,
                        "right_users_moved",
                        moved_entry,
                    )
                    .expect("stage right->left move")
                    .expect("right->left move handle");
                handle.discard().expect("discard right->left move");
                done_tx.send(()).expect("signal right thread");
            });
        }

        for _ in 0..2 {
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("concurrent move/discard should finish");
        }

        assert!(left.get_committed_entry("left_users").is_some());
        assert!(right.get_committed_entry("right_users").is_some());
        assert!(left.get_committed_entry("right_users_moved").is_none());
        assert!(right.get_committed_entry("left_users_moved").is_none());
    }
}
