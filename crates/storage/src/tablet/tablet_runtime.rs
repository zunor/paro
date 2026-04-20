// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Tablet
//!
//! Core Tablet structure managing Rowset collection with version-based MVCC.
//!
//! ## Key Design
//!
//! - Manages a collection of Rowsets with version ordering
//! - Provides version-based MVCC for concurrent read/write
//! - Supports cumulative point for compaction boundary
//! - Thread-safe access to Rowset collection

use super::delete_intent_store::DeleteIntentStore;
use super::prepared_txn_registry::PreparedTxnRegistry;
use super::statistics::TabletStatistics;
use super::tablet_meta::TabletMeta;
use super::tablet_schema::{ColumnId, TabletSchemaRef};
use super::{shutdown_sweep, wal_replay};
use crate::compaction::plan::types::CumulativePointAction;
use crate::compaction::publish::record::{
    CompactionPublishConflict, CompactionPublishConflictReason, CompactionPublishRecord,
    RetiredInput,
};
use crate::meta::TabletMetaManager;
use crate::metrics::storage_metrics;
use crate::primary_key::{
    DeleteVector, PrimaryIndex, RowID, RssidManager, PERSISTENT_INDEX_FORMAT_VERSION,
};
use crate::rowset::segment::{Segment, SegmentOptions, SegmentSharedPtr};
use crate::rowset::{Rowset, RowsetMeta, RowsetSharedPtr, RowsetState, SegmentsOverlap};
use crate::wal::write_ahead_log::WriteAheadLog;
use paro_common::durability::PrepareToken;
use paro_common::effect::{
    CompactionCumulativePointAction, RetiredRowsetInput, TabletMutation, VersionSpan,
};
use paro_common::error::{self as paro_error, Result};
use paro_journal::{JournalApplyRuntime, JournalCoordinator};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a Tablet
pub type TabletId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TabletIdentity {
    pub table_id: u64,
    pub partition_id: u64,
    pub tablet_id: u64,
    pub schema_id: u64,
    pub schema_version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalRowRef {
    pub rowset_id: u64,
    pub segment_id: u32,
    pub row_offset: u32,
}

impl PhysicalRowRef {
    pub const fn new(rowset_id: u64, segment_id: u32, row_offset: u32) -> Self {
        Self {
            rowset_id,
            segment_id,
            row_offset,
        }
    }

    pub const fn segment_key(self) -> (u64, u32) {
        (self.rowset_id, self.segment_id)
    }
}

impl From<(u64, u32, u32)> for PhysicalRowRef {
    fn from(value: (u64, u32, u32)) -> Self {
        Self::new(value.0, value.1, value.2)
    }
}

impl From<PhysicalRowRef> for (u64, u32, u32) {
    fn from(value: PhysicalRowRef) -> Self {
        (value.rowset_id, value.segment_id, value.row_offset)
    }
}

/// Tablet state enumeration (re-exported from tablet_meta)
pub use super::tablet_meta::TabletState;

/// Version range for a Rowset
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    /// Start version (inclusive)
    pub start: i64,
    /// End version (inclusive)
    pub end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionGap {
    pub missing_start: i64,
    pub missing_end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiredGcBarrier {
    PendingSnapshotBarrier,
    PendingRuntimeHandles,
    PendingRssidRetirement,
    Eligible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredPendingGcStatus {
    pub rowset_id: u64,
    pub version: Version,
    pub barrier: RetiredGcBarrier,
    pub refs_by_reader: u64,
    pub rssid_count: usize,
}

#[derive(Debug, Clone)]
struct RetiredPendingGcEntry {
    rowset: RowsetSharedPtr,
    version: Version,
    rssids: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointMaintenanceTicket {
    pub lsn: u64,
    pub maintenance_id: u64,
}

pub trait CheckpointPublishObserver: Send + Sync + std::fmt::Debug {
    fn begin_compaction_publish(&self, tablet_id: TabletId) -> CheckpointMaintenanceTicket;
    fn finish_compaction_publish(&self, ticket: CheckpointMaintenanceTicket);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTabletFreezeMode {
    Optimistic,
    MetaLock,
}

#[derive(Debug, Clone)]
pub struct CheckpointTabletSnapshot {
    pub identity: TabletIdentity,
    pub schema: TabletSchemaRef,
    pub cumulative_point: i64,
    pub max_version: i64,
    pub visible_version: i64,
    pub rowsets: Vec<RowsetSharedPtr>,
    pub freeze_mode: CheckpointTabletFreezeMode,
}

/// Primary index update metadata for a newly built rowset.
#[derive(Debug)]
pub struct PrimaryIndexUpdate {
    pub written: Vec<(Vec<u8>, Option<RowID>)>,
    pub pending_delete_vectors: HashMap<(u64, u32), DeleteVector>,
}

#[derive(Debug, Clone, Copy)]
struct VersionGraphEntry {
    rowset_id: u64,
    version: Version,
}

impl VersionGraphEntry {
    fn from_rowset(rowset: &Rowset) -> Self {
        Self {
            rowset_id: rowset.rowset_id(),
            version: rowset.version(),
        }
    }
}

const CURRENT_ROW_ID_FORMAT_VERSION: u32 = PERSISTENT_INDEX_FORMAT_VERSION;
const DELETE_INTENT_TIMEOUT_MILLIS: u64 = 5 * 60 * 1000;

pub struct TabletReadGuard {
    tablet: Arc<Tablet>,
    registration_id: u64,
    visible_version: i64,
}

impl TabletReadGuard {
    pub fn pin(tablet: &Arc<Tablet>, visible_version: i64) -> Self {
        let registration_id = tablet.register_active_snapshot(visible_version);
        Self {
            tablet: tablet.clone(),
            registration_id,
            visible_version,
        }
    }

    pub fn visible_version(&self) -> i64 {
        self.visible_version
    }
}

impl std::fmt::Debug for TabletReadGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabletReadGuard")
            .field("tablet_id", &self.tablet.tablet_id())
            .field("registration_id", &self.registration_id)
            .field("visible_version", &self.visible_version)
            .finish()
    }
}

impl Drop for TabletReadGuard {
    fn drop(&mut self) {
        self.tablet.release_active_snapshot(self.registration_id);
        self.tablet.sweep_retired_inputs();
    }
}

impl Version {
    /// Create a new version range
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    /// Create a singleton version (start == end)
    pub fn singleton(version: i64) -> Self {
        Self {
            start: version,
            end: version,
        }
    }

    /// Check if this is a singleton version
    pub fn is_singleton(&self) -> bool {
        self.start == self.end
    }

    /// Check if this version contains another version
    pub fn contains(&self, version: i64) -> bool {
        self.start <= version && version <= self.end
    }

    /// Check if this version range contains another version range
    pub fn contains_range(&self, other: &Version) -> bool {
        self.start <= other.start && other.end <= self.end
    }

    /// Check if this version range overlaps with another
    pub fn overlaps(&self, other: &Version) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

impl std::cmp::Ord for Version {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.start.cmp(&other.start) {
            std::cmp::Ordering::Equal => self.end.cmp(&other.end),
            ord => ord,
        }
    }
}

impl std::cmp::PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_singleton() {
            write!(f, "[{}]", self.start)
        } else {
            write!(f, "[{}-{}]", self.start, self.end)
        }
    }
}

/// Tablet is the fundamental storage unit in Paro
///
/// A Tablet manages a collection of Rowsets with version-based MVCC.
/// It corresponds to a table partition in the logical model.
///
/// ## Thread Safety
///
/// - `meta` is protected by RwLock for metadata updates
/// - `rs_version_map` is protected by RwLock for rowset collection
/// - Atomic counters for version tracking
///
#[derive(Debug)]
pub struct Tablet {
    /// Tablet metadata
    meta: RwLock<TabletMeta>,

    /// Data directory path
    data_dir: PathBuf,

    /// Centralized tablet metadata manager.
    tablet_meta_manager: Option<Arc<TabletMetaManager>>,

    /// Database-level WAL bound by managed database runtime.
    database_wal: RwLock<Option<Arc<WriteAheadLog>>>,

    /// Database-level journal coordinator bound by managed runtime.
    journal_coordinator: RwLock<Option<Arc<JournalCoordinator>>>,

    /// Database-level journal apply runtime bound by managed runtime.
    journal_apply_runtime: RwLock<Option<Arc<JournalApplyRuntime>>>,

    /// Rowsets indexed by version (for version-based lookup)
    /// Key: Version, Value: Rowset reference
    pub(super) rs_version_map: RwLock<BTreeMap<Version, RowsetSharedPtr>>,

    /// Incremental rowsets (recently added, not yet compacted)
    inc_rs_version_map: RwLock<HashMap<Version, RowsetSharedPtr>>,

    /// Prepared transactions for this tablet (DeltaWriter open/abort/commit)
    prepared_txns: PreparedTxnRegistry,

    /// Transactional intents for primary-key deletes/updates.
    /// key -> owner txn id
    primary_delete_intents: DeleteIntentStore<Vec<u8>>,

    /// Transactional intents for row-id deletes/updates.
    /// (rowset_id, segment_id, row_id) -> owner txn id
    row_id_delete_intents: DeleteIntentStore<PhysicalRowRef>,

    /// Cumulative compaction point (versions below this are base)
    cumulative_point: AtomicI64,

    /// Maximum committed version
    max_version: AtomicI64,

    /// Next rowset ID to assign
    next_rowset_id: AtomicI64,

    /// Monotonic epoch covering visible rowset/delete state.
    rowset_epoch: AtomicU64,

    /// Highest journal LSN durably reflected by authoritative tablet state.
    applied_lsn: AtomicU64,

    /// Tablet-local rssid allocator and mapping state.
    rssid_manager: RssidManager,

    /// Meta lock to synchronize metadata operations
    pub(super) meta_lock: RwLock<()>,

    /// Lightweight mutation counter used by checkpoint optimistic capture.
    checkpoint_capture_epoch: AtomicU64,

    /// Bound checkpoint observer for compaction publish sequencing.
    checkpoint_publish_observer: RwLock<Option<Arc<dyn CheckpointPublishObserver>>>,

    /// Maintenance publish ids for visible and retired rowsets.
    rowset_maintenance_ids: RwLock<HashMap<u64, u64>>,

    /// In-memory primary index (L0) for PRIMARY_KEYS model.
    pub(super) primary_index: RwLock<Arc<PrimaryIndex>>,

    /// Flush request flag for L0→L1 persistent index.
    pub(super) primary_index_flush_requested: Arc<AtomicBool>,

    /// Whether L0 currently contains a full keyset (vs. partial overlay).
    pub(super) primary_index_full: AtomicBool,

    /// Cached tablet statistics (lazy).
    statistics_cache: RwLock<Option<TabletStatistics>>,

    /// Dirty flag for statistics cache.
    statistics_dirty: AtomicBool,

    /// Active reader snapshots keyed by registration id.
    active_snapshots: RwLock<HashMap<u64, i64>>,

    /// Monotonic registration id allocator for active snapshots.
    next_snapshot_id: AtomicI64,

    /// Retired compaction inputs kept alive until all GC barriers clear.
    retired_pending_gc: RwLock<HashMap<u64, RetiredPendingGcEntry>>,

    /// Declared ART predicate indexes that should be rebuilt for new rowsets.
    declared_art_columns: RwLock<HashSet<ColumnId>>,
}

impl Tablet {
    /// Create a new Tablet from metadata
    ///
    /// # Arguments
    /// * `meta` - Tablet metadata
    ///
    /// # Returns
    /// A new Tablet instance
    pub fn create_from_meta(
        meta: TabletMeta,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Result<Self> {
        let data_dir = PathBuf::from(meta.data_dir());
        Self::ensure_storage_layout_root(&data_dir)?;
        let cumulative_point = meta.cumulative_layer_point();
        let max_version = meta.max_version();
        let rowset_epoch = meta.rowset_epoch();
        let applied_lsn = meta.applied_lsn();
        let rssid_manager = RssidManager::from_entries(meta.rssid_mappings());
        let primary_index_flush_requested = Arc::new(AtomicBool::new(false));
        let primary_index = Arc::new(PrimaryIndex::new());
        {
            let flush_flag = primary_index_flush_requested.clone();
            primary_index.register_mem_exceed_callback(move |_| {
                flush_flag.store(true, Ordering::Release);
            });
        }

        Ok(Self {
            meta: RwLock::new(meta),
            data_dir,
            tablet_meta_manager,
            database_wal: RwLock::new(None),
            journal_coordinator: RwLock::new(None),
            journal_apply_runtime: RwLock::new(None),
            rs_version_map: RwLock::new(BTreeMap::new()),
            inc_rs_version_map: RwLock::new(HashMap::new()),
            prepared_txns: PreparedTxnRegistry::new(),
            primary_delete_intents: DeleteIntentStore::new(DELETE_INTENT_TIMEOUT_MILLIS),
            row_id_delete_intents: DeleteIntentStore::new(DELETE_INTENT_TIMEOUT_MILLIS),
            cumulative_point: AtomicI64::new(cumulative_point),
            max_version: AtomicI64::new(max_version),
            next_rowset_id: AtomicI64::new(1),
            rowset_epoch: AtomicU64::new(rowset_epoch),
            applied_lsn: AtomicU64::new(applied_lsn),
            rssid_manager,
            meta_lock: RwLock::new(()),
            checkpoint_capture_epoch: AtomicU64::new(0),
            checkpoint_publish_observer: RwLock::new(None),
            rowset_maintenance_ids: RwLock::new(HashMap::new()),
            primary_index: RwLock::new(primary_index),
            primary_index_flush_requested,
            primary_index_full: AtomicBool::new(true),
            statistics_cache: RwLock::new(None),
            statistics_dirty: AtomicBool::new(true),
            active_snapshots: RwLock::new(HashMap::new()),
            next_snapshot_id: AtomicI64::new(1),
            retired_pending_gc: RwLock::new(HashMap::new()),
            declared_art_columns: RwLock::new(HashSet::new()),
        })
    }

    /// Create a new Tablet with schema
    ///
    /// # Arguments
    /// * `tablet_id` - Unique tablet identifier
    /// * `table_id` - Parent table identifier
    /// * `partition_id` - Partition identifier
    /// * `schema` - Tablet schema
    /// * `data_dir` - Data directory path
    pub fn new(
        tablet_id: TabletId,
        table_id: u64,
        partition_id: u64,
        schema: TabletSchemaRef,
        data_dir: impl Into<PathBuf>,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
    ) -> Result<Self> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir).map_err(|e| {
            paro_error::io_error(format!("create tablet data dir {:?}: {}", data_dir, e))
        })?;
        let meta = TabletMeta::new(
            tablet_id,
            table_id,
            partition_id,
            schema,
            data_dir.to_string_lossy().to_string(),
        )?;

        Self::create_from_meta(meta, tablet_meta_manager)
    }

    // ==================== Getters ====================

    /// Get tablet ID
    pub fn tablet_id(&self) -> TabletId {
        self.meta.read().unwrap().tablet_id()
    }

    /// Get table ID
    pub fn table_id(&self) -> u64 {
        self.meta.read().unwrap().table_id()
    }

    /// Get partition ID
    pub fn partition_id(&self) -> u64 {
        self.meta.read().unwrap().partition_id()
    }

    /// Get schema hash from tablet metadata.
    pub fn schema_hash(&self) -> u32 {
        self.meta.read().unwrap().schema_hash()
    }

    /// Record that this tablet declares a runtime ART predicate index on `column_id`.
    pub fn mark_declared_art_column(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_art_columns.write() {
            guard.insert(column_id);
        }
    }

    /// Remove a declared runtime ART predicate index from `column_id`.
    pub fn unmark_declared_art_column(&self, column_id: ColumnId) {
        if let Ok(mut guard) = self.declared_art_columns.write() {
            guard.remove(&column_id);
        }
    }

    /// Return declared ART predicate columns in a stable order.
    pub fn declared_art_columns(&self) -> Vec<ColumnId> {
        self.declared_art_columns
            .read()
            .map(|guard| {
                let mut columns = guard.iter().copied().collect::<Vec<_>>();
                columns.sort_unstable();
                columns
            })
            .unwrap_or_default()
    }

    /// Get tablet state
    pub fn state(&self) -> TabletState {
        self.meta.read().unwrap().tablet_state()
    }

    /// Get schema reference
    pub fn schema(&self) -> Option<TabletSchemaRef> {
        self.meta.read().unwrap().schema().cloned()
    }

    /// Get data directory
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    pub fn bind_database_wal(&self, wal: Option<Arc<WriteAheadLog>>) {
        *self.database_wal.write().unwrap() = wal;
    }

    pub fn database_wal(&self) -> Option<Arc<WriteAheadLog>> {
        self.database_wal.read().unwrap().clone()
    }

    pub fn bind_journal_coordinator(&self, coordinator: Option<Arc<JournalCoordinator>>) {
        *self.journal_coordinator.write().unwrap() = coordinator;
    }

    pub fn journal_coordinator(&self) -> Option<Arc<JournalCoordinator>> {
        self.journal_coordinator.read().unwrap().clone()
    }

    pub fn bind_journal_apply_runtime(&self, runtime: Option<Arc<JournalApplyRuntime>>) {
        *self.journal_apply_runtime.write().unwrap() = runtime;
    }

    pub fn journal_apply_runtime(&self) -> Option<Arc<JournalApplyRuntime>> {
        self.journal_apply_runtime.read().unwrap().clone()
    }

    pub fn bind_checkpoint_publish_observer(&self, observer: Arc<dyn CheckpointPublishObserver>) {
        *self.checkpoint_publish_observer.write().unwrap() = Some(observer);
    }

    pub fn begin_checkpoint_compaction_publish(&self) -> Option<CheckpointMaintenanceTicket> {
        self.checkpoint_publish_observer
            .read()
            .unwrap()
            .as_ref()
            .map(|observer| observer.begin_compaction_publish(self.tablet_id()))
    }

    pub fn finish_checkpoint_compaction_publish(&self, ticket: CheckpointMaintenanceTicket) {
        if let Some(observer) = self.checkpoint_publish_observer.read().unwrap().as_ref() {
            observer.finish_compaction_publish(ticket);
        }
    }

    pub fn rowsets_dir(&self) -> PathBuf {
        self.data_dir.join("rowsets")
    }

    pub fn compaction_staging_dir(&self) -> PathBuf {
        self.data_dir.join("_compaction")
    }

    pub fn canonical_rowset_path(&self, rowset_id: u64) -> PathBuf {
        self.rowsets_dir().join(format!("rowset_{}", rowset_id))
    }

    /// Get cumulative point
    pub fn cumulative_point(&self) -> i64 {
        self.cumulative_point.load(Ordering::Acquire)
    }

    pub fn rowset_epoch(&self) -> u64 {
        self.rowset_epoch.load(Ordering::Acquire)
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn.load(Ordering::Acquire)
    }

    pub fn schema_epoch(&self) -> Option<u64> {
        self.schema().map(|schema| schema.schema_version() as u64)
    }

    pub fn prepare_token(&self, visible_version: i64) -> PrepareToken {
        PrepareToken {
            visible_version,
            rowset_epoch: self.rowset_epoch(),
            schema_epoch: self.schema_epoch(),
        }
    }

    pub fn validate_prepare_token(&self, token: &PrepareToken) -> Result<()> {
        let current_visible_version = self.max_version();
        if current_visible_version != token.visible_version {
            return Err(paro_error::serialization_failure(format!(
                "tablet {} prepare token stale: visible_version {} -> {}",
                self.tablet_id(),
                token.visible_version,
                current_visible_version
            )));
        }
        let current_epoch = self.rowset_epoch();
        if current_epoch != token.rowset_epoch {
            return Err(paro_error::serialization_failure(format!(
                "tablet {} prepare token stale: rowset_epoch {} -> {}",
                self.tablet_id(),
                token.rowset_epoch,
                current_epoch
            )));
        }
        if self.schema_epoch() != token.schema_epoch {
            return Err(paro_error::serialization_failure(format!(
                "tablet {} prepare token stale: schema_epoch {:?} -> {:?}",
                self.tablet_id(),
                token.schema_epoch,
                self.schema_epoch()
            )));
        }
        Ok(())
    }

    /// Get maximum committed version
    pub fn max_version(&self) -> i64 {
        self.max_version.load(Ordering::Acquire)
    }

    /// Get aggregated tablet statistics for visible rowsets.
    pub fn statistics(&self) -> Result<TabletStatistics> {
        if !self.statistics_dirty.load(Ordering::Acquire) {
            if let Some(stats) = self.statistics_cache.read().unwrap().clone() {
                return Ok(stats);
            }
        }

        let visible = self.max_version();
        let rowsets = self.capture_consistent_rowsets(visible)?;
        let stats = TabletStatistics::from_rowsets(&rowsets)?;

        {
            let mut cache = self.statistics_cache.write().unwrap();
            *cache = Some(stats.clone());
        }
        self.statistics_dirty.store(false, Ordering::Release);
        Ok(stats)
    }

    pub(crate) fn invalidate_statistics(&self) {
        self.statistics_dirty.store(true, Ordering::Release);
        let mut cache = self.statistics_cache.write().unwrap();
        *cache = None;
    }

    fn note_checkpoint_capture_mutation(&self) {
        self.checkpoint_capture_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Get number of rowsets
    pub fn num_rowsets(&self) -> usize {
        self.rs_version_map.read().unwrap().len()
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Acquire transactional primary-key delete intents for `txn_id`.
    pub fn acquire_primary_delete_intents(&self, txn_id: u64, keys: &[Vec<u8>]) -> Result<()> {
        let now_ms = Self::now_millis();
        self.primary_delete_intents.expire_before(now_ms);
        self.primary_delete_intents
            .acquire_many(txn_id, keys, now_ms, |owner_txn_id, _| {
                paro_error::serialization_failure(format!(
                    "write-write conflict on tablet {} primary key delete (txn {} vs txn {})",
                    self.tablet_id(),
                    owner_txn_id,
                    txn_id
                ))
            })
    }

    /// Release transactional primary-key delete intents owned by `txn_id`.
    pub fn release_primary_delete_intents(&self, txn_id: u64, keys: &[Vec<u8>]) {
        self.primary_delete_intents.release_many(txn_id, keys);
    }

    pub(crate) fn has_pending_delete_intents(&self) -> bool {
        !self.primary_delete_intents.is_empty() || !self.row_id_delete_intents.is_empty()
    }

    /// Acquire transactional row-id delete intents for `txn_id`.
    pub fn acquire_row_id_delete_intents(
        &self,
        txn_id: u64,
        locations: &[PhysicalRowRef],
    ) -> Result<()> {
        let now_ms = Self::now_millis();
        self.row_id_delete_intents.expire_before(now_ms);
        self.row_id_delete_intents
            .acquire_many(txn_id, locations, now_ms, |owner_txn_id, location| {
                paro_error::serialization_failure(format!(
                    "write-write conflict on tablet {} row-id delete (txn {} vs txn {}) at rowset={}, segment={}, row={}",
                    self.tablet_id(),
                    owner_txn_id,
                    txn_id,
                    location.rowset_id,
                    location.segment_id,
                    location.row_offset
                ))
            })
    }

    /// Release transactional row-id delete intents owned by `txn_id`.
    pub fn release_row_id_delete_intents(&self, txn_id: u64, locations: &[PhysicalRowRef]) {
        self.row_id_delete_intents.release_many(txn_id, locations);
    }

    #[cfg(test)]
    pub(crate) fn expire_delete_intents_for_test(&self) {
        self.primary_delete_intents.force_expire_all();
        self.row_id_delete_intents.force_expire_all();
    }

    /// Register a prepared transaction for this tablet (DeltaWriter::open).
    pub fn prepare_txn(&self, txn_id: u64) -> Result<()> {
        self.ensure_not_shutdown("prepare transaction")?;
        self.prepared_txns.prepare(self.tablet_id(), txn_id)
    }

    /// Finish a prepared transaction (DeltaWriter::commit/abort/cancel).
    pub fn finish_txn(&self, txn_id: u64) {
        self.prepared_txns.finish(txn_id);
    }

    /// Validate that the committed rowset version graph is internally legal.
    pub fn validate_version_graph(&self) -> Result<()> {
        let entries: Vec<_> = self
            .rs_version_map
            .read()
            .unwrap()
            .values()
            .map(|rowset| VersionGraphEntry::from_rowset(rowset))
            .collect();
        Self::validate_version_graph_entries(&entries)
    }

    /// Find rowset by rowset_id across committed and incremental maps.
    pub fn find_rowset_by_id(&self, rowset_id: crate::rowset::RowsetId) -> Option<RowsetSharedPtr> {
        {
            let map = self.rs_version_map.read().ok()?;
            if let Some(rs) = map.values().find(|rs| rs.rowset_id() == rowset_id) {
                return Some(rs.clone());
            }
        }
        {
            let inc = self.inc_rs_version_map.read().ok()?;
            if let Some(rs) = inc.values().find(|rs| rs.rowset_id() == rowset_id) {
                return Some(rs.clone());
            }
        }
        self.retired_pending_gc
            .read()
            .ok()?
            .get(&rowset_id)
            .map(|entry| entry.rowset.clone())
    }

    fn register_active_snapshot(&self, visible_version: i64) -> u64 {
        let registration_id = self.next_snapshot_id.fetch_add(1, Ordering::SeqCst) as u64;
        self.active_snapshots
            .write()
            .unwrap()
            .insert(registration_id, visible_version);
        registration_id
    }

    fn release_active_snapshot(&self, registration_id: u64) {
        self.active_snapshots
            .write()
            .unwrap()
            .remove(&registration_id);
    }

    pub fn min_active_visible_version(&self) -> Option<i64> {
        self.active_snapshots
            .read()
            .unwrap()
            .values()
            .copied()
            .min()
    }

    pub fn retired_pending_gc_statuses(&self) -> Vec<RetiredPendingGcStatus> {
        let min_active_visible_version = self.min_active_visible_version();
        let retired = self.retired_pending_gc.read().unwrap();
        let mut statuses = retired
            .iter()
            .map(|(&rowset_id, entry)| RetiredPendingGcStatus {
                rowset_id,
                version: entry.version,
                barrier: self.retired_gc_barrier(entry, min_active_visible_version),
                refs_by_reader: entry.rowset.ref_count(),
                rssid_count: entry.rssids.len(),
            })
            .collect::<Vec<_>>();
        statuses.sort_by_key(|status| status.rowset_id);
        statuses
    }

    fn retired_gc_barrier(
        &self,
        entry: &RetiredPendingGcEntry,
        min_active_visible_version: Option<i64>,
    ) -> RetiredGcBarrier {
        if min_active_visible_version.is_some_and(|version| version <= entry.version.end) {
            return RetiredGcBarrier::PendingSnapshotBarrier;
        }
        if entry.rowset.ref_count() > 0 {
            return RetiredGcBarrier::PendingRuntimeHandles;
        }
        if !entry.rssids.is_empty() {
            return RetiredGcBarrier::PendingRssidRetirement;
        }
        RetiredGcBarrier::Eligible
    }

    fn register_retired_inputs(&self, inputs: &[RowsetSharedPtr], retired_inputs: &[RetiredInput]) {
        let retired_meta: HashMap<_, _> = retired_inputs
            .iter()
            .map(|input| (input.rowset_id, input))
            .collect();
        let mut retired = self.retired_pending_gc.write().unwrap();
        for rowset in inputs {
            let meta = retired_meta.get(&rowset.rowset_id());
            retired.insert(
                rowset.rowset_id(),
                RetiredPendingGcEntry {
                    rowset: rowset.clone(),
                    version: meta
                        .map(|input| input.version)
                        .unwrap_or_else(|| rowset.version()),
                    rssids: meta.map(|input| input.rssids.clone()).unwrap_or_default(),
                },
            );
        }
        drop(retired);
        self.sweep_retired_inputs();
    }

    fn sweep_retired_inputs(&self) {
        let removable: Vec<u64> = self
            .retired_pending_gc_statuses()
            .into_iter()
            .filter(|status| status.barrier == RetiredGcBarrier::Eligible)
            .map(|status| status.rowset_id)
            .collect();
        if removable.is_empty() {
            return;
        }

        let mut retired = self.retired_pending_gc.write().unwrap();
        let mut maintenance_ids = self.rowset_maintenance_ids.write().unwrap();
        for rowset_id in removable {
            retired.remove(&rowset_id);
            maintenance_ids.remove(&rowset_id);
        }
    }

    // ==================== State Management ====================

    /// Initialize the tablet (transition to Running state and load rowsets)
    pub fn init(&self) -> Result<()> {
        // Scope the meta guard to avoid holding it across I/O heavy rebuilds.
        {
            let mut meta_guard = self.meta.write().unwrap();
            if meta_guard.tablet_state() != TabletState::NotReady {
                return Err(paro_error::invalid_input(format!(
                    "Cannot init tablet in state {}",
                    meta_guard.tablet_state()
                )));
            }

            // Load rowsets from metadata
            let schema = meta_guard.schema().cloned().ok_or_else(|| {
                paro_error::internal(format!("Tablet {} has no schema", meta_guard.tablet_id()))
            })?;

            let mut rs_version_map = self.rs_version_map.write().unwrap();
            let mut inc_rs_version_map = self.inc_rs_version_map.write().unwrap();

            let mut max_rowset_id = 0u64;

            // Load base rowsets
            for rs_meta in meta_guard.rowset_metas() {
                max_rowset_id = max_rowset_id.max(rs_meta.rowset_id());
                let rowset_path =
                    self.resolve_loaded_rowset_path(rs_meta.rowset_id(), rs_meta.rowset_path());
                let rowset =
                    crate::rowset::Rowset::create(schema.clone(), rs_meta.clone(), rowset_path)?;
                let rowset_ptr = Arc::new(rowset);
                rs_version_map.insert(rs_meta.version(), rowset_ptr);
            }

            // Load incremental rowsets
            for rs_meta in meta_guard.inc_rowset_metas() {
                max_rowset_id = max_rowset_id.max(rs_meta.rowset_id());
                let rowset_ptr = if let Some(rowset) = rs_version_map.get(&rs_meta.version()) {
                    rowset.clone()
                } else {
                    let rowset_path =
                        self.resolve_loaded_rowset_path(rs_meta.rowset_id(), rs_meta.rowset_path());
                    let rowset = crate::rowset::Rowset::create(
                        schema.clone(),
                        rs_meta.clone(),
                        rowset_path,
                    )?;
                    let ptr = Arc::new(rowset);
                    rs_version_map.insert(rs_meta.version(), ptr.clone());
                    ptr
                };
                inc_rs_version_map.insert(rs_meta.version(), rowset_ptr);
            }

            // Align next_rowset_id with the highest loaded rowset.
            self.next_rowset_id
                .store((max_rowset_id + 1) as i64, Ordering::Release);

            meta_guard.set_tablet_state(TabletState::Running);
        }

        let loaded_rowsets: Vec<_> = self
            .rs_version_map
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        {
            let mut maintenance_ids = self.rowset_maintenance_ids.write().unwrap();
            for rowset in &loaded_rowsets {
                maintenance_ids.entry(rowset.rowset_id()).or_insert(0);
            }
        }
        for rowset in loaded_rowsets {
            self.ensure_rowset_rssids(&rowset);
        }

        // Rebuild primary index from persistent index + WAL (best-effort).
        let rebuilt_persistent_index = self.rebuild_primary_index_from_persistent()?;
        let replay_report = wal_replay::replay_primary_wal(self)?;
        crate::compaction::cleanup::reconcile_recovery_state(self);
        if replay_report.replayed_missing_rowset_commit || replay_report.replayed_compaction_publish
        {
            self.repair_primary_index_after_replay()?;
        }
        if rebuilt_persistent_index
            || self.meta.read().unwrap().row_id_format_version() != CURRENT_ROW_ID_FORMAT_VERSION
        {
            let mut meta = self.meta.write().unwrap();
            self.sync_runtime_meta_fields(&mut meta);
            drop(meta);
            self.save_meta()?;
        }
        Ok(())
    }

    /// Set tablet state
    pub fn set_state(&self, state: TabletState) {
        self.meta.write().unwrap().set_tablet_state(state);
    }

    fn ensure_not_shutdown(&self, op: &str) -> Result<()> {
        if self.state() == TabletState::Shutdown {
            return Err(paro_error::invalid_input(format!(
                "tablet {} is shutdown; cannot {}",
                self.tablet_id(),
                op
            )));
        }
        Ok(())
    }

    // ==================== Rowset Management ====================

    /// Add a rowset to the tablet
    ///
    /// # Arguments
    /// * `rowset` - The rowset to add
    ///
    /// # Returns
    /// Ok(()) on success, error if version conflicts
    pub fn add_rowset(&self, rowset: RowsetSharedPtr) -> Result<()> {
        self.ensure_not_shutdown("add rowset")?;
        let _lock = self.meta_lock.write().unwrap();
        self.add_rowset_internal(rowset)
    }

    fn add_rowset_internal(&self, rowset: RowsetSharedPtr) -> Result<()> {
        match rowset.rowset_state() {
            RowsetState::Deleting | RowsetState::Deleted => {
                return Err(paro_error::invalid_input(format!(
                    "Cannot add rowset {} in state {}",
                    rowset.rowset_id(),
                    rowset.rowset_state()
                )));
            }
            _ => {}
        }

        if !rowset.is_visible() {
            rowset.make_visible()?;
        }

        self.validate_rowset_registration_locked(&rowset)?;
        self.register_rowset_locked(rowset);
        Ok(())
    }

    fn validate_rowset_registration_locked(&self, rowset: &Rowset) -> Result<()> {
        let rs_map = self.rs_version_map.read().unwrap();
        let entries: Vec<_> = rs_map
            .values()
            .map(|existing| VersionGraphEntry::from_rowset(existing))
            .chain(std::iter::once(VersionGraphEntry::from_rowset(rowset)))
            .collect();
        Self::validate_version_graph_entries(&entries)
    }

    fn validate_version_graph_entries(entries: &[VersionGraphEntry]) -> Result<()> {
        let mut sorted = entries.to_vec();
        sorted.sort_by_key(|entry| entry.version);

        let mut seen_rowset_ids = HashSet::with_capacity(sorted.len());
        let mut active: Option<VersionGraphEntry> = None;

        for entry in sorted {
            if !seen_rowset_ids.insert(entry.rowset_id) {
                return Err(paro_error::invalid_input(format!(
                    "invalid version graph: duplicate rowset registration for rowset {}",
                    entry.rowset_id
                )));
            }

            if let Some(active_entry) = active {
                if entry.version.start <= active_entry.version.end {
                    let detail = if active_entry.version.contains_range(&entry.version)
                        || entry.version.contains_range(&active_entry.version)
                    {
                        "compaction-style overlap"
                    } else {
                        "overlap"
                    };
                    return Err(paro_error::invalid_input(format!(
                        "invalid version graph: {detail} between rowset {} {} and rowset {} {}",
                        active_entry.rowset_id,
                        active_entry.version,
                        entry.rowset_id,
                        entry.version
                    )));
                }

                if entry.version.end > active_entry.version.end {
                    active = Some(entry);
                }
            } else {
                active = Some(entry);
            }
        }

        Ok(())
    }

    fn register_rowset_locked(&self, rowset: RowsetSharedPtr) {
        let version = rowset.version();
        let rowset_id = rowset.rowset_id();

        // Add to version map
        {
            let mut rs_map = self.rs_version_map.write().unwrap();
            rs_map.insert(version, rowset.clone());
        }

        // Add to incremental map if above cumulative point
        if version.start >= self.cumulative_point() {
            let mut inc_map = self.inc_rs_version_map.write().unwrap();
            inc_map.insert(version, rowset.clone());
        }

        self.ensure_rowset_rssids(&rowset);
        self.align_next_rowset_id(rowset.rowset_id());

        // Update max version
        let current_max = self.max_version.load(Ordering::Acquire);
        if version.end > current_max {
            self.max_version.store(version.end, Ordering::Release);
        }

        // Update metadata (ensure rowset path is persisted).
        {
            let mut meta = self.meta.write().unwrap();
            let mut rs_meta = rowset.rowset_meta();
            rs_meta.set_rowset_path(rowset.rowset_path().to_string_lossy().to_string());
            meta.add_rowset_meta(rs_meta);
        }

        self.rowset_maintenance_ids
            .write()
            .unwrap()
            .entry(rowset_id)
            .or_insert(0);

        self.invalidate_statistics();
        self.bump_rowset_epoch();
    }

    /// Commit a rowset with the given version.
    ///
    /// This is the unified publish entry point. It validates the version graph,
    /// updates rowset state, persists WAL, and registers the rowset.
    pub fn rowset_commit(&self, version: i64, rowset: RowsetSharedPtr) -> Result<()> {
        self.ensure_not_shutdown("commit rowset")?;
        let _lock = self.meta_lock.write().unwrap();
        self.rowset_commit_locked(version, rowset)
    }

    /// Commit a rowset using the next available version.
    pub fn rowset_commit_auto(&self, rowset: RowsetSharedPtr) -> Result<i64> {
        self.ensure_not_shutdown("commit rowset")?;
        let _lock = self.meta_lock.write().unwrap();
        let next_version = self.max_version.load(Ordering::Acquire) + 1;
        self.rowset_commit_locked(next_version, rowset)?;
        Ok(next_version)
    }

    fn rowset_commit_locked(&self, version: i64, rowset: RowsetSharedPtr) -> Result<()> {
        let current_max = self.max_version.load(Ordering::Acquire);

        if version <= current_max {
            // Already committed (idempotent)
            return Ok(());
        }

        self.apply_rowset_commit_locked(version, rowset)?;
        Ok(())
    }

    fn apply_rowset_commit_locked(&self, version: i64, rowset: RowsetSharedPtr) -> Result<()> {
        rowset.set_version(Version::singleton(version));
        self.validate_rowset_registration_locked(&rowset)?;
        self.write_rowset_commit_wal(&rowset)?;
        rowset.make_visible()?;
        self.register_rowset_locked(rowset);
        self.save_meta()?;

        self.validate_version_graph()?;
        Ok(())
    }

    /// Publish a PRIMARY_KEYS rowset and staged index updates atomically.
    pub fn publish_rowset_with_index(
        &self,
        version: i64,
        rowset: RowsetSharedPtr,
        update: PrimaryIndexUpdate,
    ) -> Result<()> {
        self.ensure_not_shutdown("publish rowset with primary index")?;
        let _lock = self.meta_lock.write().unwrap();
        self.publish_rowset_with_index_locked(version, rowset, update)
    }

    /// Publish a PRIMARY_KEYS rowset using the next available version.
    pub fn publish_rowset_with_index_auto(
        &self,
        rowset: RowsetSharedPtr,
        update: PrimaryIndexUpdate,
    ) -> Result<i64> {
        self.ensure_not_shutdown("publish rowset with primary index")?;
        let _lock = self.meta_lock.write().unwrap();
        let version = self.max_version.load(Ordering::Acquire) + 1;
        self.publish_rowset_with_index_locked(version, rowset, update)?;
        Ok(version)
    }

    fn publish_rowset_with_index_locked(
        &self,
        version: i64,
        rowset: RowsetSharedPtr,
        update: PrimaryIndexUpdate,
    ) -> Result<()> {
        let current_max = self.max_version.load(Ordering::Acquire);

        if version <= current_max {
            return Ok(());
        }

        rowset.set_version(Version::singleton(version));
        self.ensure_rowset_rssids(&rowset);
        self.align_next_rowset_id(rowset.rowset_id());
        self.validate_rowset_registration_locked(&rowset)?;
        let prepared = self.prepare_primary_index_publish(&rowset, update)?;
        self.write_rowset_commit_wal(&rowset)?;
        self.apply_prepared_primary_index_publish(version, prepared)?;
        rowset.make_visible()?;
        self.register_rowset_locked(rowset);
        self.reconcile_primary_index_row_count()?;
        self.maybe_flush_primary_index()?;
        self.save_meta()?;
        self.validate_version_graph()?;
        Ok(())
    }

    /// Apply row-id deletes and persist delete vectors.
    ///
    /// Loads existing segment delete vectors before applying locations so replaying
    /// multiple WAL RowIdDelete entries remains additive.
    pub(crate) fn apply_row_id_delete_locations(
        &self,
        locations: &[(u64, u32, u32)],
    ) -> Result<()> {
        let physical_locations: Vec<_> = locations
            .iter()
            .copied()
            .map(PhysicalRowRef::from)
            .collect();
        self.apply_row_id_delete_refs(&physical_locations)
    }

    pub(crate) fn apply_row_id_delete_locations_idempotent_at_version(
        &self,
        locations: &[(u64, u32, u32)],
        delete_version: i64,
    ) -> Result<()> {
        let physical_locations: Vec<_> = locations
            .iter()
            .copied()
            .map(PhysicalRowRef::from)
            .collect();
        self.apply_row_id_delete_refs_internal(&physical_locations, delete_version, true)
    }

    pub(crate) fn apply_row_id_delete_refs(&self, locations: &[PhysicalRowRef]) -> Result<()> {
        let version = self.max_version.load(Ordering::Acquire);
        self.apply_row_id_delete_refs_internal(locations, version, false)
    }

    fn apply_row_id_delete_refs_internal(
        &self,
        locations: &[PhysicalRowRef],
        version: i64,
        ignore_already_deleted: bool,
    ) -> Result<()> {
        if locations.is_empty() {
            return Ok(());
        }

        // Serialize with compaction rowset replacement to keep rowset visibility stable.
        let _meta_lock = self.meta_lock.write().unwrap();

        let mut dedup = HashSet::with_capacity(locations.len());
        let mut unique_locations = Vec::with_capacity(locations.len());
        for &location in locations {
            if dedup.insert(location) {
                unique_locations.push(location);
            }
        }

        let mut pending: HashMap<(u64, u32), DeleteVector> = HashMap::new();
        let mut rowset_cache: HashMap<u64, RowsetSharedPtr> = HashMap::new();

        for location in unique_locations {
            let rowset = if let Some(cached) = rowset_cache.get(&location.rowset_id) {
                cached.clone()
            } else {
                let resolved = self.find_rowset_by_id(location.rowset_id).ok_or_else(|| {
                    paro_error::serialization_failure(format!(
                        "write-write conflict on tablet {} row-id delete: rowset {} no longer visible",
                        self.tablet_id(),
                        location.rowset_id
                    ))
                })?;
                rowset_cache.insert(location.rowset_id, resolved.clone());
                resolved
            };

            let key = location.segment_key();
            let existing = DeleteVector::load_from_dir_at_version(
                rowset.rowset_path(),
                location.segment_id,
                version,
            )?;
            match pending.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    if o.get().is_deleted(location.row_offset) {
                        if ignore_already_deleted {
                            continue;
                        }
                        return Err(paro_error::serialization_failure(format!(
                            "write-write conflict on tablet {} row-id delete: rowset={}, segment={}, row={} already deleted",
                            self.tablet_id(),
                            location.rowset_id,
                            location.segment_id,
                            location.row_offset
                        )));
                    }
                    o.get_mut().mark_deleted(location.row_offset);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    let mut dv = DeleteVector::with_version(version);
                    if dv.is_deleted(location.row_offset) {
                        if ignore_already_deleted {
                            continue;
                        }
                        return Err(paro_error::serialization_failure(format!(
                            "write-write conflict on tablet {} row-id delete: rowset={}, segment={}, row={} already deleted",
                            self.tablet_id(),
                            location.rowset_id,
                            location.segment_id,
                            location.row_offset
                        )));
                    }
                    if existing
                        .as_ref()
                        .is_some_and(|delete_vector| delete_vector.is_deleted(location.row_offset))
                    {
                        if ignore_already_deleted {
                            continue;
                        }
                        return Err(paro_error::serialization_failure(format!(
                            "write-write conflict on tablet {} row-id delete: rowset={}, segment={}, row={} already deleted",
                            self.tablet_id(),
                            location.rowset_id,
                            location.segment_id,
                            location.row_offset
                        )));
                    }
                    dv.mark_deleted(location.row_offset);
                    v.insert(dv);
                }
            }
        }

        self.persist_delete_vectors(version, pending)?;
        Ok(())
    }

    pub(super) fn persist_delete_vectors(
        &self,
        version: i64,
        mut vectors: HashMap<(u64, u32), DeleteVector>,
    ) -> Result<()> {
        let mut updated_rowsets = HashSet::new();
        for ((rs_id, seg_id), dv) in vectors.drain() {
            if let Some(rowset) = self.find_rowset_by_id(rs_id) {
                let mut chain =
                    DeleteVector::load_versioned_from_dir(rowset.rowset_path(), seg_id)?;
                let deletes: Vec<u32> = dv.iter().collect();
                if deletes.is_empty() {
                    continue;
                }
                chain.add_dels_as_new_version(&deletes, version);
                let min_visible_version = self
                    .min_active_visible_version()
                    .unwrap_or_else(|| self.max_version.load(Ordering::Acquire));
                chain.gc_versions_older_than(min_visible_version);
                let path = chain.save_to_dir(rowset.rowset_path(), seg_id)?;
                rowset.invalidate_delete_vector_cache(seg_id);
                updated_rowsets.insert(rs_id);
                if !path.exists() {
                    return Err(paro_error::io_error("delete vector not persisted"));
                }
            }
        }
        let had_updates = !updated_rowsets.is_empty();
        for rs_id in updated_rowsets {
            if let Some(rowset) = self.find_rowset_by_id(rs_id) {
                self.refresh_rowset_delete_stats(&rowset, version)?;
            }
        }
        if had_updates {
            self.invalidate_statistics();
            self.bump_rowset_epoch();
        }
        Ok(())
    }

    fn refresh_rowset_delete_stats(&self, rowset: &RowsetSharedPtr, version: i64) -> Result<()> {
        let segment_ids: Vec<u32> = {
            let segments = rowset.segments();
            if segments.is_empty() {
                (0..rowset.rowset_meta().num_segments()).collect()
            } else {
                segments
                    .iter()
                    .map(|segment| segment.segment_id())
                    .collect()
            }
        };
        let mut num_vectors = 0u32;
        let mut num_deleted_rows = 0u64;
        for seg_id in segment_ids {
            if let Some(dv) =
                DeleteVector::load_from_dir_at_version(rowset.rowset_path(), seg_id, version)?
            {
                if dv.cardinality() > 0 {
                    num_vectors += 1;
                    num_deleted_rows += dv.cardinality();
                }
            }
        }
        rowset.set_delete_stats(num_vectors, num_deleted_rows);
        Ok(())
    }

    fn row_locations_for_rowset(rowset: &Rowset) -> Result<Vec<PhysicalRowRef>> {
        let mut out = Vec::with_capacity(rowset.num_rows() as usize);
        for seg in rowset.segments() {
            let num_rows = seg.num_rows() as u32;
            for row_id in 0..num_rows {
                out.push(PhysicalRowRef::new(
                    rowset.rowset_id(),
                    seg.segment_id(),
                    row_id,
                ));
            }
        }
        Ok(out)
    }

    pub(crate) fn with_meta_lock<T, F>(&self, op: &str, f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.ensure_not_shutdown(op)?;
        let _lock = self.meta_lock.write().unwrap();
        f()
    }

    pub(crate) fn rowset_rssids(&self, rowset: &Rowset) -> Vec<u32> {
        let segment_ids: Vec<u32> = {
            let segments = rowset.segments();
            if segments.is_empty() {
                (0..rowset.rowset_meta().num_segments()).collect()
            } else {
                segments
                    .iter()
                    .map(|segment| segment.segment_id())
                    .collect()
            }
        };

        segment_ids
            .into_iter()
            .filter_map(|segment_id| self.rssid_manager.rssid_for(rowset.rowset_id(), segment_id))
            .collect()
    }

    pub(crate) fn validate_compaction_publish_locked(
        &self,
        record: &CompactionPublishRecord,
        inputs: &[RowsetSharedPtr],
        output: &Rowset,
    ) -> Result<()> {
        let input_ids: HashSet<u64> = inputs.iter().map(|rowset| rowset.rowset_id()).collect();

        for input in inputs {
            let Some(current) = self.find_rowset_by_id(input.rowset_id()) else {
                return Err(CompactionPublishConflict {
                    tablet_id: self.tablet_id(),
                    plan_id: record.plan_id,
                    job_id: record.job_id,
                    reason: CompactionPublishConflictReason::InputsMissing,
                }
                .into_paro_error());
            };
            if current.version() != input.version() {
                return Err(CompactionPublishConflict {
                    tablet_id: self.tablet_id(),
                    plan_id: record.plan_id,
                    job_id: record.job_id,
                    reason: CompactionPublishConflictReason::InputsReplaced,
                }
                .into_paro_error());
            }
        }

        if self.find_rowset_by_id(record.output_rowset_id).is_some() {
            return Err(CompactionPublishConflict {
                tablet_id: self.tablet_id(),
                plan_id: record.plan_id,
                job_id: record.job_id,
                reason: CompactionPublishConflictReason::VersionOverlap,
            }
            .into_paro_error());
        }

        let rs_map = self.rs_version_map.read().unwrap();
        let candidate_entries: Vec<_> = rs_map
            .values()
            .filter(|rowset| !input_ids.contains(&rowset.rowset_id()))
            .map(|rowset| VersionGraphEntry::from_rowset(rowset))
            .chain(std::iter::once(VersionGraphEntry::from_rowset(output)))
            .collect();
        Self::validate_version_graph_entries(&candidate_entries).map_err(|_| {
            CompactionPublishConflict {
                tablet_id: self.tablet_id(),
                plan_id: record.plan_id,
                job_id: record.job_id,
                reason: CompactionPublishConflictReason::VersionOverlap,
            }
            .into_paro_error()
        })
    }

    pub(crate) fn install_compaction_publish_locked(
        &self,
        inputs: &[RowsetSharedPtr],
        retired_inputs: &[RetiredInput],
        output: RowsetSharedPtr,
        output_maintenance_id: u64,
        cumulative_point_action: CumulativePointAction,
        allow_existing_output: bool,
    ) -> Result<()> {
        let version = output.version();
        let to_delete: Vec<RowsetSharedPtr> = inputs.to_vec();

        {
            let mut rs_map = self.rs_version_map.write().unwrap();
            let mut inc_map = self.inc_rs_version_map.write().unwrap();
            for rs in inputs {
                rs_map.remove(&rs.version());
                inc_map.remove(&rs.version());
            }
            if allow_existing_output {
                rs_map.remove(&version);
                inc_map.remove(&version);
            }

            let candidate_entries: Vec<_> = rs_map
                .values()
                .map(|rowset| VersionGraphEntry::from_rowset(rowset))
                .chain(std::iter::once(VersionGraphEntry::from_rowset(
                    output.as_ref(),
                )))
                .collect();
            Self::validate_version_graph_entries(&candidate_entries)?;

            rs_map.insert(version, output.clone());
            if version.start >= self.cumulative_point() {
                inc_map.insert(version, output.clone());
            }

            if matches!(
                cumulative_point_action,
                CumulativePointAction::AdvanceToOutputEndExclusive
            ) {
                let next_point = version.end.saturating_add(1);
                self.cumulative_point.store(next_point, Ordering::Release);
                inc_map.retain(|candidate, _| candidate.start >= next_point);
                self.meta
                    .write()
                    .unwrap()
                    .set_cumulative_layer_point(next_point);
            }
        }

        self.ensure_rowset_rssids(&output);

        let current_max = self.max_version.load(Ordering::Acquire);
        if version.end > current_max {
            self.max_version.store(version.end, Ordering::Release);
        }

        {
            let mut meta = self.meta.write().unwrap();
            for rs in inputs {
                meta.delete_rowset_meta(rs.rowset_id());
            }
            let mut output_meta = output.rowset_meta();
            output_meta.set_rowset_path(output.rowset_path().to_string_lossy().to_string());
            meta.add_rowset_meta(output_meta);
        }

        {
            let mut maintenance_ids = self.rowset_maintenance_ids.write().unwrap();
            for rs in inputs {
                maintenance_ids.entry(rs.rowset_id()).or_insert(0);
            }
            maintenance_ids.insert(output.rowset_id(), output_maintenance_id);
        }

        self.invalidate_statistics();
        self.register_retired_inputs(&to_delete, retired_inputs);

        for rs in to_delete {
            let _ = rs.mark_deleting();
        }

        self.validate_version_graph()?;
        self.bump_rowset_epoch();
        Ok(())
    }

    /// Get rowset with the maximum version
    pub fn rowset_with_max_version(&self) -> Option<RowsetSharedPtr> {
        let rs_map = self.rs_version_map.read().unwrap();
        rs_map.values().last().cloned()
    }

    /// Get rowset by version
    pub fn get_rowset_by_version(&self, version: i64) -> Option<RowsetSharedPtr> {
        let rs_map = self.rs_version_map.read().unwrap();
        // Since BTreeMap is ordered by start version, we can scan for the matching range.
        // Multiple rowsets may contain the same version if they overlap.
        for (v, rowset) in rs_map.iter() {
            if v.contains(version) {
                return Some(rowset.clone());
            }
        }
        None
    }

    /// Capture consistent rowsets for a given visible version
    ///
    /// Returns all rowsets that should be visible at the given version.
    /// This is the core of version-based MVCC.
    ///
    /// # Arguments
    /// * `visible_version` - The version to read at
    ///
    /// # Returns
    /// Vector of rowsets visible at the given version
    pub fn capture_consistent_rowsets(&self, visible_version: i64) -> Result<Vec<RowsetSharedPtr>> {
        let rs_map = self.rs_version_map.read().unwrap();

        let mut result: Vec<RowsetSharedPtr> = rs_map
            .values()
            .filter(|rs| rs.is_visible() && rs.end_version() <= visible_version)
            .cloned()
            .collect();
        result.sort_by_key(|rs| rs.start_version());
        Ok(result)
    }

    pub fn capture_checkpoint_snapshot(
        self: &Arc<Self>,
        checkpoint_commit_id: u64,
        checkpoint_maintenance_id: u64,
        optimistic_retries: usize,
    ) -> Result<CheckpointTabletSnapshot> {
        let visible_version = i64::try_from(checkpoint_commit_id)
            .map_err(|_| paro_error::invalid_input("checkpoint commit id exceeds i64"))?;
        let mut invalidated_optimistic_snapshots = 0usize;

        for _ in 0..optimistic_retries {
            let epoch_before = self.checkpoint_capture_epoch.load(Ordering::Acquire);
            let guard = TabletReadGuard::pin(self, visible_version);
            let snapshot = self.build_checkpoint_snapshot(
                visible_version,
                checkpoint_maintenance_id,
                CheckpointTabletFreezeMode::Optimistic,
            )?;
            let epoch_after = self.checkpoint_capture_epoch.load(Ordering::Acquire);
            drop(guard);
            if epoch_before == epoch_after {
                storage_metrics()
                    .record_checkpoint_capture(false, invalidated_optimistic_snapshots);
                return Ok(snapshot);
            }
            invalidated_optimistic_snapshots += 1;
        }

        let snapshot = self.with_meta_lock("capture checkpoint snapshot", || {
            let _guard = TabletReadGuard::pin(self, visible_version);
            self.build_checkpoint_snapshot(
                visible_version,
                checkpoint_maintenance_id,
                CheckpointTabletFreezeMode::MetaLock,
            )
        })?;
        storage_metrics().record_checkpoint_capture(true, invalidated_optimistic_snapshots);
        Ok(snapshot)
    }

    pub fn capture_checkpoint_meta_bytes(
        &self,
        snapshot: &CheckpointTabletSnapshot,
    ) -> Result<Vec<u8>> {
        let mut meta = self.meta.read().unwrap().clone();
        let existing_rowset_ids: Vec<u64> = meta
            .rowset_metas()
            .iter()
            .chain(meta.inc_rowset_metas().iter())
            .map(|rowset| rowset.rowset_id())
            .collect();
        for rowset_id in existing_rowset_ids {
            meta.delete_rowset_meta(rowset_id);
        }
        meta.set_cumulative_layer_point(snapshot.cumulative_point);
        for rowset in &snapshot.rowsets {
            meta.add_rowset_meta(rowset.rowset_meta());
        }
        meta.serialize()
    }

    fn build_checkpoint_snapshot(
        &self,
        visible_version: i64,
        checkpoint_maintenance_id: u64,
        freeze_mode: CheckpointTabletFreezeMode,
    ) -> Result<CheckpointTabletSnapshot> {
        let schema = self.schema().ok_or_else(|| {
            paro_error::internal("tablet schema missing during checkpoint capture")
        })?;
        let rowsets =
            self.capture_checkpoint_rowsets(visible_version, checkpoint_maintenance_id)?;
        Ok(CheckpointTabletSnapshot {
            identity: TabletIdentity {
                table_id: self.table_id(),
                partition_id: self.partition_id(),
                tablet_id: self.tablet_id(),
                schema_id: schema.schema_id(),
                schema_version: schema.schema_version(),
            },
            schema,
            cumulative_point: self.cumulative_point(),
            max_version: self.max_version(),
            visible_version,
            rowsets,
            freeze_mode,
        })
    }

    fn capture_checkpoint_rowsets(
        &self,
        visible_version: i64,
        checkpoint_maintenance_id: u64,
    ) -> Result<Vec<RowsetSharedPtr>> {
        let visible_rowsets = self.capture_consistent_rowsets(visible_version)?;
        let mut resolved = Vec::new();
        let mut visited = HashSet::new();
        for rowset in visible_rowsets {
            self.expand_checkpoint_rowset(
                &rowset,
                checkpoint_maintenance_id,
                &mut visited,
                &mut resolved,
            )?;
        }
        resolved.sort_by_key(|rowset| rowset.version());
        Ok(resolved)
    }

    fn expand_checkpoint_rowset(
        &self,
        rowset: &RowsetSharedPtr,
        checkpoint_maintenance_id: u64,
        visited: &mut HashSet<u64>,
        resolved: &mut Vec<RowsetSharedPtr>,
    ) -> Result<()> {
        if !visited.insert(rowset.rowset_id()) {
            return Ok(());
        }

        let metadata = rowset.rowset_meta();
        let maintenance_id = self
            .rowset_maintenance_ids
            .read()
            .unwrap()
            .get(&rowset.rowset_id())
            .copied()
            .unwrap_or(0);

        if !metadata.is_compaction_output() || maintenance_id <= checkpoint_maintenance_id {
            resolved.push(rowset.clone());
            return Ok(());
        }

        for source_rowset_id in metadata.source_rowset_ids() {
            let source_rowset = self.find_rowset_by_id(*source_rowset_id).ok_or_else(|| {
                paro_error::internal(format!(
                    "checkpoint capture missing retired compaction input {} on tablet {}",
                    source_rowset_id,
                    self.tablet_id()
                ))
            })?;
            self.expand_checkpoint_rowset(
                &source_rowset,
                checkpoint_maintenance_id,
                visited,
                resolved,
            )?;
        }

        Ok(())
    }

    /// Get maximum continuous version from version 0
    ///
    /// Returns the highest version N such that versions [0, N] are all present.
    pub fn max_continuous_version(&self) -> i64 {
        match self.detect_version_gaps().first().copied() {
            Some(gap) => gap.missing_start - 1,
            None => self.max_version(),
        }
    }

    /// Detect missing committed versions in the read-only range `[0, max_version]`.
    pub fn detect_version_gaps(&self) -> Vec<VersionGap> {
        let target_max = self.max_version();
        if target_max < 0 {
            return Vec::new();
        }

        let rs_map = self.rs_version_map.read().unwrap();
        let mut gaps = Vec::new();
        let mut next_expected = 0i64;

        for version in rs_map.keys() {
            if version.end < 0 {
                continue;
            }
            let start = version.start.max(0);
            let end = version.end.min(target_max);
            if end < 0 || start > target_max {
                continue;
            }

            if start > next_expected {
                gaps.push(VersionGap {
                    missing_start: next_expected,
                    missing_end: start - 1,
                });
            }

            next_expected = next_expected.max(end.saturating_add(1));
            if next_expected > target_max {
                break;
            }
        }

        if next_expected <= target_max {
            gaps.push(VersionGap {
                missing_start: next_expected,
                missing_end: target_max,
            });
        }

        gaps
    }

    /// Pick rowsets for compaction
    ///
    /// Returns rowsets that should be compacted based on the compaction policy.
    /// This is a simplified version - actual implementation will use CompactionPolicy.
    ///
    /// # Arguments
    /// * `max_rowsets` - Maximum number of rowsets to pick
    ///
    /// # Returns
    /// Vector of rowsets to compact
    pub fn pick_rowsets_to_compact(&self, max_rowsets: usize) -> Vec<RowsetSharedPtr> {
        let inc_map = self.inc_rs_version_map.read().unwrap();
        let mut candidate_rowsets: Vec<_> = inc_map.values().cloned().collect();
        // Sort by version to ensure deterministic order
        candidate_rowsets.sort_by_key(|rs| rs.version());
        candidate_rowsets.into_iter().take(max_rowsets).collect()
    }

    /// Set cumulative point after compaction
    pub fn set_cumulative_point(&self, point: i64) {
        self.cumulative_point.store(point, Ordering::Release);
        self.meta.write().unwrap().set_cumulative_layer_point(point);

        // Move rowsets from incremental to base
        let mut inc_map = self.inc_rs_version_map.write().unwrap();
        inc_map.retain(|v, _| v.start >= point);
    }

    /// Allocate next rowset ID
    pub fn next_rowset_id(&self) -> u64 {
        self.next_rowset_id.fetch_add(1, Ordering::SeqCst) as u64
    }

    pub fn rssid_manager(&self) -> &RssidManager {
        &self.rssid_manager
    }

    // ==================== Drop/Shutdown ====================

    /// Mark this tablet as shutdown and enqueue asynchronous directory cleanup.
    ///
    /// Frontground DDL only performs state transition + queueing. Actual file
    /// deletion or move-to-trash happens in background and is idempotent.
    pub fn mark_shutdown_and_schedule_sweep(&self, move_to_trash: bool) -> Result<()> {
        let meta_snapshot = {
            let mut meta = self.meta.write().unwrap();
            if meta.tablet_state() != TabletState::Shutdown {
                meta.set_tablet_state(TabletState::Shutdown);
            }
            meta.clone()
        };
        self.persist_meta(&meta_snapshot)?;
        shutdown_sweep::schedule_shutdown_sweep(&self.data_dir, move_to_trash)
    }

    /// Mark shutdown by persisted data dir and enqueue background cleanup.
    ///
    /// Used by catalog drop path when only a storage descriptor is available.
    pub fn mark_shutdown_and_schedule_sweep_by_data_dir(
        data_dir: impl AsRef<Path>,
        move_to_trash: bool,
    ) -> Result<()> {
        let data_dir = data_dir.as_ref();
        if !data_dir.exists() {
            return Ok(());
        }
        shutdown_sweep::schedule_shutdown_sweep(data_dir, move_to_trash)
    }

    // ==================== Persistence ====================

    /// Save tablet metadata to disk
    pub fn save_meta(&self) -> Result<()> {
        let mut meta = self.meta.read().unwrap().clone();
        self.sync_runtime_meta_fields(&mut meta);
        self.persist_meta(&meta)
    }

    pub fn persist_meta_snapshot(&self) -> Result<()> {
        self.save_meta()
    }

    fn persist_meta(&self, meta: &TabletMeta) -> Result<()> {
        let mut persisted = meta.clone();
        self.sync_runtime_meta_fields(&mut persisted);
        if let Some(manager) = &self.tablet_meta_manager {
            manager.save_tablet_meta(&persisted)?;
        }
        Ok(())
    }

    fn sync_runtime_meta_fields(&self, meta: &mut TabletMeta) {
        meta.set_rssid_mappings(self.rssid_manager.snapshot_entries());
        meta.set_row_id_format_version(CURRENT_ROW_ID_FORMAT_VERSION);
        meta.set_rowset_epoch(self.rowset_epoch());
        meta.set_applied_lsn(self.applied_lsn());
    }

    fn write_rowset_commit_wal(&self, rowset: &Rowset) -> Result<()> {
        let wal = WriteAheadLog::new(self.wal_path())?;
        wal.write_rowset_commit(
            self.tablet_id(),
            rowset.rowset_id(),
            rowset.start_version(),
            rowset.end_version(),
            rowset.rowset_path().to_string_lossy().as_ref(),
        )?;
        wal.flush()?;
        Ok(())
    }

    pub(crate) fn write_compaction_publish_wal(
        &self,
        record: &CompactionPublishRecord,
    ) -> Result<()> {
        let wal = WriteAheadLog::new(self.wal_path())?;
        wal.write_compaction_publish(record)?;
        wal.flush()?;
        Ok(())
    }

    pub(crate) fn bump_rowset_epoch(&self) -> u64 {
        self.note_checkpoint_capture_mutation();
        self.rowset_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn note_applied_lsn(&self, lsn: u64) -> Result<()> {
        if lsn == 0 {
            return Ok(());
        }
        let previous = self.applied_lsn.fetch_max(lsn, Ordering::AcqRel);
        if previous >= lsn {
            return Ok(());
        }
        self.save_meta()
    }

    fn align_next_rowset_id(&self, rowset_id: u64) {
        self.next_rowset_id
            .fetch_max((rowset_id.saturating_add(1)) as i64, Ordering::SeqCst);
    }

    fn resolve_loaded_rowset_path(&self, rowset_id: u64, stored_path: &str) -> PathBuf {
        if !stored_path.is_empty() {
            return PathBuf::from(stored_path);
        }

        let canonical = self.canonical_rowset_path(rowset_id);
        if canonical.exists() {
            canonical
        } else {
            self.data_dir.clone()
        }
    }

    fn ensure_storage_layout_root(data_dir: &Path) -> Result<()> {
        fs::create_dir_all(data_dir.join("rowsets")).map_err(|err| {
            paro_error::io_error(format!(
                "create tablet rowsets root {}: {}",
                data_dir.join("rowsets").display(),
                err
            ))
        })?;
        fs::create_dir_all(data_dir.join("_compaction")).map_err(|err| {
            paro_error::io_error(format!(
                "create tablet compaction root {}: {}",
                data_dir.join("_compaction").display(),
                err
            ))
        })?;
        fs::create_dir_all(data_dir.join("_delete_patch")).map_err(|err| {
            paro_error::io_error(format!(
                "create tablet delete patch root {}: {}",
                data_dir.join("_delete_patch").display(),
                err
            ))
        })?;
        Ok(())
    }

    pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        let dir = File::open(parent).map_err(|err| {
            paro_error::io_error(format!(
                "open parent directory {} for fsync: {}",
                parent.display(),
                err
            ))
        })?;
        dir.sync_all().map_err(|err| {
            paro_error::io_error(format!(
                "fsync parent directory {}: {}",
                parent.display(),
                err
            ))
        })
    }

    pub(crate) fn ensure_rowset_rssids(&self, rowset: &Rowset) {
        let segment_ids: Vec<u32> = {
            let segments = rowset.segments();
            if segments.is_empty() {
                (0..rowset.rowset_meta().num_segments()).collect()
            } else {
                segments
                    .iter()
                    .map(|segment| segment.segment_id())
                    .collect()
            }
        };

        for segment_id in segment_ids {
            self.rssid_manager.allocate(rowset.rowset_id(), segment_id);
        }
    }

    pub fn encode_row_location(&self, location: PhysicalRowRef) -> Result<RowID> {
        let rssid = self
            .rssid_manager
            .rssid_for(location.rowset_id, location.segment_id)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "missing rssid mapping for rowset {} segment {}",
                    location.rowset_id, location.segment_id
                ))
            })?;
        Ok(RowID::new(rssid, location.row_offset))
    }

    pub fn decode_row_id(&self, row_id: RowID) -> Result<PhysicalRowRef> {
        if row_id.is_null() {
            return Err(paro_error::invalid_input("NULL RowID cannot be decoded"));
        }
        let (rowset_id, segment_id) =
            self.rssid_manager.resolve(row_id.rssid()).ok_or_else(|| {
                paro_error::internal(format!(
                    "missing rssid {} mapping while decoding RowID",
                    row_id.rssid()
                ))
            })?;
        Ok(PhysicalRowRef::new(
            rowset_id,
            segment_id,
            row_id.row_offset(),
        ))
    }

    pub fn encode_row_locations(&self, locations: &[PhysicalRowRef]) -> Result<Vec<RowID>> {
        locations
            .iter()
            .copied()
            .map(|location| self.encode_row_location(location))
            .collect()
    }

    pub fn decode_row_ids(&self, row_ids: &[RowID]) -> Result<Vec<PhysicalRowRef>> {
        row_ids
            .iter()
            .copied()
            .map(|row_id| self.decode_row_id(row_id))
            .collect()
    }

    pub(crate) fn row_ids_for_rowset(&self, rowset: &Rowset) -> Result<Vec<RowID>> {
        let locations = Self::row_locations_for_rowset(rowset)?;
        self.encode_row_locations(&locations)
    }

    /// Open tablet metadata from the centralized metadata manager.
    pub fn open(
        tablet_id: TabletId,
        data_dir: impl Into<PathBuf>,
        tablet_meta_manager: Arc<TabletMetaManager>,
    ) -> Result<Self> {
        let data_dir = data_dir.into();
        let mut meta = tablet_meta_manager
            .load_tablet_meta(tablet_id)?
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "tablet {} metadata missing from TabletMetaManager for {}",
                    tablet_id,
                    data_dir.display()
                ))
            })?;

        // Metadata is persisted in RUNNING state; reset to allow init on reload.
        if meta.tablet_state() != TabletState::NotReady {
            meta.set_tablet_state(TabletState::NotReady);
        }
        let tablet = Self::create_from_meta(meta, Some(tablet_meta_manager))?;
        tablet.init()?;
        Ok(tablet)
    }
}

impl Tablet {
    /// WAL path helper.
    fn wal_path(&self) -> PathBuf {
        self.data_dir.join("tablet.wal")
    }

    fn load_replayed_rowset(
        &self,
        rowset_id: u64,
        version: Version,
        rowset_path: &str,
    ) -> Result<Option<RowsetSharedPtr>> {
        let schema = match self.schema() {
            Some(schema) => schema,
            None => return Ok(None),
        };

        let path = PathBuf::from(rowset_path);
        if !path.exists() {
            return Ok(None);
        }

        let mut segment_ids: Vec<u32> = Vec::new();
        for entry in fs::read_dir(&path)
            .map_err(|e| paro_error::io_error(format!("read rowset dir {:?}: {}", path, e)))?
        {
            let entry =
                entry.map_err(|e| paro_error::io_error(format!("read rowset entry: {}", e)))?;
            let candidate = entry.path();
            if candidate.extension().and_then(|s| s.to_str()) != Some("dat") {
                continue;
            }
            if let Some(stem) = candidate.file_stem().and_then(|s| s.to_str()) {
                if let Ok(id) = stem.parse::<u32>() {
                    segment_ids.push(id);
                }
            }
        }
        segment_ids.sort_unstable();

        let rowset_gen = if version.end < 0 {
            0
        } else {
            version.end as u64
        };

        let mut segments: Vec<SegmentSharedPtr> = Vec::with_capacity(segment_ids.len());
        let mut num_rows = 0u64;
        let mut data_size = 0u64;
        let mut index_size = 0u64;

        for seg_id in &segment_ids {
            let seg_path = path.join(format!("{}.dat", seg_id));
            if !seg_path.exists() {
                continue;
            }
            let segment = Segment::open(
                *seg_id,
                &seg_path,
                schema.clone(),
                SegmentOptions::default().with_verify_checksum(false),
                self.tablet_id(),
                rowset_id,
                rowset_gen,
            )?;
            num_rows += segment.num_rows();
            data_size += segment.data_size();
            index_size += segment.index_size();
            segments.push(Arc::new(segment));
        }

        let mut num_delete_vectors = 0u32;
        let mut num_deleted_rows = 0u64;
        for seg_id in &segment_ids {
            if let Some(delete_vector) =
                DeleteVector::load_from_dir_at_version(&path, *seg_id, self.max_version())?
            {
                num_delete_vectors += 1;
                num_deleted_rows += delete_vector.cardinality();
            }
        }

        let mut meta = RowsetMeta::new(rowset_id, self.tablet_id(), version);
        meta.set_num_rows(num_rows);
        meta.set_num_segments(segments.len() as u32);
        meta.set_disk_sizes(data_size, index_size);
        meta.set_rowset_state(RowsetState::Visible);
        meta.set_rowset_path(rowset_path.to_string());
        meta.set_delete_info(num_delete_vectors, num_deleted_rows);
        meta.set_segments_overlap(if segments.len() <= 1 {
            SegmentsOverlap::NonOverlapping
        } else {
            SegmentsOverlap::Unknown
        });

        let rowset = if segments.is_empty() {
            Rowset::create(schema, meta, &path)?
        } else {
            Rowset::create_with_segments(schema, meta, &path, segments)?
        };
        Ok(Some(Arc::new(rowset)))
    }

    /// Replay a RowsetCommit WAL entry by reconstructing rowset metadata and registering it.
    pub(crate) fn replay_rowset_commit(
        &self,
        rowset_id: u64,
        start_version: i64,
        end_version: i64,
        rowset_path: &str,
    ) -> Result<()> {
        let version = Version::new(start_version, end_version);

        // Skip if already present.
        if self.find_rowset_by_id(rowset_id).is_some() {
            return Ok(());
        }

        // Skip if the version has already been recovered or superseded by a
        // compaction output that covers the same range.
        if !wal_replay::should_replay_rowset_commit(self, &version) {
            return Ok(());
        }

        let Some(rowset) = self.load_replayed_rowset(rowset_id, version, rowset_path)? else {
            return Ok(());
        };

        self.add_rowset(rowset)?;
        self.save_meta()?;
        Ok(())
    }

    pub(crate) fn replay_compaction_publish(&self, record: &CompactionPublishRecord) -> Result<()> {
        let maybe_output = self.find_rowset_by_id(record.output_rowset_id);
        let live_inputs: Vec<_> = record
            .replaced_inputs
            .iter()
            .filter_map(|rowset_id| self.find_rowset_by_id(*rowset_id))
            .collect();

        if maybe_output.is_some() && live_inputs.is_empty() {
            return Ok(());
        }

        let output = if let Some(existing) = maybe_output {
            existing
        } else {
            self.load_replayed_rowset(
                record.output_rowset_id,
                record.output_version,
                &record.output_rowset_path,
            )?
            .ok_or_else(|| {
                paro_error::io_error(format!(
                    "published compaction output {} missing from {} during replay",
                    record.output_rowset_id, record.output_rowset_path
                ))
            })?
        };

        output.make_visible()?;
        self.with_meta_lock("replay compaction publish", || {
            self.install_compaction_publish_locked(
                &live_inputs,
                &[],
                output.clone(),
                0,
                record.cumulative_point_action,
                true,
            )
        })?;
        self.save_meta()?;
        Ok(())
    }

    pub(crate) fn apply_compaction_publish(&self, op: &TabletMutation) -> Result<()> {
        let TabletMutation::PublishCompaction {
            output_rowset_id,
            output_version,
            cumulative_point_action,
            staged_ref,
            output_ref,
            replaced_inputs,
            retired_inputs,
            ..
        } = op
        else {
            return Err(paro_error::internal(
                "apply_compaction_publish called with non-compaction op",
            ));
        };

        let VersionSpan { start, end } = *output_version;
        let version = Version::new(start, end);
        let maybe_output = self.find_rowset_by_id(*output_rowset_id);
        let live_inputs: Vec<_> = replaced_inputs
            .iter()
            .filter_map(|rowset_id| self.find_rowset_by_id(*rowset_id))
            .collect();

        if maybe_output.is_some() && live_inputs.is_empty() {
            return Ok(());
        }

        let final_path = output_ref.resolve_for_tablet(self.data_dir());
        let staged_path = staged_ref.resolve_for_tablet(self.data_dir());
        let output = if let Some(existing) = maybe_output {
            existing
        } else {
            if !final_path.exists() && staged_path.exists() {
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).map_err(|err| {
                        paro_error::io_error(format!(
                            "create compaction output parent {}: {}",
                            parent.display(),
                            err
                        ))
                    })?;
                }
                fs::rename(&staged_path, &final_path).map_err(|err| {
                    paro_error::io_error(format!(
                        "publish compaction artifact {} -> {}: {}",
                        staged_path.display(),
                        final_path.display(),
                        err
                    ))
                })?;
                Self::sync_parent_dir(&final_path)?;
            }

            self.load_replayed_rowset(*output_rowset_id, version, &final_path.to_string_lossy())?
                .ok_or_else(|| {
                    paro_error::io_error(format!(
                        "published compaction output {} missing from {} during apply",
                        output_rowset_id,
                        final_path.display()
                    ))
                })?
        };

        output.make_visible()?;
        self.with_meta_lock("apply compaction publish", || {
            self.install_compaction_publish_locked(
                &live_inputs,
                &retired_inputs
                    .iter()
                    .map(Self::retired_input_from_op)
                    .collect::<Vec<_>>(),
                output.clone(),
                0,
                match cumulative_point_action {
                    CompactionCumulativePointAction::Preserve => CumulativePointAction::Preserve,
                    CompactionCumulativePointAction::AdvanceToOutputEndExclusive => {
                        CumulativePointAction::AdvanceToOutputEndExclusive
                    }
                },
                true,
            )
        })?;
        self.save_meta()?;
        self.validate_primary_index_consistency_after_compaction(output.as_ref())?;
        Ok(())
    }

    fn retired_input_from_op(input: &RetiredRowsetInput) -> RetiredInput {
        RetiredInput {
            rowset_id: input.rowset_id,
            version: Version::new(input.start_version, input.end_version),
            rssids: input.rssids.clone(),
        }
    }
}

/// Shared pointer to Tablet (thread-safe)
pub type TabletRef = Arc<Tablet>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::plan::types::CumulativePointAction;
    use crate::meta::{FileMetadataStore, MetadataStore, TabletMetaManager};
    use crate::metrics::storage_metrics;
    use crate::primary_key::{PersistentIndex, PrimaryKeySerializer, RowID};
    use crate::rowset::RowsetMeta;
    use crate::rowset::{RowsetWriter, RowsetWriterContext};
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use crate::wal::wal_entry::WalEntry;
    use crate::wal::write_ahead_log::WriteAheadLog;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
    }

    fn create_test_meta_manager(tmp: &TempDir) -> Arc<TabletMetaManager> {
        let store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(tmp.path().join("meta")).unwrap());
        Arc::new(TabletMetaManager::with_store_and_data_root(
            store,
            tmp.path(),
        ))
    }

    #[test]
    fn test_version_basic() {
        let v1 = Version::singleton(5);
        assert!(v1.is_singleton());
        assert!(v1.contains(5));
        assert!(!v1.contains(4));

        let v2 = Version::new(1, 10);
        assert!(!v2.is_singleton());
        assert!(v2.contains(5));
        assert!(!v2.contains(0));
    }

    #[test]
    fn test_version_overlaps() {
        let v1 = Version::new(1, 5);
        let v2 = Version::new(3, 7);
        let v3 = Version::new(6, 10);

        assert!(v1.overlaps(&v2));
        assert!(v2.overlaps(&v3));
        assert!(!v1.overlaps(&v3));
    }

    #[test]
    fn test_tablet_new() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test_tablet", None).unwrap();

        assert_eq!(tablet.tablet_id(), 1);
        assert_eq!(tablet.table_id(), 100);
        assert_eq!(tablet.partition_id(), 1000);
        assert_eq!(tablet.state(), TabletState::NotReady);
        assert_eq!(tablet.num_rowsets(), 0);
    }

    fn create_test_rowset(id: u64, version: Version, tablet_id: u64) -> RowsetSharedPtr {
        let schema = create_test_schema();
        let meta = RowsetMeta::new(id, tablet_id, version);
        let rowset = crate::rowset::Rowset::create(schema, meta, "/tmp/test").unwrap();
        Arc::new(rowset)
    }

    fn chunk_with_names(ids: &[i64], names: &[&str]) -> Chunk {
        Chunk::from_vectors(vec![Vector::from_i64(ids), Vector::from_strings(names)])
    }

    #[test]
    fn test_tablet_init() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        assert_eq!(tablet.state(), TabletState::NotReady);
        tablet.init().unwrap();
        assert_eq!(tablet.state(), TabletState::Running);
    }

    #[test]
    fn test_tablet_add_rowset() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        let rowset1 = create_test_rowset(1, Version::singleton(0), 1);
        let rowset2 = create_test_rowset(2, Version::singleton(1), 1);

        tablet.add_rowset(rowset1).unwrap();
        tablet.add_rowset(rowset2).unwrap();

        assert_eq!(tablet.num_rowsets(), 2);
        assert_eq!(tablet.max_version(), 1);
    }

    #[test]
    fn test_rowset_commit_allocates_and_persists_rssid() {
        let tmp = TempDir::new().unwrap();
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![
                    TabletColumn::key(0, "id", LogicalType::BigInt),
                    TabletColumn::new(1, "value", LogicalType::BigInt),
                ],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let data_dir = tmp.path().to_string_lossy().to_string();
        let manager = create_test_meta_manager(&tmp);
        let tablet = Tablet::new(
            1,
            100,
            1000,
            schema.clone(),
            data_dir,
            Some(manager.clone()),
        )
        .unwrap();

        let rowset_path = tmp.path().join("rowset_7");
        let context = crate::rowset::RowsetWriterContext::new(
            schema,
            tablet.tablet_id(),
            Version::singleton(0),
            &rowset_path,
        )
        .with_rowset_id(7)
        .with_short_key_index(false)
        .with_compression(crate::rowset::page::CompressionType::None);
        let mut writer = crate::rowset::RowsetWriter::create(context).unwrap();
        let key_bytes: Vec<u8> = 1i64.to_le_bytes().to_vec();
        let value_bytes: Vec<u8> = 99i64.to_le_bytes().to_vec();
        writer
            .add_chunk(&[
                crate::rowset::segment::ColumnData::new(key_bytes, 1),
                crate::rowset::segment::ColumnData::new(value_bytes, 1),
            ])
            .unwrap();
        let rowset = Arc::new(writer.build().unwrap());

        tablet.rowset_commit_auto(rowset.clone()).unwrap();

        assert_eq!(
            tablet.rssid_manager().rssid_for(rowset.rowset_id(), 0),
            Some(0)
        );
        assert_eq!(
            tablet.rssid_manager().resolve(0),
            Some((rowset.rowset_id(), 0))
        );

        tablet.save_meta().unwrap();

        let reopened = Tablet::open(1, tmp.path(), manager).unwrap();
        assert_eq!(
            reopened.rssid_manager().rssid_for(rowset.rowset_id(), 0),
            Some(0)
        );
        assert_eq!(
            reopened.rssid_manager().resolve(0),
            Some((rowset.rowset_id(), 0))
        );
    }

    #[test]
    fn test_tablet_version_conflict() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        let rowset1 = create_test_rowset(1, Version::new(0, 5), 1);
        let rowset2 = create_test_rowset(2, Version::new(3, 7), 1);

        tablet.add_rowset(rowset1).unwrap();
        let result = tablet.add_rowset(rowset2);
        assert!(result.is_err());
    }

    #[test]
    fn test_tablet_capture_consistent_rowsets() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        tablet
            .add_rowset(create_test_rowset(1, Version::singleton(0), 1))
            .unwrap();
        tablet
            .add_rowset(create_test_rowset(2, Version::singleton(1), 1))
            .unwrap();
        tablet
            .add_rowset(create_test_rowset(3, Version::singleton(2), 1))
            .unwrap();

        let rowsets = tablet.capture_consistent_rowsets(1).unwrap();
        assert_eq!(rowsets.len(), 2);

        let rowsets = tablet.capture_consistent_rowsets(5).unwrap();
        assert_eq!(rowsets.len(), 3);

        tablet
            .add_rowset(create_test_rowset(4, Version::new(3, 10), 1))
            .unwrap();
        let rowsets = tablet.capture_consistent_rowsets(10).unwrap();
        assert_eq!(rowsets.len(), 4);
    }

    #[test]
    #[serial_test::serial]
    fn checkpoint_snapshot_reuses_retired_inputs_for_post_cut_compaction_output() {
        storage_metrics().reset_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = create_test_schema();
        let tablet =
            Arc::new(Tablet::new(1, 100, 1000, schema, dir.path().join("tablet"), None).unwrap());

        let input_a = create_test_rowset(10, Version::singleton(0), tablet.tablet_id());
        let input_b = create_test_rowset(11, Version::singleton(1), tablet.tablet_id());
        tablet.add_rowset(input_a.clone()).unwrap();
        tablet.add_rowset(input_b.clone()).unwrap();

        let output = create_test_rowset(12, Version::new(0, 1), tablet.tablet_id());
        output.mark_compaction_output(vec![10, 11]);
        output.make_visible().unwrap();

        tablet
            .with_meta_lock("test compaction publish", || {
                tablet.install_compaction_publish_locked(
                    &[input_a.clone(), input_b.clone()],
                    &[
                        RetiredInput {
                            rowset_id: 10,
                            version: Version::singleton(0),
                            rssids: vec![1],
                        },
                        RetiredInput {
                            rowset_id: 11,
                            version: Version::singleton(1),
                            rssids: vec![2],
                        },
                    ],
                    output,
                    7,
                    CumulativePointAction::Preserve,
                    false,
                )
            })
            .unwrap();

        let snapshot = tablet
            .capture_checkpoint_snapshot(1, 0, 0)
            .expect("checkpoint snapshot should resolve through retired inputs");
        let rowset_ids: Vec<_> = snapshot
            .rowsets
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        let metrics = storage_metrics().snapshot();
        assert_eq!(snapshot.freeze_mode, CheckpointTabletFreezeMode::MetaLock);
        assert_eq!(rowset_ids, vec![10, 11]);
        assert_eq!(metrics.checkpoint_capture_optimistic_total, 0);
        assert_eq!(metrics.checkpoint_capture_meta_lock_total, 1);
        assert_eq!(metrics.checkpoint_capture_retry_total, 0);
    }

    #[test]
    #[serial_test::serial]
    fn checkpoint_snapshot_records_optimistic_capture_metrics() {
        storage_metrics().reset_for_tests();
        let schema = create_test_schema();
        let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap());

        tablet
            .add_rowset(create_test_rowset(
                1,
                Version::singleton(0),
                tablet.tablet_id(),
            ))
            .unwrap();

        let snapshot = tablet
            .capture_checkpoint_snapshot(0, 0, 1)
            .expect("checkpoint snapshot should succeed optimistically");
        let metrics = storage_metrics().snapshot();

        assert_eq!(snapshot.freeze_mode, CheckpointTabletFreezeMode::Optimistic);
        assert_eq!(metrics.checkpoint_capture_optimistic_total, 1);
        assert_eq!(metrics.checkpoint_capture_meta_lock_total, 0);
        assert_eq!(metrics.checkpoint_capture_retry_total, 0);
    }

    #[test]
    fn test_validate_version_graph_allows_gaps_and_detects_them() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        tablet
            .add_rowset(create_test_rowset(1, Version::singleton(0), 1))
            .unwrap();
        tablet
            .add_rowset(create_test_rowset(2, Version::singleton(2), 1))
            .unwrap();
        tablet
            .add_rowset(create_test_rowset(3, Version::new(4, 5), 1))
            .unwrap();

        tablet.validate_version_graph().unwrap();
        assert_eq!(
            tablet.detect_version_gaps(),
            vec![
                VersionGap {
                    missing_start: 1,
                    missing_end: 1,
                },
                VersionGap {
                    missing_start: 3,
                    missing_end: 3,
                },
            ]
        );
        assert_eq!(tablet.max_continuous_version(), 0);
    }

    #[test]
    fn test_tablet_max_continuous_version() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        tablet
            .add_rowset(create_test_rowset(1, Version::singleton(0), 1))
            .unwrap();
        tablet
            .add_rowset(create_test_rowset(2, Version::singleton(1), 1))
            .unwrap();
        // Skip version 2
        tablet
            .add_rowset(create_test_rowset(3, Version::singleton(3), 1))
            .unwrap();

        assert_eq!(tablet.max_continuous_version(), 1);
    }

    #[test]
    fn test_validate_version_graph_rejects_overlap() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        {
            let mut rs_map = tablet.rs_version_map.write().unwrap();
            rs_map.insert(
                Version::new(0, 5),
                create_test_rowset(1, Version::new(0, 5), 1),
            );
            rs_map.insert(
                Version::new(3, 7),
                create_test_rowset(2, Version::new(3, 7), 1),
            );
        }

        let err = tablet.validate_version_graph().unwrap_err();
        assert!(format!("{err}").contains("invalid version graph: overlap"));
    }

    #[test]
    fn test_validate_version_graph_rejects_compaction_overlap() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, "/tmp/test", None).unwrap();

        {
            let mut rs_map = tablet.rs_version_map.write().unwrap();
            rs_map.insert(
                Version::new(0, 5),
                create_test_rowset(1, Version::new(0, 5), 1),
            );
            rs_map.insert(
                Version::singleton(3),
                create_test_rowset(2, Version::singleton(3), 1),
            );
        }

        let err = tablet.validate_version_graph().unwrap_err();
        assert!(format!("{err}").contains("compaction-style overlap"));
    }

    #[test]
    fn test_version_display() {
        assert_eq!(format!("{}", Version::singleton(5)), "[5]");
        assert_eq!(format!("{}", Version::new(1, 10)), "[1-10]");
    }

    #[test]
    fn test_tablet_init_rebuilds_primary_index_from_persistent() {
        let tmp = TempDir::new().unwrap();
        let schema = create_test_schema();
        let data_dir = tmp.path().to_string_lossy().to_string();
        let meta = TabletMeta::new(1, 100, 1000, schema.clone(), data_dir).unwrap();

        // Build persistent index with one key
        let pi_dir = tmp.path().join("primary_index");
        let pi = PersistentIndex::new(&pi_dir).unwrap();
        pi.apply_upserts(&[(b"k1".to_vec(), RowID::new(1, 0))])
            .unwrap();

        let tablet = Tablet::create_from_meta(meta, None).unwrap();
        tablet.init().unwrap();

        assert!(tablet.lookup_primary_key(b"k1").unwrap().is_some());
    }

    #[test]
    fn test_publish_rowset_with_index_failure_keeps_rowset_invisible() {
        let tmp = TempDir::new().unwrap();
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema.clone(), tmp.path(), None).unwrap();
        tablet.init().unwrap();

        let rowset_path = tmp.path().join("rowset_publish_failure");
        let context = RowsetWriterContext::new(
            schema,
            tablet.tablet_id(),
            Version::singleton(0),
            &rowset_path,
        )
        .with_rowset_id(77);
        let mut writer = RowsetWriter::create(context).unwrap();
        writer
            .add_chunk(&[
                crate::rowset::segment::ColumnData::new(1i64.to_le_bytes().to_vec(), 1),
                crate::rowset::segment::ColumnData::new(
                    {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(&(3u32).to_le_bytes());
                        bytes.extend_from_slice(b"abc");
                        bytes
                    },
                    1,
                ),
            ])
            .unwrap();
        let rowset = Arc::new(writer.build().unwrap());

        let err = tablet
            .publish_rowset_with_index(
                0,
                rowset,
                PrimaryIndexUpdate {
                    written: Vec::new(),
                    pending_delete_vectors: HashMap::new(),
                },
            )
            .unwrap_err();
        assert!(!format!("{}", err).is_empty());
        assert_eq!(tablet.num_rowsets(), 0);
        assert_eq!(tablet.max_version(), -1);
        assert!(tablet.capture_consistent_rowsets(0).unwrap().is_empty());
        assert!(!tmp.path().join("tablet.wal").exists());
    }

    #[test]
    fn test_tablet_init_replays_rowset_commit_from_wal() {
        let tmp = TempDir::new().unwrap();
        let manager = create_test_meta_manager(&tmp);
        let schema = create_test_schema();
        let tablet = Tablet::new(
            1,
            100,
            1000,
            schema.clone(),
            tmp.path(),
            Some(manager.clone()),
        )
        .unwrap();
        tablet.init().unwrap();
        tablet.save_meta().unwrap();

        let rowset_id = 77u64;
        let rowset_path = tmp.path().join("rowset_replay_commit");
        let context = RowsetWriterContext::new(
            schema,
            tablet.tablet_id(),
            Version::singleton(0),
            &rowset_path,
        )
        .with_rowset_id(rowset_id);
        let mut writer = RowsetWriter::create(context).unwrap();
        writer
            .add_chunk(&[
                crate::rowset::segment::ColumnData::new(1i64.to_le_bytes().to_vec(), 1),
                crate::rowset::segment::ColumnData::new(
                    {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(&(3u32).to_le_bytes());
                        bytes.extend_from_slice(b"abc");
                        bytes
                    },
                    1,
                ),
            ])
            .unwrap();
        writer.build().unwrap();
        drop(tablet);

        let wal = WriteAheadLog::new(tmp.path().join("tablet.wal")).unwrap();
        wal.write_rowset_commit(1, rowset_id, 0, 0, rowset_path.to_string_lossy().as_ref())
            .unwrap();
        wal.flush().unwrap();

        let reopened = Tablet::open(1, tmp.path(), manager).unwrap();
        let rowset = reopened.find_rowset_by_id(rowset_id).unwrap();
        assert_eq!(rowset.version(), Version::singleton(0));
        assert_eq!(reopened.max_version(), 0);
    }

    #[test]
    fn test_tablet_init_replays_primary_delete_from_wal() {
        let tmp = TempDir::new().unwrap();
        let manager = create_test_meta_manager(&tmp);
        let schema = create_test_schema();
        let tablet =
            Arc::new(Tablet::new(1, 100, 1000, schema, tmp.path(), Some(manager.clone())).unwrap());
        tablet.init().unwrap();

        let mut writer = crate::write::DeltaWriter::open(tablet.clone(), 10).unwrap();
        writer
            .write_chunk(&chunk_with_names(&[1], &["old"]))
            .unwrap();
        writer.commit().unwrap();
        tablet.save_meta().unwrap();

        let serializer = PrimaryKeySerializer::from_schema_ref(&tablet.schema().unwrap()).unwrap();
        let key = serializer
            .encode_row(&chunk_with_names(&[1], &["ignored"]), 0)
            .unwrap();

        let wal = WriteAheadLog::new(tmp.path().join("tablet.wal")).unwrap();
        let entry = WalEntry::PrimaryDelete { keys: vec![key] };
        wal.writer()
            .write_entry(entry.wal_type(), &entry.serialize_data())
            .unwrap();
        wal.flush().unwrap();

        drop(tablet);
        let tablet = Tablet::open(1, tmp.path(), manager).unwrap();

        let serializer = PrimaryKeySerializer::from_schema_ref(&tablet.schema().unwrap()).unwrap();
        let key = serializer
            .encode_row(&chunk_with_names(&[1], &["ignored"]), 0)
            .unwrap();
        assert!(tablet.lookup_primary_key(&key).unwrap().is_none());
    }

    #[test]
    fn test_tablet_init_rebuilds_current_persistent_index_when_legacy_snapshot_exists() {
        let tmp = TempDir::new().unwrap();
        let manager = create_test_meta_manager(&tmp);
        let schema = create_test_schema();
        let tablet =
            Arc::new(Tablet::new(1, 100, 1000, schema, tmp.path(), Some(manager.clone())).unwrap());
        tablet.init().unwrap();
        tablet.save_meta().unwrap();

        let mut writer1 = crate::write::DeltaWriter::open(tablet.clone(), 10).unwrap();
        writer1
            .write_chunk(&chunk_with_names(&[1], &["old"]))
            .unwrap();
        let rowset1 = writer1.commit().unwrap();

        let mut writer2 = crate::write::DeltaWriter::open(tablet.clone(), 11).unwrap();
        writer2
            .write_chunk(&chunk_with_names(&[1], &["new"]))
            .unwrap();
        let rowset2 = writer2.commit().unwrap();

        let pi_dir = tmp.path().join("primary_index");
        std::fs::write(pi_dir.join("sst_1.sst"), b"legacy").unwrap();
        drop(tablet);

        let reopened = Tablet::open(1, tmp.path(), manager).unwrap();
        let serializer =
            PrimaryKeySerializer::from_schema_ref(&reopened.schema().unwrap()).unwrap();
        let key = serializer
            .encode_row(&chunk_with_names(&[1], &["ignored"]), 0)
            .unwrap();
        let row_id = reopened.lookup_primary_key(&key).unwrap().unwrap();
        let location = reopened.decode_row_id(row_id).unwrap();

        assert_eq!(reopened.snapshot_primary_index_entries().unwrap().len(), 1);
        assert_eq!(location.rowset_id, rowset2.rowset_id());
        assert_ne!(location.rowset_id, rowset1.rowset_id());
        assert_eq!(
            reopened.meta.read().unwrap().row_id_format_version(),
            CURRENT_ROW_ID_FORMAT_VERSION
        );
        assert!(!pi_dir.join("sst_1.sst").exists());

        let persistent = PersistentIndex::new(pi_dir).unwrap();
        assert_eq!(persistent.get(&key).unwrap(), Some(row_id));
    }

    #[test]
    fn test_tablet_init_repairs_primary_index_from_visible_rowsets() {
        let tmp = TempDir::new().unwrap();
        let manager = create_test_meta_manager(&tmp);
        let schema = create_test_schema();
        let tablet =
            Arc::new(Tablet::new(1, 100, 1000, schema, tmp.path(), Some(manager.clone())).unwrap());
        tablet.init().unwrap();
        tablet.save_meta().unwrap();

        let mut writer1 = crate::write::DeltaWriter::open(tablet.clone(), 10).unwrap();
        writer1
            .write_chunk(&chunk_with_names(&[1], &["old"]))
            .unwrap();
        let rowset1 = writer1.commit().unwrap();

        let mut writer2 = crate::write::DeltaWriter::open(tablet.clone(), 11).unwrap();
        writer2
            .write_chunk(&chunk_with_names(&[1], &["new"]))
            .unwrap();
        let rowset2 = writer2.commit().unwrap();

        let persistent = PersistentIndex::new(tmp.path().join("primary_index")).unwrap();
        persistent.reset().unwrap();

        drop(tablet);
        let reopened = Tablet::open(1, tmp.path(), manager).unwrap();
        let serializer =
            PrimaryKeySerializer::from_schema_ref(&reopened.schema().unwrap()).unwrap();
        let key = serializer
            .encode_row(&chunk_with_names(&[1], &["ignored"]), 0)
            .unwrap();
        let row_id = reopened.lookup_primary_key(&key).unwrap().unwrap();
        let location = reopened.decode_row_id(row_id).unwrap();

        assert_eq!(reopened.snapshot_primary_index_entries().unwrap().len(), 1);
        assert_eq!(location.rowset_id, rowset2.rowset_id());
        assert_ne!(location.rowset_id, rowset1.rowset_id());
    }

    #[test]
    fn test_tablet_init_replays_row_id_delete_from_wal() {
        let tmp = TempDir::new().unwrap();
        let manager = create_test_meta_manager(&tmp);
        let schema = create_test_schema();
        let data_dir = tmp.path().to_string_lossy().to_string();
        let meta = TabletMeta::new(1, 100, 1000, schema.clone(), data_dir).unwrap();

        let rowset_id = 1001u64;
        let rowset_path = tmp.path().join(format!("rowset_{}", rowset_id));
        std::fs::create_dir_all(&rowset_path).unwrap();

        let tablet = Tablet::create_from_meta(meta, Some(manager.clone())).unwrap();
        tablet.init().unwrap();

        let rowset_meta = RowsetMeta::new(rowset_id, tablet.tablet_id(), Version::singleton(0));
        let rowset = Arc::new(
            crate::rowset::Rowset::create(schema.clone(), rowset_meta, rowset_path.clone())
                .unwrap(),
        );
        tablet.add_rowset(rowset).unwrap();
        tablet.save_meta().unwrap();
        drop(tablet);

        // Two RowIdDelete entries for the same segment must be merged during replay.
        let wal_path = tmp.path().join("tablet.wal");
        let wal = WriteAheadLog::new(&wal_path).unwrap();
        let first = WalEntry::RowIdDelete {
            locations: vec![(rowset_id, 0, 7)],
        };
        wal.writer()
            .write_entry(first.wal_type(), &first.serialize_data())
            .unwrap();
        let second = WalEntry::RowIdDelete {
            locations: vec![(rowset_id, 0, 9)],
        };
        wal.writer()
            .write_entry(second.wal_type(), &second.serialize_data())
            .unwrap();
        wal.flush().unwrap();

        let reopened = Tablet::open(1, tmp.path(), manager).unwrap();
        assert!(reopened.find_rowset_by_id(rowset_id).is_some());

        let dv = crate::primary_key::DeleteVector::load_from_dir(&rowset_path, 0)
            .unwrap()
            .unwrap();
        assert!(dv.is_deleted(7));
        assert!(dv.is_deleted(9));
    }

    fn wait_until_missing(path: &Path) {
        for _ in 0..50 {
            if !path.exists() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !path.exists(),
            "path still exists after waiting: {:?}",
            path
        );
    }

    #[test]
    fn test_drop_cleanup_idempotent() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("tablet_drop_cleanup");
        let schema = create_test_schema();
        let tablet = Tablet::new(99, 100, 1000, schema, &data_dir, None).unwrap();
        tablet.init().unwrap();
        tablet.save_meta().unwrap();

        Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, false).unwrap();
        Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, false).unwrap();
        wait_until_missing(&data_dir);

        // Re-run after cleanup: should remain idempotent.
        Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, false).unwrap();
    }
}
