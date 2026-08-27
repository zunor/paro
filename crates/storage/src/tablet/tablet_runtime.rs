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

use super::prepared_txn_registry::PreparedTxnRegistry;
use super::schema_adapter::TabletSchemaAdaptationPlan;
use super::shutdown_sweep;
use super::statistics::TabletStatistics;
use super::tablet_meta::{AppliedMutationMeta, SearchGenerationHeadMeta, TabletMeta};
use super::tablet_schema::{ColumnId, TabletSchemaRef};
use super::versioned_rowset_catalog::{
    RowsetCatalogCheckpointSlice, RowsetCatalogDescriptor, RowsetCatalogFlags,
    VersionedRowsetCatalog,
};
use crate::compaction::plan::types::CumulativePointAction;
use crate::compaction::publish::record::{
    CompactionPublishConflict, CompactionPublishConflictReason, CompactionPublishRecord,
    RetiredInput,
};
use crate::meta::TabletMetaManager;
use crate::metrics::storage_metrics;
use crate::primary_key::{
    primary_key_hash, DeleteVector, PrimaryIndex, RowID, RssidManager,
    PERSISTENT_INDEX_FORMAT_VERSION,
};
use crate::rowset::segment::{Segment, SegmentOptions, SegmentSharedPtr};
use crate::rowset::{
    PhysicalRowRef, Rowset, RowsetMeta, RowsetSharedPtr, RowsetState, SegmentRowId, SegmentsOverlap,
};
use crate::search::manifest::{LoadedManifest, ManifestStore};
use crate::search::SearchInlineBuilderSet;
use paro_common::durability::PrepareToken;
use paro_common::effect::{
    ArtifactNamespace, CompactionCumulativePointAction, RetiredRowsetInput,
    SearchGenerationPublication, TabletMutation, VersionSpan,
};
use paro_common::error::{self as paro_error, Result};
use paro_journal::wal::write_ahead_log::WriteAheadLog;
use paro_journal::{JournalApplyRuntime, JournalCoordinator, MutationIdentity, MutationKind};
use paro_transaction::{
    CommitTs, DatabaseId, DerivedLagLease, LayoutEpoch, LayoutEpochLease, LockAcquireError,
    LockMode, LockNamespace, LockRequest, LockResource, ReadSnapshotLease, ReadTs,
    RetentionLeaseKind, RetentionRegistry, RetentionWatermarks, ShardedLockManager, TableId, TxnId,
    TxnLockSet, MAX_TRANSACTION_ID,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

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

impl From<PhysicalRowRef> for (u64, u32, SegmentRowId) {
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
    retired_at_epoch: u64,
    rssids: Vec<u32>,
}

pub struct TabletSnapshotMaterialization {
    pub layout_epoch_snapshot: u64,
    pub schema_epoch_snapshot: Option<u64>,
    pub physical_schema_token: Option<u64>,
    pub schema_adaptation: TabletSchemaAdaptationPlan,
    pub rowsets: Vec<RowsetSharedPtr>,
    _layout_lease: LayoutEpochLease,
}

impl std::fmt::Debug for TabletSnapshotMaterialization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabletSnapshotMaterialization")
            .field("layout_epoch_snapshot", &self.layout_epoch_snapshot)
            .field("schema_epoch_snapshot", &self.schema_epoch_snapshot)
            .field("physical_schema_token", &self.physical_schema_token)
            .field("schema_adaptation", &self.schema_adaptation)
            .field("rowset_count", &self.rowsets.len())
            .finish()
    }
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

#[derive(Debug)]
pub(crate) struct PreparedSearchGenerationHeadUpdate {
    head: SearchGenerationHeadMeta,
    manifest: LoadedManifest,
}

#[derive(Debug, Default)]
pub(crate) struct SearchGenerationHeadUpdates {
    prepared: Vec<PreparedSearchGenerationHeadUpdate>,
    stale_definition_ids: HashSet<u64>,
}

impl SearchGenerationHeadUpdates {
    pub(crate) fn push(&mut self, head: SearchGenerationHeadMeta, manifest: LoadedManifest) {
        debug_assert_eq!(head.definition_id, manifest.root.definition_id);
        self.prepared
            .push(PreparedSearchGenerationHeadUpdate { head, manifest });
    }

    pub(crate) fn mark_stale(&mut self, definition_id: u64) {
        self.stale_definition_ids.insert(definition_id);
    }

    fn accept_in_memory_heads(&mut self, advanced_definition_ids: &HashSet<u64>) {
        for update in &self.prepared {
            if advanced_definition_ids.contains(&update.head.definition_id) {
                // Once the tablet's in-memory head points at this immutable
                // revision, rollback cleanup must never delete it. A later
                // metadata-save failure may leave an orphan on disk, which is
                // safe and reclaimed by recovery reachability GC. Deleting it
                // here would create a dangling head that a later save could
                // make durable.
                update.manifest.mark_revision_published();
            } else {
                self.stale_definition_ids.insert(update.head.definition_id);
            }
        }
        self.prepared
            .retain(|update| advanced_definition_ids.contains(&update.head.definition_id));
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<(SearchGenerationHeadMeta, LoadedManifest)>,
        HashSet<u64>,
    ) {
        (
            self.prepared
                .into_iter()
                .map(|update| (update.head, update.manifest))
                .collect(),
            self.stale_definition_ids,
        )
    }
}

pub(crate) trait RowsetPublishObserver: Send + Sync + std::fmt::Debug {
    fn prepare_rowset_publish(
        &self,
        _tablet_id: TabletId,
        _version: i64,
        _visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<SearchGenerationHeadUpdates> {
        Ok(SearchGenerationHeadUpdates::default())
    }

    fn rowset_published(
        &self,
        tablet_id: TabletId,
        version: i64,
        rowset: RowsetSharedPtr,
        search_updates: SearchGenerationHeadUpdates,
    );

    fn search_inline_builders_for_compaction(
        &self,
        _tablet_id: TabletId,
    ) -> SearchInlineBuilderSet {
        SearchInlineBuilderSet::default()
    }
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
    pub layout_epoch_cut: u64,
    pub rowset_catalog_slice: RowsetCatalogCheckpointSlice,
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

pub struct TabletReadGuard {
    tablet: Arc<Tablet>,
    visible_version: i64,
    snapshot_lease: Option<ReadSnapshotLease>,
}

impl TabletReadGuard {
    pub fn pin(tablet: &Arc<Tablet>, visible_version: i64) -> Result<Self> {
        let snapshot_lease = tablet.lease_read_snapshot(visible_version)?;
        tablet.register_snapshot_pin(visible_version);
        Ok(Self {
            tablet: tablet.clone(),
            visible_version,
            snapshot_lease: Some(snapshot_lease),
        })
    }

    pub fn visible_version(&self) -> i64 {
        self.visible_version
    }
}

impl std::fmt::Debug for TabletReadGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TabletReadGuard")
            .field("tablet_id", &self.tablet.tablet_id())
            .field("visible_version", &self.visible_version)
            .finish()
    }
}

impl Drop for TabletReadGuard {
    fn drop(&mut self) {
        self.snapshot_lease.take();
        self.tablet.release_snapshot_pin(self.visible_version);
        self.tablet.sweep_retired_inputs();
    }
}

#[derive(Debug, Default)]
struct TabletSnapshotPins {
    by_visible_version: Mutex<BTreeMap<i64, usize>>,
}

impl TabletSnapshotPins {
    fn pin(&self, visible_version: i64) {
        let mut pins = self
            .by_visible_version
            .lock()
            .expect("tablet snapshot pins poisoned");
        *pins.entry(visible_version).or_insert(0) += 1;
    }

    fn release(&self, visible_version: i64) {
        let Ok(mut pins) = self.by_visible_version.lock() else {
            return;
        };
        let Some(count) = pins.get_mut(&visible_version) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            pins.remove(&visible_version);
        }
    }

    fn min_visible_version(&self) -> Option<i64> {
        self.by_visible_version
            .lock()
            .ok()
            .and_then(|pins| pins.first_key_value().map(|(&version, _)| version))
    }

    fn active_count(&self) -> usize {
        self.by_visible_version
            .lock()
            .map(|pins| pins.values().copied().sum())
            .unwrap_or(0)
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

    /// Runtime rowset handles by stable rowset id. Includes live and retained
    /// retired inputs until GC barriers clear.
    rowsets_by_id: RwLock<HashMap<u64, RowsetSharedPtr>>,

    /// Authoritative logical/physical rowset history.
    rowset_catalog: RwLock<VersionedRowsetCatalog>,

    /// Prepared transactions for this tablet (DeltaWriter open/abort/commit)
    prepared_txns: PreparedTxnRegistry,

    /// Namespace used when mapping tablet-local write conflicts into lock resources.
    lock_namespace: LockNamespace,

    /// Transactional write locks for primary-key and row-id deletes/updates.
    delete_lock_manager: Arc<ShardedLockManager>,

    /// Cumulative compaction point (versions below this are base)
    cumulative_point: AtomicI64,

    /// Maximum committed version
    max_version: AtomicI64,

    /// Next rowset ID to assign
    next_rowset_id: AtomicI64,

    /// Monotonic epoch covering visible rowset/delete state.
    layout_epoch: AtomicU64,

    /// Prevents physical rowset replacement while a staged artifact embeds
    /// the current layout. Ordinary reads never acquire this gate.
    layout_maintenance_gate: super::LayoutMaintenanceGate,

    /// Outermost lock for every search-generation manifest/head transition.
    /// It is acquired before `meta_lock` and before registry definition locks,
    /// eliminating the rowset-publish (meta -> definition) versus maintenance
    /// (definition -> meta) inversion.
    search_generation_publish_lock: Mutex<()>,

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

    /// Best-effort observer for derived-state refresh after rowset publish.
    rowset_publish_observer: RwLock<Option<std::sync::Weak<dyn RowsetPublishObserver>>>,

    /// Maintenance publish ids for visible and retired rowsets.
    rowset_maintenance_ids: RwLock<HashMap<u64, u64>>,

    /// Durable mutation identities already reflected by this tablet.
    applied_mutations: RwLock<HashSet<AppliedMutationMeta>>,

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

    /// Global retention leases for logical read snapshots that touch this tablet.
    retention_registry: Arc<RetentionRegistry>,

    /// Tablet-local lightweight pin counts for retired input GC.
    snapshot_pins: TabletSnapshotPins,

    /// Retired compaction inputs kept alive until all GC barriers clear.
    retired_pending_gc: RwLock<HashMap<u64, RetiredPendingGcEntry>>,

    /// Declared ART predicate indexes that should be rebuilt for new rowsets.
    declared_art_columns: RwLock<HashSet<ColumnId>>,
}

/// Typed proof that the caller owns the outer search-generation publication
/// lock for this tablet.
pub(crate) struct SearchGenerationPublishGuard<'a> {
    tablet_id: TabletId,
    _guard: MutexGuard<'a, ()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SearchGenerationPublishOutcome {
    Advanced,
    AlreadyCurrent,
    Superseded,
    Retired,
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
        Self::create_from_meta_with_lock_manager(
            meta,
            tablet_meta_manager,
            Arc::new(ShardedLockManager::default()),
            LockNamespace::single_tenant(DatabaseId::new(0)),
        )
    }

    pub fn create_from_meta_with_lock_manager(
        meta: TabletMeta,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        delete_lock_manager: Arc<ShardedLockManager>,
        lock_namespace: LockNamespace,
    ) -> Result<Self> {
        let data_dir = PathBuf::from(meta.data_dir());
        Self::ensure_storage_layout_root(&data_dir)?;
        let cumulative_point = meta.cumulative_layer_point();
        let max_version = meta.max_version();
        let layout_epoch = meta.layout_epoch();
        let applied_lsn = meta.applied_lsn();
        let rssid_manager = RssidManager::from_entries(meta.rssid_mappings());
        let applied_mutations = meta.applied_mutations().iter().copied().collect();
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
            rowsets_by_id: RwLock::new(HashMap::new()),
            rowset_catalog: RwLock::new(VersionedRowsetCatalog::new()),
            prepared_txns: PreparedTxnRegistry::new(),
            lock_namespace,
            delete_lock_manager,
            cumulative_point: AtomicI64::new(cumulative_point),
            max_version: AtomicI64::new(max_version),
            next_rowset_id: AtomicI64::new(1),
            layout_epoch: AtomicU64::new(layout_epoch),
            layout_maintenance_gate: super::LayoutMaintenanceGate::default(),
            search_generation_publish_lock: Mutex::new(()),
            applied_lsn: AtomicU64::new(applied_lsn),
            rssid_manager,
            meta_lock: RwLock::new(()),
            checkpoint_capture_epoch: AtomicU64::new(0),
            checkpoint_publish_observer: RwLock::new(None),
            rowset_publish_observer: RwLock::new(None),
            rowset_maintenance_ids: RwLock::new(HashMap::new()),
            applied_mutations: RwLock::new(applied_mutations),
            primary_index: RwLock::new(primary_index),
            primary_index_flush_requested,
            primary_index_full: AtomicBool::new(true),
            statistics_cache: RwLock::new(None),
            statistics_dirty: AtomicBool::new(true),
            retention_registry: Arc::new(RetentionRegistry::default()),
            snapshot_pins: TabletSnapshotPins::default(),
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
        Self::new_with_lock_manager(
            tablet_id,
            table_id,
            partition_id,
            schema,
            data_dir,
            tablet_meta_manager,
            Arc::new(ShardedLockManager::default()),
            LockNamespace::single_tenant(DatabaseId::new(0)),
        )
    }

    pub fn new_with_lock_manager(
        tablet_id: TabletId,
        table_id: u64,
        partition_id: u64,
        schema: TabletSchemaRef,
        data_dir: impl Into<PathBuf>,
        tablet_meta_manager: Option<Arc<TabletMetaManager>>,
        delete_lock_manager: Arc<ShardedLockManager>,
        lock_namespace: LockNamespace,
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

        Self::create_from_meta_with_lock_manager(
            meta,
            tablet_meta_manager,
            delete_lock_manager,
            lock_namespace,
        )
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

    pub(crate) fn bind_rowset_publish_observer(
        &self,
        observer: std::sync::Weak<dyn RowsetPublishObserver>,
    ) {
        *self.rowset_publish_observer.write().unwrap() = Some(observer);
    }

    fn notify_rowset_published(
        &self,
        version: i64,
        rowset: RowsetSharedPtr,
        search_updates: SearchGenerationHeadUpdates,
    ) {
        let observer = self
            .rowset_publish_observer
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(observer) = observer {
            observer.rowset_published(self.tablet_id(), version, rowset, search_updates);
        }
    }

    fn prepare_search_rowset_publish(
        &self,
        version: i64,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> SearchGenerationHeadUpdates {
        let observer = self
            .rowset_publish_observer
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        let Some(observer) = observer else {
            return SearchGenerationHeadUpdates::default();
        };
        match observer.prepare_rowset_publish(self.tablet_id(), version, visible_rowsets) {
            Ok(updates) => updates,
            Err(error) => {
                tracing::error!(
                    tablet_id = self.tablet_id(),
                    error = %error,
                    "kept prior search generation heads after derived manifest preparation failed"
                );
                SearchGenerationHeadUpdates::default()
            }
        }
    }

    fn rowsets_with_pending_publish(
        &self,
        current_max_version: i64,
        version: i64,
        rowset: &RowsetSharedPtr,
    ) -> Result<Vec<RowsetSharedPtr>> {
        let mut rowsets = if current_max_version >= 0 {
            self.capture_consistent_rowsets(current_max_version)?
        } else {
            Vec::new()
        };
        rowset.set_version(Version::singleton(version));
        rowsets.push(rowset.clone());
        rowsets.sort_by_key(|rowset| rowset.start_version());
        Ok(rowsets)
    }

    fn apply_search_generation_heads_locked(
        &self,
        updates: &SearchGenerationHeadUpdates,
    ) -> HashSet<u64> {
        if updates.prepared.is_empty() {
            return HashSet::new();
        }
        let mut advanced_definition_ids = HashSet::new();
        let mut meta = self.meta.write().unwrap_or_else(|poisoned| {
            tracing::error!(
                tablet_id = self.tablet_id(),
                "recovering poisoned tablet meta while applying derived search heads"
            );
            poisoned.into_inner()
        });
        for head in updates.prepared.iter().map(|update| update.head.clone()) {
            let definition_id = head.definition_id;
            if let Err(error) = meta.advance_search_generation_head(head) {
                // Search metadata is derived from the just-published base
                // rowsets. A conflicting index revision must make that index
                // unavailable, never roll back or poison an already-durable
                // base-table write.
                tracing::error!(
                    tablet_id = self.tablet_id(),
                    definition_id,
                    error = %error,
                    "kept prior search generation head after inconsistent derived revision"
                );
            } else {
                advanced_definition_ids.insert(definition_id);
            }
        }
        advanced_definition_ids
    }

    pub(crate) fn search_generation_head(
        &self,
        definition_id: u64,
    ) -> Option<SearchGenerationHeadMeta> {
        self.meta
            .read()
            .unwrap()
            .search_generation_heads()
            .iter()
            .find(|head| head.definition_id == definition_id)
            .cloned()
    }

    pub(crate) fn search_generation_heads(&self) -> Vec<SearchGenerationHeadMeta> {
        self.meta.read().unwrap().search_generation_heads().to_vec()
    }

    pub(crate) fn is_search_definition_retired(&self, definition_id: u64) -> bool {
        self.meta
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_search_definition_retired(definition_id)
    }

    pub(crate) fn remove_search_generation_heads_guarded(
        &self,
        definition_ids: &[u64],
        guard: &SearchGenerationPublishGuard<'_>,
    ) -> Result<()> {
        if guard.tablet_id != self.tablet_id() {
            return Err(paro_error::internal(
                "search generation publication guard belongs to another tablet",
            ));
        }
        if definition_ids.is_empty() {
            return Ok(());
        }

        let _meta_guard = self
            .meta_lock
            .write()
            .map_err(|_| paro_error::internal("tablet meta lock poisoned"))?;
        let removed_heads = {
            let mut meta = self
                .meta
                .write()
                .map_err(|_| paro_error::internal("tablet meta state poisoned"))?;
            let removed = definition_ids
                .iter()
                .filter_map(|definition_id| meta.remove_search_generation_head(*definition_id))
                .collect::<Vec<_>>();
            if removed.is_empty() {
                return Ok(());
            }
            removed
        };
        if let Err(error) = self.save_meta() {
            let mut meta = self.meta.write().map_err(|_| {
                paro_error::internal("tablet meta state poisoned after save failure")
            })?;
            for head in removed_heads {
                meta.restore_search_generation_head(head.definition_id, Some(head));
            }
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn search_inline_builders_for_compaction(&self) -> SearchInlineBuilderSet {
        self.rowset_publish_observer
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .map(|observer| observer.search_inline_builders_for_compaction(self.tablet_id()))
            .unwrap_or_default()
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

    pub fn layout_epoch(&self) -> u64 {
        self.layout_epoch.load(Ordering::Acquire)
    }

    pub fn acquire_stable_layout_lease(
        &self,
        owner_id: u64,
        should_stop: impl Fn() -> bool,
    ) -> Result<super::LayoutMaintenanceLease> {
        self.layout_maintenance_gate
            .acquire_exclusive(owner_id, should_stop)
    }

    pub(crate) fn try_acquire_compaction_layout_lease(
        &self,
    ) -> Result<Option<super::LayoutMaintenanceLease>> {
        self.layout_maintenance_gate.try_acquire_shared()
    }

    pub(crate) fn acquire_storage_publish_layout_lease(
        &self,
        should_stop: impl Fn() -> bool,
    ) -> Result<super::LayoutMaintenanceLease> {
        self.layout_maintenance_gate.acquire_shared(should_stop)
    }

    pub(crate) fn acquire_search_generation_publish_guard(
        &self,
    ) -> Result<SearchGenerationPublishGuard<'_>> {
        // Durable tablet apply is not cancellable after WAL append. Keep this
        // critical section short and recover poison just like the layout gate:
        // the mutex protects ordering only and contains no partially valid
        // payload whose invariants could be lost by a panic.
        let guard = match self.search_generation_publish_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    tablet_id = self.tablet_id(),
                    "recovering poisoned search-generation publication lock"
                );
                poisoned.into_inner()
            }
        };
        Ok(SearchGenerationPublishGuard {
            tablet_id: self.tablet_id(),
            _guard: guard,
        })
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn.load(Ordering::Acquire)
    }

    pub fn has_applied_mutation_identity(&self, identity: MutationIdentity) -> bool {
        if matches!(
            identity.mutation_kind,
            MutationKind::PublishSearchGeneration | MutationKind::RetireSearchGeneration
        ) {
            return false;
        }
        let applied = AppliedMutationMeta::from_journal(identity);
        self.applied_mutations.read().unwrap().contains(&applied)
    }

    pub fn note_applied_mutation_identity(&self, identity: MutationIdentity) -> Result<bool> {
        if matches!(
            identity.mutation_kind,
            MutationKind::PublishSearchGeneration | MutationKind::RetireSearchGeneration
        ) {
            // Search publication and retirement are already represented by
            // the durable head/tombstone state. Replaying them through that
            // contract is idempotent and validates the referenced artifact;
            // retaining a second skip-hint history would add an extra meta
            // fsync to every revision and could turn failure to persist a
            // disposable hint into a fatal apply-runtime error.
            return Ok(false);
        }
        let applied = AppliedMutationMeta::from_journal(identity);
        {
            let mut mutations = self.applied_mutations.write().unwrap();
            if mutations.contains(&applied) {
                return Ok(false);
            }
            mutations.insert(applied);
        }
        self.save_meta()?;
        Ok(true)
    }

    pub fn schema_epoch(&self) -> Option<u64> {
        self.schema().map(|schema| schema.schema_version() as u64)
    }

    fn rowset_catalog_descriptor(&self, rowset: &Rowset) -> RowsetCatalogDescriptor {
        let meta = rowset.rowset_meta();
        let schema_version = self
            .schema()
            .map(|schema| schema.schema_version())
            .unwrap_or(0);
        let physical_schema_token = self.rowset_physical_schema_token_from_meta(&meta);
        let flags = if meta.is_compaction_output() {
            RowsetCatalogFlags::COMPACTION_OUTPUT
        } else {
            RowsetCatalogFlags::empty()
        };
        RowsetCatalogDescriptor {
            rowset_id: rowset.rowset_id(),
            version: rowset.version(),
            schema_version,
            physical_schema_token,
            delete_vector_catalog_token: 0,
            artifact_id: rowset.rowset_id(),
            flags,
            cold_meta_id: 0,
        }
    }

    fn rowset_physical_schema_token_from_meta(&self, meta: &RowsetMeta) -> u64 {
        if meta.schema_hash() == 0 {
            self.schema_hash() as u64
        } else {
            meta.schema_hash() as u64
        }
    }

    fn rowset_physical_schema_token(&self, rowset: &Rowset) -> u64 {
        self.rowset_physical_schema_token_from_meta(&rowset.rowset_meta())
    }

    fn compaction_schema_tokens_match(&self, inputs: &[RowsetSharedPtr], output: &Rowset) -> bool {
        let output_schema_token = self.rowset_physical_schema_token(output);
        inputs
            .iter()
            .all(|input| self.rowset_physical_schema_token(input.as_ref()) == output_schema_token)
    }

    fn next_layout_epoch(&self) -> u64 {
        self.note_checkpoint_capture_mutation();
        self.layout_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn prepare_token(&self, visible_version: i64) -> PrepareToken {
        PrepareToken {
            visible_version,
            layout_epoch: self.layout_epoch(),
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
        let current_epoch = self.layout_epoch();
        if current_epoch != token.layout_epoch {
            return Err(paro_error::serialization_failure(format!(
                "tablet {} prepare token stale: layout_epoch {} -> {}",
                self.tablet_id(),
                token.layout_epoch,
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

    /// Acquire transactional primary-key write locks for `txn_id`.
    pub fn acquire_primary_key_write_locks(
        &self,
        txn_id: TxnId,
        keys: &[Vec<u8>],
    ) -> Result<TxnLockSet> {
        let requests = keys.iter().map(|key| {
            LockRequest::new(
                LockResource::primary_key(
                    self.lock_namespace,
                    TableId::new(self.table_id()),
                    self.tablet_id(),
                    primary_key_hash(key),
                ),
                LockMode::X,
            )
        });
        self.delete_lock_manager
            .lock_many(txn_id, requests)
            .map_err(|err| self.primary_delete_lock_error(txn_id, err))
    }

    /// Acquire transactional primary-key delete/update locks for `txn_id`.
    pub fn acquire_primary_delete_locks(
        &self,
        txn_id: TxnId,
        keys: &[Vec<u8>],
    ) -> Result<TxnLockSet> {
        self.acquire_primary_key_write_locks(txn_id, keys)
    }

    pub(crate) fn has_pending_delete_locks(&self) -> bool {
        let tablet = LockResource::Tablet {
            namespace: self.lock_namespace,
            table_id: TableId::new(self.table_id()),
            tablet_id: self.tablet_id(),
        };
        self.delete_lock_manager.has_lock_conflicting_with(&tablet)
    }

    /// Acquire transactional row-id delete/update locks for `txn_id`.
    pub fn acquire_row_id_delete_locks(
        &self,
        txn_id: TxnId,
        locations: &[PhysicalRowRef],
    ) -> Result<TxnLockSet> {
        let requests = locations.iter().map(|location| {
            LockRequest::new(
                LockResource::row_id(
                    self.lock_namespace,
                    TableId::new(self.table_id()),
                    self.tablet_id(),
                    location.rowset_id,
                    location.segment_id,
                    location.row_offset.get(),
                ),
                LockMode::X,
            )
        });
        self.delete_lock_manager
            .lock_many(txn_id, requests)
            .map_err(|err| self.row_id_delete_lock_error(txn_id, locations.first().copied(), err))
    }

    fn primary_delete_lock_error(
        &self,
        txn_id: TxnId,
        err: LockAcquireError,
    ) -> paro_error::ParoError {
        match err {
            LockAcquireError::WouldWait { blockers } => paro_error::serialization_failure(format!(
                "write-write conflict on tablet {} primary key delete (txn {} blocked by {:?})",
                self.tablet_id(),
                txn_id,
                blockers
            )),
            LockAcquireError::WouldWound { victims } => paro_error::serialization_failure(format!(
                "write-write conflict on tablet {} primary key delete (txn {} would wound {:?})",
                self.tablet_id(),
                txn_id,
                victims
            )),
            LockAcquireError::WouldWoundAndWait { victims, blockers } => {
                paro_error::serialization_failure(format!(
                    "write-write conflict on tablet {} primary key delete (txn {} would wound {:?} and wait for {:?})",
                    self.tablet_id(),
                    txn_id,
                    victims,
                    blockers
                ))
            }
        }
    }

    fn row_id_delete_lock_error(
        &self,
        txn_id: TxnId,
        location: Option<PhysicalRowRef>,
        err: LockAcquireError,
    ) -> paro_error::ParoError {
        let location = location
            .map(|location| {
                format!(
                    " at rowset={}, segment={}, row={}",
                    location.rowset_id, location.segment_id, location.row_offset
                )
            })
            .unwrap_or_default();
        match err {
            LockAcquireError::WouldWait { blockers } => paro_error::serialization_failure(format!(
                "write-write conflict on tablet {} row-id delete (txn {} blocked by {:?}){}",
                self.tablet_id(),
                txn_id,
                blockers,
                location
            )),
            LockAcquireError::WouldWound { victims } => paro_error::serialization_failure(format!(
                "write-write conflict on tablet {} row-id delete (txn {} would wound {:?}){}",
                self.tablet_id(),
                txn_id,
                victims,
                location
            )),
            LockAcquireError::WouldWoundAndWait { victims, blockers } => {
                paro_error::serialization_failure(format!(
                    "write-write conflict on tablet {} row-id delete (txn {} would wound {:?} and wait for {:?}){}",
                    self.tablet_id(),
                    txn_id,
                    victims,
                    blockers,
                    location
                ))
            }
        }
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
        Self::validate_version_graph_entries(&entries)?;
        self.rowset_catalog.read().unwrap().validate_latest()?;
        self.debug_assert_catalog_live_map_parity();
        Ok(())
    }

    /// Find a live rowset by rowset_id across committed and incremental maps.
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
        None
    }

    pub(crate) fn find_retained_rowset_by_id(
        &self,
        rowset_id: crate::rowset::RowsetId,
    ) -> Option<RowsetSharedPtr> {
        if let Some(rowset) = self.rowsets_by_id.read().ok()?.get(&rowset_id).cloned() {
            return Some(rowset);
        }
        if let Some(rowset) = self.find_rowset_by_id(rowset_id) {
            return Some(rowset);
        }
        self.retired_pending_gc
            .read()
            .ok()?
            .get(&rowset_id)
            .map(|entry| entry.rowset.clone())
    }

    fn debug_assert_catalog_live_map_parity(&self) {
        let live = self
            .rs_version_map
            .read()
            .unwrap()
            .values()
            .map(|rowset| (rowset.rowset_id(), rowset.version()))
            .collect::<Vec<_>>();
        self.rowset_catalog
            .read()
            .unwrap()
            .assert_live_map_parity(live);
    }

    fn lease_read_snapshot(&self, visible_version: i64) -> Result<ReadSnapshotLease> {
        let read_ts = visible_version.max(0) as u64;
        self.retention_registry
            .lease_read_snapshot(ReadTs::new(read_ts))
            .map_err(|e| paro_error::internal(format!("failed to lease read snapshot: {e}")))
    }

    fn lease_layout_epoch(&self, layout_epoch: u64) -> Result<LayoutEpochLease> {
        self.retention_registry
            .lease_layout_epoch(LayoutEpoch::new(layout_epoch))
            .map_err(|e| paro_error::internal(format!("failed to lease layout epoch: {e}")))
    }

    pub(crate) fn lease_derived_lag_range(
        &self,
        indexed_through_ts: u64,
        target_ts: u64,
    ) -> Result<DerivedLagLease> {
        self.retention_registry
            .lease_derived_lag_range(CommitTs::new(indexed_through_ts), CommitTs::new(target_ts))
            .map_err(|e| paro_error::internal(format!("failed to lease derived lag: {e}")))
    }

    fn register_snapshot_pin(&self, visible_version: i64) {
        self.snapshot_pins.pin(visible_version);
    }

    fn release_snapshot_pin(&self, visible_version: i64) {
        self.snapshot_pins.release(visible_version);
    }

    pub fn min_active_visible_version(&self) -> Option<i64> {
        self.snapshot_pins.min_visible_version()
    }

    pub fn active_snapshot_pin_count(&self) -> usize {
        self.snapshot_pins.active_count()
    }

    pub fn read_snapshot_lease_count(&self) -> u64 {
        self.retention_registry
            .watermarks()
            .lease_count(RetentionLeaseKind::ReadSnapshot)
    }

    pub fn layout_epoch_lease_count(&self) -> u64 {
        self.retention_registry
            .watermarks()
            .lease_count(RetentionLeaseKind::LayoutEpoch)
    }

    pub fn derived_lag_lease_count(&self) -> u64 {
        self.retention_registry
            .watermarks()
            .lease_count(RetentionLeaseKind::DerivedLag)
    }

    pub fn retired_pending_gc_statuses(&self) -> Vec<RetiredPendingGcStatus> {
        let min_active_visible_version = self.min_active_visible_version();
        let mut statuses = self
            .retention_registry
            .with_confirmed_watermarks(|watermarks| {
                let retired = self.retired_pending_gc.read().unwrap();
                retired
                    .iter()
                    .map(|(&rowset_id, entry)| RetiredPendingGcStatus {
                        rowset_id,
                        version: entry.version,
                        barrier: self.retired_gc_barrier(
                            entry,
                            min_active_visible_version,
                            watermarks,
                        ),
                        refs_by_reader: entry.rowset.ref_count(),
                        rssid_count: entry.rssids.len(),
                    })
                    .collect::<Vec<_>>()
            });
        statuses.sort_by_key(|status| status.rowset_id);
        statuses
    }

    fn retired_gc_barrier(
        &self,
        entry: &RetiredPendingGcEntry,
        min_active_visible_version: Option<i64>,
        watermarks: RetentionWatermarks,
    ) -> RetiredGcBarrier {
        let oldest_read_ts = watermarks.oldest_read_ts.into_raw();
        let read_floor_blocks = oldest_read_ts != MAX_TRANSACTION_ID
            && entry.version.end >= 0
            && (entry.version.end as u64) >= oldest_read_ts;
        if read_floor_blocks
            || min_active_visible_version.is_some_and(|version| version <= entry.version.end)
        {
            return RetiredGcBarrier::PendingSnapshotBarrier;
        }
        if let Some(oldest_layout_epoch) = watermarks.oldest_layout_epoch {
            if entry.retired_at_epoch >= oldest_layout_epoch.into_raw() {
                return RetiredGcBarrier::PendingSnapshotBarrier;
            }
        }
        if entry.rowset.ref_count() > 0 {
            return RetiredGcBarrier::PendingRuntimeHandles;
        }
        RetiredGcBarrier::Eligible
    }

    fn register_retired_inputs(
        &self,
        inputs: &[RowsetSharedPtr],
        retired_inputs: &[RetiredInput],
        retired_at_epoch: u64,
    ) {
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
                    retired_at_epoch,
                    rssids: meta.map(|input| input.rssids.clone()).unwrap_or_default(),
                },
            );
        }
    }

    fn sweep_retired_inputs(&self) {
        // Read-guard release is a hot path. This preflight is deliberately
        // advisory: a concurrent compaction may publish a retired input after
        // the check, in which case a later guard release or explicit sweep will
        // collect it. The authoritative eligibility check remains below under
        // the retention lifecycle barrier.
        if self.retired_pending_gc.read().unwrap().is_empty() {
            return;
        }

        let min_active_visible_version = self.min_active_visible_version();
        let (cleanup_paths, removed_rssids) =
            self.retention_registry
                .with_confirmed_watermarks(|watermarks| {
                    let mut retired = self.retired_pending_gc.write().unwrap();
                    let removable: Vec<u64> = retired
                        .iter()
                        .filter_map(|(&rowset_id, entry)| {
                            (self.retired_gc_barrier(entry, min_active_visible_version, watermarks)
                                == RetiredGcBarrier::Eligible)
                                .then_some(rowset_id)
                        })
                        .collect();
                    if removable.is_empty() {
                        return (Vec::new(), Vec::new());
                    }

                    let mut cleanup_paths = Vec::with_capacity(removable.len());
                    let mut removed_rssids = Vec::new();
                    let mut maintenance_ids = self.rowset_maintenance_ids.write().unwrap();
                    let mut rowsets_by_id = self.rowsets_by_id.write().unwrap();
                    for rowset_id in removable {
                        if let Some(entry) = retired.remove(&rowset_id) {
                            cleanup_paths.push(entry.rowset.rowset_path().to_path_buf());
                            removed_rssids.extend(entry.rssids.iter().copied());
                        }
                        maintenance_ids.remove(&rowset_id);
                        rowsets_by_id.remove(&rowset_id);
                    }
                    (cleanup_paths, removed_rssids)
                });
        if cleanup_paths.is_empty() {
            return;
        }

        self.rssid_manager.remove_many(&removed_rssids);
        let _ = self.save_meta();
        for path in cleanup_paths {
            crate::compaction::cleanup::enqueue_cleanup(path);
        }
    }

    // ==================== State Management ====================

    /// Initialize the tablet (transition to Running state and load rowsets)
    pub fn init(&self) -> Result<()> {
        let mut retained_loaded_rowsets = Vec::new();
        let persisted_catalog_slice;
        let persisted_maintenance_ids;

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
            persisted_catalog_slice = meta_guard.rowset_catalog_slice().cloned();
            persisted_maintenance_ids = meta_guard.rowset_maintenance_ids().to_vec();

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

            for rs_meta in meta_guard.retained_rowset_metas() {
                max_rowset_id = max_rowset_id.max(rs_meta.rowset_id());
                let rowset_path =
                    self.resolve_loaded_rowset_path(rs_meta.rowset_id(), rs_meta.rowset_path());
                let rowset =
                    crate::rowset::Rowset::create(schema.clone(), rs_meta.clone(), rowset_path)?;
                retained_loaded_rowsets.push(Arc::new(rowset));
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
        let all_loaded_rowsets: Vec<_> = loaded_rowsets
            .iter()
            .cloned()
            .chain(retained_loaded_rowsets.iter().cloned())
            .collect();
        {
            let mut rowsets_by_id = self.rowsets_by_id.write().unwrap();
            rowsets_by_id.clear();
            for rowset in &all_loaded_rowsets {
                rowsets_by_id.insert(rowset.rowset_id(), rowset.clone());
            }
        }
        {
            let catalog = if let Some(slice) = persisted_catalog_slice {
                let handles = self.rowsets_by_id.read().unwrap();
                for entry in &slice.entries {
                    if !handles.contains_key(&entry.rowset_id) {
                        return Err(paro_error::internal(format!(
                            "checkpoint catalog entry references missing rowset {} on tablet {}",
                            entry.rowset_id,
                            self.tablet_id()
                        )));
                    }
                }
                VersionedRowsetCatalog::rebuild_from_checkpoint(slice)?
            } else {
                let descriptors = loaded_rowsets
                    .iter()
                    .map(|rowset| self.rowset_catalog_descriptor(rowset.as_ref()))
                    .collect::<Vec<_>>();
                VersionedRowsetCatalog::rebuild_from_live(
                    descriptors,
                    self.layout_epoch(),
                    self.max_version(),
                )?
            };
            *self.rowset_catalog.write().unwrap() = catalog;
            self.debug_assert_catalog_live_map_parity();
        }
        {
            let known_rowset_ids: HashSet<_> = all_loaded_rowsets
                .iter()
                .map(|rowset| rowset.rowset_id())
                .collect();
            let mut maintenance_ids = self.rowset_maintenance_ids.write().unwrap();
            maintenance_ids.clear();
            for entry in persisted_maintenance_ids {
                if known_rowset_ids.contains(&entry.rowset_id) {
                    maintenance_ids.insert(entry.rowset_id, entry.maintenance_id);
                }
            }
            for rowset in &all_loaded_rowsets {
                maintenance_ids.entry(rowset.rowset_id()).or_insert(0);
            }
        }
        if !retained_loaded_rowsets.is_empty() {
            let catalog = self.rowset_catalog.read().unwrap();
            let rssid_entries = self.rssid_manager.snapshot_entries();
            let mut retired = self.retired_pending_gc.write().unwrap();
            retired.clear();
            for rowset in &retained_loaded_rowsets {
                if let Some(entry) = catalog.entry_for_rowset_id(rowset.rowset_id()) {
                    if let Some(retired_at_epoch) = entry.retired_at_epoch {
                        let rssids = rssid_entries
                            .iter()
                            .filter(|rssid| rssid.rowset_id == rowset.rowset_id())
                            .map(|rssid| rssid.rssid)
                            .collect();
                        retired.insert(
                            rowset.rowset_id(),
                            RetiredPendingGcEntry {
                                rowset: rowset.clone(),
                                version: entry.version,
                                retired_at_epoch,
                                rssids,
                            },
                        );
                    }
                }
            }
        }
        for rowset in all_loaded_rowsets {
            self.ensure_rowset_rssids(&rowset);
        }

        // Rebuild primary index from persistent metadata (best-effort).
        let rebuilt_persistent_index = self.rebuild_primary_index_from_persistent()?;
        crate::compaction::cleanup::reconcile_recovery_state(self);
        if rebuilt_persistent_index
            || self.meta.read().unwrap().row_id_format_version() != CURRENT_ROW_ID_FORMAT_VERSION
        {
            let mut meta = self.meta.write().unwrap();
            self.sync_runtime_meta_fields(&mut meta)?;
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
        self.register_rowset_locked(rowset)
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

    fn register_rowset_locked(&self, rowset: RowsetSharedPtr) -> Result<()> {
        let version = rowset.version();
        let rowset_id = rowset.rowset_id();
        let layout_epoch = self.next_layout_epoch();

        self.rowsets_by_id
            .write()
            .unwrap()
            .insert(rowset_id, rowset.clone());
        {
            let descriptor = self.rowset_catalog_descriptor(rowset.as_ref());
            if let Err(err) = self.rowset_catalog.write().unwrap().publish_rowset(
                descriptor,
                layout_epoch,
                version.end,
            ) {
                self.rowsets_by_id.write().unwrap().remove(&rowset_id);
                return Err(err);
            }
        }

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
        self.debug_assert_catalog_live_map_parity();
        Ok(())
    }

    /// Commit a rowset with the given version.
    ///
    /// The database journal is the durable truth for committed rowsets; tablet
    /// metadata is only updated in memory and persisted via `save_meta`.
    pub fn rowset_commit(&self, version: i64, rowset: RowsetSharedPtr) -> Result<()> {
        self.ensure_not_shutdown("commit rowset")?;
        let search_updates = {
            let _search_publish = self.acquire_search_generation_publish_guard()?;
            let _lock = self.meta_lock.write().unwrap();
            self.rowset_commit_locked(version, rowset.clone())?
        };
        if let Some(search_updates) = search_updates {
            self.notify_rowset_published(version, rowset, search_updates);
        }
        Ok(())
    }

    /// Commit a rowset using the next available version.
    pub fn rowset_commit_auto(&self, rowset: RowsetSharedPtr) -> Result<i64> {
        self.ensure_not_shutdown("commit rowset")?;
        let (version, search_updates) = {
            let _search_publish = self.acquire_search_generation_publish_guard()?;
            let _lock = self.meta_lock.write().unwrap();
            let next_version = self.max_version.load(Ordering::Acquire) + 1;
            let search_updates = self.rowset_commit_locked(next_version, rowset.clone())?;
            (next_version, search_updates)
        };
        if let Some(search_updates) = search_updates {
            self.notify_rowset_published(version, rowset, search_updates);
        }
        Ok(version)
    }

    fn rowset_commit_locked(
        &self,
        version: i64,
        rowset: RowsetSharedPtr,
    ) -> Result<Option<SearchGenerationHeadUpdates>> {
        let current_max = self.max_version.load(Ordering::Acquire);

        if version <= current_max {
            // Already committed (idempotent)
            return Ok(None);
        }

        let visible_rowsets = self.rowsets_with_pending_publish(current_max, version, &rowset)?;
        self.validate_rowset_registration_locked(&rowset)?;
        let mut search_heads = self.prepare_search_rowset_publish(version, &visible_rowsets);
        rowset.make_visible()?;
        self.register_rowset_locked(rowset)?;
        let advanced_search_heads = self.apply_search_generation_heads_locked(&search_heads);
        search_heads.accept_in_memory_heads(&advanced_search_heads);
        self.save_meta()?;

        self.validate_version_graph()?;
        Ok(Some(search_heads))
    }

    /// Publish a PRIMARY_KEYS rowset and staged index updates atomically.
    pub fn publish_rowset_with_index(
        &self,
        version: i64,
        rowset: RowsetSharedPtr,
        update: PrimaryIndexUpdate,
    ) -> Result<()> {
        self.ensure_not_shutdown("publish rowset with primary index")?;
        let search_updates = {
            let _search_publish = self.acquire_search_generation_publish_guard()?;
            let _lock = self.meta_lock.write().unwrap();
            self.publish_rowset_with_index_locked(version, rowset.clone(), update)?
        };
        if let Some(search_updates) = search_updates {
            self.notify_rowset_published(version, rowset, search_updates);
        }
        Ok(())
    }

    /// Publish a PRIMARY_KEYS rowset using the next available version.
    pub fn publish_rowset_with_index_auto(
        &self,
        rowset: RowsetSharedPtr,
        update: PrimaryIndexUpdate,
    ) -> Result<i64> {
        self.ensure_not_shutdown("publish rowset with primary index")?;
        let (version, search_updates) = {
            let _search_publish = self.acquire_search_generation_publish_guard()?;
            let _lock = self.meta_lock.write().unwrap();
            let version = self.max_version.load(Ordering::Acquire) + 1;
            let search_updates =
                self.publish_rowset_with_index_locked(version, rowset.clone(), update)?;
            (version, search_updates)
        };
        if let Some(search_updates) = search_updates {
            self.notify_rowset_published(version, rowset, search_updates);
        }
        Ok(version)
    }

    fn publish_rowset_with_index_locked(
        &self,
        version: i64,
        rowset: RowsetSharedPtr,
        update: PrimaryIndexUpdate,
    ) -> Result<Option<SearchGenerationHeadUpdates>> {
        let current_max = self.max_version.load(Ordering::Acquire);

        if version <= current_max {
            return Ok(None);
        }

        let visible_rowsets = self.rowsets_with_pending_publish(current_max, version, &rowset)?;
        self.ensure_rowset_rssids(&rowset);
        self.align_next_rowset_id(rowset.rowset_id());
        self.validate_rowset_registration_locked(&rowset)?;
        let prepared = self.prepare_primary_index_publish(&rowset, update)?;
        let mut search_heads = self.prepare_search_rowset_publish(version, &visible_rowsets);
        self.apply_prepared_primary_index_publish(version, &rowset, prepared)?;
        rowset.make_visible()?;
        self.register_rowset_locked(rowset)?;
        self.reconcile_primary_index_row_count()?;
        self.maybe_flush_primary_index()?;
        let advanced_search_heads = self.apply_search_generation_heads_locked(&search_heads);
        search_heads.accept_in_memory_heads(&advanced_search_heads);
        self.save_meta()?;
        self.validate_version_graph()?;
        Ok(Some(search_heads))
    }

    /// Apply row-id deletes for journal replay and persist delete vectors.
    /// Existing segment delete vectors are merged so repeated delivery of the
    /// same committed mutation remains idempotent.
    pub(crate) fn apply_row_id_delete_locations_idempotent_at_version(
        &self,
        locations: &[(u64, u32, u32)],
        delete_version: i64,
    ) -> Result<()> {
        let physical_locations = locations
            .iter()
            .map(|&(rowset_id, segment_id, row_offset)| {
                PhysicalRowRef::new(rowset_id, segment_id, SegmentRowId::from_raw(row_offset))
            })
            .collect::<Vec<_>>();
        self.apply_row_id_delete_refs_internal(&physical_locations, delete_version, true, true)
    }

    pub(crate) fn apply_row_id_delete_refs(
        &self,
        locations: &[PhysicalRowRef],
        commit_ts: CommitTs,
    ) -> Result<()> {
        let version = i64::try_from(commit_ts.into_raw())
            .map_err(|_| paro_error::invalid_input("commit_ts exceeds i64"))?;
        self.apply_row_id_delete_refs_at_version(locations, version)
    }

    pub(crate) fn apply_row_id_delete_refs_at_version(
        &self,
        locations: &[PhysicalRowRef],
        version: i64,
    ) -> Result<()> {
        self.apply_row_id_delete_refs_internal(locations, version, false, true)
    }

    pub(crate) fn apply_row_id_delete_refs_at_version_without_publish_advance(
        &self,
        locations: &[PhysicalRowRef],
        version: i64,
    ) -> Result<()> {
        self.apply_row_id_delete_refs_internal(locations, version, false, false)
    }

    fn apply_row_id_delete_refs_internal(
        &self,
        locations: &[PhysicalRowRef],
        version: i64,
        ignore_already_deleted: bool,
        advance_publish_version: bool,
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

        self.persist_delete_vectors_with_publish_advance(
            version,
            pending,
            advance_publish_version,
        )?;
        Ok(())
    }

    pub(super) fn persist_delete_vectors(
        &self,
        version: i64,
        vectors: HashMap<(u64, u32), DeleteVector>,
    ) -> Result<()> {
        self.persist_delete_vectors_with_publish_advance(version, vectors, true)
    }

    pub(super) fn persist_delete_vectors_for_rowset_publish(
        &self,
        version: i64,
        rowset: &RowsetSharedPtr,
        mut vectors: HashMap<(u64, u32), DeleteVector>,
    ) -> Result<()> {
        let rowset_id = rowset.rowset_id();
        let mut rowset_vectors = HashMap::new();
        let mut committed_vectors = HashMap::new();
        for (key @ (candidate_rowset_id, _), delete_vector) in vectors.drain() {
            if candidate_rowset_id == rowset_id {
                rowset_vectors.insert(key, delete_vector);
            } else {
                committed_vectors.insert(key, delete_vector);
            }
        }

        let mut had_rowset_updates = false;
        for ((_, segment_id), delete_vector) in rowset_vectors {
            let deletes: Vec<SegmentRowId> = delete_vector.iter().collect();
            if deletes.is_empty() {
                continue;
            }
            let mut chain =
                DeleteVector::load_versioned_from_dir(rowset.rowset_path(), segment_id)?;
            chain.add_dels_as_new_version(&deletes, version);
            let path = chain.save_to_dir(rowset.rowset_path(), segment_id)?;
            rowset.invalidate_delete_vector_cache(segment_id);
            if !path.exists() {
                return Err(paro_error::io_error("delete vector not persisted"));
            }
            had_rowset_updates = true;
        }
        if had_rowset_updates {
            self.refresh_rowset_delete_stats(rowset, version)?;
        }

        self.persist_delete_vectors(version, committed_vectors)
    }

    pub(super) fn persist_delete_vectors_with_publish_advance(
        &self,
        version: i64,
        mut vectors: HashMap<(u64, u32), DeleteVector>,
        advance_publish_version: bool,
    ) -> Result<()> {
        let mut updated_rowsets = HashSet::new();
        for ((rs_id, seg_id), dv) in vectors.drain() {
            if let Some(rowset) = self.find_rowset_by_id(rs_id) {
                let mut chain =
                    DeleteVector::load_versioned_from_dir(rowset.rowset_path(), seg_id)?;
                let deletes: Vec<SegmentRowId> = dv.iter().collect();
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
        if had_updates && advance_publish_version {
            let current_max = self.max_version.load(Ordering::Acquire);
            if version > current_max {
                self.max_version.store(version, Ordering::Release);
            }
        }
        if had_updates {
            self.invalidate_statistics();
            let layout_epoch = self.next_layout_epoch();
            self.rowset_catalog
                .write()
                .unwrap()
                .publish_delete_vector(layout_epoch, version)?;
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
        rowset.load()?;
        let mut out = Vec::with_capacity(rowset.num_rows() as usize);
        for seg in rowset.segments() {
            let num_rows = seg.num_rows() as u32;
            for row_id in 0..num_rows {
                out.push(PhysicalRowRef::new(
                    rowset.rowset_id(),
                    seg.segment_id(),
                    SegmentRowId::from_raw(row_id),
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

        if !self.compaction_schema_tokens_match(inputs, output) {
            return Err(CompactionPublishConflict {
                tablet_id: self.tablet_id(),
                plan_id: record.plan_id,
                job_id: record.job_id,
                reason: CompactionPublishConflictReason::SchemaEpochChanged,
            }
            .into_paro_error());
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

        let input_rowset_ids = inputs
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect::<Vec<_>>();
        self.rowset_catalog
            .read()
            .unwrap()
            .validate_compaction_publish(&input_rowset_ids, output.version())
            .map_err(|_| {
                CompactionPublishConflict {
                    tablet_id: self.tablet_id(),
                    plan_id: record.plan_id,
                    job_id: record.job_id,
                    reason: CompactionPublishConflictReason::VersionOverlap,
                }
                .into_paro_error()
            })?;

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
        let input_rowset_ids = inputs
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect::<Vec<_>>();
        let output_descriptor = self.rowset_catalog_descriptor(output.as_ref());

        if !self.compaction_schema_tokens_match(inputs, output.as_ref()) {
            return Err(paro_error::serialization_failure(format!(
                "compaction publish conflict on tablet {}: {:?}",
                self.tablet_id(),
                CompactionPublishConflictReason::SchemaEpochChanged
            )));
        }

        self.rowset_catalog
            .read()
            .unwrap()
            .validate_compaction_publish(&input_rowset_ids, version)?;

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
        self.rowsets_by_id
            .write()
            .unwrap()
            .insert(output.rowset_id(), output.clone());

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
        for rs in &to_delete {
            let _ = rs.mark_deleting();
        }

        let layout_epoch = self.next_layout_epoch();
        self.rowset_catalog.write().unwrap().publish_compaction(
            &input_rowset_ids,
            output_descriptor,
            layout_epoch,
            self.max_version(),
        )?;
        self.register_retired_inputs(&to_delete, retired_inputs, layout_epoch);

        self.validate_version_graph()?;
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
        self.capture_consistent_rowsets_at_layout(visible_version, self.layout_epoch())
    }

    pub fn capture_consistent_rowsets_at_layout(
        &self,
        visible_version: i64,
        layout_epoch: u64,
    ) -> Result<Vec<RowsetSharedPtr>> {
        let cut = self
            .rowset_catalog
            .read()
            .unwrap()
            .capture_entry_ids(visible_version, layout_epoch)?;
        let handles = self.rowsets_by_id.read().unwrap();
        let mut result = Vec::with_capacity(cut.rowset_ids.len());
        for rowset_id in cut.rowset_ids {
            let rowset = handles.get(&rowset_id).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "catalog cut references missing rowset handle {rowset_id}"
                ))
            })?;
            result.push(rowset);
        }
        result.retain(|rowset| rowset.rowset_state() != RowsetState::Deleted);
        result.sort_by_key(|rs| rs.start_version());
        Ok(result)
    }

    pub fn materialize_storage_snapshot(
        &self,
        visible_version: i64,
    ) -> Result<TabletSnapshotMaterialization> {
        let _layout_guard = self.meta_lock.read().unwrap();
        let layout_epoch_snapshot = self.layout_epoch();
        let layout_lease = self.lease_layout_epoch(layout_epoch_snapshot)?;

        let (
            mut rowsets,
            cut_schema_version,
            cut_schema_version_consistent,
            cut_physical_schema_token,
            cut_physical_schema_token_consistent,
        ) = {
            let catalog = self.rowset_catalog.read().unwrap();
            let handles = self.rowsets_by_id.read().unwrap();
            let cut = catalog.capture_entry_ids(visible_version, layout_epoch_snapshot)?;
            let mut rowsets = Vec::with_capacity(cut.rowset_ids.len());
            for rowset_id in &cut.rowset_ids {
                let rowset = handles.get(rowset_id).cloned().ok_or_else(|| {
                    paro_error::internal(format!(
                        "catalog cut references missing rowset handle {rowset_id}"
                    ))
                })?;
                rowsets.push(rowset);
            }
            (
                rowsets,
                cut.schema_version,
                cut.schema_version_consistent,
                cut.physical_schema_token,
                cut.physical_schema_token_consistent,
            )
        };

        rowsets.retain(|rowset| rowset.rowset_state() != RowsetState::Deleted);
        rowsets.sort_by_key(|rowset| rowset.start_version());
        let schema_version = if cut_schema_version_consistent {
            cut_schema_version
        } else {
            None
        };
        let schema_epoch_snapshot = schema_version
            .map(u64::from)
            .or_else(|| self.schema_epoch());
        let physical_schema_token = if cut_physical_schema_token_consistent {
            cut_physical_schema_token
        } else {
            None
        };
        let schema_adaptation = TabletSchemaAdaptationPlan::for_snapshot(
            &rowsets,
            schema_version,
            physical_schema_token,
            cut_schema_version_consistent,
            cut_physical_schema_token_consistent,
        );

        Ok(TabletSnapshotMaterialization {
            layout_epoch_snapshot,
            schema_epoch_snapshot,
            physical_schema_token,
            schema_adaptation,
            rowsets,
            _layout_lease: layout_lease,
        })
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
            let guard = TabletReadGuard::pin(self, visible_version)?;
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
            let _guard = TabletReadGuard::pin(self, visible_version)?;
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
            let mut rowset_meta = rowset.rowset_meta();
            rowset_meta.set_rowset_path(rowset.rowset_path().to_string_lossy().to_string());
            rowset_meta.set_rowset_state(RowsetState::Visible);
            meta.add_rowset_meta(rowset_meta);
        }
        let snapshot_rowset_ids: HashSet<_> = snapshot
            .rowsets
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        meta.set_retained_rowset_metas(Vec::new());
        meta.set_rssid_mappings(
            self.rssid_manager
                .snapshot_entries()
                .into_iter()
                .filter(|entry| snapshot_rowset_ids.contains(&entry.rowset_id))
                .collect(),
        );
        meta.set_row_id_format_version(CURRENT_ROW_ID_FORMAT_VERSION);
        meta.set_layout_epoch(snapshot.layout_epoch_cut);
        meta.set_applied_lsn(self.applied_lsn());
        meta.set_rowset_catalog_slice(Some(snapshot.rowset_catalog_slice.clone()));
        let maintenance_ids = self.rowset_maintenance_ids.read().unwrap();
        meta.set_rowset_maintenance_ids(
            snapshot_rowset_ids
                .iter()
                .map(|rowset_id| super::tablet_meta::RowsetMaintenanceMeta {
                    rowset_id: *rowset_id,
                    maintenance_id: maintenance_ids.get(rowset_id).copied().unwrap_or(0),
                })
                .collect(),
        );
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
        let layout_epoch_cut = self.layout_epoch();
        let rowsets =
            self.capture_checkpoint_rowsets(visible_version, checkpoint_maintenance_id)?;
        let rowset_catalog_slice = self.checkpoint_catalog_slice_from_rowsets(
            &rowsets,
            layout_epoch_cut,
            visible_version,
        )?;
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
            layout_epoch_cut,
            rowset_catalog_slice,
            rowsets,
            freeze_mode,
        })
    }

    fn checkpoint_catalog_slice_from_rowsets(
        &self,
        rowsets: &[RowsetSharedPtr],
        layout_epoch_cut: u64,
        visible_version: i64,
    ) -> Result<RowsetCatalogCheckpointSlice> {
        let descriptors = rowsets
            .iter()
            .map(|rowset| self.rowset_catalog_descriptor(rowset.as_ref()))
            .collect::<Vec<_>>();
        Ok(VersionedRowsetCatalog::rebuild_from_live(
            descriptors,
            layout_epoch_cut,
            visible_version,
        )?
        .checkpoint_slice())
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
            let source_rowset = self
                .find_retained_rowset_by_id(*source_rowset_id)
                .ok_or_else(|| {
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
        self.rowset_catalog
            .read()
            .unwrap()
            .detect_version_gaps(target_max, self.layout_epoch())
            .unwrap_or_default()
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
        self.sync_runtime_meta_fields(&mut meta)?;
        self.persist_meta(&meta)
    }

    pub fn persist_meta_snapshot(&self) -> Result<()> {
        self.save_meta()
    }

    fn persist_meta(&self, meta: &TabletMeta) -> Result<()> {
        let mut persisted = meta.clone();
        self.sync_runtime_meta_fields(&mut persisted)?;
        if let Some(manager) = &self.tablet_meta_manager {
            manager.save_tablet_meta(&persisted)?;
        }
        Ok(())
    }

    fn sync_runtime_meta_fields(&self, meta: &mut TabletMeta) -> Result<()> {
        meta.set_rssid_mappings(self.rssid_manager.snapshot_entries());
        meta.set_row_id_format_version(CURRENT_ROW_ID_FORMAT_VERSION);
        meta.set_layout_epoch(self.layout_epoch());
        meta.set_applied_lsn(self.applied_lsn());
        let retained_metas = self
            .retired_pending_gc
            .read()
            .unwrap()
            .values()
            .map(|entry| {
                let mut meta = entry.rowset.rowset_meta();
                meta.set_rowset_path(entry.rowset.rowset_path().to_string_lossy().to_string());
                meta
            })
            .collect::<Vec<_>>();
        meta.set_retained_rowset_metas(retained_metas);

        let available_rowset_ids = self
            .rowsets_by_id
            .read()
            .unwrap()
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        let catalog_slice = self
            .rowset_catalog
            .read()
            .unwrap()
            .checkpoint_slice_for_rowsets(&available_rowset_ids)?;
        meta.set_rowset_catalog_slice(Some(catalog_slice));

        let maintenance_ids = self
            .rowset_maintenance_ids
            .read()
            .unwrap()
            .iter()
            .map(
                |(&rowset_id, &maintenance_id)| super::tablet_meta::RowsetMaintenanceMeta {
                    rowset_id,
                    maintenance_id,
                },
            )
            .collect();
        meta.set_rowset_maintenance_ids(maintenance_ids);
        meta.set_applied_mutations(
            self.applied_mutations
                .read()
                .unwrap()
                .iter()
                .copied()
                .collect(),
        );
        Ok(())
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
        Self::open_with_lock_manager(
            tablet_id,
            data_dir,
            tablet_meta_manager,
            Arc::new(ShardedLockManager::default()),
            LockNamespace::single_tenant(DatabaseId::new(0)),
        )
    }

    pub fn open_with_lock_manager(
        tablet_id: TabletId,
        data_dir: impl Into<PathBuf>,
        tablet_meta_manager: Arc<TabletMetaManager>,
        delete_lock_manager: Arc<ShardedLockManager>,
        lock_namespace: LockNamespace,
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
        let tablet = Self::create_from_meta_with_lock_manager(
            meta,
            Some(tablet_meta_manager),
            delete_lock_manager,
            lock_namespace,
        )?;
        tablet.init()?;
        Ok(tablet)
    }
}

impl Tablet {
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
        if !self.should_replay_rowset_commit(&version) {
            return Ok(());
        }

        let Some(rowset) = self.load_replayed_rowset(rowset_id, version, rowset_path)? else {
            return Ok(());
        };

        self.add_rowset(rowset)?;
        self.save_meta()?;
        Ok(())
    }

    fn should_replay_rowset_commit(&self, version: &Version) -> bool {
        let rs_map = self.rs_version_map.read().unwrap();
        !rs_map
            .keys()
            .any(|existing_version| existing_version.contains_range(version))
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

        let final_path = output_ref.resolve_for_tablet(self.data_dir())?;
        let staged_path = staged_ref.resolve_for_tablet(self.data_dir())?;
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

    /// Install one immutable search-generation directory and its tablet head
    /// as a single idempotent storage mutation.
    ///
    /// A crash after `rename` but before `save_meta` leaves a harmless
    /// unreferenced generation directory. Replaying the same mutation observes
    /// that directory, validates it, and completes the head update without
    /// rebuilding or overwriting any artifact.
    pub(crate) fn apply_search_generation_publish(&self, op: &TabletMutation) -> Result<()> {
        let guard = self.acquire_search_generation_publish_guard()?;
        match self.apply_search_generation_publish_guarded(op, &guard)? {
            SearchGenerationPublishOutcome::Advanced
            | SearchGenerationPublishOutcome::AlreadyCurrent => Ok(()),
            SearchGenerationPublishOutcome::Superseded => Err(paro_error::invalid_input(
                "online search generation publication was superseded by a newer durable head",
            )),
            SearchGenerationPublishOutcome::Retired => Err(paro_error::invalid_input(
                "online search generation publication targeted a retired definition",
            )),
        }
    }

    /// Recovery is intentionally tolerant of an older record whose effect is
    /// already dominated by a newer durable head.
    pub(crate) fn replay_search_generation_publish(&self, op: &TabletMutation) -> Result<()> {
        let guard = self.acquire_search_generation_publish_guard()?;
        self.apply_search_generation_publish_guarded(op, &guard)
            .map(|_| ())
    }

    pub(crate) fn apply_search_generation_publish_guarded(
        &self,
        op: &TabletMutation,
        guard: &SearchGenerationPublishGuard<'_>,
    ) -> Result<SearchGenerationPublishOutcome> {
        self.apply_search_generation_publish_guarded_with_after_install(op, guard, || Ok(()))
    }

    pub(crate) fn preflight_search_generation_publish_guarded(
        &self,
        head: &SearchGenerationHeadMeta,
        guard: &SearchGenerationPublishGuard<'_>,
    ) -> Result<SearchGenerationPublishOutcome> {
        if guard.tablet_id != self.tablet_id() {
            return Err(paro_error::internal(
                "search generation publication guard belongs to another tablet",
            ));
        }
        let meta = self
            .meta
            .read()
            .map_err(|_| paro_error::internal("tablet meta state poisoned"))?;
        if meta.is_search_definition_retired(head.definition_id) {
            return Ok(SearchGenerationPublishOutcome::Retired);
        }
        let Some(current) = meta
            .search_generation_heads()
            .iter()
            .find(|current| current.definition_id == head.definition_id)
        else {
            return Ok(SearchGenerationPublishOutcome::Advanced);
        };
        match (head.generation_id, head.root_version)
            .cmp(&(current.generation_id, current.root_version))
        {
            std::cmp::Ordering::Less => Ok(SearchGenerationPublishOutcome::Superseded),
            std::cmp::Ordering::Equal if current == head => {
                Ok(SearchGenerationPublishOutcome::AlreadyCurrent)
            }
            std::cmp::Ordering::Equal => Err(paro_error::data_corrupted(format!(
                "search generation {} has conflicting head at generation {} revision {}",
                head.definition_id, head.generation_id, head.root_version
            ))),
            std::cmp::Ordering::Greater => Ok(SearchGenerationPublishOutcome::Advanced),
        }
    }

    #[cfg(test)]
    fn apply_search_generation_publish_with_after_install(
        &self,
        op: &TabletMutation,
        after_install: impl FnOnce() -> Result<()>,
    ) -> Result<SearchGenerationPublishOutcome> {
        let guard = self.acquire_search_generation_publish_guard()?;
        self.apply_search_generation_publish_guarded_with_after_install(op, &guard, after_install)
    }

    fn apply_search_generation_publish_guarded_with_after_install(
        &self,
        op: &TabletMutation,
        guard: &SearchGenerationPublishGuard<'_>,
        after_install: impl FnOnce() -> Result<()>,
    ) -> Result<SearchGenerationPublishOutcome> {
        if guard.tablet_id != self.tablet_id() {
            return Err(paro_error::internal(
                "search generation publication guard belongs to another tablet",
            ));
        }
        let TabletMutation::PublishSearchGeneration {
            publication,
            generation_ref,
            head,
        } = op
        else {
            return Err(paro_error::internal(
                "apply_search_generation_publish called with non-search-generation op",
            ));
        };
        if head.definition_id == 0 || head.generation_id == 0 || head.root_file_name.is_empty() {
            return Err(paro_error::invalid_input(
                "search generation publish requires non-zero identities and a root file",
            ));
        }
        if generation_ref.namespace != ArtifactNamespace::SearchGeneration {
            return Err(paro_error::invalid_input(
                "search generation publish uses invalid artifact namespaces",
            ));
        }
        let root_file = Path::new(&head.root_file_name);
        if !matches!(
            root_file.components().next(),
            Some(std::path::Component::Normal(_))
        ) || root_file.components().count() != 1
        {
            return Err(paro_error::invalid_input(
                "search generation root file must be one relative path component",
            ));
        }
        let manifests = ManifestStore::new(self.data_dir().to_path_buf());
        let expected_generation_path =
            manifests.generation_dir(head.definition_id, head.generation_id);
        let final_path = generation_ref.resolve_for_tablet(self.data_dir())?;
        if final_path != expected_generation_path {
            return Err(paro_error::invalid_input(format!(
                "search generation destination {} does not match definition {} generation {}",
                final_path.display(),
                head.definition_id,
                head.generation_id
            )));
        }
        if let SearchGenerationPublication::InstallStaged { staged_ref } = publication {
            manifests.validate_staged_generation_ref(staged_ref, head)?;
        }

        let _meta_guard = self
            .meta_lock
            .write()
            .map_err(|_| paro_error::internal("tablet meta lock poisoned"))?;

        // Publication ordering is checked while holding the same lock as the
        // optional directory install. A stale publisher therefore cannot
        // install an orphan after a newer head has won the race.
        let previous_head = self
            .meta
            .read()
            .map_err(|_| paro_error::internal("tablet meta state poisoned"))?
            .search_generation_heads()
            .iter()
            .find(|current| current.definition_id == head.definition_id)
            .cloned();
        if self
            .meta
            .read()
            .map_err(|_| paro_error::internal("tablet meta state poisoned"))?
            .is_search_definition_retired(head.definition_id)
        {
            return Ok(SearchGenerationPublishOutcome::Retired);
        }
        if let Some(current) = previous_head.as_ref() {
            let current_revision = (current.generation_id, current.root_version);
            let candidate_revision = (head.generation_id, head.root_version);
            match candidate_revision.cmp(&current_revision) {
                std::cmp::Ordering::Less => {
                    Self::validate_installed_search_generation(&manifests, current)?;
                    return Ok(SearchGenerationPublishOutcome::Superseded);
                }
                std::cmp::Ordering::Equal if current == head => {
                    Self::validate_installed_search_generation(&manifests, current)?;
                    return Ok(SearchGenerationPublishOutcome::AlreadyCurrent);
                }
                std::cmp::Ordering::Equal => {
                    return Err(paro_error::data_corrupted(format!(
                        "search generation {} raced with conflicting revision {}:{}",
                        head.definition_id, head.generation_id, head.root_version
                    )));
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        match publication {
            SearchGenerationPublication::InstallStaged { staged_ref } => {
                let staged_path = staged_ref.resolve_for_tablet(self.data_dir())?;
                if !final_path.exists() {
                    if !staged_path.exists() {
                        return Err(paro_error::io_error(format!(
                            "staged search generation {} and destination {} are both missing",
                            staged_path.display(),
                            final_path.display()
                        )));
                    }
                    if let Some(parent) = final_path.parent() {
                        fs::create_dir_all(parent).map_err(|error| {
                            paro_error::io_error(format!(
                                "create search generation parent {}: {}",
                                parent.display(),
                                error
                            ))
                        })?;
                    }
                    fs::rename(&staged_path, &final_path).map_err(|error| {
                        paro_error::io_error(format!(
                            "atomically publish search generation {} -> {}: {} (staging and final roots must share a filesystem)",
                            staged_path.display(),
                            final_path.display(),
                            error
                        ))
                    })?;
                    Self::sync_parent_dir(&final_path)?;
                }
            }
            SearchGenerationPublication::AdvanceInstalled => {
                if !final_path.is_dir() {
                    return Err(paro_error::data_corrupted(format!(
                        "installed search generation directory {} is missing",
                        final_path.display()
                    )));
                }
            }
        }

        after_install()?;
        Self::validate_installed_search_generation(&manifests, head)?;

        {
            let mut meta = self
                .meta
                .write()
                .map_err(|_| paro_error::internal("tablet meta state poisoned"))?;
            if !meta.advance_search_generation_head(head.clone())? {
                return Err(paro_error::internal(
                    "search generation publication lost monotonic head advance under meta lock",
                ));
            }
        }
        if let Err(error) = self.save_meta() {
            let mut meta = self.meta.write().map_err(|_| {
                paro_error::internal("tablet meta state poisoned after save failure")
            })?;
            meta.restore_search_generation_head(head.definition_id, previous_head);
            return Err(error);
        }
        Ok(SearchGenerationPublishOutcome::Advanced)
    }

    /// Durably retire one catalog search definition. Retirement is a tablet
    /// mutation rather than an in-memory cleanup so delayed maintenance records
    /// cannot recreate the head during live apply or recovery.
    pub(crate) fn apply_search_generation_retirement(&self, op: &TabletMutation) -> Result<()> {
        let TabletMutation::RetireSearchGeneration { definition_id } = op else {
            return Err(paro_error::internal(
                "apply_search_generation_retirement called with non-retirement op",
            ));
        };
        let _publication_guard = self.acquire_search_generation_publish_guard()?;
        let _meta_guard = self
            .meta_lock
            .write()
            .map_err(|_| paro_error::internal("tablet meta lock poisoned"))?;
        let (previous_head, was_retired) = {
            let meta = self
                .meta
                .read()
                .map_err(|_| paro_error::internal("tablet meta state poisoned"))?;
            (
                meta.search_generation_heads()
                    .iter()
                    .find(|head| head.definition_id == *definition_id)
                    .cloned(),
                meta.is_search_definition_retired(*definition_id),
            )
        };
        if was_retired {
            return Ok(());
        }
        {
            let mut meta = self
                .meta
                .write()
                .map_err(|_| paro_error::internal("tablet meta state poisoned"))?;
            meta.retire_search_definition(*definition_id)?;
        }
        if let Err(error) = self.save_meta() {
            let mut meta = self.meta.write().map_err(|_| {
                paro_error::internal("tablet meta state poisoned after retirement save failure")
            })?;
            meta.restore_search_definition_retirement(*definition_id, previous_head, was_retired);
            return Err(error);
        }
        Ok(())
    }

    fn validate_installed_search_generation(
        manifests: &ManifestStore,
        head: &SearchGenerationHeadMeta,
    ) -> Result<()> {
        let loaded = manifests.load_manifest_for_head(head)?.ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "published search generation {} generation {} is missing root {}",
                head.definition_id, head.generation_id, head.root_file_name
            ))
        })?;
        if loaded.root.definition_id != head.definition_id
            || loaded.root.generation_id != head.generation_id
            || loaded.root.root_version != head.root_version
            || loaded.root.config_fingerprint != head.config_fingerprint
        {
            return Err(paro_error::data_corrupted(format!(
                "published search generation {} manifest identity mismatch",
                head.definition_id
            )));
        }
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
    use crate::search::capability::CoverageState;
    use crate::search::manifest::{GenerationManifestRoot, ManifestShard, ManifestStore};
    use crate::search::stats::{ExecutionModes, GenerationMaintenanceState, GenerationStats};
    use crate::search::tail::TailEntryId;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn, TabletSchema};
    use crate::test_utils::*;
    use paro_common::chunk::Chunk;
    use paro_common::effect::ArtifactRef;
    use paro_common::types::LogicalType;
    use paro_journal::mutation_identity_for_tablet;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct FailingSearchPublishObserver;

    impl RowsetPublishObserver for FailingSearchPublishObserver {
        fn prepare_rowset_publish(
            &self,
            _tablet_id: TabletId,
            _version: i64,
            _visible_rowsets: &[RowsetSharedPtr],
        ) -> Result<SearchGenerationHeadUpdates> {
            Err(paro_error::internal("injected search manifest failure"))
        }

        fn rowset_published(
            &self,
            _tablet_id: TabletId,
            _version: i64,
            _rowset: RowsetSharedPtr,
            _search_updates: SearchGenerationHeadUpdates,
        ) {
        }
    }

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

    fn test_data_dir() -> PathBuf {
        tempfile::Builder::new()
            .prefix("paro-tablet-runtime-")
            .tempdir()
            .unwrap()
            .keep()
    }

    fn staged_search_generation_publish(
        tablet: &Tablet,
        txn_id: u64,
        definition_id: u64,
        generation_id: u64,
    ) -> TabletMutation {
        let staging_root = tablet
            .data_dir()
            .join("_staged")
            .join("search-generation")
            .join(format!(
                "txn-{txn_id}-def-{definition_id}-gen-{generation_id}"
            ));
        let staged_manifests = ManifestStore::new(staging_root.clone());
        let shard = staged_manifests
            .write_shard(definition_id, generation_id, 1, &ManifestShard::default())
            .unwrap();
        let mut root = GenerationManifestRoot {
            definition_id,
            generation_id,
            build_epoch: 1,
            build_snapshot_version: 0,
            indexed_through_ts: 0,
            config_fingerprint: 991,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            persisted_tail_entry_id_seed: TailEntryId(1),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 1,
            checksum: 0,
            shard_files: vec![shard],
            recent_delta_files: Vec::new(),
        };
        root.recompute_checksum().unwrap();
        staged_manifests.write_root(definition_id, &root).unwrap();
        let staged_path = staged_manifests.generation_dir(definition_id, generation_id);
        let final_manifests = ManifestStore::new(tablet.data_dir().to_path_buf());
        let final_path = final_manifests.generation_dir(definition_id, generation_id);
        TabletMutation::PublishSearchGeneration {
            publication: paro_common::effect::SearchGenerationPublication::InstallStaged {
                staged_ref: ArtifactRef::from_tablet_path(tablet.data_dir(), &staged_path).unwrap(),
            },
            generation_ref: ArtifactRef::from_tablet_path(tablet.data_dir(), &final_path).unwrap(),
            head: staged_manifests.head_for_root(&root),
        }
    }

    fn installed_search_generation_revision(
        tablet: &Tablet,
        definition_id: u64,
        root_version: u64,
    ) -> TabletMutation {
        let current = tablet
            .search_generation_head(definition_id)
            .expect("installed search generation head");
        let manifests = ManifestStore::new(tablet.data_dir().to_path_buf());
        let loaded = manifests
            .load_manifest_for_head(&current)
            .unwrap()
            .expect("installed search generation manifest");
        let mut root = loaded.root;
        root.root_version = root_version;
        root.recompute_checksum().unwrap();
        manifests.write_root(definition_id, &root).unwrap();
        TabletMutation::PublishSearchGeneration {
            publication: paro_common::effect::SearchGenerationPublication::AdvanceInstalled,
            generation_ref: manifests
                .generation_ref(definition_id, root.generation_id)
                .unwrap(),
            head: manifests.head_for_root(&root),
        }
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
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

        assert_eq!(tablet.tablet_id(), 1);
        assert_eq!(tablet.table_id(), 100);
        assert_eq!(tablet.partition_id(), 1000);
        assert_eq!(tablet.state(), TabletState::NotReady);
        assert_eq!(tablet.num_rowsets(), 0);
    }

    #[test]
    fn search_generation_publish_is_idempotent() {
        let data_dir = test_data_dir();
        let tablet = Tablet::new(1, 100, 1000, create_test_schema(), &data_dir, None).unwrap();
        let mutation = staged_search_generation_publish(&tablet, 7, 41, 1);

        tablet.apply_search_generation_publish(&mutation).unwrap();
        let first = tablet.meta.read().unwrap().serialize().unwrap();
        tablet
            .apply_search_generation_publish_with_after_install(&mutation, || {
                Err(paro_error::internal(
                    "idempotent replay must not re-enter installation",
                ))
            })
            .unwrap();
        let second = tablet.meta.read().unwrap().serialize().unwrap();

        assert_eq!(first, second);
        assert_eq!(tablet.search_generation_head(41).unwrap().generation_id, 1);
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn retired_search_definition_rejects_live_publish_and_absorbs_replay() {
        let data_dir = test_data_dir();
        let tablet = Tablet::new(1, 100, 1000, create_test_schema(), &data_dir, None).unwrap();
        let publish = staged_search_generation_publish(&tablet, 7, 41, 1);
        tablet.apply_search_generation_publish(&publish).unwrap();

        let retire = TabletMutation::RetireSearchGeneration { definition_id: 41 };
        tablet.apply_search_generation_retirement(&retire).unwrap();
        tablet.apply_search_generation_retirement(&retire).unwrap();

        assert!(tablet.search_generation_head(41).is_none());
        assert!(tablet.meta.read().unwrap().is_search_definition_retired(41));
        assert!(tablet.apply_search_generation_publish(&publish).is_err());
        tablet.replay_search_generation_publish(&publish).unwrap();
        assert!(tablet.search_generation_head(41).is_none());

        let persisted = tablet.meta.read().unwrap().serialize().unwrap();
        let restored = TabletMeta::deserialize(&persisted).unwrap();
        assert!(restored.is_search_definition_retired(41));
        assert!(restored.search_generation_heads().is_empty());
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn search_generation_replay_completes_crash_between_rename_and_meta_save() {
        let data_dir = test_data_dir();
        let baseline = TabletMeta::new(
            1,
            100,
            1000,
            create_test_schema(),
            data_dir.to_string_lossy(),
        )
        .unwrap();

        let live = Tablet::create_from_meta(baseline.clone(), None).unwrap();
        let live_mutation = staged_search_generation_publish(&live, 7, 41, 1);
        live.apply_search_generation_publish(&live_mutation)
            .unwrap();
        let identity = mutation_identity_for_tablet(88, live.tablet_id(), &live_mutation);
        live.note_applied_mutation_identity(identity).unwrap();
        let live_meta = live.meta.read().unwrap().serialize().unwrap();
        fs::remove_dir_all(data_dir.join("search_registry")).unwrap();

        let replay = Tablet::create_from_meta(baseline, None).unwrap();
        let replay_mutation = staged_search_generation_publish(&replay, 7, 41, 1);
        let injected = replay
            .apply_search_generation_publish_with_after_install(&replay_mutation, || {
                Err(paro_error::internal(
                    "injected crash after generation rename",
                ))
            })
            .unwrap_err();
        assert!(injected.to_string().contains("injected crash"));
        assert!(replay.search_generation_head(41).is_none());
        let TabletMutation::PublishSearchGeneration { generation_ref, .. } = &replay_mutation
        else {
            unreachable!();
        };
        assert!(generation_ref
            .resolve_for_tablet(replay.data_dir())
            .unwrap()
            .exists());

        replay
            .apply_search_generation_publish(&replay_mutation)
            .unwrap();
        replay.note_applied_mutation_identity(identity).unwrap();
        let replay_meta = replay.meta.read().unwrap().serialize().unwrap();
        assert_eq!(live_meta, replay_meta);
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn search_generation_replay_never_overwrites_newer_root_revision() {
        let data_dir = test_data_dir();
        let tablet = Tablet::new(1, 100, 1000, create_test_schema(), &data_dir, None).unwrap();
        let initial = staged_search_generation_publish(&tablet, 7, 41, 1);
        tablet.replay_search_generation_publish(&initial).unwrap();
        let next = installed_search_generation_revision(&tablet, 41, 2);
        tablet.apply_search_generation_publish(&next).unwrap();

        let initial_identity = initial.stable_artifact_id();
        let next_identity = next.stable_artifact_id();
        assert_ne!(initial_identity, next_identity);
        tablet.replay_search_generation_publish(&initial).unwrap();
        tablet.apply_search_generation_publish(&next).unwrap();
        assert_eq!(tablet.search_generation_head(41).unwrap().root_version, 2);
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn staged_generation_and_maintenance_revision_converge_on_newest_head() {
        let data_dir = test_data_dir();
        let tablet =
            Arc::new(Tablet::new(1, 100, 1000, create_test_schema(), &data_dir, None).unwrap());
        let initial = staged_search_generation_publish(&tablet, 7, 41, 1);
        tablet.apply_search_generation_publish(&initial).unwrap();
        let maintenance = installed_search_generation_revision(&tablet, 41, 2);
        let replacement = staged_search_generation_publish(&tablet, 8, 41, 2);

        let maintenance_tablet = Arc::clone(&tablet);
        let maintenance_thread = std::thread::spawn(move || {
            maintenance_tablet.apply_search_generation_publish(&maintenance)
        });
        let replacement_tablet = Arc::clone(&tablet);
        let replacement_thread = std::thread::spawn(move || {
            replacement_tablet
                .apply_search_generation_publish(&replacement)
                .unwrap();
        });
        let maintenance_result = maintenance_thread.join().unwrap();
        replacement_thread.join().unwrap();

        let head = tablet.search_generation_head(41).unwrap();
        assert_eq!((head.generation_id, head.root_version), (2, 1));
        if let Err(error) = maintenance_result {
            assert!(error.to_string().contains("superseded"));
        }
        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn search_mutation_identity_history_is_derived_from_durable_lifecycle_state() {
        let data_dir = test_data_dir();
        let tablet = Tablet::new(1, 100, 1000, create_test_schema(), &data_dir, None).unwrap();
        let initial = staged_search_generation_publish(&tablet, 7, 41, 1);
        tablet.apply_search_generation_publish(&initial).unwrap();
        tablet
            .note_applied_mutation_identity(mutation_identity_for_tablet(
                1,
                tablet.tablet_id(),
                &initial,
            ))
            .unwrap();
        let next = installed_search_generation_revision(&tablet, 41, 2);
        tablet.apply_search_generation_publish(&next).unwrap();
        tablet
            .note_applied_mutation_identity(mutation_identity_for_tablet(
                2,
                tablet.tablet_id(),
                &next,
            ))
            .unwrap();
        let retire = TabletMutation::RetireSearchGeneration { definition_id: 41 };
        tablet.apply_search_generation_retirement(&retire).unwrap();
        tablet
            .note_applied_mutation_identity(mutation_identity_for_tablet(
                3,
                tablet.tablet_id(),
                &retire,
            ))
            .unwrap();
        assert!(
            !tablet.has_applied_mutation_identity(mutation_identity_for_tablet(
                3,
                tablet.tablet_id(),
                &retire,
            ))
        );

        let search_identities = tablet
            .applied_mutations
            .read()
            .unwrap()
            .iter()
            .filter(|identity| {
                matches!(
                    identity.mutation_kind,
                    super::super::tablet_meta::AppliedMutationKind::PublishSearchGeneration
                        | super::super::tablet_meta::AppliedMutationKind::RetireSearchGeneration
                )
            })
            .count();
        assert_eq!(search_identities, 0);
        let _ = fs::remove_dir_all(data_dir);
    }

    fn create_test_rowset(id: u64, version: Version, tablet_id: u64) -> RowsetSharedPtr {
        let schema = create_test_schema();
        let meta = RowsetMeta::new(id, tablet_id, version);
        let rowset = crate::rowset::Rowset::create(schema, meta, test_data_dir()).unwrap();
        Arc::new(rowset)
    }

    fn chunk_with_names(ids: &[i64], names: &[&str]) -> Chunk {
        test_chunk_from_vectors(vec![test_i64_vector(ids), test_string_vector(names)])
    }

    #[test]
    fn test_tablet_init() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

        assert_eq!(tablet.state(), TabletState::NotReady);
        tablet.init().unwrap();
        assert_eq!(tablet.state(), TabletState::Running);
    }

    #[test]
    fn test_tablet_add_rowset() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

        let rowset1 = create_test_rowset(1, Version::singleton(0), 1);
        let rowset2 = create_test_rowset(2, Version::singleton(1), 1);

        tablet.add_rowset(rowset1).unwrap();
        tablet.add_rowset(rowset2).unwrap();

        assert_eq!(tablet.num_rowsets(), 2);
        assert_eq!(tablet.max_version(), 1);
    }

    #[test]
    fn search_manifest_failure_preserves_last_head_without_rolling_back_rowset() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();
        tablet
            .meta
            .write()
            .unwrap()
            .advance_search_generation_head(SearchGenerationHeadMeta {
                definition_id: 77,
                generation_id: 1,
                root_version: 1,
                config_fingerprint: 99,
                root_file_name: "manifest_root_g1_v1_f99.json".to_string(),
            })
            .unwrap();
        assert!(tablet.search_generation_head(77).is_some());

        let observer: Arc<dyn RowsetPublishObserver> = Arc::new(FailingSearchPublishObserver);
        tablet.bind_rowset_publish_observer(Arc::downgrade(&observer));
        let rowset = create_test_rowset(1, Version::singleton(0), 1);
        tablet
            .rowset_commit_auto(rowset)
            .expect("derived search failure must not roll back base-table DML");

        assert_eq!(tablet.max_version(), 0);
        assert_eq!(tablet.search_generation_head(77).unwrap().root_version, 1);
    }

    #[test]
    fn accepted_manifest_revision_survives_tablet_meta_save_failure() {
        let tmp = TempDir::new().unwrap();
        let manager = create_test_meta_manager(&tmp);
        let tablet = Tablet::new(
            1,
            100,
            1000,
            create_test_schema(),
            tmp.path(),
            Some(manager),
        )
        .unwrap();
        let manifests = ManifestStore::new(tablet.data_dir().to_path_buf());
        let definition_id = 78;
        let mut root = GenerationManifestRoot {
            definition_id,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: 0,
            indexed_through_ts: 0,
            config_fingerprint: 1001,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            persisted_tail_entry_id_seed: TailEntryId(1),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 0,
            checksum: 0,
            shard_files: Vec::new(),
            recent_delta_files: Vec::new(),
        };
        root.recompute_checksum().unwrap();
        let mut revision = manifests.begin_empty_revision(definition_id, root).unwrap();
        revision
            .replace_with_shard(&ManifestShard::default())
            .unwrap();
        let manifest = revision.commit().unwrap();
        let root_path = manifest.root_path.clone();
        let head = manifests.head_for_root(&manifest.root);
        let mut updates = SearchGenerationHeadUpdates::default();
        updates.push(head.clone(), manifest);

        let advanced = tablet.apply_search_generation_heads_locked(&updates);
        updates.accept_in_memory_heads(&advanced);
        crate::meta::metadata_store::testing::arm_metadata_rename_failure_for_path_on_nth_call(
            tmp.path().join("meta/tablet/1/meta.bin"),
            1,
        );
        assert!(tablet.save_meta().is_err());
        drop(updates);

        assert_eq!(tablet.search_generation_head(definition_id), Some(head));
        assert!(
            root_path.exists(),
            "an in-memory head must retain its immutable manifest after save failure"
        );
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
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

        let rowset1 = create_test_rowset(1, Version::new(0, 5), 1);
        let rowset2 = create_test_rowset(2, Version::new(3, 7), 1);

        tablet.add_rowset(rowset1).unwrap();
        let result = tablet.add_rowset(rowset2);
        assert!(result.is_err());
    }

    #[test]
    fn test_tablet_capture_consistent_rowsets() {
        let schema = create_test_schema();
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

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
    fn tablet_read_guard_uses_retention_lease_and_lightweight_pin() {
        let dir = tempfile::tempdir().unwrap();
        let schema = create_test_schema();
        let tablet =
            Arc::new(Tablet::new(1, 100, 1000, schema, dir.path().join("tablet"), None).unwrap());

        assert_eq!(tablet.active_snapshot_pin_count(), 0);
        assert_eq!(tablet.min_active_visible_version(), None);
        assert_eq!(tablet.read_snapshot_lease_count(), 0);

        let guard = TabletReadGuard::pin(&tablet, 7).unwrap();
        assert_eq!(guard.visible_version(), 7);
        assert_eq!(tablet.active_snapshot_pin_count(), 1);
        assert_eq!(tablet.min_active_visible_version(), Some(7));
        assert_eq!(tablet.read_snapshot_lease_count(), 1);

        {
            let _older_guard = TabletReadGuard::pin(&tablet, 3).unwrap();
            assert_eq!(tablet.active_snapshot_pin_count(), 2);
            assert_eq!(tablet.min_active_visible_version(), Some(3));
            assert_eq!(tablet.read_snapshot_lease_count(), 2);
        }

        assert_eq!(tablet.active_snapshot_pin_count(), 1);
        assert_eq!(tablet.min_active_visible_version(), Some(7));
        assert_eq!(tablet.read_snapshot_lease_count(), 1);

        drop(guard);
        assert_eq!(tablet.active_snapshot_pin_count(), 0);
        assert_eq!(tablet.min_active_visible_version(), None);
        assert_eq!(tablet.read_snapshot_lease_count(), 0);
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
    fn checkpoint_catalog_slice_recovers_retained_compaction_inputs_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let manager = create_test_meta_manager(&dir);
        let schema = create_test_schema();
        let tablet = Arc::new(
            Tablet::new(
                77,
                100,
                1000,
                schema,
                dir.path().join("tablet"),
                Some(manager.clone()),
            )
            .unwrap(),
        );
        tablet.init().unwrap();

        let input_a = create_test_rowset(10, Version::singleton(0), tablet.tablet_id());
        let input_b = create_test_rowset(11, Version::singleton(1), tablet.tablet_id());
        tablet.add_rowset(input_a.clone()).unwrap();
        tablet.add_rowset(input_b.clone()).unwrap();
        let history_layout_epoch = tablet.layout_epoch();

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
        tablet.save_meta().unwrap();

        drop(tablet);
        let reopened = Arc::new(
            Tablet::open(77, dir.path().join("tablet"), manager)
                .expect("tablet should reopen from catalog checkpoint slice"),
        );

        let latest_ids: Vec<_> = reopened
            .capture_consistent_rowsets(1)
            .unwrap()
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        assert_eq!(latest_ids, vec![12]);

        let history_ids: Vec<_> = reopened
            .capture_consistent_rowsets_at_layout(1, history_layout_epoch)
            .unwrap()
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        assert_eq!(history_ids, vec![10, 11]);
        assert_eq!(reopened.retired_pending_gc_statuses().len(), 2);

        let checkpoint = reopened
            .capture_checkpoint_snapshot(1, 0, 0)
            .expect("persisted maintenance ids should expand post-cut compaction output");
        let checkpoint_ids: Vec<_> = checkpoint
            .rowsets
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect();
        assert_eq!(checkpoint_ids, vec![10, 11]);
    }

    #[test]
    fn empty_retired_set_does_not_confirm_watermarks_on_read_guard_drop() {
        let dir = tempfile::tempdir().unwrap();
        let tablet = Arc::new(
            Tablet::new(
                1,
                100,
                1000,
                create_test_schema(),
                dir.path().join("tablet"),
                None,
            )
            .unwrap(),
        );
        let confirmed_epoch = tablet.retention_registry.confirmed_watermarks().epoch;

        let read_guard = TabletReadGuard::pin(&tablet, 1).unwrap();
        drop(read_guard);

        assert_eq!(
            tablet.retention_registry.watermarks().epoch,
            confirmed_epoch,
            "an empty retired set must not force a confirmed registry scan"
        );
    }

    #[test]
    #[serial_test::serial]
    fn retired_inputs_wait_for_read_and_layout_leases_before_gc() {
        storage_metrics().reset_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = create_test_schema();
        let tablet = Arc::new(
            Tablet::new(
                1,
                100,
                1000,
                schema.clone(),
                dir.path().join("tablet"),
                None,
            )
            .unwrap(),
        );
        let make_rowset = |id, version| {
            let meta = RowsetMeta::new(id, tablet.tablet_id(), version);
            Arc::new(
                crate::rowset::Rowset::create(
                    schema.clone(),
                    meta,
                    dir.path().join(format!("rowset-{id}")),
                )
                .unwrap(),
            )
        };

        let input_a = make_rowset(10, Version::singleton(0));
        let input_b = make_rowset(11, Version::singleton(1));
        tablet.add_rowset(input_a.clone()).unwrap();
        tablet.add_rowset(input_b.clone()).unwrap();

        let read_guard = TabletReadGuard::pin(&tablet, 1).unwrap();
        let materialized = tablet.materialize_storage_snapshot(1).unwrap();

        let output = make_rowset(12, Version::new(0, 1));
        output.mark_compaction_output(vec![10, 11]);
        output.make_visible().unwrap();
        tablet
            .with_meta_lock("test compaction publish", || {
                tablet.install_compaction_publish_locked(
                    &[input_a.clone(), input_b.clone()],
                    &[],
                    output,
                    7,
                    CumulativePointAction::Preserve,
                    false,
                )
            })
            .unwrap();

        assert!(tablet
            .retired_pending_gc_statuses()
            .iter()
            .all(|status| status.barrier == RetiredGcBarrier::PendingSnapshotBarrier));
        assert!(tablet.find_retained_rowset_by_id(10).is_some());

        drop(materialized);
        drop(read_guard);
        tablet.sweep_retired_inputs();

        assert!(tablet.retired_pending_gc_statuses().is_empty());
        assert!(tablet.find_retained_rowset_by_id(10).is_none());
        assert!(tablet.find_retained_rowset_by_id(11).is_none());
    }

    #[test]
    fn materialize_storage_snapshot_admits_mixed_physical_schema_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let schema = create_test_schema();
        let tablet = Tablet::new(
            1,
            100,
            1000,
            schema.clone(),
            dir.path().join("tablet"),
            None,
        )
        .unwrap();
        let make_rowset = |id, version, schema_hash| {
            let mut meta = RowsetMeta::new(id, tablet.tablet_id(), version);
            meta.set_schema_hash(schema_hash);
            Arc::new(
                crate::rowset::Rowset::create(
                    schema.clone(),
                    meta,
                    dir.path().join(format!("rowset-{id}")),
                )
                .unwrap(),
            )
        };

        tablet
            .add_rowset(make_rowset(10, Version::singleton(0), 11))
            .unwrap();
        tablet
            .add_rowset(make_rowset(11, Version::singleton(1), 22))
            .unwrap();

        let materialized = tablet.materialize_storage_snapshot(1).unwrap();
        assert_eq!(materialized.rowsets.len(), 2);
        assert_eq!(materialized.physical_schema_token, None);
        assert!(materialized.schema_adaptation.mixed_physical_schema_tokens);
        assert!(materialized.schema_adaptation.adaptation_required());
    }

    #[test]
    fn compaction_publish_rejects_physical_schema_token_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let schema = create_test_schema();
        let tablet = Tablet::new(
            1,
            100,
            1000,
            schema.clone(),
            dir.path().join("tablet"),
            None,
        )
        .unwrap();
        let make_rowset = |id, version, schema_hash| {
            let mut meta = RowsetMeta::new(id, tablet.tablet_id(), version);
            meta.set_schema_hash(schema_hash);
            Arc::new(
                crate::rowset::Rowset::create(
                    schema.clone(),
                    meta,
                    dir.path().join(format!("rowset-{id}")),
                )
                .unwrap(),
            )
        };

        let input = make_rowset(10, Version::singleton(0), 11);
        tablet.add_rowset(input.clone()).unwrap();
        let output = make_rowset(11, Version::singleton(0), 22);
        let record = CompactionPublishRecord {
            plan_id: crate::compaction::plan::types::CompactionPlanId(1),
            job_id: crate::compaction::plan::types::CompactionJobId(1),
            tablet_id: tablet.tablet_id(),
            output_rowset_id: output.rowset_id(),
            output_version: output.version(),
            cumulative_point_action: CumulativePointAction::Preserve,
            output_rowset_path: output.rowset_path().to_string_lossy().into_owned(),
            replaced_inputs: vec![input.rowset_id()],
        };

        let err = tablet
            .with_meta_lock("test compaction schema guard", || {
                tablet.validate_compaction_publish_locked(&record, &[input], output.as_ref())
            })
            .unwrap_err();
        assert!(format!("{err}").contains("SchemaEpochChanged"));
    }

    #[test]
    #[serial_test::serial]
    fn checkpoint_snapshot_records_optimistic_capture_metrics() {
        storage_metrics().reset_for_tests();
        let schema = create_test_schema();
        let tablet = Arc::new(Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap());

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
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

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
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

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
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

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
        let tablet = Tablet::new(1, 100, 1000, schema, test_data_dir(), None).unwrap();

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
        pi.apply_upserts(&[(b"k1".to_vec(), RowID::new(1, SegmentRowId::from_raw(0)))])
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
