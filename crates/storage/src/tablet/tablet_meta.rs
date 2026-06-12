// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # TabletMeta
//!
//! Tablet metadata for persistence and recovery.
//!
//! ## Key Design
//!
//! - Stores all metadata needed to reconstruct a Tablet
//! - Includes schema, rowset metadata, and version information
//! - Supports serialization for persistence to disk
//! - Tracks tablet state (RUNNING, SHUTDOWN, etc.)

use super::tablet_schema::{TabletSchema, TabletSchemaRef};
use super::versioned_rowset_catalog::{
    RowsetCatalogCheckpointEntry, RowsetCatalogCheckpointSlice, RowsetCatalogFlags,
};
use crate::primary_key::RssidMappingEntry;
use crate::rowset::RowsetMeta;
use paro_common::error::{self as paro_error, Result};
use paro_journal::{MutationIdentity, MutationKind};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a Tablet
pub type TabletId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsetMaintenanceMeta {
    pub rowset_id: u64,
    pub maintenance_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AppliedMutationKind {
    PublishRowset = 1,
    ApplyPrimaryDelete = 2,
    ApplyDeletePatch = 3,
    PublishCompaction = 4,
}

impl AppliedMutationKind {
    fn from_raw(raw: u8) -> Result<Self> {
        match raw {
            1 => Ok(Self::PublishRowset),
            2 => Ok(Self::ApplyPrimaryDelete),
            3 => Ok(Self::ApplyDeletePatch),
            4 => Ok(Self::PublishCompaction),
            _ => Err(paro_error::internal(format!(
                "TabletMeta: invalid applied mutation kind {raw}"
            ))),
        }
    }
}

impl From<MutationKind> for AppliedMutationKind {
    fn from(kind: MutationKind) -> Self {
        match kind {
            MutationKind::PublishRowset => Self::PublishRowset,
            MutationKind::ApplyPrimaryDelete => Self::ApplyPrimaryDelete,
            MutationKind::ApplyDeletePatch => Self::ApplyDeletePatch,
            MutationKind::PublishCompaction => Self::PublishCompaction,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AppliedMutationMeta {
    pub commit_ts: u64,
    pub tablet_id: u64,
    pub mutation_kind: AppliedMutationKind,
    pub artifact_id: u64,
}

impl AppliedMutationMeta {
    pub fn from_journal(identity: MutationIdentity) -> Self {
        Self {
            commit_ts: identity.commit_ts,
            tablet_id: identity.tablet_id,
            mutation_kind: identity.mutation_kind.into(),
            artifact_id: identity.artifact_id,
        }
    }
}

/// Tablet state enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum TabletState {
    /// Tablet is not initialized
    NotReady = 0,

    /// Tablet is running normally
    Running = 1,

    /// Tablet is being created (schema clone)
    SchemaChange = 2,

    /// Tablet is being rolled up
    Rollup = 3,

    /// Tablet is being restored from backup
    Restore = 4,

    /// Tablet is shutting down
    Shutdown = 5,
}

impl Default for TabletState {
    fn default() -> Self {
        TabletState::NotReady
    }
}

impl std::fmt::Display for TabletState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TabletState::NotReady => write!(f, "NOT_READY"),
            TabletState::Running => write!(f, "RUNNING"),
            TabletState::SchemaChange => write!(f, "SCHEMA_CHANGE"),
            TabletState::Rollup => write!(f, "ROLLUP"),
            TabletState::Restore => write!(f, "RESTORE"),
            TabletState::Shutdown => write!(f, "SHUTDOWN"),
        }
    }
}

/// Helper function to get current timestamp
fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// TabletMeta contains all metadata for a Tablet
#[derive(Debug, Clone)]
pub struct TabletMeta {
    /// Unique tablet ID
    tablet_id: TabletId,

    /// Table ID this tablet belongs to
    table_id: u64,

    /// Partition ID this tablet belongs to
    partition_id: u64,

    /// Schema hash (for schema versioning)
    schema_hash: u32,

    /// Shard ID (for distributed deployment)
    shard_id: u32,

    /// Creation timestamp
    creation_time: i64,

    /// Cumulative layer point (version boundary for compaction)
    cumulative_layer_point: i64,

    /// Current tablet state
    tablet_state: TabletState,

    /// Tablet schema
    schema: Option<TabletSchemaRef>,

    /// Serialized schema bytes (for persistence)
    schema_bytes: Vec<u8>,

    /// Rowset metadata
    rowset_metas: Vec<RowsetMeta>,

    /// Incremental rowset metadata
    inc_rowset_metas: Vec<RowsetMeta>,

    /// Data directory path
    data_dir: String,

    /// Persisted rssid -> (rowset_id, segment_id) mappings.
    rssid_mappings: Vec<RssidMappingEntry>,

    /// Primary-index RowID encoding format version persisted for upgrade handling.
    row_id_format_version: u32,

    /// Monotonic epoch covering visible rowset/delete state.
    layout_epoch: u64,

    /// Highest journal LSN whose synchronous storage effects are reflected by this snapshot.
    applied_lsn: u64,

    /// Retired rowsets that are no longer latest-live, but are still required
    /// by retained layout/history cuts or checkpoint replay.
    retained_rowset_metas: Vec<RowsetMeta>,

    /// Persisted rowset layout catalog slice used to rebuild history before WAL replay.
    rowset_catalog_slice: Option<RowsetCatalogCheckpointSlice>,

    /// Maintenance publish ids for live and retained rowsets.
    rowset_maintenance_ids: Vec<RowsetMaintenanceMeta>,

    /// Durable mutation identities already reflected by this tablet snapshot.
    applied_mutations: Vec<AppliedMutationMeta>,

    /// Durable search generation heads whose referenced manifest roots are
    /// visible for this tablet snapshot.
    search_generation_heads: Vec<SearchGenerationHeadMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchGenerationHeadMeta {
    pub definition_id: u64,
    pub generation_id: u64,
    pub root_version: u64,
    pub config_fingerprint: u64,
    pub root_file_name: String,
}

impl TabletMeta {
    /// Create a new TabletMeta
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
        data_dir: impl Into<String>,
    ) -> Result<Self> {
        let schema_bytes = schema.serialize()?;

        Ok(Self {
            tablet_id,
            table_id,
            partition_id,
            schema_hash: Self::compute_schema_hash(&schema_bytes),
            shard_id: 0,
            creation_time: current_timestamp(),
            cumulative_layer_point: -1,
            tablet_state: TabletState::NotReady,
            schema: Some(schema),
            schema_bytes,
            rowset_metas: Vec::new(),
            inc_rowset_metas: Vec::new(),
            data_dir: data_dir.into(),
            rssid_mappings: Vec::new(),
            row_id_format_version: 0,
            layout_epoch: 0,
            applied_lsn: 0,
            retained_rowset_metas: Vec::new(),
            rowset_catalog_slice: None,
            rowset_maintenance_ids: Vec::new(),
            applied_mutations: Vec::new(),
            search_generation_heads: Vec::new(),
        })
    }

    /// Compute schema hash from serialized bytes
    fn compute_schema_hash(schema_bytes: &[u8]) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        schema_bytes.hash(&mut hasher);
        hasher.finish() as u32
    }

    // ==================== Getters ====================

    /// Get tablet ID
    pub fn tablet_id(&self) -> TabletId {
        self.tablet_id
    }

    /// Get table ID
    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Get partition ID
    pub fn partition_id(&self) -> u64 {
        self.partition_id
    }

    /// Get schema hash
    pub fn schema_hash(&self) -> u32 {
        self.schema_hash
    }

    /// Get shard ID
    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    /// Get creation time
    pub fn creation_time(&self) -> i64 {
        self.creation_time
    }

    /// Get cumulative layer point
    pub fn cumulative_layer_point(&self) -> i64 {
        self.cumulative_layer_point
    }

    /// Get tablet state
    pub fn tablet_state(&self) -> TabletState {
        self.tablet_state
    }

    /// Get data directory
    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    /// Get schema reference
    pub fn schema(&self) -> Option<&TabletSchemaRef> {
        self.schema.as_ref()
    }

    /// Get rowset metadata references
    pub fn rowset_metas(&self) -> &[RowsetMeta] {
        &self.rowset_metas
    }

    /// Get incremental rowset metadata
    pub fn inc_rowset_metas(&self) -> &[RowsetMeta] {
        &self.inc_rowset_metas
    }

    /// Get number of rowsets
    pub fn num_rowsets(&self) -> usize {
        self.rowset_metas.len() + self.inc_rowset_metas.len()
    }

    pub fn rssid_mappings(&self) -> &[RssidMappingEntry] {
        &self.rssid_mappings
    }

    pub fn row_id_format_version(&self) -> u32 {
        self.row_id_format_version
    }

    pub fn layout_epoch(&self) -> u64 {
        self.layout_epoch
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn
    }

    pub fn retained_rowset_metas(&self) -> &[RowsetMeta] {
        &self.retained_rowset_metas
    }

    pub fn rowset_catalog_slice(&self) -> Option<&RowsetCatalogCheckpointSlice> {
        self.rowset_catalog_slice.as_ref()
    }

    pub fn rowset_maintenance_ids(&self) -> &[RowsetMaintenanceMeta] {
        &self.rowset_maintenance_ids
    }

    pub fn applied_mutations(&self) -> &[AppliedMutationMeta] {
        &self.applied_mutations
    }

    pub fn search_generation_heads(&self) -> &[SearchGenerationHeadMeta] {
        &self.search_generation_heads
    }

    // ==================== Setters ====================

    /// Set shard ID
    pub fn set_shard_id(&mut self, shard_id: u32) {
        self.shard_id = shard_id;
    }

    /// Set cumulative layer point
    pub fn set_cumulative_layer_point(&mut self, point: i64) {
        self.cumulative_layer_point = point;
    }

    /// Set tablet state
    pub fn set_tablet_state(&mut self, state: TabletState) {
        self.tablet_state = state;
    }

    pub fn set_rssid_mappings(&mut self, mappings: Vec<RssidMappingEntry>) {
        self.rssid_mappings = mappings;
    }

    pub fn set_row_id_format_version(&mut self, version: u32) {
        self.row_id_format_version = version;
    }

    pub fn set_layout_epoch(&mut self, epoch: u64) {
        self.layout_epoch = epoch;
    }

    pub fn set_applied_lsn(&mut self, lsn: u64) {
        self.applied_lsn = lsn;
    }

    pub fn set_retained_rowset_metas(&mut self, metas: Vec<RowsetMeta>) {
        self.retained_rowset_metas = metas;
    }

    pub fn set_rowset_catalog_slice(&mut self, slice: Option<RowsetCatalogCheckpointSlice>) {
        self.rowset_catalog_slice = slice;
    }

    pub fn set_rowset_maintenance_ids(&mut self, mut ids: Vec<RowsetMaintenanceMeta>) {
        ids.sort_by_key(|entry| entry.rowset_id);
        ids.dedup_by_key(|entry| entry.rowset_id);
        self.rowset_maintenance_ids = ids;
    }

    pub fn set_applied_mutations(&mut self, mut mutations: Vec<AppliedMutationMeta>) {
        mutations.sort_by_key(|entry| {
            (
                entry.commit_ts,
                entry.tablet_id,
                entry.mutation_kind as u8,
                entry.artifact_id,
            )
        });
        mutations.dedup();
        self.applied_mutations = mutations;
    }

    pub fn set_search_generation_heads(&mut self, mut heads: Vec<SearchGenerationHeadMeta>) {
        heads.sort_by_key(|head| head.definition_id);
        heads.dedup_by_key(|head| head.definition_id);
        self.search_generation_heads = heads;
    }

    pub fn upsert_search_generation_head(&mut self, head: SearchGenerationHeadMeta) {
        match self
            .search_generation_heads
            .binary_search_by_key(&head.definition_id, |existing| existing.definition_id)
        {
            Ok(index) => self.search_generation_heads[index] = head,
            Err(index) => self.search_generation_heads.insert(index, head),
        }
    }

    pub fn remove_search_generation_head(&mut self, definition_id: u64) {
        if let Ok(index) = self
            .search_generation_heads
            .binary_search_by_key(&definition_id, |head| head.definition_id)
        {
            self.search_generation_heads.remove(index);
        }
    }

    // ==================== Rowset Management ====================

    /// Add a rowset metadata reference
    pub fn add_rowset_meta(&mut self, meta: RowsetMeta) {
        if meta.start_version() > self.cumulative_layer_point {
            self.inc_rowset_metas.push(meta);
        } else {
            self.rowset_metas.push(meta);
        }
    }

    /// Remove a rowset metadata reference by ID
    pub fn delete_rowset_meta(&mut self, rowset_id: u64) -> Option<RowsetMeta> {
        if let Some(pos) = self
            .rowset_metas
            .iter()
            .position(|m| m.rowset_id() == rowset_id)
        {
            Some(self.rowset_metas.remove(pos))
        } else if let Some(pos) = self
            .inc_rowset_metas
            .iter()
            .position(|m| m.rowset_id() == rowset_id)
        {
            Some(self.inc_rowset_metas.remove(pos))
        } else {
            None
        }
    }

    /// Find rowset metadata by ID
    pub fn find_rowset_meta(&self, rowset_id: u64) -> Option<&RowsetMeta> {
        self.rowset_metas
            .iter()
            .find(|m| m.rowset_id() == rowset_id)
            .or_else(|| {
                self.inc_rowset_metas
                    .iter()
                    .find(|m| m.rowset_id() == rowset_id)
            })
    }

    /// Find rowset metadata by version
    pub fn find_rowset_meta_by_version(&self, version: i64) -> Option<&RowsetMeta> {
        self.rowset_metas
            .iter()
            .find(|m| m.version().contains(version))
            .or_else(|| {
                self.inc_rowset_metas
                    .iter()
                    .find(|m| m.version().contains(version))
            })
    }

    /// Get max rowset version
    pub fn max_version(&self) -> i64 {
        self.rowset_metas
            .iter()
            .chain(self.inc_rowset_metas.iter())
            .map(|m| m.end_version())
            .max()
            .unwrap_or(-1)
    }

    // ==================== Serialization ====================

    /// Serialize TabletMeta to bytes
    ///
    /// Format: header + fixed fields + schema_bytes + rowset_metas + data_dir
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Fixed fields
        data.extend_from_slice(&self.tablet_id.to_le_bytes());
        data.extend_from_slice(&self.table_id.to_le_bytes());
        data.extend_from_slice(&self.partition_id.to_le_bytes());
        data.extend_from_slice(&self.schema_hash.to_le_bytes());
        data.extend_from_slice(&self.shard_id.to_le_bytes());
        data.extend_from_slice(&self.creation_time.to_le_bytes());
        data.extend_from_slice(&self.cumulative_layer_point.to_le_bytes());
        data.push(self.tablet_state as u8);

        // Schema bytes (length + data)
        data.extend_from_slice(&(self.schema_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(&self.schema_bytes);

        // Rowset metas (count + entries)
        data.extend_from_slice(&(self.rowset_metas.len() as u32).to_le_bytes());
        for meta in &self.rowset_metas {
            let meta_bytes = meta.serialize()?;
            data.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&meta_bytes);
        }

        // Incremental rowset metas (count + entries)
        data.extend_from_slice(&(self.inc_rowset_metas.len() as u32).to_le_bytes());
        for meta in &self.inc_rowset_metas {
            let meta_bytes = meta.serialize()?;
            data.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&meta_bytes);
        }

        // Data dir (length + bytes)
        let dir_bytes = self.data_dir.as_bytes();
        data.extend_from_slice(&(dir_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(dir_bytes);

        // Optional rssid mappings appended for backward compatibility.
        data.extend_from_slice(&(self.rssid_mappings.len() as u32).to_le_bytes());
        for entry in &self.rssid_mappings {
            data.extend_from_slice(&entry.rssid.to_le_bytes());
            data.extend_from_slice(&entry.rowset_id.to_le_bytes());
            data.extend_from_slice(&entry.segment_id.to_le_bytes());
        }

        data.extend_from_slice(&self.row_id_format_version.to_le_bytes());
        data.extend_from_slice(&self.layout_epoch.to_le_bytes());
        data.extend_from_slice(&self.applied_lsn.to_le_bytes());

        Self::write_rowset_meta_vec(&mut data, &self.retained_rowset_metas)?;
        Self::write_catalog_slice(&mut data, self.rowset_catalog_slice.as_ref());
        data.extend_from_slice(&(self.rowset_maintenance_ids.len() as u32).to_le_bytes());
        for entry in &self.rowset_maintenance_ids {
            data.extend_from_slice(&entry.rowset_id.to_le_bytes());
            data.extend_from_slice(&entry.maintenance_id.to_le_bytes());
        }
        data.extend_from_slice(&(self.applied_mutations.len() as u32).to_le_bytes());
        for entry in &self.applied_mutations {
            data.extend_from_slice(&entry.commit_ts.to_le_bytes());
            data.extend_from_slice(&entry.tablet_id.to_le_bytes());
            data.push(entry.mutation_kind as u8);
            data.extend_from_slice(&entry.artifact_id.to_le_bytes());
        }
        data.extend_from_slice(&(self.search_generation_heads.len() as u32).to_le_bytes());
        for head in &self.search_generation_heads {
            data.extend_from_slice(&head.definition_id.to_le_bytes());
            data.extend_from_slice(&head.generation_id.to_le_bytes());
            data.extend_from_slice(&head.root_version.to_le_bytes());
            data.extend_from_slice(&head.config_fingerprint.to_le_bytes());
            let name = head.root_file_name.as_bytes();
            data.extend_from_slice(&(name.len() as u32).to_le_bytes());
            data.extend_from_slice(name);
        }

        Ok(data)
    }

    fn write_rowset_meta_vec(data: &mut Vec<u8>, metas: &[RowsetMeta]) -> Result<()> {
        data.extend_from_slice(&(metas.len() as u32).to_le_bytes());
        for meta in metas {
            let meta_bytes = meta.serialize()?;
            data.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&meta_bytes);
        }
        Ok(())
    }

    fn write_catalog_slice(data: &mut Vec<u8>, slice: Option<&RowsetCatalogCheckpointSlice>) {
        let Some(slice) = slice else {
            data.push(0);
            return;
        };

        data.push(1);
        data.extend_from_slice(&slice.layout_epoch_cut.to_le_bytes());
        data.extend_from_slice(&slice.latest_published_ts.to_le_bytes());
        data.extend_from_slice(&(slice.entries.len() as u32).to_le_bytes());
        for entry in &slice.entries {
            data.extend_from_slice(&entry.entry_id.to_le_bytes());
            data.extend_from_slice(&entry.rowset_id.to_le_bytes());
            data.extend_from_slice(&entry.version.start.to_le_bytes());
            data.extend_from_slice(&entry.version.end.to_le_bytes());
            data.extend_from_slice(&entry.installed_at_epoch.to_le_bytes());
            match entry.retired_at_epoch {
                Some(epoch) => {
                    data.push(1);
                    data.extend_from_slice(&epoch.to_le_bytes());
                }
                None => {
                    data.push(0);
                    data.extend_from_slice(&0u64.to_le_bytes());
                }
            }
            data.extend_from_slice(&entry.schema_version.to_le_bytes());
            data.extend_from_slice(&entry.physical_schema_token.to_le_bytes());
            data.extend_from_slice(&entry.delete_vector_catalog_token.to_le_bytes());
            data.extend_from_slice(&entry.artifact_id.to_le_bytes());
            data.extend_from_slice(&entry.flags.bits().to_le_bytes());
            data.extend_from_slice(&entry.cold_meta_id.to_le_bytes());
        }
        data.extend_from_slice(&(slice.delete_vector_epochs.len() as u32).to_le_bytes());
        for epoch in &slice.delete_vector_epochs {
            data.extend_from_slice(&epoch.to_le_bytes());
        }
    }

    /// Deserialize TabletMeta from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut offset = 0;

        // Helper to read bytes
        let read_u64 = |data: &[u8], offset: &mut usize| -> Result<u64> {
            if *offset + 8 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(val)
        };

        let read_i64 = |data: &[u8], offset: &mut usize| -> Result<i64> {
            if *offset + 8 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(val)
        };

        let read_u32 = |data: &[u8], offset: &mut usize| -> Result<u32> {
            if *offset + 4 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;
            Ok(val)
        };

        // Fixed fields
        let tablet_id = read_u64(data, &mut offset)?;
        let table_id = read_u64(data, &mut offset)?;
        let partition_id = read_u64(data, &mut offset)?;
        let schema_hash = read_u32(data, &mut offset)?;
        let shard_id = read_u32(data, &mut offset)?;
        let creation_time = read_i64(data, &mut offset)?;
        let cumulative_layer_point = read_i64(data, &mut offset)?;

        if offset >= data.len() {
            return Err(paro_error::internal("TabletMeta: truncated state"));
        }
        let tablet_state = match data[offset] {
            0 => TabletState::NotReady,
            1 => TabletState::Running,
            2 => TabletState::SchemaChange,
            3 => TabletState::Rollup,
            4 => TabletState::Restore,
            5 => TabletState::Shutdown,
            _ => TabletState::NotReady,
        };
        offset += 1;

        // Schema bytes
        let schema_len = read_u32(data, &mut offset)? as usize;
        if offset + schema_len > data.len() {
            return Err(paro_error::internal("TabletMeta: truncated schema"));
        }
        let schema_bytes = data[offset..offset + schema_len].to_vec();
        offset += schema_len;

        // Restore schema
        let schema = if !schema_bytes.is_empty() {
            Some(Arc::new(TabletSchema::deserialize(&schema_bytes)?))
        } else {
            None
        };

        // Rowset metas
        let num_rowsets = read_u32(data, &mut offset)? as usize;
        let mut rowset_metas = Vec::with_capacity(num_rowsets);
        for _ in 0..num_rowsets {
            let meta_len = read_u32(data, &mut offset)? as usize;
            if offset + meta_len > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated rowset meta"));
            }
            let meta = RowsetMeta::deserialize(&data[offset..offset + meta_len])?;
            rowset_metas.push(meta);
            offset += meta_len;
        }

        // Incremental rowset metas
        let num_inc_rowsets = read_u32(data, &mut offset)? as usize;
        let mut inc_rowset_metas = Vec::with_capacity(num_inc_rowsets);
        for _ in 0..num_inc_rowsets {
            let meta_len = read_u32(data, &mut offset)? as usize;
            if offset + meta_len > data.len() {
                return Err(paro_error::internal(
                    "TabletMeta: truncated inc rowset meta",
                ));
            }
            let meta = RowsetMeta::deserialize(&data[offset..offset + meta_len])?;
            inc_rowset_metas.push(meta);
            offset += meta_len;
        }

        // Data dir
        let dir_len = read_u32(data, &mut offset)? as usize;
        if offset + dir_len > data.len() {
            return Err(paro_error::internal("TabletMeta: truncated data_dir"));
        }
        let data_dir = String::from_utf8_lossy(&data[offset..offset + dir_len]).to_string();
        offset += dir_len;

        let mut rssid_mappings = Vec::new();
        if offset < data.len() {
            let rssid_count = read_u32(data, &mut offset)? as usize;
            rssid_mappings.reserve(rssid_count);
            for _ in 0..rssid_count {
                let rssid = read_u32(data, &mut offset)?;
                let rowset_id = read_u64(data, &mut offset)?;
                let segment_id = read_u32(data, &mut offset)?;
                rssid_mappings.push(RssidMappingEntry {
                    rssid,
                    rowset_id,
                    segment_id,
                });
            }
        }

        let row_id_format_version = if offset < data.len() {
            read_u32(data, &mut offset)?
        } else {
            0
        };

        let layout_epoch = if offset < data.len() {
            read_u64(data, &mut offset)?
        } else {
            0
        };

        let applied_lsn = if offset < data.len() {
            read_u64(data, &mut offset)?
        } else {
            0
        };

        let retained_rowset_metas = if offset < data.len() {
            Self::read_rowset_meta_vec(data, &mut offset)?
        } else {
            Vec::new()
        };

        let rowset_catalog_slice = if offset < data.len() {
            Self::read_catalog_slice(data, &mut offset)?
        } else {
            None
        };

        let rowset_maintenance_ids = if offset < data.len() {
            let maintenance_count = read_u32(data, &mut offset)? as usize;
            let mut ids = Vec::with_capacity(maintenance_count);
            for _ in 0..maintenance_count {
                ids.push(RowsetMaintenanceMeta {
                    rowset_id: read_u64(data, &mut offset)?,
                    maintenance_id: read_u64(data, &mut offset)?,
                });
            }
            ids.sort_by_key(|entry| entry.rowset_id);
            ids.dedup_by_key(|entry| entry.rowset_id);
            ids
        } else {
            Vec::new()
        };

        let applied_mutations = if offset < data.len() {
            let mutation_count = read_u32(data, &mut offset)? as usize;
            let mut mutations = Vec::with_capacity(mutation_count);
            for _ in 0..mutation_count {
                let commit_ts = read_u64(data, &mut offset)?;
                let tablet_id = read_u64(data, &mut offset)?;
                if offset >= data.len() {
                    return Err(paro_error::internal(
                        "TabletMeta: truncated applied mutation kind",
                    ));
                }
                let mutation_kind = AppliedMutationKind::from_raw(data[offset])?;
                offset += 1;
                let artifact_id = read_u64(data, &mut offset)?;
                mutations.push(AppliedMutationMeta {
                    commit_ts,
                    tablet_id,
                    mutation_kind,
                    artifact_id,
                });
            }
            mutations.sort_by_key(|entry| {
                (
                    entry.commit_ts,
                    entry.tablet_id,
                    entry.mutation_kind as u8,
                    entry.artifact_id,
                )
            });
            mutations.dedup();
            mutations
        } else {
            Vec::new()
        };

        let search_generation_heads = if offset < data.len() {
            let head_count = read_u32(data, &mut offset)? as usize;
            let mut heads = Vec::with_capacity(head_count);
            for _ in 0..head_count {
                let definition_id = read_u64(data, &mut offset)?;
                let generation_id = read_u64(data, &mut offset)?;
                let root_version = read_u64(data, &mut offset)?;
                let config_fingerprint = read_u64(data, &mut offset)?;
                let name_len = read_u32(data, &mut offset)? as usize;
                if offset + name_len > data.len() {
                    return Err(paro_error::internal(
                        "TabletMeta: truncated search generation head root file name",
                    ));
                }
                let root_file_name =
                    String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
                offset += name_len;
                heads.push(SearchGenerationHeadMeta {
                    definition_id,
                    generation_id,
                    root_version,
                    config_fingerprint,
                    root_file_name,
                });
            }
            heads.sort_by_key(|head| head.definition_id);
            heads.dedup_by_key(|head| head.definition_id);
            heads
        } else {
            Vec::new()
        };

        Ok(Self {
            tablet_id,
            table_id,
            partition_id,
            schema_hash,
            shard_id,
            creation_time,
            cumulative_layer_point,
            tablet_state,
            schema,
            schema_bytes,
            rowset_metas,
            inc_rowset_metas,
            data_dir,
            rssid_mappings,
            row_id_format_version,
            layout_epoch,
            applied_lsn,
            retained_rowset_metas,
            rowset_catalog_slice,
            rowset_maintenance_ids,
            applied_mutations,
            search_generation_heads,
        })
    }

    fn read_rowset_meta_vec(data: &[u8], offset: &mut usize) -> Result<Vec<RowsetMeta>> {
        let read_u32 = |data: &[u8], offset: &mut usize| -> Result<u32> {
            if *offset + 4 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;
            Ok(val)
        };

        let count = read_u32(data, offset)? as usize;
        let mut metas = Vec::with_capacity(count);
        for _ in 0..count {
            let meta_len = read_u32(data, offset)? as usize;
            if *offset + meta_len > data.len() {
                return Err(paro_error::internal(
                    "TabletMeta: truncated retained rowset meta",
                ));
            }
            metas.push(RowsetMeta::deserialize(&data[*offset..*offset + meta_len])?);
            *offset += meta_len;
        }
        Ok(metas)
    }

    fn read_catalog_slice(
        data: &[u8],
        offset: &mut usize,
    ) -> Result<Option<RowsetCatalogCheckpointSlice>> {
        let read_u64 = |data: &[u8], offset: &mut usize| -> Result<u64> {
            if *offset + 8 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(val)
        };
        let read_i64 = |data: &[u8], offset: &mut usize| -> Result<i64> {
            if *offset + 8 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(val)
        };
        let read_u32 = |data: &[u8], offset: &mut usize| -> Result<u32> {
            if *offset + 4 > data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
            *offset += 4;
            Ok(val)
        };
        let read_u8 = |data: &[u8], offset: &mut usize| -> Result<u8> {
            if *offset >= data.len() {
                return Err(paro_error::internal("TabletMeta: truncated data"));
            }
            let val = data[*offset];
            *offset += 1;
            Ok(val)
        };

        if read_u8(data, offset)? == 0 {
            return Ok(None);
        }

        let layout_epoch_cut = read_u64(data, offset)?;
        let latest_published_ts = read_i64(data, offset)?;
        let entry_count = read_u32(data, offset)? as usize;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let entry_id = read_u32(data, offset)?;
            let rowset_id = read_u64(data, offset)?;
            let version_start = read_i64(data, offset)?;
            let version_end = read_i64(data, offset)?;
            let installed_at_epoch = read_u64(data, offset)?;
            let retired_present = read_u8(data, offset)? != 0;
            let retired_epoch = read_u64(data, offset)?;
            let schema_version = read_u32(data, offset)?;
            let physical_schema_token = read_u64(data, offset)?;
            let delete_vector_catalog_token = read_u64(data, offset)?;
            let artifact_id = read_u64(data, offset)?;
            let flags = RowsetCatalogFlags::from_bits(read_u32(data, offset)?);
            let cold_meta_id = read_u32(data, offset)?;
            entries.push(RowsetCatalogCheckpointEntry {
                entry_id,
                rowset_id,
                version: super::Version::new(version_start, version_end),
                installed_at_epoch,
                retired_at_epoch: retired_present.then_some(retired_epoch),
                schema_version,
                physical_schema_token,
                delete_vector_catalog_token,
                artifact_id,
                flags,
                cold_meta_id,
            });
        }
        let delete_vector_epoch_count = read_u32(data, offset)? as usize;
        let mut delete_vector_epochs = Vec::with_capacity(delete_vector_epoch_count);
        for _ in 0..delete_vector_epoch_count {
            delete_vector_epochs.push(read_u64(data, offset)?);
        }

        Ok(Some(RowsetCatalogCheckpointSlice {
            layout_epoch_cut,
            latest_published_ts,
            entries,
            delete_vector_epochs,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::tablet_schema::{KeysType, TabletColumn};
    use crate::tablet::Version;
    use paro_common::types::LogicalType;

    fn create_test_schema() -> TabletSchemaRef {
        let columns = vec![
            TabletColumn::key(0, "id", LogicalType::BigInt),
            TabletColumn::new(1, "name", LogicalType::Varchar),
        ];
        Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap())
    }

    #[test]
    fn test_tablet_meta_new() {
        let schema = create_test_schema();
        let meta = TabletMeta::new(1, 100, 1000, schema, "/data/tablet_1").unwrap();

        assert_eq!(meta.tablet_id(), 1);
        assert_eq!(meta.table_id(), 100);
        assert_eq!(meta.partition_id(), 1000);
        assert_eq!(meta.tablet_state(), TabletState::NotReady);
        assert!(meta.schema().is_some());
    }

    #[test]
    fn test_tablet_meta_rowset_management() {
        let schema = create_test_schema();
        let mut meta = TabletMeta::new(1, 100, 1000, schema, "/data").unwrap();

        // Add rowsets
        meta.add_rowset_meta(RowsetMeta::new(1, 1, Version::singleton(0)));
        meta.add_rowset_meta(RowsetMeta::new(2, 1, Version::singleton(1)));
        meta.add_rowset_meta(RowsetMeta::new(3, 1, Version::new(2, 5)));

        assert_eq!(meta.num_rowsets(), 3);
        assert_eq!(meta.max_version(), 5);

        // Find by ID
        assert!(meta.find_rowset_meta(2).is_some());
        assert!(meta.find_rowset_meta(99).is_none());

        // Find by version
        assert!(meta.find_rowset_meta_by_version(3).is_some());
        assert_eq!(meta.find_rowset_meta_by_version(3).unwrap().rowset_id(), 3);

        // Delete
        let deleted = meta.delete_rowset_meta(2);
        assert!(deleted.is_some());
        assert_eq!(meta.num_rowsets(), 2);
    }

    #[test]
    fn test_tablet_meta_serialize_deserialize() {
        let schema = create_test_schema();
        let mut meta = TabletMeta::new(1, 100, 1000, schema, "/data").unwrap();
        meta.set_tablet_state(TabletState::Running);
        meta.set_cumulative_layer_point(5);
        meta.add_rowset_meta(RowsetMeta::new(1, 1, Version::singleton(0)));
        meta.set_rssid_mappings(vec![RssidMappingEntry {
            rssid: 7,
            rowset_id: 1,
            segment_id: 0,
        }]);
        meta.set_row_id_format_version(1);
        meta.set_layout_epoch(9);
        meta.set_applied_lsn(77);
        meta.set_retained_rowset_metas(vec![RowsetMeta::new(9, 1, Version::singleton(0))]);
        meta.set_rowset_catalog_slice(Some(RowsetCatalogCheckpointSlice {
            layout_epoch_cut: 9,
            latest_published_ts: 5,
            entries: vec![RowsetCatalogCheckpointEntry {
                entry_id: 0,
                rowset_id: 1,
                version: Version::singleton(0),
                installed_at_epoch: 1,
                retired_at_epoch: None,
                schema_version: 1,
                physical_schema_token: 11,
                delete_vector_catalog_token: 0,
                artifact_id: 1,
                flags: RowsetCatalogFlags::empty(),
                cold_meta_id: 0,
            }],
            delete_vector_epochs: vec![8],
        }));
        meta.set_rowset_maintenance_ids(vec![RowsetMaintenanceMeta {
            rowset_id: 1,
            maintenance_id: 3,
        }]);
        meta.set_applied_mutations(vec![AppliedMutationMeta {
            commit_ts: 5,
            tablet_id: 1,
            mutation_kind: AppliedMutationKind::ApplyDeletePatch,
            artifact_id: 99,
        }]);
        meta.set_search_generation_heads(vec![SearchGenerationHeadMeta {
            definition_id: 42,
            generation_id: 1,
            root_version: 7,
            config_fingerprint: 99,
            root_file_name: "manifest_root_g1_v7.json".to_string(),
        }]);

        let bytes = meta.serialize().unwrap();
        let restored = TabletMeta::deserialize(&bytes).unwrap();

        assert_eq!(restored.tablet_id(), 1);
        assert_eq!(restored.tablet_state(), TabletState::Running);
        assert_eq!(restored.cumulative_layer_point(), 5);
        assert_eq!(restored.num_rowsets(), 1);
        assert_eq!(
            restored.rssid_mappings(),
            &[RssidMappingEntry {
                rssid: 7,
                rowset_id: 1,
                segment_id: 0,
            }]
        );
        assert_eq!(restored.row_id_format_version(), 1);
        assert_eq!(restored.layout_epoch(), 9);
        assert_eq!(restored.applied_lsn(), 77);
        assert_eq!(restored.retained_rowset_metas().len(), 1);
        let slice = restored.rowset_catalog_slice().unwrap();
        assert_eq!(slice.layout_epoch_cut, 9);
        assert_eq!(slice.latest_published_ts, 5);
        assert_eq!(slice.entries.len(), 1);
        assert_eq!(slice.entries[0].rowset_id, 1);
        assert_eq!(slice.delete_vector_epochs, vec![8]);
        assert_eq!(
            restored.rowset_maintenance_ids(),
            &[RowsetMaintenanceMeta {
                rowset_id: 1,
                maintenance_id: 3,
            }]
        );
        assert_eq!(
            restored.applied_mutations(),
            &[AppliedMutationMeta {
                commit_ts: 5,
                tablet_id: 1,
                mutation_kind: AppliedMutationKind::ApplyDeletePatch,
                artifact_id: 99,
            }]
        );
        assert_eq!(
            restored.search_generation_heads(),
            &[SearchGenerationHeadMeta {
                definition_id: 42,
                generation_id: 1,
                root_version: 7,
                config_fingerprint: 99,
                root_file_name: "manifest_root_g1_v7.json".to_string(),
            }]
        );
        assert!(restored.schema().is_some());
    }

    #[test]
    fn test_tablet_state_display() {
        assert_eq!(format!("{}", TabletState::Running), "RUNNING");
        assert_eq!(format!("{}", TabletState::Shutdown), "SHUTDOWN");
    }
}
