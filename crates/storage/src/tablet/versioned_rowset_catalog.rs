// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Versioned rowset history for tablet-local layout cuts.
//!
//! The catalog is deliberately rowset-id based. The tablet runtime owns live
//! `RowsetSharedPtr` handles and uses this structure as the authoritative
//! logical/physical history index.

use super::{Version, VersionGap};
use paro_common::error::{self as paro_error, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub type EntryId = u32;

const DEFAULT_FENCE_EVENT_INTERVAL: usize = 512;
const EARLY_FENCE_MIN_EVENTS: usize = 128;
const ENTRY_ID_REWRITE_THRESHOLD: EntryId = (EntryId::MAX as f64 * 0.90) as EntryId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowsetCatalogFlags(u32);

impl RowsetCatalogFlags {
    pub const COMPACTION_OUTPUT: Self = Self(1 << 0);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn contains(self, flag: Self) -> bool {
        self.0 & flag.0 == flag.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowsetCatalogDescriptor {
    pub rowset_id: u64,
    pub version: Version,
    pub schema_version: u32,
    pub physical_schema_token: u64,
    pub delete_vector_catalog_token: u64,
    pub artifact_id: u64,
    pub flags: RowsetCatalogFlags,
    pub cold_meta_id: u32,
}

impl RowsetCatalogDescriptor {
    pub const fn new(rowset_id: u64, version: Version) -> Self {
        Self {
            rowset_id,
            version,
            schema_version: 0,
            physical_schema_token: 0,
            delete_vector_catalog_token: 0,
            artifact_id: rowset_id,
            flags: RowsetCatalogFlags::empty(),
            cold_meta_id: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetCatalogEntry {
    pub entry_id: EntryId,
    pub rowset_id: u64,
    pub version: Version,
    pub installed_at_epoch: u64,
    pub retired_at_epoch: Option<u64>,
    pub schema_version: u32,
    pub physical_schema_token: u64,
    pub delete_vector_catalog_token: u64,
    pub artifact_id: u64,
    pub flags: RowsetCatalogFlags,
    pub cold_meta_id: u32,
}

impl RowsetCatalogEntry {
    pub fn version_start(&self) -> i64 {
        self.version.start
    }

    pub fn version_end(&self) -> i64 {
        self.version.end
    }

    pub fn is_active_at(&self, layout_epoch: u64) -> bool {
        self.installed_at_epoch <= layout_epoch
            && self
                .retired_at_epoch
                .map_or(true, |retired| retired > layout_epoch)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetCatalogCheckpointEntry {
    pub entry_id: EntryId,
    pub rowset_id: u64,
    pub version: Version,
    pub installed_at_epoch: u64,
    pub retired_at_epoch: Option<u64>,
    pub schema_version: u32,
    pub physical_schema_token: u64,
    pub delete_vector_catalog_token: u64,
    pub artifact_id: u64,
    pub flags: RowsetCatalogFlags,
    pub cold_meta_id: u32,
}

impl From<&RowsetCatalogEntry> for RowsetCatalogCheckpointEntry {
    fn from(entry: &RowsetCatalogEntry) -> Self {
        Self {
            entry_id: entry.entry_id,
            rowset_id: entry.rowset_id,
            version: entry.version,
            installed_at_epoch: entry.installed_at_epoch,
            retired_at_epoch: entry.retired_at_epoch,
            schema_version: entry.schema_version,
            physical_schema_token: entry.physical_schema_token,
            delete_vector_catalog_token: entry.delete_vector_catalog_token,
            artifact_id: entry.artifact_id,
            flags: entry.flags,
            cold_meta_id: entry.cold_meta_id,
        }
    }
}

impl From<RowsetCatalogCheckpointEntry> for RowsetCatalogEntry {
    fn from(entry: RowsetCatalogCheckpointEntry) -> Self {
        Self {
            entry_id: entry.entry_id,
            rowset_id: entry.rowset_id,
            version: entry.version,
            installed_at_epoch: entry.installed_at_epoch,
            retired_at_epoch: entry.retired_at_epoch,
            schema_version: entry.schema_version,
            physical_schema_token: entry.physical_schema_token,
            delete_vector_catalog_token: entry.delete_vector_catalog_token,
            artifact_id: entry.artifact_id,
            flags: entry.flags,
            cold_meta_id: entry.cold_meta_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetCatalogCheckpointSlice {
    pub layout_epoch_cut: u64,
    pub latest_published_ts: i64,
    pub entries: Vec<RowsetCatalogCheckpointEntry>,
    pub delete_vector_epochs: Vec<u64>,
}

impl RowsetCatalogCheckpointSlice {
    pub fn empty(layout_epoch_cut: u64, latest_published_ts: i64) -> Self {
        Self {
            layout_epoch_cut,
            latest_published_ts,
            entries: Vec::new(),
            delete_vector_epochs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowsetLayoutEvent {
    Install { epoch: u64, entry_id: EntryId },
    Retire { epoch: u64, entry_id: EntryId },
    DeleteVector { epoch: u64 },
}

impl RowsetLayoutEvent {
    const fn epoch(self) -> u64 {
        match self {
            RowsetLayoutEvent::Install { epoch, .. }
            | RowsetLayoutEvent::Retire { epoch, .. }
            | RowsetLayoutEvent::DeleteVector { epoch } => epoch,
        }
    }
}

#[derive(Debug, Clone)]
struct EpochFence {
    layout_epoch: u64,
    event_offset: u32,
    cover: Arc<[EntryId]>,
}

#[derive(Debug, Clone)]
pub struct SnapshotCut {
    pub entry_ids: Vec<EntryId>,
    pub rowset_ids: Vec<u64>,
    pub schema_version: Option<u32>,
    pub schema_version_consistent: bool,
    pub physical_schema_token: Option<u64>,
    pub physical_schema_token_consistent: bool,
    pub gaps: Vec<VersionGap>,
}

#[derive(Debug, Clone)]
pub struct VersionedRowsetCatalog {
    entries: Vec<Option<RowsetCatalogEntry>>,
    events: Vec<RowsetLayoutEvent>,
    rowset_id_to_entry: HashMap<u64, EntryId>,
    latest_cover: Arc<[EntryId]>,
    latest_rowset_ids: Arc<[u64]>,
    latest_schema_version: Option<u32>,
    latest_schema_version_consistent: bool,
    latest_physical_schema_token: Option<u64>,
    latest_physical_schema_token_consistent: bool,
    latest_gaps: Arc<[VersionGap]>,
    latest_layout_epoch: u64,
    latest_published_ts: i64,
    epoch_fences: Vec<EpochFence>,
}

impl Default for VersionedRowsetCatalog {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            events: Vec::new(),
            rowset_id_to_entry: HashMap::new(),
            latest_cover: Arc::from([]),
            latest_rowset_ids: Arc::from([]),
            latest_schema_version: None,
            latest_schema_version_consistent: true,
            latest_physical_schema_token: None,
            latest_physical_schema_token_consistent: true,
            latest_gaps: Arc::from([]),
            latest_layout_epoch: 0,
            latest_published_ts: 0,
            epoch_fences: Vec::new(),
        }
    }
}

impl VersionedRowsetCatalog {
    pub fn new() -> Self {
        let mut catalog = Self::default();
        catalog.rebuild_fence_at_current();
        catalog
    }

    pub fn rebuild_from_live(
        descriptors: impl IntoIterator<Item = RowsetCatalogDescriptor>,
        layout_epoch: u64,
        latest_published_ts: i64,
    ) -> Result<Self> {
        let mut catalog = Self {
            latest_layout_epoch: layout_epoch,
            latest_published_ts,
            ..Self::default()
        };
        let mut cover = Vec::new();
        for descriptor in descriptors {
            let entry_id = catalog.allocate_entry(descriptor, 0)?;
            cover.push(entry_id);
        }
        catalog.sort_cover(&mut cover);
        catalog.validate_cover(&cover)?;
        catalog.latest_gaps =
            Arc::from(catalog.validate_snapshot_cut(&cover, latest_published_ts)?);
        let (
            latest_rowset_ids,
            latest_schema_version,
            latest_schema_version_consistent,
            latest_physical_schema_token,
            latest_physical_schema_token_consistent,
        ) = catalog.cover_metadata(&cover)?;
        catalog.latest_cover = Arc::from(cover);
        catalog.latest_rowset_ids = latest_rowset_ids;
        catalog.latest_schema_version = latest_schema_version;
        catalog.latest_schema_version_consistent = latest_schema_version_consistent;
        catalog.latest_physical_schema_token = latest_physical_schema_token;
        catalog.latest_physical_schema_token_consistent = latest_physical_schema_token_consistent;
        catalog.epoch_fences.push(EpochFence {
            layout_epoch: 0,
            event_offset: 0,
            cover: catalog.latest_cover.clone(),
        });
        Ok(catalog)
    }

    pub fn latest_layout_epoch(&self) -> u64 {
        self.latest_layout_epoch
    }

    pub fn latest_published_ts(&self) -> i64 {
        self.latest_published_ts
    }

    pub fn entry_id_high_watermark(&self) -> EntryId {
        self.entries
            .len()
            .saturating_sub(1)
            .min(EntryId::MAX as usize) as EntryId
    }

    pub fn should_rewrite_entry_ids(&self) -> bool {
        self.entry_id_high_watermark() >= ENTRY_ID_REWRITE_THRESHOLD
    }

    pub fn entry(&self, entry_id: EntryId) -> Option<&RowsetCatalogEntry> {
        self.entries.get(entry_id as usize)?.as_ref()
    }

    pub fn latest_entry_ids(&self) -> &[EntryId] {
        self.latest_cover.as_ref()
    }

    pub fn latest_rowset_ids(&self) -> Vec<u64> {
        self.latest_rowset_ids.to_vec()
    }

    pub fn entry_for_rowset_id(&self, rowset_id: u64) -> Option<&RowsetCatalogEntry> {
        let entry_id = self.rowset_id_to_entry.get(&rowset_id)?;
        self.entry(*entry_id)
    }

    pub fn checkpoint_slice(&self) -> RowsetCatalogCheckpointSlice {
        RowsetCatalogCheckpointSlice {
            layout_epoch_cut: self.latest_layout_epoch,
            latest_published_ts: self.latest_published_ts,
            entries: self
                .entries
                .iter()
                .filter_map(|entry| entry.as_ref().map(RowsetCatalogCheckpointEntry::from))
                .collect(),
            delete_vector_epochs: self.checkpoint_delete_vector_epochs(self.latest_layout_epoch),
        }
    }

    pub fn checkpoint_slice_for_rowsets(
        &self,
        available_rowset_ids: &HashSet<u64>,
    ) -> Result<RowsetCatalogCheckpointSlice> {
        for entry_id in self.latest_cover.iter().copied() {
            let entry = self.entry(entry_id).ok_or_else(|| {
                paro_error::internal(format!("latest cover references missing entry {entry_id}"))
            })?;
            if !available_rowset_ids.contains(&entry.rowset_id) {
                return Err(paro_error::internal(format!(
                    "checkpoint catalog latest cover references unavailable rowset {}",
                    entry.rowset_id
                )));
            }
        }

        Ok(RowsetCatalogCheckpointSlice {
            layout_epoch_cut: self.latest_layout_epoch,
            latest_published_ts: self.latest_published_ts,
            entries: self
                .entries
                .iter()
                .filter_map(|entry| entry.as_ref())
                .filter(|entry| available_rowset_ids.contains(&entry.rowset_id))
                .map(RowsetCatalogCheckpointEntry::from)
                .collect(),
            delete_vector_epochs: self.checkpoint_delete_vector_epochs(self.latest_layout_epoch),
        })
    }

    pub fn rebuild_from_checkpoint(slice: RowsetCatalogCheckpointSlice) -> Result<Self> {
        let mut entries: Vec<Option<RowsetCatalogEntry>> = Vec::new();
        let mut rowset_id_to_entry = HashMap::with_capacity(slice.entries.len());
        let mut events = Vec::with_capacity(slice.entries.len().saturating_mul(2));

        for checkpoint_entry in slice.entries {
            let entry: RowsetCatalogEntry = checkpoint_entry.into();
            let slot = entry.entry_id as usize;
            if entries.len() <= slot {
                entries.resize_with(slot + 1, || None);
            }
            if entries[slot].is_some() {
                return Err(paro_error::invalid_input(format!(
                    "duplicate checkpoint catalog entry id {}",
                    entry.entry_id
                )));
            }
            if rowset_id_to_entry
                .insert(entry.rowset_id, entry.entry_id)
                .is_some()
            {
                return Err(paro_error::invalid_input(format!(
                    "duplicate checkpoint catalog rowset {}",
                    entry.rowset_id
                )));
            }
            events.push(RowsetLayoutEvent::Install {
                epoch: entry.installed_at_epoch,
                entry_id: entry.entry_id,
            });
            if let Some(retired_at_epoch) = entry.retired_at_epoch {
                events.push(RowsetLayoutEvent::Retire {
                    epoch: retired_at_epoch,
                    entry_id: entry.entry_id,
                });
            }
            entries[slot] = Some(entry);
        }

        for epoch in slice.delete_vector_epochs {
            if epoch > slice.layout_epoch_cut {
                return Err(paro_error::invalid_input(format!(
                    "checkpoint delete-vector layout epoch {epoch} exceeds cut {}",
                    slice.layout_epoch_cut
                )));
            }
            events.push(RowsetLayoutEvent::DeleteVector { epoch });
        }

        events.sort_by_key(|event| match *event {
            RowsetLayoutEvent::Retire { epoch, entry_id } => (epoch, 0u8, entry_id),
            RowsetLayoutEvent::Install { epoch, entry_id } => (epoch, 1u8, entry_id),
            RowsetLayoutEvent::DeleteVector { epoch } => (epoch, 2u8, EntryId::MAX),
        });

        let mut catalog = Self {
            entries,
            events,
            rowset_id_to_entry,
            latest_layout_epoch: slice.layout_epoch_cut,
            latest_published_ts: slice.latest_published_ts,
            ..Self::default()
        };

        let mut latest_cover = catalog
            .entries
            .iter()
            .filter_map(|entry| entry.as_ref())
            .filter(|entry| entry.is_active_at(slice.layout_epoch_cut))
            .map(|entry| entry.entry_id)
            .collect::<Vec<_>>();
        catalog.sort_cover(&mut latest_cover);
        catalog.validate_cover(&latest_cover)?;
        let latest_gaps =
            catalog.validate_snapshot_cut(&latest_cover, slice.latest_published_ts)?;
        let (
            latest_rowset_ids,
            latest_schema_version,
            latest_schema_version_consistent,
            latest_physical_schema_token,
            latest_physical_schema_token_consistent,
        ) = catalog.cover_metadata(&latest_cover)?;
        catalog.latest_cover = Arc::from(latest_cover);
        catalog.latest_rowset_ids = latest_rowset_ids;
        catalog.latest_schema_version = latest_schema_version;
        catalog.latest_schema_version_consistent = latest_schema_version_consistent;
        catalog.latest_physical_schema_token = latest_physical_schema_token;
        catalog.latest_physical_schema_token_consistent = latest_physical_schema_token_consistent;
        catalog.latest_gaps = Arc::from(latest_gaps);
        catalog.rebuild_epoch_fences()?;
        Ok(catalog)
    }

    pub fn publish_rowset(
        &mut self,
        descriptor: RowsetCatalogDescriptor,
        layout_epoch: u64,
        latest_published_ts: i64,
    ) -> Result<EntryId> {
        if layout_epoch <= self.latest_layout_epoch {
            return Err(paro_error::invalid_input(format!(
                "layout epoch must increase: {} -> {}",
                self.latest_layout_epoch, layout_epoch
            )));
        }
        if self.rowset_id_to_entry.contains_key(&descriptor.rowset_id) {
            return Err(paro_error::invalid_input(format!(
                "duplicate rowset catalog entry for rowset {}",
                descriptor.rowset_id
            )));
        }

        let mut next_cover = self.latest_cover.to_vec();
        let entry_id = self.allocate_entry(descriptor, layout_epoch)?;
        next_cover.push(entry_id);
        self.sort_cover(&mut next_cover);
        self.validate_cover(&next_cover)?;
        let latest_gaps = self.validate_snapshot_cut(&next_cover, latest_published_ts)?;
        let (
            latest_rowset_ids,
            latest_schema_version,
            latest_schema_version_consistent,
            latest_physical_schema_token,
            latest_physical_schema_token_consistent,
        ) = self.cover_metadata(&next_cover)?;

        self.events.push(RowsetLayoutEvent::Install {
            epoch: layout_epoch,
            entry_id,
        });
        self.latest_cover = Arc::from(next_cover);
        self.latest_rowset_ids = latest_rowset_ids;
        self.latest_schema_version = latest_schema_version;
        self.latest_schema_version_consistent = latest_schema_version_consistent;
        self.latest_physical_schema_token = latest_physical_schema_token;
        self.latest_physical_schema_token_consistent = latest_physical_schema_token_consistent;
        self.latest_gaps = Arc::from(latest_gaps);
        self.latest_layout_epoch = layout_epoch;
        self.latest_published_ts = latest_published_ts;
        self.maybe_build_fence();
        Ok(entry_id)
    }

    pub fn publish_delete_vector(
        &mut self,
        layout_epoch: u64,
        latest_published_ts: i64,
    ) -> Result<()> {
        if layout_epoch <= self.latest_layout_epoch {
            return Err(paro_error::invalid_input(format!(
                "layout epoch must increase: {} -> {}",
                self.latest_layout_epoch, layout_epoch
            )));
        }
        let latest_gaps =
            self.validate_snapshot_cut(self.latest_cover.as_ref(), latest_published_ts)?;
        self.events.push(RowsetLayoutEvent::DeleteVector {
            epoch: layout_epoch,
        });
        self.latest_gaps = Arc::from(latest_gaps);
        self.latest_layout_epoch = layout_epoch;
        self.latest_published_ts = latest_published_ts;
        self.maybe_build_fence();
        Ok(())
    }

    pub fn validate_compaction_publish(
        &self,
        input_rowset_ids: &[u64],
        output_version: Version,
    ) -> Result<()> {
        if input_rowset_ids.is_empty() {
            return Err(paro_error::invalid_input(
                "compaction publish requires at least one input",
            ));
        }
        let input_entry_ids = self.input_entry_ids(input_rowset_ids)?;
        self.validate_compaction_output_span(&input_entry_ids, output_version)?;
        let input_set: HashSet<EntryId> = input_entry_ids.iter().copied().collect();
        let next_cover: Vec<EntryId> = self
            .latest_cover
            .iter()
            .copied()
            .filter(|entry_id| !input_set.contains(entry_id))
            .collect();
        if next_cover.len() + input_entry_ids.len() != self.latest_cover.len() {
            return Err(paro_error::serialization_failure(
                "compaction inputs no longer match latest cover",
            ));
        }
        Ok(())
    }

    pub fn publish_compaction(
        &mut self,
        input_rowset_ids: &[u64],
        output: RowsetCatalogDescriptor,
        layout_epoch: u64,
        latest_published_ts: i64,
    ) -> Result<EntryId> {
        if layout_epoch <= self.latest_layout_epoch {
            return Err(paro_error::invalid_input(format!(
                "layout epoch must increase: {} -> {}",
                self.latest_layout_epoch, layout_epoch
            )));
        }
        if self.rowset_id_to_entry.contains_key(&output.rowset_id) {
            return Err(paro_error::invalid_input(format!(
                "duplicate compaction output rowset {}",
                output.rowset_id
            )));
        }

        self.validate_compaction_publish(input_rowset_ids, output.version)?;
        let input_entry_ids = self.input_entry_ids(input_rowset_ids)?;

        let input_set: HashSet<EntryId> = input_entry_ids.iter().copied().collect();
        let mut next_cover: Vec<EntryId> = self
            .latest_cover
            .iter()
            .copied()
            .filter(|entry_id| !input_set.contains(entry_id))
            .collect();
        if next_cover.len() + input_entry_ids.len() != self.latest_cover.len() {
            return Err(paro_error::serialization_failure(
                "compaction inputs no longer match latest cover",
            ));
        }

        let output_entry_id = self.allocate_entry(output, layout_epoch)?;
        next_cover.push(output_entry_id);
        self.sort_cover(&mut next_cover);
        self.validate_cover(&next_cover)?;
        let latest_gaps = self.validate_snapshot_cut(&next_cover, latest_published_ts)?;
        let (
            latest_rowset_ids,
            latest_schema_version,
            latest_schema_version_consistent,
            latest_physical_schema_token,
            latest_physical_schema_token_consistent,
        ) = self.cover_metadata(&next_cover)?;

        for entry_id in input_entry_ids {
            let entry = self.entry_mut(entry_id)?;
            entry.retired_at_epoch = Some(layout_epoch);
            self.events.push(RowsetLayoutEvent::Retire {
                epoch: layout_epoch,
                entry_id,
            });
        }
        self.events.push(RowsetLayoutEvent::Install {
            epoch: layout_epoch,
            entry_id: output_entry_id,
        });

        self.latest_cover = Arc::from(next_cover);
        self.latest_rowset_ids = latest_rowset_ids;
        self.latest_schema_version = latest_schema_version;
        self.latest_schema_version_consistent = latest_schema_version_consistent;
        self.latest_physical_schema_token = latest_physical_schema_token;
        self.latest_physical_schema_token_consistent = latest_physical_schema_token_consistent;
        self.latest_gaps = Arc::from(latest_gaps);
        self.latest_layout_epoch = layout_epoch;
        self.latest_published_ts = latest_published_ts;
        self.maybe_build_fence();
        Ok(output_entry_id)
    }

    pub fn capture_entry_ids(&self, read_ts: i64, layout_epoch: u64) -> Result<SnapshotCut> {
        let layout_epoch = layout_epoch.min(self.latest_layout_epoch);
        if layout_epoch == self.latest_layout_epoch && read_ts >= self.latest_published_ts {
            return Ok(SnapshotCut {
                entry_ids: self.latest_cover.to_vec(),
                rowset_ids: self.latest_rowset_ids.to_vec(),
                schema_version: self.latest_schema_version,
                schema_version_consistent: self.latest_schema_version_consistent,
                physical_schema_token: self.latest_physical_schema_token,
                physical_schema_token_consistent: self.latest_physical_schema_token_consistent,
                gaps: self.latest_gaps_for_read_ts(read_ts),
            });
        }
        let cover = if layout_epoch == self.latest_layout_epoch {
            self.latest_cover.to_vec()
        } else {
            self.cover_at_layout(layout_epoch)?
        };
        let mut entry_ids = Vec::with_capacity(cover.len());
        let mut rowset_ids = Vec::with_capacity(cover.len());
        let mut schema_version = None;
        let mut schema_version_consistent = true;
        let mut physical_schema_token = None;
        let mut physical_schema_token_consistent = true;
        for entry_id in cover {
            let entry = self.entry(entry_id).ok_or_else(|| {
                paro_error::internal(format!("catalog fence references missing entry {entry_id}"))
            })?;
            if entry.version_end() <= read_ts && entry.is_active_at(layout_epoch) {
                entry_ids.push(entry_id);
                rowset_ids.push(entry.rowset_id);
                schema_version_consistent &=
                    Self::merge_schema_version(&mut schema_version, entry.schema_version);
                physical_schema_token_consistent &= Self::merge_physical_schema_token(
                    &mut physical_schema_token,
                    entry.physical_schema_token,
                );
            }
        }
        let gaps = self.validate_snapshot_cut(&entry_ids, read_ts)?;
        Ok(SnapshotCut {
            entry_ids,
            rowset_ids,
            schema_version,
            schema_version_consistent,
            physical_schema_token,
            physical_schema_token_consistent,
            gaps,
        })
    }

    pub fn detect_version_gaps(&self, read_ts: i64, layout_epoch: u64) -> Result<Vec<VersionGap>> {
        Ok(self.capture_entry_ids(read_ts, layout_epoch)?.gaps)
    }

    fn cover_metadata(
        &self,
        cover: &[EntryId],
    ) -> Result<(Arc<[u64]>, Option<u32>, bool, Option<u64>, bool)> {
        let mut rowset_ids = Vec::with_capacity(cover.len());
        let mut schema_version = None;
        let mut schema_version_consistent = true;
        let mut physical_schema_token = None;
        let mut physical_schema_token_consistent = true;
        for &entry_id in cover {
            let entry = self.entry(entry_id).ok_or_else(|| {
                paro_error::internal(format!("catalog cover references missing entry {entry_id}"))
            })?;
            rowset_ids.push(entry.rowset_id);
            schema_version_consistent &=
                Self::merge_schema_version(&mut schema_version, entry.schema_version);
            physical_schema_token_consistent &= Self::merge_physical_schema_token(
                &mut physical_schema_token,
                entry.physical_schema_token,
            );
        }
        Ok((
            Arc::from(rowset_ids),
            schema_version,
            schema_version_consistent,
            physical_schema_token,
            physical_schema_token_consistent,
        ))
    }

    fn merge_schema_version(current: &mut Option<u32>, candidate: u32) -> bool {
        match *current {
            Some(schema_version) if schema_version != candidate => false,
            None => {
                *current = Some(candidate);
                true
            }
            Some(_) => true,
        }
    }

    fn merge_physical_schema_token(current: &mut Option<u64>, candidate: u64) -> bool {
        match *current {
            Some(token) if token != candidate => false,
            None => {
                *current = Some(candidate);
                true
            }
            Some(_) => true,
        }
    }

    pub fn validate_latest(&self) -> Result<()> {
        self.validate_cover(self.latest_cover.as_ref())?;
        let mut seen_events = HashSet::with_capacity(self.events.len());
        for event in &self.events {
            match *event {
                RowsetLayoutEvent::Install { entry_id, .. }
                | RowsetLayoutEvent::Retire { entry_id, .. } => {
                    if self.entry(entry_id).is_none() {
                        return Err(paro_error::internal(format!(
                            "layout event references missing entry {entry_id}"
                        )));
                    }
                    seen_events.insert(entry_id);
                }
                RowsetLayoutEvent::DeleteVector { .. } => {}
            }
        }
        Ok(())
    }

    pub fn assert_live_map_parity<I>(&self, live: I)
    where
        I: IntoIterator<Item = (u64, Version)>,
    {
        if !cfg!(debug_assertions) {
            return;
        }
        let mut from_catalog = self
            .latest_cover
            .iter()
            .filter_map(|&entry_id| {
                self.entry(entry_id)
                    .map(|entry| (entry.rowset_id, entry.version))
            })
            .collect::<Vec<_>>();
        let mut from_live = live.into_iter().collect::<Vec<_>>();
        from_catalog.sort_unstable_by_key(|(rowset_id, version)| (*rowset_id, *version));
        from_live.sort_unstable_by_key(|(rowset_id, version)| (*rowset_id, *version));
        debug_assert_eq!(from_catalog, from_live, "rowset catalog/live map drift");
    }

    fn allocate_entry(
        &mut self,
        descriptor: RowsetCatalogDescriptor,
        installed_at_epoch: u64,
    ) -> Result<EntryId> {
        let slot = self.entries.len();
        if slot > EntryId::MAX as usize {
            return Err(paro_error::internal(
                "rowset catalog entry id exhausted; full rewrite required",
            ));
        }
        let entry_id = slot as EntryId;
        let entry = RowsetCatalogEntry {
            entry_id,
            rowset_id: descriptor.rowset_id,
            version: descriptor.version,
            installed_at_epoch,
            retired_at_epoch: None,
            schema_version: descriptor.schema_version,
            physical_schema_token: descriptor.physical_schema_token,
            delete_vector_catalog_token: descriptor.delete_vector_catalog_token,
            artifact_id: descriptor.artifact_id,
            flags: descriptor.flags,
            cold_meta_id: descriptor.cold_meta_id,
        };
        self.entries.push(Some(entry));
        self.rowset_id_to_entry
            .insert(descriptor.rowset_id, entry_id);
        Ok(entry_id)
    }

    fn input_entry_ids(&self, input_rowset_ids: &[u64]) -> Result<Vec<EntryId>> {
        let mut ids = Vec::with_capacity(input_rowset_ids.len());
        let mut seen = HashSet::with_capacity(input_rowset_ids.len());
        for rowset_id in input_rowset_ids {
            if !seen.insert(*rowset_id) {
                return Err(paro_error::invalid_input(format!(
                    "duplicate compaction input rowset {rowset_id}"
                )));
            }
            let Some(&entry_id) = self.rowset_id_to_entry.get(rowset_id) else {
                return Err(paro_error::serialization_failure(format!(
                    "compaction input rowset {rowset_id} is missing from catalog"
                )));
            };
            let entry = self
                .entry(entry_id)
                .ok_or_else(|| paro_error::internal(format!("catalog entry {entry_id} missing")))?;
            if entry.retired_at_epoch.is_some() || !self.latest_cover.contains(&entry_id) {
                return Err(paro_error::serialization_failure(format!(
                    "compaction input rowset {rowset_id} is no longer active"
                )));
            }
            ids.push(entry_id);
        }
        Ok(ids)
    }

    fn validate_compaction_output_span(
        &self,
        input_entry_ids: &[EntryId],
        output_version: Version,
    ) -> Result<()> {
        let mut input_versions = input_entry_ids
            .iter()
            .map(|entry_id| {
                self.entry(*entry_id)
                    .map(|entry| entry.version)
                    .ok_or_else(|| paro_error::internal("missing compaction input entry"))
            })
            .collect::<Result<Vec<_>>>()?;
        input_versions.sort_unstable();

        let expected_start = input_versions
            .iter()
            .map(|version| version.start)
            .min()
            .unwrap_or(output_version.start);
        let expected_end = input_versions
            .iter()
            .map(|version| version.end)
            .max()
            .unwrap_or(output_version.end);

        let expected = Version::new(expected_start, expected_end);
        if output_version != expected {
            return Err(paro_error::serialization_failure(format!(
                "compaction output span {} does not match input cover {}",
                output_version, expected
            )));
        }
        Ok(())
    }

    fn entry_mut(&mut self, entry_id: EntryId) -> Result<&mut RowsetCatalogEntry> {
        self.entries
            .get_mut(entry_id as usize)
            .and_then(Option::as_mut)
            .ok_or_else(|| paro_error::internal(format!("missing catalog entry {entry_id}")))
    }

    fn cover_at_layout(&self, layout_epoch: u64) -> Result<Vec<EntryId>> {
        let target_offset = self
            .events
            .partition_point(|event| event.epoch() <= layout_epoch);
        let fence_index = self
            .epoch_fences
            .partition_point(|fence| fence.layout_epoch <= layout_epoch)
            .saturating_sub(1);
        let fence = self
            .epoch_fences
            .get(fence_index)
            .ok_or_else(|| paro_error::internal("rowset catalog has no epoch fence"))?;
        if target_offset < fence.event_offset as usize {
            return Err(paro_error::internal(
                "rowset catalog fence offset is ahead of target event offset",
            ));
        }
        let delta = target_offset - fence.event_offset as usize;
        if delta > DEFAULT_FENCE_EVENT_INTERVAL {
            return Err(paro_error::internal(format!(
                "catalog history delta {delta} exceeds fence budget"
            )));
        }

        let mut cover_set: HashSet<EntryId> = fence.cover.iter().copied().collect();
        for event in &self.events[fence.event_offset as usize..target_offset] {
            match *event {
                RowsetLayoutEvent::Install { entry_id, .. } => {
                    cover_set.insert(entry_id);
                }
                RowsetLayoutEvent::Retire { entry_id, .. } => {
                    cover_set.remove(&entry_id);
                }
                RowsetLayoutEvent::DeleteVector { .. } => {}
            }
        }
        let mut cover: Vec<_> = cover_set.into_iter().collect();
        self.sort_cover(&mut cover);
        Ok(cover)
    }

    fn validate_cover(&self, cover: &[EntryId]) -> Result<()> {
        let mut versions = Vec::with_capacity(cover.len());
        let mut seen = HashSet::with_capacity(cover.len());
        for &entry_id in cover {
            if !seen.insert(entry_id) {
                return Err(paro_error::invalid_input(format!(
                    "duplicate catalog entry {entry_id} in cover"
                )));
            }
            let entry = self.entry(entry_id).ok_or_else(|| {
                paro_error::internal(format!("catalog cover references missing entry {entry_id}"))
            })?;
            versions.push((entry.rowset_id, entry.version));
        }
        versions.sort_unstable_by_key(|(_, version)| *version);
        for pair in versions.windows(2) {
            let (left_id, left) = pair[0];
            let (right_id, right) = pair[1];
            if left.overlaps(&right) {
                return Err(paro_error::invalid_input(format!(
                    "invalid catalog cover: overlap between rowset {left_id} {left} and rowset {right_id} {right}"
                )));
            }
        }
        Ok(())
    }

    fn validate_snapshot_cut(
        &self,
        entry_ids: &[EntryId],
        read_ts: i64,
    ) -> Result<Vec<VersionGap>> {
        self.validate_cover(entry_ids)?;
        if read_ts < 0 {
            return Ok(Vec::new());
        }

        let mut versions = entry_ids
            .iter()
            .filter_map(|entry_id| self.entry(*entry_id).map(|entry| entry.version))
            .filter(|version| version.end >= 0)
            .collect::<Vec<_>>();
        versions.sort_unstable();

        let mut gaps = Vec::new();
        let mut next_expected = 0i64;
        for version in versions {
            let start = version.start.max(0);
            let end = version.end.min(read_ts);
            if end < 0 || start > read_ts {
                continue;
            }
            if start > next_expected {
                gaps.push(VersionGap {
                    missing_start: next_expected,
                    missing_end: start - 1,
                });
            }
            next_expected = next_expected.max(end.saturating_add(1));
            if next_expected > read_ts {
                break;
            }
        }
        if next_expected <= read_ts {
            gaps.push(VersionGap {
                missing_start: next_expected,
                missing_end: read_ts,
            });
        }
        Ok(gaps)
    }

    fn latest_gaps_for_read_ts(&self, read_ts: i64) -> Vec<VersionGap> {
        let mut gaps = self.latest_gaps.to_vec();
        if read_ts > self.latest_published_ts {
            let missing_start = self.latest_published_ts.saturating_add(1).max(0);
            if missing_start <= read_ts {
                gaps.push(VersionGap {
                    missing_start,
                    missing_end: read_ts,
                });
            }
        }
        gaps
    }

    fn sort_cover(&self, cover: &mut [EntryId]) {
        cover.sort_unstable_by_key(|entry_id| {
            self.entry(*entry_id)
                .map(|entry| (entry.version.start, entry.version.end, entry.rowset_id))
                .unwrap_or((i64::MAX, i64::MAX, u64::MAX))
        });
    }

    fn checkpoint_delete_vector_epochs(&self, layout_epoch_cut: u64) -> Vec<u64> {
        self.events
            .iter()
            .filter_map(|event| match *event {
                RowsetLayoutEvent::DeleteVector { epoch } if epoch <= layout_epoch_cut => {
                    Some(epoch)
                }
                _ => None,
            })
            .collect()
    }

    #[inline]
    fn should_build_epoch_fence(events_since_last: usize, cover_len: usize) -> bool {
        events_since_last >= DEFAULT_FENCE_EVENT_INTERVAL
            || events_since_last > EARLY_FENCE_MIN_EVENTS.max(cover_len / 8)
    }

    fn maybe_build_fence(&mut self) {
        let should_build = match self.epoch_fences.last() {
            None => true,
            Some(last) => {
                let events_since_last = self.events.len() - last.event_offset as usize;
                Self::should_build_epoch_fence(events_since_last, self.latest_cover.len())
            }
        };
        if should_build {
            self.rebuild_fence_at_current();
        }
    }

    fn rebuild_fence_at_current(&mut self) {
        self.epoch_fences.push(EpochFence {
            layout_epoch: self.latest_layout_epoch,
            event_offset: self.events.len() as u32,
            cover: self.latest_cover.clone(),
        });
    }

    fn rebuild_epoch_fences(&mut self) -> Result<()> {
        self.epoch_fences.clear();
        let mut cover = Vec::new();
        self.epoch_fences.push(EpochFence {
            layout_epoch: 0,
            event_offset: 0,
            cover: Arc::from([]),
        });

        let mut last_fence_offset = 0usize;
        for offset in 0..self.events.len() {
            match self.events[offset] {
                RowsetLayoutEvent::Install { entry_id, .. } => {
                    if !cover.contains(&entry_id) {
                        cover.push(entry_id);
                    }
                }
                RowsetLayoutEvent::Retire { entry_id, .. } => {
                    cover.retain(|candidate| *candidate != entry_id);
                }
                RowsetLayoutEvent::DeleteVector { .. } => {}
            }
            self.sort_cover(&mut cover);

            let event_offset = offset + 1;
            let events_since_last = event_offset - last_fence_offset;
            let should_build = Self::should_build_epoch_fence(events_since_last, cover.len());
            if should_build {
                self.validate_cover(&cover)?;
                self.epoch_fences.push(EpochFence {
                    layout_epoch: self.events[offset].epoch(),
                    event_offset: event_offset as u32,
                    cover: Arc::from(cover.clone()),
                });
                last_fence_offset = event_offset;
            }
        }

        if self
            .epoch_fences
            .last()
            .is_none_or(|fence| fence.layout_epoch != self.latest_layout_epoch)
        {
            self.epoch_fences.push(EpochFence {
                layout_epoch: self.latest_layout_epoch,
                event_offset: self.events.len() as u32,
                cover: self.latest_cover.clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(rowset_id: u64, version: Version) -> RowsetCatalogDescriptor {
        RowsetCatalogDescriptor {
            rowset_id,
            version,
            schema_version: 1,
            physical_schema_token: 7,
            delete_vector_catalog_token: 0,
            artifact_id: rowset_id,
            flags: RowsetCatalogFlags::empty(),
            cold_meta_id: 0,
        }
    }

    #[test]
    fn latest_fast_path_filters_by_read_ts() {
        let mut catalog = VersionedRowsetCatalog::new();
        catalog
            .publish_rowset(descriptor(10, Version::singleton(0)), 1, 0)
            .unwrap();
        catalog
            .publish_rowset(descriptor(11, Version::singleton(1)), 2, 1)
            .unwrap();

        let cut = catalog
            .capture_entry_ids(0, catalog.latest_layout_epoch())
            .unwrap();
        let rowset_ids: Vec<_> = cut
            .entry_ids
            .iter()
            .map(|id| catalog.entry(*id).unwrap().rowset_id)
            .collect();
        assert_eq!(rowset_ids, vec![10]);
        assert!(cut.gaps.is_empty());
    }

    #[test]
    fn latest_fast_path_preserves_gap_reporting() {
        let mut catalog = VersionedRowsetCatalog::new();
        catalog
            .publish_rowset(descriptor(10, Version::singleton(0)), 1, 0)
            .unwrap();
        catalog
            .publish_rowset(descriptor(11, Version::singleton(2)), 2, 2)
            .unwrap();

        let cut = catalog
            .capture_entry_ids(2, catalog.latest_layout_epoch())
            .unwrap();
        assert_eq!(
            cut.gaps,
            vec![VersionGap {
                missing_start: 1,
                missing_end: 1,
            }]
        );

        let future_cut = catalog
            .capture_entry_ids(4, catalog.latest_layout_epoch())
            .unwrap();
        assert_eq!(
            future_cut.gaps,
            vec![
                VersionGap {
                    missing_start: 1,
                    missing_end: 1,
                },
                VersionGap {
                    missing_start: 3,
                    missing_end: 4,
                },
            ]
        );
    }

    #[test]
    fn history_cut_uses_layout_epoch_before_compaction() {
        let mut catalog = VersionedRowsetCatalog::new();
        catalog
            .publish_rowset(descriptor(10, Version::singleton(0)), 1, 0)
            .unwrap();
        catalog
            .publish_rowset(descriptor(11, Version::singleton(1)), 2, 1)
            .unwrap();
        catalog
            .publish_compaction(
                &[10, 11],
                RowsetCatalogDescriptor {
                    flags: RowsetCatalogFlags::COMPACTION_OUTPUT,
                    ..descriptor(20, Version::new(0, 1))
                },
                3,
                1,
            )
            .unwrap();

        let cut = catalog.capture_entry_ids(1, 2).unwrap();
        let rowset_ids: Vec<_> = cut
            .entry_ids
            .iter()
            .map(|id| catalog.entry(*id).unwrap().rowset_id)
            .collect();
        assert_eq!(rowset_ids, vec![10, 11]);

        let latest = catalog.capture_entry_ids(1, 3).unwrap();
        let latest_ids: Vec<_> = latest
            .entry_ids
            .iter()
            .map(|id| catalog.entry(*id).unwrap().rowset_id)
            .collect();
        assert_eq!(latest_ids, vec![20]);
    }

    #[test]
    fn compaction_allows_commit_gaps_but_requires_min_max_span() {
        let mut catalog = VersionedRowsetCatalog::new();
        catalog
            .publish_rowset(descriptor(10, Version::singleton(0)), 1, 0)
            .unwrap();
        catalog
            .publish_rowset(descriptor(11, Version::singleton(2)), 2, 2)
            .unwrap();

        let err = catalog
            .publish_compaction(&[10, 11], descriptor(20, Version::new(0, 1)), 3, 2)
            .unwrap_err();
        assert!(format!("{err}").contains("does not match input cover"));

        catalog
            .publish_compaction(&[10, 11], descriptor(20, Version::new(0, 2)), 3, 2)
            .unwrap();
    }

    #[test]
    fn delete_vector_publish_advances_layout_without_changing_cover() {
        let mut catalog = VersionedRowsetCatalog::new();
        catalog
            .publish_rowset(descriptor(10, Version::singleton(0)), 1, 0)
            .unwrap();
        let before = catalog.latest_rowset_ids();
        catalog.publish_delete_vector(2, 1).unwrap();
        assert_eq!(catalog.latest_layout_epoch(), 2);
        assert_eq!(catalog.latest_published_ts(), 1);
        assert_eq!(catalog.latest_rowset_ids(), before);

        let checkpoint = catalog.checkpoint_slice();
        assert_eq!(checkpoint.delete_vector_epochs, vec![2]);
        let restored = VersionedRowsetCatalog::rebuild_from_checkpoint(checkpoint).unwrap();
        assert!(matches!(
            restored.events.last(),
            Some(RowsetLayoutEvent::DeleteVector { epoch: 2 })
        ));
        assert_eq!(restored.latest_layout_epoch(), 2);
        assert_eq!(restored.latest_rowset_ids(), before);
    }

    #[test]
    fn checkpoint_slice_roundtrips_compaction_history() {
        let mut catalog = VersionedRowsetCatalog::new();
        catalog
            .publish_rowset(descriptor(10, Version::singleton(0)), 1, 0)
            .unwrap();
        catalog
            .publish_rowset(descriptor(11, Version::singleton(1)), 2, 1)
            .unwrap();
        catalog
            .publish_compaction(
                &[10, 11],
                RowsetCatalogDescriptor {
                    flags: RowsetCatalogFlags::COMPACTION_OUTPUT,
                    ..descriptor(20, Version::new(0, 1))
                },
                3,
                1,
            )
            .unwrap();

        let restored =
            VersionedRowsetCatalog::rebuild_from_checkpoint(catalog.checkpoint_slice()).unwrap();

        assert_eq!(restored.latest_layout_epoch(), 3);
        assert_eq!(restored.latest_rowset_ids(), vec![20]);
        let history = restored.capture_entry_ids(1, 2).unwrap();
        assert_eq!(history.rowset_ids, vec![10, 11]);
        let latest = restored.capture_entry_ids(1, 3).unwrap();
        assert_eq!(latest.rowset_ids, vec![20]);
    }
}
