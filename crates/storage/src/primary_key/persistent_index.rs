// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! PersistentIndex - on-disk primary index with current-format WAL + immutable L1/L2 files.

use crate::metrics::storage_metrics;
use crate::primary_key::{
    ImmutableIndexReader, ImmutableIndexStats, ImmutableIndexWriter, PrimaryIndex,
    PrimaryIndexVersion, PrimaryKeyWriteConflict, RowID, NULL_ROW_ID,
};
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub const PERSISTENT_INDEX_FORMAT_VERSION: u32 = 6;

const WAL_MAGIC: [u8; 4] = *b"PIWL";
const FILE_HEADER_LEN: usize = 8;
const VALUE_LEN: usize = 16;
const MINOR_COMPACTION_THRESHOLD: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileHeaderStatus {
    Empty,
    Current,
    Legacy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ImmutableIndexLevel {
    L1,
    L2,
}

impl ImmutableIndexLevel {
    const fn file_prefix(self) -> &'static str {
        match self {
            Self::L1 => "l1",
            Self::L2 => "l2",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ImmutableFileMeta {
    file_id: u64,
    level: ImmutableIndexLevel,
    edit_version: u64,
    entry_count: u64,
}

/// Exact durable state from which a primary-index cache was derived.
///
/// A persistent primary index is a rebuildable cache, not an authority.  It
/// may only be reused when this provenance is identical to the tablet state
/// restored from a checkpoint.  In particular, entry cardinality is not a
/// correctness proof: an upsert can preserve cardinality while moving a key to
/// a different physical row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryIndexProvenance {
    pub tablet_id: u64,
    pub indexed_through_version: i64,
    pub layout_epoch: u64,
    pub schema_epoch: Option<u64>,
    pub schema_hash: u32,
    pub rowset_root: Vec<PrimaryIndexRowsetRoot>,
}

/// One member of the exact visible-rowset root covered by a cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryIndexRowsetRoot {
    pub rowset_id: u64,
    pub start_version: i64,
    pub end_version: i64,
    pub num_segments: u32,
    pub effective_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format_version: u32,
    active_wal_id: u64,
    next_file_id: u64,
    edit_version: u64,
    applied_lsn: u64,
    provenance: Option<PrimaryIndexProvenance>,
    l1_files: Vec<ImmutableFileMeta>,
    l2_file: Option<ImmutableFileMeta>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            format_version: default_manifest_format_version(),
            active_wal_id: 0,
            next_file_id: default_next_file_id(),
            edit_version: 0,
            applied_lsn: 0,
            provenance: None,
            l1_files: Vec::new(),
            l2_file: None,
        }
    }
}

fn default_manifest_format_version() -> u32 {
    PERSISTENT_INDEX_FORMAT_VERSION
}

fn default_next_file_id() -> u64 {
    1
}

#[derive(Debug)]
pub struct PersistentIndex {
    root_dir: PathBuf,
    manifest_path: PathBuf,
    active_wal_id: u64,
    next_file_id: u64,
    edit_version: u64,
    applied_lsn: u64,
    provenance: Option<PrimaryIndexProvenance>,
    l1_files: Vec<ImmutableFileMeta>,
    l2_file: Option<ImmutableFileMeta>,
    /// Single-flight owner for a prepared L1/L2 merge. Preparation reserves
    /// an output id while the expensive immutable-file merge runs without the
    /// tablet's PersistentIndex write lock.
    compaction_in_progress: bool,
    /// The manifest-defined mutable tier, parsed once at open and updated in
    /// the same critical section as its WAL append. Point and batch lookups
    /// must never rediscover this state by scanning or rereading the directory.
    active_wal_index: Arc<PrimaryIndex>,
    wal_writer: Mutex<File>,
    /// Immutable readers pinned for exactly the current manifest generation.
    /// Replaced only while the tablet owns the outer PersistentIndex write
    /// guard, so query lookups are filesystem-free after open/publication.
    immutable_readers: Vec<Arc<ImmutableIndexReader>>,
}

/// Immutable primary-index compaction input captured under the tablet's short
/// publication lock. The readers pin the exact source generation while the
/// merge performs filesystem I/O outside foreground lookup and flush locks.
pub(crate) struct PersistentIndexCompactionPlan {
    selected_l1_file_ids: Vec<u64>,
    expected_l2_file_id: Option<u64>,
    readers: Vec<Arc<ImmutableIndexReader>>,
    output_file_id: u64,
    staging_path: PathBuf,
    final_path: PathBuf,
}

pub(crate) struct PersistentIndexCompactionOutput {
    stats: ImmutableIndexStats,
}

impl PersistentIndex {
    /// Open a rebuildable tablet-local cache. A malformed manifest must not
    /// make the authoritative rowsets unreadable; discard that cache and let
    /// the normal provenance-aware recovery path reconstruct it from rowsets.
    pub fn open_rebuildable(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref();
        match Self::new(root_dir) {
            Ok(index) => Ok(index),
            Err(error) => {
                tracing::warn!(
                    path = %root_dir.display(),
                    error = %error,
                    "discarding unreadable rebuildable primary-index cache"
                );
                if root_dir.exists() {
                    fs::remove_dir_all(root_dir).map_err(|remove_error| {
                        paro_error::io_error(format!(
                            "discard unreadable persistent index dir {:?}: {} (open error: {})",
                            root_dir, remove_error, error
                        ))
                    })?;
                }
                Self::new(root_dir)
            }
        }
    }

    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir).map_err(|e| {
            paro_error::io_error(format!("create persistent index dir {:?}: {}", root_dir, e))
        })?;
        let manifest_path = root_dir.join("primary_index.manifest");
        let manifest = Self::read_manifest(&manifest_path)?.unwrap_or_default();
        if manifest.format_version != PERSISTENT_INDEX_FORMAT_VERSION {
            return Err(paro_error::data_corrupted(format!(
                "unsupported persistent index manifest version {}",
                manifest.format_version
            )));
        }
        if manifest
            .l1_files
            .iter()
            .any(|meta| meta.level != ImmutableIndexLevel::L1)
            || manifest
                .l2_file
                .iter()
                .any(|meta| meta.level != ImmutableIndexLevel::L2)
        {
            return Err(paro_error::data_corrupted(
                "persistent index manifest level metadata is inconsistent",
            ));
        }

        let max_known_file_id = manifest
            .l1_files
            .iter()
            .map(|meta| meta.file_id)
            .chain(manifest.l2_file.iter().map(|meta| meta.file_id))
            .max()
            .unwrap_or(0);

        let active_wal_path = root_dir.join(format!("wal_{}.wal", manifest.active_wal_id));
        let mut wal_writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_wal_path)
            .map_err(|e| paro_error::io_error(format!("open wal {:?}: {}", active_wal_path, e)))?;
        Self::ensure_file_header(&mut wal_writer)?;
        wal_writer
            .flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", active_wal_path, e)))?;
        let active_wal_index = Arc::new(PrimaryIndex::new());
        active_wal_index.batch_apply_versions(Self::read_wal_records_path(&active_wal_path)?);

        let mut index = Self {
            root_dir,
            manifest_path,
            active_wal_id: manifest.active_wal_id,
            next_file_id: manifest
                .next_file_id
                .max(max_known_file_id.saturating_add(1))
                .max(1),
            edit_version: manifest
                .edit_version
                .max(
                    manifest
                        .l1_files
                        .iter()
                        .map(|meta| meta.edit_version)
                        .max()
                        .unwrap_or(0),
                )
                .max(
                    manifest
                        .l2_file
                        .as_ref()
                        .map(|meta| meta.edit_version)
                        .unwrap_or(0),
                ),
            applied_lsn: manifest.applied_lsn,
            provenance: manifest.provenance,
            l1_files: manifest.l1_files,
            l2_file: manifest.l2_file,
            compaction_in_progress: false,
            active_wal_index,
            wal_writer: Mutex::new(wal_writer),
            immutable_readers: Vec::new(),
        };
        index.validate_current_format()?;
        index.refresh_immutable_readers()?;
        index.cleanup_obsolete_files()?;
        Ok(index)
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn
    }

    pub fn provenance(&self) -> Option<&PrimaryIndexProvenance> {
        self.provenance.as_ref()
    }

    pub fn load(&self) -> Result<PrimaryIndex> {
        let index = PrimaryIndex::new();
        // The read view is stored newest-first for point lookups. Rebuild the
        // complete hash index oldest-first so equal-timestamp replacements
        // preserve durable edit order, then apply the active WAL last.
        for reader in self.immutable_readers.iter().rev() {
            index.batch_apply_versions(reader.entries()?);
        }
        index.batch_apply_versions(self.active_wal_index.snapshot_versions());

        Ok(index)
    }

    pub fn apply_upserts(&self, pairs: &[(Vec<u8>, RowID)]) -> Result<()> {
        self.apply_upserts_at(pairs, 0)
    }

    pub fn apply_upserts_at(&self, pairs: &[(Vec<u8>, RowID)], commit_ts: u64) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let wal_path = self.active_wal_path();
        let mut file = self
            .wal_writer
            .lock()
            .map_err(|_| paro_error::internal("lock persistent index WAL writer"))?;
        let mut encoded = Vec::new();
        for (key, row_id) in pairs {
            let key_len = key.len() as u32;
            encoded.extend_from_slice(&key_len.to_le_bytes());
            encoded.extend_from_slice(key);
            encoded.extend_from_slice(&u64::from(*row_id).to_le_bytes());
            encoded.extend_from_slice(&commit_ts.to_le_bytes());
        }
        file.write_all(&encoded).map_err(|e| {
            paro_error::io_error(format!("write WAL batch to {:?}: {}", wal_path, e))
        })?;
        file.flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", wal_path, e)))?;
        self.active_wal_index
            .batch_upsert_at(pairs.iter().cloned(), commit_ts);
        Ok(())
    }

    pub fn apply_deletes(&self, keys: &[Vec<u8>]) -> Result<()> {
        self.apply_deletes_at(keys, 0)
    }

    pub fn apply_deletes_at(&self, keys: &[Vec<u8>], commit_ts: u64) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let wal_path = self.active_wal_path();
        let mut file = self
            .wal_writer
            .lock()
            .map_err(|_| paro_error::internal("lock persistent index WAL writer"))?;
        let tombstone = NULL_ROW_ID.to_le_bytes();
        let mut encoded = Vec::new();
        for key in keys {
            let key_len = key.len() as u32;
            encoded.extend_from_slice(&key_len.to_le_bytes());
            encoded.extend_from_slice(key);
            encoded.extend_from_slice(&tombstone);
            encoded.extend_from_slice(&commit_ts.to_le_bytes());
        }
        file.write_all(&encoded).map_err(|e| {
            paro_error::io_error(format!("write WAL batch to {:?}: {}", wal_path, e))
        })?;
        file.flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", wal_path, e)))?;
        for key in keys {
            self.active_wal_index.remove_at(key, commit_ts);
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<RowID>> {
        Ok(self
            .get_version_at(key, u64::MAX)?
            .and_then(PrimaryIndexVersion::visible_row_id))
    }

    pub fn get_version_at(&self, key: &[u8], read_ts: u64) -> Result<Option<PrimaryIndexVersion>> {
        let mut best = self.active_wal_index.get_version_at(key, read_ts);
        for reader in &self.immutable_readers {
            Self::select_newer_visible(&mut best, reader.get_version_at(key, read_ts)?);
        }

        Ok(best)
    }

    pub fn lookup_keys(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<RowID>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        if keys.len() == 1 {
            return Ok(vec![self.get(&keys[0])?]);
        }

        self.lookup_keys_at(keys, u64::MAX)
    }

    pub fn lookup_keys_at(&self, keys: &[Vec<u8>], read_ts: u64) -> Result<Vec<Option<RowID>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        Ok(self
            .lookup_versions_at(keys, read_ts)?
            .into_iter()
            .map(|version| version.and_then(PrimaryIndexVersion::visible_row_id))
            .collect())
    }

    pub fn lookup_versions_at(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
    ) -> Result<Vec<Option<PrimaryIndexVersion>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        if keys.len() == 1 {
            return Ok(vec![self.get_version_at(&keys[0], read_ts)?]);
        }

        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let mut best = self.active_wal_index.get_version_at(key, read_ts);
            for reader in &self.immutable_readers {
                Self::select_newer_visible(&mut best, reader.get_version_at(key, read_ts)?);
            }
            results.push(best);
        }
        Ok(results)
    }

    pub fn has_write_in_range(&self, key: &[u8], read_ts: u64, commit_ts: u64) -> Result<bool> {
        Ok(self
            .first_write_in_range(key, read_ts, commit_ts)?
            .is_some())
    }

    pub fn first_write_in_range(
        &self,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        if read_ts >= commit_ts {
            return Ok(None);
        }

        let mut best = self
            .active_wal_index
            .first_write_in_range(key, read_ts, commit_ts);
        for reader in &self.immutable_readers {
            if let Some(conflict) = reader.first_write_in_range(key, read_ts, commit_ts)? {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        Ok(best)
    }

    pub fn first_write_for_keys_in_range(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        if keys.is_empty() || read_ts >= commit_ts {
            return Ok(None);
        }
        if keys.len() == 1 {
            return self.first_write_in_range(&keys[0], read_ts, commit_ts);
        }

        let mut best = self
            .active_wal_index
            .first_write_for_keys_in_range(keys, read_ts, commit_ts);
        for key in keys {
            for reader in &self.immutable_readers {
                if let Some(conflict) = reader.first_write_in_range(key, read_ts, commit_ts)? {
                    select_earlier_conflict(&mut best, conflict);
                }
            }
        }
        Ok(best)
    }

    pub fn first_key_range_write_in_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        if read_ts >= commit_ts {
            return Ok(None);
        }

        let mut best = self
            .active_wal_index
            .first_key_range_write_in_range(lower, upper, read_ts, commit_ts);
        for reader in &self.immutable_readers {
            if let Some(conflict) =
                reader.first_key_range_write_in_range(lower, upper, read_ts, commit_ts)?
            {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        Ok(best)
    }

    pub fn batch_get_loaded(&self, idx: &PrimaryIndex, keys: &[Vec<u8>]) -> Vec<Option<RowID>> {
        keys.iter().map(|key| idx.get(key)).collect()
    }

    pub fn flush_l0(&mut self, idx: &PrimaryIndex, truncate_wal: bool) -> Result<()> {
        self.flush_l0_with_provenance(idx, truncate_wal, None)
    }

    /// Publish the current in-memory delta and its exact tablet provenance in
    /// one manifest replacement.  Immutable files are written first; the new
    /// provenance cannot become visible without the file set it describes.
    pub fn flush_l0_with_provenance(
        &mut self,
        idx: &PrimaryIndex,
        truncate_wal: bool,
        provenance: Option<PrimaryIndexProvenance>,
    ) -> Result<()> {
        let mut next = self.manifest_snapshot();
        let mut records = self.active_wal_index.snapshot_versions();
        records.extend(idx.snapshot_versions());
        let mut wrote_l1 = false;

        if !records.is_empty() {
            let file_id = next.next_file_id;
            let stats =
                self.write_immutable_level_file(ImmutableIndexLevel::L1, file_id, &records)?;
            next.edit_version += 1;
            next.l1_files.push(ImmutableFileMeta {
                file_id,
                level: ImmutableIndexLevel::L1,
                edit_version: next.edit_version,
                entry_count: stats.entry_count as u64,
            });
            next.next_file_id += 1;
            wrote_l1 = true;
        }

        let next_wal_writer = if truncate_wal {
            next.active_wal_id += 1;
            Some(self.create_empty_wal_writer(next.active_wal_id)?)
        } else {
            None
        };

        next.provenance = provenance;
        // Open every immutable member before publishing its manifest. Once
        // the manifest rename succeeds, installing the in-process read view
        // is infallible and cannot strand queries between generations.
        let next_readers = self.open_immutable_readers(&next.l1_files, next.l2_file.as_ref())?;
        self.write_manifest_value(&next)?;

        self.install_manifest(next);
        self.immutable_readers = next_readers;
        if let Some(writer) = next_wal_writer {
            self.active_wal_index = Arc::new(PrimaryIndex::new());
            self.wal_writer = Mutex::new(writer);
        }
        if wrote_l1 {
            storage_metrics().inc_persistent_index_flushes();
        }
        if let Err(error) = self.cleanup_obsolete_files() {
            tracing::warn!(
                path = %self.root_dir.display(),
                error = %error,
                "persistent primary-index publication succeeded but obsolete-file cleanup failed"
            );
        }
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        if self.root_dir.exists() {
            fs::remove_dir_all(&self.root_dir).map_err(|e| {
                paro_error::io_error(format!(
                    "reset persistent index dir {:?}: {}",
                    self.root_dir, e
                ))
            })?;
        }
        fs::create_dir_all(&self.root_dir).map_err(|e| {
            paro_error::io_error(format!(
                "recreate persistent index dir {:?}: {}",
                self.root_dir, e
            ))
        })?;
        self.active_wal_id = 0;
        self.next_file_id = 1;
        self.edit_version = 0;
        self.applied_lsn = 0;
        self.provenance = None;
        self.l1_files.clear();
        self.l2_file = None;
        self.compaction_in_progress = false;
        self.immutable_readers.clear();
        self.active_wal_index = Arc::new(PrimaryIndex::new());
        let wal_path = self.active_wal_path();
        let mut writer = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal_path)
            .map_err(|e| paro_error::io_error(format!("create wal {:?}: {}", wal_path, e)))?;
        Self::write_file_header(&mut writer)?;
        writer
            .flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", wal_path, e)))?;
        self.wal_writer = Mutex::new(writer);
        Ok(())
    }

    pub fn set_applied_lsn(&mut self, applied_lsn: u64) -> Result<()> {
        let mut next = self.manifest_snapshot();
        next.applied_lsn = applied_lsn;
        self.write_manifest_value(&next)?;
        self.install_manifest(next);
        Ok(())
    }

    fn validate_current_format(&self) -> Result<()> {
        let wal_path = self.active_wal_path();
        let status = Self::path_header_status(&wal_path)?;
        if !matches!(status, FileHeaderStatus::Current | FileHeaderStatus::Empty) {
            return Err(paro_error::data_corrupted(format!(
                "unsupported persistent index WAL format at {:?}",
                wal_path
            )));
        }

        if let Some((_, path)) = self.list_files_with_prefix("sst_")?.into_iter().next() {
            return Err(paro_error::data_corrupted(format!(
                "unsupported legacy persistent index snapshot at {:?}",
                path
            )));
        }

        Ok(())
    }

    /// Whether immutable read amplification has crossed the point at which a
    /// background merge should be admitted. Foreground L0 flushes never run
    /// this merge inline: a small delta must not inherit a full-base rewrite.
    pub(crate) fn compaction_needed(&self) -> bool {
        self.l1_files.len() > MINOR_COMPACTION_THRESHOLD
    }

    /// Reserve one exact immutable generation for background compaction.
    ///
    /// File-id allocation is advanced before releasing the lock, so a
    /// concurrent foreground L0 flush cannot collide with this output. The
    /// staging name is intentionally outside the `l2_` namespace: foreground
    /// obsolete-file cleanup may run while this plan is being written.
    pub(crate) fn prepare_compaction(&mut self) -> Result<Option<PersistentIndexCompactionPlan>> {
        if self.compaction_in_progress || !self.compaction_needed() {
            return Ok(None);
        }

        let selected_l1_file_ids = self
            .l1_files
            .iter()
            .map(|meta| meta.file_id)
            .collect::<Vec<_>>();
        let expected_l2_file_id = self.l2_file.as_ref().map(|meta| meta.file_id);
        let mut readers = Vec::with_capacity(
            selected_l1_file_ids
                .len()
                .saturating_add(usize::from(expected_l2_file_id.is_some())),
        );
        if let Some(meta) = &self.l2_file {
            readers.push(ImmutableIndexReader::open_cached(
                self.immutable_meta_path(meta),
            )?);
        }
        let mut l1_files = self.l1_files.clone();
        l1_files.sort_by_key(|meta| meta.edit_version);
        for meta in &l1_files {
            readers.push(ImmutableIndexReader::open_cached(
                self.immutable_meta_path(meta),
            )?);
        }

        let output_file_id = self.next_file_id;
        self.next_file_id = self.next_file_id.saturating_add(1);
        self.compaction_in_progress = true;
        Ok(Some(PersistentIndexCompactionPlan {
            selected_l1_file_ids,
            expected_l2_file_id,
            readers,
            output_file_id,
            staging_path: self
                .root_dir
                .join(format!("compaction_{output_file_id}.tmp")),
            final_path: self.immutable_path(ImmutableIndexLevel::L2, output_file_id),
        }))
    }

    /// Merge a prepared immutable generation without holding any tablet lock.
    pub(crate) fn execute_compaction(
        plan: &PersistentIndexCompactionPlan,
    ) -> Result<PersistentIndexCompactionOutput> {
        let mut merged = HashMap::<(Vec<u8>, u64), PrimaryIndexVersion>::new();
        for reader in &plan.readers {
            for (key, version) in reader.entries()? {
                merged.insert((key, version.commit_ts), version);
            }
        }
        let mut entries: Vec<_> = merged
            .into_iter()
            .map(|((key, _), version)| (key, version))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.commit_ts.cmp(&b.1.commit_ts)));
        let stats = ImmutableIndexWriter::default().write_entries(&plan.staging_path, &entries)?;
        Ok(PersistentIndexCompactionOutput { stats })
    }

    /// Atomically replace only the immutable inputs captured by `plan`.
    /// Foreground flushes published while the merge ran remain as newer L1
    /// members and are never folded into an output that did not read them.
    pub(crate) fn publish_compaction(
        &mut self,
        plan: PersistentIndexCompactionPlan,
        output: PersistentIndexCompactionOutput,
    ) -> Result<bool> {
        let selected = plan
            .selected_l1_file_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let sources_still_current = self.l2_file.as_ref().map(|meta| meta.file_id)
            == plan.expected_l2_file_id
            && selected
                .iter()
                .all(|file_id| self.l1_files.iter().any(|meta| meta.file_id == *file_id));
        if !sources_still_current {
            self.compaction_in_progress = false;
            let _ = fs::remove_file(&plan.staging_path);
            return Ok(false);
        }

        fs::rename(&plan.staging_path, &plan.final_path).map_err(|error| {
            self.compaction_in_progress = false;
            paro_error::io_error(format!(
                "publish primary-index compaction {:?} -> {:?}: {}",
                plan.staging_path, plan.final_path, error
            ))
        })?;

        let mut next = self.manifest_snapshot();
        next.l1_files
            .retain(|meta| !selected.contains(&meta.file_id));
        next.edit_version = next.edit_version.saturating_add(1);
        next.l2_file = Some(ImmutableFileMeta {
            file_id: plan.output_file_id,
            level: ImmutableIndexLevel::L2,
            edit_version: next.edit_version,
            entry_count: output.stats.entry_count as u64,
        });
        let next_readers = match self.open_immutable_readers(&next.l1_files, next.l2_file.as_ref())
        {
            Ok(readers) => readers,
            Err(error) => {
                self.compaction_in_progress = false;
                let _ = fs::remove_file(&plan.final_path);
                return Err(error);
            }
        };
        if let Err(error) = self.write_manifest_value(&next) {
            self.compaction_in_progress = false;
            let _ = fs::remove_file(&plan.final_path);
            return Err(error);
        }
        self.install_manifest(next);
        self.immutable_readers = next_readers;
        self.compaction_in_progress = false;
        if let Err(error) = self.cleanup_obsolete_files() {
            tracing::warn!(
                path = %self.root_dir.display(),
                error = %error,
                "primary-index compaction published but obsolete-file cleanup failed"
            );
        }
        Ok(true)
    }

    pub(crate) fn abort_compaction(&mut self, plan: &PersistentIndexCompactionPlan) {
        self.compaction_in_progress = false;
        let _ = fs::remove_file(&plan.staging_path);
    }

    /// Build the query read view exactly once per manifest publication.
    /// Readers are newest-first: active WAL lookups are consulted before this
    /// slice, then newer L1 edits before older L1/L2 data. That ordering is the
    /// deterministic tie-break when low-level callers use equal commit times.
    fn refresh_immutable_readers(&mut self) -> Result<()> {
        self.immutable_readers =
            self.open_immutable_readers(&self.l1_files, self.l2_file.as_ref())?;
        Ok(())
    }

    fn open_immutable_readers(
        &self,
        l1_files: &[ImmutableFileMeta],
        l2_file: Option<&ImmutableFileMeta>,
    ) -> Result<Vec<Arc<ImmutableIndexReader>>> {
        let mut readers = Vec::with_capacity(
            l1_files
                .len()
                .saturating_add(usize::from(l2_file.is_some())),
        );
        let mut l1_files = l1_files.to_vec();
        l1_files.sort_by_key(|meta| std::cmp::Reverse(meta.edit_version));
        for meta in l1_files {
            readers.push(ImmutableIndexReader::open_cached(
                self.immutable_meta_path(&meta),
            )?);
        }
        if let Some(meta) = l2_file {
            readers.push(ImmutableIndexReader::open_cached(
                self.immutable_meta_path(meta),
            )?);
        }
        Ok(readers)
    }

    fn immutable_meta_path(&self, meta: &ImmutableFileMeta) -> PathBuf {
        self.immutable_path(meta.level, meta.file_id)
    }

    fn write_immutable_level_file(
        &self,
        level: ImmutableIndexLevel,
        file_id: u64,
        entries: &[(Vec<u8>, PrimaryIndexVersion)],
    ) -> Result<ImmutableIndexStats> {
        ImmutableIndexWriter::default().write_entries(self.immutable_path(level, file_id), entries)
    }

    fn active_wal_path(&self) -> PathBuf {
        self.root_dir
            .join(format!("wal_{}.wal", self.active_wal_id))
    }

    fn create_empty_wal_writer(&self, wal_id: u64) -> Result<File> {
        let wal_path = self.root_dir.join(format!("wal_{wal_id}.wal"));
        let mut writer = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal_path)
            .map_err(|e| paro_error::io_error(format!("create wal {:?}: {}", wal_path, e)))?;
        Self::write_file_header(&mut writer)?;
        writer
            .flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", wal_path, e)))?;
        Ok(writer)
    }

    fn immutable_path(&self, level: ImmutableIndexLevel, file_id: u64) -> PathBuf {
        self.root_dir
            .join(format!("{}_{}.idx", level.file_prefix(), file_id))
    }

    fn read_wal_records_path(path: &Path) -> Result<Vec<(Vec<u8>, PrimaryIndexVersion)>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let data = fs::read(path)
            .map_err(|e| paro_error::io_error(format!("read wal {:?}: {}", path, e)))?;
        parse_records(&data)
    }

    fn select_newer_visible(
        best: &mut Option<PrimaryIndexVersion>,
        candidate: Option<PrimaryIndexVersion>,
    ) {
        let Some(candidate) = candidate else {
            return;
        };
        if best
            .map(|current| candidate.commit_ts > current.commit_ts)
            .unwrap_or(true)
        {
            *best = Some(candidate);
        }
    }

    fn list_wal_files(&self) -> Result<Vec<(u64, PathBuf)>> {
        self.list_files_with_prefix("wal_")
    }

    fn list_files_with_prefix(&self, prefix: &str) -> Result<Vec<(u64, PathBuf)>> {
        let entries = fs::read_dir(&self.root_dir)
            .map_err(|e| paro_error::io_error(format!("read dir {:?}: {}", self.root_dir, e)))?;
        let mut out = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|e| paro_error::io_error(format!("read dir entry: {}", e)))?;
            let path = entry.path();
            if let Some(id) = file_id_with_prefix(&path, prefix) {
                out.push((id, path));
            }
        }
        Ok(out)
    }

    fn manifest_snapshot(&self) -> Manifest {
        Manifest {
            format_version: PERSISTENT_INDEX_FORMAT_VERSION,
            active_wal_id: self.active_wal_id,
            next_file_id: self.next_file_id,
            edit_version: self.edit_version,
            applied_lsn: self.applied_lsn,
            provenance: self.provenance.clone(),
            l1_files: self.l1_files.clone(),
            l2_file: self.l2_file.clone(),
        }
    }

    fn install_manifest(&mut self, manifest: Manifest) {
        self.active_wal_id = manifest.active_wal_id;
        self.next_file_id = manifest.next_file_id;
        self.edit_version = manifest.edit_version;
        self.applied_lsn = manifest.applied_lsn;
        self.provenance = manifest.provenance;
        self.l1_files = manifest.l1_files;
        self.l2_file = manifest.l2_file;
    }

    fn write_manifest_value(&self, manifest: &Manifest) -> Result<()> {
        let data = serde_json::to_vec_pretty(manifest)
            .map_err(|e| paro_error::serialization_error(e.to_string()))?;
        let tmp_path = self.manifest_path.with_extension("tmp");
        fs::write(&tmp_path, data)
            .map_err(|e| paro_error::io_error(format!("write manifest {:?}: {}", tmp_path, e)))?;
        fs::rename(&tmp_path, &self.manifest_path).map_err(|e| {
            paro_error::io_error(format!(
                "rename manifest {:?} -> {:?}: {}",
                tmp_path, self.manifest_path, e
            ))
        })
    }

    fn read_manifest(path: &Path) -> Result<Option<Manifest>> {
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read(path)
            .map_err(|e| paro_error::io_error(format!("read manifest {:?}: {}", path, e)))?;
        let manifest: Manifest = serde_json::from_slice(&data)
            .map_err(|e| paro_error::serialization_error(e.to_string()))?;
        Ok(Some(manifest))
    }

    fn cleanup_obsolete_files(&self) -> Result<()> {
        for (id, path) in self.list_wal_files()? {
            if id < self.active_wal_id {
                let _ = fs::remove_file(&path);
            }
        }

        let keep_l1: std::collections::HashSet<_> =
            self.l1_files.iter().map(|meta| meta.file_id).collect();
        for (id, path) in self.list_files_with_prefix("l1_")? {
            if !keep_l1.contains(&id) {
                let _ = fs::remove_file(&path);
            }
        }

        let keep_l2 = self.l2_file.as_ref().map(|meta| meta.file_id);
        for (id, path) in self.list_files_with_prefix("l2_")? {
            if Some(id) != keep_l2 {
                let _ = fs::remove_file(&path);
            }
        }

        for (_, path) in self.list_files_with_prefix("sst_")? {
            let _ = fs::remove_file(&path);
        }
        // A foreground L0 publication may run while a prepared background
        // compaction is still writing its staging file. Only startup/reset
        // cleanup, where no plan can be live, may reclaim this namespace.
        if !self.compaction_in_progress {
            for (_, path) in self.list_files_with_prefix("compaction_")? {
                let _ = fs::remove_file(&path);
            }
        }

        Ok(())
    }

    fn ensure_file_header(file: &mut File) -> Result<()> {
        let len = file
            .metadata()
            .map_err(|e| paro_error::io_error(format!("stat persistent index file: {}", e)))?
            .len();
        if len == 0 {
            Self::write_file_header(file)?;
        }
        Ok(())
    }

    fn write_file_header(file: &mut File) -> Result<()> {
        file.write_all(&WAL_MAGIC)
            .map_err(|e| paro_error::io_error(format!("write persistent index magic: {}", e)))?;
        file.write_all(&PERSISTENT_INDEX_FORMAT_VERSION.to_le_bytes())
            .map_err(|e| paro_error::io_error(format!("write persistent index version: {}", e)))
    }

    fn path_header_status(path: &Path) -> Result<FileHeaderStatus> {
        let data = fs::read(path).map_err(|e| {
            paro_error::io_error(format!("read persistent index header {:?}: {}", path, e))
        })?;
        Ok(Self::file_header_status(&data))
    }

    fn file_header_status(data: &[u8]) -> FileHeaderStatus {
        if data.is_empty() {
            return FileHeaderStatus::Empty;
        }
        if data.len() < FILE_HEADER_LEN || data[0..4] != WAL_MAGIC {
            return FileHeaderStatus::Legacy;
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version == PERSISTENT_INDEX_FORMAT_VERSION {
            FileHeaderStatus::Current
        } else {
            FileHeaderStatus::Legacy
        }
    }
}

fn parse_records(data: &[u8]) -> Result<Vec<(Vec<u8>, PrimaryIndexVersion)>> {
    let status = PersistentIndex::file_header_status(data);
    let mut offset = match status {
        FileHeaderStatus::Current => FILE_HEADER_LEN,
        FileHeaderStatus::Empty => return Ok(Vec::new()),
        FileHeaderStatus::Legacy => {
            return Err(paro_error::data_corrupted(
                "legacy persistent index WAL format is not supported",
            ))
        }
    };

    let mut out = Vec::new();
    while offset + 4 + VALUE_LEN <= data.len() {
        let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + key_len + VALUE_LEN > data.len() {
            return Err(paro_error::data_corrupted(
                "truncated persistent index WAL record",
            ));
        }
        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;
        let row_id = RowID::from_raw(u64::from_le_bytes(
            data[offset..offset + 8].try_into().unwrap(),
        ));
        let commit_ts =
            u64::from_le_bytes(data[offset + 8..offset + VALUE_LEN].try_into().unwrap());
        offset += VALUE_LEN;
        out.push((key, PrimaryIndexVersion::live(row_id, commit_ts)));
    }
    if offset != data.len() {
        return Err(paro_error::data_corrupted(
            "trailing bytes in persistent index WAL",
        ));
    }
    Ok(out)
}

fn select_earlier_conflict(
    slot: &mut Option<PrimaryKeyWriteConflict>,
    candidate: PrimaryKeyWriteConflict,
) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.version.commit_ts < current.version.commit_ts
                || (candidate.version.commit_ts == current.version.commit_ts
                    && candidate.key < current.key)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

fn file_id_with_prefix(path: &Path, prefix: &str) -> Option<u64> {
    let name = path.file_stem()?.to_string_lossy();
    if !name.starts_with(prefix) {
        return None;
    }
    name[prefix.len()..].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;
    use tempfile::tempdir;

    #[test]
    fn load_returns_empty_index_when_no_wal() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let pi = PersistentIndex::new(dir.path()).unwrap();
        let idx = pi.load().unwrap();
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn apply_and_load_roundtrip_across_flushes() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        let pairs = vec![
            (
                b"k1".to_vec(),
                RowID::new(1, crate::rowset::SegmentRowId::from_raw(10)),
            ),
            (
                b"k2".to_vec(),
                RowID::new(2, crate::rowset::SegmentRowId::from_raw(20)),
            ),
        ];
        pi.apply_upserts(&pairs).unwrap();

        let mut idx = pi.load().unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(
            idx.get(b"k2"),
            Some(RowID::new(2, crate::rowset::SegmentRowId::from_raw(20)))
        );

        pi.flush_l0(&idx, true).unwrap();
        let new_pairs = vec![
            (
                b"k2".to_vec(),
                RowID::new(9, crate::rowset::SegmentRowId::from_raw(99)),
            ),
            (
                b"k3".to_vec(),
                RowID::new(3, crate::rowset::SegmentRowId::from_raw(30)),
            ),
        ];
        pi.apply_upserts(&new_pairs).unwrap();
        idx = pi.load().unwrap();
        assert_eq!(idx.len(), 3);
        assert_eq!(
            idx.get(b"k2"),
            Some(RowID::new(9, crate::rowset::SegmentRowId::from_raw(99)))
        );
        assert_eq!(
            idx.get(b"k3"),
            Some(RowID::new(3, crate::rowset::SegmentRowId::from_raw(30)))
        );
    }

    #[test]
    fn get_respects_tombstone() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let pi = PersistentIndex::new(dir.path()).unwrap();
        pi.apply_upserts(&[(
            b"k1".to_vec(),
            RowID::new(1, crate::rowset::SegmentRowId::from_raw(10)),
        )])
        .unwrap();
        assert_eq!(
            pi.get(b"k1").unwrap(),
            Some(RowID::new(1, crate::rowset::SegmentRowId::from_raw(10)))
        );

        pi.apply_deletes(&[b"k1".to_vec()]).unwrap();
        assert_eq!(pi.get(b"k1").unwrap(), None);

        let idx = pi.load().unwrap();
        assert!(idx.get(b"k1").is_none());
    }

    #[test]
    fn lookup_keys_batches_persistent_entries_after_flush() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();

        pi.apply_upserts(&[
            (
                b"k1".to_vec(),
                RowID::new(1, crate::rowset::SegmentRowId::from_raw(10)),
            ),
            (
                b"k2".to_vec(),
                RowID::new(2, crate::rowset::SegmentRowId::from_raw(20)),
            ),
        ])
        .unwrap();
        let idx = pi.load().unwrap();
        pi.flush_l0(&idx, true).unwrap();

        pi.apply_upserts(&[(
            b"k3".to_vec(),
            RowID::new(3, crate::rowset::SegmentRowId::from_raw(30)),
        )])
        .unwrap();
        pi.apply_deletes(&[b"k1".to_vec()]).unwrap();

        let rows = pi
            .lookup_keys(&[
                b"k1".to_vec(),
                b"k2".to_vec(),
                b"k3".to_vec(),
                b"missing".to_vec(),
            ])
            .unwrap();
        assert_eq!(
            rows,
            vec![
                None,
                Some(RowID::new(2, crate::rowset::SegmentRowId::from_raw(20))),
                Some(RowID::new(3, crate::rowset::SegmentRowId::from_raw(30))),
                None,
            ]
        );
    }

    #[test]
    fn write_conflict_window_spans_wal_and_immutable_files() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        let empty = PrimaryIndex::new();

        pi.apply_upserts_at(
            &[(
                b"a".to_vec(),
                RowID::new(1, crate::rowset::SegmentRowId::from_raw(1)),
            )],
            10,
        )
        .unwrap();
        pi.flush_l0(&empty, true).unwrap();
        pi.apply_upserts_at(
            &[(
                b"b".to_vec(),
                RowID::new(1, crate::rowset::SegmentRowId::from_raw(2)),
            )],
            20,
        )
        .unwrap();
        pi.apply_deletes_at(&[b"b".to_vec()], 30).unwrap();

        assert!(!pi.has_write_in_range(b"a", 10, 99).unwrap());
        assert!(pi.has_write_in_range(b"a", 9, 10).unwrap());

        let conflict = pi
            .first_write_for_keys_in_range(&[b"missing".to_vec(), b"b".to_vec()], 19, 99)
            .unwrap()
            .unwrap();
        assert_eq!(conflict.key, b"b".to_vec());
        assert_eq!(conflict.commit_ts(), 20);

        let range_conflict = pi
            .first_key_range_write_in_range(Some(b"b"), Some(b"c"), 20, 30)
            .unwrap()
            .unwrap();
        assert!(range_conflict.is_tombstone());
        assert_eq!(range_conflict.commit_ts(), 30);
    }

    #[test]
    fn background_compaction_merges_l1_and_preserves_tombstones() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        let empty = PrimaryIndex::new();

        for i in 0..6u32 {
            pi.apply_upserts(&[(
                format!("k{i}").into_bytes(),
                RowID::new(1, crate::rowset::SegmentRowId::from_raw(i)),
            )])
            .unwrap();
            pi.flush_l0(&empty, true).unwrap();
        }
        pi.apply_deletes(&[b"k1".to_vec()]).unwrap();
        pi.flush_l0(&empty, true).unwrap();

        assert!(pi.compaction_needed());
        assert!(pi.l2_file.is_none());
        let plan = pi.prepare_compaction().unwrap().unwrap();
        let output = PersistentIndex::execute_compaction(&plan).unwrap();
        assert!(pi.publish_compaction(plan, output).unwrap());

        let reopened = PersistentIndex::new(dir.path()).unwrap();
        assert!(reopened.l2_file.is_some());
        assert!(reopened.l1_files.is_empty());
        assert_eq!(reopened.get(b"k1").unwrap(), None);
        assert_eq!(
            reopened.get(b"k4").unwrap(),
            Some(RowID::new(1, crate::rowset::SegmentRowId::from_raw(4)))
        );
    }

    #[test]
    fn background_compaction_preserves_l1_published_after_prepare() {
        storage_metrics().reset_for_tests();
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        let empty = PrimaryIndex::new();

        for i in 0..6u32 {
            pi.apply_upserts_at(
                &[(
                    format!("old-{i}").into_bytes(),
                    RowID::new(1, crate::rowset::SegmentRowId::from_raw(i)),
                )],
                u64::from(i + 1),
            )
            .unwrap();
            pi.flush_l0(&empty, true).unwrap();
        }
        let plan = pi.prepare_compaction().unwrap().unwrap();
        let output = PersistentIndex::execute_compaction(&plan).unwrap();

        let new_row = RowID::new(2, crate::rowset::SegmentRowId::from_raw(77));
        pi.apply_upserts_at(&[(b"new".to_vec(), new_row)], 77)
            .unwrap();
        pi.flush_l0(&empty, true).unwrap();
        assert_eq!(pi.l1_files.len(), 7);

        assert!(pi.publish_compaction(plan, output).unwrap());
        assert_eq!(pi.l1_files.len(), 1);
        assert_eq!(pi.get(b"new").unwrap(), Some(new_row));
        assert_eq!(
            pi.get(b"old-4").unwrap(),
            Some(RowID::new(1, crate::rowset::SegmentRowId::from_raw(4)))
        );
    }

    #[test]
    fn rejects_legacy_snapshot_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("sst_1.sst"), b"legacy").unwrap();

        assert!(PersistentIndex::new(dir.path()).is_err());
    }

    #[test]
    fn rejects_legacy_wal_header_version() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal_0.wal");
        let mut file = File::create(&wal_path).unwrap();
        file.write_all(&WAL_MAGIC).unwrap();
        file.write_all(&(PERSISTENT_INDEX_FORMAT_VERSION - 1).to_le_bytes())
            .unwrap();
        file.flush().unwrap();

        assert!(PersistentIndex::new(dir.path()).is_err());
    }

    #[test]
    fn rejects_truncated_current_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal_0.wal");
        let mut file = File::create(&wal_path).unwrap();
        PersistentIndex::write_file_header(&mut file).unwrap();
        file.write_all(&4u32.to_le_bytes()).unwrap();
        file.write_all(b"ab").unwrap();
        file.flush().unwrap();

        assert!(PersistentIndex::new(dir.path()).is_err());
    }

    #[test]
    fn manifest_read_view_ignores_orphan_files() {
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        let original = RowID::new(1, crate::rowset::SegmentRowId::from_raw(1));
        pi.apply_upserts_at(&[(b"k".to_vec(), original)], 10)
            .unwrap();
        pi.flush_l0(&PrimaryIndex::new(), true).unwrap();
        drop(pi);

        let orphan_row = RowID::new(99, crate::rowset::SegmentRowId::from_raw(99));
        let orphan_wal = dir.path().join("wal_999.wal");
        let mut file = File::create(&orphan_wal).unwrap();
        PersistentIndex::write_file_header(&mut file).unwrap();
        file.write_all(&1u32.to_le_bytes()).unwrap();
        file.write_all(b"k").unwrap();
        file.write_all(&u64::from(orphan_row).to_le_bytes())
            .unwrap();
        file.write_all(&100u64.to_le_bytes()).unwrap();
        file.flush().unwrap();
        ImmutableIndexWriter::default()
            .write_entries(
                dir.path().join("l1_999.idx"),
                &[(b"k".to_vec(), PrimaryIndexVersion::live(orphan_row, 200))],
            )
            .unwrap();

        let reopened = PersistentIndex::new(dir.path()).unwrap();
        assert_eq!(reopened.get(b"k").unwrap(), Some(original));
    }

    #[test]
    fn concurrent_batches_append_complete_wal_records() {
        let dir = tempdir().unwrap();
        let pi = Arc::new(PersistentIndex::new(dir.path()).unwrap());
        std::thread::scope(|scope| {
            for worker in 0..4u32 {
                let pi = Arc::clone(&pi);
                scope.spawn(move || {
                    let pairs = (0..64u32)
                        .map(|row| {
                            (
                                format!("k-{worker}-{row}").into_bytes(),
                                RowID::new(worker, crate::rowset::SegmentRowId::from_raw(row)),
                            )
                        })
                        .collect::<Vec<_>>();
                    pi.apply_upserts_at(&pairs, u64::from(worker) + 1).unwrap();
                });
            }
        });
        drop(pi);

        let reopened = PersistentIndex::new(dir.path()).unwrap();
        for worker in 0..4u32 {
            for row in 0..64u32 {
                assert_eq!(
                    reopened
                        .get(format!("k-{worker}-{row}").as_bytes())
                        .unwrap(),
                    Some(RowID::new(
                        worker,
                        crate::rowset::SegmentRowId::from_raw(row),
                    ))
                );
            }
        }
    }

    #[test]
    fn reset_clears_obsolete_files() {
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        fs::write(dir.path().join("wal_99.wal"), b"orphan").unwrap();
        fs::write(dir.path().join("sst_1.sst"), b"legacy").unwrap();

        pi.reset().unwrap();

        assert!(dir.path().exists());
        let files = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(files, vec!["wal_0.wal"]);
        assert_eq!(
            PersistentIndex::path_header_status(&dir.path().join("wal_0.wal")).unwrap(),
            FileHeaderStatus::Current
        );
    }

    #[test]
    fn applied_lsn_roundtrips_in_manifest() {
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        pi.set_applied_lsn(42).unwrap();

        let reopened = PersistentIndex::new(dir.path()).unwrap();
        assert_eq!(reopened.applied_lsn(), 42);
    }

    #[test]
    fn provenance_is_published_with_the_flushed_file_set() {
        let dir = tempdir().unwrap();
        let mut pi = PersistentIndex::new(dir.path()).unwrap();
        let index = PrimaryIndex::new();
        index.upsert_at(
            b"k".to_vec(),
            RowID::new(7, crate::rowset::SegmentRowId::from_raw(3)),
            11,
        );
        let provenance = PrimaryIndexProvenance {
            tablet_id: 9,
            indexed_through_version: 11,
            layout_epoch: 4,
            schema_epoch: Some(2),
            schema_hash: 77,
            rowset_root: vec![PrimaryIndexRowsetRoot {
                rowset_id: 7,
                start_version: 0,
                end_version: 11,
                num_segments: 1,
                effective_rows: 1,
            }],
        };

        pi.flush_l0_with_provenance(&index, true, Some(provenance.clone()))
            .unwrap();
        let reopened = PersistentIndex::new(dir.path()).unwrap();
        assert_eq!(reopened.provenance(), Some(&provenance));
        assert!(reopened.get(b"k").unwrap().is_some());
    }

    #[test]
    fn tombstone_uses_reserved_null_row_id() {
        let tombstone = PrimaryIndexVersion::tombstone(7);
        assert_eq!(u64::from(tombstone.row_id), NULL_ROW_ID);
        assert!(tombstone.is_tombstone());
    }
}
