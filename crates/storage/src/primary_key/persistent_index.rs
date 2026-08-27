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

pub const PERSISTENT_INDEX_FORMAT_VERSION: u32 = 5;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ImmutableFileMeta {
    file_id: u64,
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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Clone)]
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
}

impl PersistentIndex {
    pub fn new(root_dir: impl AsRef<Path>) -> Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        fs::create_dir_all(&root_dir).map_err(|e| {
            paro_error::io_error(format!("create persistent index dir {:?}: {}", root_dir, e))
        })?;
        let manifest_path = root_dir.join("primary_index.manifest");
        let manifest = Self::read_manifest(&manifest_path)?.unwrap_or_default();

        let max_known_file_id = manifest
            .l1_files
            .iter()
            .map(|meta| meta.file_id)
            .chain(manifest.l2_file.iter().map(|meta| meta.file_id))
            .max()
            .unwrap_or(0);

        Ok(Self {
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
        })
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn
    }

    pub fn provenance(&self) -> Option<&PrimaryIndexProvenance> {
        self.provenance.as_ref()
    }

    pub fn load(&self) -> Result<PrimaryIndex> {
        self.validate_current_format()?;

        let index = PrimaryIndex::new();

        if let Some(l2_file) = &self.l2_file {
            self.apply_immutable_file_to_index(l2_file, &index)?;
        }

        let mut l1_files = self.l1_files.clone();
        l1_files.sort_by_key(|meta| meta.edit_version);
        for meta in l1_files {
            self.apply_immutable_file_to_index(&meta, &index)?;
        }

        let mut wal_files = self.list_wal_files()?;
        wal_files.sort_by_key(|(id, _)| *id);
        for (id, path) in wal_files {
            if id < self.active_wal_id {
                continue;
            }
            index.batch_apply_versions(self.read_wal_records(&path)?);
        }

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
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| paro_error::io_error(format!("open wal {:?}: {}", wal_path, e)))?;
        Self::ensure_file_header(&mut file)?;

        for (key, row_id) in pairs {
            let key_len = key.len() as u32;
            file.write_all(&key_len.to_le_bytes()).map_err(|e| {
                paro_error::io_error(format!("write wal len to {:?}: {}", wal_path, e))
            })?;
            file.write_all(key).map_err(|e| {
                paro_error::io_error(format!("write wal key to {:?}: {}", wal_path, e))
            })?;
            file.write_all(&u64::from(*row_id).to_le_bytes())
                .map_err(|e| {
                    paro_error::io_error(format!("write wal row id to {:?}: {}", wal_path, e))
                })?;
            file.write_all(&commit_ts.to_le_bytes()).map_err(|e| {
                paro_error::io_error(format!("write wal commit ts to {:?}: {}", wal_path, e))
            })?;
        }
        file.flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", wal_path, e)))?;
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
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| paro_error::io_error(format!("open wal {:?}: {}", wal_path, e)))?;
        Self::ensure_file_header(&mut file)?;

        let tombstone = NULL_ROW_ID.to_le_bytes();
        for key in keys {
            let key_len = key.len() as u32;
            file.write_all(&key_len.to_le_bytes()).map_err(|e| {
                paro_error::io_error(format!("write wal len to {:?}: {}", wal_path, e))
            })?;
            file.write_all(key).map_err(|e| {
                paro_error::io_error(format!("write wal key to {:?}: {}", wal_path, e))
            })?;
            file.write_all(&tombstone).map_err(|e| {
                paro_error::io_error(format!("write wal tombstone to {:?}: {}", wal_path, e))
            })?;
            file.write_all(&commit_ts.to_le_bytes()).map_err(|e| {
                paro_error::io_error(format!("write wal commit ts to {:?}: {}", wal_path, e))
            })?;
        }
        file.flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", wal_path, e)))?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<RowID>> {
        Ok(self
            .get_version_at(key, u64::MAX)?
            .and_then(PrimaryIndexVersion::visible_row_id))
    }

    pub fn get_version_at(&self, key: &[u8], read_ts: u64) -> Result<Option<PrimaryIndexVersion>> {
        self.validate_current_format()?;

        let mut best = None;
        let mut wal_files = self.list_wal_files()?;
        wal_files.retain(|(id, _)| *id >= self.active_wal_id);
        wal_files.sort_by_key(|(id, _)| *id);
        for (_, path) in wal_files {
            Self::select_newer_visible(&mut best, self.search_wal_for_key_at(&path, key, read_ts)?);
        }

        let mut l1_files = self.l1_files.clone();
        l1_files.sort_by_key(|meta| meta.edit_version);
        for meta in l1_files {
            Self::select_newer_visible(
                &mut best,
                self.search_immutable_file_for_key_at(&meta, key, read_ts)?,
            );
        }

        if let Some(l2_file) = &self.l2_file {
            Self::select_newer_visible(
                &mut best,
                self.search_immutable_file_for_key_at(l2_file, key, read_ts)?,
            );
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

        let idx = self.load()?;
        Ok(idx.multi_get_versions_at(keys.iter().map(Vec::as_slice), read_ts))
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
        self.validate_current_format()?;
        if read_ts >= commit_ts {
            return Ok(None);
        }

        let mut best = None;
        let mut wal_files = self.list_wal_files()?;
        wal_files.retain(|(id, _)| *id >= self.active_wal_id);
        wal_files.sort_by_key(|(id, _)| *id);
        for (_, path) in wal_files {
            if let Some(conflict) =
                self.search_wal_for_write_in_range(&path, key, read_ts, commit_ts)?
            {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        let mut l1_files = self.l1_files.clone();
        l1_files.sort_by_key(|meta| meta.edit_version);
        for meta in l1_files {
            if let Some(conflict) =
                self.search_immutable_file_for_write_in_range(&meta, key, read_ts, commit_ts)?
            {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        if let Some(l2_file) = &self.l2_file {
            if let Some(conflict) =
                self.search_immutable_file_for_write_in_range(l2_file, key, read_ts, commit_ts)?
            {
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

        let idx = self.load()?;
        let mut best = None;
        for key in keys {
            if let Some(conflict) = idx.first_write_in_range(key, read_ts, commit_ts) {
                select_earlier_conflict(&mut best, conflict);
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
        self.validate_current_format()?;
        if read_ts >= commit_ts {
            return Ok(None);
        }

        let mut best = None;
        let mut wal_files = self.list_wal_files()?;
        wal_files.retain(|(id, _)| *id >= self.active_wal_id);
        wal_files.sort_by_key(|(id, _)| *id);
        for (_, path) in wal_files {
            if let Some(conflict) = self
                .search_wal_for_key_range_write_in_range(&path, lower, upper, read_ts, commit_ts)?
            {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        let mut l1_files = self.l1_files.clone();
        l1_files.sort_by_key(|meta| meta.edit_version);
        for meta in l1_files {
            if let Some(conflict) =
                ImmutableIndexReader::open_cached(self.resolve_immutable_meta_path(&meta))?
                    .first_key_range_write_in_range(lower, upper, read_ts, commit_ts)?
            {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        if let Some(l2_file) = &self.l2_file {
            if let Some(conflict) =
                ImmutableIndexReader::open_cached(self.resolve_immutable_meta_path(l2_file))?
                    .first_key_range_write_in_range(lower, upper, read_ts, commit_ts)?
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
        let wal_path = self.active_wal_path();
        let mut records = self.read_wal_records(&wal_path)?;
        records.extend(idx.snapshot_versions());

        if !records.is_empty() {
            let file_id = self.next_file_id;
            let stats = self.write_immutable_level_file("l1", file_id, &records)?;
            self.edit_version += 1;
            self.l1_files.push(ImmutableFileMeta {
                file_id,
                edit_version: self.edit_version,
                entry_count: stats.entry_count as u64,
            });
            self.next_file_id += 1;
            storage_metrics().inc_persistent_index_flushes();
        }

        if self.l1_files.len() > MINOR_COMPACTION_THRESHOLD {
            self.minor_compact()?;
        }

        if truncate_wal {
            self.active_wal_id += 1;
            self.create_empty_wal(self.active_wal_id)?;
        }

        self.provenance = provenance;
        self.write_manifest()?;
        self.cleanup_obsolete_files()?;
        Ok(())
    }

    pub fn reset(&self) -> Result<()> {
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
        Ok(())
    }

    pub fn set_applied_lsn(&mut self, applied_lsn: u64) -> Result<()> {
        self.applied_lsn = applied_lsn;
        self.write_manifest()
    }

    fn validate_current_format(&self) -> Result<()> {
        let manifest = Self::read_manifest(&self.manifest_path)?;
        if let Some(manifest) = manifest {
            if manifest.format_version != PERSISTENT_INDEX_FORMAT_VERSION {
                return Err(paro_error::data_corrupted(format!(
                    "unsupported persistent index manifest version {}",
                    manifest.format_version
                )));
            }
        }

        for (_, path) in self.list_wal_files()? {
            let status = Self::path_header_status(&path)?;
            if !matches!(status, FileHeaderStatus::Current | FileHeaderStatus::Empty) {
                return Err(paro_error::data_corrupted(format!(
                    "unsupported persistent index WAL format at {:?}",
                    path
                )));
            }
        }

        if let Some((_, path)) = self.list_files_with_prefix("sst_")?.into_iter().next() {
            return Err(paro_error::data_corrupted(format!(
                "unsupported legacy persistent index snapshot at {:?}",
                path
            )));
        }

        for path in self.current_immutable_paths() {
            let _ = ImmutableIndexReader::open_cached(path)?;
        }
        Ok(())
    }

    fn minor_compact(&mut self) -> Result<()> {
        let mut merged = HashMap::<(Vec<u8>, u64), PrimaryIndexVersion>::new();

        if let Some(l2_file) = &self.l2_file {
            for (key, version) in self.read_immutable_entries(l2_file)? {
                merged.insert((key, version.commit_ts), version);
            }
        }

        let mut l1_files = self.l1_files.clone();
        l1_files.sort_by_key(|meta| meta.edit_version);
        for meta in l1_files {
            for (key, version) in self.read_immutable_entries(&meta)? {
                merged.insert((key, version.commit_ts), version);
            }
        }

        self.l1_files.clear();

        if merged.is_empty() {
            self.l2_file = None;
            return Ok(());
        }

        let mut entries: Vec<_> = merged
            .into_iter()
            .map(|((key, _), version)| (key, version))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.commit_ts.cmp(&b.1.commit_ts)));

        let file_id = self.next_file_id;
        let stats = self.write_immutable_level_file("l2", file_id, &entries)?;
        self.edit_version += 1;
        self.l2_file = Some(ImmutableFileMeta {
            file_id,
            edit_version: self.edit_version,
            entry_count: stats.entry_count as u64,
        });
        self.next_file_id += 1;
        Ok(())
    }

    fn apply_immutable_file_to_index(
        &self,
        meta: &ImmutableFileMeta,
        index: &PrimaryIndex,
    ) -> Result<()> {
        index.batch_apply_versions(self.read_immutable_entries(meta)?);
        Ok(())
    }

    fn search_immutable_file_for_key_at(
        &self,
        meta: &ImmutableFileMeta,
        key: &[u8],
        read_ts: u64,
    ) -> Result<Option<PrimaryIndexVersion>> {
        ImmutableIndexReader::open_cached(self.resolve_immutable_meta_path(meta))?
            .get_version_at(key, read_ts)
    }

    fn search_immutable_file_for_write_in_range(
        &self,
        meta: &ImmutableFileMeta,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        ImmutableIndexReader::open_cached(self.resolve_immutable_meta_path(meta))?
            .first_write_in_range(key, read_ts, commit_ts)
    }

    fn read_immutable_entries(
        &self,
        meta: &ImmutableFileMeta,
    ) -> Result<Vec<(Vec<u8>, PrimaryIndexVersion)>> {
        ImmutableIndexReader::open_cached(self.resolve_immutable_meta_path(meta))?.entries()
    }

    fn resolve_immutable_meta_path(&self, meta: &ImmutableFileMeta) -> PathBuf {
        let preferred_l1 = self.immutable_path("l1", meta.file_id);
        if preferred_l1.exists() {
            return preferred_l1;
        }
        self.immutable_path("l2", meta.file_id)
    }

    fn current_immutable_paths(&self) -> Vec<PathBuf> {
        self.l1_files
            .iter()
            .map(|meta| self.resolve_immutable_meta_path(meta))
            .chain(
                self.l2_file
                    .iter()
                    .map(|meta| self.resolve_immutable_meta_path(meta)),
            )
            .collect()
    }

    fn write_immutable_level_file(
        &self,
        level: &str,
        file_id: u64,
        entries: &[(Vec<u8>, PrimaryIndexVersion)],
    ) -> Result<ImmutableIndexStats> {
        ImmutableIndexWriter::default().write_entries(self.immutable_path(level, file_id), entries)
    }

    fn active_wal_path(&self) -> PathBuf {
        self.root_dir
            .join(format!("wal_{}.wal", self.active_wal_id))
    }

    fn immutable_path(&self, level: &str, file_id: u64) -> PathBuf {
        self.root_dir.join(format!("{}_{}.idx", level, file_id))
    }

    fn create_empty_wal(&self, wal_id: u64) -> Result<()> {
        let path = self.root_dir.join(format!("wal_{}.wal", wal_id));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| paro_error::io_error(format!("create wal {:?}: {}", path, e)))?;
        Self::write_file_header(&mut file)?;
        file.flush()
            .map_err(|e| paro_error::io_error(format!("flush wal {:?}: {}", path, e)))
    }

    fn read_wal_records(&self, path: &Path) -> Result<Vec<(Vec<u8>, PrimaryIndexVersion)>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let data = fs::read(path)
            .map_err(|e| paro_error::io_error(format!("read wal {:?}: {}", path, e)))?;
        parse_records(&data)
    }

    fn search_wal_for_key_at(
        &self,
        path: &Path,
        key: &[u8],
        read_ts: u64,
    ) -> Result<Option<PrimaryIndexVersion>> {
        let mut found = None;
        for (current_key, version) in self.read_wal_records(path)? {
            if current_key == key && version.commit_ts <= read_ts {
                if found
                    .map(|current: PrimaryIndexVersion| version.commit_ts >= current.commit_ts)
                    .unwrap_or(true)
                {
                    found = Some(version);
                }
            }
        }
        Ok(found)
    }

    fn search_wal_for_write_in_range(
        &self,
        path: &Path,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        let mut best = None;
        for (current_key, version) in self.read_wal_records(path)? {
            if current_key == key && version_in_window(version, read_ts, commit_ts) {
                select_earlier_conflict(
                    &mut best,
                    PrimaryKeyWriteConflict {
                        key: current_key,
                        version,
                    },
                );
            }
        }
        Ok(best)
    }

    fn search_wal_for_key_range_write_in_range(
        &self,
        path: &Path,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        let mut best = None;
        for (key, version) in self.read_wal_records(path)? {
            if key_in_bounds(&key, lower, upper) && version_in_window(version, read_ts, commit_ts) {
                select_earlier_conflict(&mut best, PrimaryKeyWriteConflict { key, version });
            }
        }
        Ok(best)
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

    fn write_manifest(&self) -> Result<()> {
        let manifest = Manifest {
            format_version: PERSISTENT_INDEX_FORMAT_VERSION,
            active_wal_id: self.active_wal_id,
            next_file_id: self.next_file_id,
            edit_version: self.edit_version,
            applied_lsn: self.applied_lsn,
            provenance: self.provenance.clone(),
            l1_files: self.l1_files.clone(),
            l2_file: self.l2_file.clone(),
        };
        let data = serde_json::to_vec_pretty(&manifest)
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
            break;
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
    Ok(out)
}

fn version_in_window(version: PrimaryIndexVersion, read_ts: u64, commit_ts: u64) -> bool {
    version.commit_ts > read_ts && version.commit_ts <= commit_ts
}

fn key_in_bounds(key: &[u8], lower: Option<&[u8]>, upper: Option<&[u8]>) -> bool {
    lower.map_or(true, |lower| key >= lower) && upper.map_or(true, |upper| key <= upper)
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
    fn minor_compaction_merges_l1_and_cleans_tombstones() {
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

        let reopened = PersistentIndex::new(dir.path()).unwrap();
        assert!(reopened.l2_file.is_some());
        assert!(reopened.l1_files.len() <= 1);
        assert_eq!(reopened.get(b"k1").unwrap(), None);
        assert_eq!(
            reopened.get(b"k4").unwrap(),
            Some(RowID::new(1, crate::rowset::SegmentRowId::from_raw(4)))
        );
    }

    #[test]
    fn rejects_legacy_snapshot_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("sst_1.sst"), b"legacy").unwrap();

        let pi = PersistentIndex::new(dir.path()).unwrap();
        assert!(pi.load().is_err());
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

        let pi = PersistentIndex::new(dir.path()).unwrap();
        assert!(pi.load().is_err());
    }

    #[test]
    fn reset_clears_obsolete_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("wal_0.wal"), b"legacy").unwrap();
        fs::write(dir.path().join("sst_1.sst"), b"legacy").unwrap();

        let pi = PersistentIndex::new(dir.path()).unwrap();
        pi.reset().unwrap();

        assert!(dir.path().exists());
        assert!(fs::read_dir(dir.path()).unwrap().next().is_none());
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
