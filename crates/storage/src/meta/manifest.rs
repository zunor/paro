// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::tablet::{TabletId, TabletMeta, TabletState};
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Manifest entry for one tablet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TabletEntry {
    pub tablet_id: TabletId,
    pub table_id: u64,
    pub data_dir: String,
    pub state: TabletState,
    pub schema_hash: u32,
}

impl TabletEntry {
    pub fn from_tablet_meta(meta: &TabletMeta) -> Self {
        Self {
            tablet_id: meta.tablet_id(),
            table_id: meta.table_id(),
            data_dir: meta.data_dir().to_string(),
            state: meta.tablet_state(),
            schema_hash: meta.schema_hash(),
        }
    }

    pub fn matches_meta(&self, meta: &TabletMeta) -> bool {
        self.tablet_id == meta.tablet_id()
            && self.table_id == meta.table_id()
            && self.data_dir == meta.data_dir()
            && self.state == meta.tablet_state()
            && self.schema_hash == meta.schema_hash()
    }
}

/// Startup manifest for fast tablet discovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StorageManifest {
    pub tablets: Vec<TabletEntry>,
    pub last_updated: i64,
    pub format_version: u16,
}

impl StorageManifest {
    pub const CURRENT_FORMAT_VERSION: u16 = 1;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_tablet_metas(metas: &[TabletMeta]) -> Self {
        let mut tablets: Vec<TabletEntry> = metas
            .iter()
            .filter(|meta| meta.tablet_state() != TabletState::Shutdown)
            .map(TabletEntry::from_tablet_meta)
            .collect();
        tablets.sort_by_key(|entry| entry.tablet_id);
        Self {
            tablets,
            last_updated: current_timestamp(),
            format_version: Self::CURRENT_FORMAT_VERSION,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to read StorageManifest from {:?}: {}",
                path, e
            ))
        })?;
        Self::from_json_bytes(&bytes)
    }

    pub fn load_or_default(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }
        Self::load(path)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create StorageManifest parent directory {:?}: {}",
                    parent, e
                ))
            })?;
        }

        let tmp_path = temp_path(path);
        let payload = self.to_json_bytes()?;

        let mut file = File::create(&tmp_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create StorageManifest temp file {:?}: {}",
                tmp_path, e
            ))
        })?;
        file.write_all(&payload).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to write StorageManifest temp file {:?}: {}",
                tmp_path, e
            ))
        })?;
        file.sync_all().map_err(|e| {
            paro_error::io_error(format!(
                "Failed to fsync StorageManifest temp file {:?}: {}",
                tmp_path, e
            ))
        })?;

        fs::rename(&tmp_path, path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            paro_error::io_error(format!(
                "Failed to atomically save StorageManifest {:?} -> {:?}: {}",
                tmp_path, path, e
            ))
        })?;

        Ok(())
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self).map_err(|e| {
            paro_error::internal(format!(
                "Failed to serialize StorageManifest to JSON: {}",
                e
            ))
        })
    }

    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let mut manifest: StorageManifest = serde_json::from_slice(bytes).map_err(|e| {
            paro_error::invalid_input(format!("Failed to deserialize StorageManifest JSON: {}", e))
        })?;
        manifest.normalize();
        Ok(manifest)
    }

    /// Add or replace a tablet entry by tablet_id.
    pub fn add_tablet(&mut self, entry: TabletEntry) {
        if let Some(existing) = self
            .tablets
            .iter_mut()
            .find(|existing| existing.tablet_id == entry.tablet_id)
        {
            *existing = entry;
        } else {
            self.tablets.push(entry);
        }
        self.touch();
    }

    pub fn add_tablet_meta(&mut self, meta: &TabletMeta) {
        if meta.tablet_state() == TabletState::Shutdown {
            self.remove_tablet(meta.tablet_id());
            return;
        }
        self.add_tablet(TabletEntry::from_tablet_meta(meta));
    }

    pub fn remove_tablet(&mut self, tablet_id: TabletId) -> bool {
        let before = self.tablets.len();
        self.tablets.retain(|entry| entry.tablet_id != tablet_id);
        let changed = before != self.tablets.len();
        if changed {
            self.touch();
        }
        changed
    }

    pub fn update_tablet_state(&mut self, tablet_id: TabletId, state: TabletState) -> bool {
        if state == TabletState::Shutdown {
            return self.remove_tablet(tablet_id);
        }

        let mut changed = false;
        if let Some(entry) = self
            .tablets
            .iter_mut()
            .find(|entry| entry.tablet_id == tablet_id)
        {
            if entry.state != state {
                entry.state = state;
                changed = true;
            }
        }

        if changed {
            self.touch();
        }
        changed
    }

    pub fn get_tablet(&self, tablet_id: TabletId) -> Option<&TabletEntry> {
        self.tablets
            .iter()
            .find(|entry| entry.tablet_id == tablet_id)
    }

    fn touch(&mut self) {
        self.last_updated = current_timestamp();
        self.normalize();
    }

    fn normalize(&mut self) {
        self.tablets.sort_by_key(|entry| entry.tablet_id);
        self.tablets.dedup_by_key(|entry| entry.tablet_id);
        self.format_version = Self::CURRENT_FORMAT_VERSION;
    }
}

impl Default for StorageManifest {
    fn default() -> Self {
        Self {
            tablets: Vec::new(),
            last_updated: current_timestamp(),
            format_version: Self::CURRENT_FORMAT_VERSION,
        }
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn temp_path(path: &Path) -> PathBuf {
    let sequence = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id();
    let file_name = path
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_else(|| "tablets.json".to_string());
    path.with_file_name(format!("{}.tmp-{}-{}", file_name, pid, sequence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{KeysType, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn build_meta(tablet_id: TabletId, table_id: u64, state: TabletState) -> TabletMeta {
        let schema = Arc::new(
            TabletSchema::with_version(
                777,
                3,
                vec![
                    TabletColumn::key(0, "id", LogicalType::BigInt),
                    TabletColumn::new(1, "value", LogicalType::Varchar),
                ],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );

        let mut meta = TabletMeta::new(
            tablet_id,
            table_id,
            9,
            schema,
            format!("/tmp/tablet-{tablet_id}"),
        )
        .unwrap();
        meta.set_tablet_state(state);
        meta
    }

    #[test]
    fn storage_manifest_roundtrip_test() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("manifest/tablets.json");

        let mut manifest = StorageManifest::new();
        manifest.add_tablet_meta(&build_meta(2, 200, TabletState::Running));
        manifest.add_tablet_meta(&build_meta(1, 100, TabletState::NotReady));

        manifest.save(&path).unwrap();
        let loaded = StorageManifest::load(&path).unwrap();

        assert_eq!(loaded.tablets.len(), 2);
        assert_eq!(loaded.tablets[0].tablet_id, 1);
        assert_eq!(loaded.tablets[1].tablet_id, 2);
        assert_eq!(
            loaded.format_version,
            StorageManifest::CURRENT_FORMAT_VERSION
        );
    }

    #[test]
    fn storage_manifest_mutation_test() {
        let mut manifest = StorageManifest::new();
        manifest.add_tablet_meta(&build_meta(10, 88, TabletState::NotReady));
        manifest.add_tablet_meta(&build_meta(11, 99, TabletState::Running));

        assert_eq!(manifest.tablets.len(), 2);
        assert!(manifest.update_tablet_state(10, TabletState::Running));
        assert_eq!(manifest.get_tablet(10).unwrap().state, TabletState::Running);

        assert!(manifest.remove_tablet(11));
        assert!(!manifest.remove_tablet(999));
        assert_eq!(manifest.tablets.len(), 1);
        assert_eq!(manifest.tablets[0].tablet_id, 10);
    }

    #[test]
    fn storage_manifest_json_bytes_roundtrip_test() {
        let metas = vec![
            build_meta(5, 50, TabletState::Running),
            build_meta(3, 30, TabletState::Shutdown),
        ];

        let manifest = StorageManifest::from_tablet_metas(&metas);
        let payload = manifest.to_json_bytes().unwrap();
        let restored = StorageManifest::from_json_bytes(&payload).unwrap();

        assert_eq!(restored.tablets.len(), 1);
        assert_eq!(restored.tablets[0].tablet_id, 5);
    }

    #[test]
    fn storage_manifest_shutdown_update_removes_tablet_test() {
        let mut manifest = StorageManifest::new();
        manifest.add_tablet_meta(&build_meta(10, 88, TabletState::Running));

        assert!(manifest.update_tablet_state(10, TabletState::Shutdown));
        assert!(manifest.get_tablet(10).is_none());
    }
}
