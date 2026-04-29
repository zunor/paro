// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};

use super::capability::{ArtifactSegmentRef, CoverageState, SearchArtifactRef};
use super::cursor::GenerationArtifactSet;
use super::stats::{
    BuildEpoch, ExecutionModes, GenerationMaintenanceState, GenerationStats, SearchGenerationId,
};
use super::tail::TailPendingEntry;

pub(crate) const DELTA_COUNT_SOFT_LIMIT: usize = 32;
pub(crate) const DELTA_COUNT_HARD_LIMIT: usize = 128;
pub(crate) const DELTA_BYTES_SOFT_LIMIT: u64 = 64 * 1024 * 1024;
pub(crate) const DELTA_BYTES_HARD_LIMIT: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GenerationManifestRoot {
    pub definition_id: u64,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    pub build_snapshot_version: i64,
    pub indexed_through_ts: u64,
    pub config_fingerprint: u64,
    pub coverage: CoverageState,
    pub generation_stats: GenerationStats,
    pub execution_modes: ExecutionModes,
    pub tail_pending_entries: Vec<TailPendingEntry>,
    pub maintenance_state: GenerationMaintenanceState,
    pub root_version: u64,
    pub checksum: u64,
    pub shard_files: Vec<String>,
    pub recent_delta_files: Vec<String>,
}

impl GenerationManifestRoot {
    pub(crate) fn recompute_checksum(&mut self) -> Result<()> {
        self.checksum = 0;
        self.checksum = checksum_bytes(&serde_json::to_vec(self).map_err(|err| {
            paro_error::serialization_error(format!("serialize generation manifest root: {err}"))
        })?);
        Ok(())
    }

    pub(crate) fn delta_window_bytes(&self, definition_dir: &Path) -> u64 {
        self.recent_delta_files
            .iter()
            .filter_map(|name| {
                fs::metadata(definition_dir.join(name))
                    .ok()
                    .map(|meta| meta.len())
            })
            .sum()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ManifestShard {
    pub artifact_refs: Vec<SearchArtifactRef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ManifestDelta {
    pub added_artifacts: Vec<SearchArtifactRef>,
    pub removed_segments: Vec<ArtifactSegmentRef>,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedManifest {
    pub root: GenerationManifestRoot,
    pub root_path: PathBuf,
    pub shard_paths: Vec<PathBuf>,
    pub delta_paths: Vec<PathBuf>,
    pub artifacts: GenerationArtifactSet,
}

impl LoadedManifest {
    pub(crate) fn all_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(1 + self.shard_paths.len() + self.delta_paths.len());
        paths.push(self.root_path.clone());
        paths.extend(self.shard_paths.iter().cloned());
        paths.extend(self.delta_paths.iter().cloned());
        paths
    }
}

pub(crate) struct ManifestStore {
    table_data_dir: PathBuf,
}

impl ManifestStore {
    pub(crate) fn new(table_data_dir: impl Into<PathBuf>) -> Self {
        Self {
            table_data_dir: table_data_dir.into(),
        }
    }

    pub(crate) fn definition_dir(&self, definition_id: u64) -> PathBuf {
        self.table_data_dir
            .join("search_registry")
            .join("definitions")
            .join(definition_id.to_string())
    }

    pub(crate) fn root_path(&self, definition_id: u64) -> PathBuf {
        self.definition_dir(definition_id)
            .join("manifest_root.json")
    }

    pub(crate) fn write_root(
        &self,
        definition_id: u64,
        root: &GenerationManifestRoot,
    ) -> Result<PathBuf> {
        let path = self.root_path(definition_id);
        self.write_json(&path, root)?;
        Ok(path)
    }

    pub(crate) fn write_shard(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
        root_version: u64,
        shard: &ManifestShard,
    ) -> Result<String> {
        let file_name = format!("shard_g{generation_id}_v{root_version}.json");
        let path = self.definition_dir(definition_id).join(&file_name);
        self.write_json(&path, shard)?;
        Ok(file_name)
    }

    pub(crate) fn write_delta(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
        root_version: u64,
        ordinal: usize,
        delta: &ManifestDelta,
    ) -> Result<String> {
        let file_name = format!("delta_g{generation_id}_v{root_version}_{ordinal}.json");
        let path = self.definition_dir(definition_id).join(&file_name);
        self.write_json(&path, delta)?;
        Ok(file_name)
    }

    pub(crate) fn load_manifest(&self, definition_id: u64) -> Result<Option<LoadedManifest>> {
        let root_path = self.root_path(definition_id);
        if !root_path.exists() {
            return Ok(None);
        }

        let root = self.read_json::<GenerationManifestRoot>(&root_path)?;
        let mut verified = root.clone();
        let expected_checksum = verified.checksum;
        verified.recompute_checksum()?;
        if verified.checksum != expected_checksum {
            return Err(paro_error::invalid_input(format!(
                "search manifest checksum mismatch for definition {}",
                definition_id
            )));
        }

        let definition_dir = self.definition_dir(definition_id);
        let shard_paths = root
            .shard_files
            .iter()
            .map(|name| definition_dir.join(name))
            .collect::<Vec<_>>();
        let delta_paths = root
            .recent_delta_files
            .iter()
            .map(|name| definition_dir.join(name))
            .collect::<Vec<_>>();
        let artifacts = self.load_artifacts(definition_id, &root)?;

        Ok(Some(LoadedManifest {
            root,
            root_path,
            shard_paths,
            delta_paths,
            artifacts,
        }))
    }

    pub(crate) fn materialize_loaded_manifest(
        &self,
        definition_id: u64,
        root: GenerationManifestRoot,
        artifacts: GenerationArtifactSet,
    ) -> LoadedManifest {
        let definition_dir = self.definition_dir(definition_id);
        LoadedManifest {
            root_path: self.root_path(definition_id),
            shard_paths: root
                .shard_files
                .iter()
                .map(|name| definition_dir.join(name))
                .collect(),
            delta_paths: root
                .recent_delta_files
                .iter()
                .map(|name| definition_dir.join(name))
                .collect(),
            root,
            artifacts,
        }
    }

    pub(crate) fn load_artifacts(
        &self,
        definition_id: u64,
        root: &GenerationManifestRoot,
    ) -> Result<GenerationArtifactSet> {
        let definition_dir = self.definition_dir(definition_id);
        let mut artifact_map = BTreeMap::<(u64, u32, u32), SearchArtifactRef>::new();

        for shard_name in &root.shard_files {
            let shard = self.read_json::<ManifestShard>(&definition_dir.join(shard_name))?;
            for artifact in shard.artifact_refs {
                artifact_map.insert(artifact_key(&artifact), artifact);
            }
        }

        for delta_name in &root.recent_delta_files {
            let delta = self.read_json::<ManifestDelta>(&definition_dir.join(delta_name))?;
            for removed in delta.removed_segments {
                artifact_map.retain(|(rowset_id, segment_id, _), _| {
                    *rowset_id != removed.rowset_id || *segment_id != removed.segment_id
                });
            }
            for artifact in delta.added_artifacts {
                artifact_map.insert(artifact_key(&artifact), artifact);
            }
        }

        Ok(GenerationArtifactSet {
            artifacts: artifact_map.into_values().collect(),
        })
    }

    pub(crate) fn remove_paths(&self, paths: &[PathBuf]) {
        for path in paths {
            let _ = fs::remove_file(path);
        }
    }

    pub(crate) fn definition_paths(&self, definition_id: u64) -> Vec<PathBuf> {
        let root_path = self.root_path(definition_id);
        let Some(parent) = root_path.parent() else {
            return vec![root_path];
        };
        if let Ok(entries) = fs::read_dir(parent) {
            let mut paths = entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| path.is_file())
                .collect::<Vec<_>>();
            paths.sort();
            return paths;
        }
        vec![root_path]
    }

    pub(crate) fn maybe_compact_deltas(
        &self,
        definition_id: u64,
        root: &mut GenerationManifestRoot,
    ) -> Result<()> {
        let definition_dir = self.definition_dir(definition_id);
        let delta_bytes = root.delta_window_bytes(&definition_dir);
        let delta_count = root.recent_delta_files.len();
        if delta_count <= DELTA_COUNT_SOFT_LIMIT && delta_bytes <= DELTA_BYTES_SOFT_LIMIT {
            return Ok(());
        }

        let artifacts = self.load_artifacts(definition_id, root)?;
        root.root_version = root.root_version.saturating_add(1);
        let shard = ManifestShard {
            artifact_refs: artifacts.artifacts,
        };
        let shard_name =
            self.write_shard(definition_id, root.generation_id, root.root_version, &shard)?;
        let old_delta_paths = root
            .recent_delta_files
            .iter()
            .map(|name| definition_dir.join(name))
            .collect::<Vec<_>>();

        root.shard_files = vec![shard_name];
        root.recent_delta_files.clear();
        root.recompute_checksum()?;
        self.write_root(definition_id, root)?;
        self.remove_paths(&old_delta_paths);

        if delta_count > DELTA_COUNT_HARD_LIMIT || delta_bytes > DELTA_BYTES_HARD_LIMIT {
            tracing::warn!(
                definition_id,
                delta_count,
                delta_bytes,
                "search manifest delta window exceeded hard threshold before compaction"
            );
        }
        Ok(())
    }

    fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                paro_error::internal(format!(
                    "create search manifest parent dir {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        }
        let bytes = serde_json::to_vec_pretty(value).map_err(|err| {
            paro_error::serialization_error(format!(
                "serialize search manifest {}: {err}",
                path.display()
            ))
        })?;
        fs::write(path, bytes).map_err(|err| {
            paro_error::internal(format!("write search manifest {}: {}", path.display(), err))
        })
    }

    fn read_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> Result<T> {
        let bytes = fs::read(path).map_err(|err| {
            paro_error::internal(format!("read search manifest {}: {}", path.display(), err))
        })?;
        serde_json::from_slice(&bytes).map_err(|err| {
            paro_error::serialization_error(format!(
                "deserialize search manifest {}: {err}",
                path.display()
            ))
        })
    }
}

fn artifact_key(artifact: &SearchArtifactRef) -> (u64, u32, u32) {
    (
        artifact.segment.rowset_id,
        artifact.segment.segment_id,
        artifact.column_id,
    )
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    seahash::hash(bytes)
}
