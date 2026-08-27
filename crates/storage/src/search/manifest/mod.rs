// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::metrics::storage_metrics;
use crate::tablet::SearchGenerationHeadMeta;
use paro_common::effect::{ArtifactNamespace, ArtifactRef};
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};

use super::artifact::ArtifactLocation;
use super::capability::{
    CoverageState, SearchArtifactRef, SearchIndexKind, SearchPartitionCoverage,
};
use super::cursor::GenerationArtifactSet;
use super::inline_sink::SearchStatsDelta;
mod binary;

use self::binary::{
    decode_binary_manifest_fragment, encode_binary_manifest_fragment,
    encode_manifest_root_checksum_image, BinaryManifestFragment,
};
use super::sidecar::SidecarArtifactStore;
use super::stats::{
    BuildEpoch, ExecutionModes, GenerationMaintenanceState, GenerationStats, SearchGenerationId,
};
use super::tail::{TailEntryId, TailMutationKind, TailPendingEntry, TailRowImageRef};

pub(crate) const DELTA_COUNT_SOFT_LIMIT: usize = 32;
pub(crate) const DELTA_COUNT_HARD_LIMIT: usize = 128;
pub(crate) const DELTA_BYTES_SOFT_LIMIT: u64 = 64 * 1024 * 1024;
pub(crate) const DELTA_BYTES_HARD_LIMIT: u64 = 256 * 1024 * 1024;
static MANIFEST_STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ManifestCodecFamily {
    JsonDebug,
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct ManifestCodecKind {
    pub family: ManifestCodecFamily,
    pub version: u32,
}

impl ManifestCodecKind {
    pub(crate) const JSON_DEBUG_V4: Self = Self {
        family: ManifestCodecFamily::JsonDebug,
        version: 4,
    };

    pub(crate) const BINARY_V4: Self = Self {
        family: ManifestCodecFamily::Binary,
        version: 4,
    };

    pub(crate) const fn metric_label(self) -> &'static str {
        match (self.family, self.version) {
            (ManifestCodecFamily::JsonDebug, 4) => "json-debug-v4",
            (ManifestCodecFamily::Binary, 4) => "binary-v4",
            _ => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ManifestFileRef {
    pub file_name: String,
    pub codec: ManifestCodecKind,
}

impl ManifestFileRef {
    fn new(file_name: String, codec: ManifestCodecKind) -> Self {
        Self { file_name, codec }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct GenerationManifestRoot {
    pub definition_id: u64,
    pub generation_id: SearchGenerationId,
    pub build_epoch: BuildEpoch,
    pub build_snapshot_version: i64,
    pub indexed_through_ts: u64,
    pub config_fingerprint: u64,
    pub coverage: CoverageState,
    pub generation_stats: GenerationStats,
    /// Durable allocator seed. Runtime callers must use
    /// `LoadedManifest::next_tail_entry_id()` because recovery may derive a
    /// larger value after degrading missing artifacts to tail entries.
    pub persisted_tail_entry_id_seed: TailEntryId,
    pub execution_modes: ExecutionModes,
    pub maintenance_state: GenerationMaintenanceState,
    pub root_version: u64,
    pub checksum: u64,
    pub shard_files: Vec<ManifestFileRef>,
    pub recent_delta_files: Vec<ManifestFileRef>,
}

impl GenerationManifestRoot {
    pub(crate) fn recompute_checksum(&mut self) -> Result<()> {
        // The integrity image is an explicit, versioned binary schema rather
        // than a serde representation. Floats therefore participate by exact
        // bit pattern, and JSON field ordering/renames cannot silently change
        // the durable checksum contract.
        let bytes = encode_manifest_root_checksum_image(self)?;
        self.checksum = checksum_bytes(&bytes);
        Ok(())
    }

    pub(crate) fn delta_window_bytes(&self, definition_dir: &Path) -> u64 {
        self.recent_delta_files
            .iter()
            .filter_map(|file| {
                fs::metadata(definition_dir.join(&file.file_name))
                    .ok()
                    .map(|meta| meta.len())
            })
            .sum()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ManifestShard {
    pub artifact_refs: Vec<SearchArtifactRef>,
    pub tail_pending_entries: Vec<TailPendingEntry>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestDelta {
    pub entries: Vec<ManifestDeltaEntry>,
}

impl ManifestDelta {
    pub(crate) fn new(entries: Vec<ManifestDeltaEntry>) -> Self {
        Self { entries }
    }

    pub(crate) fn publish_changes(
        artifacts: Vec<SearchArtifactRef>,
        tail_entries: Vec<TailPendingEntry>,
        stats_deltas: Vec<SearchStatsDelta>,
    ) -> Self {
        let mut entries =
            Vec::with_capacity(artifacts.len() + tail_entries.len() + stats_deltas.len());
        entries.extend(artifacts.into_iter().map(ManifestDeltaEntry::AddArtifact));
        entries.extend(tail_entries.into_iter().map(ManifestDeltaEntry::UpsertTail));
        entries.extend(stats_deltas.into_iter().map(ManifestDeltaEntry::StatsDelta));
        Self::new(entries)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", content = "payload", rename_all = "snake_case")]
pub(crate) enum ManifestDeltaEntry {
    AddArtifact(SearchArtifactRef),
    RemoveArtifact(SearchPartitionCoverage),
    UpsertTail(TailPendingEntry),
    CoverTail(TailEntryId),
    StatsDelta(SearchStatsDelta),
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedManifest {
    pub root: GenerationManifestRoot,
    pub root_path: PathBuf,
    pub shard_paths: Vec<PathBuf>,
    pub delta_paths: Vec<PathBuf>,
    /// Runtime allocator state. Callers consume it through
    /// `next_tail_entry_id`; crate visibility is retained only for focused
    /// construction in storage tests.
    pub(crate) tail_entry_id_allocator: TailEntryId,
    pub(crate) publication_lease: Option<SearchManifestRevisionLease>,
    /// Shared by immutable registry snapshots, query leases, and the retirement queue.
    /// Keeping one ownership token makes artifact reclamation wait for every reader and
    /// keeps copy-on-write view publication from cloning the complete artifact list.
    pub artifacts: Arc<GenerationArtifactSet>,
    pub tail_pending_entries: Vec<TailPendingEntry>,
}

impl LoadedManifest {
    pub(crate) fn all_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(1 + self.shard_paths.len() + self.delta_paths.len());
        paths.push(self.root_path.clone());
        paths.extend(self.shard_paths.iter().cloned());
        paths.extend(self.delta_paths.iter().cloned());
        paths
    }

    pub(crate) fn next_tail_entry_id(&self) -> TailEntryId {
        self.tail_entry_id_allocator
    }

    pub(crate) fn mark_revision_published(&self) {
        if let Some(lease) = &self.publication_lease {
            lease.mark_published();
        }
    }

    pub(crate) fn rollback_owned_paths(&self) -> BTreeSet<PathBuf> {
        self.publication_lease
            .as_ref()
            .map_or_else(BTreeSet::new, |lease| {
                lease.inner.created_paths.iter().cloned().collect()
            })
    }
}

#[derive(Clone)]
struct MaterializedManifestState {
    artifacts: Arc<GenerationArtifactSet>,
    tail_pending_entries: Vec<TailPendingEntry>,
    next_tail_entry_id: TailEntryId,
}

type ArtifactManifestKey = (SearchPartitionCoverage, u32, SearchIndexKind, u32);

#[derive(Clone)]
pub(crate) struct ManifestStore {
    table_data_dir: PathBuf,
    codec_kind: ManifestCodecKind,
    #[cfg(test)]
    full_replay_count: Arc<AtomicU64>,
}

/// Owns every immutable fragment prepared for one manifest revision.
///
/// A revision number is allocated once from both durable state and the
/// installed namespace. Every fragment remains rollback-owned until the root
/// has been installed and acknowledged by the durable head publication. The
/// materialized view is updated from the same typed delta that is persisted;
/// commit only reads back the bounded root fragment. Recovery replay remains
/// the sole path that replays the shard and delta window or repairs missing
/// sidecars.
pub(crate) struct StagedManifestRevision<'a> {
    store: &'a ManifestStore,
    definition_id: u64,
    root: GenerationManifestRoot,
    materialized: MaterializedManifestState,
    unpublished_paths: Vec<PathBuf>,
    absorbed_created_paths: Vec<PathBuf>,
    compaction_checked: bool,
    layout_replaced: bool,
    committed: bool,
}

#[derive(Clone)]
pub(crate) struct SearchManifestRevisionLease {
    inner: Arc<SearchManifestRevisionLeaseInner>,
}

impl std::fmt::Debug for SearchManifestRevisionLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SearchManifestRevisionLease")
            .field("created_path_count", &self.inner.created_paths.len())
            .finish_non_exhaustive()
    }
}

struct SearchManifestRevisionLeaseInner {
    store: ManifestStore,
    created_paths: Vec<PathBuf>,
    published: std::sync::atomic::AtomicBool,
}

impl SearchManifestRevisionLease {
    pub(crate) fn mark_published(&self) {
        self.inner
            .published
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for SearchManifestRevisionLeaseInner {
    fn drop(&mut self) {
        if !self.published.load(std::sync::atomic::Ordering::Acquire) {
            self.store.remove_paths(&self.created_paths);
        }
    }
}

impl StagedManifestRevision<'_> {
    pub(crate) fn append_delta(&mut self, delta: &ManifestDelta) -> Result<()> {
        if self.compaction_checked || self.layout_replaced {
            return Err(paro_error::internal(
                "cannot append a search manifest delta after revision layout was finalized",
            ));
        }
        let materialized =
            apply_manifest_delta(self.definition_id, &self.root, &self.materialized, delta)?;
        let delta_ref = self.store.write_delta(
            self.definition_id,
            self.root.generation_id,
            self.root.root_version,
            self.root.recent_delta_files.len(),
            delta,
        )?;
        let path = self
            .store
            .generation_dir(self.definition_id, self.root.generation_id)
            .join(&delta_ref.file_name);
        self.unpublished_paths.push(path);
        self.root.recent_delta_files.push(delta_ref);
        self.materialized = materialized;
        Ok(())
    }

    pub(crate) fn replace_with_shard(&mut self, shard: &ManifestShard) -> Result<()> {
        if self.layout_replaced {
            return Err(paro_error::internal(
                "search manifest revision layout may only be replaced once",
            ));
        }
        let materialized =
            materialized_state_from_published_shard(self.definition_id, &self.root, shard)?;
        let shard_ref = self.store.write_shard(
            self.definition_id,
            self.root.generation_id,
            self.root.root_version,
            shard,
        )?;
        let path = self
            .store
            .generation_dir(self.definition_id, self.root.generation_id)
            .join(&shard_ref.file_name);
        self.unpublished_paths.push(path);
        self.root.shard_files = vec![shard_ref];
        self.root.recent_delta_files.clear();
        self.materialized = materialized;
        self.layout_replaced = true;
        Ok(())
    }

    pub(crate) fn compact_if_needed(&mut self) -> Result<bool> {
        self.compaction_checked = true;
        let definition_dir = self
            .store
            .generation_dir(self.definition_id, self.root.generation_id);
        let delta_bytes = self.root.delta_window_bytes(&definition_dir);
        let delta_count = self.root.recent_delta_files.len();
        if delta_count <= DELTA_COUNT_SOFT_LIMIT && delta_bytes <= DELTA_BYTES_SOFT_LIMIT {
            return Ok(false);
        }

        let absorbed_delta_paths = self
            .root
            .recent_delta_files
            .iter()
            .map(|file| definition_dir.join(&file.file_name))
            .collect::<std::collections::BTreeSet<_>>();
        self.absorbed_created_paths.extend(
            self.unpublished_paths
                .iter()
                .filter(|path| absorbed_delta_paths.contains(*path))
                .cloned(),
        );
        let shard = ManifestShard {
            artifact_refs: self.materialized.artifacts.artifacts.clone(),
            tail_pending_entries: self.materialized.tail_pending_entries.clone(),
        };
        self.replace_with_shard(&shard)?;

        if delta_count > DELTA_COUNT_HARD_LIMIT || delta_bytes > DELTA_BYTES_HARD_LIMIT {
            tracing::warn!(
                definition_id = self.definition_id,
                delta_count,
                delta_bytes,
                "search manifest delta window exceeded hard threshold before compaction"
            );
        }
        Ok(true)
    }

    pub(crate) fn commit(mut self) -> Result<LoadedManifest> {
        if !self.compaction_checked {
            self.compact_if_needed()?;
        }
        self.root.persisted_tail_entry_id_seed = self.materialized.next_tail_entry_id;
        self.root.recompute_checksum()?;
        let root_path = self.store.write_root(self.definition_id, &self.root)?;
        self.unpublished_paths.push(root_path.clone());
        let persisted_root = self.store.read_root_fragment(&root_path)?;
        if persisted_root != self.root {
            return Err(paro_error::data_corrupted(format!(
                "newly committed search manifest root for definition {} changed during verification",
                self.definition_id
            )));
        }

        self.store.remove_paths(&self.absorbed_created_paths);
        let lease = SearchManifestRevisionLease {
            inner: Arc::new(SearchManifestRevisionLeaseInner {
                store: self.store.clone(),
                created_paths: self.unpublished_paths.clone(),
                published: std::sync::atomic::AtomicBool::new(false),
            }),
        };
        let definition_dir = self
            .store
            .generation_dir(self.definition_id, self.root.generation_id);
        let loaded = LoadedManifest {
            root: self.root.clone(),
            root_path,
            shard_paths: self
                .root
                .shard_files
                .iter()
                .map(|file| definition_dir.join(&file.file_name))
                .collect(),
            delta_paths: self
                .root
                .recent_delta_files
                .iter()
                .map(|file| definition_dir.join(&file.file_name))
                .collect(),
            tail_entry_id_allocator: self.materialized.next_tail_entry_id,
            publication_lease: Some(lease),
            artifacts: self.materialized.artifacts.clone(),
            tail_pending_entries: self.materialized.tail_pending_entries.clone(),
        };
        self.committed = true;
        Ok(loaded)
    }
}

fn materialized_state_from_published_shard(
    definition_id: u64,
    root: &GenerationManifestRoot,
    shard: &ManifestShard,
) -> Result<MaterializedManifestState> {
    let artifacts = GenerationArtifactSet::try_new(shard.artifact_refs.clone())?;
    artifacts.validate_for_generation(definition_id, root.generation_id)?;
    let mut tail_map = BTreeMap::new();
    for entry in shard.tail_pending_entries.iter().cloned() {
        upsert_tail_entry(definition_id, &mut tail_map, entry)?;
    }
    let tail_pending_entries = tail_map.into_values().collect::<Vec<_>>();
    validate_tail_entry_id_allocator(
        definition_id,
        root.persisted_tail_entry_id_seed,
        &tail_pending_entries,
    )?;
    Ok(MaterializedManifestState {
        artifacts: Arc::new(artifacts),
        tail_pending_entries,
        next_tail_entry_id: root.persisted_tail_entry_id_seed,
    })
}

fn apply_manifest_delta(
    definition_id: u64,
    root: &GenerationManifestRoot,
    current: &MaterializedManifestState,
    delta: &ManifestDelta,
) -> Result<MaterializedManifestState> {
    let mut artifact_map = current
        .artifacts
        .artifacts
        .iter()
        .cloned()
        .map(|artifact| (artifact_key(&artifact), artifact))
        .collect::<BTreeMap<_, _>>();
    let mut tail_map = current
        .tail_pending_entries
        .iter()
        .cloned()
        .map(|entry| (entry.entry_id, entry))
        .collect::<BTreeMap<_, _>>();
    for entry in &delta.entries {
        match entry {
            ManifestDeltaEntry::AddArtifact(artifact) => {
                artifact.validate()?;
                artifact_map.insert(artifact_key(artifact), artifact.clone());
            }
            ManifestDeltaEntry::RemoveArtifact(removed) => {
                artifact_map.retain(|(coverage, _, _, _), _| coverage != removed);
            }
            ManifestDeltaEntry::UpsertTail(tail_entry) => {
                upsert_tail_entry(definition_id, &mut tail_map, tail_entry.clone())?;
            }
            ManifestDeltaEntry::CoverTail(entry_id) => {
                tail_map.remove(entry_id);
            }
            ManifestDeltaEntry::StatsDelta(_) => {}
        }
    }
    let artifacts = GenerationArtifactSet::try_new(artifact_map.into_values().collect())?;
    artifacts.validate_for_generation(definition_id, root.generation_id)?;
    let tail_pending_entries = tail_map.into_values().collect::<Vec<_>>();
    validate_tail_entry_id_allocator(
        definition_id,
        root.persisted_tail_entry_id_seed,
        &tail_pending_entries,
    )?;
    Ok(MaterializedManifestState {
        artifacts: Arc::new(artifacts),
        tail_pending_entries,
        next_tail_entry_id: root.persisted_tail_entry_id_seed,
    })
}

impl Drop for StagedManifestRevision<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.store.remove_paths(&self.unpublished_paths);
        }
    }
}

impl ManifestStore {
    pub(crate) fn new(table_data_dir: impl Into<PathBuf>) -> Self {
        Self::new_with_codec(table_data_dir, ManifestCodecKind::JSON_DEBUG_V4)
    }

    pub(crate) fn new_with_codec(
        table_data_dir: impl Into<PathBuf>,
        codec_kind: ManifestCodecKind,
    ) -> Self {
        Self {
            table_data_dir: table_data_dir.into(),
            codec_kind,
            #[cfg(test)]
            full_replay_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn codec_label(&self) -> &'static str {
        self.codec_kind.metric_label()
    }

    #[cfg(test)]
    pub(crate) fn full_replay_count(&self) -> u64 {
        self.full_replay_count.load(Ordering::Relaxed)
    }

    /// Private workspace root for a transaction-owned generation build.
    /// Keeping this layout next to the final generation layout prevents the
    /// builder and replay validator from inventing parallel path contracts.
    pub(crate) fn staged_generation_workspace(
        &self,
        txn_id: u64,
        definition_id: u64,
        generation_id: SearchGenerationId,
    ) -> PathBuf {
        self.table_data_dir
            .join("_staged")
            .join("search-generation")
            .join(format!(
                "txn-{txn_id}-def-{definition_id}-gen-{generation_id}"
            ))
    }

    pub(crate) fn generation_ref(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
    ) -> Result<ArtifactRef> {
        ArtifactRef::from_tablet_path(
            &self.table_data_dir,
            &self.generation_dir(definition_id, generation_id),
        )
    }

    pub(crate) fn validate_staged_generation_ref(
        &self,
        staged_ref: &ArtifactRef,
        head: &SearchGenerationHeadMeta,
    ) -> Result<()> {
        let suffix = [
            "search_registry".to_string(),
            "definitions".to_string(),
            head.definition_id.to_string(),
            "generations".to_string(),
            format!("g{}", head.generation_id),
        ];
        if staged_ref.namespace != ArtifactNamespace::Staged
            || staged_ref.locator.first().map(String::as_str) != Some("search-generation")
            || !staged_ref.locator.ends_with(&suffix)
        {
            return Err(paro_error::invalid_input(
                "search generation staging reference does not match its durable identity",
            ));
        }
        Ok(())
    }

    /// Stable container for all immutable generations of one definition.
    pub(crate) fn definition_dir(&self, definition_id: u64) -> PathBuf {
        self.table_data_dir
            .join("search_registry")
            .join("definitions")
            .join(definition_id.to_string())
    }

    /// Immutable namespace installed atomically by `PublishSearchGeneration`.
    pub(crate) fn generation_dir(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
    ) -> PathBuf {
        self.definition_dir(definition_id)
            .join("generations")
            .join(format!("g{generation_id}"))
    }

    pub(crate) fn root_path_for_file(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
        file_name: &str,
    ) -> PathBuf {
        self.generation_dir(definition_id, generation_id)
            .join(file_name)
    }

    pub(crate) fn root_file_name(root: &GenerationManifestRoot) -> String {
        format!(
            "manifest_root_g{}_v{}_f{}.json",
            root.generation_id, root.root_version, root.config_fingerprint
        )
    }

    pub(crate) fn head_for_root(&self, root: &GenerationManifestRoot) -> SearchGenerationHeadMeta {
        SearchGenerationHeadMeta {
            definition_id: root.definition_id,
            generation_id: root.generation_id,
            root_version: root.root_version,
            config_fingerprint: root.config_fingerprint,
            root_file_name: Self::root_file_name(root),
        }
    }

    pub(crate) fn begin_empty_revision(
        &self,
        definition_id: u64,
        root: GenerationManifestRoot,
    ) -> Result<StagedManifestRevision<'_>> {
        let next_tail_entry_id = root.persisted_tail_entry_id_seed;
        self.begin_revision_with_state(
            definition_id,
            root,
            MaterializedManifestState {
                artifacts: Arc::new(GenerationArtifactSet::default()),
                tail_pending_entries: Vec::new(),
                next_tail_entry_id,
            },
        )
    }

    pub(crate) fn begin_revision_from_manifest(
        &self,
        definition_id: u64,
        root: GenerationManifestRoot,
        manifest: &LoadedManifest,
    ) -> Result<StagedManifestRevision<'_>> {
        self.begin_revision_with_state(
            definition_id,
            root,
            MaterializedManifestState {
                artifacts: manifest.artifacts.clone(),
                tail_pending_entries: manifest.tail_pending_entries.clone(),
                next_tail_entry_id: manifest.next_tail_entry_id(),
            },
        )
    }

    fn begin_revision_with_state(
        &self,
        definition_id: u64,
        mut root: GenerationManifestRoot,
        materialized: MaterializedManifestState,
    ) -> Result<StagedManifestRevision<'_>> {
        if root.definition_id != definition_id {
            return Err(paro_error::data_corrupted(format!(
                "search manifest root definition {} does not match revision owner {definition_id}",
                root.definition_id
            )));
        }
        let installed_max = self
            .greatest_fragment_version_in_generation(definition_id, root.generation_id)?
            .unwrap_or(0);
        root.root_version = root
            .root_version
            .max(installed_max)
            .checked_add(1)
            .ok_or_else(|| {
                paro_error::invalid_input(format!(
                    "search manifest root version exhausted for definition {definition_id} generation {}",
                    root.generation_id
                ))
            })?;
        Ok(StagedManifestRevision {
            store: self,
            definition_id,
            root,
            materialized,
            unpublished_paths: Vec::new(),
            absorbed_created_paths: Vec::new(),
            compaction_checked: false,
            layout_replaced: false,
            committed: false,
        })
    }

    fn greatest_fragment_version_in_generation(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
    ) -> Result<Option<u64>> {
        let generation_dir = self.generation_dir(definition_id, generation_id);
        let entries = match fs::read_dir(&generation_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(paro_error::io_error(format!(
                    "scan search manifest generation {}: {}",
                    generation_dir.display(),
                    error
                )))
            }
        };
        let mut greatest = None;
        for entry in entries {
            let entry = entry.map_err(paro_error::io)?;
            if !entry.file_type().map_err(paro_error::io)?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some((file_generation_id, root_version)) =
                name.to_str().and_then(parse_manifest_fragment_version)
            else {
                continue;
            };
            if file_generation_id != generation_id {
                return Err(paro_error::data_corrupted(format!(
                    "search manifest root {} disagrees with generation directory g{generation_id}",
                    entry.path().display()
                )));
            }
            greatest = Some(greatest.map_or(root_version, |value: u64| value.max(root_version)));
        }
        Ok(greatest)
    }

    pub(crate) fn write_root(
        &self,
        definition_id: u64,
        root: &GenerationManifestRoot,
    ) -> Result<PathBuf> {
        let path = self
            .generation_dir(definition_id, root.generation_id)
            .join(Self::root_file_name(root));
        self.write_root_fragment(&path, root)?;
        Ok(path)
    }

    pub(crate) fn write_shard(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
        root_version: u64,
        shard: &ManifestShard,
    ) -> Result<ManifestFileRef> {
        let file_name = format!("shard_g{generation_id}_v{root_version}.json");
        let path = self
            .generation_dir(definition_id, generation_id)
            .join(&file_name);
        self.write_typed_fragment(&path, self.codec_kind, shard)?;
        Ok(ManifestFileRef::new(file_name, self.codec_kind))
    }

    pub(crate) fn write_delta(
        &self,
        definition_id: u64,
        generation_id: SearchGenerationId,
        root_version: u64,
        ordinal: usize,
        delta: &ManifestDelta,
    ) -> Result<ManifestFileRef> {
        let file_name = format!("delta_g{generation_id}_v{root_version}_{ordinal}.json");
        let path = self
            .generation_dir(definition_id, generation_id)
            .join(&file_name);
        self.write_typed_fragment(&path, self.codec_kind, delta)?;
        Ok(ManifestFileRef::new(file_name, self.codec_kind))
    }

    /// Load the greatest immutable root in an isolated build workspace.
    ///
    /// This must not be used for an installed table: failed and superseded
    /// roots intentionally remain on disk, so only the tablet's durable head
    /// can select a queryable revision there.
    pub(crate) fn load_latest_manifest_for_private_workspace(
        &self,
        definition_id: u64,
    ) -> Result<Option<LoadedManifest>> {
        let Some(path) = self.latest_versioned_root_path(definition_id)? else {
            return Ok(None);
        };
        let root_path = path;
        self.load_manifest_from_root_path(definition_id, root_path)
    }

    fn latest_versioned_root_path(&self, definition_id: u64) -> Result<Option<PathBuf>> {
        let generations_dir = self.definition_dir(definition_id).join("generations");
        if !generations_dir.exists() {
            return Ok(None);
        }
        let mut latest: Option<(SearchGenerationId, u64, PathBuf)> = None;
        for generation in fs::read_dir(&generations_dir).map_err(|err| {
            paro_error::io_error(format!("scan {}: {}", generations_dir.display(), err))
        })? {
            let generation = generation.map_err(paro_error::io)?;
            if !generation.file_type().map_err(paro_error::io)?.is_dir() {
                continue;
            }
            let Some(directory_generation_id) = generation
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix('g'))
                .and_then(|value| value.parse::<SearchGenerationId>().ok())
            else {
                continue;
            };
            for entry in fs::read_dir(generation.path()).map_err(paro_error::io)? {
                let entry = entry.map_err(paro_error::io)?;
                if !entry.file_type().map_err(paro_error::io)?.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                let Some((generation_id, root_version)) = parse_manifest_root_file_name(&name)
                else {
                    continue;
                };
                if generation_id != directory_generation_id {
                    return Err(paro_error::data_corrupted(format!(
                        "search manifest root {} disagrees with generation directory g{}",
                        entry.path().display(),
                        directory_generation_id
                    )));
                }
                let candidate = (generation_id, root_version, entry.path());
                if latest.as_ref().is_none_or(|current| {
                    candidate.0 > current.0 || candidate.0 == current.0 && candidate.1 > current.1
                }) {
                    latest = Some(candidate);
                }
            }
        }
        Ok(latest.map(|(_, _, path)| path))
    }

    pub(crate) fn load_manifest_for_head(
        &self,
        head: &SearchGenerationHeadMeta,
    ) -> Result<Option<LoadedManifest>> {
        let root_path =
            self.root_path_for_file(head.definition_id, head.generation_id, &head.root_file_name);
        if !root_path.exists() {
            return Ok(None);
        }
        let loaded = self.load_manifest_from_root_path(head.definition_id, root_path)?;
        let Some(loaded) = loaded else {
            return Ok(None);
        };
        let root = &loaded.root;
        if root.definition_id != head.definition_id
            || root.generation_id != head.generation_id
            || root.root_version != head.root_version
            || root.config_fingerprint != head.config_fingerprint
        {
            return Err(paro_error::invalid_input(format!(
                "search generation head mismatch for definition {}: head g{} v{} fp{}, root g{} v{} fp{}",
                head.definition_id,
                head.generation_id,
                head.root_version,
                head.config_fingerprint,
                root.generation_id,
                root.root_version,
                root.config_fingerprint
            )));
        }
        Ok(Some(loaded))
    }

    fn load_manifest_from_root_path(
        &self,
        definition_id: u64,
        root_path: PathBuf,
    ) -> Result<Option<LoadedManifest>> {
        let started_at = Instant::now();
        let root = self.read_root_fragment(&root_path)?;
        if root.definition_id != definition_id {
            return Err(paro_error::data_corrupted(format!(
                "search manifest root definition {} does not match directory {definition_id}",
                root.definition_id
            )));
        }
        let mut verified = root.clone();
        let expected_checksum = verified.checksum;
        verified.recompute_checksum()?;
        if verified.checksum != expected_checksum {
            return Err(paro_error::invalid_input(format!(
                "search manifest checksum mismatch for definition {}",
                definition_id
            )));
        }

        let materialized = self.load_materialized_state(definition_id, &root, true)?;
        let definition_dir = self.generation_dir(definition_id, root.generation_id);
        let shard_paths = root
            .shard_files
            .iter()
            .map(|file| definition_dir.join(&file.file_name))
            .collect::<Vec<_>>();
        let delta_paths = root
            .recent_delta_files
            .iter()
            .map(|file| definition_dir.join(&file.file_name))
            .collect::<Vec<_>>();
        let loaded = LoadedManifest {
            root,
            root_path,
            shard_paths,
            delta_paths,
            tail_entry_id_allocator: materialized.next_tail_entry_id,
            publication_lease: None,
            artifacts: materialized.artifacts,
            tail_pending_entries: materialized.tail_pending_entries,
        };
        storage_metrics()
            .record_search_manifest_open(self.codec_label(), elapsed_micros_since(started_at));
        storage_metrics().set_search_manifest_delta_count(
            self.codec_label(),
            loaded.root.recent_delta_files.len(),
        );
        Ok(Some(loaded))
    }

    fn load_materialized_state(
        &self,
        definition_id: u64,
        root: &GenerationManifestRoot,
        enforce_open_budget: bool,
    ) -> Result<MaterializedManifestState> {
        #[cfg(test)]
        self.full_replay_count.fetch_add(1, Ordering::Relaxed);
        let definition_dir = self.generation_dir(definition_id, root.generation_id);
        if enforce_open_budget {
            self.enforce_delta_open_budget(definition_id, root, &definition_dir)?;
        }
        let mut artifact_map = BTreeMap::<ArtifactManifestKey, SearchArtifactRef>::new();
        let mut tail_map = BTreeMap::<TailEntryId, TailPendingEntry>::new();

        for shard_file in &root.shard_files {
            let shard = self.read_typed_fragment::<ManifestShard>(
                &definition_dir.join(&shard_file.file_name),
                shard_file.codec,
            )?;
            for artifact in shard.artifact_refs {
                artifact.validate()?;
                artifact_map.insert(artifact_key(&artifact), artifact);
            }
            for entry in shard.tail_pending_entries {
                upsert_tail_entry(definition_id, &mut tail_map, entry)?;
            }
        }

        for delta_file in &root.recent_delta_files {
            let delta = self.read_typed_fragment::<ManifestDelta>(
                &definition_dir.join(&delta_file.file_name),
                delta_file.codec,
            )?;
            for entry in delta.entries {
                match entry {
                    ManifestDeltaEntry::AddArtifact(artifact) => {
                        artifact.validate()?;
                        artifact_map.insert(artifact_key(&artifact), artifact);
                    }
                    ManifestDeltaEntry::RemoveArtifact(removed) => {
                        artifact_map.retain(|(coverage, _, _, _), _| coverage != &removed);
                    }
                    ManifestDeltaEntry::UpsertTail(tail_entry) => {
                        upsert_tail_entry(definition_id, &mut tail_map, tail_entry)?;
                    }
                    ManifestDeltaEntry::CoverTail(entry_id) => {
                        tail_map.remove(&entry_id);
                    }
                    ManifestDeltaEntry::StatsDelta(_) => {}
                }
            }
        }

        let (artifact_map, next_tail_entry_id) = self.filter_missing_sidecar_artifacts(
            definition_id,
            root,
            artifact_map,
            &mut tail_map,
        )?;

        let artifacts = GenerationArtifactSet::try_new(artifact_map.into_values().collect())?;
        artifacts.validate_for_generation(root.definition_id, root.generation_id)?;
        let tail_pending_entries = tail_map.into_values().collect::<Vec<_>>();
        validate_tail_entry_id_allocator(definition_id, next_tail_entry_id, &tail_pending_entries)?;
        Ok(MaterializedManifestState {
            artifacts: Arc::new(artifacts),
            tail_pending_entries,
            next_tail_entry_id,
        })
    }

    fn filter_missing_sidecar_artifacts(
        &self,
        definition_id: u64,
        root: &GenerationManifestRoot,
        artifact_map: BTreeMap<ArtifactManifestKey, SearchArtifactRef>,
        tail_map: &mut BTreeMap<TailEntryId, TailPendingEntry>,
    ) -> Result<(
        BTreeMap<ArtifactManifestKey, SearchArtifactRef>,
        TailEntryId,
    )> {
        let mut next_recovery_tail_id = root.persisted_tail_entry_id_seed.0.max(1);
        let mut retained = BTreeMap::new();
        for (key, artifact) in artifact_map {
            if self.sidecar_artifact_range_exists(&artifact.location)? {
                retained.insert(key, artifact);
                continue;
            }

            tracing::warn!(
                definition_id,
                generation_id = root.generation_id,
                covered_segments = artifact.coverage.segments().len(),
                "search manifest references a missing sidecar artifact; degrading artifact to tail-pending recovery entry"
            );
            let mut recovery_by_rowset = BTreeMap::<u64, (Vec<u32>, u64)>::new();
            for span in artifact.coverage.segments() {
                let entry = recovery_by_rowset
                    .entry(span.segment.rowset_id)
                    .or_default();
                entry.0.push(span.segment.segment_id);
                entry.1 = entry.1.saturating_add(span.row_count);
            }
            for (rowset_id, (segment_ids, row_count)) in recovery_by_rowset {
                let entry_id = loop {
                    let candidate = TailEntryId(next_recovery_tail_id);
                    next_recovery_tail_id =
                        next_recovery_tail_id.checked_add(1).ok_or_else(|| {
                            paro_error::invalid_input(format!(
                                "search recovery tail id exhausted for definition {definition_id}"
                            ))
                        })?;
                    if !tail_map.contains_key(&candidate) {
                        break candidate;
                    }
                };
                let byte_count = artifact
                    .stats
                    .bytes_on_disk
                    .saturating_mul(row_count)
                    .div_ceil(artifact.stats.row_count.max(1));
                tail_map.insert(
                    entry_id,
                    TailPendingEntry {
                        entry_id,
                        rowset_id,
                        segment_ids,
                        mutation: TailMutationKind::Append,
                        row_count,
                        byte_count,
                        row_image_ref: Some(TailRowImageRef::WholeRowset),
                    },
                );
            }
        }
        Ok((retained, TailEntryId(next_recovery_tail_id)))
    }

    fn sidecar_artifact_range_exists(&self, location: &ArtifactLocation) -> Result<bool> {
        let ArtifactLocation::SidecarArtifactFile {
            file_id,
            offset,
            len,
            ..
        } = location
        else {
            return Ok(true);
        };
        let Some(end) = offset.checked_add(*len) else {
            return Ok(false);
        };
        let path = self
            .table_data_dir
            .join(SidecarArtifactStore::package_relative_path(*file_id));
        match fs::metadata(&path) {
            Ok(metadata) => Ok(metadata.len() >= end),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(paro_error::io_error(format!(
                "inspect search sidecar artifact {}: {}",
                path.display(),
                error
            ))),
        }
    }

    fn enforce_delta_open_budget(
        &self,
        definition_id: u64,
        root: &GenerationManifestRoot,
        definition_dir: &Path,
    ) -> Result<()> {
        let delta_count = root.recent_delta_files.len();
        let delta_bytes = root.delta_window_bytes(definition_dir);
        if delta_count > DELTA_COUNT_HARD_LIMIT || delta_bytes > DELTA_BYTES_HARD_LIMIT {
            return Err(paro_error::invalid_input(format!(
                "search manifest delta window for definition {} exceeds open hard budget: count={} bytes={} limits=({}, {})",
                definition_id,
                delta_count,
                delta_bytes,
                DELTA_COUNT_HARD_LIMIT,
                DELTA_BYTES_HARD_LIMIT
            )));
        }
        Ok(())
    }

    pub(crate) fn remove_paths(&self, paths: &[PathBuf]) {
        for path in paths {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to remove unpublished search manifest fragment"
                ),
            }
        }
    }

    pub(crate) fn prune_empty_definition_dirs(&self, definition_id: u64) {
        let definition_dir = self.definition_dir(definition_id);
        let generations_dir = definition_dir.join("generations");
        if let Ok(entries) = fs::read_dir(&generations_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    match fs::remove_dir(&path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => tracing::warn!(
                            path = %path.display(),
                            error = %error,
                            "failed to prune empty search generation directory"
                        ),
                    }
                }
            }
        }
        for path in [&generations_dir, &definition_dir] {
            match fs::remove_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to prune empty search definition directory"
                ),
            }
        }
    }

    pub(crate) fn sweep_orphan_staging_fragments(&self) -> Result<usize> {
        let definitions_dir = self
            .table_data_dir
            .join("search_registry")
            .join("definitions");
        let Ok(entries) = fs::read_dir(&definitions_dir) else {
            return Ok(0);
        };
        let mut removed = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            removed = removed.saturating_add(self.sweep_orphan_staging_fragments_in_dir(&path)?);
            let generations = path.join("generations");
            if let Ok(entries) = fs::read_dir(generations) {
                for generation in entries.flatten() {
                    if generation.path().is_dir() {
                        removed = removed.saturating_add(
                            self.sweep_orphan_staging_fragments_in_dir(&generation.path())?,
                        );
                    }
                }
            }
        }
        Ok(removed)
    }

    /// Remove private generation workspaces left by a process crash.
    ///
    /// This must run only after WAL replay: a committed generation publish may
    /// still name its private workspace as the source of an unapplied rename.
    /// Once replay has completed, every remaining entry is necessarily
    /// unreferenced by the durable prefix and may be removed.
    pub(crate) fn sweep_orphan_generation_workspaces(&self) -> Result<usize> {
        let staging_root = self
            .table_data_dir
            .join("_staged")
            .join("search-generation");
        let Ok(entries) = fs::read_dir(&staging_root) else {
            return Ok(0);
        };
        let mut removed = 0usize;
        for entry in entries {
            let entry = entry.map_err(paro_error::io)?;
            let path = entry.path();
            if entry.file_type().map_err(paro_error::io)?.is_dir() {
                fs::remove_dir_all(&path).map_err(|error| {
                    paro_error::io_error(format!(
                        "remove orphan search-generation workspace {}: {}",
                        path.display(),
                        error
                    ))
                })?;
            } else {
                fs::remove_file(&path).map_err(|error| {
                    paro_error::io_error(format!(
                        "remove invalid search-generation staging entry {}: {}",
                        path.display(),
                        error
                    ))
                })?;
            }
            removed = removed.saturating_add(1);
        }
        if fs::read_dir(&staging_root)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
        {
            let _ = fs::remove_dir(&staging_root);
        }
        Ok(removed)
    }

    /// Reclaim immutable manifest revisions that are not reachable from the
    /// tablet's durable heads. This runs only after WAL replay, when the head
    /// set is the complete visibility boundary and no unapplied publish can
    /// still make a newer installed root live.
    pub(crate) fn sweep_unpublished_installed_revisions(
        &self,
        heads: &[SearchGenerationHeadMeta],
    ) -> Result<usize> {
        let heads_by_definition = heads
            .iter()
            .map(|head| (head.definition_id, head))
            .collect::<BTreeMap<_, _>>();

        let definitions_dir = self
            .table_data_dir
            .join("search_registry")
            .join("definitions");
        let Ok(definitions) = fs::read_dir(&definitions_dir) else {
            return Ok(0);
        };
        let mut removed = 0usize;
        for definition in definitions {
            let definition = definition.map_err(paro_error::io)?;
            if !definition.file_type().map_err(paro_error::io)?.is_dir() {
                continue;
            }
            let Some(definition_id) = definition
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u64>().ok())
            else {
                continue;
            };
            let Some(head) = heads_by_definition.get(&definition_id) else {
                // Absence of a head does not prove retirement. Definitions are
                // reclaimed only by the explicit retirement path.
                continue;
            };
            let reachable = self.reachable_fragment_paths_for_head(head)?;
            let generations_dir = definition.path().join("generations");
            let generations = match fs::read_dir(&generations_dir) {
                Ok(generations) => generations,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(paro_error::io(error)),
            };
            for generation in generations {
                let generation = generation.map_err(paro_error::io)?;
                if !generation.file_type().map_err(paro_error::io)?.is_dir() {
                    continue;
                }
                for entry in fs::read_dir(generation.path()).map_err(paro_error::io)? {
                    let entry = entry.map_err(paro_error::io)?;
                    let path = entry.path();
                    if !entry.file_type().map_err(paro_error::io)?.is_file() {
                        continue;
                    }
                    let Some(_) = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .and_then(parse_manifest_fragment_version)
                    else {
                        continue;
                    };
                    if reachable.contains(&path) {
                        continue;
                    }
                    fs::remove_file(&path).map_err(|error| {
                        paro_error::io_error(format!(
                            "remove unpublished search manifest fragment {}: {}",
                            path.display(),
                            error
                        ))
                    })?;
                    removed = removed.saturating_add(1);
                }
            }
            self.prune_empty_definition_dirs(definition_id);
        }
        Ok(removed)
    }

    fn reachable_fragment_paths_for_head(
        &self,
        head: &SearchGenerationHeadMeta,
    ) -> Result<BTreeSet<PathBuf>> {
        let root_path =
            self.root_path_for_file(head.definition_id, head.generation_id, &head.root_file_name);
        if !root_path.exists() {
            return Err(paro_error::data_corrupted(format!(
                "durable search generation head for definition {} is missing root {}",
                head.definition_id,
                root_path.display()
            )));
        }
        let root = self.read_root_fragment(&root_path)?;
        let mut verified = root.clone();
        let expected_checksum = verified.checksum;
        verified.recompute_checksum()?;
        if verified.checksum != expected_checksum
            || root.definition_id != head.definition_id
            || root.generation_id != head.generation_id
            || root.root_version != head.root_version
            || root.config_fingerprint != head.config_fingerprint
        {
            return Err(paro_error::data_corrupted(format!(
                "durable search generation head for definition {} does not match its root",
                head.definition_id
            )));
        }
        let generation_dir = self.generation_dir(head.definition_id, head.generation_id);
        let mut reachable = BTreeSet::new();
        reachable.insert(root_path);
        reachable.extend(
            root.shard_files
                .iter()
                .map(|fragment| generation_dir.join(&fragment.file_name)),
        );
        reachable.extend(
            root.recent_delta_files
                .iter()
                .map(|fragment| generation_dir.join(&fragment.file_name)),
        );
        Ok(reachable)
    }

    fn sweep_orphan_staging_fragments_in_dir(&self, dir: &Path) -> Result<usize> {
        let Ok(entries) = fs::read_dir(dir) else {
            return Ok(0);
        };
        let mut removed = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() || !is_manifest_staging_path(&path) {
                continue;
            }
            fs::remove_file(&path).map_err(|err| {
                paro_error::internal(format!(
                    "remove orphan search manifest staging fragment {}: {}",
                    path.display(),
                    err
                ))
            })?;
            removed = removed.saturating_add(1);
        }
        Ok(removed)
    }

    pub(crate) fn definition_paths(&self, definition_id: u64) -> Vec<PathBuf> {
        let generations_dir = self.definition_dir(definition_id).join("generations");
        let mut paths = Vec::new();
        if let Ok(generations) = fs::read_dir(generations_dir) {
            for generation in generations.flatten() {
                if let Ok(entries) = fs::read_dir(generation.path()) {
                    paths.extend(
                        entries
                            .flatten()
                            .map(|entry| entry.path())
                            .filter(|path| path.is_file()),
                    );
                }
            }
        }
        paths.sort();
        paths
    }

    fn write_root_fragment(&self, path: &Path, root: &GenerationManifestRoot) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                paro_error::internal(format!(
                    "create search manifest parent dir {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        }
        let bytes = match self.codec_kind {
            ManifestCodecKind::JSON_DEBUG_V4 => serde_json::to_vec_pretty(root).map_err(|err| {
                paro_error::serialization_error(format!(
                    "serialize search manifest root fragment: {err}"
                ))
            })?,
            ManifestCodecKind {
                family: ManifestCodecFamily::Binary,
                version: 4,
            } => encode_binary_manifest_fragment(root)?,
            other => {
                return Err(paro_error::not_supported(format!(
                    "unsupported search manifest codec {:?}",
                    other
                )))
            }
        };
        write_durable_manifest_fragment(path, &bytes)
    }

    fn write_typed_fragment<T: Serialize + BinaryManifestFragment>(
        &self,
        path: &Path,
        codec: ManifestCodecKind,
        value: &T,
    ) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                paro_error::internal(format!(
                    "create search manifest parent dir {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        }
        let bytes = encode_manifest_fragment(codec, value)?;
        write_durable_manifest_fragment(path, &bytes)
    }

    fn read_root_fragment(&self, path: &Path) -> Result<GenerationManifestRoot> {
        let bytes = fs::read(path).map_err(|err| {
            paro_error::internal(format!("read search manifest {}: {}", path.display(), err))
        })?;
        storage_metrics().add_search_manifest_open_bytes(self.codec_label(), bytes.len() as u64);
        match self.codec_kind {
            ManifestCodecKind::JSON_DEBUG_V4 => {
                let root = serde_json::from_slice(&bytes).map_err(|err| {
                    paro_error::serialization_error(format!(
                        "deserialize search manifest root fragment: {err}"
                    ))
                })?;
                Ok(root)
            }
            ManifestCodecKind {
                family: ManifestCodecFamily::Binary,
                version: 4,
            } => decode_binary_manifest_fragment(&bytes),
            other => Err(paro_error::not_supported(format!(
                "unsupported search manifest codec {:?}",
                other
            ))),
        }
    }

    fn read_typed_fragment<T: for<'de> Deserialize<'de> + BinaryManifestFragment>(
        &self,
        path: &Path,
        codec: ManifestCodecKind,
    ) -> Result<T> {
        let bytes = fs::read(path).map_err(|err| {
            paro_error::internal(format!("read search manifest {}: {}", path.display(), err))
        })?;
        storage_metrics().add_search_manifest_open_bytes(codec.metric_label(), bytes.len() as u64);
        decode_manifest_fragment(codec, &bytes)
    }
}

fn encode_manifest_fragment<T: Serialize + BinaryManifestFragment>(
    codec: ManifestCodecKind,
    value: &T,
) -> Result<Vec<u8>> {
    match codec {
        ManifestCodecKind::JSON_DEBUG_V4 => serde_json::to_vec_pretty(value).map_err(|err| {
            paro_error::serialization_error(format!("serialize search manifest fragment: {err}"))
        }),
        ManifestCodecKind {
            family: ManifestCodecFamily::Binary,
            version: 4,
        } => encode_binary_manifest_fragment(value),
        other => Err(paro_error::not_supported(format!(
            "unsupported search manifest codec {:?}",
            other
        ))),
    }
}

fn write_durable_manifest_fragment(path: &Path, bytes: &[u8]) -> Result<()> {
    let staging_path = manifest_staging_path(path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(|err| {
            paro_error::internal(format!(
                "create search manifest staging fragment {}: {}",
                staging_path.display(),
                err
            ))
        })?;
    let write_result = (|| -> Result<()> {
        file.write_all(bytes).map_err(|err| {
            paro_error::internal(format!(
                "write search manifest staging fragment {}: {}",
                staging_path.display(),
                err
            ))
        })?;
        file.flush().map_err(|err| {
            paro_error::internal(format!(
                "flush search manifest staging fragment {}: {}",
                staging_path.display(),
                err
            ))
        })?;
        file.sync_all().map_err(|err| {
            paro_error::internal(format!(
                "sync search manifest staging fragment {}: {}",
                staging_path.display(),
                err
            ))
        })
    })();
    drop(file);

    if let Err(err) = write_result {
        let _ = fs::remove_file(&staging_path);
        return Err(err);
    }

    if let Err(err) = fs::hard_link(&staging_path, path) {
        let _ = fs::remove_file(&staging_path);
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(paro_error::object_exists(
                "immutable search manifest fragment",
                path.display().to_string(),
            ));
        }
        if err.kind() == std::io::ErrorKind::Unsupported {
            return Err(paro_error::not_supported(format!(
                "filesystem does not support create-exclusive hard-link publication for immutable search manifests: {}",
                path.display()
            )));
        }
        return Err(paro_error::internal(format!(
            "commit immutable search manifest fragment {} -> {}: {}",
            staging_path.display(),
            path.display(),
            err
        )));
    }
    if let Err(err) = fs::remove_file(&staging_path) {
        tracing::warn!(
            path = %staging_path.display(),
            error = %err,
            "failed to remove linked search manifest staging fragment"
        );
    }
    if let Some(parent) = path.parent() {
        sync_manifest_parent_dir(parent)?;
    }
    Ok(())
}

fn manifest_staging_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().ok_or_else(|| {
        paro_error::internal(format!(
            "search manifest fragment {} has no parent",
            path.display()
        ))
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            paro_error::internal(format!(
                "search manifest fragment {} has no valid file name",
                path.display()
            ))
        })?;
    let sequence = MANIFEST_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(parent.join(format!(
        ".{file_name}.staging-{}-{sequence}",
        std::process::id()
    )))
}

fn is_manifest_staging_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with('.') && name.contains(".staging-"))
}

fn parse_manifest_root_file_name(name: &str) -> Option<(SearchGenerationId, u64)> {
    let body = name
        .strip_prefix("manifest_root_g")?
        .strip_suffix(".json")?;
    let (generation, revision) = body.split_once("_v")?;
    let (version, fingerprint) = revision.split_once("_f")?;
    fingerprint.parse::<u64>().ok()?;
    Some((generation.parse().ok()?, version.parse().ok()?))
}

fn parse_manifest_fragment_version(name: &str) -> Option<(SearchGenerationId, u64)> {
    if let Some(identity) = parse_manifest_root_file_name(name) {
        return Some(identity);
    }
    let body = name.strip_suffix(".json")?;
    let body = body
        .strip_prefix("shard_g")
        .or_else(|| body.strip_prefix("delta_g"))?;
    let (generation, revision) = body.split_once("_v")?;
    let version = revision.split('_').next()?;
    Some((generation.parse().ok()?, version.parse().ok()?))
}

fn sync_manifest_parent_dir(parent: &Path) -> Result<()> {
    let dir = fs::File::open(parent).map_err(|err| {
        paro_error::internal(format!(
            "open search manifest parent dir {} for sync: {}",
            parent.display(),
            err
        ))
    })?;
    dir.sync_all().map_err(|err| {
        paro_error::internal(format!(
            "sync search manifest parent dir {}: {}",
            parent.display(),
            err
        ))
    })
}

fn decode_manifest_fragment<T: for<'de> Deserialize<'de> + BinaryManifestFragment>(
    codec: ManifestCodecKind,
    bytes: &[u8],
) -> Result<T> {
    match codec {
        ManifestCodecKind::JSON_DEBUG_V4 => serde_json::from_slice(bytes).map_err(|err| {
            paro_error::serialization_error(format!("deserialize search manifest fragment: {err}"))
        }),
        ManifestCodecKind {
            family: ManifestCodecFamily::Binary,
            version: 4,
        } => decode_binary_manifest_fragment(bytes),
        other => Err(paro_error::not_supported(format!(
            "unsupported search manifest codec {:?}",
            other
        ))),
    }
}

fn elapsed_micros_since(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

fn artifact_key(artifact: &SearchArtifactRef) -> ArtifactManifestKey {
    (
        artifact.coverage.clone(),
        artifact.column_id,
        artifact.kind,
        artifact.provider_variant,
    )
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    seahash::hash(bytes)
}

fn upsert_tail_entry(
    definition_id: u64,
    tail_map: &mut BTreeMap<TailEntryId, TailPendingEntry>,
    entry: TailPendingEntry,
) -> Result<()> {
    if !entry.entry_id.is_assigned() {
        return Err(paro_error::invalid_input(format!(
            "search manifest tail entry for definition {} is missing entry_id",
            definition_id
        )));
    }
    tail_map.insert(entry.entry_id, entry);
    Ok(())
}

fn validate_tail_entry_id_allocator(
    definition_id: u64,
    next_entry_id: TailEntryId,
    entries: &[TailPendingEntry],
) -> Result<()> {
    let max_assigned = entries
        .iter()
        .filter(|entry| entry.entry_id.is_assigned())
        .map(|entry| entry.entry_id.0)
        .max()
        .unwrap_or(0);
    if next_entry_id.0 == 0 || next_entry_id.0 <= max_assigned {
        return Err(paro_error::data_corrupted(format!(
            "search manifest for definition {definition_id} has tail allocator {} at or below assigned id {max_assigned}",
            next_entry_id.0
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        decode_manifest_fragment, encode_manifest_fragment, parse_manifest_root_file_name,
        GenerationManifestRoot, ManifestCodecKind, ManifestDelta, ManifestDeltaEntry,
        ManifestFileRef, ManifestShard, ManifestStore, DELTA_COUNT_HARD_LIMIT,
        DELTA_COUNT_SOFT_LIMIT,
    };
    use crate::search::artifact::{ArtifactLocation, SegmentPagePointer};
    use crate::search::capability::{
        ArtifactSegmentRef, CoverageState, SearchArtifactRef, SearchIndexKind,
        SearchPartitionCoverage,
    };
    use crate::search::cursor::GenerationArtifactSet;
    use crate::search::inline_sink::{FullTextStatsDelta, SearchStatsDelta};
    use crate::search::stats::{
        ExecutionModes, FullTextProviderStats, GenerationMaintenanceState, GenerationStats,
        SearchArtifactStats, SearchProviderStats,
    };
    use crate::search::tail::{TailEntryId, TailMutationKind, TailPendingEntry};
    use tempfile::TempDir;

    #[test]
    fn manifest_delta_entries_are_typed_and_round_trip() {
        let delta = ManifestDelta::new(vec![
            ManifestDeltaEntry::AddArtifact(sample_artifact(10, 2)),
            ManifestDeltaEntry::RemoveArtifact(
                SearchPartitionCoverage::singleton(
                    ArtifactSegmentRef {
                        rowset_id: 9,
                        segment_id: 1,
                    },
                    100,
                )
                .unwrap(),
            ),
            ManifestDeltaEntry::UpsertTail(sample_tail_entry(7, 12, 0)),
            ManifestDeltaEntry::CoverTail(TailEntryId(42)),
            ManifestDeltaEntry::StatsDelta(SearchStatsDelta::FullText(FullTextStatsDelta {
                stats: sample_fulltext_stats(),
            })),
        ]);

        let encoded = serde_json::to_string(&delta).expect("serialize typed delta");
        assert!(encoded.contains(r#""op":"add_artifact""#));
        assert!(encoded.contains(r#""op":"remove_artifact""#));
        assert!(encoded.contains(r#""op":"upsert_tail""#));
        assert!(encoded.contains(r#""op":"cover_tail""#));
        assert!(encoded.contains(r#""op":"stats_delta""#));

        let decoded: ManifestDelta =
            serde_json::from_str(&encoded).expect("deserialize typed delta");
        assert_eq!(decoded, delta);
    }

    #[test]
    fn generation_artifact_set_rejects_overlapping_partition_coverage() {
        let first = sample_artifact(10, 2);
        let duplicate = first.clone();

        let error = GenerationArtifactSet::try_new(vec![first, duplicate])
            .expect_err("overlapping partitions must not publish");
        assert!(error.to_string().contains("overlapping partitions"));
    }

    #[test]
    fn generation_artifact_set_rejects_mixed_provider_and_root_identities() {
        let first = sample_artifact(10, 2);
        let mut mixed_provider = sample_artifact(11, 0);
        mixed_provider.kind = SearchIndexKind::Sparse;
        mixed_provider.provider_variant = 2;

        let error = GenerationArtifactSet::try_new(vec![first.clone(), mixed_provider])
            .expect_err("one generation cannot mix providers");
        assert!(error.to_string().contains("provider kind"));

        let set = GenerationArtifactSet::try_new(vec![first]).unwrap();
        let error = set
            .validate_for_generation(999, 11)
            .expect_err("artifact identity must match its manifest root");
        assert!(error.to_string().contains("does not belong"));
    }

    #[test]
    fn manifest_tail_entries_replay_upsert_and_cover_by_entry_id() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 7;
        let covered_tail = sample_tail_entry(1, 10, 0);
        let live_tail = sample_tail_entry(2, 11, 0);
        let delta_name = store
            .write_delta(
                definition_id,
                1,
                2,
                0,
                &ManifestDelta::new(vec![
                    ManifestDeltaEntry::CoverTail(TailEntryId(1)),
                    ManifestDeltaEntry::UpsertTail(live_tail.clone()),
                ]),
            )
            .expect("write delta");
        let shard_name = store
            .write_shard(
                definition_id,
                1,
                1,
                &ManifestShard {
                    artifact_refs: Vec::new(),
                    tail_pending_entries: vec![covered_tail],
                },
            )
            .expect("write shard");
        let root = sample_root(definition_id, vec![shard_name], vec![delta_name]);

        let loaded = store
            .load_materialized_state(definition_id, &root, false)
            .expect("load tail pending entries");
        assert_eq!(loaded.tail_pending_entries, vec![live_tail]);
    }

    #[test]
    fn manifest_codec_dispatch_round_trips_json_and_binary_v4() {
        let delta = ManifestDelta::new(vec![
            ManifestDeltaEntry::AddArtifact(sample_artifact(10, 0)),
            ManifestDeltaEntry::UpsertTail(sample_tail_entry(1, 10, 0)),
        ]);
        let json_bytes =
            encode_manifest_fragment(ManifestCodecKind::JSON_DEBUG_V4, &delta).unwrap();
        let decoded_json: ManifestDelta =
            decode_manifest_fragment(ManifestCodecKind::JSON_DEBUG_V4, &json_bytes).unwrap();
        assert_eq!(decoded_json, delta);

        let binary_v4 = ManifestCodecKind::BINARY_V4;
        let binary_bytes = encode_manifest_fragment(binary_v4, &delta).unwrap();
        assert!(!binary_bytes.starts_with(b"{"));
        let decoded_binary: ManifestDelta =
            decode_manifest_fragment(binary_v4, &binary_bytes).unwrap();
        assert_eq!(decoded_binary, delta);
        assert_eq!(binary_v4.metric_label(), "binary-v4");
    }

    #[test]
    fn manifest_root_records_fragment_codec_kind() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 8;
        let shard_ref = store
            .write_shard(
                definition_id,
                1,
                1,
                &ManifestShard {
                    artifact_refs: vec![sample_artifact_for_generation(definition_id, 1, 1, 0)],
                    tail_pending_entries: Vec::new(),
                },
            )
            .expect("write shard");
        let delta_ref = store
            .write_delta(
                definition_id,
                1,
                2,
                0,
                &ManifestDelta::new(vec![ManifestDeltaEntry::RemoveArtifact(
                    SearchPartitionCoverage::singleton(
                        ArtifactSegmentRef {
                            rowset_id: 1,
                            segment_id: 0,
                        },
                        100,
                    )
                    .unwrap(),
                )]),
            )
            .expect("write delta");
        let mut root = sample_root(definition_id, vec![shard_ref], vec![delta_ref]);
        root.recompute_checksum().unwrap();
        let root_path = store.write_root(definition_id, &root).unwrap();

        let bytes = std::fs::read(root_path).unwrap();
        let root_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            root_json["shard_files"][0]["codec"]["family"],
            serde_json::Value::String("json_debug".to_string())
        );
        assert_eq!(root_json["shard_files"][0]["codec"]["version"], 4);
        assert_eq!(
            root_json["recent_delta_files"][0]["codec"]["family"],
            serde_json::Value::String("json_debug".to_string())
        );
        assert_eq!(root_json["recent_delta_files"][0]["codec"]["version"], 4);
    }

    #[test]
    fn binary_manifest_root_keeps_fragment_graph_explicit() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new_with_codec(temp_dir.path(), ManifestCodecKind::BINARY_V4);
        let definition_id = 88;
        let shard_ref = store
            .write_shard(
                definition_id,
                1,
                1,
                &ManifestShard {
                    artifact_refs: vec![sample_artifact_for_generation(definition_id, 1, 1, 0)],
                    tail_pending_entries: vec![sample_tail_entry(1, 1, 0)],
                },
            )
            .expect("write shard");
        let delta_ref = store
            .write_delta(
                definition_id,
                1,
                2,
                0,
                &ManifestDelta::new(vec![ManifestDeltaEntry::UpsertTail(sample_tail_entry(
                    2, 2, 0,
                ))]),
            )
            .expect("write delta");
        let mut root = sample_root(definition_id, vec![shard_ref], vec![delta_ref]);
        root.recompute_checksum().unwrap();
        let root_path = store.write_root(definition_id, &root).unwrap();

        let bytes = std::fs::read(&root_path).unwrap();
        assert!(bytes.starts_with(b"PMB4"));
        let loaded = store
            .load_latest_manifest_for_private_workspace(definition_id)
            .unwrap()
            .expect("binary manifest should load");
        assert_eq!(loaded.all_paths().len(), 3);
        assert_eq!(loaded.all_paths()[0], root_path);
        assert_eq!(loaded.artifacts.artifacts.len(), 1);
        assert_eq!(loaded.tail_pending_entries.len(), 2);
    }

    #[test]
    fn manifest_fragments_commit_without_staging_leftovers() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 18;
        let shard_ref = store
            .write_shard(
                definition_id,
                1,
                1,
                &ManifestShard {
                    artifact_refs: vec![sample_artifact_for_generation(definition_id, 1, 1, 0)],
                    tail_pending_entries: Vec::new(),
                },
            )
            .expect("write shard");
        let delta_ref = store
            .write_delta(
                definition_id,
                1,
                2,
                0,
                &ManifestDelta::new(vec![ManifestDeltaEntry::UpsertTail(sample_tail_entry(
                    1, 1, 0,
                ))]),
            )
            .expect("write delta");
        let mut root = sample_root(definition_id, vec![shard_ref], vec![delta_ref]);
        root.recompute_checksum().unwrap();
        store.write_root(definition_id, &root).unwrap();

        let definition_dir = store.generation_dir(definition_id, 1);
        let staging_leftovers = std::fs::read_dir(definition_dir)
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.contains(".staging-"))
            })
            .count();
        assert_eq!(staging_leftovers, 0);
    }

    #[test]
    fn manifest_explicit_sweep_removes_orphan_staging_fragments() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 19;
        let definition_dir = store.generation_dir(definition_id, 1);
        std::fs::create_dir_all(&definition_dir).unwrap();
        let orphan = definition_dir.join(".manifest_root.json.staging-test");
        std::fs::write(&orphan, b"partial-root").unwrap();

        assert!(orphan.exists());
        assert_eq!(store.sweep_orphan_staging_fragments().unwrap(), 1);
        assert!(!orphan.exists());
    }

    #[test]
    fn generation_workspace_sweep_runs_after_replay_boundary() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let workspace = temp_dir
            .path()
            .join("_staged")
            .join("search-generation")
            .join("txn-9-def-44-gen-1");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("partial"), b"staged").unwrap();

        assert_eq!(store.sweep_orphan_generation_workspaces().unwrap(), 1);
        assert!(!workspace.exists());
    }

    #[test]
    fn manifest_latest_root_orders_generation_ids_numerically() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 27;
        let mut generation_nine = sample_root(definition_id, Vec::new(), Vec::new());
        generation_nine.generation_id = 9;
        generation_nine.root_version = 99;
        generation_nine.recompute_checksum().unwrap();
        store.write_root(definition_id, &generation_nine).unwrap();
        let mut generation_ten = sample_root(definition_id, Vec::new(), Vec::new());
        generation_ten.generation_id = 10;
        generation_ten.root_version = 1;
        generation_ten.recompute_checksum().unwrap();
        store.write_root(definition_id, &generation_ten).unwrap();

        let loaded = store
            .load_latest_manifest_for_private_workspace(definition_id)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.root.generation_id, 10);
        assert_eq!(loaded.root.root_version, 1);
    }

    #[test]
    fn manifest_root_identity_includes_contract_fingerprint() {
        let mut first = sample_root(9, Vec::new(), Vec::new());
        first.generation_id = 3;
        first.root_version = 7;
        let mut changed_contract = first.clone();
        changed_contract.config_fingerprint = first.config_fingerprint + 1;

        assert_ne!(
            ManifestStore::root_file_name(&first),
            ManifestStore::root_file_name(&changed_contract)
        );
        assert_eq!(
            parse_manifest_root_file_name(&ManifestStore::root_file_name(&first)),
            Some((first.generation_id, first.root_version))
        );
    }

    #[test]
    fn manifest_root_checksum_preserves_non_finite_float_bits() {
        let mut positive = sample_root(44, Vec::new(), Vec::new());
        positive.generation_stats.provider_stats =
            Some(SearchProviderStats::FullText(FullTextProviderStats {
                avg_doc_length: f32::INFINITY,
                ..FullTextProviderStats::default()
            }));
        positive.recompute_checksum().unwrap();

        let mut negative = positive.clone();
        negative.checksum = 0;
        let Some(SearchProviderStats::FullText(stats)) =
            negative.generation_stats.provider_stats.as_mut()
        else {
            unreachable!()
        };
        stats.avg_doc_length = f32::NEG_INFINITY;
        negative.recompute_checksum().unwrap();

        assert_ne!(positive.checksum, negative.checksum);
    }

    #[test]
    fn manifest_root_is_only_visibility_boundary_for_delta_candidates() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 20;
        let shard_ref = store
            .write_shard(
                definition_id,
                1,
                1,
                &ManifestShard {
                    artifact_refs: Vec::new(),
                    tail_pending_entries: Vec::new(),
                },
            )
            .expect("write shard");
        let unreferenced_delta = store
            .write_delta(
                definition_id,
                1,
                2,
                0,
                &ManifestDelta::new(vec![ManifestDeltaEntry::AddArtifact(sample_artifact(
                    99, 0,
                ))]),
            )
            .expect("write unreferenced delta candidate");
        let mut root = sample_root(definition_id, vec![shard_ref], Vec::new());
        root.recompute_checksum().unwrap();
        store.write_root(definition_id, &root).unwrap();

        let loaded = store
            .load_latest_manifest_for_private_workspace(definition_id)
            .expect("load manifest")
            .expect("manifest exists");
        assert!(loaded.artifacts.artifacts.is_empty());
        assert!(
            store
                .generation_dir(definition_id, 1)
                .join(unreferenced_delta.file_name)
                .exists(),
            "unreferenced delta may remain on disk, but root does not publish it"
        );
    }

    #[test]
    fn manifest_delta_compaction_collapses_soft_limit_window() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 9;
        let shard_ref = store
            .write_shard(
                definition_id,
                1,
                1,
                &ManifestShard {
                    artifact_refs: vec![sample_artifact_for_generation(definition_id, 1, 1, 0)],
                    tail_pending_entries: vec![sample_tail_entry(1, 1, 0)],
                },
            )
            .expect("write shard");
        let mut delta_refs = Vec::new();
        for ordinal in 0..=DELTA_COUNT_SOFT_LIMIT {
            let rowset_id = 100 + ordinal as u64;
            delta_refs.push(
                store
                    .write_delta(
                        definition_id,
                        1,
                        2,
                        ordinal,
                        &ManifestDelta::new(vec![
                            ManifestDeltaEntry::AddArtifact(sample_artifact_for_generation(
                                definition_id,
                                1,
                                rowset_id,
                                0,
                            )),
                            ManifestDeltaEntry::UpsertTail(sample_tail_entry(
                                2 + ordinal as u64,
                                rowset_id,
                                0,
                            )),
                        ]),
                    )
                    .expect("write delta"),
            );
        }
        let old_delta_paths = delta_refs
            .iter()
            .map(|file| store.generation_dir(definition_id, 1).join(&file.file_name))
            .collect::<Vec<_>>();
        let mut root = sample_root(definition_id, vec![shard_ref], delta_refs);
        root.persisted_tail_entry_id_seed = TailEntryId(3 + DELTA_COUNT_SOFT_LIMIT as u64);
        root.recompute_checksum().unwrap();
        store.write_root(definition_id, &root).unwrap();
        let old_head = store.head_for_root(&root);

        let prepared_root_path = store.generation_dir(definition_id, 1).join(format!(
            "manifest_root_g1_v3_f{}.json",
            root.config_fingerprint
        ));
        let current = store
            .load_manifest_for_head(&old_head)
            .unwrap()
            .expect("load current manifest");
        let mut revision = store
            .begin_revision_from_manifest(definition_id, root, &current)
            .expect("begin compacted revision");
        assert!(
            revision.compact_if_needed().expect("compact deltas"),
            "compaction should prepare a shard"
        );
        assert!(
            !prepared_root_path.exists(),
            "delta compaction must prepare fragments without publishing its root"
        );
        let loaded = revision.commit().expect("commit compacted revision");

        assert_eq!(loaded.root.recent_delta_files.len(), 0);
        assert_eq!(loaded.root.shard_files.len(), 1);
        assert_eq!(loaded.root.root_version, 3);
        assert!(
            old_delta_paths.iter().all(|path| path.exists()),
            "manifest construction must not retire files still reachable by the durable head"
        );
        assert!(store.load_manifest_for_head(&old_head).unwrap().is_some());

        assert_eq!(loaded.root.recent_delta_files.len(), 0);
        assert_eq!(
            loaded.artifacts.artifacts.len(),
            1 + DELTA_COUNT_SOFT_LIMIT + 1
        );
        assert_eq!(
            loaded.tail_pending_entries.len(),
            1 + DELTA_COUNT_SOFT_LIMIT + 1
        );
    }

    #[test]
    fn abandoned_root_cannot_poison_revision_allocation_and_is_swept_by_durable_head() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 91;

        let mut durable = sample_root(definition_id, Vec::new(), Vec::new());
        durable.recompute_checksum().unwrap();
        let durable_path = store.write_root(definition_id, &durable).unwrap();
        let durable_head = store.head_for_root(&durable);

        let mut abandoned = durable.clone();
        abandoned.root_version = 2;
        abandoned.recompute_checksum().unwrap();
        let abandoned_path = store.write_root(definition_id, &abandoned).unwrap();
        let abandoned_shard = store
            .write_shard(
                definition_id,
                durable.generation_id,
                4,
                &ManifestShard::default(),
            )
            .expect("write rootless abandoned shard");
        let unreachable_same_revision_shard = store
            .write_shard(
                definition_id,
                durable.generation_id,
                durable.root_version,
                &ManifestShard::default(),
            )
            .expect("write unreachable same-revision shard");
        let mut abandoned_generation = durable.clone();
        abandoned_generation.generation_id = 2;
        abandoned_generation.recompute_checksum().unwrap();
        let abandoned_generation_path = store
            .write_root(definition_id, &abandoned_generation)
            .unwrap();

        let committed = store
            .begin_empty_revision(definition_id, durable.clone())
            .expect("allocate past abandoned root")
            .commit()
            .expect("commit non-reused revision");
        assert_eq!(committed.root.root_version, 5);
        assert!(committed.root_path.exists());

        let removed = store
            .sweep_unpublished_installed_revisions(&[durable_head])
            .expect("sweep revisions newer than durable head");
        assert_eq!(removed, 5);
        assert!(durable_path.exists());
        assert!(!abandoned_path.exists());
        assert!(!committed.root_path.exists());
        assert!(!store
            .generation_dir(definition_id, durable.generation_id)
            .join(abandoned_shard.file_name)
            .exists());
        assert!(!store
            .generation_dir(definition_id, durable.generation_id)
            .join(unreachable_same_revision_shard.file_name)
            .exists());
        assert!(!abandoned_generation_path.exists());
    }

    #[test]
    fn orphan_sweep_never_interprets_missing_head_as_retirement() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 92;
        let mut root = sample_root(definition_id, Vec::new(), Vec::new());
        root.recompute_checksum().unwrap();
        let root_path = store.write_root(definition_id, &root).unwrap();

        assert_eq!(
            store
                .sweep_unpublished_installed_revisions(&[])
                .expect("sweep without head"),
            0
        );
        assert!(root_path.exists());
    }

    #[test]
    fn revision_commit_does_not_apply_recovery_sidecar_repair() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 93;
        let file_id = super::super::artifact::ArtifactFileId {
            definition_id,
            generation_id: 1,
            package_index: 0,
        };
        let package_path = store
            .table_data_dir
            .join(super::SidecarArtifactStore::package_relative_path(file_id));
        std::fs::create_dir_all(package_path.parent().unwrap()).unwrap();
        std::fs::write(&package_path, [0u8; 16]).unwrap();
        let mut artifact = sample_artifact_for_generation(definition_id, 1, 1, 0);
        artifact.location = ArtifactLocation::SidecarArtifactFile {
            file_id,
            offset: 0,
            len: 16,
            checksum: 0,
        };
        let shard = ManifestShard {
            artifact_refs: vec![artifact],
            tail_pending_entries: Vec::new(),
        };
        let shard_ref = store.write_shard(definition_id, 1, 1, &shard).unwrap();
        let mut root = sample_root(definition_id, vec![shard_ref], Vec::new());
        root.recompute_checksum().unwrap();
        store.write_root(definition_id, &root).unwrap();
        let current = store
            .load_manifest_for_head(&store.head_for_root(&root))
            .unwrap()
            .unwrap();
        std::fs::remove_file(package_path).unwrap();

        let mut revision = store
            .begin_revision_from_manifest(definition_id, root, &current)
            .unwrap();
        revision
            .append_delta(&ManifestDelta::new(vec![ManifestDeltaEntry::StatsDelta(
                SearchStatsDelta::FullText(FullTextStatsDelta {
                    stats: sample_fulltext_stats(),
                }),
            )]))
            .unwrap();
        let committed = revision.commit().unwrap();

        assert_eq!(committed.artifacts.artifacts.len(), 1);
        assert!(committed.tail_pending_entries.is_empty());
    }

    #[test]
    fn revision_lease_cleans_unacknowledged_fragments_and_preserves_acknowledged_ones() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());

        let unacknowledged = store
            .begin_empty_revision(94, sample_root(94, Vec::new(), Vec::new()))
            .unwrap()
            .commit()
            .unwrap();
        let unacknowledged_path = unacknowledged.root_path.clone();
        drop(unacknowledged);
        assert!(!unacknowledged_path.exists());

        let acknowledged = store
            .begin_empty_revision(95, sample_root(95, Vec::new(), Vec::new()))
            .unwrap()
            .commit()
            .unwrap();
        let acknowledged_path = acknowledged.root_path.clone();
        acknowledged.mark_revision_published();
        drop(acknowledged);
        assert!(acknowledged_path.exists());
    }

    #[test]
    fn manifest_recovery_replays_1k_segments_from_shards_without_rowset_scan() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 21;
        let shard_count = 16u64;
        let artifacts_per_shard = 64u64;
        let mut shard_refs = Vec::with_capacity(shard_count as usize);

        for shard_idx in 0..shard_count {
            let base_rowset_id = shard_idx * artifacts_per_shard;
            let shard = ManifestShard {
                artifact_refs: (0..artifacts_per_shard)
                    .map(|offset| {
                        sample_artifact_for_generation(
                            definition_id,
                            1,
                            base_rowset_id + offset + 1,
                            0,
                        )
                    })
                    .collect(),
                tail_pending_entries: Vec::new(),
            };
            shard_refs.push(
                store
                    .write_shard(definition_id, 1, shard_idx + 1, &shard)
                    .expect("write manifest shard"),
            );
        }

        let mut root = sample_root(definition_id, shard_refs, Vec::new());
        root.recompute_checksum().unwrap();
        store.write_root(definition_id, &root).unwrap();

        let loaded = store
            .load_latest_manifest_for_private_workspace(definition_id)
            .expect("load manifest")
            .expect("manifest exists");
        assert_eq!(
            loaded.artifacts.artifacts.len(),
            (shard_count * artifacts_per_shard) as usize
        );
        assert!(loaded.tail_pending_entries.is_empty());
        assert_eq!(loaded.all_paths().len(), 1 + shard_count as usize);
    }

    #[test]
    fn manifest_open_rejects_delta_count_above_hard_budget() {
        let temp_dir = TempDir::new().expect("temp dir");
        let store = ManifestStore::new(temp_dir.path());
        let definition_id = 10;
        let mut delta_refs = Vec::new();
        for ordinal in 0..=DELTA_COUNT_HARD_LIMIT {
            delta_refs.push(
                store
                    .write_delta(
                        definition_id,
                        1,
                        2,
                        ordinal,
                        &ManifestDelta::new(vec![ManifestDeltaEntry::UpsertTail(
                            sample_tail_entry(1 + ordinal as u64, 200 + ordinal as u64, 0),
                        )]),
                    )
                    .expect("write delta"),
            );
        }
        let mut root = sample_root(definition_id, Vec::new(), delta_refs);
        root.recompute_checksum().unwrap();
        store.write_root(definition_id, &root).unwrap();

        let err = store
            .load_latest_manifest_for_private_workspace(definition_id)
            .expect_err("hard over-budget delta window must not open");
        assert!(format!("{err}").contains("exceeds open hard budget"));
    }

    fn sample_artifact(rowset_id: u64, segment_id: u32) -> SearchArtifactRef {
        SearchArtifactRef {
            definition_id: 7,
            generation_id: 11,
            coverage: SearchPartitionCoverage::singleton(
                ArtifactSegmentRef {
                    rowset_id,
                    segment_id,
                },
                100,
            )
            .unwrap(),
            column_id: 3,
            kind: SearchIndexKind::FullText,
            provider_variant: 1,
            artifact_format_version: 1,
            location: ArtifactLocation::Inline {
                page: SegmentPagePointer {
                    rowset_id,
                    segment_id,
                    column_id: 3,
                    page_offset: 128,
                    page_len: 4096,
                    checksum: 99,
                },
            },
            stats: SearchArtifactStats {
                row_count: 100,
                bytes_on_disk: 4096,
                provider_stats: None,
            },
            checksum: 99,
        }
    }

    fn sample_artifact_for_generation(
        definition_id: u64,
        generation_id: u64,
        rowset_id: u64,
        segment_id: u32,
    ) -> SearchArtifactRef {
        let mut artifact = sample_artifact(rowset_id, segment_id);
        artifact.definition_id = definition_id;
        artifact.generation_id = generation_id;
        artifact
    }

    fn sample_tail_entry(entry_id: u64, rowset_id: u64, segment_id: u32) -> TailPendingEntry {
        TailPendingEntry {
            entry_id: TailEntryId(entry_id),
            rowset_id,
            segment_ids: vec![segment_id],
            mutation: TailMutationKind::Append,
            row_count: 10,
            byte_count: 1024,
            row_image_ref: None,
        }
    }

    fn sample_fulltext_stats() -> FullTextProviderStats {
        FullTextProviderStats {
            total_docs: 10,
            total_terms: 30,
            avg_doc_length: 3.0,
            unique_terms: 7,
            total_postings: 20,
            max_posting_list_len: 6,
            min_posting_list_len: 1,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            tokenizer: "simple".to_string(),
        }
    }

    fn sample_root(
        definition_id: u64,
        shard_files: Vec<ManifestFileRef>,
        recent_delta_files: Vec<ManifestFileRef>,
    ) -> GenerationManifestRoot {
        GenerationManifestRoot {
            definition_id,
            generation_id: 1,
            build_epoch: 1,
            build_snapshot_version: 0,
            indexed_through_ts: 0,
            config_fingerprint: 99,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats::default(),
            persisted_tail_entry_id_seed: TailEntryId(3),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 1,
            checksum: 0,
            shard_files,
            recent_delta_files,
        }
    }
}
