// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;

use super::super::artifact::{ArtifactFileId, ArtifactLocation};
use super::super::capability::{SearchArtifactRef, SearchIndexKind};
use super::super::cursor::GenerationArtifactSet;
use super::super::manifest::LoadedManifest;
use super::super::sidecar::SidecarArtifactStore;
use super::super::stats::SearchGenerationId;

pub(crate) fn manifest_location(
    path: &Path,
    definition_id: u64,
    generation_id: SearchGenerationId,
) -> Option<ArtifactLocation> {
    let bytes = fs::read(path).ok()?;
    let len = bytes.len() as u64;
    Some(ArtifactLocation::SidecarArtifactFile {
        file_id: ArtifactFileId {
            definition_id,
            generation_id,
            package_index: 0,
        },
        offset: 0,
        len,
        checksum: seahash::hash(&bytes),
    })
}

pub(crate) fn assign_generation_id(
    artifacts: Vec<SearchArtifactRef>,
    generation_id: SearchGenerationId,
) -> Vec<SearchArtifactRef> {
    artifacts
        .into_iter()
        .map(|mut artifact| {
            artifact.generation_id = generation_id;
            artifact
        })
        .collect()
}

pub(crate) fn replace_artifacts(
    current: &GenerationArtifactSet,
    replacements: impl IntoIterator<Item = SearchArtifactRef>,
) -> GenerationArtifactSet {
    let mut replacements = replacements
        .into_iter()
        .map(|artifact| (search_artifact_key(&artifact), artifact))
        .collect::<BTreeMap<_, _>>();
    let mut artifacts = Vec::with_capacity(current.artifacts.len() + replacements.len());
    for artifact in &current.artifacts {
        let key = search_artifact_key(artifact);
        if let Some(replacement) = replacements.remove(&key) {
            artifacts.push(replacement);
        } else {
            artifacts.push(artifact.clone());
        }
    }
    artifacts.extend(replacements.into_values());
    GenerationArtifactSet { artifacts }
}

pub(crate) fn search_artifact_key(artifact: &SearchArtifactRef) -> (RowsetId, u32, ColumnId) {
    (
        artifact.segment.rowset_id,
        artifact.segment.segment_id,
        artifact.column_id,
    )
}

pub(crate) fn sidecar_file_ids_for_artifacts(
    artifacts: &[SearchArtifactRef],
) -> BTreeSet<ArtifactFileId> {
    artifacts
        .iter()
        .filter_map(|artifact| match artifact.location {
            ArtifactLocation::SidecarArtifactFile { file_id, .. } => Some(file_id),
            ArtifactLocation::Inline { .. } => None,
        })
        .collect()
}

pub(crate) fn remove_sidecar_packages(
    store: &SidecarArtifactStore,
    file_ids: &BTreeSet<ArtifactFileId>,
) {
    for file_id in file_ids {
        store.remove_package(*file_id);
    }
}

pub(crate) fn inline_artifact_checksum(
    definition_id: u64,
    config_fingerprint: u64,
    rowset_id: RowsetId,
    segment_id: u32,
    column_id: ColumnId,
    kind: SearchIndexKind,
    page_offset: u64,
    page_len: u32,
) -> u64 {
    seahash::hash(
        format!(
            "{definition_id}:{config_fingerprint}:{rowset_id}:{segment_id}:{column_id}:{kind:?}:{page_offset}:{page_len}"
        )
        .as_bytes(),
    )
}

pub(crate) fn retire_paths_for_manifest(
    table_data_dir: &Path,
    manifest: &LoadedManifest,
) -> Vec<PathBuf> {
    let mut paths = manifest.all_paths().into_iter().collect::<BTreeSet<_>>();
    let store = SidecarArtifactStore::new(table_data_dir.to_path_buf());
    for artifact in &manifest.artifacts.artifacts {
        if let ArtifactLocation::SidecarArtifactFile { file_id, .. } = artifact.location {
            paths.insert(store.package_path(file_id));
        }
    }
    paths.into_iter().collect()
}
