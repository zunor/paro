// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Explicit offline qualification for immutable search generations.
//!
//! Qualification is intentionally separate from artifact open, metadata
//! inspection, and query execution. An O(N + E) graph scan belongs to an
//! operator-requested maintenance/tooling path with an explicit resource
//! budget; it must never become an accidental tax on ordinary table reads.

use std::path::Path;

use paro_common::error::{self as paro_error, Result};

use crate::index::hnsw::{
    hnsw_artifact_compatibility, HnswArtifactCompatibility, HnswGraphDiagnostics, HnswIndex,
};

use super::artifact::ArtifactLocation;
use super::budget::ResourceBudget;
use super::capability::{SearchIndexKind, SearchPartitionCoverage};
use super::manifest::ManifestStore;
use super::sidecar::{
    SidecarArtifactStore, SidecarIntegrityPolicy, SidecarReaderCache, SidecarReaderRequest,
    SIDECAR_PACKAGE_CODEC,
};
use super::stats::{SearchDefinitionId, SearchGenerationId};

/// Identity and retained diagnostic image for one generation-owned HNSW
/// artifact.
#[derive(Debug)]
pub struct HnswGenerationQualification {
    pub definition_id: SearchDefinitionId,
    pub generation_id: SearchGenerationId,
    pub column_id: u32,
    pub artifact_format_version: u32,
    pub coverage: SearchPartitionCoverage,
    pub diagnostics: HnswGraphDiagnostics,
}

struct OpenedHnswGeneration {
    definition_id: SearchDefinitionId,
    generation_id: SearchGenerationId,
    column_id: u32,
    artifact_format_version: u32,
    coverage: SearchPartitionCoverage,
    index: HnswIndex,
}

/// Qualify the current durable generation for one HNSW definition.
///
/// This entry point deliberately requires one generation-owned sidecar graph.
/// A fragmented generation has no single point-id or reachability domain; its
/// graph quality must first be made stable by search compaction instead of
/// averaging unrelated per-segment reports.
pub fn qualify_hnsw_generation(
    table_data_dir: &Path,
    definition_id: SearchDefinitionId,
    budget: &ResourceBudget,
) -> Result<HnswGenerationQualification> {
    let opened = open_hnsw_generation(table_data_dir, definition_id)?;
    let diagnostics = opened.index.graph_diagnostics(budget)?;

    Ok(HnswGenerationQualification {
        definition_id: opened.definition_id,
        generation_id: opened.generation_id,
        column_id: opened.column_id,
        artifact_format_version: opened.artifact_format_version,
        coverage: opened.coverage,
        diagnostics,
    })
}

fn open_hnsw_generation(
    table_data_dir: &Path,
    definition_id: SearchDefinitionId,
) -> Result<OpenedHnswGeneration> {
    let manifest = ManifestStore::new(table_data_dir)
        .load_manifest(definition_id)?
        .ok_or_else(|| {
            paro_error::invalid_input(format!(
                "search definition {definition_id} has no durable generation manifest"
            ))
        })?;
    manifest
        .artifacts
        .validate_for_generation(definition_id, manifest.root.generation_id)?;

    let mut artifacts = manifest
        .artifacts
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == SearchIndexKind::Hnsw);
    let artifact = artifacts.next().ok_or_else(|| {
        paro_error::invalid_input(format!(
            "search definition {definition_id} has no HNSW artifact"
        ))
    })?;
    if artifacts.next().is_some() {
        return Err(paro_error::invalid_input(format!(
            "search definition {definition_id} generation {} is fragmented; compact it to one generation-owned HNSW artifact before qualification",
            manifest.root.generation_id
        )));
    }
    if artifact.stats.row_count != manifest.root.generation_stats.indexed_rows {
        return Err(paro_error::data_corrupted(format!(
            "HNSW qualification artifact covers {} rows but generation statistics report {}",
            artifact.stats.row_count, manifest.root.generation_stats.indexed_rows
        )));
    }
    if !matches!(
        artifact.location,
        ArtifactLocation::SidecarArtifactFile { .. }
    ) {
        return Err(paro_error::invalid_input(
            "HNSW generation qualification requires a durable sidecar artifact",
        ));
    }

    let cache = SidecarReaderCache::new(SidecarArtifactStore::new(table_data_dir));
    let cached = cache.open(SidecarReaderRequest {
        location: &artifact.location,
        artifact_format_version: artifact.artifact_format_version,
        provider: SearchIndexKind::Hnsw,
        codec: SIDECAR_PACKAGE_CODEC,
        integrity: SidecarIntegrityPolicy::SelfValidatingArtifact,
    })?;
    let compatibility = hnsw_artifact_compatibility(cached.bytes())?;
    if compatibility != HnswArtifactCompatibility::Current {
        return Err(paro_error::invalid_input(
            compatibility
                .rebuild_reason()
                .unwrap_or_else(|| "HNSW artifact requires rebuild".to_string()),
        ));
    }
    let (mmap, offset, len) = cached.mmap_range();
    let index = HnswIndex::deserialize_mmap_range(mmap, offset, len)?;

    Ok(OpenedHnswGeneration {
        definition_id,
        generation_id: manifest.root.generation_id,
        column_id: artifact.column_id,
        artifact_format_version: artifact.artifact_format_version,
        coverage: artifact.coverage.clone(),
        index,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::index::hnsw::{
        DistanceMetric, HnswConfig, InMemoryVectorStorage, VectorStorage,
        HNSW_ARTIFACT_FORMAT_VERSION,
    };
    use crate::search::artifact::ArtifactFileId;
    use crate::search::capability::{
        ArtifactSegmentRef, CoverageState, SearchArtifactRef, SearchPartitionCoverage,
    };
    use crate::search::manifest::{GenerationManifestRoot, ManifestShard};
    use crate::search::stats::{
        ExecutionModes, GenerationMaintenanceState, GenerationStats, SearchArtifactStats,
    };
    use crate::search::tail::TailEntryId;

    #[test]
    fn qualification_opens_one_current_generation_owned_sidecar() {
        let directory = tempfile::tempdir().unwrap();
        let definition_id = 7;
        let generation_id = 3;
        let storage: Arc<dyn VectorStorage> = Arc::new(InMemoryVectorStorage::new(
            vec![0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0],
            2,
        ));
        let index = HnswIndex::build(storage, HnswConfig::default(), DistanceMetric::Euclidean);
        let bytes = index.serialize().unwrap();

        let sidecars = SidecarArtifactStore::new(directory.path());
        let mut writer = sidecars
            .create_package_writer(ArtifactFileId {
                definition_id,
                generation_id,
                package_index: 0,
            })
            .unwrap();
        let location = writer.append_artifact(&bytes).unwrap();
        writer.finalize().unwrap();
        let artifact = SearchArtifactRef {
            definition_id,
            generation_id,
            coverage: SearchPartitionCoverage::singleton(
                ArtifactSegmentRef {
                    rowset_id: 1,
                    segment_id: 0,
                },
                4,
            )
            .unwrap(),
            column_id: 2,
            kind: SearchIndexKind::Hnsw,
            provider_variant: 1,
            artifact_format_version: HNSW_ARTIFACT_FORMAT_VERSION,
            location,
            stats: SearchArtifactStats {
                row_count: 4,
                bytes_on_disk: bytes.len() as u64,
                provider_stats: None,
            },
            checksum: seahash::hash(&bytes),
        };

        let manifests = ManifestStore::new(directory.path());
        let shard = manifests
            .write_shard(
                definition_id,
                generation_id,
                1,
                &ManifestShard {
                    artifact_refs: vec![artifact],
                    tail_pending_entries: Vec::new(),
                },
            )
            .unwrap();
        let mut root = GenerationManifestRoot {
            definition_id,
            generation_id,
            build_epoch: 1,
            build_snapshot_version: 1,
            indexed_through_ts: 1,
            config_fingerprint: 1,
            coverage: CoverageState::Complete,
            generation_stats: GenerationStats {
                indexed_rows: 4,
                artifact_count: 1,
                provider_stats: None,
            },
            next_tail_entry_id: TailEntryId(1),
            execution_modes: ExecutionModes::default(),
            maintenance_state: GenerationMaintenanceState::default(),
            root_version: 1,
            checksum: 0,
            shard_files: vec![shard],
            recent_delta_files: Vec::new(),
            materialized_state_file: None,
        };
        root.recompute_checksum().unwrap();
        manifests.write_root(definition_id, &root).unwrap();

        let budget = ResourceBudget::standalone(1 << 20, 1024, 1);
        let result = qualify_hnsw_generation(directory.path(), definition_id, &budget).unwrap();
        assert_eq!(result.definition_id, definition_id);
        assert_eq!(result.generation_id, generation_id);
        assert_eq!(result.column_id, 2);
        assert_eq!(result.diagnostics.report().point_count, 4);
        assert_eq!(result.diagnostics.indegrees().len(), 4);
    }
}
