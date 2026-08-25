// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::index::hnsw::DistanceMetric;
use crate::metrics::storage_metrics;
use crate::rowset::RowsetId;
use crate::tablet::ColumnId;
use paro_common::error::{self as paro_error, Result};

use super::super::capability::{
    CoverageState, SearchCapability, SearchDefinitionOrigin, SearchGeneration,
    SearchIndexDefinition, SearchIndexKind, SearchTailSummary,
};
use super::super::cursor::{GenerationArtifactSet, GenerationReadSnapshot};
use super::super::definition::builder_set::build_inline_builder_set;
use super::super::inline_sink::{SearchAdmission, SearchInlineBuilderSet};
use super::super::lifecycle::publisher::manifest_location;
use super::super::manifest::LoadedManifest;
use super::super::stats::{
    CatchUpBacklogTier, ExecutionModes, SearchExecutionMode, SearchGenerationId,
};
use super::super::tail::{provider_tail_exact_merge_policy, TailMutationKind, TailPendingSet};
use super::super::write_path::{
    FullTextWriteBinding, SearchWriteContext, SearchWritePlan, SparseWriteBinding,
};

#[inline]
pub(crate) fn indexed_through_ts(visible_version: i64) -> u64 {
    visible_version.max(0) as u64
}

#[derive(Debug, Clone)]
pub(crate) struct SearchDefinitionState {
    pub(crate) definition: SearchIndexDefinition,
    /// Provider contract decoded once at the registry boundary. Query and
    /// maintenance paths consume this immutable value, never the JSON image.
    pub(crate) hnsw_provider_config: Option<Arc<super::super::HnswProviderConfig>>,
    pub(crate) fulltext_provider_config: Option<Arc<super::super::FullTextProviderConfig>>,
    pub(crate) sparse_provider_config: Option<Arc<super::super::SparseProviderConfig>>,
    pub(crate) origin: SearchDefinitionOrigin,
    pub(crate) generation: Option<SearchGeneration>,
    pub(crate) capability: Option<SearchCapability>,
    pub(crate) manifest: Option<LoadedManifest>,
    pub(crate) next_generation_id: SearchGenerationId,
    pub(crate) next_build_epoch: u64,
}

impl SearchDefinitionState {
    pub(crate) fn new(
        definition: SearchIndexDefinition,
        origin: SearchDefinitionOrigin,
    ) -> Result<Self> {
        let hnsw_provider_config = if definition.kind == SearchIndexKind::Hnsw {
            Some(Arc::new(definition.hnsw_provider_config()?))
        } else {
            None
        };
        let fulltext_provider_config = if definition.kind == SearchIndexKind::FullText {
            Some(Arc::new(definition.fulltext_provider_config()?))
        } else {
            None
        };
        let sparse_provider_config = if definition.kind == SearchIndexKind::Sparse {
            Some(Arc::new(definition.sparse_provider_config()?))
        } else {
            None
        };
        Ok(Self {
            definition,
            hnsw_provider_config,
            fulltext_provider_config,
            sparse_provider_config,
            origin,
            generation: None,
            capability: None,
            manifest: None,
            next_generation_id: 1,
            next_build_epoch: 1,
        })
    }

    pub(crate) fn with_manifest(mut self, manifest: LoadedManifest) -> Self {
        let materialized_tail = TailPendingSet {
            entries: manifest.tail_pending_entries.clone(),
        };
        let materialized_coverage = coverage_for_definition(&self.definition, &materialized_tail);
        let coverage =
            if manifest.root.coverage.is_complete() && !materialized_coverage.is_complete() {
                materialized_coverage
            } else {
                manifest.root.coverage.clone()
            };
        let generation = SearchGeneration {
            definition_id: self.definition.definition_id,
            generation_id: manifest.root.generation_id,
            root_version: manifest.root.root_version,
            build_epoch: manifest.root.build_epoch,
            build_snapshot_version: manifest.root.build_snapshot_version,
            indexed_through_ts: manifest.root.indexed_through_ts,
            coverage,
            tail_summary: tail_summary_for_manifest(&manifest),
            manifest_location: manifest_location(
                &manifest.root_path,
                self.definition.definition_id,
                manifest.root.generation_id,
            ),
            generation_stats: manifest.root.generation_stats.clone(),
            execution_modes: manifest.root.execution_modes.clone(),
            config_fingerprint: manifest.root.config_fingerprint,
        };
        let capability = SearchCapability::from_generation(&self.definition, &generation);
        self.next_generation_id = generation.generation_id.saturating_add(1);
        self.next_build_epoch = generation.build_epoch.saturating_add(1);
        self.generation = Some(generation);
        self.capability = Some(capability);
        self.manifest = Some(manifest);
        self
    }

    pub(crate) fn manifest_delta_count(&self) -> usize {
        self.manifest
            .as_ref()
            .map(|manifest| manifest.root.recent_delta_files.len())
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SearchView {
    pub(crate) version: u64,
    pub(crate) definitions: BTreeMap<u64, SearchDefinitionState>,
}

impl SearchView {
    pub(crate) fn hnsw_generation_statistics(
        &self,
        definition_id: u64,
    ) -> Result<Option<crate::statistics::HnswIndexStatistics>> {
        let Some(state) = self.definitions.get(&definition_id) else {
            return Ok(None);
        };
        let Some(capability) = state.capability.as_ref() else {
            return Ok(None);
        };
        if capability.kind != SearchIndexKind::Hnsw {
            return Err(paro_error::invalid_input(format!(
                "search definition {definition_id} is not HNSW"
            )));
        }
        capability.generation_stats.hnsw_index_statistics()
    }

    pub(crate) fn capability(
        &self,
        kind: SearchIndexKind,
        column_id: ColumnId,
        config_fingerprint: Option<u64>,
    ) -> Option<SearchCapability> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if capability.kind != kind {
                return None;
            }
            if !state.definition.column_ids.contains(&column_id) {
                return None;
            }
            if let Some(config_fingerprint) = config_fingerprint {
                if capability.config_fingerprint != config_fingerprint {
                    return None;
                }
            }
            Some(capability.clone())
        })
    }

    pub(crate) fn definition_id_by_name(&self, name: &str) -> Option<u64> {
        self.definitions.iter().find_map(|(definition_id, state)| {
            if state.definition.name == name {
                Some(*definition_id)
            } else {
                None
            }
        })
    }

    pub(crate) fn fulltext_capability(
        &self,
        column_id: ColumnId,
        config: &str,
    ) -> Option<SearchCapability> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if capability.kind != SearchIndexKind::FullText {
                return None;
            }
            if !state.definition.column_ids.contains(&column_id) {
                return None;
            }
            let definition_config = &state.fulltext_provider_config.as_ref()?.config;
            if definition_config.eq_ignore_ascii_case(config) {
                Some(capability.clone())
            } else {
                None
            }
        })
    }

    pub(crate) fn hnsw_capability(
        &self,
        column_id: ColumnId,
        distance: DistanceMetric,
    ) -> Option<SearchCapability> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if capability.kind != SearchIndexKind::Hnsw
                || !state.definition.column_ids.contains(&column_id)
            {
                return None;
            }
            (state.hnsw_provider_config.as_ref()?.distance == distance).then(|| capability.clone())
        })
    }

    pub(crate) fn hnsw_search_policy(
        &self,
        column_id: ColumnId,
        distance: DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswSearchPolicy> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if capability.kind != SearchIndexKind::Hnsw
                || !capability.is_queryable()
                || !state.definition.column_ids.contains(&column_id)
            {
                return None;
            }
            let config = state.hnsw_provider_config.as_ref()?;
            (config.distance == distance).then(|| config.search_policy())
        })
    }

    pub(crate) fn hnsw_filter_topology(
        &self,
        column_id: ColumnId,
        distance: DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswFilterTopologyContract> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if capability.kind != SearchIndexKind::Hnsw
                || !capability.is_queryable()
                || !state.definition.column_ids.contains(&column_id)
            {
                return None;
            }
            let config = state.hnsw_provider_config.as_ref()?;
            (config.distance == distance).then(|| config.build_contract().filter_topology)
        })
    }

    pub(crate) fn has_queryable_artifact(
        &self,
        kind: SearchIndexKind,
        rowset_id: RowsetId,
        segment_id: u32,
        column_id: ColumnId,
    ) -> bool {
        self.definitions.values().any(|state| {
            state.definition.kind == kind
                && state
                    .capability
                    .as_ref()
                    .is_some_and(SearchCapability::is_queryable)
                && state.manifest.as_ref().is_some_and(|manifest| {
                    manifest.artifacts.artifacts.iter().any(|artifact| {
                        artifact.kind == kind
                            && artifact.column_id == column_id
                            && artifact.coverage.contains_segment(
                                super::super::capability::ArtifactSegmentRef {
                                    rowset_id,
                                    segment_id,
                                },
                            )
                    })
                })
        })
    }

    pub(crate) fn write_plan(&self) -> Result<SearchWritePlan> {
        let mut fulltext = BTreeMap::<ColumnId, String>::new();
        let mut sparse = BTreeSet::<ColumnId>::new();

        for state in self.definitions.values() {
            match state.definition.kind {
                SearchIndexKind::FullText => {
                    let Some(column_id) = state.definition.column_ids.first().copied() else {
                        continue;
                    };
                    let config = &state
                        .fulltext_provider_config
                        .as_ref()
                        .ok_or_else(|| {
                            paro_error::internal("FullText definition missing typed config")
                        })?
                        .config;
                    let normalized = TokenizerKind::from_config(config)?
                        .config_name()
                        .to_string();
                    match fulltext.entry(column_id) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(normalized);
                        }
                        std::collections::btree_map::Entry::Occupied(entry) => {
                            if !entry.get().eq_ignore_ascii_case(&normalized) {
                                return Err(paro_error::invalid_input(format!(
                                    "multiple fulltext tokenizer configs for column {} are not supported by the current durable write path",
                                    column_id
                                )));
                            }
                        }
                    }
                }
                SearchIndexKind::Sparse => {
                    state.sparse_provider_config.as_ref().ok_or_else(|| {
                        paro_error::internal("Sparse definition missing typed config")
                    })?;
                    if let Some(column_id) = state.definition.column_ids.first().copied() {
                        sparse.insert(column_id);
                    }
                }
                SearchIndexKind::Hnsw => {}
            }
        }

        Ok(SearchWritePlan {
            fulltext: fulltext
                .into_iter()
                .map(|(column_id, config)| FullTextWriteBinding { column_id, config })
                .collect(),
            sparse: sparse
                .into_iter()
                .map(|column_id| SparseWriteBinding { column_id })
                .collect(),
        })
    }

    pub(crate) fn inline_builder_set(
        &self,
        admission: Option<Arc<dyn SearchAdmission>>,
    ) -> Result<SearchInlineBuilderSet> {
        build_inline_builder_set(
            self.definitions.values().map(|state| {
                let generation_id = state
                    .generation
                    .as_ref()
                    .map(|generation| generation.generation_id)
                    .unwrap_or(state.next_generation_id);
                (&state.definition, generation_id)
            }),
            admission,
        )
    }

    pub(crate) fn write_context(
        &self,
        admission: Option<Arc<dyn SearchAdmission>>,
    ) -> Result<SearchWriteContext> {
        let plan = self.write_plan()?;
        let inline_builders = self.inline_builder_set(admission)?;
        Ok(SearchWriteContext {
            plan,
            inline_builders,
        })
    }
}

pub(crate) fn coverage_for_definition(
    definition: &SearchIndexDefinition,
    tail_pending: &TailPendingSet,
) -> CoverageState {
    if tail_pending.is_empty() {
        return CoverageState::Complete;
    }
    CoverageState::TailPending {
        pending_rowsets: tail_pending.coverage_rowsets(),
        pending_segments: tail_pending.coverage_segments(),
        pending_rows: tail_pending.coverage_rows(),
        exact_tail_merge: provider_tail_exact_merge_policy(definition.kind)
            .exact_tail_merge_enabled(tail_pending.coverage_rows()),
    }
}

pub(crate) fn generation_read_snapshot(
    definition_id: u64,
    state: &SearchDefinitionState,
) -> Result<Option<GenerationReadSnapshot>> {
    let Some(generation) = state.generation.as_ref() else {
        return Ok(None);
    };
    let artifacts = state
        .manifest
        .as_ref()
        .map(|manifest| manifest.artifacts.clone())
        .unwrap_or_else(|| Arc::new(GenerationArtifactSet::default()));

    Ok(Some(GenerationReadSnapshot {
        definition_id,
        generation_id: generation.generation_id,
        build_epoch: generation.build_epoch,
        build_snapshot_version: generation.build_snapshot_version,
        indexed_through_ts: generation.indexed_through_ts,
        coverage: generation.coverage.clone(),
        generation_stats: generation.generation_stats.clone(),
        maintenance_state: state
            .manifest
            .as_ref()
            .map(|manifest| manifest.root.maintenance_state.clone())
            .unwrap_or_default(),
        provider_config: Arc::new(state.definition.provider_config.clone()),
        hnsw_provider_config: state.hnsw_provider_config.clone(),
        artifacts,
        tail_pending_entries: Arc::from(
            state
                .manifest
                .as_ref()
                .map(|manifest| manifest.tail_pending_entries.clone())
                .unwrap_or_default(),
        ),
    }))
}

pub(crate) fn tail_summary_for_manifest(manifest: &LoadedManifest) -> SearchTailSummary {
    let mut pending_rowsets = BTreeSet::new();
    let mut pending_segments = BTreeSet::new();
    let mut pending_rows = 0u64;
    let mut pending_bytes = 0u64;
    let mut delete_rows = 0u64;

    for entry in &manifest.tail_pending_entries {
        if entry.mutation == TailMutationKind::Delete {
            delete_rows = delete_rows.saturating_add(entry.row_count);
            continue;
        }
        pending_rowsets.insert(entry.rowset_id);
        pending_rows = pending_rows.saturating_add(entry.row_count);
        pending_bytes = pending_bytes.saturating_add(entry.byte_count);
        for segment_id in &entry.segment_ids {
            pending_segments.insert((entry.rowset_id, *segment_id));
        }
    }

    let exact_tail_merge = match &manifest.root.coverage {
        CoverageState::Complete => true,
        CoverageState::TailPending {
            exact_tail_merge, ..
        } => *exact_tail_merge,
    };
    let recovery = &manifest.root.maintenance_state.recovery;
    SearchTailSummary {
        pending_rowsets: pending_rowsets.len(),
        pending_segments: pending_segments.len(),
        pending_rows,
        pending_bytes,
        delete_rows,
        exact_tail_merge,
        backlog_tier: recovery.backlog_tier,
        maintenance_priority: recovery.priority,
    }
}

pub(crate) fn record_tail_metrics_for_state(state: &SearchDefinitionState) {
    let Some(manifest) = state.manifest.as_ref() else {
        return;
    };
    let tail_pending = TailPendingSet {
        entries: manifest.tail_pending_entries.clone(),
    };
    storage_metrics().set_search_tail_gauges(
        state.definition.kind,
        tail_pending.coverage_rows(),
        tail_pending.coverage_bytes(),
        tail_backlog_tier_value(manifest.root.maintenance_state.recovery.backlog_tier),
    );
}

pub(crate) fn execution_modes_for_definition(
    definition: &SearchIndexDefinition,
    coverage: &CoverageState,
) -> ExecutionModes {
    let mut modes = match definition.kind {
        SearchIndexKind::Hnsw => ExecutionModes::new([
            SearchExecutionMode::ApproxTopK,
            SearchExecutionMode::ExactFallback,
        ]),
        SearchIndexKind::Sparse | SearchIndexKind::FullText => ExecutionModes::exact_only(),
    };
    if matches!(
        coverage,
        CoverageState::TailPending {
            exact_tail_merge: true,
            ..
        }
    ) {
        modes.insert(SearchExecutionMode::ExactTailMerge);
    }
    modes
}

fn tail_backlog_tier_value(tier: CatchUpBacklogTier) -> u64 {
    match tier {
        CatchUpBacklogTier::Healthy => 0,
        CatchUpBacklogTier::Elevated => 1,
        CatchUpBacklogTier::Degraded => 2,
    }
}
