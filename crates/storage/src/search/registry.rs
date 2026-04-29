// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use serde_json::{json, Value};

use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::rowset::{load_base_rowids, RowsetId, RowsetSharedPtr};
use crate::tablet::{ColumnId, TabletRef};
use paro_common::error::{self as paro_error, Result};

use super::artifact::{ArtifactGcContext, ArtifactGcPolicy, ArtifactLocation, GcDecision};
use super::capability::{
    ArtifactSegmentRef, CoverageState, SearchArtifactRef, SearchCapability, SearchGeneration,
    SearchIndexDefinition, SearchIndexKind,
};
use super::cursor::{GenerationArtifactSet, GenerationReadSnapshot};
use super::manifest::{
    GenerationManifestRoot, LoadedManifest, ManifestDelta, ManifestShard, ManifestStore,
};
use super::stats::{
    BuildWatermarks, CatchUpBacklogTier, ExecutionModes, FullTextProviderStats,
    GenerationMaintenanceState, GenerationRecoveryState, GenerationStats, MaintenancePriority,
    SearchArtifactStats, SearchExecutionMode, SearchGenerationId, SearchProviderStats,
};
use super::tail::{
    provider_tail_merge_policy, TailMutationKind, TailPendingEntry, TailPendingSet, TailRowImageRef,
};
use super::write_path::{
    materialize_rowset_artifacts, FullTextWriteBinding, SearchWritePlan, SparseWriteBinding,
};

const SCHEMA_SEED_BIT: u64 = 1 << 63;

#[inline]
fn indexed_through_ts(visible_version: i64) -> u64 {
    visible_version.max(0) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchDefinitionOrigin {
    Catalog,
    SchemaSeed,
}

#[derive(Debug, Clone)]
struct SearchDefinitionState {
    definition: SearchIndexDefinition,
    origin: SearchDefinitionOrigin,
    generation: Option<SearchGeneration>,
    capability: Option<SearchCapability>,
    manifest: Option<LoadedManifest>,
    next_generation_id: SearchGenerationId,
    next_build_epoch: u64,
}

impl SearchDefinitionState {
    fn new(definition: SearchIndexDefinition, origin: SearchDefinitionOrigin) -> Self {
        Self {
            definition,
            origin,
            generation: None,
            capability: None,
            manifest: None,
            next_generation_id: 1,
            next_build_epoch: 1,
        }
    }

    fn with_manifest(mut self, manifest: LoadedManifest) -> Self {
        let generation = SearchGeneration {
            definition_id: self.definition.definition_id,
            generation_id: manifest.root.generation_id,
            build_epoch: manifest.root.build_epoch,
            build_snapshot_version: manifest.root.build_snapshot_version,
            indexed_through_ts: manifest.root.indexed_through_ts,
            coverage: manifest.root.coverage.clone(),
            manifest_location: manifest_location(&manifest.root_path),
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
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SearchRegistryView {
    version: u64,
    definitions: BTreeMap<u64, SearchDefinitionState>,
}

impl SearchRegistryView {
    fn capability(
        &self,
        kind: SearchIndexKind,
        column_id: ColumnId,
        config_fingerprint: Option<u64>,
    ) -> Option<SearchCapability> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if !capability.is_queryable() {
                return None;
            }
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

    fn definition_id_by_name(&self, name: &str) -> Option<u64> {
        self.definitions.iter().find_map(|(definition_id, state)| {
            if state.definition.name == name {
                Some(*definition_id)
            } else {
                None
            }
        })
    }

    fn fulltext_capability(&self, column_id: ColumnId, config: &str) -> Option<SearchCapability> {
        self.definitions.values().find_map(|state| {
            let capability = state.capability.as_ref()?;
            if !capability.is_queryable() {
                return None;
            }
            if capability.kind != SearchIndexKind::FullText {
                return None;
            }
            if !state.definition.column_ids.contains(&column_id) {
                return None;
            }
            let definition_config = state
                .definition
                .provider_config
                .get("config")
                .and_then(Value::as_str)
                .unwrap_or("simple");
            if definition_config.eq_ignore_ascii_case(config) {
                Some(capability.clone())
            } else {
                None
            }
        })
    }

    fn has_queryable_artifact(
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
                            && artifact.segment.rowset_id == rowset_id
                            && artifact.segment.segment_id == segment_id
                    })
                })
        })
    }

    fn write_plan(&self) -> Result<SearchWritePlan> {
        let mut fulltext = BTreeMap::<ColumnId, String>::new();
        let mut sparse = BTreeSet::<ColumnId>::new();

        for state in self.definitions.values() {
            match state.definition.kind {
                SearchIndexKind::FullText => {
                    let Some(column_id) = state.definition.column_ids.first().copied() else {
                        continue;
                    };
                    let config = state
                        .definition
                        .provider_config
                        .get("config")
                        .and_then(Value::as_str)
                        .unwrap_or("simple");
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
}

#[derive(Debug)]
struct RetiredManifest {
    artifacts: Arc<GenerationArtifactSet>,
    paths: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct VisibleSearchSnapshot {
    visible_version: i64,
    artifacts: Vec<SearchArtifactRef>,
    tail_pending: TailPendingSet,
    coverage: CoverageState,
    generation_stats: GenerationStats,
    execution_modes: ExecutionModes,
    tombstone_rows: u64,
}

#[derive(Debug, Clone)]
struct RowsetSearchSnapshot {
    generation_stats: GenerationStats,
    artifacts: Vec<SearchArtifactRef>,
    tail_entries: TailPendingSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchGenerationCoverage {
    pub visible_version: i64,
    pub indexed_through_ts: u64,
    pub visible_segment_count: usize,
    pub indexed_segment_count: usize,
    pub coverage: CoverageState,
}

impl SearchGenerationCoverage {
    pub fn is_complete(&self) -> bool {
        self.coverage.is_complete()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchBootstrapReport {
    pub definitions_considered: usize,
    pub definitions_updated: usize,
    pub rowsets_materialized: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SearchMaintenanceAction {
    #[default]
    Skip,
    CatchUp,
    Compact,
    Rebuild,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionMaintenanceReport {
    pub definition_id: u64,
    pub action: SearchMaintenanceAction,
    pub gc_decision: GcDecision,
    pub tail_pending_rowsets: usize,
    pub tail_pending_rows: u64,
    pub priority: MaintenancePriority,
    pub backlog_tier: CatchUpBacklogTier,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchMaintenanceReport {
    pub definitions_considered: usize,
    pub definitions_updated: usize,
    pub catch_up_rowsets: usize,
    pub compaction_requested: bool,
    pub definitions: Vec<DefinitionMaintenanceReport>,
}

pub(crate) struct SearchIndexRegistry {
    tablet: TabletRef,
    manifests: ManifestStore,
    view: ArcSwap<SearchRegistryView>,
    publish_locks: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
    retired: Mutex<Vec<RetiredManifest>>,
}

impl std::fmt::Debug for SearchIndexRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let view = self.view.load();
        f.debug_struct("SearchIndexRegistry")
            .field("tablet_id", &self.tablet.tablet_id())
            .field("definition_count", &view.definitions.len())
            .finish()
    }
}

impl SearchIndexRegistry {
    pub(crate) fn new(tablet: TabletRef) -> Self {
        let registry = Self {
            manifests: ManifestStore::new(tablet.data_dir().to_path_buf()),
            tablet,
            view: ArcSwap::from_pointee(SearchRegistryView::default()),
            publish_locks: Mutex::new(HashMap::new()),
            retired: Mutex::new(Vec::new()),
        };
        registry.seed_schema_hnsw_definitions();
        registry
    }

    pub(crate) fn install_definition(&self, definition: SearchIndexDefinition) -> Result<()> {
        self.update_definition(definition, SearchDefinitionOrigin::Catalog)
    }

    pub(crate) fn drop_definition(&self, definition_id: u64) -> Result<()> {
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(());
        };

        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.remove(&definition_id);
        self.retire_manifest(state.manifest.as_ref());
        self.manifests
            .remove_paths(&self.manifests.definition_paths(definition_id));

        if state.origin == SearchDefinitionOrigin::Catalog
            && state.definition.kind == SearchIndexKind::Hnsw
        {
            self.restore_schema_seed_if_needed(&mut next, &state.definition)?;
        }

        self.view.store(Arc::new(next));
        self.sweep_retired();
        Ok(())
    }

    pub(crate) fn drop_definition_by_name(&self, name: &str) -> Result<()> {
        let current = self.view.load();
        if let Some(definition_id) = current.definition_id_by_name(name) {
            return self.drop_definition(definition_id);
        }
        Ok(())
    }

    pub(crate) fn capability(
        &self,
        kind: SearchIndexKind,
        column_id: ColumnId,
        config_fingerprint: Option<u64>,
    ) -> Option<SearchCapability> {
        self.ensure_fresh();
        self.view
            .load()
            .capability(kind, column_id, config_fingerprint)
    }

    pub(crate) fn fulltext_capability(
        &self,
        column_id: ColumnId,
        config: &str,
    ) -> Option<SearchCapability> {
        self.ensure_fresh();
        self.view.load().fulltext_capability(column_id, config)
    }

    pub(crate) fn write_plan(&self) -> Result<SearchWritePlan> {
        self.view.load().write_plan()
    }

    pub(crate) fn has_queryable_artifact(
        &self,
        kind: SearchIndexKind,
        rowset_id: RowsetId,
        segment_id: u32,
        column_id: ColumnId,
    ) -> bool {
        self.ensure_fresh();
        self.view
            .load()
            .has_queryable_artifact(kind, rowset_id, segment_id, column_id)
    }

    pub(crate) fn open_generation_snapshot(
        &self,
        definition_id: u64,
    ) -> Result<Option<GenerationReadSnapshot>> {
        self.ensure_fresh();
        let current = self.view.load();
        let Some(state) = current.definitions.get(&definition_id) else {
            return Ok(None);
        };
        let Some(generation) = &state.generation else {
            return Ok(None);
        };
        let artifacts = state
            .manifest
            .as_ref()
            .map(|manifest| Arc::new(manifest.artifacts.clone()))
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
            artifacts,
        }))
    }

    pub(crate) fn generation_coverage(
        &self,
        definition_id: u64,
    ) -> Result<Option<SearchGenerationCoverage>> {
        self.ensure_fresh();
        let current = self.view.load();
        let Some(state) = current.definitions.get(&definition_id) else {
            return Ok(None);
        };
        let Some(generation) = &state.generation else {
            return Ok(None);
        };
        let manifest = state.manifest.as_ref();
        let indexed_segment_count = manifest
            .map(|manifest| {
                manifest
                    .artifacts
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.segment)
                    .collect::<BTreeSet<_>>()
                    .len()
            })
            .unwrap_or_default();
        let tail_pending = TailPendingSet {
            entries: manifest
                .map(|manifest| manifest.root.tail_pending_entries.clone())
                .unwrap_or_default(),
        };
        Ok(Some(SearchGenerationCoverage {
            visible_version: generation.build_snapshot_version,
            indexed_through_ts: generation.indexed_through_ts,
            visible_segment_count: indexed_segment_count + tail_pending.coverage_segments(),
            indexed_segment_count,
            coverage: generation.coverage.clone(),
        }))
    }

    pub(crate) fn catch_up_definition(&self, definition_id: u64) -> Result<usize> {
        self.ensure_fresh();
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(0);
        };
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(0);
        };

        let plan = definition_write_plan(&state.definition)?;
        if plan.is_empty() {
            return Ok(0);
        }

        let visible_rowsets = self
            .tablet
            .capture_consistent_rowsets(self.tablet.max_version())?;
        let visible_by_id = visible_rowsets
            .into_iter()
            .map(|rowset| (rowset.rowset_id(), rowset))
            .collect::<BTreeMap<_, _>>();

        let mut touched = 0usize;
        let mut touched_rows = 0u64;
        let rowset_limit = manifest
            .root
            .maintenance_state
            .recovery
            .rowset_rate_limit
            .max(1);
        let row_limit = manifest
            .root
            .maintenance_state
            .recovery
            .row_rate_limit
            .max(1);
        for entry in &manifest.root.tail_pending_entries {
            if matches!(entry.mutation, TailMutationKind::Delete) {
                continue;
            }
            if touched >= rowset_limit {
                break;
            }
            if touched > 0 && touched_rows.saturating_add(entry.row_count) > row_limit {
                break;
            }
            if let Some(rowset) = visible_by_id.get(&entry.rowset_id) {
                rowset.load()?;
                if !rowset_can_materialize_definition(&state.definition, rowset) {
                    continue;
                }
                materialize_rowset_artifacts(rowset, &plan)?;
                touched += 1;
                touched_rows = touched_rows.saturating_add(entry.row_count);
            }
        }
        if touched > 0 {
            let _ = self.refresh_definition_inner(definition_id, true)?;
        }
        Ok(touched)
    }

    pub(crate) fn bootstrap_migration(&self) -> Result<SearchBootstrapReport> {
        self.ensure_fresh();
        let definition_ids = self
            .view
            .load()
            .definitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut report = SearchBootstrapReport {
            definitions_considered: definition_ids.len(),
            ..SearchBootstrapReport::default()
        };
        for definition_id in definition_ids {
            let updated = self.catch_up_definition(definition_id)?;
            if updated > 0 {
                report.definitions_updated += 1;
                report.rowsets_materialized += updated;
            }
        }
        Ok(report)
    }

    pub(crate) fn maintenance_sweep(&self) -> Result<SearchMaintenanceReport> {
        self.ensure_fresh();
        let current = self.view.load_full();
        let definition_ids = current.definitions.keys().copied().collect::<Vec<_>>();
        let mut report = SearchMaintenanceReport {
            definitions_considered: definition_ids.len(),
            ..SearchMaintenanceReport::default()
        };

        drop(current);

        for definition_id in definition_ids {
            let snapshot = self.view.load();
            let Some(state) = snapshot.definitions.get(&definition_id).cloned() else {
                continue;
            };
            let Some(manifest) = state.manifest.as_ref() else {
                continue;
            };

            let recovery = &manifest.root.maintenance_state.recovery;
            let gc_context = ArtifactGcContext {
                bytes_on_disk: manifest
                    .artifacts
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.stats.bytes_on_disk)
                    .sum(),
                tombstone_ratio: Some(
                    manifest.root.maintenance_state.tombstone_ratio_millis as f32 / 1000.0,
                ),
                query_pressure: Some(match recovery.priority {
                    MaintenancePriority::Idle => 0.0,
                    MaintenancePriority::Opportunistic => 0.25,
                    MaintenancePriority::Elevated => 0.6,
                    MaintenancePriority::Critical => 1.0,
                }),
                provider_stats: manifest.root.generation_stats.provider_stats.clone(),
            };
            let gc_decision = gc_policy_for_kind(state.definition.kind).should_gc(&gc_context);
            let mut action = match gc_decision {
                GcDecision::Skip => SearchMaintenanceAction::Skip,
                GcDecision::CompactOnly => SearchMaintenanceAction::Compact,
                GcDecision::Heal => SearchMaintenanceAction::CatchUp,
                GcDecision::Rebuild => SearchMaintenanceAction::Rebuild,
            };
            if recovery.tail_pending_rows > 0 {
                let touched = self.catch_up_definition(definition_id)?;
                if touched > 0 {
                    report.definitions_updated += 1;
                    report.catch_up_rowsets = report.catch_up_rowsets.saturating_add(touched);
                }
                if matches!(action, SearchMaintenanceAction::Skip) {
                    action = SearchMaintenanceAction::CatchUp;
                }
            }
            if !matches!(gc_decision, GcDecision::Skip) {
                report.compaction_requested = true;
            }
            report.definitions.push(DefinitionMaintenanceReport {
                definition_id,
                action,
                gc_decision,
                tail_pending_rowsets: recovery.tail_pending_rowsets,
                tail_pending_rows: recovery.tail_pending_rows,
                priority: recovery.priority,
                backlog_tier: recovery.backlog_tier,
            });
        }

        Ok(report)
    }

    #[cfg(test)]
    pub(crate) fn definition_count(&self) -> usize {
        self.view.load().definitions.len()
    }

    pub(crate) fn catalog_definition_count(&self) -> usize {
        self.view
            .load()
            .definitions
            .values()
            .filter(|state| state.origin == SearchDefinitionOrigin::Catalog)
            .count()
    }

    pub(crate) fn refresh_definition(
        &self,
        definition_id: u64,
    ) -> Result<Option<SearchCapability>> {
        self.refresh_definition_inner(definition_id, false)
    }

    fn refresh_definition_inner(
        &self,
        definition_id: u64,
        force: bool,
    ) -> Result<Option<SearchCapability>> {
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(None);
        };

        let visible_version = self.tablet.max_version();
        if !force
            && state
                .generation
                .as_ref()
                .is_some_and(|generation| generation.build_snapshot_version == visible_version)
        {
            return Ok(state.capability.clone());
        }

        let next_state = self.refresh_state_from_storage(&state, force)?;
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.insert(definition_id, next_state.clone());
        self.view.store(Arc::new(next));
        self.sweep_retired();
        Ok(next_state.capability)
    }

    pub(crate) fn ensure_fresh(&self) {
        let definition_ids = self
            .view
            .load()
            .definitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for definition_id in definition_ids {
            if let Err(err) = self.refresh_definition(definition_id) {
                tracing::warn!(
                    tablet_id = self.tablet.tablet_id(),
                    definition_id,
                    error = %err,
                    "search registry refresh failed"
                );
            }
        }
    }

    fn update_definition(
        &self,
        definition: SearchIndexDefinition,
        origin: SearchDefinitionOrigin,
    ) -> Result<()> {
        let definition_lock = self.definition_lock(definition.definition_id);
        let guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition update"))?;

        let current = self.view.load_full();
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);

        if origin == SearchDefinitionOrigin::Catalog {
            let duplicate_seed_ids = next
                .definitions
                .iter()
                .filter_map(|(definition_id, state)| {
                    if state.origin == SearchDefinitionOrigin::SchemaSeed
                        && state.definition.kind == definition.kind
                        && state.definition.column_ids == definition.column_ids
                    {
                        Some(*definition_id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for duplicate_seed_id in duplicate_seed_ids {
                if let Some(seed_state) = next.definitions.remove(&duplicate_seed_id) {
                    self.retire_manifest(seed_state.manifest.as_ref());
                    self.manifests
                        .remove_paths(&self.manifests.definition_paths(duplicate_seed_id));
                }
            }
        }

        let mut state = SearchDefinitionState::new(definition.clone(), origin);
        if let Some(loaded) = self.manifests.load_manifest(definition.definition_id)? {
            if loaded.root.config_fingerprint == definition.config_fingerprint {
                state = state.with_manifest(loaded);
            }
        }
        next.definitions.insert(definition.definition_id, state);
        self.view.store(Arc::new(next));
        drop(guard);
        let _ = self.refresh_definition(definition.definition_id);
        Ok(())
    }

    fn refresh_state_from_storage(
        &self,
        state: &SearchDefinitionState,
        force: bool,
    ) -> Result<SearchDefinitionState> {
        let visible_version = self.tablet.max_version();
        let visible_rowsets = self.tablet.capture_consistent_rowsets(visible_version)?;
        let visible_rowset_ids = visible_rowsets
            .iter()
            .map(|rowset| rowset.rowset_id())
            .collect::<BTreeSet<_>>();

        if let Some(manifest) = &state.manifest {
            let known_artifact_rowset_ids = manifest
                .artifacts
                .artifacts
                .iter()
                .map(|artifact| artifact.segment.rowset_id)
                .collect::<BTreeSet<_>>();
            let known_tail_rowset_ids = manifest
                .root
                .tail_pending_entries
                .iter()
                .map(|entry| entry.rowset_id)
                .collect::<BTreeSet<_>>();
            let known_rowset_ids = known_artifact_rowset_ids
                .union(&known_tail_rowset_ids)
                .copied()
                .collect::<BTreeSet<_>>();
            let removed_rowsets = known_rowset_ids
                .difference(&visible_rowset_ids)
                .copied()
                .collect::<Vec<_>>();
            if removed_rowsets.is_empty() {
                let new_rowsets = visible_rowset_ids
                    .difference(&known_rowset_ids)
                    .copied()
                    .collect::<Vec<_>>();
                if !new_rowsets.is_empty() {
                    return self.publish_delta_for_new_rowsets(
                        state,
                        &visible_rowsets,
                        &new_rowsets,
                    );
                }
                if !force && manifest.root.build_snapshot_version == visible_version {
                    return Ok(state.clone());
                }
            }
        }

        self.publish_full_snapshot(state, &visible_rowsets)
    }

    fn publish_full_snapshot(
        &self,
        state: &SearchDefinitionState,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<SearchDefinitionState> {
        let snapshot = self.collect_visible_snapshot(&state.definition, visible_rowsets)?;

        let generation_id = state
            .generation
            .as_ref()
            .map_or(state.next_generation_id, |generation| {
                generation.generation_id
            });
        let build_epoch = state.next_build_epoch;
        let root_version = state
            .manifest
            .as_ref()
            .map_or(1, |manifest| manifest.root.root_version.saturating_add(1));
        let definition_id = state.definition.definition_id;
        let mut root = GenerationManifestRoot {
            definition_id,
            generation_id,
            build_epoch,
            build_snapshot_version: snapshot.visible_version,
            indexed_through_ts: indexed_through_ts(snapshot.visible_version),
            config_fingerprint: state.definition.config_fingerprint,
            coverage: snapshot.coverage.clone(),
            generation_stats: snapshot.generation_stats.clone(),
            execution_modes: snapshot.execution_modes.clone(),
            tail_pending_entries: snapshot.tail_pending.entries.clone(),
            maintenance_state: build_maintenance_state(
                &state.definition,
                snapshot.visible_version,
                build_epoch,
                snapshot.generation_stats.indexed_rows,
                &snapshot.tail_pending,
                snapshot.tombstone_rows,
                state
                    .manifest
                    .as_ref()
                    .map(|manifest| manifest.root.build_epoch),
                state
                    .manifest
                    .as_ref()
                    .map(|manifest| {
                        manifest
                            .root
                            .maintenance_state
                            .recovery
                            .superseded_build_epochs
                            .clone()
                    })
                    .unwrap_or_default(),
            ),
            root_version,
            checksum: 0,
            shard_files: Vec::new(),
            recent_delta_files: Vec::new(),
        };
        let shard_name = self.manifests.write_shard(
            definition_id,
            generation_id,
            root.root_version,
            &ManifestShard {
                artifact_refs: assign_generation_id(snapshot.artifacts.clone(), generation_id),
            },
        )?;
        root.shard_files.push(shard_name);
        root.recompute_checksum()?;
        let root_path = self.manifests.write_root(definition_id, &root)?;
        let loaded = LoadedManifest {
            root: root.clone(),
            root_path,
            shard_paths: root
                .shard_files
                .iter()
                .map(|name| self.manifests.definition_dir(definition_id).join(name))
                .collect(),
            delta_paths: Vec::new(),
            artifacts: GenerationArtifactSet {
                artifacts: assign_generation_id(snapshot.artifacts, generation_id),
            },
        };

        let mut next_state = state.clone();
        self.retire_manifest(state.manifest.as_ref());
        next_state = next_state.with_manifest(loaded);
        Ok(next_state)
    }

    fn publish_delta_for_new_rowsets(
        &self,
        state: &SearchDefinitionState,
        visible_rowsets: &[RowsetSharedPtr],
        new_rowset_ids: &[RowsetId],
    ) -> Result<SearchDefinitionState> {
        let Some(current_manifest) = state.manifest.as_ref() else {
            return self.publish_full_snapshot(state, visible_rowsets);
        };

        let mut added_artifacts = Vec::new();
        let mut added_tail_entries = Vec::new();
        let mut delta_generation_stats = empty_generation_stats_for_definition(&state.definition);
        for rowset in visible_rowsets {
            if !new_rowset_ids.contains(&rowset.rowset_id()) {
                continue;
            }
            rowset.load()?;
            let rowset_snapshot =
                self.collect_rowset_snapshot(&state.definition, rowset, self.tablet.max_version())?;
            delta_generation_stats.merge_assign(&rowset_snapshot.generation_stats);
            added_artifacts.extend(rowset_snapshot.artifacts);
            added_tail_entries.extend(rowset_snapshot.tail_entries.entries);
        }

        let definition_id = state.definition.definition_id;
        let generation = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("delta publish requires existing generation"))?;
        added_artifacts = assign_generation_id(added_artifacts, generation.generation_id);
        let mut root = current_manifest.root.clone();
        root.root_version = root.root_version.saturating_add(1);
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = self.tablet.max_version();
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);
        root.tail_pending_entries.extend(added_tail_entries);
        root.generation_stats.merge_assign(&delta_generation_stats);
        let tail_pending = TailPendingSet {
            entries: root.tail_pending_entries.clone(),
        };
        root.coverage = coverage_for_definition(&state.definition, &tail_pending);
        root.execution_modes = execution_modes_for_definition(&state.definition, &root.coverage);
        root.maintenance_state = build_maintenance_state(
            &state.definition,
            root.build_snapshot_version,
            root.build_epoch,
            root.generation_stats.indexed_rows,
            &tail_pending,
            tail_pending.delete_rows(),
            Some(current_manifest.root.build_epoch),
            current_manifest
                .root
                .maintenance_state
                .recovery
                .superseded_build_epochs
                .clone(),
        );

        let delta_name = self.manifests.write_delta(
            definition_id,
            generation.generation_id,
            root.root_version,
            root.recent_delta_files.len(),
            &ManifestDelta {
                added_artifacts: added_artifacts.clone(),
                removed_segments: Vec::new(),
            },
        )?;
        root.recent_delta_files.push(delta_name);
        root.recompute_checksum()?;
        self.manifests
            .maybe_compact_deltas(definition_id, &mut root)?;
        self.manifests.write_root(definition_id, &root)?;
        let mut artifacts = current_manifest.artifacts.clone();
        artifacts.artifacts.extend(added_artifacts);
        let loaded = self
            .manifests
            .materialize_loaded_manifest(definition_id, root, artifacts);
        let mut next_state = state.clone();
        self.retire_manifest(state.manifest.as_ref());
        next_state = next_state.with_manifest(loaded);
        Ok(next_state)
    }

    fn collect_visible_snapshot(
        &self,
        definition: &SearchIndexDefinition,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<VisibleSearchSnapshot> {
        let mut generation_stats = empty_generation_stats_for_definition(definition);
        let mut artifacts = Vec::new();
        let mut tail_entries = Vec::new();

        for rowset in visible_rowsets {
            rowset.load()?;
            let rowset_snapshot =
                self.collect_rowset_snapshot(definition, rowset, self.tablet.max_version())?;
            generation_stats.merge_assign(&rowset_snapshot.generation_stats);
            artifacts.extend(rowset_snapshot.artifacts);
            tail_entries.extend(rowset_snapshot.tail_entries.entries);
        }

        let visible_version = self.tablet.max_version();
        let tail_pending = TailPendingSet {
            entries: tail_entries,
        };
        let coverage = coverage_for_definition(definition, &tail_pending);
        let execution_modes = execution_modes_for_definition(definition, &coverage);
        let tombstone_rows = tail_pending.delete_rows();
        if visible_rowsets.is_empty() {
            return Ok(VisibleSearchSnapshot {
                visible_version,
                artifacts: Vec::new(),
                tail_pending,
                coverage,
                generation_stats,
                execution_modes,
                tombstone_rows: 0,
            });
        }

        Ok(VisibleSearchSnapshot {
            visible_version,
            tail_pending,
            coverage,
            tombstone_rows,
            generation_stats,
            execution_modes,
            artifacts,
        })
    }

    fn collect_rowset_snapshot(
        &self,
        definition: &SearchIndexDefinition,
        rowset: &RowsetSharedPtr,
        visible_version: i64,
    ) -> Result<RowsetSearchSnapshot> {
        let mut artifacts = Vec::new();
        let mut generation_stats = empty_generation_stats_for_definition(definition);
        let mut delete_entries = Vec::new();
        let mut missing_segments = Vec::new();

        for segment in rowset.segments() {
            let deleted_rows = segment
                .load_delete_vector_with_epoch(visible_version as u64)?
                .map(|delete_vector| delete_vector.bitmap().len() as u64)
                .unwrap_or_default();
            let segment_rows = u64::try_from(segment.num_rows()).unwrap_or_default();
            let live_rows = segment_rows.saturating_sub(deleted_rows);
            if deleted_rows > 0 {
                delete_entries.push(TailPendingEntry {
                    rowset_id: rowset.rowset_id(),
                    segment_ids: vec![segment.segment_id()],
                    mutation: TailMutationKind::Delete,
                    row_count: deleted_rows,
                    row_image_ref: None,
                });
            }

            let mut segment_complete = true;
            let mut segment_artifacts = Vec::new();
            for column_id in &definition.column_ids {
                let artifact =
                    self.segment_artifact(definition, rowset, segment.segment_id(), *column_id)?;
                let Some(artifact) = artifact else {
                    segment_complete = false;
                    break;
                };
                segment_artifacts.push(artifact);
            }
            if !segment_complete {
                missing_segments.push(segment.segment_id());
                continue;
            }
            generation_stats.indexed_rows = generation_stats.indexed_rows.saturating_add(live_rows);
            generation_stats.artifact_count = generation_stats
                .artifact_count
                .saturating_add(segment_artifacts.len());
            merge_provider_stats_into_generation(
                &mut generation_stats,
                segment_artifacts
                    .iter()
                    .filter_map(|artifact| artifact.stats.provider_stats.as_ref().cloned()),
            );
            artifacts.extend(segment_artifacts);
        }

        let mut tail_entries = delete_entries;
        if !missing_segments.is_empty() {
            tail_entries.push(self.rowset_tail_entry(
                definition,
                rowset,
                &missing_segments,
                visible_version,
            )?);
        }

        Ok(RowsetSearchSnapshot {
            generation_stats,
            artifacts,
            tail_entries: TailPendingSet {
                entries: tail_entries,
            },
        })
    }

    fn segment_artifact(
        &self,
        definition: &SearchIndexDefinition,
        rowset: &RowsetSharedPtr,
        segment_id: u32,
        column_id: ColumnId,
    ) -> Result<Option<SearchArtifactRef>> {
        let rowset_id = rowset.rowset_id();
        let segment = rowset
            .segments()
            .iter()
            .find(|segment| segment.segment_id() == segment_id)
            .cloned()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "segment {} missing from rowset {}",
                    segment_id, rowset_id
                ))
            })?;

        let present = match definition.kind {
            SearchIndexKind::Hnsw => segment.hnsw_index(column_id).is_some(),
            SearchIndexKind::Sparse => segment.sparse_index(column_id).is_some(),
            SearchIndexKind::FullText => {
                let Some(index) = segment.fulltext_index(column_id) else {
                    return Ok(None);
                };
                let expected_config = definition
                    .provider_config
                    .get("config")
                    .and_then(Value::as_str)
                    .unwrap_or("simple");
                let actual = index.tokenizer().kind().config_name();
                if !actual.eq_ignore_ascii_case(expected_config) {
                    return Ok(None);
                }
                true
            }
        };

        if !present {
            return Ok(None);
        }

        let (bytes_on_disk, provider_stats) =
            search_artifact_metadata(definition.kind, &segment, column_id);

        Ok(Some(SearchArtifactRef {
            definition_id: definition.definition_id,
            generation_id: 0,
            segment: ArtifactSegmentRef {
                rowset_id,
                segment_id,
            },
            column_id,
            kind: definition.kind,
            provider_variant: definition.config_fingerprint as u32,
            artifact_format_version: 1,
            location: ArtifactLocation::InlineSegmentBlob {
                rowset_id,
                segment_id,
                column_id,
            },
            stats: SearchArtifactStats {
                row_count: segment.num_rows(),
                bytes_on_disk,
                provider_stats,
            },
            checksum: seahash::hash(
                format!(
                    "{}:{}:{}:{}",
                    definition.definition_id, rowset_id, segment_id, column_id
                )
                .as_bytes(),
            ),
        }))
    }

    fn rowset_tail_entry(
        &self,
        _definition: &SearchIndexDefinition,
        rowset: &RowsetSharedPtr,
        missing_segments: &[u32],
        visible_version: i64,
    ) -> Result<TailPendingEntry> {
        let mut touched_columns = BTreeSet::new();
        let mut base_rowids_segments = Vec::new();
        let mut row_count = 0u64;

        for segment in rowset.segments() {
            if !missing_segments.contains(&segment.segment_id()) {
                continue;
            }
            let deleted_rows = segment
                .load_delete_vector_with_epoch(visible_version as u64)?
                .map(|delete_vector| delete_vector.bitmap().len() as u64)
                .unwrap_or_default();
            let segment_rows = u64::try_from(segment.num_rows()).unwrap_or_default();
            row_count = row_count.saturating_add(segment_rows.saturating_sub(deleted_rows));
            for meta in segment.column_metas() {
                touched_columns.insert(meta.column_id);
            }
            if load_base_rowids(rowset.rowset_path(), segment.segment_id())?.is_some() {
                base_rowids_segments.push(segment.segment_id());
            }
        }

        let row_image_ref = if base_rowids_segments.is_empty() {
            Some(TailRowImageRef::WholeRowset)
        } else {
            Some(TailRowImageRef::PartialRowset {
                touched_columns: touched_columns.into_iter().collect(),
                base_rowids_segments: base_rowids_segments.clone(),
            })
        };

        Ok(TailPendingEntry {
            rowset_id: rowset.rowset_id(),
            segment_ids: missing_segments.to_vec(),
            mutation: if base_rowids_segments.is_empty() {
                TailMutationKind::Append
            } else {
                TailMutationKind::Replace
            },
            row_count,
            row_image_ref,
        })
    }

    fn definition_lock(&self, definition_id: u64) -> Arc<Mutex<()>> {
        let mut guard = self.publish_locks.lock().expect("search publish locks");
        guard
            .entry(definition_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn retire_manifest(&self, manifest: Option<&LoadedManifest>) {
        let Some(manifest) = manifest else {
            return;
        };
        let retired = RetiredManifest {
            artifacts: Arc::new(manifest.artifacts.clone()),
            paths: manifest.all_paths(),
        };
        if let Ok(mut guard) = self.retired.lock() {
            guard.push(retired);
        }
    }

    fn sweep_retired(&self) {
        let mut keep = Vec::new();
        let mut remove = Vec::new();
        if let Ok(mut guard) = self.retired.lock() {
            for retired in guard.drain(..) {
                // Each retired manifest snapshots its own Arc clone of the artifact set.
                // Once the retire queue is the sole remaining owner, no active read lease can
                // still observe these manifest paths and it is safe to reclaim them.
                if Arc::strong_count(&retired.artifacts) == 1 {
                    remove.push(retired.paths);
                } else {
                    keep.push(retired);
                }
            }
            *guard = keep;
        }
        for paths in remove {
            self.manifests.remove_paths(&paths);
        }
    }

    fn restore_schema_seed_if_needed(
        &self,
        view: &mut SearchRegistryView,
        definition: &SearchIndexDefinition,
    ) -> Result<()> {
        if definition.kind != SearchIndexKind::Hnsw || definition.column_ids.len() != 1 {
            return Ok(());
        }
        let Some(schema) = self.tablet.schema() else {
            return Ok(());
        };
        let column_id = definition.column_ids[0];
        let Some(column) = schema.column_by_id(column_id) else {
            return Ok(());
        };
        if !column.index_hnsw {
            return Ok(());
        }
        let seed = schema_seed_definition(self.tablet.table_id(), column)?;
        view.definitions
            .entry(seed.definition_id)
            .or_insert_with(|| {
                SearchDefinitionState::new(seed, SearchDefinitionOrigin::SchemaSeed)
            });
        Ok(())
    }

    fn seed_schema_hnsw_definitions(&self) {
        let Some(schema) = self.tablet.schema() else {
            return;
        };
        for column in schema.columns().iter().filter(|column| column.index_hnsw) {
            match schema_seed_definition(self.tablet.table_id(), column) {
                Ok(definition) => {
                    let _ = self.update_definition(definition, SearchDefinitionOrigin::SchemaSeed);
                }
                Err(err) => {
                    tracing::warn!(
                        tablet_id = self.tablet.tablet_id(),
                        column_id = column.id,
                        error = %err,
                        "seed schema hnsw definition failed"
                    );
                }
            }
        }
    }
}

fn definition_write_plan(definition: &SearchIndexDefinition) -> Result<SearchWritePlan> {
    match definition.kind {
        SearchIndexKind::FullText => {
            let Some(column_id) = definition.column_ids.first().copied() else {
                return Ok(SearchWritePlan::default());
            };
            let config = definition
                .provider_config
                .get("config")
                .and_then(Value::as_str)
                .unwrap_or("simple");
            let normalized = TokenizerKind::from_config(config)?
                .config_name()
                .to_string();
            Ok(SearchWritePlan {
                fulltext: vec![FullTextWriteBinding {
                    column_id,
                    config: normalized,
                }],
                sparse: Vec::new(),
            })
        }
        SearchIndexKind::Sparse => {
            let Some(column_id) = definition.column_ids.first().copied() else {
                return Ok(SearchWritePlan::default());
            };
            Ok(SearchWritePlan {
                fulltext: Vec::new(),
                sparse: vec![SparseWriteBinding { column_id }],
            })
        }
        SearchIndexKind::Hnsw => Ok(SearchWritePlan::default()),
    }
}

fn rowset_can_materialize_definition(
    definition: &SearchIndexDefinition,
    rowset: &RowsetSharedPtr,
) -> bool {
    rowset.segments().iter().all(|segment| {
        definition.column_ids.iter().all(|column_id| {
            segment
                .column_metas()
                .iter()
                .any(|meta| meta.column_id == *column_id)
        })
    })
}

fn coverage_for_definition(
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
        exact_tail_merge: provider_tail_merge_policy(definition.kind)
            .exact_tail_merge_enabled(tail_pending.coverage_rows()),
    }
}

fn execution_modes_for_definition(
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

fn schema_seed_definition(
    table_id: u64,
    column: &crate::tablet::TabletColumn,
) -> Result<SearchIndexDefinition> {
    let provider_config = json!({
        "m": column.hnsw_m,
        "ef_construct": column.hnsw_ef_construct,
        "distance": column.hnsw_distance,
    });
    Ok(SearchIndexDefinition {
        definition_id: SCHEMA_SEED_BIT | column.id as u64,
        table_id,
        name: format!("__schema_hnsw_col_{}", column.id),
        kind: SearchIndexKind::Hnsw,
        column_ids: vec![column.id],
        expression: None,
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Hnsw,
            &[column.id],
            None,
            &provider_config,
        ),
        provider_config,
    })
}

fn manifest_location(path: &std::path::Path) -> Option<ArtifactLocation> {
    let byte_length = fs::metadata(path).ok()?.len();
    Some(ArtifactLocation::SidecarArtifactFile {
        relative_path: path.to_path_buf(),
        byte_offset: 0,
        byte_length,
    })
}

fn assign_generation_id(
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

fn empty_generation_stats_for_definition(definition: &SearchIndexDefinition) -> GenerationStats {
    let provider_stats = match definition.kind {
        SearchIndexKind::FullText => Some(SearchProviderStats::FullText(
            FullTextProviderStats::empty_for_config(
                definition
                    .provider_config
                    .get("config")
                    .and_then(Value::as_str)
                    .unwrap_or("simple"),
            ),
        )),
        SearchIndexKind::Hnsw | SearchIndexKind::Sparse => None,
    };
    GenerationStats {
        indexed_rows: 0,
        artifact_count: 0,
        provider_stats,
    }
}

fn merge_provider_stats_into_generation<I>(
    generation_stats: &mut GenerationStats,
    provider_stats: I,
) where
    I: IntoIterator<Item = SearchProviderStats>,
{
    for provider_stats in provider_stats {
        generation_stats.merge_assign(&GenerationStats {
            indexed_rows: 0,
            artifact_count: 0,
            provider_stats: Some(provider_stats),
        });
    }
}

fn search_artifact_metadata(
    kind: SearchIndexKind,
    segment: &crate::rowset::SegmentSharedPtr,
    column_id: ColumnId,
) -> (u64, Option<SearchProviderStats>) {
    let bytes_on_disk = segment
        .get_column_meta(column_id)
        .and_then(|meta| match kind {
            SearchIndexKind::Hnsw => meta.hnsw_index_pointer.map(|ptr| ptr.size as u64),
            SearchIndexKind::Sparse => meta.sparse_index_pointer.map(|ptr| ptr.size as u64),
            SearchIndexKind::FullText => meta.fulltext_index_pointer.map(|ptr| ptr.size as u64),
        })
        .unwrap_or(0);
    let provider_stats = match kind {
        SearchIndexKind::FullText => segment
            .fulltext_index_statistics(column_id)
            .map(|stats| SearchProviderStats::FullText((&stats).into())),
        SearchIndexKind::Hnsw | SearchIndexKind::Sparse => None,
    };
    (bytes_on_disk, provider_stats)
}

fn build_maintenance_state(
    definition: &SearchIndexDefinition,
    visible_version: i64,
    build_epoch: u64,
    indexed_rows: u64,
    tail_pending: &TailPendingSet,
    tombstone_rows: u64,
    previous_build_epoch: Option<u64>,
    mut superseded_build_epochs: Vec<u64>,
) -> GenerationMaintenanceState {
    let pending_rows = tail_pending.coverage_rows();
    let pending_rowsets = tail_pending.coverage_rowsets();
    let tail_policy = provider_tail_merge_policy(definition.kind);
    let backlog_tier = if pending_rows <= tail_policy.soft_row_limit {
        CatchUpBacklogTier::Healthy
    } else if pending_rows <= tail_policy.hard_row_limit {
        CatchUpBacklogTier::Elevated
    } else {
        CatchUpBacklogTier::Degraded
    };
    let priority = if pending_rows == 0 {
        MaintenancePriority::Idle
    } else if pending_rows <= tail_policy.soft_row_limit {
        MaintenancePriority::Opportunistic
    } else if pending_rows <= tail_policy.hard_row_limit {
        MaintenancePriority::Elevated
    } else {
        MaintenancePriority::Critical
    };
    let rowset_rate_limit = match priority {
        MaintenancePriority::Idle => 0,
        MaintenancePriority::Opportunistic => 8,
        MaintenancePriority::Elevated => 4,
        MaintenancePriority::Critical => 2,
    };
    let row_rate_limit = match priority {
        MaintenancePriority::Idle => 0,
        MaintenancePriority::Opportunistic => tail_policy.soft_row_limit.max(1),
        MaintenancePriority::Elevated | MaintenancePriority::Critical => {
            tail_policy.hard_row_limit.max(1)
        }
    };
    if let Some(previous_build_epoch) = previous_build_epoch.filter(|epoch| *epoch != build_epoch) {
        if !superseded_build_epochs.contains(&previous_build_epoch) {
            superseded_build_epochs.push(previous_build_epoch);
        }
    }
    if superseded_build_epochs.len() > 8 {
        let keep_from = superseded_build_epochs.len().saturating_sub(8);
        superseded_build_epochs = superseded_build_epochs.split_off(keep_from);
    }
    let tombstone_ratio_millis = if tombstone_rows == 0 {
        0
    } else {
        let denominator = indexed_rows.saturating_add(tombstone_rows).max(1);
        ((tombstone_rows as u128)
            .saturating_mul(1000)
            .saturating_div(denominator as u128))
        .min(u32::MAX as u128) as u32
    };

    GenerationMaintenanceState {
        build_watermarks: BuildWatermarks {
            snapshot_version: visible_version,
            replay_watermark: visible_version,
            cutover_watermark: visible_version,
        },
        recovery: GenerationRecoveryState {
            catch_up_build_epoch: (pending_rows > 0).then_some(build_epoch),
            superseded_build_epochs,
            tail_pending_rowsets: pending_rowsets,
            tail_pending_rows: pending_rows,
            backlog_tier,
            priority,
            rowset_rate_limit,
            row_rate_limit,
        },
        tombstone_rows,
        tombstone_ratio_millis,
    }
}

struct HnswGcPolicy;
struct SparseGcPolicy;
struct FullTextGcPolicy;

impl ArtifactGcPolicy for HnswGcPolicy {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
        let tombstone_ratio = context.tombstone_ratio.unwrap_or_default();
        if tombstone_ratio >= 0.15 {
            GcDecision::Rebuild
        } else if tombstone_ratio >= 0.05 && context.query_pressure.unwrap_or_default() >= 0.7 {
            GcDecision::Heal
        } else if context.bytes_on_disk >= 256 * 1024 * 1024 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

impl ArtifactGcPolicy for SparseGcPolicy {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
        let tombstone_ratio = context.tombstone_ratio.unwrap_or_default();
        if tombstone_ratio >= 0.35 {
            GcDecision::Rebuild
        } else if context.bytes_on_disk >= 128 * 1024 * 1024 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

impl ArtifactGcPolicy for FullTextGcPolicy {
    fn should_gc(&self, context: &ArtifactGcContext) -> GcDecision {
        let tombstone_ratio = context.tombstone_ratio.unwrap_or_default();
        if tombstone_ratio >= 0.25 {
            GcDecision::Rebuild
        } else if tombstone_ratio >= 0.1 && context.query_pressure.unwrap_or_default() >= 0.6 {
            GcDecision::Heal
        } else if context.bytes_on_disk >= 128 * 1024 * 1024 {
            GcDecision::CompactOnly
        } else {
            GcDecision::Skip
        }
    }
}

fn gc_policy_for_kind(kind: SearchIndexKind) -> &'static dyn ArtifactGcPolicy {
    static HNSW: HnswGcPolicy = HnswGcPolicy;
    static SPARSE: SparseGcPolicy = SparseGcPolicy;
    static FULLTEXT: FullTextGcPolicy = FullTextGcPolicy;
    match kind {
        SearchIndexKind::Hnsw => &HNSW,
        SearchIndexKind::Sparse => &SPARSE,
        SearchIndexKind::FullText => &FULLTEXT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::text_index::FullTextIndex;
    use crate::meta::{FileMetadataStore, GlobalSchemaMap, MetadataStore, TabletMetaManager};
    use crate::search::{ResourceBudget, SearchBatchConfig, SearchBatchState};
    use crate::table::table_factory::TableFactory;
    use crate::test_utils::*;
    use paro_common::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_table_with_root(
        root: &std::path::Path,
        types: &[LogicalType],
    ) -> crate::table::table_handle::TableHandle {
        TableFactory::new(Some(meta_manager(root)))
            .with_storage_root(root)
            .create_table(types)
            .expect("create table")
    }

    fn reopen_table_with_root(
        root: &std::path::Path,
        types: &[LogicalType],
        descriptor: &crate::table::storage_descriptor::TableStorageDescriptor,
    ) -> crate::table::table_handle::TableHandle {
        TableFactory::new(Some(meta_manager(root)))
            .with_storage_root(root)
            .open_from_descriptor(types, descriptor)
            .expect("open table")
    }

    fn meta_manager(root: &std::path::Path) -> Arc<TabletMetaManager> {
        let metadata_store: Arc<dyn MetadataStore> =
            Arc::new(FileMetadataStore::new(root.join("meta")).expect("meta store"));
        Arc::new(TabletMetaManager::new(
            metadata_store,
            Arc::new(GlobalSchemaMap::default()),
        ))
    }

    #[test]
    fn schema_seeded_hnsw_definition_is_registered() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        );

        assert!(table.search_registry().definition_count() >= 1);
        assert!(table.vector_capability(0).is_some());
    }

    #[test]
    fn explicit_fulltext_definition_publishes_manifest_and_survives_reload() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 42,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };

        table
            .register_search_definition(definition.clone())
            .expect("register fulltext definition");
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "graph vector",
            ])]))
            .unwrap();
        let capability = table
            .search_registry()
            .capability(
                SearchIndexKind::FullText,
                0,
                Some(definition.config_fingerprint),
            )
            .expect("fulltext capability");
        assert_eq!(capability.definition_id, 42);

        let reopened = reopen_table_with_root(
            root.path(),
            &[LogicalType::Varchar],
            &table.to_descriptor().expect("descriptor"),
        );
        reopened
            .register_search_definition(definition)
            .expect("reload definition");
        assert!(reopened.fulltext_capability(0, "simple").is_some());
    }

    #[test]
    fn fulltext_registry_refresh_appends_delta_for_new_rowsets() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 7,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "graph alpha",
            ])]))
            .unwrap();

        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "graph beta",
            ])]))
            .unwrap();
        let query = FullTextIndex::new_default().parse_query("graph").unwrap();
        let opened = table
            .open_fulltext_filter_cursor(0, &query, "simple", None, table.max_version())
            .unwrap();
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let mut chunks = Vec::new();
        let batch = SearchBatchConfig {
            row_limit: 1024,
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget {
            memory_limit_bytes: 64 * 1024 * 1024,
            heap_budget_items: 1024,
            parallelism_slots: 4,
            cpu_step_budget: None,
            context: None,
        };
        loop {
            match cursor.next_batch(&batch, &mut budget).unwrap() {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => chunks.push(
                    table
                        .materialize_search_batch(&snapshot, batch, &[0], false)
                        .unwrap(),
                ),
                SearchBatchState::Exhausted => break,
            }
        }
        assert_eq!(chunks.iter().map(|chunk| chunk.size()).sum::<usize>(), 2);
        assert!(table.fulltext_capability(0, "simple").is_some());
    }
}
