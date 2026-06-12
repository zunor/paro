// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use arc_swap::ArcSwap;
use paro_scheduler::scheduler::TaskScheduler;

use crate::metrics::storage_metrics;
use crate::rowset::{RowsetId, RowsetSharedPtr};
use crate::tablet::{ColumnId, RowsetPublishObserver, TabletId, TabletRef};
use paro_common::error::{self as paro_error, Result};

use super::artifact::{ArtifactGcContext, ArtifactLocation, GcDecision};
use super::capability::{
    CapabilityToken, SearchArtifactRef, SearchCapability, SearchDefinitionOrigin,
    SearchIndexDefinition, SearchIndexKind,
};
use super::cursor::{GenerationArtifactSet, GenerationReadSnapshot, OpenSearchCursorResult};
use super::definition::freshness::capability_needs_required_freshness_wait;
use super::definition::origin::{hnsw_schema_seed_definitions, restored_schema_seed_definition};
use super::definition::validation::validate_definition;
use super::generation::coverage::{search_generation_coverage_for_state, SearchGenerationCoverage};
use super::generation::head::{head_for_state, persist_head_for_state};
use super::generation::maintenance_state::build_maintenance_state;
use super::generation::snapshot::{
    collect_rowset_snapshot, collect_visible_snapshot, RowsetSearchSnapshot,
};
use super::generation::stats::{
    empty_generation_stats_for_definition, generation_stats_after_artifact_replacement,
    generation_stats_from_artifacts, stats_deltas_from_generation_stats,
};
use super::generation::tail_entries::{
    assign_tail_entry_ids, assign_tail_entry_ids_for_full_snapshot, tail_entry_already_live,
    tail_entry_is_covered_by_artifacts,
};
use super::generation::view::{
    coverage_for_definition, execution_modes_for_definition, generation_read_snapshot,
    indexed_through_ts, record_tail_metrics_for_state, SearchDefinitionState, SearchView,
};
use super::inline_sink::{
    BuildBudget, SearchAdmission, SearchInlineBuilderSet, SidecarArtifactBuilder,
};
use super::lifecycle::bootstrap::SearchBootstrapReport;
use super::lifecycle::gc::gc_policy_for_kind;
use super::lifecycle::maintenance_request::provider_maintenance_request_for_definition;
use super::lifecycle::publisher::{
    assign_generation_id, remove_sidecar_packages, replace_artifacts, retire_paths_for_manifest,
    search_artifact_key, sidecar_file_ids_for_artifacts,
};
use super::maintenance::{
    CatchUpPlanner, DefinitionMaintenanceReport, InlineSearchAdmission, MaintenanceScheduler,
    SearchMaintenanceAction, SearchMaintenanceReport,
};
use super::manifest::{
    GenerationManifestRoot, LoadedManifest, ManifestDelta, ManifestDeltaEntry, ManifestShard,
    ManifestStore,
};
use super::sidecar::SidecarArtifactStore;
use super::sidecar_builder::ProviderSidecarArtifactBuilder;
use super::stats::MaintenancePriority;
use super::tail::{TailEntryId, TailMutationKind, TailPendingSet};
use super::write_path::SearchWriteContext;

const REQUIRED_FRESHNESS_WAIT_SWEEPS: usize = 32;

#[derive(Debug)]
struct RetiredManifest {
    provider: SearchIndexKind,
    artifacts: Arc<GenerationArtifactSet>,
    paths: Vec<PathBuf>,
    retired_at: Instant,
}

pub(crate) struct SearchIndexRegistry {
    tablet: TabletRef,
    manifests: ManifestStore,
    view: ArcSwap<SearchView>,
    publish_locks: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
    retired: Mutex<Vec<RetiredManifest>>,
    maintenance_scheduler: Arc<MaintenanceScheduler>,
    hnsw_task_scheduler: RwLock<Option<Arc<TaskScheduler>>>,
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

impl RowsetPublishObserver for SearchIndexRegistry {
    fn prepare_rowset_publish(
        &self,
        tablet_id: TabletId,
        version: i64,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<Vec<crate::tablet::SearchGenerationHeadMeta>> {
        if tablet_id != self.tablet.tablet_id() {
            return Ok(Vec::new());
        }
        self.prepare_heads_for_visible_rowsets(version, visible_rowsets)
    }

    fn rowset_published(&self, tablet_id: TabletId, version: i64, rowset: RowsetSharedPtr) {
        if tablet_id != self.tablet.tablet_id() {
            return;
        }
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
                    tablet_id,
                    definition_id,
                    rowset_id = rowset.rowset_id(),
                    version,
                    error = %err,
                    "search registry eager refresh after rowset publish failed"
                );
            }
        }
    }

    fn search_inline_builders_for_compaction(&self, tablet_id: TabletId) -> SearchInlineBuilderSet {
        if tablet_id != self.tablet.tablet_id() {
            return SearchInlineBuilderSet::default();
        }
        match self.write_context() {
            Ok(context) => context.inline_builders,
            Err(err) => {
                tracing::warn!(
                    tablet_id,
                    error = %err,
                    "failed to build search inline builders for compaction"
                );
                SearchInlineBuilderSet::default()
            }
        }
    }
}

impl SearchIndexRegistry {
    pub(crate) fn new(tablet: TabletRef) -> Self {
        let registry = Self {
            manifests: ManifestStore::new(tablet.data_dir().to_path_buf()),
            tablet,
            view: ArcSwap::from_pointee(SearchView::default()),
            publish_locks: Mutex::new(HashMap::new()),
            retired: Mutex::new(Vec::new()),
            maintenance_scheduler: Arc::new(MaintenanceScheduler::default()),
            hnsw_task_scheduler: RwLock::new(None),
        };
        if let Err(err) = registry.manifests.sweep_orphan_staging_fragments() {
            tracing::warn!(
                tablet_id = registry.tablet.tablet_id(),
                error = %err,
                "failed to sweep orphan search manifest staging fragments"
            );
        }
        registry.seed_schema_hnsw_definitions();
        registry
    }

    pub(crate) fn bind_task_scheduler(&self, scheduler: Option<Arc<TaskScheduler>>) {
        *self.hnsw_task_scheduler.write().unwrap() = scheduler;
    }

    fn hnsw_task_scheduler(&self) -> Option<Arc<TaskScheduler>> {
        self.hnsw_task_scheduler.read().unwrap().clone()
    }

    fn load_manifest_for_definition(&self, definition_id: u64) -> Result<Option<LoadedManifest>> {
        let Some(head) = self.tablet.search_generation_head(definition_id) else {
            return Ok(None);
        };
        self.manifests.load_manifest_for_head(&head)
    }

    pub(crate) fn install_definition(&self, definition: SearchIndexDefinition) -> Result<()> {
        self.update_definition(
            definition.clone(),
            SearchDefinitionOrigin::catalog(definition.definition_id),
        )
    }

    pub(crate) fn drop_definition(&self, definition_id: u64) -> Result<()> {
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(());
        };

        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.remove(&definition_id);
        self.retire_manifest(state.definition.kind, state.manifest.as_ref());
        self.manifests
            .remove_paths(&self.manifests.definition_paths(definition_id));
        self.tablet.remove_search_generation_head(definition_id)?;

        let restored_seed_definition_id =
            if state.origin.is_catalog_index() && state.definition.kind == SearchIndexKind::Hnsw {
                self.restore_schema_seed_if_needed(&mut next, &state.definition)?
            } else {
                None
            };

        self.view.store(Arc::new(next));
        if let Some(seed_definition_id) = restored_seed_definition_id {
            self.refresh_definition(seed_definition_id)?;
        }
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
        self.resolve_capability_with_required_wait(|view| {
            view.capability(kind, column_id, config_fingerprint)
        })
    }

    pub(crate) fn fulltext_capability(
        &self,
        column_id: ColumnId,
        config: &str,
    ) -> Option<SearchCapability> {
        self.resolve_capability_with_required_wait(|view| {
            view.fulltext_capability(column_id, config)
        })
    }

    fn resolve_capability_with_required_wait(
        &self,
        finder: impl Fn(&SearchView) -> Option<SearchCapability>,
    ) -> Option<SearchCapability> {
        self.ensure_fresh();
        let capability = {
            let view = self.view.load();
            finder(&view)
        };
        let Some(definition_id) = capability
            .as_ref()
            .filter(|capability| capability_needs_required_freshness_wait(capability))
            .map(|capability| capability.definition_id)
        else {
            return capability;
        };
        if let Err(err) = self.wait_for_required_freshness(definition_id) {
            tracing::warn!(
                tablet_id = self.tablet.tablet_id(),
                definition_id,
                error = %err,
                "required search freshness wait failed"
            );
        }
        let view = self.view.load();
        finder(&view)
    }

    fn wait_for_required_freshness(&self, definition_id: u64) -> Result<()> {
        for _ in 0..REQUIRED_FRESHNESS_WAIT_SWEEPS {
            if !self.definition_needs_required_freshness_wait(definition_id) {
                return Ok(());
            }
            let report = self.maintenance_sweep()?;
            if report.catch_up_rowsets == 0 && report.definitions_updated == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    fn definition_needs_required_freshness_wait(&self, definition_id: u64) -> bool {
        self.view
            .load()
            .definitions
            .get(&definition_id)
            .and_then(|state| state.capability.as_ref())
            .is_some_and(capability_needs_required_freshness_wait)
    }

    pub(crate) fn write_context(&self) -> Result<SearchWriteContext> {
        let admission: Arc<dyn SearchAdmission> = Arc::new(InlineSearchAdmission::with_scheduler(
            Arc::clone(&self.maintenance_scheduler),
        ));
        self.view.load().write_context(Some(admission))
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
        Ok(generation_read_snapshot(definition_id, state))
    }

    pub(crate) fn open_generation_snapshot_with_token(
        &self,
        token: &CapabilityToken,
    ) -> Result<OpenSearchCursorResult<GenerationReadSnapshot>> {
        self.ensure_fresh();
        let current = self.view.load();
        let Some(state) = current.definitions.get(&token.definition_id) else {
            return Ok(OpenSearchCursorResult::NotQueryable);
        };
        let Some(generation) = &state.generation else {
            return Ok(OpenSearchCursorResult::NotQueryable);
        };
        if token.is_generation_stale(generation.generation_id) {
            return Ok(OpenSearchCursorResult::CapabilityTokenStale);
        }
        if !token.is_queryable()
            || !state
                .capability
                .as_ref()
                .is_some_and(SearchCapability::is_queryable)
        {
            return Ok(OpenSearchCursorResult::NotQueryable);
        }
        generation_read_snapshot(token.definition_id, state)
            .map(OpenSearchCursorResult::Opened)
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "search generation {} disappeared after token validation",
                    token.definition_id
                ))
            })
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
        Ok(search_generation_coverage_for_state(state))
    }

    pub(crate) fn catch_up_definition(&self, definition_id: u64) -> Result<usize> {
        self.ensure_fresh();
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(0);
        };
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(0);
        };
        if state.definition.kind == SearchIndexKind::Hnsw {
            return self.catch_up_hnsw_definition_locked(current, state);
        }
        if !matches!(
            state.definition.kind,
            SearchIndexKind::FullText | SearchIndexKind::Sparse
        ) {
            return Ok(0);
        }

        let visible_rowsets = self
            .tablet
            .capture_consistent_rowsets(self.tablet.max_version())?;
        let visible_by_id = visible_rowsets
            .into_iter()
            .map(|rowset| (rowset.rowset_id(), rowset))
            .collect::<BTreeMap<_, _>>();

        let catch_up_plan = CatchUpPlanner.plan(&state.definition, manifest, &visible_by_id)?;
        let touched = catch_up_plan.len();
        if touched == 0 {
            return Ok(0);
        }

        let sidecar_store = SidecarArtifactStore::new(self.tablet.data_dir().clone());
        let builder = ProviderSidecarArtifactBuilder::new(sidecar_store.clone());
        let input = super::inline_sink::SidecarBuildInput {
            definition: state.definition.clone(),
            generation_id: manifest.root.generation_id,
            tail_window: manifest.tail_pending_entries.clone(),
            rowset_refs: catch_up_plan
                .items
                .iter()
                .map(|item| item.rowset.clone())
                .collect(),
            snapshot_version: self.tablet.max_version(),
        };
        let estimate = builder.estimate_cost(&input);
        let result = builder.build(
            input,
            &BuildBudget {
                cost_envelope: estimate.cost,
                deadline: None,
                grant_id: None,
            },
        )?;
        if result.artifact_refs.is_empty() {
            return Ok(0);
        }
        let touched = result
            .artifact_refs
            .iter()
            .map(|artifact| artifact.segment.rowset_id)
            .collect::<BTreeSet<_>>()
            .len();
        let sidecar_file_ids = sidecar_file_ids_for_artifacts(&result.artifact_refs);
        let next_state = match self.publish_sidecar_catch_up_delta(&state, result.artifact_refs) {
            Ok(next_state) => next_state,
            Err(err) => {
                remove_sidecar_packages(&sidecar_store, &sidecar_file_ids);
                return Err(err);
            }
        };
        persist_head_for_state(&self.tablet, &self.manifests, &next_state)?;
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.insert(definition_id, next_state.clone());
        self.view.store(Arc::new(next));
        self.sweep_retired();
        record_tail_metrics_for_state(&next_state);
        Ok(touched)
    }

    fn catch_up_hnsw_definition_locked(
        &self,
        current: Arc<SearchView>,
        state: SearchDefinitionState,
    ) -> Result<usize> {
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(0);
        };
        if self.hnsw_task_scheduler().is_none() {
            tracing::debug!(
                tablet_id = self.tablet.tablet_id(),
                definition_id = state.definition.definition_id,
                "HNSW maintenance request admitted but no task scheduler is bound"
            );
            return Ok(0);
        };

        let visible_rowsets = self
            .tablet
            .capture_consistent_rowsets(self.tablet.max_version())?;
        let visible_by_id = visible_rowsets
            .iter()
            .map(|rowset| (rowset.rowset_id(), rowset.clone()))
            .collect::<BTreeMap<_, _>>();
        let catch_up_plan = CatchUpPlanner.plan(&state.definition, manifest, &visible_by_id)?;
        if catch_up_plan.items.is_empty() {
            return Ok(0);
        }

        let sidecar_store = SidecarArtifactStore::new(self.tablet.data_dir().clone());
        let builder = ProviderSidecarArtifactBuilder::new(sidecar_store.clone());
        let input = super::inline_sink::SidecarBuildInput {
            definition: state.definition.clone(),
            generation_id: manifest.root.generation_id,
            tail_window: manifest.tail_pending_entries.clone(),
            rowset_refs: catch_up_plan
                .items
                .iter()
                .map(|item| item.rowset.clone())
                .collect(),
            snapshot_version: self.tablet.max_version(),
        };
        let estimate = builder.estimate_cost(&input);
        let result = builder.build(
            input,
            &BuildBudget {
                cost_envelope: estimate.cost,
                deadline: None,
                grant_id: None,
            },
        )?;
        if result.artifact_refs.is_empty() {
            return Ok(0);
        }
        let touched = result
            .artifact_refs
            .iter()
            .map(|artifact| artifact.segment.rowset_id)
            .collect::<BTreeSet<_>>()
            .len();
        let sidecar_file_ids = sidecar_file_ids_for_artifacts(&result.artifact_refs);
        let next_state = match self.publish_sidecar_catch_up_delta(&state, result.artifact_refs) {
            Ok(next_state) => next_state,
            Err(err) => {
                remove_sidecar_packages(&sidecar_store, &sidecar_file_ids);
                return Err(err);
            }
        };
        persist_head_for_state(&self.tablet, &self.manifests, &next_state)?;
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions
            .insert(state.definition.definition_id, next_state.clone());
        self.view.store(Arc::new(next));
        self.sweep_retired();
        record_tail_metrics_for_state(&next_state);
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

        let mut planned = Vec::new();
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
            let delta_window_bytes = manifest
                .root
                .delta_window_bytes(&self.manifests.definition_dir(definition_id));
            let decision = self.maintenance_scheduler.plan_definition(
                &state.definition,
                manifest,
                gc_decision,
                &gc_context,
                delta_window_bytes,
            );
            let provider_request = provider_maintenance_request_for_definition(
                &state.definition,
                manifest,
                &self.tablet,
            );
            let request = self.maintenance_scheduler.admission_request(
                &state.definition,
                manifest,
                &decision,
            );
            planned.push((
                definition_id,
                manifest.root.maintenance_state.recovery.clone(),
                decision,
                provider_request,
                request,
            ));
        }

        let requests = planned
            .iter()
            .map(|(_, _, _, _, request)| request.clone())
            .collect::<Vec<_>>();
        let admissions = self.maintenance_scheduler.schedule_requests(&requests);
        for ((definition_id, recovery, mut decision, provider_request, _), admission) in
            planned.into_iter().zip(admissions)
        {
            decision.admission = admission;
            if decision.manifest_delta_compaction_requested && decision.admission.is_admitted() {
                report.manifest_delta_compaction_requested = true;
            }
            if decision.sidecar_repack_requested && decision.admission.is_admitted() {
                report.sidecar_repack_requested = true;
            }
            if !matches!(decision.gc_decision, GcDecision::Skip) && decision.admission.is_admitted()
            {
                report.compaction_requested = true;
            }
            report.definitions.push(DefinitionMaintenanceReport {
                definition_id,
                action: decision.action,
                provider_request,
                admission: decision.admission,
                gc_decision: decision.gc_decision,
                estimate: decision.estimate,
                manifest_delta_compaction_requested: decision.manifest_delta_compaction_requested,
                sidecar_repack_requested: decision.sidecar_repack_requested,
                tail_pending_rowsets: recovery.tail_pending_rowsets,
                tail_pending_rows: recovery.tail_pending_rows,
                priority: recovery.priority,
                backlog_tier: recovery.backlog_tier,
            });
        }

        while let Some(task) = self.maintenance_scheduler.pop_next_task() {
            let _grant_lease = self.maintenance_scheduler.scoped_task_lease(&task);
            match task.request.action {
                SearchMaintenanceAction::CatchUp => {
                    let touched = self.catch_up_definition(task.request.definition_id)?;
                    if touched > 0 {
                        report.definitions_updated += 1;
                        report.catch_up_rowsets = report.catch_up_rowsets.saturating_add(touched);
                    }
                }
                SearchMaintenanceAction::CompactManifestDelta => {
                    if self.compact_manifest_deltas_for_definition(task.request.definition_id)? {
                        report.definitions_updated += 1;
                    }
                }
                SearchMaintenanceAction::RepackSidecar => {
                    let repacked =
                        self.repack_sidecars_for_definition(task.request.definition_id)?;
                    if repacked > 0 {
                        report.definitions_updated += 1;
                    }
                }
                SearchMaintenanceAction::Compact | SearchMaintenanceAction::Rebuild => {
                    report.compaction_requested = true;
                }
                SearchMaintenanceAction::Skip => {}
            }
        }

        Ok(report)
    }

    pub(crate) fn repack_sidecars_for_definition(&self, definition_id: u64) -> Result<usize> {
        self.ensure_fresh();
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(0);
        };
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(0);
        };
        if !super::maintenance::sidecar_repack_needed(manifest) {
            return Ok(0);
        }
        let generation = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("sidecar repack requires generation"))?;

        let store = SidecarArtifactStore::new(self.tablet.data_dir().clone());
        let mut writer =
            store.create_next_package_writer(definition_id, generation.generation_id)?;
        let started_at = Instant::now();
        let mut repacked_artifacts = Vec::new();
        let mut rows = 0u64;
        let mut read_bytes = 0u64;
        let mut artifact_bytes = 0u64;

        for artifact in &manifest.artifacts.artifacts {
            if !matches!(
                artifact.location,
                ArtifactLocation::SidecarArtifactFile { .. }
            ) {
                continue;
            }
            let bytes = store.read_artifact(&artifact.location)?;
            read_bytes = read_bytes.saturating_add(bytes.len() as u64);
            rows = rows.saturating_add(artifact.stats.row_count);
            let mut repacked = artifact.clone();
            repacked.location = writer.append_artifact(&bytes)?;
            repacked.stats.bytes_on_disk = bytes.len() as u64;
            artifact_bytes = artifact_bytes.saturating_add(repacked.stats.bytes_on_disk);
            repacked_artifacts.push(repacked);
        }

        if repacked_artifacts.is_empty() {
            writer.abort();
            return Ok(0);
        }

        let bytes_written = writer.bytes_written();
        writer.finalize()?;
        storage_metrics().record_search_sidecar_build(
            crate::metrics::SearchSidecarBuildMetricKey {
                definition_id,
                provider: state.definition.kind,
            },
            rows,
            read_bytes,
            bytes_written,
            artifact_bytes,
            elapsed_micros_since(started_at),
        );

        let repacked_count = repacked_artifacts.len();
        let sidecar_file_ids = sidecar_file_ids_for_artifacts(&repacked_artifacts);
        let next_state = match self.publish_sidecar_repack_delta(&state, repacked_artifacts) {
            Ok(next_state) => next_state,
            Err(err) => {
                remove_sidecar_packages(&store, &sidecar_file_ids);
                return Err(err);
            }
        };
        persist_head_for_state(&self.tablet, &self.manifests, &next_state)?;
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.insert(definition_id, next_state.clone());
        self.view.store(Arc::new(next));
        self.sweep_retired();
        record_tail_metrics_for_state(&next_state);
        Ok(repacked_count)
    }

    fn compact_manifest_deltas_for_definition(&self, definition_id: u64) -> Result<bool> {
        self.ensure_fresh();
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(false);
        };
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(false);
        };
        let mut root = manifest.root.clone();
        if !self
            .manifests
            .maybe_compact_deltas(definition_id, &mut root)?
        {
            return Ok(false);
        }
        let head = self.manifests.head_for_root(&root);
        let Some(loaded) = self.manifests.load_manifest_for_head(&head)? else {
            return Ok(false);
        };
        let next_state = state.with_manifest(loaded);
        persist_head_for_state(&self.tablet, &self.manifests, &next_state)?;
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.insert(definition_id, next_state.clone());
        self.view.store(Arc::new(next));
        record_tail_metrics_for_state(&next_state);
        Ok(true)
    }

    pub(crate) fn compact_manifest_deltas(&self) -> Result<usize> {
        let definition_ids = self
            .view
            .load()
            .definitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut compacted = 0usize;
        for definition_id in definition_ids {
            if self.compact_manifest_deltas_for_definition(definition_id)? {
                compacted += 1;
            }
        }
        Ok(compacted)
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
            .filter(|state| state.origin.is_catalog_index())
            .count()
    }

    pub(crate) fn refresh_definition(
        &self,
        definition_id: u64,
    ) -> Result<Option<SearchCapability>> {
        self.refresh_definition_inner(definition_id, false)
    }

    pub(crate) fn refresh_after_rowset_replacement(&self) -> Result<()> {
        let definition_ids = self
            .view
            .load()
            .definitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for definition_id in definition_ids {
            self.refresh_definition_inner(definition_id, true)?;
        }
        Ok(())
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

        let visible_rowsets = self.tablet.capture_consistent_rowsets(visible_version)?;
        let next_state = self.refresh_state_from_snapshot(
            &state,
            visible_version,
            &visible_rowsets,
            force,
            true,
        )?;
        persist_head_for_state(&self.tablet, &self.manifests, &next_state)?;
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);
        next.definitions.insert(definition_id, next_state.clone());
        self.view.store(Arc::new(next));
        self.sweep_retired();
        Ok(next_state.capability)
    }

    fn prepare_heads_for_visible_rowsets(
        &self,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<Vec<crate::tablet::SearchGenerationHeadMeta>> {
        let definition_ids = self
            .view
            .load()
            .definitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut heads = Vec::new();
        for definition_id in definition_ids {
            let definition_lock = self.definition_lock(definition_id);
            let _guard = definition_lock
                .lock()
                .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

            let current = self.view.load_full();
            let Some(state) = current.definitions.get(&definition_id).cloned() else {
                continue;
            };
            let next_state = self.refresh_state_from_snapshot(
                &state,
                visible_version,
                visible_rowsets,
                false,
                false,
            )?;
            if let Some(head) = head_for_state(&self.manifests, &next_state) {
                heads.push(head);
            }
        }
        Ok(heads)
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
        validate_definition(&definition, &self.tablet)?;
        let definition_lock = self.definition_lock(definition.definition_id);
        let guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition update"))?;

        let current = self.view.load_full();
        let mut next = (*current).clone();
        next.version = next.version.saturating_add(1);

        if origin.is_catalog_index() {
            let duplicate_seed_ids = next
                .definitions
                .iter()
                .filter_map(|(definition_id, state)| {
                    if state
                        .definition
                        .column_ids
                        .first()
                        .is_some_and(|column_id| state.origin.is_schema_seed_for(*column_id))
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
                    self.retire_manifest(seed_state.definition.kind, seed_state.manifest.as_ref());
                    self.manifests
                        .remove_paths(&self.manifests.definition_paths(duplicate_seed_id));
                }
            }
        }

        let mut state = SearchDefinitionState::new(definition.clone(), origin);
        if let Some(loaded) = self.load_manifest_for_definition(definition.definition_id)? {
            if loaded.root.config_fingerprint == definition.config_fingerprint {
                state = state.with_manifest(loaded);
                record_tail_metrics_for_state(&state);
            }
        }
        next.definitions.insert(definition.definition_id, state);
        self.view.store(Arc::new(next));
        drop(guard);
        let _ = self.refresh_definition(definition.definition_id);
        Ok(())
    }

    fn refresh_state_from_snapshot(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
        force: bool,
        retire_old_manifest: bool,
    ) -> Result<SearchDefinitionState> {
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
            let new_rowsets = visible_rowset_ids
                .difference(&known_rowset_ids)
                .copied()
                .collect::<Vec<_>>();
            if !removed_rowsets.is_empty() {
                return self.publish_delta_for_replaced_rowsets(
                    state,
                    visible_version,
                    visible_rowsets,
                    &removed_rowsets,
                    &new_rowsets,
                    retire_old_manifest,
                );
            }
            if removed_rowsets.is_empty() {
                if !new_rowsets.is_empty() {
                    return self.publish_delta_for_new_rowsets(
                        state,
                        visible_version,
                        visible_rowsets,
                        &new_rowsets,
                        retire_old_manifest,
                    );
                }
                if force {
                    if let Some(next_state) = self.publish_delta_for_covered_tail_entries(
                        state,
                        visible_version,
                        visible_rowsets,
                        retire_old_manifest,
                    )? {
                        return Ok(next_state);
                    }
                    if manifest.root.build_snapshot_version == visible_version {
                        return Ok(state.clone());
                    }
                }
                if !force && manifest.root.build_snapshot_version == visible_version {
                    return Ok(state.clone());
                }
            }
        }

        self.publish_full_snapshot(state, visible_version, visible_rowsets, retire_old_manifest)
    }

    fn publish_full_snapshot(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
        retire_old_manifest: bool,
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let mut snapshot =
            collect_visible_snapshot(&state.definition, visible_version, visible_rowsets)?;

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
        let next_tail_entry_id = assign_tail_entry_ids_for_full_snapshot(
            &mut snapshot.tail_pending.entries,
            state.manifest.as_ref(),
        );
        let mut root = GenerationManifestRoot {
            definition_id,
            generation_id,
            build_epoch,
            build_snapshot_version: snapshot.visible_version,
            indexed_through_ts: indexed_through_ts(snapshot.visible_version),
            config_fingerprint: state.definition.config_fingerprint,
            coverage: snapshot.coverage.clone(),
            generation_stats: snapshot.generation_stats.clone(),
            next_tail_entry_id,
            execution_modes: snapshot.execution_modes.clone(),
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
            materialized_state_file: None,
        };
        let shard_name = self.manifests.write_shard(
            definition_id,
            generation_id,
            root.root_version,
            &ManifestShard {
                artifact_refs: assign_generation_id(snapshot.artifacts.clone(), generation_id),
                tail_pending_entries: snapshot.tail_pending.entries.clone(),
            },
        )?;
        let shard_path = self
            .manifests
            .definition_dir(definition_id)
            .join(&shard_name.file_name);
        root.shard_files.push(shard_name);
        root.recompute_checksum()?;
        let root_path = match self.manifests.write_root(definition_id, &root) {
            Ok(root_path) => root_path,
            Err(err) => {
                self.manifests.remove_paths(&[shard_path]);
                return Err(err);
            }
        };
        let loaded = LoadedManifest {
            root: root.clone(),
            root_path,
            shard_paths: root
                .shard_files
                .iter()
                .map(|file| {
                    self.manifests
                        .definition_dir(definition_id)
                        .join(&file.file_name)
                })
                .collect(),
            delta_paths: Vec::new(),
            materialized_state_path: None,
            embedded_materialized_state: false,
            artifacts: GenerationArtifactSet {
                artifacts: assign_generation_id(snapshot.artifacts, generation_id),
            },
            tail_pending_entries: snapshot.tail_pending.entries,
        };

        let mut next_state = state.clone();
        if retire_old_manifest {
            self.retire_manifest_replaced_by(
                state.definition.kind,
                state.manifest.as_ref(),
                &loaded,
            );
        }
        next_state = next_state.with_manifest(loaded);
        storage_metrics().record_search_manifest_publish(
            self.manifests.codec_label(),
            elapsed_micros_since(started_at),
        );
        storage_metrics().set_search_manifest_delta_count(
            self.manifests.codec_label(),
            next_state.manifest_delta_count(),
        );
        record_tail_metrics_for_state(&next_state);
        Ok(next_state)
    }

    fn publish_delta_for_new_rowsets(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
        new_rowset_ids: &[RowsetId],
        retire_old_manifest: bool,
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let Some(current_manifest) = state.manifest.as_ref() else {
            return self.publish_full_snapshot(
                state,
                visible_version,
                visible_rowsets,
                retire_old_manifest,
            );
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
                collect_rowset_snapshot(&state.definition, rowset, visible_version)?;
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
        root.build_snapshot_version = visible_version;
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);
        let mut next_tail_entry_id = root.next_tail_entry_id.0;
        assign_tail_entry_ids(&mut added_tail_entries, &mut next_tail_entry_id);
        root.next_tail_entry_id = TailEntryId(next_tail_entry_id);

        let mut tail_pending_entries = current_manifest.tail_pending_entries.clone();
        tail_pending_entries.extend(added_tail_entries.iter().cloned());
        root.generation_stats.merge_assign(&delta_generation_stats);
        let tail_pending = TailPendingSet {
            entries: tail_pending_entries.clone(),
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
            &ManifestDelta::publish_changes(
                added_artifacts.clone(),
                added_tail_entries,
                stats_deltas_from_generation_stats(&delta_generation_stats),
            ),
        )?;
        let delta_path = self
            .manifests
            .definition_dir(definition_id)
            .join(&delta_name.file_name);
        root.recent_delta_files.push(delta_name);
        root.recompute_checksum()?;
        if let Err(err) = self
            .manifests
            .maybe_compact_deltas(definition_id, &mut root)
        {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }
        if let Err(err) = self.manifests.write_root(definition_id, &root) {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }
        let mut artifacts = current_manifest.artifacts.clone();
        artifacts.artifacts.extend(added_artifacts);
        let loaded = self.manifests.materialize_loaded_manifest(
            definition_id,
            root,
            artifacts,
            tail_pending_entries,
        );
        let mut next_state = state.clone();
        if retire_old_manifest {
            self.retire_manifest_replaced_by(
                state.definition.kind,
                state.manifest.as_ref(),
                &loaded,
            );
        }
        next_state = next_state.with_manifest(loaded);
        storage_metrics().record_search_manifest_publish(
            self.manifests.codec_label(),
            elapsed_micros_since(started_at),
        );
        storage_metrics().set_search_manifest_delta_count(
            self.manifests.codec_label(),
            next_state.manifest_delta_count(),
        );
        record_tail_metrics_for_state(&next_state);
        Ok(next_state)
    }

    fn publish_delta_for_replaced_rowsets(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
        removed_rowset_ids: &[RowsetId],
        new_rowset_ids: &[RowsetId],
        retire_old_manifest: bool,
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let Some(current_manifest) = state.manifest.as_ref() else {
            return self.publish_full_snapshot(
                state,
                visible_version,
                visible_rowsets,
                retire_old_manifest,
            );
        };
        let generation = state.generation.as_ref().ok_or_else(|| {
            paro_error::internal("rowset replacement publish requires existing generation")
        })?;
        let removed_rowset_ids = removed_rowset_ids.iter().copied().collect::<BTreeSet<_>>();
        let new_rowset_ids = new_rowset_ids.iter().copied().collect::<BTreeSet<_>>();

        let removed_artifacts = current_manifest
            .artifacts
            .artifacts
            .iter()
            .filter(|artifact| removed_rowset_ids.contains(&artifact.segment.rowset_id))
            .cloned()
            .collect::<Vec<_>>();
        let removed_segments = removed_artifacts
            .iter()
            .map(|artifact| artifact.segment)
            .collect::<BTreeSet<_>>();
        let covered_tail_ids = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| removed_rowset_ids.contains(&entry.rowset_id))
            .map(|entry| entry.entry_id)
            .collect::<BTreeSet<_>>();

        let mut added_artifacts = Vec::new();
        let mut added_tail_entries = Vec::new();
        for rowset in visible_rowsets {
            if !new_rowset_ids.contains(&rowset.rowset_id()) {
                continue;
            }
            rowset.load()?;
            let rowset_snapshot =
                collect_rowset_snapshot(&state.definition, rowset, visible_version)?;
            added_artifacts.extend(rowset_snapshot.artifacts);
            added_tail_entries.extend(rowset_snapshot.tail_entries.entries);
        }

        let mut root = current_manifest.root.clone();
        root.root_version = root.root_version.saturating_add(1);
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = visible_version;
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let mut next_tail_entry_id = root.next_tail_entry_id.0;
        assign_tail_entry_ids(&mut added_tail_entries, &mut next_tail_entry_id);
        root.next_tail_entry_id = TailEntryId(next_tail_entry_id);

        added_artifacts = assign_generation_id(added_artifacts, generation.generation_id);
        let covered_tail_ids = covered_tail_ids.into_iter().collect::<BTreeSet<_>>();
        let mut tail_pending_entries = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| !covered_tail_ids.contains(&entry.entry_id))
            .cloned()
            .collect::<Vec<_>>();
        tail_pending_entries.extend(added_tail_entries.iter().cloned());

        let mut artifacts = GenerationArtifactSet {
            artifacts: current_manifest
                .artifacts
                .artifacts
                .iter()
                .filter(|artifact| !removed_rowset_ids.contains(&artifact.segment.rowset_id))
                .cloned()
                .collect(),
        };
        artifacts.artifacts.extend(added_artifacts.iter().cloned());
        root.generation_stats = generation_stats_after_artifact_replacement(
            &state.definition,
            &current_manifest.root.generation_stats,
            &removed_artifacts,
            &added_artifacts,
            &artifacts.artifacts,
        )?;
        let tail_pending = TailPendingSet {
            entries: tail_pending_entries.clone(),
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

        let mut delta_entries = Vec::new();
        delta_entries.extend(
            removed_segments
                .iter()
                .copied()
                .map(ManifestDeltaEntry::RemoveArtifact),
        );
        delta_entries.extend(
            added_artifacts
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::AddArtifact),
        );
        delta_entries.extend(
            covered_tail_ids
                .iter()
                .copied()
                .map(ManifestDeltaEntry::CoverTail),
        );
        delta_entries.extend(
            added_tail_entries
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::UpsertTail),
        );

        let definition_id = state.definition.definition_id;
        let delta_name = self.manifests.write_delta(
            definition_id,
            generation.generation_id,
            root.root_version,
            root.recent_delta_files.len(),
            &ManifestDelta::new(delta_entries),
        )?;
        let delta_path = self
            .manifests
            .definition_dir(definition_id)
            .join(&delta_name.file_name);
        root.recent_delta_files.push(delta_name);
        root.recompute_checksum()?;
        if let Err(err) = self
            .manifests
            .maybe_compact_deltas(definition_id, &mut root)
        {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }
        if let Err(err) = self.manifests.write_root(definition_id, &root) {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }

        let loaded = self.manifests.materialize_loaded_manifest(
            definition_id,
            root,
            artifacts,
            tail_pending_entries,
        );
        let mut next_state = state.clone();
        if retire_old_manifest {
            self.retire_manifest_replaced_by(
                state.definition.kind,
                state.manifest.as_ref(),
                &loaded,
            );
        }
        next_state = next_state.with_manifest(loaded);
        storage_metrics().record_search_manifest_publish(
            self.manifests.codec_label(),
            elapsed_micros_since(started_at),
        );
        storage_metrics().set_search_manifest_delta_count(
            self.manifests.codec_label(),
            next_state.manifest_delta_count(),
        );
        record_tail_metrics_for_state(&next_state);
        Ok(next_state)
    }

    fn publish_sidecar_catch_up_delta(
        &self,
        state: &SearchDefinitionState,
        mut added_artifacts: Vec<SearchArtifactRef>,
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let Some(current_manifest) = state.manifest.as_ref() else {
            return Err(paro_error::internal(
                "sidecar catch-up publish requires existing manifest",
            ));
        };
        let generation = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("sidecar catch-up publish requires generation"))?;
        let definition_id = state.definition.definition_id;

        added_artifacts = assign_generation_id(added_artifacts, generation.generation_id);
        let current_artifact_keys = current_manifest
            .artifacts
            .artifacts
            .iter()
            .map(search_artifact_key)
            .collect::<BTreeSet<_>>();
        let added_artifact_keys = added_artifacts
            .iter()
            .map(search_artifact_key)
            .collect::<BTreeSet<_>>();
        let covered_tail_ids = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| {
                !matches!(entry.mutation, TailMutationKind::Delete)
                    && tail_entry_is_covered_by_artifacts(
                        &state.definition,
                        entry,
                        &current_artifact_keys,
                        &added_artifact_keys,
                    )
            })
            .map(|entry| entry.entry_id)
            .collect::<BTreeSet<_>>();

        let mut root = current_manifest.root.clone();
        root.root_version = root.root_version.saturating_add(1);
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = self.tablet.max_version();
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let mut tail_pending_entries = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| !covered_tail_ids.contains(&entry.entry_id))
            .cloned()
            .collect::<Vec<_>>();
        let delta_generation_stats =
            generation_stats_from_artifacts(&state.definition, &added_artifacts);
        root.generation_stats.merge_assign(&delta_generation_stats);
        let tail_pending = TailPendingSet {
            entries: tail_pending_entries.clone(),
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

        let mut delta_entries = Vec::new();
        delta_entries.extend(
            added_artifacts
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::AddArtifact),
        );
        delta_entries.extend(
            covered_tail_ids
                .iter()
                .copied()
                .map(ManifestDeltaEntry::CoverTail),
        );
        delta_entries.extend(
            stats_deltas_from_generation_stats(&delta_generation_stats)
                .into_iter()
                .map(ManifestDeltaEntry::StatsDelta),
        );
        let delta_name = self.manifests.write_delta(
            definition_id,
            generation.generation_id,
            root.root_version,
            root.recent_delta_files.len(),
            &ManifestDelta::new(delta_entries),
        )?;
        let delta_path = self
            .manifests
            .definition_dir(definition_id)
            .join(&delta_name.file_name);
        root.recent_delta_files.push(delta_name);
        root.recompute_checksum()?;
        if let Err(err) = self
            .manifests
            .maybe_compact_deltas(definition_id, &mut root)
        {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }
        if let Err(err) = self.manifests.write_root(definition_id, &root) {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }

        let mut artifacts = current_manifest.artifacts.clone();
        artifacts.artifacts.extend(added_artifacts);
        let loaded = self.manifests.materialize_loaded_manifest(
            definition_id,
            root,
            artifacts,
            std::mem::take(&mut tail_pending_entries),
        );
        let mut next_state = state.clone();
        self.retire_manifest_replaced_by(state.definition.kind, state.manifest.as_ref(), &loaded);
        next_state = next_state.with_manifest(loaded);
        storage_metrics().record_search_manifest_publish(
            self.manifests.codec_label(),
            elapsed_micros_since(started_at),
        );
        storage_metrics().set_search_manifest_delta_count(
            self.manifests.codec_label(),
            next_state.manifest_delta_count(),
        );
        Ok(next_state)
    }

    fn publish_sidecar_repack_delta(
        &self,
        state: &SearchDefinitionState,
        repacked_artifacts: Vec<SearchArtifactRef>,
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let Some(current_manifest) = state.manifest.as_ref() else {
            return Err(paro_error::internal(
                "sidecar repack publish requires existing manifest",
            ));
        };
        let generation = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("sidecar repack publish requires generation"))?;
        let definition_id = state.definition.definition_id;

        let mut root = current_manifest.root.clone();
        root.root_version = root.root_version.saturating_add(1);
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = self.tablet.max_version();
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let repacked_artifacts = assign_generation_id(repacked_artifacts, generation.generation_id);
        let delta_name = self.manifests.write_delta(
            definition_id,
            generation.generation_id,
            root.root_version,
            root.recent_delta_files.len(),
            &ManifestDelta::new(
                repacked_artifacts
                    .iter()
                    .cloned()
                    .map(ManifestDeltaEntry::AddArtifact)
                    .collect(),
            ),
        )?;
        let delta_path = self
            .manifests
            .definition_dir(definition_id)
            .join(&delta_name.file_name);
        root.recent_delta_files.push(delta_name);
        root.recompute_checksum()?;
        if let Err(err) = self
            .manifests
            .maybe_compact_deltas(definition_id, &mut root)
        {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }
        if let Err(err) = self.manifests.write_root(definition_id, &root) {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }

        let artifacts =
            replace_artifacts(&current_manifest.artifacts, repacked_artifacts.into_iter());
        let loaded = self.manifests.materialize_loaded_manifest(
            definition_id,
            root,
            artifacts,
            current_manifest.tail_pending_entries.clone(),
        );
        let mut next_state = state.clone();
        self.retire_manifest_replaced_by(state.definition.kind, state.manifest.as_ref(), &loaded);
        next_state = next_state.with_manifest(loaded);
        storage_metrics().record_search_manifest_publish(
            self.manifests.codec_label(),
            elapsed_micros_since(started_at),
        );
        storage_metrics().set_search_manifest_delta_count(
            self.manifests.codec_label(),
            next_state.manifest_delta_count(),
        );
        Ok(next_state)
    }

    fn publish_delta_for_covered_tail_entries(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
        retire_old_manifest: bool,
    ) -> Result<Option<SearchDefinitionState>> {
        let Some(current_manifest) = state.manifest.as_ref() else {
            return Ok(None);
        };
        let generation = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("tail cover publish requires generation"))?;
        let visible_by_id = visible_rowsets
            .iter()
            .map(|rowset| (rowset.rowset_id(), rowset.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut rowset_snapshots = BTreeMap::<RowsetId, RowsetSearchSnapshot>::new();
        let mut artifact_keys = current_manifest
            .artifacts
            .artifacts
            .iter()
            .map(search_artifact_key)
            .collect::<BTreeSet<_>>();
        let mut processed_rowsets = BTreeSet::new();
        let mut covered_tail_ids = Vec::new();
        let mut added_artifacts = Vec::new();
        let mut added_tail_entries = Vec::new();

        for entry in &current_manifest.tail_pending_entries {
            if matches!(entry.mutation, TailMutationKind::Delete) {
                continue;
            }
            let Some(rowset) = visible_by_id.get(&entry.rowset_id) else {
                continue;
            };
            if !processed_rowsets.insert(entry.rowset_id) {
                continue;
            }
            let snapshot = if let Some(snapshot) = rowset_snapshots.get(&entry.rowset_id) {
                snapshot.clone()
            } else {
                rowset.load()?;
                let snapshot = collect_rowset_snapshot(&state.definition, rowset, visible_version)?;
                rowset_snapshots.insert(entry.rowset_id, snapshot.clone());
                snapshot
            };
            let snapshot_artifact_keys = snapshot
                .artifacts
                .iter()
                .map(search_artifact_key)
                .collect::<BTreeSet<_>>();
            let covered_ids_for_rowset = current_manifest
                .tail_pending_entries
                .iter()
                .filter(|tail_entry| {
                    tail_entry.rowset_id == entry.rowset_id
                        && !matches!(tail_entry.mutation, TailMutationKind::Delete)
                        && tail_entry_is_covered_by_artifacts(
                            &state.definition,
                            tail_entry,
                            &artifact_keys,
                            &snapshot_artifact_keys,
                        )
                })
                .map(|tail_entry| tail_entry.entry_id)
                .collect::<Vec<_>>();
            if covered_ids_for_rowset.is_empty() {
                continue;
            }

            covered_tail_ids.extend(covered_ids_for_rowset);
            for artifact in snapshot.artifacts {
                if artifact_keys.insert(search_artifact_key(&artifact)) {
                    added_artifacts.push(artifact);
                }
            }
            for tail_entry in snapshot.tail_entries.entries {
                if tail_entry_already_live(&current_manifest.tail_pending_entries, &tail_entry)
                    || tail_entry_already_live(&added_tail_entries, &tail_entry)
                {
                    continue;
                }
                added_tail_entries.push(tail_entry);
            }
        }

        if covered_tail_ids.is_empty()
            && added_artifacts.is_empty()
            && added_tail_entries.is_empty()
        {
            return Ok(None);
        }

        let started_at = Instant::now();
        let definition_id = state.definition.definition_id;
        let mut root = current_manifest.root.clone();
        root.root_version = root.root_version.saturating_add(1);
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = visible_version;
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let mut next_tail_entry_id = root.next_tail_entry_id.0;
        assign_tail_entry_ids(&mut added_tail_entries, &mut next_tail_entry_id);
        root.next_tail_entry_id = TailEntryId(next_tail_entry_id);

        added_artifacts = assign_generation_id(added_artifacts, generation.generation_id);
        let covered_tail_ids = covered_tail_ids.into_iter().collect::<BTreeSet<_>>();
        let mut tail_pending_entries = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| !covered_tail_ids.contains(&entry.entry_id))
            .cloned()
            .collect::<Vec<_>>();
        tail_pending_entries.extend(added_tail_entries.iter().cloned());

        let delta_generation_stats =
            generation_stats_from_artifacts(&state.definition, &added_artifacts);
        root.generation_stats.merge_assign(&delta_generation_stats);
        let tail_pending = TailPendingSet {
            entries: tail_pending_entries.clone(),
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

        let mut delta_entries = Vec::new();
        delta_entries.extend(
            added_artifacts
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::AddArtifact),
        );
        delta_entries.extend(
            covered_tail_ids
                .iter()
                .copied()
                .map(ManifestDeltaEntry::CoverTail),
        );
        delta_entries.extend(
            added_tail_entries
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::UpsertTail),
        );
        delta_entries.extend(
            stats_deltas_from_generation_stats(&delta_generation_stats)
                .into_iter()
                .map(ManifestDeltaEntry::StatsDelta),
        );

        let delta_name = self.manifests.write_delta(
            definition_id,
            generation.generation_id,
            root.root_version,
            root.recent_delta_files.len(),
            &ManifestDelta::new(delta_entries),
        )?;
        let delta_path = self
            .manifests
            .definition_dir(definition_id)
            .join(&delta_name.file_name);
        root.recent_delta_files.push(delta_name);
        root.recompute_checksum()?;
        if let Err(err) = self
            .manifests
            .maybe_compact_deltas(definition_id, &mut root)
        {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }
        if let Err(err) = self.manifests.write_root(definition_id, &root) {
            self.manifests.remove_paths(&[delta_path]);
            return Err(err);
        }

        let mut artifacts = current_manifest.artifacts.clone();
        artifacts.artifacts.extend(added_artifacts);
        let loaded = self.manifests.materialize_loaded_manifest(
            definition_id,
            root,
            artifacts,
            tail_pending_entries,
        );
        let mut next_state = state.clone();
        if retire_old_manifest {
            self.retire_manifest_replaced_by(
                state.definition.kind,
                state.manifest.as_ref(),
                &loaded,
            );
        }
        next_state = next_state.with_manifest(loaded);
        storage_metrics().record_search_manifest_publish(
            self.manifests.codec_label(),
            elapsed_micros_since(started_at),
        );
        storage_metrics().set_search_manifest_delta_count(
            self.manifests.codec_label(),
            next_state.manifest_delta_count(),
        );
        record_tail_metrics_for_state(&next_state);
        Ok(Some(next_state))
    }

    fn definition_lock(&self, definition_id: u64) -> Arc<Mutex<()>> {
        let mut guard = self.publish_locks.lock().expect("search publish locks");
        guard
            .entry(definition_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn retire_manifest(&self, provider: SearchIndexKind, manifest: Option<&LoadedManifest>) {
        let Some(manifest) = manifest else {
            return;
        };
        let paths = retire_paths_for_manifest(&self.tablet.data_dir().clone(), manifest);
        self.retire_manifest_paths(provider, manifest, paths);
    }

    fn retire_manifest_replaced_by(
        &self,
        provider: SearchIndexKind,
        old: Option<&LoadedManifest>,
        new: &LoadedManifest,
    ) {
        let Some(old) = old else {
            return;
        };
        let keep_paths = retire_paths_for_manifest(&self.tablet.data_dir().clone(), new)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let retired_paths = retire_paths_for_manifest(&self.tablet.data_dir().clone(), old)
            .into_iter()
            .filter(|path| !keep_paths.contains(path))
            .collect::<Vec<_>>();
        self.retire_manifest_paths(provider, old, retired_paths);
    }

    fn retire_manifest_paths(
        &self,
        provider: SearchIndexKind,
        manifest: &LoadedManifest,
        paths: Vec<PathBuf>,
    ) {
        if paths.is_empty() {
            return;
        }
        let bytes = manifest_path_bytes(&paths);
        storage_metrics().record_search_generation_retired(provider, bytes);
        let retired = RetiredManifest {
            provider,
            artifacts: Arc::new(manifest.artifacts.clone()),
            paths,
            retired_at: Instant::now(),
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
                    remove.push(retired);
                } else {
                    keep.push(retired);
                }
            }
            *guard = keep;
        }
        for retired in remove {
            let delay_us = elapsed_micros_since(retired.retired_at);
            storage_metrics().record_search_generation_lease_hold(retired.provider, delay_us);
            storage_metrics().record_search_artifact_gc_delay(
                retired.provider,
                "lease_released",
                delay_us,
            );
            self.manifests.remove_paths(&retired.paths);
        }
    }

    fn restore_schema_seed_if_needed(
        &self,
        view: &mut SearchView,
        definition: &SearchIndexDefinition,
    ) -> Result<Option<u64>> {
        if definition.kind != SearchIndexKind::Hnsw || definition.column_ids.len() != 1 {
            return Ok(None);
        }
        let Some(schema) = self.tablet.schema() else {
            return Ok(None);
        };
        let Some((column_id, seed)) =
            restored_schema_seed_definition(self.tablet.table_id(), &schema, definition)?
        else {
            return Ok(None);
        };
        let seed_definition_id = seed.definition_id;
        view.definitions
            .entry(seed.definition_id)
            .or_insert_with(|| {
                SearchDefinitionState::new(seed, SearchDefinitionOrigin::schema_seed(column_id))
            });
        Ok(Some(seed_definition_id))
    }

    fn seed_schema_hnsw_definitions(&self) {
        let Some(schema) = self.tablet.schema() else {
            return;
        };
        for (column_id, definition) in hnsw_schema_seed_definitions(self.tablet.table_id(), &schema)
        {
            match definition {
                Ok(definition) => {
                    let _ = self.update_definition(
                        definition,
                        SearchDefinitionOrigin::schema_seed(column_id),
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        tablet_id = self.tablet.tablet_id(),
                        column_id,
                        error = %err,
                        "seed schema hnsw definition failed"
                    );
                }
            }
        }
    }
}

fn elapsed_micros_since(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

fn manifest_path_bytes(paths: &[PathBuf]) -> u64 {
    paths
        .iter()
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::text_index::FullTextIndex;
    use crate::index::hnsw::SearchParams;
    use crate::meta::{FileMetadataStore, GlobalSchemaMap, MetadataStore, TabletMetaManager};
    use crate::rowset::{ColumnData, RowsetWriter, RowsetWriterContext, SparseVector};
    use crate::search::artifact::{ArtifactLocation, SegmentPagePointer};
    use crate::search::capability::ArtifactSegmentRef;
    use crate::search::definition::origin::SCHEMA_SEED_BIT;
    use crate::search::maintenance::ProviderMaintenanceRequest;
    use crate::search::manifest::{ManifestDelta, ManifestDeltaEntry, DELTA_COUNT_SOFT_LIMIT};
    use crate::search::stats::{
        ExecutionModes, FullTextProviderStats, GenerationMaintenanceState, GenerationStats,
        HnswProviderStats, SearchArtifactStats, SearchProviderStats, SparseProviderStats,
    };
    use crate::search::tail::{TailMutationKind, TailPendingEntry, TailRowImageRef};
    use crate::search::{ArtifactFileId, SearchFreshnessPolicy, SearchStatsDelta};
    use crate::search::{
        CoverageState, FlushSearchMode, OpenSearchCursorResult, SearchCapabilityState,
        SearchMaintenanceAction, SearchNotQueryableReason,
    };
    use crate::search::{OpenedSearchCursor, ResourceBudget, SearchBatchConfig, SearchBatchState};
    use crate::table::table_factory::TableFactory;
    use crate::table::table_handle::{TableColumnSpec, TableHandle};
    use crate::tablet::{KeysType, Tablet, TabletColumn, TabletSchema, Version};
    use crate::test_utils::*;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_scheduler::scheduler::TaskScheduler;
    use serde_json::json;
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

    fn create_table_without_default_indexes(
        root: &std::path::Path,
        types: &[LogicalType],
    ) -> TableHandle {
        let columns = types
            .iter()
            .enumerate()
            .map(|(idx, logical_type)| {
                TabletColumn::new(idx as u32, format!("col_{idx}"), logical_type.clone())
            })
            .collect();
        let schema = Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap());
        let tablet = Tablet::new(10_001, 10_001, 0, schema, root.join("tablet"), None).unwrap();
        tablet.init().unwrap();
        TableHandle::from_runtime_tablet(tablet, types.to_vec())
    }

    fn test_sparse_blob_vector(values: &[SparseVector]) -> Vector {
        let mut vector = Vector::try_new(LogicalType::Blob, values.len(), test_allocator())
            .expect("blob vector allocation");
        for (idx, value) in values.iter().enumerate() {
            vector.set_blob(idx, &value.to_row_image_v1().expect("sparse row image"));
        }
        vector.set_count(values.len());
        vector
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

    fn encode_varlen(values: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes
    }

    fn drain_search_cursor(
        table: &TableHandle,
        opened: OpenedSearchCursor,
        projected_columns: &[usize],
        emit_score: bool,
        row_limit: usize,
    ) -> paro_common::error::Result<Vec<Chunk>> {
        let mut chunks = Vec::new();
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let batch_config = SearchBatchConfig {
            row_limit: row_limit.max(1),
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget {
            memory_limit_bytes: 64 * 1024 * 1024,
            heap_budget_items: row_limit.max(1024),
            parallelism_slots: 1,
            cpu_step_budget: None,
            context: None,
        };

        loop {
            match cursor.next_batch(&batch_config, &mut budget)? {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => chunks.push(table.materialize_search_batch(
                    &snapshot,
                    batch,
                    projected_columns,
                    emit_score,
                )?),
                SearchBatchState::Exhausted => return Ok(chunks),
            }
        }
    }

    fn load_manifest_delta_entries(
        table: &crate::table::table_handle::TableHandle,
        definition_id: u64,
    ) -> Vec<ManifestDeltaEntry> {
        let current = table.search_registry().view.load();
        let state = current
            .definitions
            .get(&definition_id)
            .expect("definition state");
        let manifest = state.manifest.as_ref().expect("manifest");
        let delta_files = manifest.root.recent_delta_files.clone();
        let definition_dir = table
            .search_registry()
            .manifests
            .definition_dir(definition_id);
        drop(current);

        delta_files
            .iter()
            .flat_map(|delta_file| {
                let bytes =
                    std::fs::read(definition_dir.join(&delta_file.file_name)).expect("read delta");
                serde_json::from_slice::<ManifestDelta>(&bytes)
                    .expect("decode delta")
                    .entries
            })
            .collect()
    }

    fn fulltext_test_definition(definition_id: u64) -> SearchIndexDefinition {
        let provider_config = json!({"config": "simple"});
        SearchIndexDefinition {
            definition_id,
            table_id: 10,
            name: format!("fts_{definition_id}"),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::Required,
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        }
    }

    fn fulltext_test_artifact(
        definition_id: u64,
        rowset_id: u64,
        total_docs: u32,
        total_terms: u64,
        unique_terms: u32,
        total_postings: u64,
        max_posting_list_len: u32,
    ) -> SearchArtifactRef {
        SearchArtifactRef {
            definition_id,
            generation_id: 1,
            segment: ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            column_id: 0,
            kind: SearchIndexKind::FullText,
            provider_variant: 1,
            artifact_format_version: 1,
            location: ArtifactLocation::Inline {
                page: SegmentPagePointer {
                    rowset_id,
                    segment_id: 0,
                    column_id: 0,
                    page_offset: rowset_id * 100,
                    page_len: 64,
                    checksum: rowset_id,
                },
            },
            stats: SearchArtifactStats {
                row_count: u64::from(total_docs),
                bytes_on_disk: 64,
                provider_stats: Some(SearchProviderStats::FullText(FullTextProviderStats {
                    total_docs,
                    total_terms,
                    avg_doc_length: if total_docs == 0 {
                        0.0
                    } else {
                        total_terms as f32 / total_docs as f32
                    },
                    unique_terms,
                    total_postings,
                    max_posting_list_len,
                    min_posting_list_len: 1,
                    bm25_k1: 1.2,
                    bm25_b: 0.75,
                    tokenizer: "simple".to_string(),
                })),
            },
            checksum: rowset_id,
        }
    }

    fn sparse_test_definition(definition_id: u64) -> SearchIndexDefinition {
        let provider_config = json!({ "physical_encoding": "binary-v1" });
        SearchIndexDefinition {
            definition_id,
            table_id: 1,
            name: format!("sparse_{definition_id}"),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Sparse,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        }
    }

    fn sparse_test_artifact(
        definition_id: u64,
        rowset_id: u64,
        row_count: u64,
        nnz: u64,
        unique_dimensions: u64,
        max_l2_norm: f32,
    ) -> SearchArtifactRef {
        SearchArtifactRef {
            definition_id,
            generation_id: 1,
            segment: ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            column_id: 0,
            kind: SearchIndexKind::Sparse,
            provider_variant: 1,
            artifact_format_version: 1,
            location: ArtifactLocation::Inline {
                page: SegmentPagePointer {
                    rowset_id,
                    segment_id: 0,
                    column_id: 0,
                    page_offset: rowset_id * 100,
                    page_len: 64,
                    checksum: rowset_id,
                },
            },
            stats: SearchArtifactStats {
                row_count,
                bytes_on_disk: 64,
                provider_stats: Some(SearchProviderStats::Sparse(SparseProviderStats {
                    row_count,
                    nnz,
                    posting_fanout: nnz,
                    unique_dimensions,
                    avg_vector_nnz: if row_count == 0 {
                        0.0
                    } else {
                        nnz as f32 / row_count as f32
                    },
                    l2_norm_sum: max_l2_norm as f64 * row_count as f64,
                    max_l2_norm,
                })),
            },
            checksum: rowset_id,
        }
    }

    fn hnsw_test_definition(definition_id: u64) -> SearchIndexDefinition {
        let provider_config = json!({
            "m": 16,
            "ef_construct": 100,
            "distance": "l2",
            "dimension": 128,
        });
        SearchIndexDefinition {
            definition_id,
            table_id: 1,
            name: format!("hnsw_{definition_id}"),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        }
    }

    fn hnsw_test_artifact(
        definition_id: u64,
        rowset_id: u64,
        vector_count: u64,
        max_level: u32,
        max_level0_degree: u32,
    ) -> SearchArtifactRef {
        SearchArtifactRef {
            definition_id,
            generation_id: 1,
            segment: ArtifactSegmentRef {
                rowset_id,
                segment_id: 0,
            },
            column_id: 0,
            kind: SearchIndexKind::Hnsw,
            provider_variant: 1,
            artifact_format_version: 1,
            location: ArtifactLocation::Inline {
                page: SegmentPagePointer {
                    rowset_id,
                    segment_id: 0,
                    column_id: 0,
                    page_offset: rowset_id * 100,
                    page_len: 64,
                    checksum: rowset_id,
                },
            },
            stats: SearchArtifactStats {
                row_count: vector_count,
                bytes_on_disk: 64,
                provider_stats: Some(SearchProviderStats::Hnsw(HnswProviderStats {
                    vector_count,
                    dimension: 128,
                    max_level,
                    m: 16,
                    ef_construction: 100,
                    graph_memory_bytes: vector_count * 256,
                    vector_storage_bytes: vector_count * 512,
                    total_graph_links: vector_count * 18,
                    level0_graph_links: vector_count * 12,
                    avg_level0_degree: if vector_count == 0 { 0.0 } else { 12.0 },
                    max_level0_degree,
                })),
            },
            checksum: rowset_id,
        }
    }

    #[test]
    fn artifact_replacement_stats_rebuilds_irreversible_fulltext_summary() {
        let definition = fulltext_test_definition(91);
        let removed = fulltext_test_artifact(91, 1, 4, 8, 4, 8, 10);
        let kept = fulltext_test_artifact(91, 2, 6, 18, 5, 12, 6);
        let added = fulltext_test_artifact(91, 3, 2, 4, 2, 3, 3);
        let current =
            generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]);
        let materialized = vec![kept, added.clone()];

        let next = generation_stats_after_artifact_replacement(
            &definition,
            &current,
            &[removed],
            &[added],
            &materialized,
        )
        .unwrap();

        let fulltext = next.fulltext_provider_stats().expect("fulltext stats");
        assert_eq!(next.indexed_rows, 8);
        assert_eq!(next.artifact_count, 2);
        assert_eq!(fulltext.total_docs, 8);
        assert_eq!(fulltext.total_terms, 22);
        assert_eq!(fulltext.unique_terms, 7);
        assert_eq!(fulltext.total_postings, 15);
        assert_eq!(fulltext.max_posting_list_len, 6);
    }

    #[test]
    fn artifact_replacement_stats_rebuilds_irreversible_sparse_summary() {
        let definition = sparse_test_definition(92);
        let removed = sparse_test_artifact(92, 1, 4, 12, 3, 3.0);
        let kept = sparse_test_artifact(92, 2, 6, 20, 5, 4.0);
        let added = sparse_test_artifact(92, 3, 2, 8, 6, 5.0);
        let current =
            generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]);
        let materialized = vec![kept, added.clone()];

        let next = generation_stats_after_artifact_replacement(
            &definition,
            &current,
            &[removed],
            &[added],
            &materialized,
        )
        .unwrap();

        let sparse = next.sparse_provider_stats().expect("sparse stats");
        assert_eq!(next.indexed_rows, 8);
        assert_eq!(next.artifact_count, 2);
        assert_eq!(sparse.row_count, 8);
        assert_eq!(sparse.nnz, 28);
        assert_eq!(sparse.posting_fanout, 28);
        assert_eq!(sparse.unique_dimensions, 11);
        assert_eq!(sparse.max_l2_norm, 5.0);
        assert!((sparse.avg_vector_nnz - 3.5).abs() < 1e-6);
    }

    #[test]
    fn artifact_replacement_stats_rebuilds_irreversible_hnsw_summary() {
        let definition = hnsw_test_definition(93);
        let removed = hnsw_test_artifact(93, 1, 4, 2, 24);
        let kept = hnsw_test_artifact(93, 2, 6, 3, 32);
        let added = hnsw_test_artifact(93, 3, 2, 5, 48);
        let current =
            generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]);
        let materialized = vec![kept, added.clone()];

        let next = generation_stats_after_artifact_replacement(
            &definition,
            &current,
            &[removed],
            &[added],
            &materialized,
        )
        .unwrap();

        let hnsw = next.hnsw_provider_stats().expect("hnsw stats");
        assert_eq!(next.indexed_rows, 8);
        assert_eq!(next.artifact_count, 2);
        assert_eq!(hnsw.vector_count, 8);
        assert_eq!(hnsw.dimension, 128);
        assert_eq!(hnsw.max_level, 5);
        assert_eq!(hnsw.max_level0_degree, 48);
        assert_eq!(hnsw.graph_memory_bytes, 8 * 256);
        assert_eq!(hnsw.vector_storage_bytes, 8 * 512);
        assert_eq!(hnsw.total_graph_links, 8 * 18);
        assert_eq!(hnsw.level0_graph_links, 8 * 12);
        assert!((hnsw.avg_level0_degree - 12.0).abs() < 1e-6);
    }

    #[test]
    fn full_snapshot_tail_id_assignment_reuses_existing_ids_and_root_cursor() {
        let existing_tail = TailPendingEntry {
            entry_id: TailEntryId(7),
            rowset_id: 11,
            segment_ids: vec![0],
            mutation: TailMutationKind::Append,
            row_count: 10,
            byte_count: 1024,
            row_image_ref: Some(TailRowImageRef::WholeRowset),
        };
        let manifest = LoadedManifest {
            root: GenerationManifestRoot {
                definition_id: 44,
                generation_id: 1,
                build_epoch: 1,
                build_snapshot_version: 1,
                indexed_through_ts: 1,
                config_fingerprint: 99,
                coverage: CoverageState::TailPending {
                    pending_rowsets: 1,
                    pending_segments: 1,
                    pending_rows: 10,
                    exact_tail_merge: true,
                },
                generation_stats: GenerationStats::default(),
                next_tail_entry_id: TailEntryId(10),
                execution_modes: ExecutionModes::default(),
                maintenance_state: GenerationMaintenanceState::default(),
                root_version: 1,
                checksum: 0,
                shard_files: Vec::new(),
                recent_delta_files: Vec::new(),
                materialized_state_file: None,
            },
            root_path: std::path::PathBuf::new(),
            shard_paths: Vec::new(),
            delta_paths: Vec::new(),
            materialized_state_path: None,
            embedded_materialized_state: false,
            artifacts: GenerationArtifactSet::default(),
            tail_pending_entries: vec![existing_tail.clone()],
        };
        let mut snapshot_entries = vec![
            TailPendingEntry {
                entry_id: TailEntryId::UNASSIGNED,
                ..existing_tail
            },
            TailPendingEntry {
                entry_id: TailEntryId::UNASSIGNED,
                rowset_id: 12,
                segment_ids: vec![0],
                mutation: TailMutationKind::Append,
                row_count: 20,
                byte_count: 2048,
                row_image_ref: Some(TailRowImageRef::WholeRowset),
            },
        ];

        let next_id =
            assign_tail_entry_ids_for_full_snapshot(&mut snapshot_entries, Some(&manifest));

        assert_eq!(snapshot_entries[0].entry_id, TailEntryId(7));
        assert_eq!(snapshot_entries[1].entry_id, TailEntryId(10));
        assert_eq!(next_id, TailEntryId(11));
    }

    #[test]
    fn registry_write_context_carries_inline_builder_set() {
        let mut view = SearchView::default();
        let fulltext_definition = SearchIndexDefinition {
            definition_id: 10,
            table_id: 20,
            name: "body_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![1],
            expression: None,
            provider_config: json!({"config": "simple"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: 100,
        };
        let sparse_definition = SearchIndexDefinition {
            definition_id: 11,
            table_id: 20,
            name: "emb_sparse".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![2],
            expression: None,
            provider_config: json!({}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: 101,
        };

        view.definitions.insert(
            fulltext_definition.definition_id,
            SearchDefinitionState::new(
                fulltext_definition.clone(),
                SearchDefinitionOrigin::catalog(fulltext_definition.definition_id),
            ),
        );
        view.definitions.insert(
            sparse_definition.definition_id,
            SearchDefinitionState::new(
                sparse_definition.clone(),
                SearchDefinitionOrigin::catalog(sparse_definition.definition_id),
            ),
        );

        let admission: Arc<dyn SearchAdmission> = Arc::new(InlineSearchAdmission::default());
        let context = view.write_context(Some(admission)).unwrap();
        assert_eq!(context.plan.fulltext.len(), 1);
        assert_eq!(context.plan.sparse.len(), 1);
        assert_eq!(context.inline_builders.len(), 2);
        assert!(context.inline_builders.admission().is_some());
        assert!(context
            .inline_builders
            .entries()
            .iter()
            .any(|entry| entry.definition.kind == SearchIndexKind::FullText));
        assert!(context
            .inline_builders
            .entries()
            .iter()
            .any(|entry| entry.definition.kind == SearchIndexKind::Sparse));
        assert!(context
            .inline_builders
            .entries()
            .iter()
            .all(|entry| entry.generation_id == 1));
    }

    #[test]
    fn inline_builder_set_coalesces_duplicate_fulltext_payloads_with_strict_policy() {
        let mut view = SearchView::default();
        let physical_config = json!({"config": "simple"});
        let opportunistic = SearchIndexDefinition {
            definition_id: 12,
            table_id: 20,
            name: "docs_fts_opportunistic".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            provider_config: physical_config.clone(),
            freshness_policy: SearchFreshnessPolicy::Opportunistic,
            config_fingerprint: 201,
        };
        let required = SearchIndexDefinition {
            definition_id: 13,
            table_id: 20,
            name: "docs_fts_required".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            provider_config: physical_config,
            freshness_policy: SearchFreshnessPolicy::Required,
            config_fingerprint: 202,
        };
        view.definitions.insert(
            opportunistic.definition_id,
            SearchDefinitionState::new(
                opportunistic.clone(),
                SearchDefinitionOrigin::catalog(opportunistic.definition_id),
            ),
        );
        view.definitions.insert(
            required.definition_id,
            SearchDefinitionState::new(
                required.clone(),
                SearchDefinitionOrigin::catalog(required.definition_id),
            ),
        );

        let context = view.write_context(None).unwrap();
        assert_eq!(context.plan.fulltext.len(), 1);
        assert_eq!(context.inline_builders.len(), 1);
        let entry = &context.inline_builders.entries()[0];
        assert_eq!(entry.definition.definition_id, required.definition_id);
        assert_eq!(entry.flush_mode(), FlushSearchMode::InlineRequired);
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
    fn explicit_hnsw_definition_overrides_and_restores_schema_seed_origin() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        );
        let seed_definition_id = SCHEMA_SEED_BIT;
        {
            let current = table.search_registry().view.load();
            let seed_state = current
                .definitions
                .get(&seed_definition_id)
                .expect("schema seed definition");
            assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(0));
        }

        let provider_config = json!({
            "m": 16,
            "ef_construct": 64,
            "distance": "l2",
        });
        let definition = SearchIndexDefinition {
            definition_id: 77,
            table_id: table.tablet_id(),
            name: "explicit_vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };

        table.register_search_definition(definition).unwrap();
        {
            let current = table.search_registry().view.load();
            assert!(!current.definitions.contains_key(&seed_definition_id));
            let catalog_state = current.definitions.get(&77).expect("catalog definition");
            assert_eq!(catalog_state.origin, SearchDefinitionOrigin::catalog(77));
        }

        table.unregister_search_definition(77).unwrap();
        let current = table.search_registry().view.load();
        let restored = current
            .definitions
            .get(&seed_definition_id)
            .expect("restored schema seed");
        assert_eq!(restored.origin, SearchDefinitionOrigin::schema_seed(0));
    }

    #[test]
    fn hnsw_schema_seed_definition_recovers_after_reopen() {
        let root = TempDir::new().unwrap();
        let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 4);
        let table = TableFactory::new(Some(meta_manager(root.path())))
            .with_storage_root(root.path())
            .create_table_from_specs(&[
                TableColumnSpec {
                    name: "id".to_string(),
                    logical_type: LogicalType::Integer,
                    is_key: true,
                    not_null: true,
                },
                TableColumnSpec {
                    name: "vec".to_string(),
                    logical_type: vector_type,
                    is_key: false,
                    not_null: false,
                },
            ])
            .expect("create hnsw table");
        table
            .append(&test_chunk_from_vectors(vec![
                test_i32_vector(&[1, 2]),
                test_embedding_vector(&[vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]], 4),
            ]))
            .unwrap();

        let seed_definition_id = SCHEMA_SEED_BIT | 1;
        {
            let current = table.search_registry().view.load();
            let seed_state = current
                .definitions
                .get(&seed_definition_id)
                .expect("schema seed definition");
            assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
            let generation = seed_state.generation.as_ref().expect("seed generation");
            assert!(generation.coverage.is_complete());
            assert_eq!(generation.generation_stats.artifact_count, 1);
            let manifest = seed_state.manifest.as_ref().expect("seed manifest");
            assert_eq!(manifest.artifacts.artifacts.len(), 1);
            assert!(matches!(
                manifest.artifacts.artifacts[0].location,
                ArtifactLocation::Inline { .. }
            ));
        }

        let descriptor = table.to_descriptor().expect("descriptor");
        drop(table);
        let reopened = reopen_table_with_root(root.path(), &[], &descriptor);
        let recovered_capability = reopened
            .vector_capability(1)
            .expect("recovered schema seed capability");
        assert_eq!(recovered_capability.definition_id, seed_definition_id);
        assert!(recovered_capability.coverage.is_complete());
        assert_eq!(recovered_capability.generation_stats.artifact_count, 1);
        {
            let current = reopened.search_registry().view.load();
            let seed_state = current
                .definitions
                .get(&seed_definition_id)
                .expect("recovered schema seed definition");
            assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
        }

        let provider_config = json!({
            "m": 16,
            "ef_construct": 64,
            "distance": "l2",
        });
        let explicit = SearchIndexDefinition {
            definition_id: 78,
            table_id: reopened.tablet_id(),
            name: "explicit_recovered_vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![1],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[1],
                None,
                &provider_config,
            ),
            provider_config,
        };
        reopened.register_search_definition(explicit).unwrap();
        {
            let current = reopened.search_registry().view.load();
            assert!(!current.definitions.contains_key(&seed_definition_id));
            assert_eq!(
                current
                    .definitions
                    .get(&78)
                    .expect("explicit definition")
                    .origin,
                SearchDefinitionOrigin::catalog(78)
            );
        }

        reopened.unregister_search_definition(78).unwrap();
        {
            let current = reopened.search_registry().view.load();
            assert_eq!(
                current
                    .definitions
                    .get(&seed_definition_id)
                    .expect("restored schema seed")
                    .origin,
                SearchDefinitionOrigin::schema_seed(1)
            );
        }

        let descriptor = reopened.to_descriptor().expect("descriptor after restore");
        drop(reopened);
        let reopened_again = reopen_table_with_root(root.path(), &[], &descriptor);
        let current = reopened_again.search_registry().view.load();
        let seed_state = current
            .definitions
            .get(&seed_definition_id)
            .expect("schema seed restored after second reopen");
        assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
        assert!(reopened_again.vector_capability(1).is_some());

        let opened = reopened_again
            .open_vector_search_cursor(
                1,
                &[1.0, 0.0, 0.0, 0.0],
                1,
                SearchParams {
                    ef: Some(16),
                    random_entry_point: Some(false),
                    ..Default::default()
                },
                None,
                reopened_again.max_version(),
            )
            .expect("query restored schema seed generation");
        let chunks = drain_search_cursor(&reopened_again, opened, &[0], false, 1)
            .expect("materialize restored schema seed query");
        let mut ids = Vec::new();
        for chunk in chunks {
            let id_col = chunk.column(0).expect("id projection");
            for row in 0..chunk.size() {
                ids.push(id_col.get_i32(row).expect("id value"));
            }
        }
        assert_eq!(ids, vec![1]);
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
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
        let snapshot = table
            .open_search_generation_snapshot(42)
            .unwrap()
            .expect("generation snapshot");
        assert_eq!(snapshot.artifacts.artifacts.len(), 1);
        let artifact = &snapshot.artifacts.artifacts[0];
        assert_eq!(artifact.segment.rowset_id, 1);
        assert_eq!(artifact.segment.segment_id, 0);
        assert_eq!(artifact.column_id, 0);
        match &artifact.location {
            ArtifactLocation::Inline { page } => {
                assert_eq!(page.rowset_id, 1);
                assert_eq!(page.segment_id, 0);
                assert_eq!(page.column_id, 0);
                assert!(page.page_offset > 0);
                assert!(page.page_len > 0);
                assert_ne!(page.checksum, 0);
            }
            other => panic!("expected inline artifact location, got {other:?}"),
        }
        assert_ne!(artifact.checksum, 0);

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
    fn token_open_validates_generation_head_before_snapshot() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 43,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "token stale guard",
            ])]))
            .unwrap();

        let capability = table
            .search_registry()
            .capability(
                SearchIndexKind::FullText,
                0,
                Some(definition.config_fingerprint),
            )
            .expect("queryable capability");
        let token = capability.capability_token();

        match table
            .open_search_generation_snapshot_with_token(&token)
            .unwrap()
        {
            OpenSearchCursorResult::Opened(snapshot) => {
                assert_eq!(snapshot.definition_id, 43);
                assert_eq!(snapshot.generation_id, token.generation_id);
            }
            other => panic!("expected opened snapshot, got {other:?}"),
        }

        let mut stale = token.clone();
        stale.generation_id = stale.generation_id.saturating_add(1);
        assert!(matches!(
            table
                .open_search_generation_snapshot_with_token(&stale)
                .unwrap(),
            OpenSearchCursorResult::CapabilityTokenStale
        ));

        let mut not_queryable = token;
        not_queryable.capability_state = SearchCapabilityState::NotQueryable {
            reason: SearchNotQueryableReason::CoverageIncomplete,
        };
        assert!(matches!(
            table
                .open_search_generation_snapshot_with_token(&not_queryable)
                .unwrap(),
            OpenSearchCursorResult::NotQueryable
        ));
    }

    #[test]
    fn required_freshness_tail_pending_waits_for_catch_up_before_capability() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "required freshness waits",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 48,
            table_id: table.tablet_id(),
            name: "docs_fts_required".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::Required,
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
            .unwrap();

        let capability = table
            .search_registry()
            .capability(
                SearchIndexKind::FullText,
                0,
                Some(definition.config_fingerprint),
            )
            .expect("required freshness capability after catch up");
        assert!(capability.is_queryable());
        assert_eq!(
            capability.capability_state(),
            SearchCapabilityState::Queryable
        );
        assert_eq!(capability.tail_summary.pending_rows, 0);
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&48)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after required freshness wait");
        assert!(manifest.root.coverage.is_complete());
        assert!(manifest.tail_pending_entries.is_empty());
        assert!(manifest.artifacts.artifacts.iter().any(|artifact| matches!(
            artifact.location,
            ArtifactLocation::SidecarArtifactFile { .. }
        )));

        assert!(matches!(
            table
                .open_search_generation_snapshot_with_token(&capability.capability_token())
                .unwrap(),
            OpenSearchCursorResult::Opened(_)
        ));
    }

    #[test]
    fn token_open_rechecks_same_generation_freshness_degradation() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "bounded lag initially queryable",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 49,
            table_id: table.tablet_id(),
            name: "docs_fts_bounded".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
            .unwrap();

        let token = table
            .search_registry()
            .capability(
                SearchIndexKind::FullText,
                0,
                Some(definition.config_fingerprint),
            )
            .expect("bounded lag capability")
            .capability_token();
        assert!(token.is_queryable());

        let current = table.search_registry().view.load_full();
        let mut next = (*current).clone();
        let state = next
            .definitions
            .get_mut(&49)
            .expect("definition state to tighten freshness");
        state.definition.freshness_policy = SearchFreshnessPolicy::Required;
        if let Some(capability) = state.capability.as_mut() {
            capability.freshness_policy = SearchFreshnessPolicy::Required;
        }
        next.version = next.version.saturating_add(1);
        table.search_registry().view.store(Arc::new(next));

        assert!(matches!(
            table
                .open_search_generation_snapshot_with_token(&token)
                .unwrap(),
            OpenSearchCursorResult::NotQueryable
        ));
    }

    #[test]
    fn rowset_publish_observer_eagerly_refreshes_search_manifest() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 43,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
                "observer refresh",
            ])]))
            .unwrap();

        let current = table.search_registry().view.load();
        let state = current.definitions.get(&43).expect("definition state");
        let manifest = state.manifest.as_ref().expect("manifest after append");
        assert_eq!(manifest.root.build_snapshot_version, table.max_version());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        assert_eq!(manifest.artifacts.artifacts[0].segment.rowset_id, 1);
    }

    #[test]
    fn unpublished_rowset_with_inline_artifact_is_not_queryability_truth() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 143,
            table_id: table.tablet_id(),
            name: "docs_fts_unpublished".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();

        let rowset_id = 900;
        let rowset_path = table.tablet().canonical_rowset_path(rowset_id);
        let schema = table.tablet().schema().expect("schema");
        let write_context = table.search_registry().write_context().unwrap();
        let context = RowsetWriterContext::new(
            schema,
            table.tablet_id(),
            Version::singleton(0),
            &rowset_path,
        )
        .with_rowset_id(rowset_id)
        .with_search_inline_builders(write_context.inline_builders);
        let mut writer = RowsetWriter::create(context).unwrap();
        writer
            .add_chunk(&[ColumnData::new(
                encode_varlen(&["orphan inline artifact"]),
                1,
            )])
            .unwrap();
        let rowset = writer.build().unwrap();
        assert!(
            rowset.segments()[0].fulltext_index(0).is_some(),
            "test setup must write a real inline artifact before publish"
        );

        assert!(!table.search_registry().has_queryable_artifact(
            SearchIndexKind::FullText,
            rowset_id,
            0,
            0
        ));
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&143)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest");
        assert!(manifest.artifacts.artifacts.is_empty());
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
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
        let delta_entries = load_manifest_delta_entries(&table, 7);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::FullText
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::StatsDelta(SearchStatsDelta::FullText(delta))
                if delta.stats.total_docs > 0
        )));

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

    #[test]
    fn rowset_publish_persists_generation_head_with_tablet_meta() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let definition = fulltext_test_definition(107);
        table
            .register_search_definition(definition.clone())
            .unwrap();

        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "generation head is part of rowset publish",
            ])]))
            .unwrap();

        let manager = meta_manager(root.path());
        let meta = manager
            .load_tablet_meta(table.tablet_id())
            .unwrap()
            .expect("tablet meta");
        let head = meta
            .search_generation_heads()
            .iter()
            .find(|head| head.definition_id == 107)
            .expect("search generation head");
        assert_eq!(head.root_version, 2);
        assert!(head.root_file_name.starts_with("manifest_root_g1_v2"));

        let manifest = table
            .search_registry()
            .manifests
            .load_manifest_for_head(head)
            .unwrap()
            .expect("manifest by durable head");
        assert_eq!(manifest.root.build_snapshot_version, table.max_version());
        assert_eq!(
            manifest.root.config_fingerprint,
            definition.config_fingerprint
        );
        assert!(manifest.artifacts.artifacts.iter().any(|artifact| {
            artifact.kind == SearchIndexKind::FullText && artifact.segment.rowset_id > 0
        }));
    }

    #[test]
    fn rowset_publish_failure_does_not_advance_prepared_search_head() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar, LogicalType::Blob]);
        let fulltext = SearchIndexDefinition {
            table_id: table.tablet_id(),
            ..fulltext_test_definition(200)
        };
        table.register_search_definition(fulltext).unwrap();

        let provider_config = json!({ "physical_encoding": "binary-v1" });
        let sparse = SearchIndexDefinition {
            definition_id: 201,
            table_id: table.tablet_id(),
            name: "sparse_201".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![1],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Sparse,
                &[1],
                None,
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(sparse).unwrap();

        let initial_fulltext_root = {
            let current = table.search_registry().view.load();
            current
                .definitions
                .get(&200)
                .and_then(|state| state.manifest.as_ref())
                .expect("fulltext manifest")
                .root
                .root_version
        };
        let failing_sparse_root = table
            .search_registry()
            .manifests
            .definition_dir(201)
            .join("manifest_root_g1_v2.json");
        std::fs::create_dir(&failing_sparse_root).unwrap();

        let err = table.append(&test_chunk_from_vectors(vec![
            test_string_vector(&["first definition prepares a candidate"]),
            test_sparse_blob_vector(&[SparseVector::new(vec![1], vec![1.0]).unwrap()]),
        ]));
        assert!(err.is_err());
        assert_eq!(table.max_version(), -1);

        let current = table.search_registry().view.load();
        let fulltext_manifest = current
            .definitions
            .get(&200)
            .and_then(|state| state.manifest.as_ref())
            .expect("fulltext manifest after failed publish");
        assert_eq!(fulltext_manifest.root.root_version, initial_fulltext_root);
        drop(current);

        let meta = meta_manager(root.path())
            .load_tablet_meta(table.tablet_id())
            .unwrap()
            .expect("tablet meta");
        let durable_head = meta
            .search_generation_heads()
            .iter()
            .find(|head| head.definition_id == 200)
            .expect("durable fulltext head");
        assert_eq!(durable_head.root_version, initial_fulltext_root);
    }

    #[test]
    fn registry_reopen_ignores_unreferenced_versioned_root_candidate() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let definition = fulltext_test_definition(108);
        table
            .register_search_definition(definition.clone())
            .unwrap();

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&108)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest");
        let durable_root_version = manifest.root.root_version;
        let mut orphan_root = manifest.root.clone();
        drop(current);
        orphan_root.root_version = 99;
        orphan_root.build_snapshot_version = 99;
        orphan_root.indexed_through_ts = 99;
        orphan_root.recompute_checksum().unwrap();
        table
            .search_registry()
            .manifests
            .write_root(108, &orphan_root)
            .unwrap();

        let descriptor = table.to_descriptor().expect("descriptor");
        drop(table);
        let reopened = reopen_table_with_root(root.path(), &[LogicalType::Varchar], &descriptor);
        reopened.register_search_definition(definition).unwrap();

        let current = reopened.search_registry().view.load();
        let reopened_manifest = current
            .definitions
            .get(&108)
            .and_then(|state| state.manifest.as_ref())
            .expect("reopened manifest");
        assert_eq!(reopened_manifest.root.root_version, durable_root_version);
        assert_ne!(reopened_manifest.root.root_version, 99);
    }

    #[test]
    fn fulltext_catch_up_publishes_cover_tail_delta() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        storage_metrics().reset_for_tests();
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "late indexed graph",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 44,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();
        {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&44)
                .and_then(|state| state.manifest.as_ref())
                .expect("manifest before catch up");
            assert_eq!(manifest.tail_pending_entries.len(), 1);
            assert_eq!(manifest.tail_pending_entries[0].entry_id, TailEntryId(1));
            assert!(matches!(
                manifest.root.coverage,
                CoverageState::TailPending {
                    pending_rowsets: 1,
                    pending_segments: 1,
                    pending_rows: 1,
                    ..
                }
            ));
            assert!(manifest.root.recent_delta_files.is_empty());
            assert_eq!(manifest.root.next_tail_entry_id, TailEntryId(2));
        }

        let touched = table.search_registry().catch_up_definition(44).unwrap();
        assert_eq!(touched, 1);

        let delta_entries = load_manifest_delta_entries(&table, 44);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::FullText
                    && artifact.segment.rowset_id == 1
                    && artifact.segment.segment_id == 0
                    && matches!(artifact.location, ArtifactLocation::SidecarArtifactFile { .. })
        )));
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::StatsDelta(SearchStatsDelta::FullText(delta))
                if delta.stats.total_docs > 0
        )));

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&44)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after catch up");
        assert!(manifest.tail_pending_entries.is_empty());
        assert!(manifest.root.coverage.is_complete());
        assert_eq!(manifest.root.next_tail_entry_id, TailEntryId(2));
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        assert!(matches!(
            manifest.artifacts.artifacts[0].location,
            ArtifactLocation::SidecarArtifactFile { .. }
        ));

        let query = FullTextIndex::new_default().parse_query("graph").unwrap();
        let opened = table
            .open_fulltext_filter_cursor(0, &query, "simple", None, table.max_version())
            .expect("query catch-up sidecar fulltext artifact");
        let chunks =
            drain_search_cursor(&table, opened, &[0], false, 1).expect("materialize sidecar query");
        let mut docs = Vec::new();
        for chunk in chunks {
            let text_col = chunk.column(0).expect("text projection");
            for row in 0..chunk.size() {
                docs.push(text_col.get_string(row).expect("text value").to_string());
            }
        }
        assert_eq!(docs, vec!["late indexed graph"]);

        let metrics = storage_metrics().snapshot();
        let sidecar_build = metrics
            .search_sidecar_build_by_key
            .iter()
            .find(|series| {
                series.key
                    == crate::metrics::SearchSidecarBuildMetricKey {
                        definition_id: 44,
                        provider: SearchIndexKind::FullText,
                    }
            })
            .expect("fulltext sidecar build metric");
        assert!(sidecar_build.counters.rows_total >= 1);
        assert!(sidecar_build.counters.read_bytes_total > 0);
        assert!(sidecar_build.counters.write_bytes_total > 0);
        assert!(sidecar_build.counters.artifact_bytes_total > 0);
        assert!(
            sidecar_build
                .counters
                .latency_us_buckets
                .iter()
                .sum::<u64>()
                > 0
        );
    }

    #[test]
    fn sidecar_catch_up_publish_failure_cleans_package_and_delta_candidate() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "publish failure cleanup",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 144,
            table_id: table.tablet_id(),
            name: "docs_fts_cleanup".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();

        let root_path = table
            .search_registry()
            .manifests
            .definition_dir(144)
            .join("manifest_root_g1_v2.json");
        std::fs::create_dir(&root_path).unwrap();

        let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
        let package_path = store.package_path(SidecarArtifactStore::default_shard_file_id(144, 1));
        let delta_path = table
            .search_registry()
            .manifests
            .definition_dir(144)
            .join("delta_g1_v2_0.json");

        let err = table
            .search_registry()
            .catch_up_definition(144)
            .expect_err("root path directory must make manifest root publish fail");
        assert!(
            err.to_string()
                .contains("commit search manifest staging fragment"),
            "{err}"
        );
        assert!(
            !package_path.exists(),
            "sidecar package finalized before manifest root failure must be removed"
        );
        assert!(
            !delta_path.exists(),
            "manifest delta candidate must be removed when root publish fails"
        );
    }

    #[test]
    fn missing_sidecar_artifact_degrades_recovered_generation_to_tail_pending() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "recover missing sidecar",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 46,
            table_id: table.tablet_id(),
            name: "docs_fts_required".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::Required,
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
            .unwrap();
        assert_eq!(table.search_registry().catch_up_definition(46).unwrap(), 1);

        let sidecar_package = {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&46)
                .and_then(|state| state.manifest.as_ref())
                .expect("complete manifest before corruption");
            assert!(manifest.root.coverage.is_complete());
            assert!(manifest.tail_pending_entries.is_empty());
            let artifact = manifest
                .artifacts
                .artifacts
                .iter()
                .find(|artifact| {
                    matches!(
                        artifact.location,
                        ArtifactLocation::SidecarArtifactFile { .. }
                    )
                })
                .expect("sidecar artifact");
            let ArtifactLocation::SidecarArtifactFile { file_id, .. } = &artifact.location else {
                unreachable!("matched sidecar artifact");
            };
            SidecarArtifactStore::new(table.tablet().data_dir().clone()).package_path(*file_id)
        };
        assert!(sidecar_package.exists());
        std::fs::remove_file(&sidecar_package).unwrap();

        let descriptor = table.to_descriptor().expect("descriptor");
        drop(table);
        let reopened = reopen_table_with_root(root.path(), &[LogicalType::Varchar], &descriptor);
        reopened
            .register_search_definition(definition)
            .expect("reload required definition with missing sidecar");

        let current = reopened.search_registry().view.load();
        let state = current.definitions.get(&46).expect("recovered definition");
        let manifest = state.manifest.as_ref().expect("recovered manifest");
        assert!(
            manifest.artifacts.artifacts.is_empty(),
            "missing sidecar artifact must not remain in active artifact set"
        );
        assert_eq!(manifest.tail_pending_entries.len(), 1);
        assert!(matches!(
            state
                .generation
                .as_ref()
                .expect("recovered generation")
                .coverage,
            CoverageState::TailPending {
                pending_rowsets: 1,
                pending_segments: 1,
                pending_rows: 1,
                exact_tail_merge: true,
            }
        ));
        let capability = state.capability.as_ref().expect("recovered capability");
        assert_eq!(
            capability.capability_state(),
            SearchCapabilityState::NotQueryable {
                reason: SearchNotQueryableReason::FreshnessRequired
            }
        );
    }

    #[test]
    fn orphan_sidecar_package_is_not_recovered_as_artifact() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 47,
            table_id: table.tablet_id(),
            name: "empty_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::Required,
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
            .unwrap();

        let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
        let orphan_file_id = ArtifactFileId {
            definition_id: 47,
            generation_id: 1,
            package_index: 19,
        };
        let mut orphan = store.create_package_writer(orphan_file_id).unwrap();
        orphan
            .append_artifact(b"not referenced by manifest")
            .unwrap();
        let orphan_path = orphan.finalize().unwrap();
        assert!(orphan_path.exists());

        let descriptor = table.to_descriptor().expect("descriptor");
        drop(table);
        let reopened = reopen_table_with_root(root.path(), &[LogicalType::Varchar], &descriptor);
        reopened
            .register_search_definition(definition)
            .expect("reload definition with orphan sidecar package");

        let current = reopened.search_registry().view.load();
        let state = current.definitions.get(&47).expect("recovered definition");
        let manifest = state.manifest.as_ref().expect("recovered manifest");
        assert!(
            manifest.artifacts.artifacts.is_empty(),
            "orphan sidecar package must not become queryability evidence"
        );
        assert!(manifest.tail_pending_entries.is_empty());
        assert!(
            orphan_path.exists(),
            "manifest load must not delete orphan sidecar packages outside explicit GC"
        );
    }

    #[test]
    fn sparse_catch_up_publishes_sidecar_artifact_and_covers_tail() {
        let _metrics_guard = crate::metrics::storage_metrics_test_guard();
        storage_metrics().reset_for_tests();
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Blob]);
        table
            .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
                SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap(),
            ])]))
            .unwrap();

        let provider_config = json!({ "physical_encoding": "binary-v1" });
        let definition = SearchIndexDefinition {
            definition_id: 45,
            table_id: table.tablet_id(),
            name: "docs_sparse".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Sparse,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();

        let touched = table.search_registry().catch_up_definition(45).unwrap();
        assert_eq!(touched, 1);

        let delta_entries = load_manifest_delta_entries(&table, 45);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::Sparse
                    && artifact.segment.rowset_id == 1
                    && matches!(artifact.location, ArtifactLocation::SidecarArtifactFile { .. })
        )));
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::StatsDelta(SearchStatsDelta::Sparse(delta))
                if delta.row_count == 1 && delta.nnz == 2
        )));

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&45)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after sparse catch up");
        assert!(manifest.tail_pending_entries.is_empty());
        assert!(manifest.root.coverage.is_complete());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        assert!(matches!(
            manifest.artifacts.artifacts[0].location,
            ArtifactLocation::SidecarArtifactFile { .. }
        ));
        let provider_stats = manifest
            .root
            .generation_stats
            .sparse_provider_stats()
            .expect("sparse provider stats");
        assert_eq!(manifest.root.generation_stats.indexed_rows, 1);
        assert_eq!(provider_stats.row_count, 1);
        assert_eq!(provider_stats.nnz, 2);

        let metrics = storage_metrics().snapshot();
        let sidecar_build = metrics
            .search_sidecar_build_by_key
            .iter()
            .find(|series| {
                series.key
                    == crate::metrics::SearchSidecarBuildMetricKey {
                        definition_id: 45,
                        provider: SearchIndexKind::Sparse,
                    }
            })
            .expect("sparse sidecar build metric");
        assert!(sidecar_build.counters.rows_total >= 1);
        assert!(sidecar_build.counters.read_bytes_total > 0);
        assert!(sidecar_build.counters.write_bytes_total > 0);
        assert!(sidecar_build.counters.artifact_bytes_total > 0);
        assert!(
            sidecar_build
                .counters
                .latency_us_buckets
                .iter()
                .sum::<u64>()
                > 0
        );
    }

    #[test]
    fn maintenance_sweep_repacks_fragmented_sidecar_packages() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "first graph document",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 47,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::Opportunistic,
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();
        assert_eq!(table.search_registry().catch_up_definition(47).unwrap(), 1);

        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "second graph document",
            ])]))
            .unwrap();
        assert_eq!(table.search_registry().catch_up_definition(47).unwrap(), 1);

        let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
        let package_0 = store.package_path(ArtifactFileId {
            definition_id: 47,
            generation_id: 1,
            package_index: 0,
        });
        let package_1 = store.package_path(ArtifactFileId {
            definition_id: 47,
            generation_id: 1,
            package_index: 1,
        });
        assert!(package_0.exists());
        assert!(package_1.exists());

        let before = table.search_registry().view.load();
        let before_manifest = before
            .definitions
            .get(&47)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest before sidecar repack");
        assert!(super::super::maintenance::sidecar_repack_needed(
            before_manifest
        ));

        let report = table.search_registry().maintenance_sweep().unwrap();
        let definition_report = report
            .definitions
            .iter()
            .find(|definition| definition.definition_id == 47)
            .expect("sidecar repack report");
        assert_eq!(
            definition_report.action,
            SearchMaintenanceAction::RepackSidecar
        );
        assert!(definition_report.sidecar_repack_requested);
        assert!(report.sidecar_repack_requested);

        let after = table.search_registry().view.load();
        let after_manifest = after
            .definitions
            .get(&47)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after sidecar repack");
        assert_eq!(after_manifest.artifacts.artifacts.len(), 2);
        assert!(!super::super::maintenance::sidecar_repack_needed(
            after_manifest
        ));
        let package_ids = after_manifest
            .artifacts
            .artifacts
            .iter()
            .map(|artifact| match artifact.location {
                ArtifactLocation::SidecarArtifactFile { file_id, .. } => file_id,
                ArtifactLocation::Inline { .. } => panic!("expected sidecar artifact after repack"),
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(package_ids.len(), 1);
        let repacked_file = package_ids.iter().next().copied().unwrap();
        assert_eq!(repacked_file.package_index, 2);
        assert!(store.package_path(repacked_file).exists());
        assert!(!package_0.exists());
        assert!(!package_1.exists());
    }

    #[test]
    fn maintenance_report_carries_cost_benefit_for_tail_catch_up() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "late indexed graph",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 46,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();

        let report = table.search_registry().maintenance_sweep().unwrap();
        assert_eq!(report.definitions_updated, 1);
        assert_eq!(report.catch_up_rowsets, 1);
        let definition_report = report
            .definitions
            .iter()
            .find(|definition| definition.definition_id == 46)
            .expect("definition maintenance report");

        assert_eq!(definition_report.action, SearchMaintenanceAction::CatchUp);
        assert_eq!(definition_report.tail_pending_rowsets, 1);
        assert_eq!(definition_report.tail_pending_rows, 1);
        assert_eq!(
            definition_report
                .estimate
                .benefit
                .expected_tail_rows_drained,
            1
        );
        assert!(definition_report.estimate.cost.cpu_ns > 0);
        assert!(definition_report.estimate.cost.publish_bytes > 0);
    }

    #[test]
    fn maintenance_sweep_reports_and_compacts_manifest_delta_window() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 47,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
                "manifest compact",
            ])]))
            .unwrap();

        {
            let current = table.search_registry().view.load();
            let state = current.definitions.get(&47).expect("definition state");
            let manifest = state.manifest.as_ref().expect("manifest");
            let mut root = manifest.root.clone();
            let store = &table.search_registry().manifests;
            for ordinal in 0..=DELTA_COUNT_SOFT_LIMIT {
                let delta_ref = store
                    .write_delta(
                        47,
                        root.generation_id,
                        root.root_version,
                        ordinal,
                        &ManifestDelta::default(),
                    )
                    .expect("write synthetic delta");
                root.recent_delta_files.push(delta_ref);
            }
            root.recompute_checksum().unwrap();
            store.write_root(47, &root).unwrap();
        }
        {
            let loaded = table
                .search_registry()
                .manifests
                .load_manifest(47)
                .expect("load synthetic over-soft manifest")
                .expect("manifest exists");
            let current = table.search_registry().view.load_full();
            let state = current
                .definitions
                .get(&47)
                .expect("definition state")
                .clone()
                .with_manifest(loaded);
            let mut next = (*current).clone();
            next.version = next.version.saturating_add(1);
            next.definitions.insert(47, state);
            table.search_registry().view.store(Arc::new(next));
        }

        let report = table.search_registry().maintenance_sweep().unwrap();
        let definition_report = report
            .definitions
            .iter()
            .find(|definition| definition.definition_id == 47)
            .expect("definition report");
        assert!(report.manifest_delta_compaction_requested);
        assert!(definition_report.manifest_delta_compaction_requested);
        assert_eq!(
            definition_report.action,
            SearchMaintenanceAction::CompactManifestDelta
        );

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&47)
            .and_then(|state| state.manifest.as_ref())
            .expect("compacted manifest");
        assert!(manifest.root.recent_delta_files.is_empty());
        let compacted = table.search_registry().compact_manifest_deltas().unwrap();
        assert_eq!(compacted, 0);
    }

    #[test]
    fn fulltext_rowset_replacement_publishes_remove_artifact_delta() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 45,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
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
                "vector one",
            ])]))
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "vector two",
            ])]))
            .unwrap();

        {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&45)
                .and_then(|state| state.manifest.as_ref())
                .expect("manifest before compaction");
            let rowset_ids = manifest
                .artifacts
                .artifacts
                .iter()
                .map(|artifact| artifact.segment.rowset_id)
                .collect::<BTreeSet<_>>();
            assert_eq!(rowset_ids, BTreeSet::from([1, 2]));
        }

        assert!(
            table.optimize_compact().unwrap(),
            "expected compaction output"
        );
        table.search_registry().ensure_fresh();

        let delta_entries = load_manifest_delta_entries(&table, 45);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::RemoveArtifact(segment)
                if segment.rowset_id == 1 && segment.segment_id == 0
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::RemoveArtifact(segment)
                if segment.rowset_id == 2 && segment.segment_id == 0
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::FullText
                    && artifact.segment.rowset_id != 1
                    && artifact.segment.rowset_id != 2
        )));

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&45)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after compaction");
        assert!(manifest
            .artifacts
            .artifacts
            .iter()
            .all(|artifact| artifact.segment.rowset_id != 1 && artifact.segment.rowset_id != 2));
        assert_eq!(manifest.root.generation_stats.indexed_rows, 2);
        assert_eq!(
            manifest
                .root
                .generation_stats
                .fulltext_provider_stats()
                .expect("fulltext stats")
                .total_docs,
            2
        );

        let query = FullTextIndex::new_default().parse_query("vector").unwrap();
        let opened = table
            .open_fulltext_filter_cursor(0, &query, "simple", None, table.max_version())
            .unwrap();
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
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
        let mut row_count = 0usize;
        loop {
            match cursor.next_batch(&batch, &mut budget).unwrap() {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => {
                    row_count += table
                        .materialize_search_batch(&snapshot, batch, &[0], false)
                        .unwrap()
                        .size();
                }
                SearchBatchState::Exhausted => break,
            }
        }
        assert_eq!(row_count, 2);
    }

    #[test]
    fn compaction_output_absorbs_tail_pending_fulltext_rowsets() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "tail one",
            ])]))
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "tail two",
            ])]))
            .unwrap();

        let provider_config = json!({"config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 91,
            table_id: table.tablet_id(),
            name: "docs_fts".to_string(),
            kind: SearchIndexKind::FullText,
            column_ids: vec![0],
            expression: Some("to_tsvector('simple', col_0)".to_string()),
            freshness_policy: SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 100,
                max_lag_millis: 0,
            },
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::FullText,
                &[0],
                Some("to_tsvector('simple', col_0)"),
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();

        {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&91)
                .and_then(|state| state.manifest.as_ref())
                .expect("tail-pending manifest");
            assert!(manifest.artifacts.artifacts.is_empty());
            assert_eq!(manifest.tail_pending_entries.len(), 2);
        }

        assert!(
            table.optimize_compact().unwrap(),
            "expected compaction output"
        );
        table.search_registry().ensure_fresh();

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&91)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after compaction absorption");
        assert!(
            manifest.tail_pending_entries.is_empty(),
            "compaction output should cover input tail entries instead of leaving catch-up work"
        );
        assert!(manifest.root.coverage.is_complete());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        let output_artifact = &manifest.artifacts.artifacts[0];
        assert_eq!(output_artifact.kind, SearchIndexKind::FullText);
        assert_ne!(output_artifact.segment.rowset_id, 1);
        assert_ne!(output_artifact.segment.rowset_id, 2);
        assert!(matches!(
            output_artifact.location,
            ArtifactLocation::Inline { .. }
        ));

        let delta_entries = load_manifest_delta_entries(&table, 91);
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(2)))));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::FullText
                    && artifact.segment.rowset_id == output_artifact.segment.rowset_id
        )));
        assert_eq!(manifest.root.generation_stats.indexed_rows, 2);
        assert_eq!(
            manifest
                .root
                .generation_stats
                .fulltext_provider_stats()
                .expect("fulltext stats")
                .total_docs,
            2
        );
        assert_eq!(table.search_registry().catch_up_definition(91).unwrap(), 0);
    }

    #[test]
    fn compaction_output_absorbs_tail_pending_sparse_rowsets() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Blob]);
        table
            .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
                SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap(),
            ])]))
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_sparse_blob_vector(&[
                SparseVector::new(vec![1, 2], vec![0.7, 0.2]).unwrap(),
            ])]))
            .unwrap();

        let provider_config = json!({ "physical_encoding": "binary-v1" });
        let definition = SearchIndexDefinition {
            definition_id: 94,
            table_id: table.tablet_id(),
            name: "docs_sparse".to_string(),
            kind: SearchIndexKind::Sparse,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::BoundedLag {
                max_tail_rows: 100,
                max_lag_millis: 0,
            },
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Sparse,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();

        {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&94)
                .and_then(|state| state.manifest.as_ref())
                .expect("tail-pending manifest");
            assert!(manifest.artifacts.artifacts.is_empty());
            assert_eq!(manifest.tail_pending_entries.len(), 2);
        }

        assert!(
            table.optimize_compact().unwrap(),
            "expected compaction output"
        );
        table.search_registry().ensure_fresh();

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&94)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after compaction absorption");
        assert!(manifest.tail_pending_entries.is_empty());
        assert!(manifest.root.coverage.is_complete());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        let output_artifact = &manifest.artifacts.artifacts[0];
        assert_eq!(output_artifact.kind, SearchIndexKind::Sparse);
        assert_ne!(output_artifact.segment.rowset_id, 1);
        assert_ne!(output_artifact.segment.rowset_id, 2);
        assert!(matches!(
            output_artifact.location,
            ArtifactLocation::Inline { .. }
        ));

        let delta_entries = load_manifest_delta_entries(&table, 94);
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(2)))));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::Sparse
                    && artifact.segment.rowset_id == output_artifact.segment.rowset_id
        )));
        assert_eq!(manifest.root.generation_stats.indexed_rows, 2);
        let provider_stats = manifest
            .root
            .generation_stats
            .sparse_provider_stats()
            .expect("sparse stats");
        assert_eq!(provider_stats.row_count, 2);
        assert_eq!(provider_stats.nnz, 4);
        assert_eq!(table.search_registry().catch_up_definition(94).unwrap(), 0);
    }

    #[test]
    fn shared_fulltext_payload_definitions_replay_compaction_output_once() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"config": "simple"});

        for definition_id in [92, 93] {
            let definition = SearchIndexDefinition {
                definition_id,
                table_id: table.tablet_id(),
                name: format!("docs_fts_{definition_id}"),
                kind: SearchIndexKind::FullText,
                column_ids: vec![0],
                expression: Some("to_tsvector('simple', col_0)".to_string()),
                freshness_policy: SearchFreshnessPolicy::Required,
                config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                    SearchIndexKind::FullText,
                    &[0],
                    Some("to_tsvector('simple', col_0)"),
                    &provider_config,
                ),
                provider_config: provider_config.clone(),
            };
            table.register_search_definition(definition).unwrap();
        }

        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "shared payload one",
            ])]))
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "shared payload two",
            ])]))
            .unwrap();

        {
            let current = table.search_registry().view.load();
            for definition_id in [92, 93] {
                let manifest = current
                    .definitions
                    .get(&definition_id)
                    .and_then(|state| state.manifest.as_ref())
                    .expect("manifest before compaction");
                assert_eq!(manifest.artifacts.artifacts.len(), 2);
                assert!(manifest.tail_pending_entries.is_empty());
            }
        }

        assert!(
            table.optimize_compact().unwrap(),
            "expected compaction output"
        );
        table.search_registry().ensure_fresh();

        let current = table.search_registry().view.load();
        let mut output_locations = Vec::new();
        for definition_id in [92, 93] {
            let manifest = current
                .definitions
                .get(&definition_id)
                .and_then(|state| state.manifest.as_ref())
                .expect("manifest after compaction");
            assert!(manifest.root.coverage.is_complete());
            assert!(manifest.tail_pending_entries.is_empty());
            assert_eq!(manifest.artifacts.artifacts.len(), 1);
            let artifact = &manifest.artifacts.artifacts[0];
            assert_ne!(artifact.segment.rowset_id, 1);
            assert_ne!(artifact.segment.rowset_id, 2);
            let ArtifactLocation::Inline { page } = artifact.location else {
                panic!("expected inline compaction output artifact");
            };
            assert_ne!(page.checksum, 0);
            output_locations.push((
                page.rowset_id,
                page.segment_id,
                page.column_id,
                page.page_offset,
                page.page_len,
            ));
        }
        assert_eq!(
            output_locations[0], output_locations[1],
            "shared physical payload definitions should replay the same compaction output page"
        );
    }

    #[test]
    fn hnsw_tail_pending_delta_records_upsert_tail_entry() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Integer]);
        let provider_config = json!({
            "m": 16,
            "ef_construct": 64,
            "distance": "l2",
        });
        let definition = SearchIndexDefinition {
            definition_id: 88,
            table_id: table.tablet_id(),
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };

        table.register_search_definition(definition).unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[10])]))
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[20])]))
            .unwrap();

        let delta_entries = load_manifest_delta_entries(&table, 88);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::UpsertTail(tail)
                if tail.entry_id == TailEntryId(1)
                    && tail.rowset_id == 1
                    && tail.segment_ids == vec![0]
                    && tail.mutation == TailMutationKind::Append
                    && tail.row_count == 1
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::UpsertTail(tail)
                if tail.entry_id == TailEntryId(2)
                    && tail.rowset_id == 2
                    && tail.segment_ids == vec![0]
                    && tail.mutation == TailMutationKind::Append
                    && tail.row_count == 1
        )));
        assert!(!delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::AddArtifact(_))));

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&88)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest");
        assert!(matches!(
            manifest.root.coverage,
            CoverageState::TailPending {
                pending_rowsets: 2,
                pending_segments: 2,
                pending_rows: 2,
                ..
            }
        ));
    }

    #[test]
    fn hnsw_tail_pending_maintenance_report_carries_provider_request() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Integer]);
        let provider_config = json!({
            "m": 16,
            "ef_construct": 64,
            "distance": "l2",
            "dimension": 4,
        });
        let definition = SearchIndexDefinition {
            definition_id: 90,
            table_id: table.tablet_id(),
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };

        table.register_search_definition(definition).unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[10])]))
            .unwrap();

        let report = table.search_registry().maintenance_sweep().unwrap();
        let definition_report = report
            .definitions
            .iter()
            .find(|definition| definition.definition_id == 90)
            .expect("hnsw definition report");
        let request = definition_report
            .provider_request
            .as_ref()
            .and_then(ProviderMaintenanceRequest::as_hnsw)
            .expect("hnsw provider maintenance request");
        assert_eq!(request.definition_id, 90);
        assert_eq!(request.dimension, 4);
        assert_eq!(request.tail_window.len(), 1);
        assert_eq!(request.rowset_refs.len(), 1);
        assert_eq!(request.rowset_refs[0].row_count, 1);
        assert_eq!(
            request.freshness_priority, definition_report.priority,
            "request should carry scheduler freshness priority"
        );
        assert!(request.estimated_graph_memory_bytes > 0);
    }

    #[test]
    fn hnsw_tail_pending_maintenance_sweep_consumes_request_and_publishes_artifact() {
        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 2)],
        );
        table.bind_search_task_scheduler(Some(Arc::new(TaskScheduler::new())));
        let provider_config = json!({
            "m": 8,
            "ef_construct": 32,
            "distance": "l2",
            "dimension": 2,
        });
        let definition = SearchIndexDefinition {
            definition_id: 94,
            table_id: table.tablet_id(),
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };
        table.register_search_definition(definition).unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![1.0, 0.0], vec![0.0, 1.0]],
                2,
            )]))
            .unwrap();

        {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&94)
                .and_then(|state| state.manifest.as_ref())
                .expect("tail-pending manifest");
            assert!(manifest.artifacts.artifacts.is_empty());
            assert_eq!(manifest.tail_pending_entries.len(), 1);
        }

        let report = table.search_registry().maintenance_sweep().unwrap();
        assert_eq!(report.definitions_updated, 1);
        assert_eq!(report.catch_up_rowsets, 1);
        let definition_report = report
            .definitions
            .iter()
            .find(|definition| definition.definition_id == 94)
            .expect("hnsw definition report");
        assert!(definition_report
            .provider_request
            .as_ref()
            .and_then(ProviderMaintenanceRequest::as_hnsw)
            .is_some());

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&94)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after hnsw catch-up");
        assert!(manifest.tail_pending_entries.is_empty());
        assert!(manifest.root.coverage.is_complete());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        let artifact = &manifest.artifacts.artifacts[0];
        assert_eq!(artifact.kind, SearchIndexKind::Hnsw);
        assert!(matches!(
            artifact.location,
            ArtifactLocation::SidecarArtifactFile { .. }
        ));
        let provider_stats = manifest
            .root
            .generation_stats
            .hnsw_provider_stats()
            .expect("hnsw provider stats");
        assert_eq!(provider_stats.vector_count, 2);
        assert_eq!(provider_stats.dimension, 2);

        let delta_entries = load_manifest_delta_entries(&table, 94);
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact) if artifact.kind == SearchIndexKind::Hnsw
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::StatsDelta(SearchStatsDelta::Hnsw(delta))
                if delta.vector_count == 2 && delta.dimension == 2
        )));

        let rowsets = table
            .tablet()
            .capture_consistent_rowsets(table.max_version())
            .unwrap();
        let segment = rowsets[0].segments()[0].clone();
        assert!(
            segment.hnsw_index(0).is_none(),
            "HNSW TailOnly catch-up must not patch published segment footers"
        );

        let opened = table
            .open_vector_search_cursor(
                0,
                &[1.0, 0.0],
                2,
                SearchParams::default(),
                None,
                table.max_version(),
            )
            .unwrap();
        let mut cursor = opened.cursor;
        let batch = SearchBatchConfig {
            row_limit: 16,
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget {
            memory_limit_bytes: 64 * 1024 * 1024,
            heap_budget_items: 16,
            parallelism_slots: 2,
            cpu_step_budget: None,
            context: None,
        };
        let mut returned = 0usize;
        while let SearchBatchState::Ready(batch) = cursor.next_batch(&batch, &mut budget).unwrap() {
            returned += batch.len();
        }
        assert_eq!(returned, 2);
    }

    #[test]
    fn hnsw_full_snapshot_stores_tail_in_shard_not_root() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Integer]);
        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[10])]))
            .unwrap();

        let provider_config = json!({
            "m": 16,
            "ef_construct": 64,
            "distance": "l2",
        });
        let definition = SearchIndexDefinition {
            definition_id: 89,
            table_id: table.tablet_id(),
            name: "vec_hnsw".to_string(),
            kind: SearchIndexKind::Hnsw,
            column_ids: vec![0],
            expression: None,
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Hnsw),
            config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
                SearchIndexKind::Hnsw,
                &[0],
                None,
                &provider_config,
            ),
            provider_config,
        };

        table.register_search_definition(definition).unwrap();
        {
            let current = table.search_registry().view.load();
            let manifest = current
                .definitions
                .get(&89)
                .and_then(|state| state.manifest.as_ref())
                .expect("manifest");
            assert_eq!(manifest.tail_pending_entries.len(), 1);
            assert_eq!(manifest.tail_pending_entries[0].entry_id, TailEntryId(1));
            assert_eq!(manifest.root.next_tail_entry_id, TailEntryId(2));
            assert!(manifest.root.recent_delta_files.is_empty());
            assert_eq!(manifest.root.shard_files.len(), 1);

            let definition_dir = table.search_registry().manifests.definition_dir(89);
            let root_bytes = std::fs::read(&manifest.root_path).expect("read root");
            let root_json: serde_json::Value =
                serde_json::from_slice(&root_bytes).expect("decode root json");
            assert!(
                root_json.get("tail_pending_entries").is_none(),
                "manifest root must stay small and must not duplicate compacted tail entries"
            );

            let shard_bytes =
                std::fs::read(definition_dir.join(&manifest.root.shard_files[0].file_name))
                    .expect("read shard");
            let shard: ManifestShard = serde_json::from_slice(&shard_bytes).expect("decode shard");
            assert_eq!(shard.tail_pending_entries.len(), 1);
            assert_eq!(shard.tail_pending_entries[0].entry_id, TailEntryId(1));
            assert_eq!(shard.tail_pending_entries[0].rowset_id, 1);
        }

        table
            .append(&test_chunk_from_vectors(vec![test_i32_vector(&[20])]))
            .unwrap();

        let delta_entries = load_manifest_delta_entries(&table, 89);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::UpsertTail(tail)
                if tail.entry_id == TailEntryId(2)
                    && tail.rowset_id == 2
                    && tail.segment_ids == vec![0]
                    && tail.mutation == TailMutationKind::Append
                    && tail.row_count == 1
        )));
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&89)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after delta");
        assert_eq!(manifest.tail_pending_entries.len(), 2);
        assert_eq!(manifest.root.next_tail_entry_id, TailEntryId(3));

        let root_bytes = std::fs::read(&manifest.root_path).expect("read root");
        let root_json: serde_json::Value =
            serde_json::from_slice(&root_bytes).expect("decode root json");
        assert!(root_json.get("tail_pending_entries").is_none());
    }
}
