// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Instant;

use arc_swap::ArcSwap;
use paro_scheduler::scheduler::TaskScheduler;

use crate::metrics::storage_metrics;
use crate::rowset::{RowsetId, RowsetSharedPtr};
use crate::tablet::{
    ColumnId, RowsetPublishObserver, SearchGenerationHeadUpdates, TabletId, TabletRef,
};
use paro_common::effect::ArtifactRef;
use paro_common::error::{self as paro_error, Result};

use super::artifact::{ArtifactFileId, ArtifactGcContext, ArtifactLocation, GcDecision};
use super::capability::{
    ArtifactSegmentRef, CapabilityToken, SearchArtifactRef, SearchCapability,
    SearchDefinitionOrigin, SearchIndexDefinition, SearchIndexKind,
};
use super::cursor::{GenerationArtifactSet, GenerationReadSnapshot, OpenSearchCursorResult};
use super::definition::freshness::capability_needs_required_freshness_wait;
use super::definition::origin::{hnsw_schema_seed_definitions, restored_schema_seed_definition};
use super::definition::validation::validate_definition;
use super::generation::coverage::{search_generation_coverage_for_state, SearchGenerationCoverage};
use super::generation::head::{
    head_for_state, publish_head_for_state, SearchGenerationPublishCompletion,
};
use super::generation::maintenance_state::build_maintenance_state;
use super::generation::snapshot::{
    collect_full_rebuild_tail, collect_rowset_snapshot, collect_visible_snapshot,
    RowsetSearchSnapshot,
};
use super::generation::stats::{
    empty_generation_stats_for_definition, generation_stats_after_artifact_replacement,
    generation_stats_from_artifacts, stats_deltas_from_generation_stats,
};
use super::generation::tail_entries::{
    artifact_segment_column_keys, assign_tail_entry_ids, assign_tail_entry_ids_for_full_snapshot,
    tail_entry_already_live, tail_entry_is_covered_by_artifacts,
};
use super::generation::view::{
    coverage_for_definition, execution_modes_for_definition, generation_read_snapshot,
    indexed_through_ts, record_tail_metrics_for_state, SearchDefinitionState, SearchView,
};
use super::inline_sink::{
    BuildBudget, SearchAdmission, SearchBuildStopCheck, SearchInlineBuilderSet,
    SidecarArtifactBuilder, SidecarBuildInput,
};
use super::lifecycle::bootstrap::SearchBootstrapReport;
use super::lifecycle::gc::gc_policy_for_kind;
use super::lifecycle::maintenance_request::provider_maintenance_request_for_definition;
use super::lifecycle::publisher::{
    assign_generation_id, remove_sidecar_packages, retire_paths_for_manifest, search_artifact_key,
    sidecar_file_ids_for_artifacts,
};
use super::maintenance::{
    CatchUpPlanner, DefinitionMaintenanceReport, InlineSearchAdmission, MaintenanceScheduler,
    SearchMaintenanceAction, SearchMaintenanceReport,
};
use super::manifest::{
    GenerationManifestRoot, LoadedManifest, ManifestDelta, ManifestDeltaEntry, ManifestShard,
    ManifestStore,
};
use super::sidecar::{SearchReaderRuntime, SidecarArtifactStore};
use super::sidecar_builder::ProviderSidecarArtifactBuilder;
use super::staged_generation::{StagedSearchGeneration, StagedSearchGenerationInit};
use super::stats::MaintenancePriority;
use super::tail::{
    TailEntryId, TailMutationKind, TailPendingEntry, TailPendingSet, TailRowImageRef,
};
use super::write_path::SearchWriteContext;

const REQUIRED_FRESHNESS_WAIT_SWEEPS: usize = 32;
const DEFINITION_LOCK_SHARDS: usize = 64;

#[derive(Debug)]
struct RetiredManifest {
    definition_id: u64,
    provider: SearchIndexKind,
    artifacts: Arc<GenerationArtifactSet>,
    sidecar_file_ids: BTreeSet<ArtifactFileId>,
    paths: Vec<PathBuf>,
    retired_at: Instant,
}

pub(crate) struct SearchIndexRegistry {
    tablet: TabletRef,
    manifests: ManifestStore,
    /// Readers load immutable snapshots without taking a lock.
    view: ArcSwap<SearchView>,
    /// Serializes only the final copy-on-write publication into `view`.
    view_write_lock: Mutex<()>,
    /// Serializes definition membership changes (install/drop/seed replacement).
    lifecycle_lock: Mutex<()>,
    /// Bounded per-definition exclusion for manifest/head work. Lifecycle code locks
    /// shards in ascending order before taking `view_write_lock`.
    definition_locks: [Mutex<()>; DEFINITION_LOCK_SHARDS],
    /// Single-flight ownership for expensive snapshot rebuilds. It is separate
    /// from publication locks so DML can continue while provider work runs.
    /// No path acquires this lock after a publication lock.
    definition_build_locks: [Mutex<()>; DEFINITION_LOCK_SHARDS],
    retired: Mutex<Vec<RetiredManifest>>,
    /// Long-lived mmap and decoded-reader owner. Query cursors borrow this
    /// runtime; generation retirement performs lease-safe physical eviction.
    reader_runtime: Arc<SearchReaderRuntime>,
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
    ) -> Result<SearchGenerationHeadUpdates> {
        if tablet_id != self.tablet.tablet_id() {
            return Ok(SearchGenerationHeadUpdates::default());
        }
        self.prepare_heads_for_visible_rowsets(version, visible_rowsets)
    }

    fn rowset_published(
        &self,
        tablet_id: TabletId,
        version: i64,
        rowset: RowsetSharedPtr,
        search_updates: SearchGenerationHeadUpdates,
    ) {
        if tablet_id != self.tablet.tablet_id() {
            return;
        }
        let (prepared, stale_definition_ids) = search_updates.into_parts();
        for definition_id in stale_definition_ids {
            if let Err(error) = self.disable_definition_capability(definition_id) {
                tracing::warn!(
                    tablet_id,
                    definition_id,
                    rowset_id = rowset.rowset_id(),
                    version,
                    error = %error,
                    "failed to disable search capability after rowset manifest preparation was rejected"
                );
            }
        }
        for (head, manifest) in prepared {
            let definition_id = head.definition_id;
            if self.tablet.search_generation_head(definition_id).as_ref() != Some(&head) {
                // A newer publication won the race after the rowset commit.
                // Its callback (or recovery reconciliation) owns the view.
                continue;
            }
            let result = (|| -> Result<()> {
                let definition_lock = self.definition_lock(definition_id);
                let _guard = definition_lock
                    .lock()
                    .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
                let current = self.view.load_full();
                let Some(state) = current.definitions.get(&definition_id).cloned() else {
                    return Ok(());
                };
                drop(current);
                if head_for_state(&self.manifests, &state).as_ref() == Some(&head) {
                    return Ok(());
                }
                let next_state = state.clone().with_manifest(manifest);
                if head_for_state(&self.manifests, &next_state).as_ref() != Some(&head) {
                    return Err(paro_error::data_corrupted(format!(
                        "prepared search manifest for definition {definition_id} does not match accepted tablet head"
                    )));
                }
                self.publish_definition_state(&state, next_state.clone())?;
                if let Some(next_manifest) = next_state.manifest.as_ref() {
                    self.retire_manifest_replaced_by(
                        state.definition.kind,
                        state.manifest.as_ref(),
                        next_manifest,
                    );
                }
                record_tail_metrics_for_state(&next_state);
                Ok(())
            })();
            if let Err(error) = result {
                if let Err(disable_error) = self.disable_definition_capability(definition_id) {
                    tracing::error!(
                        tablet_id,
                        definition_id,
                        error = %disable_error,
                        "failed to disable stale search capability after prepared manifest install failure"
                    );
                }
                tracing::warn!(
                    tablet_id,
                    definition_id,
                    rowset_id = rowset.rowset_id(),
                    version,
                    error = %error,
                    "failed to install prepared search manifest after rowset publish"
                );
            }
        }
        self.sweep_retired();
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
    fn disable_definition_capability(&self, definition_id: u64) -> Result<()> {
        self.mutate_view(|view| {
            let Some(state) = view.definitions.get_mut(&definition_id) else {
                return Ok((false, ()));
            };
            if state.capability.take().is_none() {
                return Ok((false, ()));
            }
            Ok((true, ()))
        })
    }

    pub(crate) fn new(tablet: TabletRef) -> Self {
        let reader_runtime = Arc::new(SearchReaderRuntime::new(SidecarArtifactStore::new(
            tablet.data_dir().clone(),
        )));
        let registry = Self {
            manifests: ManifestStore::new(tablet.data_dir().to_path_buf()),
            tablet,
            view: ArcSwap::from_pointee(SearchView::default()),
            view_write_lock: Mutex::new(()),
            lifecycle_lock: Mutex::new(()),
            definition_locks: std::array::from_fn(|_| Mutex::new(())),
            definition_build_locks: std::array::from_fn(|_| Mutex::new(())),
            retired: Mutex::new(Vec::new()),
            reader_runtime,
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

    /// Sweep pre-commit workspaces only after WAL replay has consumed every
    /// committed `PublishSearchGeneration` mutation. Running this during table
    /// construction could delete the sole source directory needed by replay.
    pub(crate) fn sweep_orphan_generation_state(
        &self,
    ) -> Result<super::SearchGenerationOrphanSweepReport> {
        Ok(super::SearchGenerationOrphanSweepReport {
            staging_workspaces: self.manifests.sweep_orphan_generation_workspaces()?,
            manifest_fragments: self
                .manifests
                .sweep_unpublished_installed_revisions(&self.tablet.search_generation_heads())?,
        })
    }

    pub(crate) fn bind_task_scheduler(&self, scheduler: Option<Arc<TaskScheduler>>) {
        *self.hnsw_task_scheduler.write().unwrap() = scheduler;
    }

    pub(crate) fn bind_hnsw_integrity_scheduler(
        &self,
        scheduler: Option<Arc<crate::index::hnsw::HnswIntegrityScheduler>>,
    ) -> Result<()> {
        self.reader_runtime.bind_hnsw_integrity_scheduler(scheduler)
    }

    pub(crate) fn reader_runtime(&self) -> Arc<SearchReaderRuntime> {
        Arc::clone(&self.reader_runtime)
    }

    fn hnsw_task_scheduler(&self) -> Option<Arc<TaskScheduler>> {
        self.hnsw_task_scheduler.read().unwrap().clone()
    }

    fn load_manifest_for_definition(&self, definition_id: u64) -> Result<Option<LoadedManifest>> {
        let Some(head) = self.tablet.search_generation_head(definition_id) else {
            return Ok(None);
        };
        self.manifests
            .load_manifest_for_head(&head)?
            .ok_or_else(|| {
                paro_error::data_corrupted(format!(
                    "durable search generation head for definition {definition_id} has no manifest"
                ))
            })
            .map(Some)
    }

    pub(crate) fn install_definition(&self, definition: SearchIndexDefinition) -> Result<()> {
        self.update_definition(
            definition.clone(),
            SearchDefinitionOrigin::catalog(definition.definition_id),
        )
    }

    /// Build a complete immutable generation without making its definition
    /// visible. The returned owner retains an exclusive physical-layout lease
    /// until transaction publish or abort.
    pub(crate) fn stage_definition_generation(
        &self,
        definition: SearchIndexDefinition,
        txn_id: u64,
        stop_check: SearchBuildStopCheck,
    ) -> Result<StagedSearchGeneration> {
        validate_definition(&definition, &self.tablet)?;
        stop_check.check()?;
        let layout_lease = self
            .tablet
            .acquire_stable_layout_lease(txn_id, || stop_check.should_stop())?;
        stop_check.check()?;

        let snapshot_version = self.tablet.max_version();
        let visible_rowsets = self.tablet.capture_consistent_rowsets(snapshot_version)?;
        let tail_window = collect_full_rebuild_tail(snapshot_version, &visible_rowsets)?;
        let generation_id = 1;
        let staging_root = self.manifests.staged_generation_workspace(
            txn_id,
            definition.definition_id,
            generation_id,
        );
        if staging_root.exists() {
            return Err(paro_error::object_exists(
                "search generation staging directory",
                staging_root.display().to_string(),
            ));
        }

        let staged_manifests = ManifestStore::new(staging_root.clone());
        let sidecar_store = SidecarArtifactStore::new(staging_root.clone());
        let builder = ProviderSidecarArtifactBuilder::new(sidecar_store);
        let input = SidecarBuildInput {
            definition: definition.clone(),
            generation_id,
            tail_window,
            rowset_refs: visible_rowsets.clone(),
            snapshot_version,
            stop_check: Some(stop_check.clone()),
        };
        let build_result = (|| {
            let estimate = builder.estimate_cost(&input)?;
            let result = builder.build(
                input,
                &BuildBudget {
                    cost_envelope: estimate.cost,
                    deadline: None,
                    grant_id: None,
                },
            )?;
            stop_check.check()?;
            validate_staged_artifact_coverage(
                &definition,
                generation_id,
                &visible_rowsets,
                &result.artifact_refs,
            )?;

            let visible_snapshot =
                collect_visible_snapshot(&definition, snapshot_version, &visible_rowsets)?;
            let mut delete_tail = visible_snapshot
                .tail_pending
                .entries
                .into_iter()
                .filter(|entry| entry.mutation == TailMutationKind::Delete)
                .collect::<Vec<_>>();
            let next_tail_entry_id =
                assign_tail_entry_ids_for_full_snapshot(&mut delete_tail, None);
            let tail_pending = TailPendingSet {
                entries: delete_tail,
            };
            let coverage = coverage_for_definition(&definition, &tail_pending);
            let generation_stats =
                generation_stats_from_artifacts(&definition, &result.artifact_refs)?;
            let execution_modes = execution_modes_for_definition(&definition, &coverage);
            let mut root = GenerationManifestRoot {
                definition_id: definition.definition_id,
                generation_id,
                build_epoch: 1,
                build_snapshot_version: snapshot_version,
                indexed_through_ts: indexed_through_ts(snapshot_version),
                config_fingerprint: definition.config_fingerprint,
                coverage: coverage.clone(),
                generation_stats: generation_stats.clone(),
                persisted_tail_entry_id_seed: next_tail_entry_id,
                execution_modes,
                maintenance_state: build_maintenance_state(
                    &definition,
                    snapshot_version,
                    1,
                    generation_stats.indexed_rows,
                    &tail_pending,
                    tail_pending.delete_rows(),
                    None,
                    Vec::new(),
                ),
                root_version: 1,
                checksum: 0,
                shard_files: Vec::new(),
                recent_delta_files: Vec::new(),
            };
            let artifact_set = GenerationArtifactSet::try_new(result.artifact_refs)?;
            root.shard_files.push(staged_manifests.write_shard(
                definition.definition_id,
                generation_id,
                root.root_version,
                &ManifestShard {
                    artifact_refs: artifact_set.artifacts,
                    tail_pending_entries: tail_pending.entries,
                },
            )?);
            root.recompute_checksum()?;
            staged_manifests.write_root(definition.definition_id, &root)?;
            let loaded = staged_manifests
                .load_latest_manifest_for_private_workspace(definition.definition_id)?
                .ok_or_else(|| paro_error::internal("staged search manifest disappeared"))?;
            if loaded.root != root {
                return Err(paro_error::data_corrupted(
                    "staged search manifest changed during self-verification",
                ));
            }
            let indexed_segment_count = expected_segment_rows(&visible_rowsets)?.len();
            Ok((
                SearchGenerationCoverage {
                    visible_version: snapshot_version,
                    indexed_through_ts: indexed_through_ts(snapshot_version),
                    visible_segment_count: indexed_segment_count,
                    indexed_segment_count,
                    coverage,
                },
                staged_manifests.head_for_root(&root),
            ))
        })();

        let (coverage, head) = match build_result {
            Ok(result) => result,
            Err(error) => {
                match fs::remove_dir_all(&staging_root) {
                    Ok(()) => {}
                    Err(cleanup_error) if cleanup_error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(cleanup_error) => {
                        tracing::warn!(
                            path = %staging_root.display(),
                            error = %cleanup_error,
                            "failed to remove aborted search-generation workspace"
                        );
                    }
                }
                return Err(error);
            }
        };
        let staging_generation_dir =
            staged_manifests.generation_dir(definition.definition_id, generation_id);
        let final_manifests = ManifestStore::new(self.tablet.data_dir().to_path_buf());
        let generation_ref =
            final_manifests.generation_ref(definition.definition_id, generation_id)?;
        Ok(StagedSearchGeneration::new(StagedSearchGenerationInit {
            staged_ref: ArtifactRef::from_tablet_path(
                self.tablet.data_dir(),
                &staging_generation_dir,
            )?,
            generation_ref,
            head,
            staging_root,
            definition_id: definition.definition_id,
            generation_id,
            build_snapshot_version: snapshot_version,
            config_fingerprint: definition.config_fingerprint,
            coverage,
            layout_lease,
        }))
    }

    pub(crate) fn drop_definition(&self, definition_id: u64) -> Result<()> {
        // Drain any snapshot rebuild that captured this definition before the
        // durable retirement mutation. The tombstone rejects its publication;
        // holding the same single-flight lock until view removal guarantees no
        // live task can outlast the detach transition.
        let _build_guard = self.lock_definition_build(definition_id);
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let lifecycle_guard = self
            .lifecycle_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition lifecycle"))?;
        let initial = self.view.load_full();
        let Some(initial_state) = initial.definitions.get(&definition_id).cloned() else {
            return Ok(());
        };
        let restored_seed = if initial_state.origin.is_catalog_index()
            && initial_state.definition.kind == SearchIndexKind::Hnsw
        {
            self.restored_schema_seed_state(&initial_state.definition)?
        } else {
            None
        };
        drop(initial_state);
        drop(initial);
        let definition_guards = self.lock_definitions(
            std::iter::once(definition_id)
                .chain(restored_seed.as_ref().map(|(seed_id, _)| *seed_id)),
        )?;
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(());
        };
        drop(current);

        self.tablet
            .remove_search_generation_heads_guarded(&[definition_id], &publication_guard)?;
        self.retire_definition(
            state.definition.kind,
            definition_id,
            state.manifest.as_ref(),
        );

        let restored_seed_definition_id = restored_seed.as_ref().map(|(seed_id, _)| *seed_id);
        self.mutate_view(|view| {
            view.definitions.remove(&definition_id);
            if let Some((seed_id, seed_state)) = restored_seed {
                view.definitions.entry(seed_id).or_insert(seed_state);
            }
            Ok((true, ()))
        })?;
        drop(state);
        drop(definition_guards);
        drop(lifecycle_guard);
        drop(publication_guard);
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

    pub(crate) fn hnsw_capability(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<SearchCapability> {
        self.resolve_capability_with_required_wait(|view| view.hnsw_capability(column_id, distance))
    }

    pub(crate) fn hnsw_search_policy(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswSearchPolicy> {
        self.ensure_fresh();
        self.view.load().hnsw_search_policy(column_id, distance)
    }

    pub(crate) fn hnsw_generation_statistics(
        &self,
        definition_id: u64,
    ) -> Result<Option<crate::statistics::HnswIndexStatistics>> {
        self.ensure_fresh();
        self.view.load().hnsw_generation_statistics(definition_id)
    }

    pub(crate) fn hnsw_filter_topology(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswFilterTopologyContract> {
        self.ensure_fresh();
        self.view.load().hnsw_filter_topology(column_id, distance)
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
        generation_read_snapshot(definition_id, state)
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
        generation_read_snapshot(token.definition_id, state)?
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

    /// Materialize one catalog definition through the same admission and
    /// publication path used by background maintenance. CREATE INDEX calls
    /// this before declaring the catalog entry ready: a visible definition
    /// with permanently incomplete physical coverage is not a built index.
    pub(crate) fn materialize_definition(
        &self,
        definition_id: u64,
    ) -> Result<SearchGenerationCoverage> {
        let mut previous = self.generation_coverage(definition_id)?.ok_or_else(|| {
            paro_error::artifact_not_ready(format!(
                "search definition {definition_id} has no materialized generation"
            ))
        })?;
        while !previous.is_complete() {
            let report = self.maintenance_sweep()?;
            let next = self.generation_coverage(definition_id)?.ok_or_else(|| {
                paro_error::artifact_not_ready(format!(
                    "search definition {definition_id} disappeared while materializing"
                ))
            })?;
            if next == previous {
                let decision = report
                    .definitions
                    .iter()
                    .find(|definition| definition.definition_id == definition_id)
                    .map(|definition| {
                        format!(
                            "action={:?}, admission={:?}",
                            definition.action, definition.admission
                        )
                    })
                    .unwrap_or_else(|| "no maintenance decision".to_string());
                return Err(paro_error::artifact_not_ready(format!(
                    "search definition {definition_id} made no materialization progress ({}/{}, {decision})",
                    next.indexed_segment_count, next.visible_segment_count
                )));
            }
            previous = next;
        }
        Ok(previous)
    }

    pub(crate) fn catch_up_definition(&self, definition_id: u64) -> Result<usize> {
        self.ensure_fresh();
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(0);
        };
        drop(current);
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(0);
        };
        if !matches!(
            state.definition.kind,
            SearchIndexKind::Hnsw | SearchIndexKind::FullText | SearchIndexKind::Sparse
        ) {
            return Ok(0);
        }
        if state.definition.kind == SearchIndexKind::Hnsw && self.hnsw_task_scheduler().is_none() {
            tracing::debug!(
                tablet_id = self.tablet.tablet_id(),
                definition_id,
                "HNSW maintenance request admitted but no task scheduler is bound"
            );
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
            stop_check: None,
        };
        let estimate = builder.estimate_cost(&input)?;
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
            .flat_map(|artifact| {
                artifact
                    .coverage
                    .segments()
                    .iter()
                    .map(|span| span.segment.rowset_id)
            })
            .collect::<BTreeSet<_>>()
            .len();
        let sidecar_file_ids = sidecar_file_ids_for_artifacts(&result.artifact_refs);
        // Expensive provider work intentionally runs without publication or
        // definition locks. Re-enter the ordered publication critical section
        // only for the immutable manifest append, WAL record, head CAS, and
        // in-memory view CAS.
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
        let latest = self.view.load_full();
        let Some(latest_state) = latest.definitions.get(&definition_id).cloned() else {
            remove_sidecar_packages(&sidecar_store, &sidecar_file_ids);
            return Ok(0);
        };
        drop(latest);
        if head_for_state(&self.manifests, &latest_state) != head_for_state(&self.manifests, &state)
            || latest_state.definition != state.definition
            || latest_state.origin != state.origin
        {
            remove_sidecar_packages(&sidecar_store, &sidecar_file_ids);
            return Ok(0);
        }

        let next_state =
            match self.publish_sidecar_catch_up_delta(&latest_state, result.artifact_refs) {
                Ok(next_state) => next_state,
                Err(err) => {
                    remove_sidecar_packages(&sidecar_store, &sidecar_file_ids);
                    return Err(err);
                }
            };
        let completion = publish_head_for_state(
            &self.tablet,
            &self.manifests,
            &next_state,
            &publication_guard,
        )?;
        let view_result =
            self.publish_durable_revision_state(&latest_state, next_state.clone(), &completion);
        drop(latest_state);
        drop(_guard);
        drop(publication_guard);
        completion.finish()?;
        view_result?;
        drop(state);
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
        // A lease can outlive the publication that retired its artifacts. Revisit the
        // queue even when this sweep finds no definition work, including after the last
        // definition was dropped.
        self.sweep_retired();
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
            let delta_window_bytes = manifest.root.delta_window_bytes(
                &self
                    .manifests
                    .generation_dir(definition_id, manifest.root.generation_id),
            );
            let decision = self.maintenance_scheduler.plan_definition(
                &state.definition,
                manifest,
                gc_decision,
                &gc_context,
                delta_window_bytes,
            );
            let provider_request = provider_maintenance_request_for_definition(&state, manifest)?;
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
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(0);
        };
        drop(current);
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
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
        let latest = self.view.load_full();
        let Some(latest_state) = latest.definitions.get(&definition_id).cloned() else {
            remove_sidecar_packages(&store, &sidecar_file_ids);
            return Ok(0);
        };
        drop(latest);
        if head_for_state(&self.manifests, &latest_state) != head_for_state(&self.manifests, &state)
            || latest_state.definition != state.definition
            || latest_state.origin != state.origin
        {
            remove_sidecar_packages(&store, &sidecar_file_ids);
            return Ok(0);
        }

        let next_state = match self.publish_sidecar_repack_delta(&latest_state, repacked_artifacts)
        {
            Ok(next_state) => next_state,
            Err(err) => {
                remove_sidecar_packages(&store, &sidecar_file_ids);
                return Err(err);
            }
        };
        let completion = publish_head_for_state(
            &self.tablet,
            &self.manifests,
            &next_state,
            &publication_guard,
        )?;
        let view_result =
            self.publish_durable_revision_state(&latest_state, next_state.clone(), &completion);
        drop(latest_state);
        drop(_guard);
        drop(publication_guard);
        completion.finish()?;
        view_result?;
        drop(state);
        self.sweep_retired();
        record_tail_metrics_for_state(&next_state);
        Ok(repacked_count)
    }

    fn compact_manifest_deltas_for_definition(&self, definition_id: u64) -> Result<bool> {
        self.ensure_fresh();
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
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
        let mut revision = self.manifests.begin_revision_from_manifest(
            definition_id,
            manifest.root.clone(),
            manifest,
        )?;
        if !revision.compact_if_needed()? {
            return Ok(false);
        }
        let loaded = revision.commit()?;
        let next_state = state.clone().with_manifest(loaded);
        let completion = match publish_head_for_state(
            &self.tablet,
            &self.manifests,
            &next_state,
            &publication_guard,
        ) {
            Ok(completion) => completion,
            Err(error) => {
                self.retire_unpublished_revision(&state, &next_state);
                return Err(error);
            }
        };
        let view_result =
            self.publish_durable_revision_state(&state, next_state.clone(), &completion);
        drop(_guard);
        drop(publication_guard);
        if let Err(error) = completion.finish() {
            self.retire_unpublished_revision(&state, &next_state);
            return Err(error);
        }
        if let Err(error) = view_result {
            self.retire_unpublished_revision(&state, &next_state);
            return Err(error);
        }
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
        let _build_guard = self.lock_definition_build(definition_id);
        // Snapshot the immutable definition and rowset layout under the short
        // publication critical section. Provider construction and manifest
        // materialization happen after both locks are released.
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

        let current = self.view.load_full();
        let Some(mut state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(None);
        };
        drop(current);

        // Tablet metadata is the durable truth. Rowset publication may have
        // advanced the head atomically with its rowset before the observer
        // callback updates this derived in-memory view. Reconcile by loading
        // the committed root instead of rebuilding and overwriting the same
        // immutable revision name.
        let mut durable_head = self.tablet.search_generation_head(definition_id);
        if let Some(head) = durable_head.as_ref() {
            if head_for_state(&self.manifests, &state).as_ref() != Some(head) {
                let loaded = self
                    .manifests
                    .load_manifest_for_head(head)?
                    .ok_or_else(|| {
                        paro_error::data_corrupted(format!(
                            "durable search generation head for definition {definition_id} has no manifest"
                        ))
                    })?;
                let reconciled = state.clone().with_manifest(loaded);
                self.publish_definition_state(&state, reconciled.clone())?;
                if let Some(next_manifest) = reconciled.manifest.as_ref() {
                    self.retire_manifest_replaced_by(
                        state.definition.kind,
                        state.manifest.as_ref(),
                        next_manifest,
                    );
                }
                state = reconciled;
                durable_head = self.tablet.search_generation_head(definition_id);
            }
        }

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
        drop(_guard);
        drop(publication_guard);

        let next_state =
            self.refresh_state_from_snapshot(&state, visible_version, &visible_rowsets, force)?;

        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
        let latest = self.view.load_full();
        let Some(latest_state) = latest.definitions.get(&definition_id).cloned() else {
            drop(latest);
            drop(_guard);
            drop(publication_guard);
            self.retire_unpublished_revision(&state, &next_state);
            drop(next_state);
            self.sweep_retired();
            return Ok(None);
        };
        drop(latest);
        let still_current = latest_state.definition == state.definition
            && latest_state.origin == state.origin
            && head_for_state(&self.manifests, &latest_state)
                == head_for_state(&self.manifests, &state)
            && self.tablet.search_generation_head(definition_id) == durable_head
            && self.tablet.max_version() == visible_version;
        if !still_current {
            let capability = latest_state.capability.clone();
            drop(latest_state);
            drop(_guard);
            drop(publication_guard);
            self.retire_unpublished_revision(&state, &next_state);
            drop(next_state);
            self.sweep_retired();
            return Ok(capability);
        }

        let completion = publish_head_for_state(
            &self.tablet,
            &self.manifests,
            &next_state,
            &publication_guard,
        )?;
        let view_result =
            self.publish_durable_revision_state(&latest_state, next_state.clone(), &completion);
        drop(latest_state);
        drop(state);
        drop(_guard);
        drop(publication_guard);
        completion.finish()?;
        view_result?;
        self.sweep_retired();
        Ok(next_state.capability)
    }

    fn prepare_heads_for_visible_rowsets(
        &self,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<SearchGenerationHeadUpdates> {
        let definition_ids = self
            .view
            .load()
            .definitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut updates = SearchGenerationHeadUpdates::default();
        for definition_id in definition_ids {
            let definition_lock = self.definition_lock(definition_id);
            let _guard = definition_lock
                .lock()
                .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

            let current = self.view.load_full();
            let Some(state) = current.definitions.get(&definition_id).cloned() else {
                continue;
            };
            let next_state = match self.refresh_state_from_snapshot(
                &state,
                visible_version,
                visible_rowsets,
                false,
            ) {
                Ok(next_state) => next_state,
                Err(error) => {
                    tracing::error!(
                        tablet_id = self.tablet.tablet_id(),
                        definition_id,
                        visible_version,
                        error = %error,
                        "kept prior search generation head after rowset manifest preparation failed"
                    );
                    updates.mark_stale(definition_id);
                    continue;
                }
            };
            if let Some(head) = head_for_state(&self.manifests, &next_state) {
                let manifest = next_state.manifest.ok_or_else(|| {
                    paro_error::internal(format!(
                        "prepared search head for definition {definition_id} has no manifest"
                    ))
                })?;
                updates.push(head, manifest);
            }
        }
        Ok(updates)
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
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let lifecycle_guard = self
            .lifecycle_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition lifecycle"))?;
        if origin.is_catalog_index()
            && definition.kind == SearchIndexKind::Hnsw
            && self
                .view
                .load()
                .definitions
                .iter()
                .any(|(definition_id, state)| {
                    *definition_id != definition.definition_id
                        && state.origin.is_catalog_index()
                        && state.definition.kind == SearchIndexKind::Hnsw
                        && state.definition.column_ids == definition.column_ids
                })
        {
            return Err(paro_error::invalid_input(format!(
                "only one catalog HNSW definition may target columns {:?}",
                definition.column_ids
            )));
        }
        let duplicate_seed_ids = if origin.is_catalog_index() {
            self.view
                .load()
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
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let definition_guards = self.lock_definitions(
            std::iter::once(definition.definition_id).chain(duplicate_seed_ids.iter().copied()),
        )?;

        // A catalog definition replacing a schema seed also replaces the
        // seed's durable head. Remove those heads before retiring any files so
        // tablet metadata can never reference a definition directory that no
        // longer exists.
        self.tablet
            .remove_search_generation_heads_guarded(&duplicate_seed_ids, &publication_guard)?;

        let mut state = SearchDefinitionState::new(definition.clone(), origin)?;
        if let Some(loaded) = self.load_manifest_for_definition(definition.definition_id)? {
            if loaded.root.config_fingerprint == definition.config_fingerprint {
                state = state.with_manifest(loaded);
                record_tail_metrics_for_state(&state);
            } else {
                state =
                    state.with_generation_floor(loaded.root.generation_id, loaded.root.build_epoch);
            }
        }
        let removed_seed_states = self.mutate_view(|view| {
            let mut removed = Vec::new();
            for duplicate_seed_id in &duplicate_seed_ids {
                if let Some(seed_state) = view.definitions.remove(duplicate_seed_id) {
                    removed.push((*duplicate_seed_id, seed_state));
                }
            }
            view.definitions.insert(definition.definition_id, state);
            Ok((true, removed))
        })?;
        for (duplicate_seed_id, seed_state) in removed_seed_states {
            self.retire_definition(
                seed_state.definition.kind,
                duplicate_seed_id,
                seed_state.manifest.as_ref(),
            );
        }
        drop(definition_guards);
        drop(lifecycle_guard);
        drop(publication_guard);
        self.sweep_retired();
        self.refresh_definition(definition.definition_id)?;
        Ok(())
    }

    fn refresh_state_from_snapshot(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
        force: bool,
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
                .flat_map(|artifact| {
                    artifact
                        .coverage
                        .segments()
                        .iter()
                        .map(|span| span.segment.rowset_id)
                })
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
                );
            }
            if removed_rowsets.is_empty() {
                if !new_rowsets.is_empty() {
                    return self.publish_delta_for_new_rowsets(
                        state,
                        visible_version,
                        visible_rowsets,
                        &new_rowsets,
                    );
                }
                if force {
                    if let Some(next_state) = self.publish_delta_for_covered_tail_entries(
                        state,
                        visible_version,
                        visible_rowsets,
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

        self.publish_full_snapshot(state, visible_version, visible_rowsets)
    }

    fn publish_full_snapshot(
        &self,
        state: &SearchDefinitionState,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
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
            .map_or(0, |manifest| manifest.root.root_version);
        let definition_id = state.definition.definition_id;
        let next_tail_entry_id = assign_tail_entry_ids_for_full_snapshot(
            &mut snapshot.tail_pending.entries,
            state.manifest.as_ref(),
        );
        let root = GenerationManifestRoot {
            definition_id,
            generation_id,
            build_epoch,
            build_snapshot_version: snapshot.visible_version,
            indexed_through_ts: indexed_through_ts(snapshot.visible_version),
            config_fingerprint: state.definition.config_fingerprint,
            coverage: snapshot.coverage.clone(),
            generation_stats: snapshot.generation_stats.clone(),
            persisted_tail_entry_id_seed: next_tail_entry_id,
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
        };
        let generation_artifacts = GenerationArtifactSet::try_new(assign_generation_id(
            snapshot.artifacts.clone(),
            generation_id,
        ))?;
        let mut revision = self.manifests.begin_empty_revision(definition_id, root)?;
        revision.replace_with_shard(&ManifestShard {
            artifact_refs: generation_artifacts.artifacts,
            tail_pending_entries: snapshot.tail_pending.entries,
        })?;
        let loaded = revision.commit()?;

        let mut next_state = state.clone();
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
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let Some(current_manifest) = state.manifest.as_ref() else {
            return self.publish_full_snapshot(state, visible_version, visible_rowsets);
        };

        let mut added_artifacts = Vec::new();
        let mut added_tail_entries = Vec::new();
        let mut delta_generation_stats = empty_generation_stats_for_definition(&state.definition)?;
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
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = visible_version;
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);
        let mut next_tail_entry_id = current_manifest.next_tail_entry_id().0;
        assign_tail_entry_ids(&mut added_tail_entries, &mut next_tail_entry_id);
        root.persisted_tail_entry_id_seed = TailEntryId(next_tail_entry_id);

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

        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, current_manifest)?;
        revision.append_delta(&ManifestDelta::publish_changes(
            added_artifacts.clone(),
            added_tail_entries,
            stats_deltas_from_generation_stats(&delta_generation_stats),
        ))?;
        let loaded = revision.commit()?;
        let mut next_state = state.clone();
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
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let Some(current_manifest) = state.manifest.as_ref() else {
            return self.publish_full_snapshot(state, visible_version, visible_rowsets);
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
            .filter(|artifact| artifact.coverage.intersects_rowsets(&removed_rowset_ids))
            .cloned()
            .collect::<Vec<_>>();
        let removed_partitions = removed_artifacts
            .iter()
            .map(|artifact| artifact.coverage.clone())
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
        added_tail_entries.extend(surviving_partition_tail_entries(
            &removed_artifacts,
            &removed_rowset_ids,
        ));

        let mut root = current_manifest.root.clone();
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = visible_version;
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let mut next_tail_entry_id = current_manifest.next_tail_entry_id().0;
        assign_tail_entry_ids(&mut added_tail_entries, &mut next_tail_entry_id);
        root.persisted_tail_entry_id_seed = TailEntryId(next_tail_entry_id);

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
                .filter(|artifact| !artifact.coverage.intersects_rowsets(&removed_rowset_ids))
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
            removed_partitions
                .iter()
                .cloned()
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
        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, current_manifest)?;
        revision.append_delta(&ManifestDelta::new(delta_entries))?;
        let loaded = revision.commit()?;
        let mut next_state = state.clone();
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
        let current_artifact_keys =
            artifact_segment_column_keys(current_manifest.artifacts.artifacts.iter());
        let added_artifact_keys = artifact_segment_column_keys(added_artifacts.iter());
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
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = self.tablet.max_version();
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let tail_pending_entries = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| !covered_tail_ids.contains(&entry.entry_id))
            .cloned()
            .collect::<Vec<_>>();
        let delta_generation_stats =
            generation_stats_from_artifacts(&state.definition, &added_artifacts)?;
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
        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, current_manifest)?;
        revision.append_delta(&ManifestDelta::new(delta_entries))?;
        let loaded = revision.commit()?;
        let mut next_state = state.clone();
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
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = self.tablet.max_version();
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let repacked_artifacts = assign_generation_id(repacked_artifacts, generation.generation_id);
        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, current_manifest)?;
        revision.append_delta(&ManifestDelta::new(
            repacked_artifacts
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::AddArtifact)
                .collect(),
        ))?;
        let loaded = revision.commit()?;
        let mut next_state = state.clone();
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
        let current_segment_column_keys =
            artifact_segment_column_keys(current_manifest.artifacts.artifacts.iter());
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
            let snapshot_artifact_keys = artifact_segment_column_keys(snapshot.artifacts.iter());
            let covered_ids_for_rowset = current_manifest
                .tail_pending_entries
                .iter()
                .filter(|tail_entry| {
                    tail_entry.rowset_id == entry.rowset_id
                        && !matches!(tail_entry.mutation, TailMutationKind::Delete)
                        && tail_entry_is_covered_by_artifacts(
                            &state.definition,
                            tail_entry,
                            &current_segment_column_keys,
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
        root.build_epoch = state.next_build_epoch;
        root.build_snapshot_version = visible_version;
        root.indexed_through_ts = indexed_through_ts(root.build_snapshot_version);

        let mut next_tail_entry_id = current_manifest.next_tail_entry_id().0;
        assign_tail_entry_ids(&mut added_tail_entries, &mut next_tail_entry_id);
        root.persisted_tail_entry_id_seed = TailEntryId(next_tail_entry_id);

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
            generation_stats_from_artifacts(&state.definition, &added_artifacts)?;
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

        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, current_manifest)?;
        revision.append_delta(&ManifestDelta::new(delta_entries))?;
        let loaded = revision.commit()?;
        let mut next_state = state.clone();
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

    fn mutate_view<R>(
        &self,
        mutation: impl FnOnce(&mut SearchView) -> Result<(bool, R)>,
    ) -> Result<R> {
        // Expensive artifact and manifest work must happen before this boundary. Cloning
        // from the latest snapshot while holding one short writer lock preserves updates
        // published concurrently for other definitions without blocking readers.
        let _guard = self
            .view_write_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search view writer"))?;
        let current = self.view.load_full();
        let mut next = (*current).clone();
        let (changed, result) = mutation(&mut next)?;
        if changed {
            next.version = current.version.saturating_add(1);
            self.view.store(Arc::new(next));
        }
        Ok(result)
    }

    fn publish_definition_state(
        &self,
        expected: &SearchDefinitionState,
        next_state: SearchDefinitionState,
    ) -> Result<()> {
        debug_assert_eq!(
            expected.definition.definition_id,
            next_state.definition.definition_id
        );
        let definition_id = expected.definition.definition_id;
        let published = self.mutate_view(|view| {
            let still_current = view.definitions.get(&definition_id).is_some_and(|state| {
                state.definition == expected.definition && state.origin == expected.origin
            });
            if !still_current {
                return Ok((false, false));
            }
            view.definitions.insert(definition_id, next_state);
            Ok((true, true))
        })?;
        if published {
            Ok(())
        } else {
            Err(paro_error::internal(format!(
                "search definition {definition_id} changed while its publish lock was held"
            )))
        }
    }

    fn publish_durable_revision_state(
        &self,
        expected: &SearchDefinitionState,
        next_state: SearchDefinitionState,
        completion: &SearchGenerationPublishCompletion,
    ) -> Result<()> {
        if !completion.publication_succeeded() {
            return Ok(());
        }
        if let Some(manifest) = next_state.manifest.as_ref() {
            manifest.mark_revision_published();
        }
        self.publish_definition_state(expected, next_state.clone())?;
        if let Some(next_manifest) = next_state.manifest.as_ref() {
            self.retire_manifest_replaced_by(
                expected.definition.kind,
                expected.manifest.as_ref(),
                next_manifest,
            );
        }
        Ok(())
    }

    fn definition_lock(&self, definition_id: u64) -> &Mutex<()> {
        let shard = (definition_id % DEFINITION_LOCK_SHARDS as u64) as usize;
        &self.definition_locks[shard]
    }

    fn definition_build_lock(&self, definition_id: u64) -> &Mutex<()> {
        let shard = (definition_id % DEFINITION_LOCK_SHARDS as u64) as usize;
        &self.definition_build_locks[shard]
    }

    fn lock_definition_build(&self, definition_id: u64) -> MutexGuard<'_, ()> {
        match self.definition_build_lock(definition_id).lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    tablet_id = self.tablet.tablet_id(),
                    definition_id,
                    "recovering poisoned search definition rebuild lock"
                );
                poisoned.into_inner()
            }
        }
    }

    fn lock_definitions(
        &self,
        definition_ids: impl IntoIterator<Item = u64>,
    ) -> Result<Vec<MutexGuard<'_, ()>>> {
        let mut shards = definition_ids
            .into_iter()
            .map(|definition_id| (definition_id % DEFINITION_LOCK_SHARDS as u64) as usize)
            .collect::<Vec<_>>();
        shards.sort_unstable();
        shards.dedup();
        shards
            .into_iter()
            .map(|shard| {
                self.definition_locks[shard].lock().map_err(|_| {
                    paro_error::internal(format!(
                        "lock search definition shard {shard} for lifecycle update"
                    ))
                })
            })
            .collect()
    }

    fn retire_definition(
        &self,
        provider: SearchIndexKind,
        definition_id: u64,
        manifest: Option<&LoadedManifest>,
    ) {
        let mut paths = self
            .manifests
            .definition_paths(definition_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let Some(manifest) = manifest else {
            self.manifests
                .remove_paths(&paths.into_iter().collect::<Vec<_>>());
            self.manifests.prune_empty_definition_dirs(definition_id);
            return;
        };
        paths.extend(retire_paths_for_manifest(
            &self.tablet.data_dir().clone(),
            manifest,
        ));
        self.retire_manifest_paths(provider, manifest, paths.into_iter().collect());
    }

    fn retire_unpublished_revision(
        &self,
        base: &SearchDefinitionState,
        candidate: &SearchDefinitionState,
    ) {
        let Some(candidate_manifest) = candidate.manifest.as_ref() else {
            return;
        };
        let keep_paths = base
            .manifest
            .as_ref()
            .map(|manifest| {
                retire_paths_for_manifest(&self.tablet.data_dir().clone(), manifest)
                    .into_iter()
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let rollback_owned_paths = candidate_manifest.rollback_owned_paths();
        let retired_paths =
            retire_paths_for_manifest(&self.tablet.data_dir().clone(), candidate_manifest)
                .into_iter()
                .filter(|path| !keep_paths.contains(path) && !rollback_owned_paths.contains(path))
                .collect();
        self.retire_manifest_paths(candidate.definition.kind, candidate_manifest, retired_paths);
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
        let retired_path_set = paths.iter().cloned().collect::<BTreeSet<_>>();
        let store = SidecarArtifactStore::new(self.tablet.data_dir().clone());
        let sidecar_file_ids = manifest
            .artifacts
            .artifacts
            .iter()
            .filter_map(|artifact| match artifact.location {
                ArtifactLocation::SidecarArtifactFile { file_id, .. }
                    if retired_path_set.contains(&store.package_path(file_id)) =>
                {
                    Some(file_id)
                }
                _ => None,
            })
            .collect();
        let bytes = manifest_path_bytes(&paths);
        storage_metrics().record_search_generation_retired(provider, bytes);
        let retired = RetiredManifest {
            definition_id: manifest.root.definition_id,
            provider,
            artifacts: manifest.artifacts.clone(),
            sidecar_file_ids,
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
            self.reader_runtime
                .evict_packages(&retired.sidecar_file_ids);
            self.manifests.remove_paths(&retired.paths);
            self.manifests
                .prune_empty_definition_dirs(retired.definition_id);
        }
    }

    fn restored_schema_seed_state(
        &self,
        definition: &SearchIndexDefinition,
    ) -> Result<Option<(u64, SearchDefinitionState)>> {
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
        Ok(Some((
            seed_definition_id,
            SearchDefinitionState::new(seed, SearchDefinitionOrigin::schema_seed(column_id))?,
        )))
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

fn expected_segment_rows(
    visible_rowsets: &[RowsetSharedPtr],
) -> Result<BTreeMap<ArtifactSegmentRef, u64>> {
    let mut expected = BTreeMap::new();
    for rowset in visible_rowsets {
        rowset.load()?;
        for segment in rowset.segments() {
            let row_count = u64::try_from(segment.num_rows()).map_err(|_| {
                paro_error::out_of_range("segment row count does not fit staged coverage")
            })?;
            if row_count == 0 {
                continue;
            }
            expected.insert(
                ArtifactSegmentRef {
                    rowset_id: rowset.rowset_id(),
                    segment_id: segment.segment_id(),
                },
                row_count,
            );
        }
    }
    Ok(expected)
}

fn validate_staged_artifact_coverage(
    definition: &SearchIndexDefinition,
    generation_id: u64,
    visible_rowsets: &[RowsetSharedPtr],
    artifacts: &[SearchArtifactRef],
) -> Result<()> {
    let expected_segments = expected_segment_rows(visible_rowsets)?;
    let expected = definition
        .column_ids
        .iter()
        .flat_map(|column_id| {
            expected_segments
                .iter()
                .map(move |(segment, rows)| ((*segment, *column_id), *rows))
        })
        .collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();
    for artifact in artifacts {
        artifact.validate()?;
        if artifact.definition_id != definition.definition_id
            || artifact.generation_id != generation_id
            || artifact.kind != definition.kind
        {
            return Err(paro_error::data_corrupted(
                "staged search artifact identity does not match its definition",
            ));
        }
        for span in artifact.coverage.segments() {
            if actual
                .insert((span.segment, artifact.column_id), span.row_count)
                .is_some()
            {
                return Err(paro_error::data_corrupted(format!(
                    "staged search generation contains duplicate coverage for {:?} column {}",
                    span.segment, artifact.column_id
                )));
            }
        }
    }
    if actual != expected {
        return Err(paro_error::data_corrupted(format!(
            "staged search generation coverage mismatch: expected {} segment-columns, built {}",
            expected.len(),
            actual.len()
        )));
    }
    Ok(())
}

fn elapsed_micros_since(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

/// Invalidating a multi-segment search partition because one source rowset was
/// compacted also removes its still-visible spans. Keep those spans in the
/// exact tail until search compaction publishes a replacement partition;
/// otherwise an atomic table-compaction publish could create a coverage hole.
fn surviving_partition_tail_entries(
    removed_artifacts: &[SearchArtifactRef],
    removed_rowset_ids: &BTreeSet<RowsetId>,
) -> Vec<TailPendingEntry> {
    let mut spans = BTreeMap::<(RowsetId, u32), (u64, u64)>::new();
    for artifact in removed_artifacts {
        for span in artifact.coverage.segments() {
            if removed_rowset_ids.contains(&span.segment.rowset_id) {
                continue;
            }
            let bytes = artifact
                .stats
                .bytes_on_disk
                .saturating_mul(span.row_count)
                .div_ceil(artifact.stats.row_count.max(1));
            spans
                .entry((span.segment.rowset_id, span.segment.segment_id))
                .and_modify(|current| {
                    current.0 = current.0.max(span.row_count);
                    current.1 = current.1.max(bytes);
                })
                .or_insert((span.row_count, bytes));
        }
    }

    let mut rowsets = BTreeMap::<RowsetId, (Vec<u32>, u64, u64)>::new();
    for ((rowset_id, segment_id), (row_count, byte_count)) in spans {
        let entry = rowsets.entry(rowset_id).or_default();
        entry.0.push(segment_id);
        entry.1 = entry.1.saturating_add(row_count);
        entry.2 = entry.2.saturating_add(byte_count);
    }
    rowsets
        .into_iter()
        .map(
            |(rowset_id, (segment_ids, row_count, byte_count))| TailPendingEntry {
                entry_id: TailEntryId::UNASSIGNED,
                rowset_id,
                segment_ids,
                mutation: TailMutationKind::Append,
                row_count,
                byte_count,
                row_image_ref: Some(TailRowImageRef::WholeRowset),
            },
        )
        .collect()
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
    use crate::index::hnsw::{DistanceMetric, SearchParams};
    use crate::meta::{FileMetadataStore, GlobalSchemaMap, MetadataStore, TabletMetaManager};
    use crate::rowset::{ColumnData, RowsetWriter, RowsetWriterContext, SparseVector};
    use crate::search::artifact::{ArtifactLocation, SegmentPagePointer};
    use crate::search::capability::{
        ArtifactSegmentRef, ArtifactSegmentSpan, SearchPartitionCoverage,
    };
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
    use crate::table::table_handle::TableHandle;
    use crate::tablet::{KeysType, Tablet, TabletColumn, TabletSchema, Version};
    use crate::test_utils::*;
    use paro_common::allocator::default_allocator;
    use paro_common::chunk::Chunk;
    use paro_common::effect::{SearchGenerationPublication, TabletMutation};
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_scheduler::scheduler::TaskScheduler;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    fn singleton_artifact_segment(artifact: &SearchArtifactRef) -> ArtifactSegmentRef {
        artifact
            .coverage
            .singleton_segment()
            .expect("test artifact must cover one segment")
    }

    fn create_schema_seeded_hnsw_table(
        root: &std::path::Path,
        types: &[LogicalType],
        vector_column: usize,
    ) -> TableHandle {
        let columns = types
            .iter()
            .enumerate()
            .map(|(idx, logical_type)| {
                let column =
                    TabletColumn::new(idx as u32, format!("col_{idx}"), logical_type.clone());
                if idx == vector_column {
                    column.with_hnsw_index(16, 64, 0)
                } else {
                    column
                }
            })
            .collect();
        let schema = Arc::new(TabletSchema::new(1, columns, KeysType::DuplicateKeys).unwrap());
        let tablet_id = 10_002;
        let tablet = Tablet::new(
            tablet_id,
            tablet_id,
            0,
            schema,
            root.join("tablet"),
            Some(meta_manager(root)),
        )
        .unwrap();
        tablet.init().unwrap();
        tablet.save_meta().unwrap();
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
        let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, row_limit.max(1024), 1);

        loop {
            match cursor.next_batch(&batch_config, &mut budget)? {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => chunks.push(table.materialize_search_batch(
                    &snapshot,
                    batch,
                    projected_columns,
                    emit_score,
                    Arc::new(default_allocator()),
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
            .generation_dir(definition_id, manifest.root.generation_id);
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
        let provider_config = json!({"version": 1, "config": "simple"});
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
            coverage: SearchPartitionCoverage::singleton(
                ArtifactSegmentRef {
                    rowset_id,
                    segment_id: 0,
                },
                u64::from(total_docs),
            )
            .unwrap(),
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
        let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
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
            coverage: SearchPartitionCoverage::singleton(
                ArtifactSegmentRef {
                    rowset_id,
                    segment_id: 0,
                },
                row_count,
            )
            .unwrap(),
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

    fn test_hnsw_provider_config(
        dimension: u32,
        m: usize,
        ef_construct: usize,
        inline_max_vector_count: u64,
    ) -> serde_json::Value {
        crate::search::HnswProviderConfig {
            version: crate::search::HNSW_PROVIDER_CONFIG_VERSION,
            dimension,
            distance: DistanceMetric::Euclidean,
            build_vector_encoding:
                crate::index::hnsw::HnswBuildVectorEncoding::default_for_dimension(dimension)
                    .unwrap(),
            m: m as u32,
            ef_construct: ef_construct as u32,
            ef_search: ef_construct as u32,
            rerank_policy: crate::index::hnsw::HnswRerankPolicy::default_for_encoding(
                crate::index::hnsw::HnswBuildVectorEncoding::default_for_dimension(dimension)
                    .unwrap(),
            ),
            distance_cost: crate::index::hnsw::HnswDistanceCostProfile::default(),
            build_seed: 1,
            proposal_wave_size: crate::search::DEFAULT_HNSW_PROPOSAL_WAVE_SIZE,
            warmup_point_count: crate::search::DEFAULT_HNSW_WARMUP_POINT_COUNT,
            filter_columns: Vec::new(),
            filter_block_rows: crate::search::DEFAULT_HNSW_FILTER_BLOCK_ROWS,
            filter_m: crate::search::DEFAULT_HNSW_FILTER_M,
            inline_threshold: crate::search::HnswInlineConfig {
                enabled: inline_max_vector_count != 0,
                max_vector_count: inline_max_vector_count,
                max_graph_memory_bytes: if inline_max_vector_count == 0 {
                    0
                } else {
                    64 * 1024 * 1024
                },
                max_dimension: if inline_max_vector_count == 0 {
                    0
                } else {
                    1_536
                },
            },
        }
        .validated()
        .unwrap()
        .to_value()
        .unwrap()
    }

    fn hnsw_test_definition(definition_id: u64) -> SearchIndexDefinition {
        let provider_config = test_hnsw_provider_config(128, 16, 100, 4_096);
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
            coverage: SearchPartitionCoverage::singleton(
                ArtifactSegmentRef {
                    rowset_id,
                    segment_id: 0,
                },
                vector_count,
            )
            .unwrap(),
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
    fn concurrent_view_publications_preserve_distinct_definitions() {
        const DEFINITION_COUNT: u64 = 16;

        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(root.path(), &[LogicalType::Varchar]);
        let registry = Arc::clone(&table.search_registry);
        registry
            .mutate_view(|view| {
                for definition_id in 1..=DEFINITION_COUNT {
                    view.definitions.insert(
                        definition_id,
                        SearchDefinitionState::new(
                            fulltext_test_definition(definition_id),
                            SearchDefinitionOrigin::catalog(definition_id),
                        )
                        .unwrap(),
                    );
                }
                Ok((true, ()))
            })
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(DEFINITION_COUNT as usize));

        std::thread::scope(|scope| {
            for definition_id in 1..=DEFINITION_COUNT {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    let expected = registry
                        .view
                        .load()
                        .definitions
                        .get(&definition_id)
                        .cloned()
                        .unwrap();
                    let mut next = expected.clone();
                    next.next_build_epoch = next.next_build_epoch.saturating_add(1);
                    barrier.wait();
                    registry.publish_definition_state(&expected, next).unwrap();
                });
            }
        });

        let view = registry.view.load();
        assert_eq!(view.definitions.len(), DEFINITION_COUNT as usize);
        assert_eq!(view.version, DEFINITION_COUNT + 1);
        for definition_id in 1..=DEFINITION_COUNT {
            assert_eq!(
                view.definitions
                    .get(&definition_id)
                    .map(|state| state.next_build_epoch),
                Some(2)
            );
        }
    }

    #[test]
    fn stale_definition_publication_cannot_resurrect_removed_definition() {
        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(root.path(), &[LogicalType::Varchar]);
        let registry = table.search_registry();
        let definition = fulltext_test_definition(101);
        let state =
            SearchDefinitionState::new(definition, SearchDefinitionOrigin::catalog(101)).unwrap();
        registry
            .mutate_view(|view| {
                view.definitions.insert(101, state);
                Ok((true, ()))
            })
            .unwrap();

        let stale = registry.view.load().definitions.get(&101).cloned().unwrap();
        registry
            .mutate_view(|view| {
                view.definitions.remove(&101);
                Ok((true, ()))
            })
            .unwrap();

        let mut stale_refresh = stale.clone();
        stale_refresh.next_build_epoch = stale_refresh.next_build_epoch.saturating_add(1);
        assert!(registry
            .publish_definition_state(&stale, stale_refresh)
            .is_err());
        assert!(!registry.view.load().definitions.contains_key(&101));
    }

    #[test]
    fn active_generation_lease_delays_retired_artifact_reclamation() {
        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(root.path(), &[LogicalType::Varchar]);
        let registry = table.search_registry();
        let definition = fulltext_test_definition(102);
        let definition_id = definition.definition_id;
        let retired_path = root.path().join("leased-search-artifact");
        std::fs::write(&retired_path, b"leased").unwrap();
        let manifest = LoadedManifest {
            root: GenerationManifestRoot {
                definition_id,
                generation_id: 1,
                build_epoch: 1,
                build_snapshot_version: 1,
                indexed_through_ts: 1,
                config_fingerprint: definition.config_fingerprint,
                coverage: CoverageState::Complete,
                generation_stats: GenerationStats::default(),
                persisted_tail_entry_id_seed: TailEntryId(1),
                execution_modes: ExecutionModes::default(),
                maintenance_state: GenerationMaintenanceState::default(),
                root_version: 1,
                checksum: 0,
                shard_files: Vec::new(),
                recent_delta_files: Vec::new(),
            },
            root_path: root.path().join("manifest-root"),
            shard_paths: Vec::new(),
            delta_paths: Vec::new(),
            tail_entry_id_allocator: TailEntryId(1),
            publication_lease: None,
            artifacts: Arc::new(GenerationArtifactSet::default()),
            tail_pending_entries: Vec::new(),
        };
        let state = SearchDefinitionState::new(
            definition.clone(),
            SearchDefinitionOrigin::catalog(definition_id),
        )
        .unwrap()
        .with_manifest(manifest);
        let snapshot = generation_read_snapshot(definition_id, &state)
            .unwrap()
            .unwrap();
        let lease = super::super::cursor::GenerationReadLease::from_snapshot(&snapshot);
        let manifest = state.manifest.as_ref().unwrap();
        assert!(Arc::ptr_eq(&snapshot.artifacts, &manifest.artifacts));

        registry.retire_manifest_paths(definition.kind, manifest, vec![retired_path.clone()]);
        drop(snapshot);
        drop(state);
        registry.sweep_retired();
        assert!(retired_path.exists());

        drop(lease);
        registry.sweep_retired();
        assert!(!retired_path.exists());
    }

    #[test]
    fn artifact_replacement_stats_rebuilds_irreversible_fulltext_summary() {
        let definition = fulltext_test_definition(91);
        let removed = fulltext_test_artifact(91, 1, 4, 8, 4, 8, 10);
        let kept = fulltext_test_artifact(91, 2, 6, 18, 5, 12, 6);
        let added = fulltext_test_artifact(91, 3, 2, 4, 2, 3, 3);
        let current =
            generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]).unwrap();
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
    fn invalidated_partition_preserves_still_visible_spans_as_exact_tail() {
        let mut artifact = fulltext_test_artifact(91, 1, 4, 8, 4, 8, 10);
        artifact.coverage = SearchPartitionCoverage::try_new(vec![
            ArtifactSegmentSpan {
                segment: ArtifactSegmentRef {
                    rowset_id: 1,
                    segment_id: 0,
                },
                row_count: 2,
            },
            ArtifactSegmentSpan {
                segment: ArtifactSegmentRef {
                    rowset_id: 2,
                    segment_id: 3,
                },
                row_count: 2,
            },
        ])
        .unwrap();
        artifact.location = ArtifactLocation::SidecarArtifactFile {
            file_id: ArtifactFileId {
                definition_id: 91,
                generation_id: 1,
                package_index: 0,
            },
            offset: 0,
            len: 64,
            checksum: 7,
        };
        artifact.validate().unwrap();

        let tails = surviving_partition_tail_entries(&[artifact], &BTreeSet::from([1]));
        assert_eq!(tails.len(), 1);
        assert_eq!(tails[0].rowset_id, 2);
        assert_eq!(tails[0].segment_ids, vec![3]);
        assert_eq!(tails[0].row_count, 2);
        assert_eq!(tails[0].mutation, TailMutationKind::Append);
        assert_eq!(tails[0].row_image_ref, Some(TailRowImageRef::WholeRowset));
    }

    #[test]
    fn artifact_replacement_stats_rebuilds_irreversible_sparse_summary() {
        let definition = sparse_test_definition(92);
        let removed = sparse_test_artifact(92, 1, 4, 12, 3, 3.0);
        let kept = sparse_test_artifact(92, 2, 6, 20, 5, 4.0);
        let added = sparse_test_artifact(92, 3, 2, 8, 6, 5.0);
        let current =
            generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]).unwrap();
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
            generation_stats_from_artifacts(&definition, &[removed.clone(), kept.clone()]).unwrap();
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
                persisted_tail_entry_id_seed: TailEntryId(10),
                execution_modes: ExecutionModes::default(),
                maintenance_state: GenerationMaintenanceState::default(),
                root_version: 1,
                checksum: 0,
                shard_files: Vec::new(),
                recent_delta_files: Vec::new(),
            },
            root_path: std::path::PathBuf::new(),
            shard_paths: Vec::new(),
            delta_paths: Vec::new(),
            tail_entry_id_allocator: TailEntryId(10),
            publication_lease: None,
            artifacts: Arc::new(GenerationArtifactSet::default()),
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
            provider_config: json!({"version": 1, "config": "simple"}),
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
            provider_config: json!({"version": 1, "physical_encoding": "binary-v1"}),
            freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
            config_fingerprint: 101,
        };

        view.definitions.insert(
            fulltext_definition.definition_id,
            SearchDefinitionState::new(
                fulltext_definition.clone(),
                SearchDefinitionOrigin::catalog(fulltext_definition.definition_id),
            )
            .unwrap(),
        );
        view.definitions.insert(
            sparse_definition.definition_id,
            SearchDefinitionState::new(
                sparse_definition.clone(),
                SearchDefinitionOrigin::catalog(sparse_definition.definition_id),
            )
            .unwrap(),
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
        let physical_config = json!({"version": 1, "config": "simple"});
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
            )
            .unwrap(),
        );
        view.definitions.insert(
            required.definition_id,
            SearchDefinitionState::new(
                required.clone(),
                SearchDefinitionOrigin::catalog(required.definition_id),
            )
            .unwrap(),
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
        let table = create_schema_seeded_hnsw_table(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
            0,
        );

        assert!(table.search_registry().definition_count() >= 1);
        assert!(table
            .vector_capability(0, DistanceMetric::Euclidean)
            .is_some());
        assert!(
            table.vector_capability(0, DistanceMetric::Cosine).is_none(),
            "metric mismatch must not expose an HNSW capability"
        );
    }

    #[test]
    fn cancelled_staged_generation_removes_workspace_and_releases_layout_lease() {
        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        );
        let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
        let definition = SearchIndexDefinition {
            definition_id: 91,
            table_id: table.tablet_id(),
            name: "cancelled_vec_hnsw".to_string(),
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
        let txn_id = 77;
        let workspace = table
            .search_registry()
            .manifests
            .staged_generation_workspace(txn_id, definition.definition_id, 1);
        let checks = Arc::new(AtomicUsize::new(0));
        let stop_checks = Arc::clone(&checks);
        let result = table.search_registry().stage_definition_generation(
            definition,
            txn_id,
            SearchBuildStopCheck::new(move || stop_checks.fetch_add(1, Ordering::Relaxed) >= 3),
        );

        assert!(result.is_err());
        assert!(!workspace.exists());
        assert!(table
            .tablet()
            .try_acquire_compaction_layout_lease()
            .unwrap()
            .is_some());
    }

    #[test]
    fn explicit_hnsw_definition_overrides_and_restores_schema_seed_origin() {
        let root = TempDir::new().unwrap();
        let table = create_schema_seeded_hnsw_table(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
            0,
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

        let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
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
        let table =
            create_schema_seeded_hnsw_table(root.path(), &[LogicalType::Integer, vector_type], 1);
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
            .vector_capability(1, DistanceMetric::Euclidean)
            .expect("recovered schema seed capability");
        assert_eq!(recovered_capability.definition_id, seed_definition_id);
        assert!(recovered_capability.coverage.is_complete());
        assert_eq!(recovered_capability.generation_stats.artifact_count, 1);
        let recovered_stats = reopened
            .hnsw_generation_statistics(seed_definition_id)
            .unwrap()
            .expect("recovered generation HNSW statistics");
        assert_eq!(recovered_stats.num_indexed_vectors, 2);
        assert_eq!(recovered_stats.dimension, 4);
        {
            let current = reopened.search_registry().view.load();
            let seed_state = current
                .definitions
                .get(&seed_definition_id)
                .expect("recovered schema seed definition");
            assert_eq!(seed_state.origin, SearchDefinitionOrigin::schema_seed(1));
        }

        let provider_config = test_hnsw_provider_config(4, 16, 64, 4_096);
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
        assert!(reopened_again
            .vector_capability(1, DistanceMetric::Euclidean)
            .is_some());

        let opened = reopened_again
            .open_vector_search_cursor(
                1,
                &[1.0, 0.0, 0.0, 0.0],
                DistanceMetric::Euclidean,
                1,
                SearchParams {
                    ef: Some(16),
                    rerank_window: None,
                    objective: crate::index::hnsw::HnswSearchObjective::CostOptimized,
                    random_entry_point: Some(false),
                },
                None,
                reopened_again.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
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

        let provider_config = json!({"version": 1, "config": "simple"});
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
        assert_eq!(
            singleton_artifact_segment(artifact),
            ArtifactSegmentRef {
                rowset_id: 1,
                segment_id: 0,
            }
        );
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
        let provider_config = json!({"version": 1, "config": "simple"});
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

        let provider_config = json!({"version": 1, "config": "simple"});
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
    fn explicit_materialization_does_not_publish_incomplete_definition_as_ready() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "create index backfills visible rows",
            ])]))
            .unwrap();

        let provider_config = json!({"version": 1, "config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 49,
            table_id: table.tablet_id(),
            name: "docs_fts_materialized".to_string(),
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
        assert!(!table
            .search_generation_coverage(49)
            .unwrap()
            .expect("coverage before materialization")
            .is_complete());

        let coverage = table.search_registry().materialize_definition(49).unwrap();
        assert!(coverage.is_complete());
        assert_eq!(
            coverage.indexed_segment_count,
            coverage.visible_segment_count
        );
        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&49)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after explicit materialization");
        assert!(manifest.tail_pending_entries.is_empty());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
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

        let provider_config = json!({"version": 1, "config": "simple"});
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

        table
            .search_registry()
            .mutate_view(|view| {
                let state = view
                    .definitions
                    .get_mut(&49)
                    .expect("definition state to tighten freshness");
                state.definition.freshness_policy = SearchFreshnessPolicy::Required;
                if let Some(capability) = state.capability.as_mut() {
                    capability.freshness_policy = SearchFreshnessPolicy::Required;
                }
                Ok((true, ()))
            })
            .unwrap();

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
        let provider_config = json!({"version": 1, "config": "simple"});
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
        let replay_count_before_append = table.search_registry().manifests.full_replay_count();
        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "observer refresh",
            ])]))
            .unwrap();
        assert_eq!(
            table.search_registry().manifests.full_replay_count(),
            replay_count_before_append,
            "rowset publish must install its prepared in-memory manifest without disk replay"
        );

        let current = table.search_registry().view.load();
        let state = current.definitions.get(&43).expect("definition state");
        let manifest = state.manifest.as_ref().expect("manifest after append");
        assert_eq!(manifest.root.build_snapshot_version, table.max_version());
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        assert_eq!(
            singleton_artifact_segment(&manifest.artifacts.artifacts[0]).rowset_id,
            1
        );
    }

    #[test]
    fn unpublished_rowset_with_inline_artifact_is_not_queryability_truth() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"version": 1, "config": "simple"});
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
        let provider_config = json!({"version": 1, "config": "simple"});
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
            .open_fulltext_filter_cursor(
                0,
                &query,
                "simple",
                None,
                table.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
            )
            .unwrap();
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let mut chunks = Vec::new();
        let batch = SearchBatchConfig {
            row_limit: 1024,
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 1024, 4);
        loop {
            match cursor.next_batch(&batch, &mut budget).unwrap() {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => chunks.push(
                    table
                        .materialize_search_batch(
                            &snapshot,
                            batch,
                            &[0],
                            false,
                            Arc::new(default_allocator()),
                        )
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
            artifact.kind == SearchIndexKind::FullText
                && singleton_artifact_segment(artifact).rowset_id > 0
        }));
    }

    #[test]
    fn rowset_publish_failure_preserves_failed_index_head_and_commits_base_rowset() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar, LogicalType::Blob]);
        let fulltext = SearchIndexDefinition {
            table_id: table.tablet_id(),
            ..fulltext_test_definition(200)
        };
        table.register_search_definition(fulltext).unwrap();

        let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
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
        let initial_sparse_head = table
            .tablet()
            .search_generation_head(201)
            .expect("initial sparse head");
        let failing_sparse_root = table
            .search_registry()
            .manifests
            .generation_dir(201, 1)
            .join(format!(
                "manifest_root_g1_v2_f{}.json",
                table
                    .search_registry()
                    .view
                    .load()
                    .definitions
                    .get(&201)
                    .unwrap()
                    .definition
                    .config_fingerprint
            ));
        std::fs::create_dir(&failing_sparse_root).unwrap();

        table
            .append(&test_chunk_from_vectors(vec![
                test_string_vector(&["first definition prepares a candidate"]),
                test_sparse_blob_vector(&[SparseVector::new(vec![1], vec![1.0]).unwrap()]),
            ]))
            .expect("derived sparse manifest failure must not roll back base rowset");
        assert_eq!(table.max_version(), 0);

        let current = table.search_registry().view.load();
        let fulltext_manifest = current
            .definitions
            .get(&200)
            .and_then(|state| state.manifest.as_ref())
            .expect("fulltext manifest after partial search publish");
        assert!(fulltext_manifest.root.root_version > initial_fulltext_root);
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
        assert!(durable_head.root_version > initial_fulltext_root);
        assert_eq!(
            meta.search_generation_heads()
                .iter()
                .find(|head| head.definition_id == 201),
            Some(&initial_sparse_head),
            "a failed derived revision must preserve the last durable head"
        );
        let current = table.search_registry().view.load();
        let sparse_state = current.definitions.get(&201).expect("sparse state");
        assert!(sparse_state.manifest.is_some());
        assert!(
            sparse_state.capability.is_none(),
            "the stale in-memory capability must be disabled without deleting its recovery head"
        );
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

        let provider_config = json!({"version": 1, "config": "simple"});
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
            assert_eq!(manifest.next_tail_entry_id(), TailEntryId(2));
        }

        let touched = table.search_registry().catch_up_definition(44).unwrap();
        assert_eq!(touched, 1);

        let delta_entries = load_manifest_delta_entries(&table, 44);
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::FullText
                    && singleton_artifact_segment(artifact) == (ArtifactSegmentRef {
                        rowset_id: 1,
                        segment_id: 0,
                    })
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
        assert_eq!(manifest.next_tail_entry_id(), TailEntryId(2));
        assert_eq!(manifest.artifacts.artifacts.len(), 1);
        assert!(matches!(
            manifest.artifacts.artifacts[0].location,
            ArtifactLocation::SidecarArtifactFile { .. }
        ));

        let query = FullTextIndex::new_default().parse_query("graph").unwrap();
        let opened = table
            .open_fulltext_filter_cursor(
                0,
                &query,
                "simple",
                None,
                table.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
            )
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

        let provider_config = json!({"version": 1, "config": "simple"});
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

        let current = table.search_registry().view.load();
        let state = current.definitions.get(&144).unwrap();
        let failing_root_version = state
            .manifest
            .as_ref()
            .unwrap()
            .root
            .root_version
            .checked_add(1)
            .unwrap();
        let root_path = table
            .search_registry()
            .manifests
            .generation_dir(144, 1)
            .join(format!(
                "manifest_root_g1_v{}_f{}.json",
                failing_root_version, state.definition.config_fingerprint
            ));
        drop(current);
        std::fs::create_dir(&root_path).unwrap();

        let store = SidecarArtifactStore::new(table.tablet().data_dir().clone());
        let package_path = store.package_path(SidecarArtifactStore::default_shard_file_id(144, 1));
        let delta_path = table
            .search_registry()
            .manifests
            .generation_dir(144, 1)
            .join(format!("delta_g1_v{failing_root_version}_0.json"));

        let err = table
            .search_registry()
            .catch_up_definition(144)
            .expect_err("root path directory must make manifest root publish fail");
        assert!(
            err.to_string()
                .contains("immutable search manifest fragment"),
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

        let provider_config = json!({"version": 1, "config": "simple"});
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

        let provider_config = json!({"version": 1, "config": "simple"});
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

        let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
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
                    && singleton_artifact_segment(artifact).rowset_id == 1
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

        let provider_config = json!({"version": 1, "config": "simple"});
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
        drop(before);

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

        let provider_config = json!({"version": 1, "config": "simple"});
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
        let provider_config = json!({"version": 1, "config": "simple"});
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

        let synthetic_head = {
            let current = table.search_registry().view.load();
            let state = current.definitions.get(&47).expect("definition state");
            let manifest = state.manifest.as_ref().expect("manifest");
            let mut root = manifest.root.clone();
            root.root_version = root.root_version.saturating_add(1);
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
            store.head_for_root(&root)
        };
        table
            .tablet()
            .apply_search_generation_publish(&TabletMutation::PublishSearchGeneration {
                publication: SearchGenerationPublication::AdvanceInstalled,
                generation_ref: table
                    .search_registry()
                    .manifests
                    .generation_ref(47, synthetic_head.generation_id)
                    .unwrap(),
                head: synthetic_head.clone(),
            })
            .unwrap();
        {
            let loaded = table
                .search_registry()
                .manifests
                .load_manifest_for_head(&synthetic_head)
                .expect("load synthetic over-soft manifest")
                .expect("manifest exists");
            table
                .search_registry()
                .mutate_view(|view| {
                    let state = view
                        .definitions
                        .get(&47)
                        .expect("definition state")
                        .clone()
                        .with_manifest(loaded);
                    view.definitions.insert(47, state);
                    Ok((true, ()))
                })
                .unwrap();
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
    fn rowset_publish_compacts_delta_window_without_leaking_transient_delta() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"version": 1, "config": "simple"});
        let definition = SearchIndexDefinition {
            definition_id: 48,
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
                "manifest base",
            ])]))
            .unwrap();

        let synthetic_head = {
            let current = table.search_registry().view.load();
            let state = current.definitions.get(&48).expect("definition state");
            let manifest = state.manifest.as_ref().expect("manifest");
            let mut root = manifest.root.clone();
            root.root_version = root.root_version.checked_add(1).unwrap();
            let store = &table.search_registry().manifests;
            for ordinal in 0..=DELTA_COUNT_SOFT_LIMIT {
                let delta_ref = store
                    .write_delta(
                        48,
                        root.generation_id,
                        root.root_version,
                        ordinal,
                        &ManifestDelta::default(),
                    )
                    .expect("write synthetic delta");
                root.recent_delta_files.push(delta_ref);
            }
            root.recompute_checksum().unwrap();
            store.write_root(48, &root).unwrap();
            store.head_for_root(&root)
        };
        table
            .tablet()
            .apply_search_generation_publish(&TabletMutation::PublishSearchGeneration {
                publication: SearchGenerationPublication::AdvanceInstalled,
                generation_ref: table
                    .search_registry()
                    .manifests
                    .generation_ref(48, synthetic_head.generation_id)
                    .unwrap(),
                head: synthetic_head.clone(),
            })
            .unwrap();
        {
            let loaded = table
                .search_registry()
                .manifests
                .load_manifest_for_head(&synthetic_head)
                .unwrap()
                .unwrap();
            table
                .search_registry()
                .mutate_view(|view| {
                    let state = view
                        .definitions
                        .get(&48)
                        .unwrap()
                        .clone()
                        .with_manifest(loaded);
                    view.definitions.insert(48, state);
                    Ok((true, ()))
                })
                .unwrap();
        }

        table
            .append(&test_chunk_from_vectors(vec![test_string_vector(&[
                "manifest delta compaction",
            ])]))
            .expect("base-table append must survive threshold-triggered compaction");

        let durable_head = table.tablet().search_generation_head(48).unwrap();
        assert_eq!(
            durable_head.root_version,
            synthetic_head.root_version + 1,
            "one rowset publish must consume exactly one manifest revision"
        );
        let loaded = table
            .search_registry()
            .manifests
            .load_manifest_for_head(&durable_head)
            .unwrap()
            .unwrap();
        assert!(loaded.root.recent_delta_files.is_empty());
        assert_eq!(loaded.root.shard_files.len(), 1);

        let transient_prefix = format!(
            "delta_g{}_v{}_",
            durable_head.generation_id, durable_head.root_version
        );
        let generation_dir = table
            .search_registry()
            .manifests
            .generation_dir(48, durable_head.generation_id);
        assert!(
            fs::read_dir(generation_dir)
                .unwrap()
                .flatten()
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&transient_prefix)),
            "delta absorbed into the committed shard must not leak"
        );
    }

    #[test]
    fn fulltext_rowset_replacement_publishes_remove_artifact_delta() {
        let root = TempDir::new().unwrap();
        let table = create_table_with_root(root.path(), &[LogicalType::Varchar]);
        let provider_config = json!({"version": 1, "config": "simple"});
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
                .flat_map(|artifact| {
                    artifact
                        .coverage
                        .segments()
                        .iter()
                        .map(|span| span.segment.rowset_id)
                })
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
            ManifestDeltaEntry::RemoveArtifact(coverage)
                if coverage.contains_segment(ArtifactSegmentRef {
                    rowset_id: 1,
                    segment_id: 0,
                })
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::RemoveArtifact(coverage)
                if coverage.contains_segment(ArtifactSegmentRef {
                    rowset_id: 2,
                    segment_id: 0,
                })
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact)
                if artifact.kind == SearchIndexKind::FullText
                    && singleton_artifact_segment(artifact).rowset_id != 1
                    && singleton_artifact_segment(artifact).rowset_id != 2
        )));

        let current = table.search_registry().view.load();
        let manifest = current
            .definitions
            .get(&45)
            .and_then(|state| state.manifest.as_ref())
            .expect("manifest after compaction");
        assert!(manifest.artifacts.artifacts.iter().all(|artifact| {
            !artifact.coverage.contains_rowset(1) && !artifact.coverage.contains_rowset(2)
        }));
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
            .open_fulltext_filter_cursor(
                0,
                &query,
                "simple",
                None,
                table.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
            )
            .unwrap();
        let mut cursor = opened.cursor;
        let snapshot = opened.snapshot;
        let batch = SearchBatchConfig {
            row_limit: 1024,
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 1024, 4);
        let mut row_count = 0usize;
        loop {
            match cursor.next_batch(&batch, &mut budget).unwrap() {
                SearchBatchState::Ready(batch) if batch.is_empty() => continue,
                SearchBatchState::Ready(batch) => {
                    row_count += table
                        .materialize_search_batch(
                            &snapshot,
                            batch,
                            &[0],
                            false,
                            Arc::new(default_allocator()),
                        )
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

        let provider_config = json!({"version": 1, "config": "simple"});
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
        assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 1);
        assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 2);
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
                    && artifact.coverage == output_artifact.coverage
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

        let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
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
        assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 1);
        assert_ne!(singleton_artifact_segment(output_artifact).rowset_id, 2);
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
                    && artifact.coverage == output_artifact.coverage
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
        let provider_config = json!({"version": 1, "config": "simple"});

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
            assert_ne!(singleton_artifact_segment(artifact).rowset_id, 1);
            assert_ne!(singleton_artifact_segment(artifact).rowset_id, 2);
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
        let table = create_table_without_default_indexes(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 1)],
        );
        let provider_config = test_hnsw_provider_config(1, 16, 64, 0);
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
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![10.0]],
                1,
            )]))
            .unwrap();
        table
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![20.0]],
                1,
            )]))
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
        let table = create_table_without_default_indexes(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 4)],
        );
        let provider_config = test_hnsw_provider_config(4, 16, 64, 0);
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
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![10.0, 0.0, 0.0, 0.0]],
                4,
            )]))
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
        assert!(request.estimated_build_peak_memory_bytes > 0);
    }

    #[test]
    fn hnsw_tail_pending_maintenance_publishes_and_queries_one_multi_segment_partition() {
        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 2)],
        );
        table.bind_search_task_scheduler(Some(Arc::new(TaskScheduler::new())));
        let provider_config = test_hnsw_provider_config(2, 8, 32, 0);
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
        table
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![2.0, 0.0], vec![0.0, 2.0]],
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
            assert_eq!(manifest.tail_pending_entries.len(), 2);
        }

        let report = table.search_registry().maintenance_sweep().unwrap();
        assert_eq!(report.definitions_updated, 1);
        assert_eq!(report.catch_up_rowsets, 2);
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
        assert_eq!(artifact.coverage.segments().len(), 2);
        assert_eq!(artifact.coverage.row_count(), 4);
        let provider_stats = manifest
            .root
            .generation_stats
            .hnsw_provider_stats()
            .expect("hnsw provider stats");
        assert_eq!(provider_stats.vector_count, 4);
        assert_eq!(provider_stats.dimension, 2);

        let delta_entries = load_manifest_delta_entries(&table, 94);
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(1)))));
        assert!(delta_entries
            .iter()
            .any(|entry| matches!(entry, ManifestDeltaEntry::CoverTail(TailEntryId(2)))));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::AddArtifact(artifact) if artifact.kind == SearchIndexKind::Hnsw
        )));
        assert!(delta_entries.iter().any(|entry| matches!(
            entry,
            ManifestDeltaEntry::StatsDelta(SearchStatsDelta::Hnsw(delta))
                if delta.vector_count == 4 && delta.dimension == 2
        )));

        let rowsets = table
            .tablet()
            .capture_consistent_rowsets(table.max_version())
            .unwrap();
        for rowset in &rowsets {
            assert!(
                rowset.segments()[0].hnsw_index(0).is_none(),
                "HNSW TailOnly catch-up must not patch published segment footers"
            );
        }

        let opened = table
            .open_vector_search_cursor(
                0,
                &[1.0, 0.0],
                DistanceMetric::Euclidean,
                4,
                SearchParams::default(),
                None,
                table.max_version(),
                &crate::search::SearchReadOptions::ungoverned(),
            )
            .unwrap();
        let mut cursor = opened.cursor;
        let batch = SearchBatchConfig {
            row_limit: 16,
            preferred_bytes: 1 << 20,
        };
        let mut budget = ResourceBudget::standalone(64 * 1024 * 1024, 16, 2);
        let mut returned = BTreeSet::new();
        while let SearchBatchState::Ready(batch) = cursor.next_batch(&batch, &mut budget).unwrap() {
            returned.extend(
                batch
                    .rows
                    .into_iter()
                    .map(|row| (row.rowset_id, row.segment_id, row.row_offset.get())),
            );
        }
        assert_eq!(returned.len(), 4);
        assert_eq!(
            returned
                .iter()
                .map(|(rowset_id, _, _)| *rowset_id)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );
    }

    #[test]
    fn hnsw_full_snapshot_stores_tail_in_shard_not_root() {
        let root = TempDir::new().unwrap();
        let table = create_table_without_default_indexes(
            root.path(),
            &[LogicalType::Array(Box::new(LogicalType::Float), 1)],
        );
        table
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![10.0]],
                1,
            )]))
            .unwrap();

        let provider_config = test_hnsw_provider_config(1, 16, 64, 0);
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
            assert_eq!(manifest.next_tail_entry_id(), TailEntryId(2));
            assert!(manifest.root.recent_delta_files.is_empty());
            assert_eq!(manifest.root.shard_files.len(), 1);

            let definition_dir = table
                .search_registry()
                .manifests
                .generation_dir(89, manifest.root.generation_id);
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
            .append(&test_chunk_from_vectors(vec![test_embedding_vector(
                &[vec![20.0]],
                1,
            )]))
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
        assert_eq!(manifest.next_tail_entry_id(), TailEntryId(3));

        let root_bytes = std::fs::read(&manifest.root_path).expect("read root");
        let root_json: serde_json::Value =
            serde_json::from_slice(&root_bytes).expect("decode root json");
        assert!(root_json.get("tail_pending_entries").is_none());
    }
}
