// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Table-scoped search definition and queryability registry.
//!
//! The root module owns the lock-free read view and the public definition/query
//! facade. Write admission, durable generation lifecycle, and background
//! maintenance live in dedicated sibling modules so their lock and I/O
//! boundaries remain reviewable in isolation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use paro_scheduler::scheduler::TaskScheduler;

use crate::metrics::storage_metrics;
use crate::rowset::{RowsetId, RowsetSharedPtr};
use crate::tablet::{
    ColumnId, RowsetPublishObserver, SearchGenerationHeadUpdates, TabletId, TabletRef,
};
use paro_common::effect::ArtifactRef;
use paro_common::error::{self as paro_error, Result};

use super::artifact::{
    ArtifactCompactionLayout, ArtifactFileId, ArtifactGcContext, ArtifactLocation, GcDecision,
};
use super::capability::{
    ArtifactSegmentRef, CapabilityToken, SearchArtifactRef, SearchCapability,
    SearchDefinitionOrigin, SearchFreshnessPolicy, SearchIndexDefinition, SearchIndexKind,
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
use super::lifecycle::catch_up_planner::CatchUpWorkItem;
use super::lifecycle::gc::{
    gc_policy_for_kind, hnsw_artifact_compaction_level, hnsw_compaction_level,
};
use super::lifecycle::maintenance_request::provider_maintenance_request_for_definition;
use super::lifecycle::publisher::{
    assign_generation_id, remove_sidecar_packages, retire_paths_for_manifest, search_artifact_key,
    sidecar_file_ids_for_artifacts,
};
use super::maintenance::{
    CatchUpPlanner, DefinitionMaintenanceReport, InlineSearchAdmission, MaintenanceScheduler,
    SearchMaintenanceAction, SearchMaintenanceFailure, SearchMaintenanceReport,
    SearchMaintenanceUrgency,
};
use super::manifest::{
    GenerationManifestRoot, LoadedManifest, ManifestDelta, ManifestDeltaEntry, ManifestShard,
    ManifestStore,
};
use super::providers::hnsw::search::{prewarm_hnsw_generation_readers, HnswReaderActivationPolicy};
use super::sidecar::{SearchReaderRuntime, SidecarArtifactStore};
use super::sidecar_builder::ProviderSidecarArtifactBuilder;
use super::staged_generation::{StagedSearchGeneration, StagedSearchGenerationInit};
use super::stats::MaintenancePriority;
use super::tail::reader_warmup::HnswTailReaderWarmupScheduler;
use super::tail::{
    TailEntryId, TailMutationKind, TailPendingEntry, TailPendingSet, TailRowImageRef,
};
use super::write_path::SearchWriteContext;

mod generation_lifecycle;
mod maintenance;
#[cfg(test)]
mod tests;
mod write_admission;

use self::write_admission::SearchIngestAdmissionState;

const REQUIRED_FRESHNESS_WAIT_SWEEPS: usize = 32;
const DEFINITION_LOCK_SHARDS: usize = 64;
const HNSW_INGEST_BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct RetiredManifest {
    definition_id: u64,
    provider: SearchIndexKind,
    artifacts: Arc<GenerationArtifactSet>,
    sidecar_file_ids: BTreeSet<ArtifactFileId>,
    paths: Vec<PathBuf>,
    retired_at: Instant,
}

#[derive(Debug, Clone)]
struct MaintenanceFailureBackoff {
    consecutive_failures: u32,
    retry_after: Instant,
}

/// Definition-scoped cancellation shared by lifecycle and provider work.
///
/// Expensive provider builds deliberately run without publication locks. A
/// lifecycle operation must therefore be able to invalidate their immutable
/// snapshot before waiting for the single-flight build lane. `retiring`
/// prevents a queued maintenance task from starting in the cancellation-to-
/// tombstone window; `generation` makes an already-running builder observe
/// the invalidation at its next deterministic stop-check barrier.
#[derive(Debug, Default)]
struct DefinitionBuildSignal {
    generation: AtomicU64,
    retiring: AtomicBool,
}

#[derive(Clone, Debug)]
struct DefinitionBuildToken {
    signal: Arc<DefinitionBuildSignal>,
    generation: u64,
}

impl DefinitionBuildToken {
    fn should_stop(&self) -> bool {
        self.signal.retiring.load(Ordering::Acquire)
            || self.signal.generation.load(Ordering::Acquire) != self.generation
    }

    fn stop_check(&self) -> SearchBuildStopCheck {
        let token = self.clone();
        SearchBuildStopCheck::new(move || token.should_stop())
    }
}

struct ActiveSearchMaintenance<'a> {
    count: &'a AtomicUsize,
}

impl<'a> ActiveSearchMaintenance<'a> {
    fn enter(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self { count }
    }
}

impl Drop for ActiveSearchMaintenance<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
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
    /// Cancellation is keyed by the durable definition id rather than the
    /// bounded lock shard. A collision may serialize two builds, but must
    /// never let dropping one definition cancel an unrelated one.
    definition_build_signals: Mutex<BTreeMap<u64, Arc<DefinitionBuildSignal>>>,
    retired: Mutex<Vec<RetiredManifest>>,
    /// Long-lived mmap and decoded-reader owner. Query cursors borrow this
    /// runtime; generation retirement performs lease-safe physical eviction.
    reader_runtime: Arc<SearchReaderRuntime>,
    maintenance_scheduler: Arc<MaintenanceScheduler>,
    hnsw_task_scheduler: RwLock<Option<Arc<TaskScheduler>>>,
    hnsw_tail_reader_warmup: RwLock<Option<HnswTailReaderWarmupScheduler>>,
    maintenance_notifier: RwLock<Option<Arc<dyn Fn(SearchMaintenanceUrgency) + Send + Sync>>>,
    maintenance_failures: Mutex<BTreeMap<u64, MaintenanceFailureBackoff>>,
    active_maintenance_tasks: AtomicUsize,
    ingest_admission: Mutex<SearchIngestAdmissionState>,
    maintenance_progress_changed: Condvar,
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
    fn disable_definition_capability(&self, definition_id: u64) -> Result<()> {
        self.update_registry_view(|view| {
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
            definition_build_signals: Mutex::new(BTreeMap::new()),
            retired: Mutex::new(Vec::new()),
            reader_runtime,
            maintenance_scheduler: Arc::new(MaintenanceScheduler::default()),
            hnsw_task_scheduler: RwLock::new(None),
            hnsw_tail_reader_warmup: RwLock::new(None),
            maintenance_notifier: RwLock::new(None),
            maintenance_failures: Mutex::new(BTreeMap::new()),
            active_maintenance_tasks: AtomicUsize::new(0),
            ingest_admission: Mutex::new(SearchIngestAdmissionState::default()),
            maintenance_progress_changed: Condvar::new(),
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
        *self.hnsw_task_scheduler.write().unwrap() = scheduler.clone();
        *self.hnsw_tail_reader_warmup.write().unwrap() =
            scheduler.map(HnswTailReaderWarmupScheduler::new);
        self.schedule_pending_hnsw_tail_reader_warmup();
    }

    pub(crate) fn bind_maintenance_notifier(
        &self,
        notifier: Option<Arc<dyn Fn(SearchMaintenanceUrgency) + Send + Sync>>,
    ) {
        *self.maintenance_notifier.write().unwrap() = notifier.clone();
        self.reader_runtime
            .bind_hnsw_integrity_failure_notifier(notifier);
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
        self.install_definition_with_origin(
            definition.clone(),
            SearchDefinitionOrigin::catalog(definition.definition_id),
            HnswReaderActivationPolicy::RECOVERY,
        )
    }

    /// Attach a definition whose complete immutable generation was installed
    /// by the current online commit.
    ///
    /// The durable tablet mutation installs the directory and head before the
    /// catalog definition enters the queryable registry view. This online-only
    /// boundary opens typed readers and requests governed authentication; it
    /// never scans a multi-gigabyte mmap while holding the publication guard.
    /// Lazy per-range checks remain the correctness boundary after restart.
    pub(crate) fn install_published_definition(
        &self,
        definition: SearchIndexDefinition,
    ) -> Result<()> {
        self.install_definition_with_origin(
            definition.clone(),
            SearchDefinitionOrigin::catalog(definition.definition_id),
            HnswReaderActivationPolicy::ATTACH_PUBLISHED,
        )
    }

    /// Install the authenticated reader image retained by a transaction-owned
    /// generation after its directory rename and before the catalog definition
    /// becomes query-visible.
    pub(crate) fn adopt_staged_generation_readers(
        &self,
        staged: &StagedSearchGeneration,
    ) -> Result<usize> {
        staged.adopt_prepared_readers_into(self.reader_runtime.as_ref())
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
        let builder = ProviderSidecarArtifactBuilder::new(sidecar_store.clone());
        let hnsw_provider = if definition.kind == SearchIndexKind::Hnsw {
            Some(definition.hnsw_provider_config()?)
        } else {
            None
        };
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
            let prepared_artifacts: Arc<[SearchArtifactRef]> =
                Arc::from(result.artifact_refs.clone());
            let prepared_reader_runtime = if let Some(provider) = hnsw_provider.as_ref() {
                let staged_runtime = Arc::new(SearchReaderRuntime::new(sidecar_store.clone()));
                staged_runtime.bind_buffer_pool(self.reader_runtime.buffer_pool())?;
                prewarm_hnsw_generation_readers(
                    staged_runtime.as_ref(),
                    &result.artifact_refs,
                    &visible_rowsets,
                    *definition.column_ids.first().ok_or_else(|| {
                        paro_error::internal("HNSW staged generation requires one vector column")
                    })?,
                    provider.dimension as usize,
                    &provider.build_contract(),
                    None,
                    HnswReaderActivationPolicy::prepared_publication(
                        crate::index::hnsw::HnswBuildExecutionPolicy::Foreground,
                    ),
                    Some(&stop_check),
                )?;
                Some(staged_runtime)
            } else {
                None
            };

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
                    hnsw_provider.as_ref(),
                    snapshot_version,
                    1,
                    generation_stats.indexed_rows,
                    &tail_pending,
                    tail_pending.delete_rows(),
                    None,
                    Vec::new(),
                )?,
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
                prepared_reader_runtime,
                prepared_artifacts,
            ))
        })();

        let (coverage, head, prepared_reader_runtime, prepared_artifacts) = match build_result {
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
            prepared_reader_runtime,
            prepared_artifacts,
            layout_lease,
        }))
    }

    pub(crate) fn drop_definition(&self, definition_id: u64) -> Result<()> {
        // Signal first, then drain the single-flight lane. Waiting on the lane
        // before publishing cancellation makes DROP depend on the wall-clock
        // duration of a multi-million-row provider build and leaves no
        // operational escape hatch for a bad build contract.
        self.retire_definition_builds(definition_id)?;
        let result = self.drop_definition_after_build_retirement(definition_id);
        if result.is_err() && self.view.load().definitions.contains_key(&definition_id) {
            // A failed lifecycle mutation left the definition visible. Give
            // future maintenance a fresh generation while every token issued
            // before the attempted DROP remains invalidated.
            if let Err(error) = self.activate_definition_builds(definition_id) {
                tracing::error!(
                    tablet_id = self.tablet.tablet_id(),
                    definition_id,
                    error = %error,
                    "failed to reactivate search builds after aborted definition drop"
                );
            }
        }
        result
    }

    fn drop_definition_after_build_retirement(&self, definition_id: u64) -> Result<()> {
        let _build_guard = self.lock_definition_build(definition_id);
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let lifecycle_guard = self
            .lifecycle_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition lifecycle"))?;
        let initial = self.view.load_full();
        let Some(initial_state) = initial.definitions.get(&definition_id).cloned() else {
            self.forget_definition_build_signal(definition_id)?;
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
        self.update_registry_view(|view| {
            view.definitions.remove(&definition_id);
            if let Some((seed_id, seed_state)) = restored_seed {
                view.definitions.entry(seed_id).or_insert(seed_state);
            }
            Ok((true, ()))
        })?;
        // The single-flight guard proves that no provider token for this
        // retired definition remains live. Do not retain one cancellation
        // cell for every definition id ever created by a long-running
        // database; a future reuse installs a fresh signal generation.
        self.forget_definition_build_signal(definition_id)?;
        drop(state);
        drop(definition_guards);
        drop(lifecycle_guard);
        drop(publication_guard);
        if let Some(seed_definition_id) = restored_seed_definition_id {
            self.activate_definition_builds(seed_definition_id)?;
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
        self.view.load().hnsw_search_policy(column_id, distance)
    }

    pub(crate) fn hnsw_generation_statistics(
        &self,
        definition_id: u64,
    ) -> Result<Option<crate::statistics::HnswIndexStatistics>> {
        self.view.load().hnsw_generation_statistics(definition_id)
    }

    pub(crate) fn generation_artifact_count(&self, definition_id: u64) -> Option<usize> {
        self.view.load().generation_artifact_count(definition_id)
    }

    pub(crate) fn active_maintenance_tasks(&self) -> usize {
        self.active_maintenance_tasks.load(Ordering::Acquire)
    }

    pub(crate) fn hnsw_filter_topology(
        &self,
        column_id: ColumnId,
        distance: crate::index::hnsw::DistanceMetric,
    ) -> Option<crate::index::hnsw::HnswFilterTopologyContract> {
        self.view.load().hnsw_filter_topology(column_id, distance)
    }

    fn resolve_capability_with_required_wait(
        &self,
        finder: impl Fn(&SearchView) -> Option<SearchCapability>,
    ) -> Option<SearchCapability> {
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
            let report = self.run_maintenance_pass()?;
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
        self.view
            .load()
            .has_queryable_artifact(kind, rowset_id, segment_id, column_id)
    }

    pub(crate) fn open_generation_snapshot(
        &self,
        definition_id: u64,
    ) -> Result<Option<GenerationReadSnapshot>> {
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
        let current = self.view.load();
        let Some(state) = current.definitions.get(&token.definition_id) else {
            return Ok(OpenSearchCursorResult::NotQueryable);
        };
        let Some(generation) = &state.generation else {
            return Ok(OpenSearchCursorResult::NotQueryable);
        };
        if token.is_stale(generation.generation_id, generation.root_version) {
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
        // Foreground DDL/OPTIMIZE must plan against the latest durable rowset
        // graph. A stale in-memory generation can otherwise look vacuously
        // complete (zero visible segments) and let explicit materialization
        // return without indexing rows committed immediately beforehand.
        self.refresh_all_definitions();
        let mut previous = self.generation_coverage(definition_id)?.ok_or_else(|| {
            paro_error::artifact_not_ready(format!(
                "search definition {definition_id} has no materialized generation"
            ))
        })?;
        while !previous.is_complete() {
            let updated = self.catch_up_definition_with_mode(definition_id, true)?;
            let next = self.generation_coverage(definition_id)?.ok_or_else(|| {
                paro_error::artifact_not_ready(format!(
                    "search definition {definition_id} disappeared while materializing"
                ))
            })?;
            if next == previous {
                return Err(paro_error::artifact_not_ready(format!(
                    "search definition {definition_id} made no foreground materialization progress ({}/{}, updated={updated})",
                    next.indexed_segment_count, next.visible_segment_count,
                )));
            }
            previous = next;
        }
        Ok(previous)
    }

    /// Materialize one catalog-owned search definition by its durable name.
    /// Search generations and rowset compaction are independent physical
    /// lifecycles, so REFRESH VECTOR INDEX never rewrites base table data.
    pub(crate) fn materialize_catalog_definition_by_name(
        &self,
        definition_name: &str,
    ) -> Result<SearchGenerationCoverage> {
        self.refresh_all_definitions();
        let definition_ids = self
            .view
            .load()
            .definitions
            .iter()
            .filter_map(|(definition_id, state)| {
                (state.origin.is_catalog_index() && state.definition.name == definition_name)
                    .then_some(*definition_id)
            })
            .collect::<Vec<_>>();
        let [definition_id] = definition_ids.as_slice() else {
            return match definition_ids.len() {
                0 => Err(paro_error::object_not_found(
                    "search index",
                    definition_name,
                )),
                count => Err(paro_error::data_corrupted(format!(
                    "catalog search index {definition_name} resolves to {count} definitions"
                ))),
            };
        };
        self.materialize_definition(*definition_id)
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
}

fn manifest_accounted_rowsets(manifest: &LoadedManifest) -> BTreeSet<RowsetId> {
    manifest
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
        .chain(
            manifest
                .tail_pending_entries
                .iter()
                .filter(|entry| entry.mutation != TailMutationKind::Delete)
                .map(|entry| entry.rowset_id),
        )
        .collect()
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

fn select_hnsw_compaction_artifacts(
    artifacts: &[SearchArtifactRef],
    provider: &crate::search::HnswProviderConfig,
) -> Vec<SearchArtifactRef> {
    let target_rows = provider.maintenance_target_rows();
    let fanout = provider.maintenance.compaction_fanout;
    let row_counts = artifacts
        .iter()
        .map(|artifact| artifact.stats.row_count)
        .collect::<Vec<_>>();
    let Some(level) = hnsw_compaction_level(&row_counts, target_rows, fanout) else {
        return Vec::new();
    };
    let mut selected = artifacts
        .iter()
        .filter(|artifact| {
            hnsw_artifact_compaction_level(artifact.stats.row_count, target_rows, fanout) == level
        })
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by_key(|artifact| {
        artifact
            .coverage
            .segments()
            .first()
            .map(|span| span.segment)
    });
    selected.truncate(usize::try_from(fanout).unwrap_or(usize::MAX));
    let shard_limit = provider.generation_layout.target_graph_rows;
    while selected.len() > 1
        && selected.iter().fold(0u64, |rows, artifact| {
            rows.saturating_add(artifact.stats.row_count)
        }) > shard_limit
    {
        selected.pop();
    }
    if selected.len() > 1 {
        selected
    } else {
        Vec::new()
    }
}

fn hnsw_compaction_build_input(
    selected_artifacts: &[SearchArtifactRef],
    visible_rowsets: &BTreeMap<RowsetId, RowsetSharedPtr>,
) -> Result<(Vec<TailPendingEntry>, Vec<RowsetSharedPtr>)> {
    let mut segments_by_rowset = BTreeMap::<RowsetId, BTreeMap<u32, u64>>::new();
    for artifact in selected_artifacts {
        if artifact.kind != SearchIndexKind::Hnsw {
            return Err(paro_error::invalid_input(
                "HNSW compaction input contains a non-HNSW artifact",
            ));
        }
        for span in artifact.coverage.segments() {
            if segments_by_rowset
                .entry(span.segment.rowset_id)
                .or_default()
                .insert(span.segment.segment_id, span.row_count)
                .is_some()
            {
                return Err(paro_error::data_corrupted(format!(
                    "HNSW compaction input overlaps segment {}/{}",
                    span.segment.rowset_id, span.segment.segment_id
                )));
            }
        }
    }

    let mut tail_window = Vec::with_capacity(segments_by_rowset.len());
    let mut rowset_refs = Vec::with_capacity(segments_by_rowset.len());
    for (rowset_id, segments) in segments_by_rowset {
        let Some(rowset) = visible_rowsets.get(&rowset_id).cloned() else {
            // A table-layout publication won the race. The caller will retry
            // from a fresh immutable artifact set on the next level-triggered
            // maintenance pass.
            return Ok((Vec::new(), Vec::new()));
        };
        rowset.load()?;
        let physical_rows = rowset
            .segments()
            .iter()
            .map(|segment| (segment.segment_id(), segment.num_rows() as u64))
            .collect::<BTreeMap<_, _>>();
        for (&segment_id, &row_count) in &segments {
            if physical_rows.get(&segment_id).copied() != Some(row_count) {
                return Ok((Vec::new(), Vec::new()));
            }
        }
        tail_window.push(TailPendingEntry {
            entry_id: TailEntryId::UNASSIGNED,
            rowset_id,
            segment_ids: segments.keys().copied().collect(),
            mutation: TailMutationKind::Append,
            row_count: segments.values().copied().sum(),
            byte_count: rowset.data_disk_size(),
            row_image_ref: Some(TailRowImageRef::WholeRowset),
        });
        rowset_refs.push(rowset);
    }
    Ok((tail_window, rowset_refs))
}

fn validate_hnsw_compaction_result(
    definition: &SearchIndexDefinition,
    generation_id: u64,
    removed_artifacts: &[SearchArtifactRef],
    added_artifacts: &[SearchArtifactRef],
) -> Result<()> {
    if added_artifacts.is_empty() {
        return Err(paro_error::data_corrupted(
            "HNSW generation compaction produced no artifacts",
        ));
    }
    let coverage_rows = |artifacts: &[SearchArtifactRef]| -> Result<BTreeMap<_, _>> {
        let mut rows = BTreeMap::new();
        for artifact in artifacts {
            artifact.validate()?;
            if artifact.definition_id != definition.definition_id
                || artifact.generation_id != generation_id
                || artifact.kind != SearchIndexKind::Hnsw
            {
                return Err(paro_error::data_corrupted(
                    "HNSW compaction artifact identity mismatch",
                ));
            }
            for span in artifact.coverage.segments() {
                if rows.insert(span.segment, span.row_count).is_some() {
                    return Err(paro_error::data_corrupted(
                        "HNSW compaction artifact coverage overlaps",
                    ));
                }
            }
        }
        Ok(rows)
    };
    if coverage_rows(removed_artifacts)? != coverage_rows(added_artifacts)? {
        return Err(paro_error::data_corrupted(
            "HNSW generation compaction changed physical segment coverage",
        ));
    }
    let shard_limit = definition
        .hnsw_provider_config()?
        .generation_layout
        .target_graph_rows;
    if added_artifacts.iter().any(|artifact| {
        artifact.stats.row_count > shard_limit && artifact.coverage.segments().len() != 1
    }) {
        return Err(paro_error::data_corrupted(format!(
            "HNSW generation compaction exceeded its {shard_limit}-row graph-shard contract"
        )));
    }
    Ok(())
}

/// Prove that a freshness build materialized exactly the immutable tail
/// quantum admitted by the planner.
///
/// Catch-up is append-only derived-state publication. A partial provider
/// result must not be treated as successful progress, and output must never
/// reach into tail entries that were not part of this build quantum.
fn validate_catch_up_artifact_coverage(
    definition: &SearchIndexDefinition,
    generation_id: u64,
    planned: &[CatchUpWorkItem],
    artifacts: &[SearchArtifactRef],
) -> Result<()> {
    let mut expected = BTreeMap::new();
    for item in planned {
        let requested_segments = item
            .tail_entry
            .segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut found_segments = BTreeSet::new();
        for segment in item.rowset.segments() {
            if !requested_segments.contains(&segment.segment_id()) {
                continue;
            }
            found_segments.insert(segment.segment_id());
            let row_count = segment.num_rows() as u64;
            let segment_ref = ArtifactSegmentRef {
                rowset_id: item.tail_entry.rowset_id,
                segment_id: segment.segment_id(),
            };
            for column_id in &definition.column_ids {
                if expected
                    .insert((segment_ref, *column_id), row_count)
                    .is_some()
                {
                    return Err(paro_error::data_corrupted(
                        "search catch-up plan contains overlapping segment coverage",
                    ));
                }
            }
        }
        if found_segments != requested_segments {
            return Err(paro_error::data_corrupted(format!(
                "search catch-up tail entry {} names segments absent from retained rowset {}",
                item.tail_entry.entry_id.0, item.tail_entry.rowset_id
            )));
        }
    }

    let mut actual = BTreeMap::new();
    for artifact in artifacts {
        artifact.validate()?;
        if artifact.definition_id != definition.definition_id
            || artifact.generation_id != generation_id
            || artifact.kind != definition.kind
        {
            return Err(paro_error::data_corrupted(
                "search catch-up artifact identity mismatch",
            ));
        }
        for span in artifact.coverage.segments() {
            if actual
                .insert((span.segment, artifact.column_id), span.row_count)
                .is_some()
            {
                return Err(paro_error::data_corrupted(
                    "search catch-up artifact coverage overlaps",
                ));
            }
        }
    }
    if actual != expected {
        return Err(paro_error::data_corrupted(
            "search catch-up artifact coverage differs from its admitted immutable tail quantum",
        ));
    }
    Ok(())
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
