// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Level-triggered search-index maintenance and integrity recovery.

use super::*;
use crate::search::maintenance::sidecar_repack_needed;

impl SearchIndexRegistry {
    pub(crate) fn catch_up_definition(&self, definition_id: u64) -> Result<usize> {
        self.catch_up_definition_with_mode(definition_id, false)
    }

    pub(super) fn catch_up_definition_with_mode(
        &self,
        definition_id: u64,
        force_complete: bool,
    ) -> Result<usize> {
        self.refresh_all_definitions();
        // Foreground OPTIMIZE and the background coordinator share the same
        // provider build lane. Re-read immutable input only after acquiring
        // ownership so a waiter observes the winner's published revision
        // instead of rebuilding and discarding the same tail.
        let _build_guard = self.lock_definition_build(definition_id);
        let Some(build_token) = self.begin_definition_build(definition_id)? else {
            return Ok(0);
        };
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

        let catch_up_plan = if force_complete {
            CatchUpPlanner.plan_all(&state.definition, manifest, &visible_by_id)?
        } else {
            CatchUpPlanner.plan(&state.definition, manifest, &visible_by_id)?
        };
        let newly_materialized_rowsets = catch_up_plan.len();
        if newly_materialized_rowsets == 0 {
            return Ok(0);
        }

        let mut rowset_refs = catch_up_plan
            .items
            .iter()
            .map(|item| item.rowset.clone())
            .map(|rowset| (rowset.rowset_id(), rowset))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        rowset_refs.sort_by_key(|rowset| rowset.rowset_id());
        let mut tail_window = catch_up_plan
            .items
            .iter()
            .map(|item| item.tail_entry.clone())
            .collect::<Vec<_>>();
        tail_window.sort_by(|left, right| {
            left.rowset_id
                .cmp(&right.rowset_id)
                .then_with(|| left.segment_ids.cmp(&right.segment_ids))
        });

        let sidecar_store = SidecarArtifactStore::new(self.tablet.data_dir().clone());
        let builder = if force_complete {
            ProviderSidecarArtifactBuilder::new(sidecar_store.clone())
        } else {
            ProviderSidecarArtifactBuilder::for_maintenance(
                sidecar_store.clone(),
                manifest.root.maintenance_state.recovery.priority,
            )
        };
        let input = SidecarBuildInput {
            definition: state.definition.clone(),
            generation_id: manifest.root.generation_id,
            tail_window,
            rowset_refs,
            snapshot_version: self.tablet.max_version(),
            stop_check: Some(build_token.stop_check()),
        };
        let estimate = builder.estimate_cost(&input)?;
        let result = match builder.build(
            input,
            &BuildBudget {
                cost_envelope: estimate.cost,
                deadline: None,
                grant_id: None,
            },
        ) {
            Ok(result) => result,
            Err(error) if error.is_query_canceled() && build_token.should_stop() => return Ok(0),
            Err(error) => return Err(error),
        };
        if result.artifact_refs.is_empty() {
            return Ok(0);
        }
        let sidecar_file_ids = sidecar_file_ids_for_artifacts(&result.artifact_refs);
        if let Err(error) = validate_catch_up_artifact_coverage(
            &state.definition,
            manifest.root.generation_id,
            &catch_up_plan.items,
            &result.artifact_refs,
        ) {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Err(error);
        }
        if let Err(error) = self.activate_artifact_readers(
            &state,
            &result.artifact_refs,
            HnswReaderActivationPolicy::PREPARED_PUBLICATION,
        ) {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Err(error);
        }
        // Expensive provider work intentionally runs without publication or
        // definition locks, including immutable reader activation. Re-enter
        // the ordered publication critical section only for the manifest
        // append, WAL record, head CAS, and in-memory view CAS.
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
        let latest = self.view.load_full();
        let Some(latest_state) = latest.definitions.get(&definition_id).cloned() else {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Ok(0);
        };
        drop(latest);
        if latest_state.definition != state.definition
            || latest_state.origin != state.origin
            || !Self::catch_up_append_rebaseable(&latest_state, &result.artifact_refs)
        {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Ok(0);
        }

        let next_state =
            match self.publish_sidecar_catch_up_delta(&latest_state, result.artifact_refs) {
                Ok(next_state) => next_state,
                Err(err) => {
                    self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
                    return Err(err);
                }
            };
        let completion =
            match self.publish_generation_head_for_state(&next_state, &publication_guard) {
                Ok(completion) => completion,
                Err(error) => {
                    self.retire_unpublished_revision(&latest_state, &next_state);
                    return Err(error);
                }
            };
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
        self.signal_maintenance_progress()?;
        Ok(newly_materialized_rowsets)
    }

    /// Coalesce an immutable HNSW artifact prefix without rewriting table
    /// rowsets. Provider work runs against retained rowset handles; publish
    /// replaces only the exact artifact identities captured at the start, so
    /// concurrent ingest can append disjoint tail/artifacts and be rebased.
    pub(crate) fn compact_hnsw_generation(
        &self,
        definition_id: u64,
        force_rebuild: bool,
    ) -> Result<bool> {
        self.refresh_all_definitions();
        let _build_guard = self.lock_definition_build(definition_id);
        let Some(build_token) = self.begin_definition_build(definition_id)? else {
            return Ok(false);
        };
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(false);
        };
        drop(current);
        if state.definition.kind != SearchIndexKind::Hnsw {
            return Ok(false);
        }
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(false);
        };
        let selected_artifacts = if force_rebuild {
            manifest.artifacts.artifacts.clone()
        } else {
            let provider = state
                .hnsw_provider_config
                .as_ref()
                .ok_or_else(|| paro_error::internal("HNSW compaction requires provider config"))?;
            select_hnsw_compaction_artifacts(&manifest.artifacts.artifacts, provider)
        };
        if selected_artifacts.is_empty() || (!force_rebuild && selected_artifacts.len() <= 1) {
            return Ok(false);
        }
        {
            let admission = self
                .ingest_admission
                .lock()
                .map_err(|_| paro_error::internal("lock search ingest admission"))?;
            let has_unmanifested_debt = admission
                .unmanifested_hnsw
                .get(&definition_id)
                .is_some_and(|rowsets| !rowsets.is_empty());
            if admission.reserved_rows > 0 || admission.reserved_bytes > 0 || has_unmanifested_debt
            {
                // Generation compaction is optional and must never enter the
                // build executor ahead of freshness work already admitted by
                // foreground writers. A sub-quantum immutable tail is not an
                // admitted build: compaction preserves it verbatim and must be
                // allowed to reduce existing graph fan-out while it waits for
                // the next complete L0 digit.
                return Ok(false);
            }
        }
        let generation_id = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("HNSW compaction requires a generation"))?
            .generation_id;

        let snapshot_version = self.tablet.max_version();
        let visible_rowsets = self.tablet.capture_consistent_rowsets(snapshot_version)?;
        let visible_by_id = visible_rowsets
            .into_iter()
            .map(|rowset| (rowset.rowset_id(), rowset))
            .collect::<BTreeMap<_, _>>();
        let (tail_window, rowset_refs) =
            hnsw_compaction_build_input(&selected_artifacts, &visible_by_id)?;
        if tail_window.is_empty() {
            return Ok(false);
        }

        let sidecar_store = SidecarArtifactStore::new(self.tablet.data_dir().clone());
        let builder = ProviderSidecarArtifactBuilder::for_maintenance(
            sidecar_store.clone(),
            manifest.root.maintenance_state.recovery.priority,
        );
        let build_epoch = self.foreground_ingest_epoch.load(Ordering::Acquire);
        let foreground_epoch = Arc::clone(&self.foreground_ingest_epoch);
        let definition_token = build_token.clone();
        let stop_check = SearchBuildStopCheck::new(move || {
            foreground_epoch.load(Ordering::Acquire) != build_epoch
                || definition_token.should_stop()
        });
        let input = SidecarBuildInput {
            definition: state.definition.clone(),
            generation_id,
            tail_window,
            rowset_refs,
            snapshot_version,
            stop_check: Some(stop_check),
        };
        let estimate = builder.estimate_cost(&input)?;
        let result = match builder.build(
            input,
            &BuildBudget {
                cost_envelope: estimate.cost,
                deadline: None,
                grant_id: None,
            },
        ) {
            Ok(result) => result,
            Err(error)
                if error.is_query_canceled()
                    && (self.foreground_ingest_epoch.load(Ordering::Acquire) != build_epoch
                        || build_token.should_stop()) =>
            {
                // Foreground ingest and definition replacement preempt graph
                // coalescing because they change its immutable input. Reads
                // do not: the maintenance build policy already shrinks to one
                // lane at deterministic wave barriers while queries are live.
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let sidecar_file_ids = sidecar_file_ids_for_artifacts(&result.artifact_refs);
        if let Err(error) = validate_hnsw_compaction_result(
            &state.definition,
            generation_id,
            &selected_artifacts,
            &result.artifact_refs,
        ) {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Err(error);
        }
        if let Err(error) = self.activate_artifact_readers(
            &state,
            &result.artifact_refs,
            HnswReaderActivationPolicy::PREPARED_PUBLICATION,
        ) {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Err(error);
        }

        // Re-enter the ordered publication section only after the graph has
        // been built and its readers activated. Every selected artifact must
        // still be present verbatim; otherwise rowset replacement or another
        // compaction won the race.
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
        let latest = self.view.load_full();
        let Some(latest_state) = latest.definitions.get(&definition_id).cloned() else {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Ok(false);
        };
        drop(latest);
        let selected_still_live = latest_state.definition == state.definition
            && latest_state.origin == state.origin
            && latest_state
                .generation
                .as_ref()
                .is_some_and(|generation| generation.generation_id == generation_id)
            && latest_state.manifest.as_ref().is_some_and(|manifest| {
                selected_artifacts.iter().all(|selected| {
                    manifest
                        .artifacts
                        .artifacts
                        .iter()
                        .any(|current| current == selected)
                })
            });
        if !selected_still_live {
            self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
            return Ok(false);
        }

        let next_state = match self.publish_hnsw_compaction_delta(
            &latest_state,
            &selected_artifacts,
            result.artifact_refs,
        ) {
            Ok(next_state) => next_state,
            Err(error) => {
                self.discard_unpublished_sidecars(&sidecar_store, &sidecar_file_ids);
                return Err(error);
            }
        };
        let completion =
            match self.publish_generation_head_for_state(&next_state, &publication_guard) {
                Ok(completion) => completion,
                Err(error) => {
                    self.retire_unpublished_revision(&latest_state, &next_state);
                    return Err(error);
                }
            };
        let view_result =
            self.publish_durable_revision_state(&latest_state, next_state, &completion);
        drop(_guard);
        drop(publication_guard);
        completion.finish()?;
        view_result?;
        self.sweep_retired();
        self.signal_maintenance_progress()?;
        Ok(true)
    }

    /// A catch-up build owns only its admitted immutable tail prefix.
    ///
    /// Foreground ingest may append new tail entries while provider work runs;
    /// those appends are safe to preserve. Catch-up never rewrites an existing
    /// artifact: graph coalescing is an independently admitted, preemptible
    /// generation-compaction action.
    pub(super) fn catch_up_append_rebaseable(
        latest_state: &SearchDefinitionState,
        added_artifacts: &[SearchArtifactRef],
    ) -> bool {
        let (Some(manifest), Some(generation)) = (
            latest_state.manifest.as_ref(),
            latest_state.generation.as_ref(),
        ) else {
            return false;
        };
        let pending_segments = manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| !matches!(entry.mutation, TailMutationKind::Delete))
            .flat_map(|entry| {
                entry
                    .segment_ids
                    .iter()
                    .map(move |segment_id| (entry.rowset_id, *segment_id))
            })
            .collect::<BTreeSet<_>>();
        !added_artifacts.is_empty()
            && added_artifacts.iter().all(|artifact| {
                artifact.definition_id == latest_state.definition.definition_id
                    && artifact.kind == latest_state.definition.kind
                    && artifact.generation_id == generation.generation_id
                    && artifact.coverage.segments().iter().all(|span| {
                        pending_segments
                            .contains(&(span.segment.rowset_id, span.segment.segment_id))
                    })
            })
    }

    pub(crate) fn bootstrap_migration(&self) -> Result<SearchBootstrapReport> {
        self.refresh_all_definitions();
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
            // Bootstrap is an explicit migration boundary, not a background
            // maintenance quantum. Materialize every currently resolvable
            // tail entry so the report describes the physical work actually
            // published instead of depending on scheduler rate limits.
            let updated = self.catch_up_definition_with_mode(definition_id, true)?;
            if updated > 0 {
                report.definitions_updated += 1;
                report.rowsets_materialized += updated;
            }
        }
        Ok(report)
    }

    pub(crate) fn run_maintenance_pass(&self) -> Result<SearchMaintenanceReport> {
        // A lease can outlive the publication that retired its artifacts. Revisit the
        // queue even when this sweep finds no definition work, including after the last
        // definition was dropped.
        self.sweep_retired();
        self.refresh_all_definitions();
        let current = self.view.load_full();
        let definition_ids = current.definitions.keys().copied().collect::<Vec<_>>();
        let mut report = SearchMaintenanceReport {
            definitions_considered: definition_ids.len(),
            ..SearchMaintenanceReport::default()
        };

        drop(current);

        // Integrity failures are durable derived-state transitions, not
        // retryable verification jobs. Remove each failed secondary artifact
        // from the manifest and restore its immutable base coverage as exact
        // tail before ordinary optimization work is planned. Recovery then
        // observes the same degraded-but-correct state instead of reattaching
        // a reader that this process already proved corrupt.
        report.definitions_updated = report
            .definitions_updated
            .saturating_add(self.recover_hnsw_integrity_failures()?);

        let mut planned = Vec::new();
        for definition_id in definition_ids {
            if self
                .maintenance_failures
                .lock()
                .map_err(|_| paro_error::internal("lock search maintenance failure backoff"))?
                .get(&definition_id)
                .is_some_and(|failure| Instant::now() < failure.retry_after)
            {
                report.retry_deferred_definitions =
                    report.retry_deferred_definitions.saturating_add(1);
                continue;
            }
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
                artifact_count: manifest.artifacts.artifacts.len(),
                indexed_rows: manifest.root.generation_stats.indexed_rows,
                largest_artifact_rows: manifest
                    .artifacts
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.stats.row_count)
                    .max()
                    .unwrap_or_default(),
                compaction_layout: state.hnsw_provider_config.as_ref().map(|config| {
                    ArtifactCompactionLayout::HnswLevelled {
                        artifact_row_counts: manifest
                            .artifacts
                            .artifacts
                            .iter()
                            .map(|artifact| artifact.stats.row_count)
                            .collect(),
                        target_rows: config.maintenance_target_rows(),
                        fanout: config.maintenance.compaction_fanout,
                    }
                }),
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
            let provider_request =
                match provider_maintenance_request_for_definition(&state, manifest) {
                    Ok(request) => request,
                    Err(error) => {
                        self.record_maintenance_failure(definition_id)?;
                        report.failures.push(SearchMaintenanceFailure {
                            definition_id: Some(definition_id),
                            message: error.to_string(),
                        });
                        continue;
                    }
                };
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
        let admissions = self.maintenance_scheduler.schedule_next_request(&requests);
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

        // One admitted definition is one table-level quantum. The database
        // coordinator visits every table before returning here, so a large
        // definition set cannot monopolize the shared HNSW build executor.
        if let Some(task) = self.maintenance_scheduler.pop_next_task() {
            let _grant_lease = self.maintenance_scheduler.scoped_task_lease(&task);
            let _active_maintenance =
                ActiveSearchMaintenance::enter(&self.active_maintenance_tasks);
            let definition_id = task.request.definition_id;
            let task_result = match task.request.action {
                SearchMaintenanceAction::CatchUp => self
                    .catch_up_definition(definition_id)
                    .map(|touched| (touched > 0, touched)),
                SearchMaintenanceAction::CompactManifestDelta => self
                    .compact_manifest_deltas_for_definition(definition_id)
                    .map(|updated| (updated, 0)),
                SearchMaintenanceAction::RepackSidecar => self
                    .repack_sidecars_for_definition(definition_id)
                    .map(|repacked| (repacked > 0, 0)),
                SearchMaintenanceAction::Compact => {
                    report.compaction_requested = true;
                    self.compact_hnsw_generation(definition_id, false)
                        .map(|updated| (updated, 0))
                }
                SearchMaintenanceAction::Rebuild => {
                    report.compaction_requested = true;
                    self.compact_hnsw_generation(definition_id, true)
                        .map(|updated| (updated, 0))
                }
                SearchMaintenanceAction::Skip => Ok((false, 0)),
            };
            match task_result {
                Ok((updated, catch_up_rowsets)) => {
                    self.clear_maintenance_failure(definition_id)?;
                    report.definitions_updated += usize::from(updated);
                    report.catch_up_rowsets =
                        report.catch_up_rowsets.saturating_add(catch_up_rowsets);
                }
                Err(error) => {
                    self.record_maintenance_failure(definition_id)?;
                    report.failures.push(SearchMaintenanceFailure {
                        definition_id: Some(definition_id),
                        message: error.to_string(),
                    });
                }
            }
        }

        Ok(report)
    }

    pub(super) fn recover_hnsw_integrity_failures(&self) -> Result<usize> {
        let failures = self.reader_runtime.drain_hnsw_integrity_failures();
        if failures.is_empty() {
            return Ok(0);
        }
        let mut by_definition = BTreeMap::<u64, Vec<SearchArtifactRef>>::new();
        for artifact in failures {
            by_definition
                .entry(artifact.definition_id)
                .or_default()
                .push(artifact);
        }

        let mut updated = 0usize;
        let mut first_error = None;
        for (definition_id, artifacts) in by_definition {
            match self.recover_hnsw_definition_from_integrity_failures(definition_id, &artifacts) {
                Ok(changed) => updated = updated.saturating_add(usize::from(changed)),
                Err(error) => {
                    self.reader_runtime
                        .restore_hnsw_integrity_failures(artifacts);
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(updated),
        }
    }

    pub(super) fn recover_hnsw_definition_from_integrity_failures(
        &self,
        definition_id: u64,
        failed: &[SearchArtifactRef],
    ) -> Result<bool> {
        let _build_guard = self.lock_definition_build(definition_id);
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _definition_guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;

        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(false);
        };
        drop(current);
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(false);
        };
        let removed = manifest
            .artifacts
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.kind == SearchIndexKind::Hnsw
                    && failed.iter().any(|failed| failed == *artifact)
            })
            .cloned()
            .collect::<Vec<_>>();
        if removed.is_empty() {
            return Ok(false);
        }

        let retained = manifest
            .artifacts
            .artifacts
            .iter()
            .filter(|artifact| !removed.iter().any(|removed| removed == *artifact))
            .cloned()
            .collect::<Vec<_>>();

        let mut recovery_by_rowset = BTreeMap::<u64, (Vec<u32>, u64, u64)>::new();
        for artifact in &removed {
            for span in artifact.coverage.segments() {
                let entry = recovery_by_rowset
                    .entry(span.segment.rowset_id)
                    .or_default();
                entry.0.push(span.segment.segment_id);
                entry.1 = entry.1.saturating_add(span.row_count);
                entry.2 = entry.2.saturating_add(
                    artifact
                        .stats
                        .bytes_on_disk
                        .saturating_mul(span.row_count)
                        .div_ceil(artifact.stats.row_count.max(1)),
                );
            }
        }
        let mut added_tail = recovery_by_rowset
            .into_iter()
            .map(|(rowset_id, (mut segment_ids, row_count, byte_count))| {
                segment_ids.sort_unstable();
                segment_ids.dedup();
                TailPendingEntry {
                    entry_id: TailEntryId::UNASSIGNED,
                    rowset_id,
                    segment_ids,
                    mutation: TailMutationKind::Append,
                    row_count,
                    byte_count,
                    row_image_ref: Some(TailRowImageRef::WholeRowset),
                }
            })
            .filter(|entry| !tail_entry_already_live(&manifest.tail_pending_entries, entry))
            .collect::<Vec<_>>();
        let mut next_tail_entry_id = manifest.next_tail_entry_id().0;
        assign_tail_entry_ids(&mut added_tail, &mut next_tail_entry_id);

        let mut root = manifest.root.clone();
        root.build_epoch = state.next_build_epoch;
        root.persisted_tail_entry_id_seed = TailEntryId(next_tail_entry_id);
        root.generation_stats = generation_stats_after_artifact_replacement(
            &state.definition,
            &manifest.root.generation_stats,
            &removed,
            &[],
            &retained,
        )?;
        let mut tail_entries = manifest.tail_pending_entries.clone();
        tail_entries.extend(added_tail.iter().cloned());
        let tail_pending = TailPendingSet {
            entries: tail_entries,
        };
        root.coverage = coverage_for_definition(&state.definition, &tail_pending);
        root.execution_modes = execution_modes_for_definition(&state.definition, &root.coverage);
        root.maintenance_state = build_maintenance_state(
            &state.definition,
            state.hnsw_provider_config.as_deref(),
            root.build_snapshot_version,
            root.build_epoch,
            root.generation_stats.indexed_rows,
            &tail_pending,
            tail_pending.delete_rows(),
            Some(manifest.root.build_epoch),
            manifest
                .root
                .maintenance_state
                .recovery
                .superseded_build_epochs
                .clone(),
        )?;

        let mut entries = removed
            .iter()
            .map(|artifact| ManifestDeltaEntry::RemoveArtifact(artifact.coverage.clone()))
            .collect::<Vec<_>>();
        entries.extend(
            added_tail
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::UpsertTail),
        );
        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, manifest)?;
        revision.append_delta(&ManifestDelta::new(entries))?;
        let loaded = revision.commit()?;
        let next_state = state.clone().with_manifest(loaded);
        let completion =
            match self.publish_generation_head_for_state(&next_state, &publication_guard) {
                Ok(completion) => completion,
                Err(error) => {
                    self.retire_unpublished_revision(&state, &next_state);
                    return Err(error);
                }
            };
        let view_result =
            self.publish_durable_revision_state(&state, next_state.clone(), &completion);
        drop(_definition_guard);
        drop(publication_guard);
        completion.finish()?;
        view_result?;
        record_tail_metrics_for_state(&next_state);
        self.signal_maintenance_progress()?;
        tracing::error!(
            tablet_id = self.tablet.tablet_id(),
            definition_id,
            quarantined_artifacts = removed.len(),
            "HNSW checksum failure persisted as exact-tail recovery work"
        );
        Ok(true)
    }

    pub(super) fn record_maintenance_failure(&self, definition_id: u64) -> Result<()> {
        let mut failures = self
            .maintenance_failures
            .lock()
            .map_err(|_| paro_error::internal("lock search maintenance failure backoff"))?;
        let consecutive_failures = failures
            .get(&definition_id)
            .map_or(1, |failure| failure.consecutive_failures.saturating_add(1));
        let exponent = consecutive_failures.saturating_sub(1).min(6);
        failures.insert(
            definition_id,
            MaintenanceFailureBackoff {
                consecutive_failures,
                retry_after: Instant::now() + Duration::from_secs(1u64 << exponent),
            },
        );
        Ok(())
    }

    pub(super) fn clear_maintenance_failure(&self, definition_id: u64) -> Result<()> {
        self.maintenance_failures
            .lock()
            .map_err(|_| paro_error::internal("lock search maintenance failure backoff"))?
            .remove(&definition_id);
        Ok(())
    }

    pub(super) fn signal_maintenance_progress(&self) -> Result<()> {
        let current = self.view.load_full();
        let visible_rowset_ids = self
            .tablet
            .capture_consistent_rowsets(self.tablet.max_version())?
            .into_iter()
            .map(|rowset| rowset.rowset_id())
            .collect::<BTreeSet<_>>();
        let accounted_by_definition = current
            .definitions
            .iter()
            .filter_map(|(definition_id, state)| {
                state
                    .manifest
                    .as_ref()
                    .map(|manifest| (*definition_id, manifest_accounted_rowsets(manifest)))
            })
            .collect::<BTreeMap<_, _>>();
        let mut admission = self
            .ingest_admission
            .lock()
            .map_err(|_| paro_error::internal("lock search maintenance progress"))?;
        admission
            .unmanifested_hnsw
            .retain(|definition_id, rowsets| {
                let Some(accounted) = accounted_by_definition.get(definition_id) else {
                    return false;
                };
                rowsets.retain(|rowset_id, _| {
                    visible_rowset_ids.contains(rowset_id) && !accounted.contains(rowset_id)
                });
                !rowsets.is_empty()
            });
        admission.progress_epoch = admission.progress_epoch.saturating_add(1);
        self.maintenance_progress_changed.notify_all();
        Ok(())
    }

    pub(crate) fn repack_sidecars_for_definition(&self, definition_id: u64) -> Result<usize> {
        self.refresh_all_definitions();
        let current = self.view.load_full();
        let Some(state) = current.definitions.get(&definition_id).cloned() else {
            return Ok(0);
        };
        drop(current);
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(0);
        };
        if !sidecar_repack_needed(state.definition.kind, manifest) {
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
        if let Err(error) = self.activate_artifact_readers(
            &state,
            &repacked_artifacts,
            HnswReaderActivationPolicy::PREPARED_PUBLICATION,
        ) {
            self.discard_unpublished_sidecars(&store, &sidecar_file_ids);
            return Err(error);
        }
        let publication_guard = self.tablet.acquire_search_generation_publish_guard()?;
        let definition_lock = self.definition_lock(definition_id);
        let _guard = definition_lock
            .lock()
            .map_err(|_| paro_error::internal("lock search definition publish lock"))?;
        let latest = self.view.load_full();
        let Some(latest_state) = latest.definitions.get(&definition_id).cloned() else {
            self.discard_unpublished_sidecars(&store, &sidecar_file_ids);
            return Ok(0);
        };
        drop(latest);
        if head_for_state(&self.manifests, &latest_state) != head_for_state(&self.manifests, &state)
            || latest_state.definition != state.definition
            || latest_state.origin != state.origin
        {
            self.discard_unpublished_sidecars(&store, &sidecar_file_ids);
            return Ok(0);
        }

        let next_state = match self.publish_sidecar_repack_delta(&latest_state, repacked_artifacts)
        {
            Ok(next_state) => next_state,
            Err(err) => {
                self.discard_unpublished_sidecars(&store, &sidecar_file_ids);
                return Err(err);
            }
        };
        let completion =
            match self.publish_generation_head_for_state(&next_state, &publication_guard) {
                Ok(completion) => completion,
                Err(error) => {
                    self.retire_unpublished_revision(&latest_state, &next_state);
                    return Err(error);
                }
            };
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

    pub(super) fn compact_manifest_deltas_for_definition(
        &self,
        definition_id: u64,
    ) -> Result<bool> {
        self.refresh_all_definitions();
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
        let completion =
            match self.publish_generation_head_for_state(&next_state, &publication_guard) {
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
}
