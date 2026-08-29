// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Durable generation publication, refresh, retirement, and schema seeding.

use super::*;

impl SearchIndexRegistry {
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

    pub(super) fn refresh_definition_inner(
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
        let visible_rowsets = self.tablet.capture_consistent_rowsets(visible_version)?;
        drop(_guard);
        drop(publication_guard);

        let next_state =
            self.refresh_state_from_snapshot(&state, visible_version, &visible_rowsets, force)?;
        if head_for_state(&self.manifests, &next_state) == head_for_state(&self.manifests, &state) {
            return Ok(state.capability.clone());
        }

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
        drop(state);
        drop(_guard);
        drop(publication_guard);
        completion.finish()?;
        view_result?;
        self.sweep_retired();
        self.signal_maintenance_progress()?;
        Ok(next_state.capability)
    }

    pub(super) fn prepare_heads_for_visible_rowsets(
        &self,
        visible_version: i64,
        visible_rowsets: &[RowsetSharedPtr],
    ) -> Result<SearchGenerationHeadUpdates> {
        // Bounded/opportunistic HNSW definitions derive their
        // exact tail from the committed rowset snapshot at read time, then the
        // instance-owned maintenance loop coalesces durable manifest/head work.
        // Persisting one derived revision per small DML transaction serializes
        // unrelated writers and records state already recoverable from the
        // tablet rowset graph.
        let definition_ids = self
            .view
            .load()
            .definitions
            .iter()
            .filter_map(|(definition_id, state)| {
                let deferred_hnsw = state.definition.kind == SearchIndexKind::Hnsw
                    && !matches!(
                        state.definition.freshness_policy,
                        SearchFreshnessPolicy::Required
                    );
                (!deferred_hnsw).then_some(*definition_id)
            })
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

    pub(crate) fn refresh_all_definitions(&self) {
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

    pub(super) fn install_definition_with_origin(
        &self,
        definition: SearchIndexDefinition,
        origin: SearchDefinitionOrigin,
        activation: HnswReaderActivationPolicy,
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
        for duplicate_seed_id in &duplicate_seed_ids {
            self.retire_definition_builds(*duplicate_seed_id)?;
        }

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
        self.activate_generation_readers(&state, activation)?;
        let definition_id = definition.definition_id;
        let removed_seed_states = self.update_registry_view(|view| {
            let mut removed = Vec::new();
            for duplicate_seed_id in &duplicate_seed_ids {
                if let Some(seed_state) = view.definitions.remove(duplicate_seed_id) {
                    removed.push((*duplicate_seed_id, seed_state));
                }
            }
            view.definitions.insert(definition_id, state);
            Ok((true, removed))
        })?;
        // Installation may legitimately reuse a durable definition id after
        // a prior retirement. Advancing the signal generation before clearing
        // `retiring` keeps all old work cancelled while admitting only tokens
        // captured for the newly visible definition state.
        self.activate_definition_builds(definition_id)?;
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
        self.refresh_definition(definition_id)?;
        self.schedule_pending_hnsw_tail_reader_warmup();
        Ok(())
    }

    pub(super) fn refresh_state_from_snapshot(
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

    pub(super) fn publish_full_snapshot(
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
                state.hnsw_provider_config.as_deref(),
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
            )?,
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

    pub(super) fn publish_delta_for_new_rowsets(
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
            state.hnsw_provider_config.as_deref(),
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
        )?;

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

    pub(super) fn publish_delta_for_replaced_rowsets(
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
            state.hnsw_provider_config.as_deref(),
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
        )?;

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

    pub(super) fn publish_sidecar_catch_up_delta(
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
        let mut materialized_artifacts = current_manifest
            .artifacts
            .artifacts
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        materialized_artifacts.extend(added_artifacts.iter().cloned());
        let materialized = GenerationArtifactSet::try_new(materialized_artifacts)?;
        let materialized_artifact_keys =
            artifact_segment_column_keys(materialized.artifacts.iter());
        let no_additional_artifact_keys = BTreeSet::new();
        let covered_tail_ids = current_manifest
            .tail_pending_entries
            .iter()
            .filter(|entry| {
                !matches!(entry.mutation, TailMutationKind::Delete)
                    && tail_entry_is_covered_by_artifacts(
                        &state.definition,
                        entry,
                        &materialized_artifact_keys,
                        &no_additional_artifact_keys,
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
        root.generation_stats =
            generation_stats_from_artifacts(&state.definition, &materialized.artifacts)?;
        let tail_pending = TailPendingSet {
            entries: tail_pending_entries.clone(),
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
            Some(current_manifest.root.build_epoch),
            current_manifest
                .root
                .maintenance_state
                .recovery
                .superseded_build_epochs
                .clone(),
        )?;

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
        let added_stats = generation_stats_from_artifacts(&state.definition, &added_artifacts)?;
        delta_entries.extend(
            stats_deltas_from_generation_stats(&added_stats)
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

    pub(super) fn publish_hnsw_compaction_delta(
        &self,
        state: &SearchDefinitionState,
        removed_artifacts: &[SearchArtifactRef],
        mut added_artifacts: Vec<SearchArtifactRef>,
    ) -> Result<SearchDefinitionState> {
        let started_at = Instant::now();
        let current_manifest = state.manifest.as_ref().ok_or_else(|| {
            paro_error::internal("HNSW compaction publish requires existing manifest")
        })?;
        let generation = state
            .generation
            .as_ref()
            .ok_or_else(|| paro_error::internal("HNSW compaction publish requires generation"))?;
        let definition_id = state.definition.definition_id;
        added_artifacts = assign_generation_id(added_artifacts, generation.generation_id);

        let removed_keys = removed_artifacts
            .iter()
            .map(search_artifact_key)
            .collect::<BTreeSet<_>>();
        let mut materialized_artifacts = current_manifest
            .artifacts
            .artifacts
            .iter()
            .filter(|artifact| !removed_keys.contains(&search_artifact_key(artifact)))
            .cloned()
            .collect::<Vec<_>>();
        materialized_artifacts.extend(added_artifacts.iter().cloned());
        let materialized = GenerationArtifactSet::try_new(materialized_artifacts.clone())?;

        let mut root = current_manifest.root.clone();
        root.build_epoch = state.next_build_epoch;
        // Compaction changes only derived physical layout. It must not claim a
        // newer table snapshot than the manifest it rebases onto.
        root.generation_stats =
            generation_stats_from_artifacts(&state.definition, &materialized.artifacts)?;
        let tail_pending = TailPendingSet {
            entries: current_manifest.tail_pending_entries.clone(),
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
            current_manifest.root.maintenance_state.tombstone_rows,
            Some(current_manifest.root.build_epoch),
            current_manifest
                .root
                .maintenance_state
                .recovery
                .superseded_build_epochs
                .clone(),
        )?;

        let mut entries = removed_artifacts
            .iter()
            .map(|artifact| ManifestDeltaEntry::RemoveArtifact(artifact.coverage.clone()))
            .collect::<Vec<_>>();
        entries.extend(
            added_artifacts
                .iter()
                .cloned()
                .map(ManifestDeltaEntry::AddArtifact),
        );
        let mut revision =
            self.manifests
                .begin_revision_from_manifest(definition_id, root, current_manifest)?;
        revision.append_delta(&ManifestDelta::new(entries))?;
        let loaded = revision.commit()?;
        debug_assert_eq!(loaded.artifacts.artifacts, materialized.artifacts);
        let next_state = state.clone().with_manifest(loaded);
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

    pub(super) fn publish_sidecar_repack_delta(
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

    pub(super) fn publish_delta_for_covered_tail_entries(
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
            state.hnsw_provider_config.as_deref(),
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
        )?;

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

    pub(super) fn update_registry_view<R>(
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

    pub(super) fn publish_definition_state(
        &self,
        expected: &SearchDefinitionState,
        next_state: SearchDefinitionState,
    ) -> Result<()> {
        debug_assert_eq!(
            expected.definition.definition_id,
            next_state.definition.definition_id
        );
        let definition_id = expected.definition.definition_id;
        let published = self.update_registry_view(|view| {
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

    pub(super) fn publish_durable_revision_state(
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

    pub(super) fn publish_generation_head_for_state(
        &self,
        next_state: &SearchDefinitionState,
        publication_guard: &crate::tablet::SearchGenerationPublishGuard<'_>,
    ) -> Result<SearchGenerationPublishCompletion> {
        publish_head_for_state(&self.tablet, &self.manifests, next_state, publication_guard)
    }

    /// Activate immutable provider readers before the manifest head or
    /// in-memory definition can expose them to a foreground query.
    ///
    /// HNSW sidecars use external base-vector pages. Reconstructing that
    /// binding and parsing the graph is generation lifecycle work; doing it
    /// lazily in the first query creates a multi-second latency cliff and a
    /// thundering herd after recovery or catch-up publication.
    pub(super) fn activate_generation_readers(
        &self,
        state: &SearchDefinitionState,
        activation: HnswReaderActivationPolicy,
    ) -> Result<()> {
        if state.definition.kind != SearchIndexKind::Hnsw {
            return Ok(());
        }
        let Some(manifest) = state.manifest.as_ref() else {
            return Ok(());
        };
        self.activate_artifact_readers(state, &manifest.artifacts.artifacts, activation)
    }

    /// Complete the expensive half of generation activation before entering
    /// the publication critical section. The resulting readers are keyed by
    /// immutable artifact identity, so a failed CAS may leave harmless cache
    /// entries that ordinary retirement evicts; a successful CAS performs no
    /// mmap, graph decode, or external-vector binding while holding the head
    /// and definition locks.
    pub(super) fn activate_artifact_readers(
        &self,
        state: &SearchDefinitionState,
        artifacts: &[SearchArtifactRef],
        activation: HnswReaderActivationPolicy,
    ) -> Result<()> {
        if state.definition.kind != SearchIndexKind::Hnsw || artifacts.is_empty() {
            return Ok(());
        }
        let provider = state.hnsw_provider_config.as_ref().ok_or_else(|| {
            paro_error::internal("HNSW generation activation requires a provider contract")
        })?;
        let column_id = *state.definition.column_ids.first().ok_or_else(|| {
            paro_error::internal("HNSW generation activation requires one vector column")
        })?;
        let visible_rowsets = self
            .tablet
            .capture_consistent_rowsets(self.tablet.max_version())?;
        prewarm_hnsw_generation_readers(
            self.reader_runtime.as_ref(),
            artifacts,
            &visible_rowsets,
            column_id,
            provider.dimension as usize,
            &provider.build_contract(),
            state.hnsw_query_activity.clone(),
            activation,
        )?;
        Ok(())
    }

    pub(super) fn discard_unpublished_sidecars(
        &self,
        store: &SidecarArtifactStore,
        file_ids: &BTreeSet<ArtifactFileId>,
    ) {
        // Prepared readers may already own decoded indexes and package mmaps.
        // Evict those identities before unlinking a revision that lost its
        // publish CAS; otherwise an unpublished artifact remains resident and
        // can outlive the file that supplied it.
        self.reader_runtime.evict_packages(file_ids);
        remove_sidecar_packages(store, file_ids);
    }

    pub(super) fn definition_lock(&self, definition_id: u64) -> &Mutex<()> {
        let shard = (definition_id % DEFINITION_LOCK_SHARDS as u64) as usize;
        &self.definition_locks[shard]
    }

    pub(super) fn definition_build_lock(&self, definition_id: u64) -> &Mutex<()> {
        let shard = (definition_id % DEFINITION_LOCK_SHARDS as u64) as usize;
        &self.definition_build_locks[shard]
    }

    pub(super) fn lock_definition_build(&self, definition_id: u64) -> MutexGuard<'_, ()> {
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

    pub(super) fn definition_build_signal(
        &self,
        definition_id: u64,
    ) -> Result<Arc<DefinitionBuildSignal>> {
        let mut signals = self
            .definition_build_signals
            .lock()
            .map_err(|_| paro_error::internal("lock search definition build signals"))?;
        Ok(Arc::clone(signals.entry(definition_id).or_default()))
    }

    /// Capture a build token after acquiring the single-flight lane.
    ///
    /// A retiring definition is not a retryable provider failure: its durable
    /// lifecycle owner has already invalidated this work and is waiting to
    /// publish the tombstone.
    pub(super) fn begin_definition_build(
        &self,
        definition_id: u64,
    ) -> Result<Option<DefinitionBuildToken>> {
        let signal = self.definition_build_signal(definition_id)?;
        if signal.retiring.load(Ordering::Acquire) {
            return Ok(None);
        }
        Ok(Some(DefinitionBuildToken {
            generation: signal.generation.load(Ordering::Acquire),
            signal,
        }))
    }

    /// Invalidate running and queued provider work before waiting for its lane.
    pub(super) fn retire_definition_builds(&self, definition_id: u64) -> Result<()> {
        let signal = self.definition_build_signal(definition_id)?;
        signal.retiring.store(true, Ordering::Release);
        signal.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub(super) fn activate_definition_builds(&self, definition_id: u64) -> Result<()> {
        let signal = self.definition_build_signal(definition_id)?;
        signal.generation.fetch_add(1, Ordering::AcqRel);
        signal.retiring.store(false, Ordering::Release);
        Ok(())
    }

    pub(super) fn forget_definition_build_signal(&self, definition_id: u64) -> Result<()> {
        let mut signals = self
            .definition_build_signals
            .lock()
            .map_err(|_| paro_error::internal("lock search definition build signals"))?;
        signals.remove(&definition_id);
        Ok(())
    }

    pub(super) fn lock_definitions(
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

    pub(super) fn retire_definition(
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

    pub(super) fn retire_unpublished_revision(
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

    pub(super) fn retire_manifest_replaced_by(
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

    pub(super) fn retire_manifest_paths(
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

    pub(super) fn sweep_retired(&self) {
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

    pub(super) fn restored_schema_seed_state(
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

    pub(super) fn seed_schema_hnsw_definitions(&self) {
        let Some(schema) = self.tablet.schema() else {
            return;
        };
        for (column_id, definition) in hnsw_schema_seed_definitions(self.tablet.table_id(), &schema)
        {
            match definition {
                Ok(definition) => {
                    let _ = self.install_definition_with_origin(
                        definition,
                        SearchDefinitionOrigin::schema_seed(column_id),
                        HnswReaderActivationPolicy::RECOVERY,
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
