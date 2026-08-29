// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Foreground ingest admission and rowset-publication integration.

use super::*;

#[derive(Debug, Default)]
pub(super) struct SearchIngestAdmissionState {
    pub(super) reserved_rows: u64,
    pub(super) reserved_bytes: u64,
    /// Committed rowsets not yet represented by a durable generation
    /// manifest. Deferred HNSW publication deliberately does not write one
    /// manifest revision per foreground transaction, so manifest tail counts
    /// alone are not a level-triggered measure of current debt.
    pub(super) unmanifested_hnsw: BTreeMap<u64, BTreeMap<RowsetId, SearchIngestDebt>>,
    pub(super) progress_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SearchIngestDebt {
    pub(super) rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct HnswIngestAdmissionBlocker {
    pub(super) definition_id: u64,
    pub(super) pending_rows: u64,
    pub(super) pending_bytes: u64,
    pub(super) row_limit: u64,
    pub(super) byte_limit: u64,
}

impl RowsetPublishObserver for SearchIndexRegistry {
    fn wait_for_rowset_publish_admission(
        &self,
        tablet_id: TabletId,
        incoming_rows: u64,
        incoming_bytes: u64,
    ) -> Result<()> {
        if tablet_id != self.tablet.tablet_id() || (incoming_rows == 0 && incoming_bytes == 0) {
            return Ok(());
        }
        let deadline = Instant::now() + HNSW_INGEST_BACKPRESSURE_TIMEOUT;
        let mut admission = self
            .ingest_admission
            .lock()
            .map_err(|_| paro_error::internal("lock search ingest admission"))?;
        loop {
            let blocked = self.hnsw_ingest_admission_blocker(incoming_rows, &admission);
            let Some(blocker) = blocked else {
                admission.reserved_rows = admission.reserved_rows.saturating_add(incoming_rows);
                admission.reserved_bytes = admission.reserved_bytes.saturating_add(incoming_bytes);
                self.foreground_ingest_epoch.fetch_add(1, Ordering::Release);
                return Ok(());
            };
            let isolated_oversized_write = blocker.pending_rows == 0
                && blocker.pending_bytes == 0
                && admission.reserved_rows == 0
                && admission.reserved_bytes == 0
                && incoming_rows > blocker.row_limit;
            if isolated_oversized_write {
                // A rebuildable secondary index cannot constrain transaction
                // atomicity. Admit one oversized transaction as an exclusive
                // freshness epoch; its publication raises critical urgency and
                // queries retain the streaming exact-tail fallback meanwhile.
                admission.reserved_rows = incoming_rows;
                admission.reserved_bytes = incoming_bytes;
                self.foreground_ingest_epoch.fetch_add(1, Ordering::Release);
                return Ok(());
            }

            if let Some(notifier) = self.maintenance_notifier.read().unwrap().as_ref() {
                notifier(SearchMaintenanceUrgency::Immediate);
            }

            let now = Instant::now();
            if now >= deadline {
                // Backpressure is a latency control, never a transaction
                // validity rule. Preserve progress if maintenance is slow;
                // level-triggered catch-up and exact query fallback continue
                // to own the resulting debt.
                admission.reserved_rows = admission.reserved_rows.saturating_add(incoming_rows);
                admission.reserved_bytes = admission.reserved_bytes.saturating_add(incoming_bytes);
                self.foreground_ingest_epoch.fetch_add(1, Ordering::Release);
                tracing::warn!(
                    tablet_id,
                    definition_id = blocker.definition_id,
                    pending_rows = blocker.pending_rows,
                    pending_bytes = blocker.pending_bytes,
                    incoming_rows,
                    incoming_bytes,
                    "search freshness backpressure timed out; admitting transaction with critical maintenance debt"
                );
                return Ok(());
            }
            let observed_epoch = admission.progress_epoch;
            let wait = deadline.saturating_duration_since(now);
            let (next, _) = self
                .maintenance_progress_changed
                .wait_timeout(admission, wait)
                .map_err(|_| paro_error::internal("wait for search maintenance progress"))?;
            admission = next;
            if admission.progress_epoch == observed_epoch && Instant::now() >= deadline {
                continue;
            }
        }
    }

    fn release_rowset_publish_admission(
        &self,
        tablet_id: TabletId,
        reserved_rows: u64,
        reserved_bytes: u64,
    ) {
        if tablet_id != self.tablet.tablet_id() || (reserved_rows == 0 && reserved_bytes == 0) {
            return;
        }
        let mut admission = self
            .ingest_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.reserved_rows = admission.reserved_rows.saturating_sub(reserved_rows);
        admission.reserved_bytes = admission.reserved_bytes.saturating_sub(reserved_bytes);
        admission.progress_epoch = admission.progress_epoch.saturating_add(1);
        self.maintenance_progress_changed.notify_all();
    }

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
        self.record_unmanifested_hnsw_rowset(&rowset);
        let (prepared, stale_definition_ids) = search_updates.into_parts();
        let maintenance_needed = !prepared.is_empty()
            || !stale_definition_ids.is_empty()
            || self.view.load().definitions.values().any(|state| {
                state.definition.kind == SearchIndexKind::Hnsw
                    && !matches!(
                        state.definition.freshness_policy,
                        SearchFreshnessPolicy::Required
                    )
            });
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
        // Only prepare readers for rowsets that the accepted generation
        // actually exposes as exact tail. Compaction outputs may already own
        // an inline/sidecar artifact; warming their base vector pages would
        // consume I/O without serving the query path.
        self.schedule_hnsw_tail_reader_warmup(&rowset);
        self.sweep_retired();
        if maintenance_needed {
            if let Some(notifier) = self.maintenance_notifier.read().unwrap().as_ref() {
                let urgency = self
                    .view
                    .load()
                    .definitions
                    .values()
                    .filter_map(|state| state.manifest.as_ref())
                    .map(
                        |manifest| match manifest.root.maintenance_state.recovery.priority {
                            MaintenancePriority::Idle | MaintenancePriority::Opportunistic => {
                                SearchMaintenanceUrgency::Quiescent
                            }
                            MaintenancePriority::Elevated => SearchMaintenanceUrgency::Deadline,
                            MaintenancePriority::Critical => SearchMaintenanceUrgency::Immediate,
                        },
                    )
                    .max()
                    .unwrap_or_default();
                notifier(urgency);
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

    fn compaction_requirement(
        &self,
        tablet_id: TabletId,
        replaced_rowset_ids: &[u64],
    ) -> crate::tablet::SearchCompactionRequirement {
        if tablet_id != self.tablet.tablet_id() || replaced_rowset_ids.is_empty() {
            return crate::tablet::SearchCompactionRequirement::Independent;
        }
        let replaced = replaced_rowset_ids.iter().copied().collect::<BTreeSet<_>>();
        let definition_ids = self
            .view
            .load()
            .definitions
            .values()
            .filter(|state| {
                state.definition.kind == SearchIndexKind::Hnsw
                    && !matches!(
                        state.definition.freshness_policy,
                        SearchFreshnessPolicy::Required
                    )
                    && state.manifest.as_ref().is_some_and(|manifest| {
                        manifest
                            .artifacts
                            .artifacts
                            .iter()
                            .any(|artifact| artifact.coverage.intersects_rowsets(&replaced))
                    })
            })
            .map(|state| state.definition.definition_id)
            .collect::<Vec<_>>();
        if definition_ids.is_empty() {
            crate::tablet::SearchCompactionRequirement::Independent
        } else {
            crate::tablet::SearchCompactionRequirement::GenerationReplacement {
                definition_ids: definition_ids.into_boxed_slice(),
            }
        }
    }
}

impl SearchIndexRegistry {
    pub(super) fn hnsw_ingest_admission_blocker(
        &self,
        incoming_rows: u64,
        admission: &SearchIngestAdmissionState,
    ) -> Option<HnswIngestAdmissionBlocker> {
        self.view.load().definitions.values().find_map(|state| {
            if state.definition.kind != SearchIndexKind::Hnsw {
                return None;
            }
            let provider = state.hnsw_provider_config.as_ref()?;
            let manifest = state.manifest.as_ref()?;
            let manifest_pending_rows = manifest.root.maintenance_state.recovery.tail_pending_rows;
            let accounted_rowsets = manifest_accounted_rowsets(manifest);
            let unmanifested_rows = admission
                .unmanifested_hnsw
                .get(&state.definition.definition_id)
                .into_iter()
                .flat_map(|rowsets| rowsets.iter())
                .filter(|(rowset_id, _)| !accounted_rowsets.contains(rowset_id))
                .fold(0u64, |rows, (_, debt)| rows.saturating_add(debt.rows));
            let pending_rows = manifest_pending_rows.saturating_add(unmanifested_rows);
            let pending_bytes = provider
                .maintenance
                .vector_bytes(provider.dimension, pending_rows);
            let maintenance_row_limit = provider.maintenance.max_pending_rows(provider.dimension);
            let row_limit = match state.definition.freshness_policy {
                SearchFreshnessPolicy::Required => return None,
                SearchFreshnessPolicy::BoundedLag { max_tail_rows, .. } => {
                    max_tail_rows.min(maintenance_row_limit)
                }
                SearchFreshnessPolicy::Opportunistic => maintenance_row_limit,
            };
            let byte_limit = provider
                .maintenance
                .vector_bytes(provider.dimension, row_limit);
            (pending_rows
                .saturating_add(admission.reserved_rows)
                .saturating_add(incoming_rows)
                > row_limit)
                .then_some(HnswIngestAdmissionBlocker {
                    definition_id: state.definition.definition_id,
                    pending_rows,
                    pending_bytes,
                    row_limit,
                    byte_limit,
                })
        })
    }

    pub(super) fn record_unmanifested_hnsw_rowset(&self, rowset: &RowsetSharedPtr) {
        let definition_ids = self
            .view
            .load()
            .definitions
            .values()
            .filter(|state| {
                state.definition.kind == SearchIndexKind::Hnsw
                    && !matches!(
                        state.definition.freshness_policy,
                        SearchFreshnessPolicy::Required
                    )
            })
            .map(|state| state.definition.definition_id)
            .collect::<Vec<_>>();
        if definition_ids.is_empty() {
            return;
        }
        let rows = rowset.num_rows();
        let mut admission = self
            .ingest_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for definition_id in definition_ids {
            admission
                .unmanifested_hnsw
                .entry(definition_id)
                .or_default()
                .insert(rowset.rowset_id(), SearchIngestDebt { rows });
        }
        admission.progress_epoch = admission.progress_epoch.saturating_add(1);
        self.maintenance_progress_changed.notify_all();
    }

    pub(super) fn schedule_hnsw_tail_reader_warmup(&self, rowset: &RowsetSharedPtr) {
        let Some(scheduler) = self
            .hnsw_tail_reader_warmup
            .read()
            .unwrap()
            .as_ref()
            .cloned()
        else {
            return;
        };
        let rowset_id = rowset.rowset_id();
        let specifications = self
            .view
            .load()
            .definitions
            .values()
            .filter_map(|state| {
                let provider = state.hnsw_provider_config.as_ref()?;
                let manifest = state.manifest.as_ref()?;
                if !manifest.tail_pending_entries.iter().any(|entry| {
                    entry.rowset_id == rowset_id && entry.mutation != TailMutationKind::Delete
                }) {
                    return None;
                }
                state
                    .definition
                    .column_ids
                    .first()
                    .copied()
                    .map(|column_id| (column_id, provider.dimension as usize))
            })
            .collect::<BTreeSet<_>>();
        for (column_id, dimension) in specifications {
            scheduler.schedule(rowset, column_id, dimension);
        }
    }

    /// Reconstruct reader-preparation work from durable tail identity.
    ///
    /// Publication callbacks are only acceleration hints and do not survive a
    /// crash. Binding the instance scheduler (and installing a restored
    /// definition) therefore derives the queue from the final manifest and
    /// visible rowset graph, exactly like level-triggered index maintenance.
    pub(super) fn schedule_pending_hnsw_tail_reader_warmup(&self) {
        let Some(scheduler) = self
            .hnsw_tail_reader_warmup
            .read()
            .unwrap()
            .as_ref()
            .cloned()
        else {
            return;
        };
        let mut by_rowset = BTreeMap::<RowsetId, BTreeSet<(ColumnId, usize)>>::new();
        for state in self.view.load().definitions.values() {
            let Some(provider) = state.hnsw_provider_config.as_ref() else {
                continue;
            };
            let Some(column_id) = state.definition.column_ids.first().copied() else {
                continue;
            };
            let Some(manifest) = state.manifest.as_ref() else {
                continue;
            };
            for entry in &manifest.tail_pending_entries {
                if entry.mutation == TailMutationKind::Delete {
                    continue;
                }
                by_rowset
                    .entry(entry.rowset_id)
                    .or_default()
                    .insert((column_id, provider.dimension as usize));
            }
        }
        if by_rowset.is_empty() {
            return;
        }
        let visible_version = self.tablet.max_version();
        let rowsets = match self.tablet.capture_consistent_rowsets(visible_version) {
            Ok(rowsets) => rowsets,
            Err(error) => {
                tracing::warn!(
                    tablet_id = self.tablet.tablet_id(),
                    error = %error,
                    "failed to reconstruct HNSW exact-tail reader warmup"
                );
                return;
            }
        };
        for rowset in rowsets {
            let Some(specifications) = by_rowset.get(&rowset.rowset_id()) else {
                continue;
            };
            for &(column_id, dimension) in specifications {
                scheduler.schedule(&rowset, column_id, dimension);
            }
        }
    }
}
