// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::replay_handler::CatalogReplayHandler;
use paro_common::effect::{ApplyDescriptor, StorageCommitOp, TabletMutation};
use paro_common::logging::targets;

pub(super) struct ApplyEngine<'handler, 'catalog> {
    handler: &'handler mut CatalogReplayHandler<'catalog>,
}

impl<'handler, 'catalog> ApplyEngine<'handler, 'catalog> {
    pub(super) fn new(handler: &'handler mut CatalogReplayHandler<'catalog>) -> Self {
        Self { handler }
    }

    pub(super) fn apply_effects(
        &mut self,
        storage_ops: &[StorageCommitOp],
        descriptors: &[ApplyDescriptor],
        lsn: u64,
        commit_id: u64,
    ) -> paro_common::error::Result<()> {
        let commit_visibility = if commit_id == 0 {
            None
        } else {
            Some(i64::try_from(commit_id).map_err(|_| {
                paro_common::error::invalid_input("commit_id exceeds supported version range")
            })?)
        };
        for op in storage_ops {
            self.apply_storage_op(op, lsn, commit_visibility)?;
        }
        self.handler.apply_descriptors(descriptors, commit_id)?;
        Ok(())
    }

    fn route_tablet(&self, tablet_id: u64) -> Option<super::registry::TabletRoute> {
        self.handler.registry.route_tablet(tablet_id).cloned()
    }

    fn apply_storage_op(
        &mut self,
        op: &StorageCommitOp,
        lsn: u64,
        commit_visibility: Option<i64>,
    ) -> paro_common::error::Result<()> {
        match op {
            StorageCommitOp::Tablet(tablet) => {
                if self
                    .handler
                    .registry
                    .tablet_applied_lsn(tablet.tablet_id)
                    .is_some_and(|applied_lsn| applied_lsn >= lsn)
                {
                    tracing::debug!(
                        target: targets::INSTANCE,
                        tablet_id = tablet.tablet_id,
                        lsn,
                        "tablet storage op skipped during recovery because applied_lsn already covers record"
                    );
                    return Ok(());
                }
                for mutation in &tablet.mutations {
                    self.apply_tablet_mutation(tablet.tablet_id, mutation, lsn, commit_visibility)?;
                }
                if let Some(route) = self.route_tablet(tablet.tablet_id) {
                    route.storage.tablet().note_applied_lsn(lsn)?;
                }
            }
        }
        Ok(())
    }

    fn apply_tablet_mutation(
        &mut self,
        tablet_id: u64,
        mutation: &TabletMutation,
        lsn: u64,
        commit_visibility: Option<i64>,
    ) -> paro_common::error::Result<()> {
        let Some(route) = self.route_tablet(tablet_id) else {
            tracing::debug!(
                target: targets::INSTANCE,
                tablet_id,
                "tablet mutation skipped (tablet not mapped in registry)"
            );
            return Ok(());
        };

        match mutation {
            TabletMutation::PublishRowset {
                rowset_id,
                version_span,
                rowset_ref,
            } => {
                route.storage.replay_rowset_commit(
                    *rowset_id,
                    version_span.start,
                    version_span.end,
                    &rowset_ref
                        .resolve_for_tablet(route.storage.tablet().data_dir())
                        .to_string_lossy(),
                )?;
                self.handler
                    .registry
                    .note_rowset_owner(*rowset_id, route.storage.tablet_id());
                if let Some(counters) = self.handler.replay_counters.as_ref() {
                    counters.record_rowset();
                }
                tracing::info!(
                    target: targets::INSTANCE,
                    schema = %route.schema_name,
                    table = %route.table_name,
                    lsn,
                    commit_id = commit_visibility.unwrap_or_default(),
                    tablet_id,
                    rowset_id = *rowset_id,
                    start_version = version_span.start,
                    end_version = version_span.end,
                    "Applied PublishRowset"
                );
            }
            TabletMutation::ApplyPrimaryDelete { keys } => {
                let delete_version = commit_visibility.ok_or_else(|| {
                    paro_common::error::internal(
                        "maintenance record cannot carry ApplyPrimaryDelete without commit visibility",
                    )
                })?;
                route
                    .storage
                    .replay_primary_delete_at_version(keys, delete_version)?;
                tracing::info!(
                    target: targets::INSTANCE,
                    schema = %route.schema_name,
                    table = %route.table_name,
                    lsn,
                    commit_id = delete_version,
                    tablet_id,
                    key_count = keys.len(),
                    "Applied PrimaryDelete"
                );
            }
            TabletMutation::ApplyDeletePatch {
                patch,
                deleted_row_count,
            } => {
                let delete_version = commit_visibility.ok_or_else(|| {
                    paro_common::error::internal(
                        "maintenance record cannot carry ApplyDeletePatch without commit visibility",
                    )
                })?;
                let locations =
                    patch.decode_row_refs_for_tablet(route.storage.tablet().data_dir())?;
                if let Some(counters) = self.handler.replay_counters.as_ref() {
                    counters.record_delete_patch();
                }
                let segment_id = locations.first().map(|(_, segment_id, _)| *segment_id);
                route
                    .storage
                    .replay_row_id_delete_at_version(&locations, delete_version)?;
                tracing::info!(
                    target: targets::INSTANCE,
                    schema = %route.schema_name,
                    table = %route.table_name,
                    lsn,
                    commit_id = delete_version,
                    tablet_id,
                    segment_id = ?segment_id,
                    deleted_row_count = *deleted_row_count,
                    "Applied DeletePatch"
                );
            }
            TabletMutation::PublishCompaction {
                output_rowset_id,
                replaced_inputs,
                retired_inputs,
                output_ref,
                ..
            } => {
                route.storage.apply_compaction_publish(mutation)?;
                self.handler
                    .registry
                    .note_rowset_owner(*output_rowset_id, route.storage.tablet_id());
                for rowset_id in replaced_inputs {
                    self.handler.registry.forget_rowset_owner(*rowset_id);
                }
                for input in retired_inputs {
                    self.handler.registry.forget_rowset_owner(input.rowset_id);
                }
                if let Some(counters) = self.handler.replay_counters.as_ref() {
                    counters.record_rowset();
                }
                tracing::info!(
                    target: targets::INSTANCE,
                    schema = %route.schema_name,
                    table = %route.table_name,
                    lsn,
                    commit_id = commit_visibility.unwrap_or_default(),
                    tablet_id,
                    output_rowset_id = *output_rowset_id,
                    output_path = %output_ref
                        .resolve_for_tablet(route.storage.tablet().data_dir())
                        .display(),
                    "Applied PublishCompaction"
                );
            }
        }
        let applied_lsn = route.storage.tablet().applied_lsn().max(lsn);
        self.handler
            .registry
            .note_tablet_applied_lsn(tablet_id, applied_lsn);
        Ok(())
    }
}
