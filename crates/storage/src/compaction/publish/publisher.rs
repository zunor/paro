// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::workspace::CompactionBuildOutput;
use crate::compaction::plan::types::CompactionJobId;
use crate::compaction::publish::record::{
    compaction_mutation, maintenance_storage_op, CompactionPublishRecord, CompactionPublishRequest,
    RetiredInput,
};
use crate::durable_maintenance::DurableMaintenanceApplyCompletion;
use crate::tablet::Tablet;
use paro_common::durability::{PrepareToken, PreparedMaintenancePlan, PreparedTabletPlan};
use paro_common::effect::ArtifactRef;
use paro_common::error::{self as paro_error, Result};
use paro_common::journal::MaintenanceKind;
use std::sync::Arc;

pub struct CompactionPublisher;

impl CompactionPublisher {
    pub fn prepare_request(
        tablet: &Tablet,
        output: CompactionBuildOutput,
        job_id: CompactionJobId,
    ) -> Result<CompactionPublishRequest> {
        let (artifact, replaced_inputs, token) = match &output {
            CompactionBuildOutput::Rowset(artifact) => (
                artifact,
                artifact
                    .plan
                    .input_rowsets
                    .iter()
                    .map(|input| input.rowset.rowset_id())
                    .collect(),
                PrepareToken {
                    visible_version: artifact.plan.read_snapshot.visible_version,
                    layout_epoch: artifact.plan.read_snapshot.layout_epoch,
                    schema_epoch: artifact.plan.read_snapshot.schema_epoch,
                },
            ),
            CompactionBuildOutput::PrimaryKey { artifact, .. } => (
                artifact,
                artifact
                    .plan
                    .input_rowsets
                    .iter()
                    .map(|input| input.rowset.rowset_id())
                    .collect(),
                PrepareToken {
                    visible_version: artifact.plan.read_snapshot.visible_version,
                    layout_epoch: artifact.plan.read_snapshot.layout_epoch,
                    schema_epoch: artifact.plan.read_snapshot.schema_epoch,
                },
            ),
        };

        let record = CompactionPublishRecord {
            plan_id: artifact.plan.plan_id,
            job_id,
            tablet_id: tablet.tablet_id(),
            output_rowset_id: artifact.plan.output_rowset_id,
            output_version: artifact.plan.output_version,
            cumulative_point_action: artifact.plan.cumulative_point_action,
            output_rowset_path: artifact
                .final_rowset_path(tablet)
                .to_string_lossy()
                .into_owned(),
            replaced_inputs,
        };
        let retired_inputs = artifact
            .plan
            .input_rowsets
            .iter()
            .map(|input| RetiredInput {
                rowset_id: input.rowset.rowset_id(),
                version: input.rowset.version(),
                rssids: tablet.rowset_rssids(input.rowset.as_ref()),
            })
            .collect::<Vec<_>>();

        let staged_ref =
            ArtifactRef::from_tablet_path(tablet.data_dir(), &artifact.workspace.rowset_dir)?;
        let output_ref =
            ArtifactRef::from_tablet_path(tablet.data_dir(), &artifact.final_rowset_path(tablet))?;
        let mutation = compaction_mutation(
            &record,
            staged_ref.clone(),
            output_ref.clone(),
            &retired_inputs,
        );
        let maintenance_plan = PreparedMaintenancePlan {
            kind: MaintenanceKind::Compaction,
            catalog_ops: Vec::new(),
            storage_ops: vec![maintenance_storage_op(
                &record,
                staged_ref,
                output_ref,
                &retired_inputs,
            )],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: vec![PreparedTabletPlan::new(tablet.tablet_id(), token)],
        };

        Ok(CompactionPublishRequest {
            output,
            record,
            mutation,
            maintenance_plan,
            token,
        })
    }

    /// Publish through the same ordered tablet-apply lane used by transactions.
    ///
    /// The WAL append and apply wait deliberately happen without `meta_lock`.
    /// Holding that lock while waiting for an older transaction to reach its
    /// tablet phase inverts the lock order and deadlocks sustained ingest.
    pub fn publish(tablet: &Arc<Tablet>, request: CompactionPublishRequest) -> Result<()> {
        let CompactionPublishRequest {
            output,
            record,
            mutation,
            maintenance_plan,
            ..
        } = request;
        let _publish_guard = tablet.acquire_compaction_publish_guard()?;
        tablet.with_meta_lock("preflight compaction publish", || match &output {
            CompactionBuildOutput::Rowset(artifact)
            | CompactionBuildOutput::PrimaryKey { artifact, .. } => tablet
                .validate_compaction_publish_locked(
                    &record,
                    &artifact.plan.input_rowset_ptrs(),
                    artifact.rowset.as_ref(),
                ),
        })?;

        let pk_delta = match &output {
            CompactionBuildOutput::Rowset(_) => None,
            CompactionBuildOutput::PrimaryKey { pk_delta, .. } => Some(pk_delta.clone()),
        };
        let coordinator = tablet.journal_coordinator();
        let runtime = coordinator
            .as_ref()
            .map(|_| {
                tablet.journal_apply_runtime().ok_or_else(|| {
                    paro_error::internal(
                        "compaction maintenance requires a bound journal apply runtime",
                    )
                })
            })
            .transpose()?;
        let maintenance_context = coordinator
            .map(|coordinator| coordinator.submit_maintenance(maintenance_plan))
            .transpose()?;
        let Some(context) = maintenance_context else {
            return tablet.apply_compaction_publish_online(&mutation, pk_delta.as_ref(), 0);
        };

        let tablet_for_apply = Arc::clone(tablet);
        let mutation_for_apply = mutation.clone();
        let maintenance_id = context.maintenance_id;
        let lsn = context.lsn;
        let completion = DurableMaintenanceApplyCompletion::arm(
            runtime
                .as_ref()
                .expect("runtime validated before compaction WAL append")
                .clone(),
            context.lsn,
            context.durable_batch_lsn,
            tablet.tablet_id(),
            move || {
                tablet_for_apply.apply_compaction_publish_online(
                    &mutation_for_apply,
                    pk_delta.as_ref(),
                    maintenance_id,
                )?;
                tablet_for_apply.note_applied_lsn(lsn)
            },
        );

        // The terminal value now means that preflight and WAL append both
        // succeeded. The actual storage mutation is the ordered apply closure
        // above; there is no second, out-of-band publication path.
        completion.record_terminal_result(Ok(()))?;
        completion.finish()?;

        // Keep the staged workspace owner alive until the ordered closure has
        // atomically renamed its rowset directory.
        drop(output);
        Ok(())
    }
}
