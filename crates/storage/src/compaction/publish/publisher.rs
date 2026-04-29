// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::workspace::{CompactionBuildOutput, StagedArtifact};
use crate::compaction::plan::types::CompactionJobId;
use crate::compaction::publish::record::{
    maintenance_storage_op, CompactionPublishRecord, CompactionPublishRequest, PkPublishDelta,
    RetiredInput,
};
use crate::rowset::{Rowset, RowsetSharedPtr};
use crate::tablet::Tablet;
use paro_common::durability::{PrepareToken, PreparedMaintenancePlan, PreparedTabletPlan};
use paro_common::effect::ArtifactRef;
use paro_common::error::{self as paro_error, Result};
use paro_common::journal::MaintenanceKind;
use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

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

        let maintenance_plan = PreparedMaintenancePlan {
            kind: MaintenanceKind::Compaction,
            catalog_ops: Vec::new(),
            storage_ops: vec![maintenance_storage_op(
                &record,
                ArtifactRef::from_tablet_path(tablet.data_dir(), &artifact.workspace.rowset_dir)?,
                ArtifactRef::from_tablet_path(
                    tablet.data_dir(),
                    &artifact.final_rowset_path(tablet),
                )?,
                &retired_inputs,
            )],
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: vec![PreparedTabletPlan::new(tablet.tablet_id(), token)],
        };

        Ok(CompactionPublishRequest {
            output,
            record,
            maintenance_plan,
            token,
        })
    }

    pub fn publish(tablet: &Tablet, request: CompactionPublishRequest) -> Result<()> {
        let CompactionPublishRequest {
            output,
            record,
            maintenance_plan,
            ..
        } = request;
        let retired_inputs: Vec<_> = match &output {
            CompactionBuildOutput::Rowset(artifact) => artifact
                .plan
                .input_rowsets
                .iter()
                .map(|input| RetiredInput {
                    rowset_id: input.rowset.rowset_id(),
                    version: input.rowset.version(),
                    rssids: tablet.rowset_rssids(input.rowset.as_ref()),
                })
                .collect(),
            CompactionBuildOutput::PrimaryKey { artifact, .. } => artifact
                .plan
                .input_rowsets
                .iter()
                .map(|input| RetiredInput {
                    rowset_id: input.rowset.rowset_id(),
                    version: input.rowset.version(),
                    rssids: tablet.rowset_rssids(input.rowset.as_ref()),
                })
                .collect(),
        };
        let final_path = PathBuf::from(&record.output_rowset_path);
        let mut installed_final_namespace = false;
        let mut durable_record = false;

        let result = match output {
            CompactionBuildOutput::Rowset(artifact) => {
                tablet.with_meta_lock("publish compaction", || {
                    Self::publish_artifact(
                        tablet,
                        artifact,
                        None,
                        &record,
                        maintenance_plan,
                        &retired_inputs,
                        &final_path,
                        &mut installed_final_namespace,
                        &mut durable_record,
                    )
                })
            }
            CompactionBuildOutput::PrimaryKey { artifact, pk_delta } => {
                tablet.with_meta_lock("publish compaction", || {
                    Self::publish_artifact(
                        tablet,
                        artifact,
                        Some(pk_delta),
                        &record,
                        maintenance_plan,
                        &retired_inputs,
                        &final_path,
                        &mut installed_final_namespace,
                        &mut durable_record,
                    )
                })
            }
        };

        if result.is_err() && installed_final_namespace && !durable_record {
            crate::compaction::cleanup::cleanup_now(&final_path);
        }

        result
    }

    fn publish_artifact(
        tablet: &Tablet,
        artifact: StagedArtifact,
        pk_delta: Option<PkPublishDelta>,
        record: &CompactionPublishRecord,
        maintenance_plan: PreparedMaintenancePlan,
        retired_inputs: &[RetiredInput],
        final_path: &Path,
        installed_final_namespace: &mut bool,
        durable_record: &mut bool,
    ) -> Result<()> {
        let staged_stats = artifact.rowset.statistics().ok();
        tablet.validate_compaction_publish_locked(
            record,
            &artifact.plan.input_rowset_ptrs(),
            artifact.rowset.as_ref(),
        )?;

        install_staged_rowset(&artifact.workspace.rowset_dir, final_path)?;
        *installed_final_namespace = true;
        Tablet::sync_parent_dir(final_path)?;

        let final_rowset = build_final_rowset(tablet, &artifact, final_path, staged_stats)?;
        tablet.ensure_rowset_rssids(&final_rowset);
        let maintenance_context = match tablet.journal_coordinator() {
            Some(coordinator) => Some(coordinator.submit_maintenance(maintenance_plan)?),
            None => None,
        };
        *durable_record = maintenance_context.is_some();
        let checkpoint_ticket = tablet
            .begin_checkpoint_compaction_publish()
            .map(|mut ticket| {
                if let Some(context) = maintenance_context.as_ref() {
                    ticket.maintenance_id = context.maintenance_id;
                }
                ticket
            });
        let output_maintenance_id = maintenance_context
            .as_ref()
            .map(|context| context.maintenance_id)
            .or_else(|| {
                checkpoint_ticket
                    .as_ref()
                    .map(|ticket| ticket.maintenance_id)
            })
            .unwrap_or(0);

        final_rowset.make_visible()?;
        tablet.install_compaction_publish_locked(
            &artifact.plan.input_rowset_ptrs(),
            retired_inputs,
            final_rowset.clone(),
            output_maintenance_id,
            record.cumulative_point_action,
            false,
        )?;
        if let Some(ticket) = checkpoint_ticket {
            tablet.finish_checkpoint_compaction_publish(ticket);
        }

        if let Some(pk_delta) = pk_delta.as_ref() {
            tablet.apply_compaction_publish_delta(
                final_rowset.rowset_id(),
                final_rowset.end_version(),
                pk_delta,
            )?;
        }

        if matches!(
            artifact.plan.merge_semantics,
            crate::compaction::plan::types::MergeSemantics::Deduplicate
        ) {
            tablet.validate_primary_index_consistency_after_compaction(&final_rowset)?;
            tablet.maybe_flush_primary_index()?;
        }

        if let (Some(runtime), Some(context)) =
            (tablet.journal_apply_runtime(), maintenance_context.as_ref())
        {
            runtime.submit(ApplyRequest {
                lsn: context.lsn,
                durable_batch_lsn: context.durable_batch_lsn,
                commit_id: None,
                wait_mode: WaitMode::Published,
                catalog_serial: false,
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: Vec::<TabletApplyPart>::new(),
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            })?;
        }

        if let Err(err) = tablet.persist_meta_snapshot() {
            warn!(
                tablet_id = tablet.tablet_id(),
                plan_id = %record.plan_id,
                job_id = %record.job_id,
                error = %err,
                "compaction publish completed but failed to persist tablet meta snapshot"
            );
        }
        Ok(())
    }
}

fn install_staged_rowset(staged_path: &Path, final_path: &Path) -> Result<()> {
    if final_path.exists() {
        return Err(paro_error::object_exists(
            "compaction output rowset",
            final_path.display().to_string(),
        ));
    }

    fs::rename(staged_path, final_path).map_err(|err| {
        paro_error::io_error(format!(
            "install compaction artifact {} -> {}: {}",
            staged_path.display(),
            final_path.display(),
            err
        ))
    })
}

fn build_final_rowset(
    tablet: &Tablet,
    artifact: &StagedArtifact,
    final_path: &Path,
    staged_stats: Option<crate::rowset::rowset_statistics::RowsetStatistics>,
) -> Result<RowsetSharedPtr> {
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("Tablet schema missing during compaction publish"))?;
    let mut rowset_meta = artifact.rowset.rowset_meta();
    rowset_meta.set_rowset_path(final_path.to_string_lossy().to_string());
    let rowset = Arc::new(Rowset::create(schema, rowset_meta, final_path)?);
    if let Some(stats) = staged_stats {
        rowset.set_statistics_cache(stats);
    }
    Ok(rowset)
}
