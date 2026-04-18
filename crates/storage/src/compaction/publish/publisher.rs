// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::workspace::{CompactionBuildOutput, StagedArtifact};
use crate::compaction::plan::types::CompactionJobId;
use crate::compaction::publish::record::{
    maintenance_storage_op, CompactionPublishRecord, CompactionPublishRequest, RetiredInput,
};
use crate::rowset::{Rowset, RowsetSharedPtr};
use crate::tablet::Tablet;
use paro_common::durability::{PrepareToken, PreparedMaintenancePlan, PreparedTabletPlan};
use paro_common::effect::ArtifactRef;
use paro_common::effect::StorageCommitOp;
use paro_common::error::{self as paro_error, Result};
use paro_common::journal::MaintenanceKind;
use paro_journal::{ApplyRequest, TabletApplyPart, WaitMode};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct CompactionPublisher;

struct PreparedPublishRuntime {
    _artifact_guard: StagedArtifact,
    input_rowsets: Vec<RowsetSharedPtr>,
    validation_rowset: RowsetSharedPtr,
    staged_path: PathBuf,
}

impl CompactionPublisher {
    pub fn prepare_request(
        tablet: &Arc<Tablet>,
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
                    rowset_epoch: artifact.plan.read_snapshot.rowset_epoch,
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
                    rowset_epoch: artifact.plan.read_snapshot.rowset_epoch,
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

    pub fn publish(tablet: &Arc<Tablet>, request: CompactionPublishRequest) -> Result<()> {
        let prepared = Self::prepare_publish(tablet, request)?;
        let cleanup_path = prepared.runtime.staged_path.clone();
        let mut durable_submitted = false;

        let result = if let Some(coordinator) = tablet.journal_coordinator() {
            let apply_runtime = tablet.journal_apply_runtime().ok_or_else(|| {
                paro_error::internal("tablet journal apply runtime missing during compaction")
            })?;
            let validate_tablet = Arc::clone(tablet);
            let apply_tablet = Arc::clone(tablet);
            let token = prepared.token;
            let record = prepared.record.clone();
            let input_rowsets = prepared.runtime.input_rowsets.clone();
            let validation_rowset = prepared.runtime.validation_rowset.clone();
            let maintenance_plan = prepared.maintenance_plan.clone();
            let ctx = coordinator.submit_maintenance_context(maintenance_plan, move |_| {
                validate_tablet.with_meta_lock("validate compaction publish", || {
                    validate_tablet.validate_prepare_token(&token)?;
                    validate_tablet.validate_compaction_publish_locked(
                        &record,
                        &input_rowsets,
                        validation_rowset.as_ref(),
                    )
                })
            })?;
            durable_submitted = true;
            let compaction_op = ctx
                .record
                .storage_ops
                .first()
                .and_then(|op| match op {
                    StorageCommitOp::Tablet(tablet_op) => tablet_op.mutations.first().cloned(),
                })
                .ok_or_else(|| {
                    paro_error::internal("compaction maintenance plan missing mutation")
                })?;
            apply_runtime.submit(ApplyRequest {
                lsn: ctx.lsn,
                durable_batch_lsn: ctx.durable_batch_lsn,
                commit_id: None,
                wait_mode: WaitMode::Published,
                catalog_serial: !ctx.record.catalog_ops.is_empty(),
                catalog_pre: Box::new(|| Ok(())),
                tablet_parts: vec![TabletApplyPart {
                    tablet_id: tablet.tablet_id(),
                    apply: Box::new(move || apply_tablet.apply_compaction_publish(&compaction_op)),
                }],
                descriptor_phase: Box::new(|| Ok(())),
                catalog_post: Box::new(|| Ok(())),
                on_published: Box::new(|| Ok(())),
            })
        } else {
            Self::validate_prepared_publish(tablet, &prepared)?;
            let compaction_op = prepared
                .maintenance_plan
                .storage_ops
                .first()
                .and_then(|op| match op {
                    StorageCommitOp::Tablet(tablet_op) => tablet_op.mutations.first(),
                })
                .ok_or_else(|| {
                    paro_error::internal("compaction maintenance plan missing mutation")
                })?;
            tablet.apply_compaction_publish(compaction_op)
        };

        if result.is_err() && !durable_submitted && cleanup_path.exists() {
            crate::compaction::cleanup::cleanup_now(&cleanup_path);
        }

        result.map(|_| ())
    }

    fn prepare_publish(
        tablet: &Arc<Tablet>,
        request: CompactionPublishRequest,
    ) -> Result<PreparedPublish> {
        let CompactionPublishRequest {
            output,
            record,
            maintenance_plan,
            token,
        } = request;

        let artifact = match output {
            CompactionBuildOutput::Rowset(artifact) => artifact,
            CompactionBuildOutput::PrimaryKey { artifact, .. } => artifact,
        };

        let input_rowsets = artifact.plan.input_rowset_ptrs();
        let staged_path = artifact.workspace.rowset_dir.clone();
        let validation_rowset = build_rowset(
            tablet,
            &artifact,
            &staged_path,
            artifact.rowset.statistics().ok(),
        )?;

        Ok(PreparedPublish {
            record,
            maintenance_plan,
            token,
            runtime: PreparedPublishRuntime {
                _artifact_guard: artifact,
                input_rowsets,
                validation_rowset,
                staged_path,
            },
        })
    }

    fn validate_prepared_publish(tablet: &Arc<Tablet>, prepared: &PreparedPublish) -> Result<()> {
        tablet.with_meta_lock("validate compaction publish", || {
            tablet.validate_prepare_token(&prepared.token)?;
            tablet.validate_compaction_publish_locked(
                &prepared.record,
                &prepared.runtime.input_rowsets,
                prepared.runtime.validation_rowset.as_ref(),
            )
        })
    }
}

struct PreparedPublish {
    record: CompactionPublishRecord,
    maintenance_plan: PreparedMaintenancePlan,
    token: PrepareToken,
    runtime: PreparedPublishRuntime,
}

fn build_rowset(
    tablet: &Tablet,
    artifact: &StagedArtifact,
    rowset_path: &Path,
    staged_stats: Option<crate::rowset::rowset_statistics::RowsetStatistics>,
) -> Result<RowsetSharedPtr> {
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("Tablet schema missing during compaction publish"))?;
    let mut rowset_meta = artifact.rowset.rowset_meta();
    rowset_meta.set_rowset_path(rowset_path.to_string_lossy().to_string());
    let rowset = Arc::new(Rowset::create(schema, rowset_meta, rowset_path)?);
    if let Some(stats) = staged_stats {
        rowset.set_statistics_cache(stats);
    }
    Ok(rowset)
}
