// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::workspace::CompactionBuildOutput;
use crate::compaction::plan::types::{CompactionJobId, CompactionPlanId, CumulativePointAction};
use crate::primary_key::DeleteVector;
use crate::rowset::RowsetId;
use crate::tablet::{PhysicalRowRef, Version};
use paro_common::durability::{PrepareToken, PreparedMaintenancePlan};
use paro_common::effect::{
    ArtifactRef, CompactionCumulativePointAction, RetiredRowsetInput, StorageCommitOp,
    TabletApplyOp, TabletMutation, VersionSpan,
};
use paro_common::error::{self as paro_error, ParoError};

#[derive(Debug, Clone)]
pub struct PkPublishDelta {
    pub snapshot_version: i64,
    pub max_input_version: i64,
    pub upsert_candidates: Vec<PkIndexUpsertCandidate>,
    pub internal_delete_vectors: Vec<SegmentDeleteDelta>,
}

#[derive(Debug, Clone)]
pub struct PkIndexUpsertCandidate {
    pub key: Vec<u8>,
    pub output_location: PhysicalRowRef,
    pub source_location: PhysicalRowRef,
}

#[derive(Debug, Clone)]
pub struct SegmentDeleteDelta {
    pub segment_id: u32,
    pub delete_vector: DeleteVector,
}

#[derive(Debug, Clone)]
pub struct RetiredInput {
    pub rowset_id: RowsetId,
    pub version: Version,
    pub rssids: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct CompactionPublishRecord {
    pub plan_id: CompactionPlanId,
    pub job_id: CompactionJobId,
    pub tablet_id: u64,
    pub output_rowset_id: RowsetId,
    pub output_version: Version,
    pub cumulative_point_action: CumulativePointAction,
    pub output_rowset_path: String,
    pub replaced_inputs: Vec<RowsetId>,
}

#[derive(Debug)]
pub struct CompactionPublishRequest {
    pub output: CompactionBuildOutput,
    pub record: CompactionPublishRecord,
    pub maintenance_plan: PreparedMaintenancePlan,
    pub token: PrepareToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPublishConflictReason {
    InputsMissing,
    InputsReplaced,
    VersionOverlap,
    SchemaEpochChanged,
    PkSourceInvalid,
}

#[derive(Debug, Clone)]
pub struct CompactionPublishConflict {
    pub tablet_id: u64,
    pub plan_id: CompactionPlanId,
    pub job_id: CompactionJobId,
    pub reason: CompactionPublishConflictReason,
}

impl CompactionPublishConflict {
    pub fn into_paro_error(self) -> ParoError {
        paro_error::serialization_failure(format!(
            "compaction publish conflict on tablet {} for {} / {}: {:?}",
            self.tablet_id, self.plan_id, self.job_id, self.reason
        ))
    }
}

pub fn maintenance_storage_op(
    record: &CompactionPublishRecord,
    staged_ref: ArtifactRef,
    output_ref: ArtifactRef,
    retired_inputs: &[RetiredInput],
) -> StorageCommitOp {
    StorageCommitOp::Tablet(TabletApplyOp {
        tablet_id: record.tablet_id,
        mutations: vec![TabletMutation::PublishCompaction {
            plan_id: record.plan_id.0,
            job_id: record.job_id.0,
            output_rowset_id: record.output_rowset_id,
            output_version: VersionSpan {
                start: record.output_version.start,
                end: record.output_version.end,
            },
            staged_ref,
            output_ref,
            replaced_inputs: record.replaced_inputs.clone(),
            retired_inputs: retired_inputs
                .iter()
                .map(|input| RetiredRowsetInput {
                    rowset_id: input.rowset_id,
                    start_version: input.version.start,
                    end_version: input.version.end,
                    rssids: input.rssids.clone(),
                })
                .collect(),
            cumulative_point_action: match record.cumulative_point_action {
                CumulativePointAction::Preserve => CompactionCumulativePointAction::Preserve,
                CumulativePointAction::AdvanceToOutputEndExclusive => {
                    CompactionCumulativePointAction::AdvanceToOutputEndExclusive
                }
            },
        }],
    })
}
