use crate::compaction::execution::workspace::CompactionBuildOutput;
use crate::compaction::plan::types::{CompactionJobId, CompactionPlanId, CumulativePointAction};
use crate::primary_key::DeleteVector;
use crate::rowset::RowsetId;
use crate::tablet::{PhysicalRowRef, Version};
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
    pub retired_inputs: Vec<RetiredInput>,
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
