// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::index_rebuild::rebuild_compaction_indexes;
use crate::compaction::execution::rowset_merger::RowsetMerger;
use crate::compaction::execution::vertical_merge::VerticalMerger;
use crate::compaction::plan::types::{
    CompactionJobId, CompactionLifecycleState, CompactionPlan, ExecutionLayout,
};
use crate::compaction::publish::{CompactionPublisher, CompactionValidator};
use crate::tablet::Tablet;
use paro_common::allocator::Allocator;
use paro_common::error::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub fn run_job(
    tablet: &Tablet,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
) -> Result<bool> {
    run_job_with_lifecycle(
        tablet,
        plan,
        job_id,
        allocator,
        CancellationToken::new(),
        |_| {},
    )
}

pub fn run_job_with_lifecycle<F>(
    tablet: &Tablet,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
    cancel_token: CancellationToken,
    mut on_state: F,
) -> Result<bool>
where
    F: FnMut(CompactionLifecycleState),
{
    on_state(CompactionLifecycleState::Building);
    let workspace =
        crate::compaction::execution::workspace::CompactionWorkspace::create_with_cancel_token(
            tablet,
            job_id,
            plan.output_rowset_id,
            cancel_token,
        )?;

    let output = match plan.execution_layout {
        ExecutionLayout::Vertical => VerticalMerger::build(tablet, plan.clone(), workspace)?,
        ExecutionLayout::Horizontal => {
            RowsetMerger::build(tablet, plan.clone(), workspace, allocator)?
        }
    };
    let Some(output) = output else {
        return Ok(false);
    };

    CompactionValidator::validate_artifact(tablet, &output)?;
    on_state(CompactionLifecycleState::Validated);
    match &output {
        crate::compaction::execution::workspace::CompactionBuildOutput::Rowset(artifact) => {
            rebuild_compaction_indexes(tablet, artifact.rowset.clone(), plan.as_ref())?;
        }
        crate::compaction::execution::workspace::CompactionBuildOutput::PrimaryKey {
            artifact,
            ..
        } => {
            rebuild_compaction_indexes(tablet, artifact.rowset.clone(), plan.as_ref())?;
        }
    }

    on_state(CompactionLifecycleState::Publishing);
    let request = CompactionPublisher::prepare_request(tablet, output, job_id)?;
    CompactionPublisher::publish(tablet, request)?;
    on_state(CompactionLifecycleState::RetiredPendingGc);
    Ok(true)
}
