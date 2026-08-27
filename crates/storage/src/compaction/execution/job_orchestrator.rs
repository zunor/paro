// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::index_rebuild::rebuild_compaction_indexes;
use crate::compaction::execution::rowset_merger::RowsetMerger;
use crate::compaction::execution::vertical_merge::VerticalMerger;
use crate::compaction::plan::types::{
    CompactionJobId, CompactionLifecycleState, CompactionPlan, ExecutionLayout,
};
use crate::compaction::publish::{CompactionPublisher, CompactionValidator};
use crate::metrics::storage_metrics;
use crate::search::SearchInlineBuilderSet;
use crate::tablet::Tablet;
use paro_common::allocator::Allocator;
use paro_common::error::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn run_job(
    tablet: &Arc<Tablet>,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
) -> Result<bool> {
    run_job_inner(
        tablet,
        plan,
        job_id,
        allocator,
        SearchInlineBuilderSet::default(),
        CancellationToken::new(),
        |_| {},
    )
}

pub fn run_job_with_search_inline_builders(
    tablet: &Arc<Tablet>,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
    search_inline_builders: SearchInlineBuilderSet,
) -> Result<bool> {
    run_job_inner(
        tablet,
        plan,
        job_id,
        allocator,
        search_inline_builders,
        CancellationToken::new(),
        |_| {},
    )
}

pub fn run_job_with_lifecycle<F>(
    tablet: &Arc<Tablet>,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
    cancel_token: CancellationToken,
    on_state: F,
) -> Result<bool>
where
    F: FnMut(CompactionLifecycleState),
{
    run_job_inner(
        tablet,
        plan,
        job_id,
        allocator,
        SearchInlineBuilderSet::default(),
        cancel_token,
        on_state,
    )
}

pub fn run_job_with_lifecycle_and_search_inline_builders<F>(
    tablet: &Arc<Tablet>,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
    search_inline_builders: SearchInlineBuilderSet,
    cancel_token: CancellationToken,
    on_state: F,
) -> Result<bool>
where
    F: FnMut(CompactionLifecycleState),
{
    run_job_inner(
        tablet,
        plan,
        job_id,
        allocator,
        search_inline_builders,
        cancel_token,
        on_state,
    )
}

fn run_job_inner<F>(
    tablet: &Arc<Tablet>,
    plan: Arc<CompactionPlan>,
    job_id: CompactionJobId,
    allocator: Arc<dyn Allocator>,
    search_inline_builders: SearchInlineBuilderSet,
    cancel_token: CancellationToken,
    mut on_state: F,
) -> Result<bool>
where
    F: FnMut(CompactionLifecycleState),
{
    // A search generation embeds physical rowset/segment identities. Yield
    // background compaction while a foreground staged build owns that stable
    // layout, and hold this shared lease through durable compaction publish so
    // the two artifact lifecycles cannot cross.
    let Some(_layout_lease) = tablet.try_acquire_compaction_layout_lease()? else {
        storage_metrics().inc_compaction_layout_gate_skips();
        return Ok(false);
    };
    on_state(CompactionLifecycleState::Building);
    let workspace =
        crate::compaction::execution::workspace::CompactionWorkspace::create_with_cancel_token(
            tablet,
            job_id,
            plan.output_rowset_id,
            cancel_token,
        )?;

    let rebuild_search_definitions = search_inline_builders.clone();
    let output = match plan.execution_layout {
        ExecutionLayout::Vertical => VerticalMerger::build_with_search_inline_builders(
            tablet,
            plan.clone(),
            workspace,
            search_inline_builders,
        )?,
        ExecutionLayout::Horizontal => RowsetMerger::build_with_search_inline_builders(
            tablet,
            plan.clone(),
            workspace,
            allocator,
            search_inline_builders,
        )?,
    };
    let Some(output) = output else {
        return Ok(false);
    };

    CompactionValidator::validate_artifact(tablet, &output)?;
    on_state(CompactionLifecycleState::Validated);
    match &output {
        crate::compaction::execution::workspace::CompactionBuildOutput::Rowset(artifact) => {
            rebuild_compaction_indexes(
                tablet,
                artifact.rowset.clone(),
                plan.as_ref(),
                &rebuild_search_definitions,
            )?;
        }
        crate::compaction::execution::workspace::CompactionBuildOutput::PrimaryKey {
            artifact,
            ..
        } => {
            rebuild_compaction_indexes(
                tablet,
                artifact.rowset.clone(),
                plan.as_ref(),
                &rebuild_search_definitions,
            )?;
        }
    }

    on_state(CompactionLifecycleState::Publishing);
    let request = CompactionPublisher::prepare_request(tablet, output, job_id)?;
    if let Err(err) = CompactionPublisher::publish(tablet, request) {
        if err.is_retryable() {
            info!(
                tablet_id = tablet.tablet_id(),
                ?job_id,
                plan_id = plan.plan_id.0,
                error = %err,
                "Compaction publish skipped after concurrent mutation invalidated the prepared snapshot"
            );
            return Ok(false);
        }
        return Err(err);
    }
    on_state(CompactionLifecycleState::RetiredPendingGc);
    Ok(true)
}
