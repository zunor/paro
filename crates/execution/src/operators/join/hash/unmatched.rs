// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, VECTOR_SIZE};
use paro_planner::operator::join::JoinType;

use crate::operators::join::join_result_helpers::{
    construct_right_outer_scan_result, construct_semi_join_result,
};
use crate::operators::output::ensure_source_output;
use crate::physical::specs::HashReductionCascadeSpec;
use crate::runtime::breaker::{HandleRef, JoinBuildHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    HashJoinUnmatchedSourceGlobal, HashJoinUnmatchedSourceLocal, SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct HashJoinUnmatchedSourceExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub left_output_types: Box<[LogicalType]>,
    pub output_types: Box<[LogicalType]>,
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

impl HashJoinUnmatchedSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::HashJoinUnmatched(Arc::new(
            HashJoinUnmatchedSourceGlobal::new(ctx.handles.get(self.handle)?),
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        let reduction_channel_masks = self
            .reduction_cascade
            .as_ref()
            .and_then(|cascade| cascade.grouped_extrema.as_ref())
            .map(|grouped| {
                grouped
                    .channels
                    .iter()
                    .map(|channel| channel.match_mask)
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            })
            .unwrap_or_default();
        Ok(SourceLocal::HashJoinUnmatched(
            HashJoinUnmatchedSourceLocal {
                scan_state: None,
                reduction_channel_masks,
            },
        ))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::HashJoinUnmatched(global) = global else {
            return Err(paro_error::internal(
                "hash join unmatched source global state mismatch",
            ));
        };
        let SourceLocal::HashJoinUnmatched(local) = local else {
            return Err(paro_error::internal(
                "hash join unmatched source local state mismatch",
            ));
        };
        if !matches!(
            self.join_type,
            JoinType::Right | JoinType::Outer | JoinType::RightSemi | JoinType::RightAnti
        ) {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        }
        if !global.handle.completion.is_complete() {
            return Err(paro_error::internal(
                "hash join unmatched source scheduled before build finalized",
            ));
        }
        let hash_table = global.handle.require_table()?;
        let emit_found = matches!(self.join_type, JoinType::RightSemi);
        let mut build_chunk = Chunk::try_initialize(
            hash_table.build_output_types(),
            VECTOR_SIZE,
            output.allocator().clone(),
        )?;
        let block_count = hash_table.build_block_count();
        let count = loop {
            if local.scan_state.is_none() {
                let Some(block_idx) = global.claim_block(block_count) else {
                    output.try_set_cardinality(0)?;
                    return Ok(SourcePoll::Finished);
                };
                local.scan_state =
                    Some(hash_table.create_full_outer_scan_state_for_block(block_idx));
            }
            let scan_state = local
                .scan_state
                .as_mut()
                .expect("hash join unmatched scan state initialized");
            let count = if let Some(cascade) = &self.reduction_cascade {
                if let Some(grouped) = &cascade.grouped_extrema {
                    hash_table.scan_grouped_reduction_extrema(
                        scan_state,
                        grouped.build_residual_offset,
                        &local.reduction_channel_masks,
                        cascade.required_mask,
                        cascade.forbidden_mask,
                        &mut build_chunk,
                    )?
                } else {
                    hash_table.scan_reduction_cascade(
                        scan_state,
                        cascade.required_mask,
                        cascade.forbidden_mask,
                        &mut build_chunk,
                    )?
                }
            } else {
                hash_table.scan_full_outer(scan_state, emit_found, &mut build_chunk)?
            };
            if count > 0 {
                break count;
            }
            local.scan_state = None;
        };

        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        let build_sel = SelectionVector::try_incremental(count, output.allocator().clone())?;
        let build_projection = (0..build_chunk.column_count()).collect::<Vec<_>>();
        match self.join_type {
            JoinType::Right | JoinType::Outer => construct_right_outer_scan_result(
                &build_chunk,
                &build_sel,
                count,
                &self.left_output_types,
                &build_projection,
                output,
            )?,
            JoinType::RightSemi | JoinType::RightAnti => construct_semi_join_result(
                &build_chunk,
                &build_sel,
                count,
                &build_projection,
                output,
            )?,
            _ => unreachable!("unmatched source only emits right-side joins"),
        }
        Ok(SourcePoll::Output)
    }
}
