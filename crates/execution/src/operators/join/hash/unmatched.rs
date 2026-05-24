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
use crate::runtime::breaker::{HandleRef, JoinBuildHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, HashJoinUnmatchedSourceLocal, SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct HashJoinUnmatchedSourceExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub left_output_types: Box<[LogicalType]>,
    pub right_projection: Box<[usize]>,
    pub output_types: Box<[LogicalType]>,
}

impl HashJoinUnmatchedSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::HashJoinUnmatched(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        Ok(SourceLocal::HashJoinUnmatched(
            HashJoinUnmatchedSourceLocal::default(),
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
        if local.scan_state.is_none() {
            local.scan_state = Some(hash_table.create_full_outer_scan_state());
        }
        let scan_state = local
            .scan_state
            .as_mut()
            .expect("hash join unmatched scan state initialized");

        let emit_found = matches!(self.join_type, JoinType::RightSemi);
        let mut build_chunk = Chunk::try_initialize(
            &hash_table.build_types,
            VECTOR_SIZE,
            output.allocator().clone(),
        )?;
        let count = hash_table.scan_full_outer(scan_state, emit_found, &mut build_chunk)?;
        if count == 0 {
            output.try_set_cardinality(0)?;
            return Ok(SourcePoll::Finished);
        }

        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        let build_sel = SelectionVector::try_incremental(count, output.allocator().clone())?;
        match self.join_type {
            JoinType::Right | JoinType::Outer => construct_right_outer_scan_result(
                &build_chunk,
                &build_sel,
                count,
                &self.left_output_types,
                &self.right_projection,
                output,
            )?,
            JoinType::RightSemi | JoinType::RightAnti => construct_semi_join_result(
                &build_chunk,
                &build_sel,
                count,
                &self.right_projection,
                output,
            )?,
            _ => unreachable!("unmatched source only emits right-side joins"),
        }
        Ok(SourcePoll::Output)
    }
}
