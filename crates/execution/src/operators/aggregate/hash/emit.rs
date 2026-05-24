// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::vector::VECTOR_SIZE;

use crate::operators::output::ensure_source_output;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{AggregateHandle, AggregateRuntimeState, HandleRef};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, HashAggregateEmitSourceLocal, SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct HashAggregateEmitSourceExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
}

impl HashAggregateEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::HashAggregateEmit(Arc::new(
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
        Ok(SourceLocal::HashAggregateEmit(
            HashAggregateEmitSourceLocal::default(),
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
        let SourceGlobal::HashAggregateEmit(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate emit source global state mismatch",
            ));
        };
        if !global.handle.is_finalized() {
            return Err(paro_error::internal(
                "hash aggregate emit source polled before handle was finalized",
            ));
        }
        let SourceLocal::HashAggregateEmit(local) = local else {
            return Err(paro_error::internal(
                "hash aggregate emit source local state mismatch",
            ));
        };
        if local.tables.is_none() {
            let Some(state) = global.handle.take_state()? else {
                return Ok(SourcePoll::Finished);
            };
            let AggregateRuntimeState::Hash(state) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            local.positions = vec![Default::default(); state.tables.len()];
            local.tables = Some(state.tables);
        }
        ensure_source_output(output, &self.spec.output_types, VECTOR_SIZE)?;
        let tables = local.tables.as_mut().ok_or_else(|| {
            paro_error::internal("hash aggregate emit source did not load hash tables")
        })?;
        while local.grouping_idx < tables.len() {
            let table = &mut tables[local.grouping_idx];
            let position = local.positions.get_mut(local.grouping_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "hash aggregate source position out of bounds: grouping_idx={}",
                    local.grouping_idx
                ))
            })?;
            if table.scan(position, output)? {
                populate_grouping_columns(&self.spec, output, local.grouping_idx)?;
                return Ok(SourcePoll::Output);
            }
            local.grouping_idx += 1;
        }
        output.try_set_cardinality(0)?;
        Ok(SourcePoll::Finished)
    }
}

pub(crate) fn populate_grouping_columns(
    spec: &AggregateSpec,
    chunk: &mut Chunk,
    grouping_idx: usize,
) -> Result<()> {
    if spec.grouping_functions.is_empty() || chunk.is_empty() {
        return Ok(());
    }
    let grouping_set = spec.grouping_sets.get(grouping_idx).ok_or_else(|| {
        paro_error::internal(format!(
            "grouping set index out of bounds while populating GROUPING(): grouping_idx={grouping_idx}"
        ))
    })?;
    let grouping_offset = spec.grouping_key_count + spec.aggregates.len();
    let row_count = chunk.size();
    for (func_idx, grouping_fn) in spec.grouping_functions.iter().enumerate() {
        let output_idx = grouping_offset + func_idx;
        let grouping_col = chunk.column_mut(output_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing GROUPING() output column at index {output_idx}"
            ))
        })?;
        let value = Value::BigInt(grouping_value(grouping_set, grouping_fn));
        for row_idx in 0..row_count {
            grouping_col.set_value(row_idx, &value);
        }
    }
    Ok(())
}

fn grouping_value(grouping_set: &[usize], grouping_function: &[usize]) -> i64 {
    let mut value = 0i64;
    for (arg_idx, &group_idx) in grouping_function.iter().enumerate() {
        if !grouping_set.contains(&group_idx) {
            let bit = (grouping_function.len() - 1 - arg_idx) as i64;
            value |= 1_i64 << bit;
        }
    }
    value
}
