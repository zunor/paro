// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_planner::operator::join::{JoinCondition, JoinType};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::join::hash::hashing::compute_hashes_for_keys_into;
use crate::operators::join::hash::keys::{evaluate_join_keys_into, join_key_types, JoinKeySide};
use crate::operators::join::hash::memory::hash_join_memory_context;
use crate::operators::join::hash::probe_output::{
    emit_empty_build_probe_result, scan_hash_join_results,
};
use crate::operators::join::hash::spill::build_probe_spill_chunk_into;
use crate::operators::output::ensure_transform_output;
use crate::runtime::breaker::{HandleRef, JoinBuildHandle, JoinProbeSpillBuffer};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    BreakerHandleGlobal, HashJoinProbeTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct HashJoinProbeTransformExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub output_types: Box<[LogicalType]>,
}

impl HashJoinProbeTransformExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::HashJoinProbe(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        Ok(TransformLocal::HashJoinProbe(HashJoinProbeTransformLocal {
            scan_structure: None,
            probe_keys: None,
            probe_key_types: join_key_types(&self.conditions, JoinKeySide::Probe),
            probe_key_executors: self
                .conditions
                .iter()
                .map(|condition| {
                    ExpressionExecutor::with_expressions_for_session(
                        std::slice::from_ref(&condition.left),
                        ctx.query.session.as_ref(),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            probe_hashes: None,
            probe_spill_chunk: None,
            probe_spill_buffer: None,
            probe_in_progress: false,
        }))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        ctx.cancel.check()?;
        let TransformGlobal::HashJoinProbe(global) = global else {
            return Err(paro_error::internal(
                "hash join probe transform global state mismatch",
            ));
        };
        let TransformLocal::HashJoinProbe(local) = local else {
            return Err(paro_error::internal(
                "hash join probe transform local state mismatch",
            ));
        };
        if !global.handle.completion.is_complete() {
            return Err(paro_error::internal(
                "hash join probe scheduled before build handle finalized",
            ));
        }
        let hash_table = global.handle.require_table()?;
        ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;

        if global.handle.is_external() {
            if input.is_empty() {
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }
            evaluate_join_keys_into(
                ctx,
                input,
                &self.conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut local.probe_keys,
            )?;
            let probe_keys = local
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join probe keys missing"))?;
            compute_hashes_for_keys_into(hash_table.as_ref(), probe_keys, &mut local.probe_hashes)?;
            let hashes = local
                .probe_hashes
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join probe hashes missing"))?;
            build_probe_spill_chunk_into(input, hashes, &mut local.probe_spill_chunk)?;
            let radix_bits = global
                .handle
                .external_config()
                .map(|config| config.radix_bits as usize)
                .ok_or_else(|| {
                    paro_error::internal("hash join external mode has no replay config")
                })?;
            {
                let spill_chunk = local
                    .probe_spill_chunk
                    .as_ref()
                    .ok_or_else(|| paro_error::internal("hash join probe spill chunk missing"))?;
                if local.probe_spill_buffer.is_none() {
                    local.probe_spill_buffer = Some(JoinProbeSpillBuffer::new(
                        hash_table.buffer_pool().clone(),
                        radix_bits,
                        input.column_count(),
                        spill_chunk.types(),
                        hash_join_memory_context(ctx.query),
                    )?);
                }
                local
                    .probe_spill_buffer
                    .as_mut()
                    .expect("hash join probe spill buffer initialized")
                    .append(spill_chunk)?;
            }
            if let Some(spill_chunk) = local.probe_spill_chunk.as_mut() {
                spill_chunk.data.clear();
            }
            output.try_set_cardinality(0)?;
            return Ok(TransformPoll::NeedMoreInput);
        }

        if hash_table.is_empty()
            && !(self.join_type == JoinType::Mark && hash_table.has_null_keys())
        {
            let emitted = emit_empty_build_probe_result(
                self.join_type,
                input,
                &self.left_projection,
                &self.right_projection,
                &self.output_types,
                output,
            )?;
            return if emitted > 0 {
                Ok(TransformPoll::Output)
            } else {
                Ok(TransformPoll::NeedMoreInput)
            };
        }

        if !local.probe_in_progress {
            if input.is_empty() {
                output.try_set_cardinality(0)?;
                return Ok(TransformPoll::NeedMoreInput);
            }
            evaluate_join_keys_into(
                ctx,
                input,
                &self.conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut local.probe_keys,
            )?;
            if local.scan_structure.is_none() {
                local.scan_structure = Some(hash_table.create_scan_structure()?);
            }
            let probe_keys = local
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join probe keys missing"))?;
            let scan_structure = local
                .scan_structure
                .as_mut()
                .expect("hash join scan structure initialized");
            hash_table.probe(&probe_keys, scan_structure, None, probe_keys.size())?;
            local.probe_in_progress = true;
        }

        loop {
            ensure_transform_output(output, &self.output_types, VECTOR_SIZE)?;
            let (count, finished) = {
                let probe_keys = local
                    .probe_keys
                    .as_ref()
                    .ok_or_else(|| paro_error::internal("hash join probe keys missing"))?;
                let scan_structure = local
                    .scan_structure
                    .as_mut()
                    .ok_or_else(|| paro_error::internal("hash join scan structure missing"))?;
                let count = scan_hash_join_results(
                    self.join_type,
                    probe_keys,
                    input,
                    output,
                    &hash_table,
                    scan_structure,
                    &self.left_projection,
                    &self.right_projection,
                )?;
                (count, scan_structure.finished)
            };
            if finished {
                local.probe_in_progress = false;
            }
            if count > 0 {
                return if local.probe_in_progress {
                    Ok(TransformPoll::OutputMore)
                } else {
                    Ok(TransformPoll::Output)
                };
            }
            if !local.probe_in_progress {
                return Ok(TransformPoll::NeedMoreInput);
            }
        }
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &TransformGlobal,
        local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        let TransformGlobal::HashJoinProbe(global) = global else {
            return Err(paro_error::internal(
                "hash join probe transform global state mismatch",
            ));
        };
        let TransformLocal::HashJoinProbe(local) = local else {
            return Err(paro_error::internal(
                "hash join probe transform local state mismatch",
            ));
        };
        if let Some(buffer) = local.probe_spill_buffer.take() {
            global.handle.spill.append_probe_buffer(buffer)?;
        }
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}
