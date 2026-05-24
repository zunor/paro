// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_function::scalar::FunctionExecContext;
use paro_planner::operator::join::{JoinCondition, JoinType};
use paro_storage::row::RowScanState;

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use crate::operators::join::hash::runtime::{
    emit_empty_build_probe_result, evaluate_join_keys_into, hash_join_memory_context,
    join_key_types, probe_input_from_spill_chunk_into, scan_hash_join_results, JoinKeySide,
};
use crate::operators::output::ensure_source_output;
use crate::runtime::breaker::{HandleRef, JoinBuildHandle};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, HashJoinSpillReplayPartitionLocal, HashJoinSpillReplaySourceLocal,
    SourceGlobal, SourceLocal,
};

#[derive(Debug, Clone)]
pub struct HashJoinSpillReplaySourceExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub probe_types: Box<[LogicalType]>,
    pub build_payload_types: Box<[LogicalType]>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub output_types: Box<[LogicalType]>,
}

impl HashJoinSpillReplaySourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::HashJoinSpillReplay(Arc::new(
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
        Ok(SourceLocal::HashJoinSpillReplay(
            HashJoinSpillReplaySourceLocal {
                probe_key_types: join_key_types(&self.conditions, JoinKeySide::Probe),
                probe_key_executors: self
                    .conditions
                    .iter()
                    .map(|condition| ExpressionExecutor::new(&condition.left))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                current: None,
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
        let SourceGlobal::HashJoinSpillReplay(global) = global else {
            return Err(paro_error::internal(
                "hash join spill replay source global state mismatch",
            ));
        };
        let SourceLocal::HashJoinSpillReplay(local) = local else {
            return Err(paro_error::internal(
                "hash join spill replay source local state mismatch",
            ));
        };
        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        if !global.handle.is_external() {
            return Ok(SourcePoll::Finished);
        }

        loop {
            if local.current.is_none() {
                local.current = self.prepare_next_partition(ctx, global.handle.as_ref())?;
                if local.current.is_none() {
                    output.try_set_cardinality(0)?;
                    return Ok(SourcePoll::Finished);
                }
            }

            let current = local
                .current
                .as_mut()
                .expect("hash join replay partition initialized");
            if current.probe_in_progress {
                let count = self.scan_current_probe(current, output)?;
                if count > 0 {
                    return Ok(SourcePoll::Output);
                }
                continue;
            }

            let scanned = current.probe_rows.scan_with_state(
                &mut current.probe_scan_state,
                &mut current.probe_spill_chunk,
            )?;
            if scanned == 0 {
                local.current = None;
                continue;
            }
            current.probe_spill_chunk.try_set_cardinality(scanned)?;
            probe_input_from_spill_chunk_into(
                &current.probe_spill_chunk,
                self.probe_types.len(),
                &mut current.probe_input,
            )?;
            let replay_input = current
                .probe_input
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join replay input chunk missing"))?;
            if current.hash_table.is_empty() {
                let emitted = emit_empty_build_probe_result(
                    self.join_type,
                    &replay_input,
                    &self.left_projection,
                    &self.right_projection,
                    &self.output_types,
                    output,
                )?;
                if emitted > 0 {
                    return Ok(SourcePoll::Output);
                }
                continue;
            }

            evaluate_join_keys_into(
                ctx,
                replay_input,
                &self.conditions,
                &mut local.probe_key_executors,
                &local.probe_key_types,
                JoinKeySide::Probe,
                &mut current.probe_keys,
            )?;
            let probe_keys = current
                .probe_keys
                .as_ref()
                .ok_or_else(|| paro_error::internal("hash join replay probe keys missing"))?;
            current.hash_table.probe(
                &probe_keys,
                &mut current.scan_structure,
                None,
                probe_keys.size(),
            )?;
            current.probe_in_progress = true;
        }
    }

    fn prepare_next_partition(
        &self,
        ctx: &mut OperatorCallContext,
        handle: &JoinBuildHandle,
    ) -> Result<Option<HashJoinSpillReplayPartitionLocal>> {
        let Some(partition) = handle.spill.take_next_replay_partition()? else {
            return Ok(None);
        };
        let template = handle.require_table()?;
        let hash_table = Arc::new(JoinHashTable::new_with_memory(
            template.buffer_pool().clone(),
            ctx.query.allocator(MemoryTag::HashTable),
            self.conditions.to_vec(),
            self.build_payload_types.to_vec(),
            self.join_type,
            JoinHashTableConfig::default(),
            hash_join_memory_context(ctx.query),
        ));

        let mut build_scan = RowScanState::default();
        let mut build_chunk = Chunk::try_new(ctx.query.allocator(MemoryTag::BaseTable))?;
        loop {
            let scanned = partition
                .build_rows
                .scan_with_state(&mut build_scan, &mut build_chunk)?;
            if scanned == 0 {
                break;
            }
            build_chunk.try_set_cardinality(scanned)?;
            hash_table.get_build_store().append_chunk(&build_chunk)?;
        }
        hash_table.refresh_count_from_data_collection();
        hash_table.finalize()?;
        let scan_structure = hash_table.create_scan_structure()?;
        Ok(Some(HashJoinSpillReplayPartitionLocal {
            partition_idx: partition.partition_idx,
            hash_table,
            probe_rows: partition.probe_rows,
            probe_scan_state: RowScanState::default(),
            probe_spill_chunk: Chunk::try_new(ctx.query.allocator(MemoryTag::BaseTable))?,
            probe_input: None,
            probe_keys: None,
            scan_structure,
            probe_in_progress: false,
        }))
    }

    fn scan_current_probe(
        &self,
        current: &mut HashJoinSpillReplayPartitionLocal,
        output: &mut Chunk,
    ) -> Result<usize> {
        ensure_source_output(output, &self.output_types, VECTOR_SIZE)?;
        let probe_keys = current
            .probe_keys
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join replay probe keys missing"))?;
        let probe_input = current
            .probe_input
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join replay probe input missing"))?;
        let count = scan_hash_join_results(
            self.join_type,
            probe_keys,
            probe_input,
            output,
            current.hash_table.as_ref(),
            &mut current.scan_structure,
            &self.left_projection,
            &self.right_projection,
        )?;
        if current.scan_structure.finished {
            current.probe_in_progress = false;
        }
        Ok(count)
    }
}
