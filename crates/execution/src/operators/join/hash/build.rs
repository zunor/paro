// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash join build-side sink operator — accumulates the build relation into a
//! shared join hash table, optionally spilling to disk for external joins.

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::scalar::FunctionExecContext;
use paro_planner::operator::join::{JoinCondition, JoinType};
use paro_storage::buffer::MemoryTag as StorageMemoryTag;
use paro_storage::row::{RadixPartitionedRowsBuilder, RowLayout};

use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use crate::operators::join::hash::runtime::{
    build_payload_chunk_ref, evaluate_join_keys_into, hash_join_memory_context, join_key_types,
    JoinKeySide,
};
use crate::physical::properties::MemoryClass;
use crate::physical::properties::RequiredProperties;
use crate::runtime::breaker::{
    HandleRef, JoinBuildHandle, JoinExternalModeConfig, JoinPartitionSet, ProbeSpillSet,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SingleTaskFinishDriver, SinkPoll,
};
use crate::runtime::state::{BreakerHandleGlobal, HashJoinBuildSinkLocal, SinkGlobal, SinkLocal};

#[derive(Debug, Clone)]
pub struct HashJoinBuildSinkExec {
    pub handle: HandleRef<JoinBuildHandle>,
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub build_projection: Box<[usize]>,
    pub build_payload_types: Box<[LogicalType]>,
    pub required: RequiredProperties,
    pub force_external: bool,
}

impl HashJoinBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        handle.initialize_table(
            ctx.query.session.buffer_pool().clone(),
            ctx.query.allocator(MemoryTag::HashTable),
            self.conditions.to_vec(),
            self.build_payload_types.to_vec(),
            self.join_type,
            hash_join_memory_context(ctx.query),
        );
        Ok(SinkGlobal::HashJoinBuild(Arc::new(BreakerHandleGlobal {
            handle,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        Ok(SinkLocal::HashJoinBuild(HashJoinBuildSinkLocal {
            hash_table: Some(Arc::new(JoinHashTable::new_with_memory(
                ctx.query.session.buffer_pool().clone(),
                ctx.query.allocator(MemoryTag::HashTable),
                self.conditions.to_vec(),
                self.build_payload_types.to_vec(),
                self.join_type,
                JoinHashTableConfig::default(),
                hash_join_memory_context(ctx.query),
            ))),
            build_keys: None,
            build_payload: None,
            build_key_types: join_key_types(&self.conditions, JoinKeySide::Build),
            build_key_executors: self
                .conditions
                .iter()
                .map(|condition| ExpressionExecutor::new(&condition.right))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkLocal::HashJoinBuild(local) = local else {
            return Err(paro_error::internal(
                "hash join build sink local state mismatch",
            ));
        };
        let hash_table = local
            .hash_table
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join local table already merged"))?;
        evaluate_join_keys_into(
            ctx,
            input,
            &self.conditions,
            &mut local.build_key_executors,
            &local.build_key_types,
            JoinKeySide::Build,
            &mut local.build_keys,
        )?;
        let key_chunk = local
            .build_keys
            .as_ref()
            .ok_or_else(|| paro_error::internal("hash join build key chunk missing"))?;
        let payload_chunk = build_payload_chunk_ref(
            input,
            &self.build_projection,
            &self.build_payload_types,
            &mut local.build_payload,
        )?;
        hash_table.build(&key_chunk, &payload_chunk)?;
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::HashJoinBuild(global) = global else {
            return Err(paro_error::internal(
                "hash join build sink global state mismatch",
            ));
        };
        let SinkLocal::HashJoinBuild(local) = local else {
            return Err(paro_error::internal(
                "hash join build sink local state mismatch",
            ));
        };
        if let Some(local_table) = local.hash_table.take() {
            global.handle.require_table()?.merge(local_table)?;
        }
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        Ok(PrepareFinishPoll::Done)
    }

    pub(crate) fn finish_work(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::HashJoinBuild(global) = global else {
            return Err(paro_error::internal(
                "hash join build sink global state mismatch",
            ));
        };
        let handle = global.handle.clone();
        let force_external = self.force_external;
        let memory_class = if force_external {
            MemoryClass::External
        } else {
            MemoryClass::Blocking
        };
        Ok(FinishWork::Parallel(SingleTaskFinishDriver::group(
            "hash_join_finalize",
            memory_class,
            move |ctx| {
                if force_external {
                    finish_external_hash_join(ctx, handle.as_ref())
                } else {
                    handle.finalize_in_memory()
                }
            },
        )))
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::HashJoinBuild(global) = global else {
            return Err(paro_error::internal(
                "hash join build sink global state mismatch",
            ));
        };
        if self.force_external && !global.handle.is_external() {
            finish_external_hash_join(ctx, global.handle.as_ref())?;
        } else if !self.force_external && !global.handle.completion.is_complete() {
            global.handle.finalize_in_memory()?;
        }
        Ok(FinishPoll::Done)
    }
}

fn finish_external_hash_join(
    ctx: &mut OperatorFinishContext,
    handle: &JoinBuildHandle,
) -> Result<()> {
    let table = handle.require_table()?;
    let radix_bits = 1usize;
    let hash_col_idx = table.hash_column_index();
    let mut builder = RadixPartitionedRowsBuilder::new_with_memory(
        table.buffer_pool().clone(),
        Arc::new(RowLayout::from_types(
            table.layout().types().to_vec(),
            paro_storage::row::RowValidityType::CanHaveNullValues,
        )),
        StorageMemoryTag::HashTable,
        radix_bits,
        hash_col_idx,
        hash_join_memory_context(ctx.query),
    )?;
    table.drain_build_store_spill_chunks(|chunk| builder.append(chunk))?;
    let build_partitions = builder.seal();
    let partition_count = build_partitions.partition_count();
    handle.spill.install_build_partitions(build_partitions)?;
    handle.set_external_mode(JoinExternalModeConfig {
        radix_bits: radix_bits as u8,
        build_partitions: JoinPartitionSet { partition_count },
        probe_partitions: ProbeSpillSet::default(),
    })?;
    handle.completion.mark_complete();
    Ok(())
}
