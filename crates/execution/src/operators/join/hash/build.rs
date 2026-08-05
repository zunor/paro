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

use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::join_hashtable::{JoinHashTable, JoinHashTableConfig};
use crate::operators::join::hash::keys::{evaluate_join_keys_into, join_key_types, JoinKeySide};
use crate::operators::join::hash::memory::{
    hash_join_memory_context, hash_join_spill_memory_context,
};
use crate::operators::join::hash::payload::build_payload_chunk_ref;
use crate::physical::properties::MemoryClass;
use crate::physical::properties::RequiredProperties;
use crate::runtime::breaker::{
    HandleRef, HashJoinBuildSpillReclaimer, HashJoinLocalBuildSpillReclaimer, JoinBuildHandle,
    JoinRuntimeFilterSketch,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
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
        )?;
        ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
            HashJoinBuildSpillReclaimer::new(
                handle.clone(),
                hash_join_spill_memory_context(ctx.query),
                ctx.query.memory.capacity_bytes(),
            ),
        ));
        Ok(SinkGlobal::HashJoinBuild(Arc::new(BreakerHandleGlobal {
            handle,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let handle = ctx.handles.get(self.handle)?;
        let build_key_types = join_key_types(&self.conditions, JoinKeySide::Build);
        let hash_table = Arc::new(JoinHashTable::new_with_memory(
            ctx.query.session.buffer_pool().clone(),
            ctx.query.allocator(MemoryTag::HashTable),
            self.conditions.to_vec(),
            self.build_payload_types.to_vec(),
            self.join_type,
            JoinHashTableConfig::default(),
            hash_join_memory_context(ctx.query),
        ));
        let build_spill = Arc::new(parking_lot::Mutex::new(None));
        let (local_build_spill_reclaimer_name, query_memory) =
            if !hash_join_local_build_spill_supported(self.join_type) {
                (None, None)
            } else {
                let local_id = HashJoinLocalBuildSpillReclaimer::next_local_id();
                let name = HashJoinLocalBuildSpillReclaimer::name_for(&handle, local_id);
                ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                    HashJoinLocalBuildSpillReclaimer::new(
                        handle,
                        local_id,
                        Arc::clone(&hash_table),
                        Arc::clone(&build_spill),
                        hash_join_spill_memory_context(ctx.query),
                        ctx.query.memory.capacity_bytes(),
                    ),
                ));
                (Some(name), Some(Arc::clone(&ctx.query.memory)))
            };
        Ok(SinkLocal::HashJoinBuild(HashJoinBuildSinkLocal {
            hash_table: Some(hash_table),
            build_keys: None,
            build_payload: None,
            build_selection: None,
            build_hashes: Vec::new(),
            runtime_filter_sketch: Some(JoinRuntimeFilterSketch::empty(&build_key_types)),
            build_spill,
            local_build_spill_reclaimer_name,
            query_memory,
            build_key_types,
            build_key_executors: self
                .conditions
                .iter()
                .map(|condition| {
                    ExpressionExecutor::with_expressions_for_session(
                        std::slice::from_ref(&condition.right),
                        ctx.query.session.as_ref(),
                    )
                })
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
        let build_selection = ensure_build_selection(
            &mut local.build_selection,
            key_chunk.size(),
            key_chunk.allocator().clone(),
        )?;
        let appended_count = hash_table.build_with_scratch(
            &key_chunk,
            &payload_chunk,
            build_selection,
            &mut local.build_hashes,
        )?;
        if appended_count > 0 {
            local
                .runtime_filter_sketch
                .as_mut()
                .ok_or_else(|| paro_error::internal("hash join runtime filter sketch missing"))?
                .add_key_chunk(key_chunk, build_selection, appended_count)?;
        }
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
        local.unregister_local_reclaimers();
        if let Some(build_spill) = local.build_spill.lock().take() {
            global.handle.spill.append_build_buffer(build_spill)?;
        }
        if let Some(local_table) = local.hash_table.take() {
            global.handle.require_table()?.merge(local_table)?;
        }
        global
            .handle
            .merge_runtime_filter_sketch(local.runtime_filter_sketch.take())?;
        Ok(MergePoll::Done)
    }

    pub(crate) fn prepare_finish(
        &self,
        _ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<PrepareFinishPoll> {
        let SinkGlobal::HashJoinBuild(global) = global else {
            return Err(paro_error::internal(
                "hash join build sink global state mismatch",
            ));
        };
        global.handle.enable_build_reclaim();
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
        Ok(FinishWork::Parallel(FinishTaskGroupRunner::group(
            "hash_join_finalize",
            memory_class,
            move |ctx| {
                if handle.completion.is_complete() {
                    unregister_hash_join_build_reclaimer(ctx, handle.as_ref());
                    return Ok(());
                }
                let result = if force_external
                    || should_use_memory_triggered_external_join(ctx, handle.as_ref())
                {
                    finish_external_hash_join(ctx, handle.as_ref())
                } else {
                    finalize_in_memory_hash_join(ctx, handle.as_ref())
                };
                if result.is_ok() {
                    unregister_hash_join_build_reclaimer(ctx, handle.as_ref());
                }
                result
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
        if global.handle.completion.is_complete() {
            unregister_hash_join_build_reclaimer(ctx, global.handle.as_ref());
            return Ok(FinishPoll::Done);
        }
        if (self.force_external
            || should_use_memory_triggered_external_join(ctx, global.handle.as_ref()))
            && !global.handle.is_external()
        {
            finish_external_hash_join(ctx, global.handle.as_ref())?;
        } else if !self.force_external && !global.handle.completion.is_complete() {
            finalize_in_memory_hash_join(ctx, global.handle.as_ref())?;
        }
        unregister_hash_join_build_reclaimer(ctx, global.handle.as_ref());
        Ok(FinishPoll::Done)
    }
}

fn hash_join_local_build_spill_supported(join_type: JoinType) -> bool {
    !matches!(join_type, JoinType::Invalid)
}

fn unregister_hash_join_build_reclaimer(ctx: &mut OperatorFinishContext, handle: &JoinBuildHandle) {
    handle.disable_build_reclaim();
    ctx.query
        .memory
        .unregister_reclaimer_by_name(&HashJoinBuildSpillReclaimer::name_for(handle));
}

fn finalize_in_memory_hash_join(
    ctx: &mut OperatorFinishContext,
    handle: &JoinBuildHandle,
) -> Result<()> {
    let table = handle.require_table()?;
    let published = publish_runtime_filter(ctx, handle)?;
    table.finalize()?;
    handle.completion.mark_complete();
    if published {
        record_runtime_filter_installed(ctx);
    }
    Ok(())
}

fn ensure_build_selection(
    slot: &mut Option<paro_common::vector::SelectionVector>,
    capacity: usize,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<&mut paro_common::vector::SelectionVector> {
    let needs_new = slot
        .as_ref()
        .map_or(true, |selection| selection.capacity() < capacity);
    if needs_new {
        *slot = Some(paro_common::vector::SelectionVector::try_with_capacity(
            capacity, allocator,
        )?);
    }
    Ok(slot
        .as_mut()
        .expect("hash join build selection was initialized above"))
}

fn finish_external_hash_join(
    ctx: &mut OperatorFinishContext,
    handle: &JoinBuildHandle,
) -> Result<()> {
    if handle.completion.is_complete() {
        return Ok(());
    }
    if handle.is_external() {
        handle.completion.mark_complete();
        return Ok(());
    }
    let was_ready = handle.runtime_filter_ready();
    handle.spill_build_for_reclaim(
        usize::MAX,
        ctx.query.memory.capacity_bytes(),
        hash_join_memory_context(ctx.query),
    )?;
    if !handle.completion.is_complete() {
        handle.publish_runtime_filter_from_builder()?;
        handle.completion.mark_complete();
    }
    if !was_ready && handle.runtime_filter_ready() {
        record_runtime_filter_installed(ctx);
    }
    Ok(())
}

fn publish_runtime_filter(_ctx: &OperatorFinishContext, handle: &JoinBuildHandle) -> Result<bool> {
    let was_ready = handle.runtime_filter_ready();
    handle.publish_runtime_filter_from_builder()?;
    Ok(!was_ready && handle.runtime_filter_ready())
}

fn record_runtime_filter_installed(ctx: &mut OperatorFinishContext) {
    ctx.profiler.record_runtime(
        ctx.operator.index() as u64,
        ExplainRuntimeStats {
            runtime_filter_installed_count: Some(1),
            ..ExplainRuntimeStats::default()
        },
    );
}

fn should_use_memory_triggered_external_join(
    ctx: &OperatorFinishContext,
    handle: &JoinBuildHandle,
) -> bool {
    if handle.completion.is_complete() {
        return false;
    }
    if handle.is_external() {
        return true;
    }
    if handle.has_build_spill() {
        return true;
    }
    let Some(table) = handle.table() else {
        return false;
    };
    let build_bytes = table.size_in_bytes();
    if build_bytes == 0 {
        return false;
    }
    let capacity = ctx.query.memory.capacity_bytes();
    let available = ctx.query.memory.available_bytes();
    build_bytes > available || build_bytes > capacity / 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_build_spill_supports_all_valid_join_types() {
        assert!(hash_join_local_build_spill_supported(JoinType::Inner));
        assert!(hash_join_local_build_spill_supported(JoinType::Left));
        assert!(hash_join_local_build_spill_supported(JoinType::Semi));
        assert!(hash_join_local_build_spill_supported(JoinType::Anti));
        assert!(hash_join_local_build_spill_supported(JoinType::Single));
        assert!(hash_join_local_build_spill_supported(JoinType::Mark));
        assert!(hash_join_local_build_spill_supported(JoinType::Right));
        assert!(hash_join_local_build_spill_supported(JoinType::Outer));
        assert!(hash_join_local_build_spill_supported(JoinType::RightSemi));
        assert!(hash_join_local_build_spill_supported(JoinType::RightAnti));
        assert!(!hash_join_local_build_spill_supported(JoinType::Invalid));
    }
}
