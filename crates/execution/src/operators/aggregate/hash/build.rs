// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Hash aggregate build sink operator.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use paro_common::allocator::{ArenaAllocator, MemoryTag};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::MemoryAccountingContext;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector, VECTOR_SIZE};
use paro_function::aggregate::{AggregateCombineType, AggregateInputData};
use paro_function::scalar::FunctionExecContext;
use paro_storage::buffer::BufferPool;
use paro_storage::row::{RowSpillWriter, RowStoreSpillWriter};

use crate::explain::types::ExplainRuntimeStats;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operators::aggregate::aggregate_kernel::{
    aggregate_state_spill_requires_serialization, aggregate_state_spill_supported, combine_states,
    deserialize_aggregate_state_blob, destroy_states,
};
use crate::operators::aggregate::aggregate_state::AggregateStateLayout;
#[cfg(test)]
use crate::operators::aggregate::build_helpers::build_groups_chunk;
use crate::operators::aggregate::build_helpers::{
    aggregate_objects, can_skip_regular_aggregate_sink, create_hash_aggregate_tables,
    group_payload_refs, group_types, has_aggregate_distinct, has_aggregate_ordered,
    normalized_grouping_sets, projected_payload_chunk, query_hash_table_memory,
    query_modifier_memory, update_hash_aggregate_tables,
};
use crate::operators::aggregate::distinct_helpers::{
    collect_distinct_rows, finalize_distinct_into_tables,
};
use crate::operators::aggregate::distinct_state::DistinctAggregateState;
use crate::operators::aggregate::group_hash::hash_group_columns;
use crate::operators::aggregate::group_key_codec::GroupKeyEncoder;
use crate::operators::aggregate::ordered_helpers::{
    collect_ordered_rows, empty_ordered_collectors_with_memory, finalize_ordered_into_hash_tables,
    merge_ordered_collectors,
};
use crate::operators::aggregate::payload_spill::{
    AggregatePayloadSpillBuffer, AggregateStateEncoding, AggregateStateSpillBuffer,
};
use crate::operators::aggregate::radix_partitioned_aggregate_hashtable::AggregateHashTable;
use crate::operators::aggregate::row_format::AggregateGroupFormat;
use crate::operators::sort::build::query_has_temporary_directory;
use crate::physical::properties::RequiredProperties;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::aggregate::AggregateSpilledOutput;
use crate::runtime::breaker::{
    AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer, AggregateHandle,
    AggregateLocalBuildCompactionReclaimer, AggregateLocalPayloadSpillReclaimer,
    AggregateLocalStateSpillReclaimer, AggregateRuntimeState, HandleRef, HashAggregateRuntimeState,
};
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::sink::{FinishPoll, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll};
use crate::runtime::state::{
    BreakerHandleGlobal, HashAggregateBuildSinkLocal, SinkGlobal, SinkLocal,
};

use super::distinct_finalize::prepare_parallel_distinct_finalize;

const HASH_AGGREGATE_PREEMPTIVE_SPILL_CAP_PER_THREAD: usize =
    paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE * 4;

/// Sink operator that builds hash aggregate tables (one per grouping set).
#[derive(Debug, Clone)]
pub struct HashAggregateBuildSinkExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
    pub required: RequiredProperties,
}

impl HashAggregateBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        if hash_aggregate_external_payload_spill_requested(ctx.query, &self.spec)
            && !query_has_temporary_directory(ctx.query)
        {
            return Err(paro_error::out_of_memory(
                "force_external hash aggregate requires a temporary directory",
            ));
        }
        let external_payload_spill =
            hash_aggregate_external_payload_spill_enabled(ctx.query, &self.spec);
        let tables = if external_payload_spill {
            Vec::new()
        } else {
            create_hash_aggregate_tables(
                &self.spec,
                ctx.query.allocator(MemoryTag::HashTable),
                query_hash_table_memory(ctx.query),
                ctx.query.session.number_of_threads(),
            )?
        };
        let group_refs = group_payload_refs(&self.spec)?;
        handle.initialize(AggregateRuntimeState::Hash(HashAggregateRuntimeState {
            tables,
            distinct: DistinctAggregateState::new(aggregate_objects(&self.spec)?.len()),
            spilled_payloads: Vec::new(),
            spilled_states: Vec::new(),
            spilled_outputs: None,
            ordered_collectors: empty_ordered_collectors_with_memory(
                &self.spec,
                &group_refs,
                ctx.query.session.buffer_pool().clone(),
                query_modifier_memory(ctx.query),
            )?,
        }))?;
        ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
            AggregateBuildCompactionReclaimer::new(handle.clone()),
        ));
        ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
            AggregateFinalizedStateReclaimer::for_query(
                handle.clone(),
                ctx.query.session.buffer_pool().clone(),
                ctx.query.memory.clone(),
            ),
        ));
        Ok(SinkGlobal::HashAggregateBuild(Arc::new(
            BreakerHandleGlobal { handle },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let handle = ctx.handles.get(self.handle)?;
        let raw_payload_spill_enabled =
            hash_aggregate_local_payload_spill_enabled(ctx.query, &self.spec);
        let aggregate_objects = Arc::from(aggregate_objects(&self.spec)?.into_boxed_slice());
        let skip_regular_sink = can_skip_regular_aggregate_sink(&self.spec, &aggregate_objects);
        let tables = Arc::new(parking_lot::Mutex::new(
            if raw_payload_spill_enabled || skip_regular_sink {
                Vec::new()
            } else {
                create_hash_aggregate_tables(
                    &self.spec,
                    ctx.query.allocator(MemoryTag::HashTable),
                    query_hash_table_memory(ctx.query),
                    ctx.query.session.number_of_threads(),
                )?
            },
        ));
        let raw_payload_spill_requested = Arc::new(AtomicBool::new(raw_payload_spill_enabled));
        let state_spill = Arc::new(parking_lot::Mutex::new(None));
        let (
            local_build_reclaimer_name,
            local_payload_spill_reclaimer_name,
            local_state_spill_reclaimer_name,
            query_memory,
        ) = if raw_payload_spill_enabled || skip_regular_sink {
            (None, None, None, None)
        } else {
            let local_id = AggregateLocalBuildCompactionReclaimer::next_local_id();
            let build_name = AggregateLocalBuildCompactionReclaimer::name_for(&handle, local_id);
            ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                AggregateLocalBuildCompactionReclaimer::new(&handle, local_id, Arc::clone(&tables)),
            ));
            let payload_spill_name = if hash_aggregate_payload_spill_supported(&self.spec)
                && query_has_temporary_directory(ctx.query)
            {
                let name = AggregateLocalPayloadSpillReclaimer::name_for(&handle, local_id);
                ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                    AggregateLocalPayloadSpillReclaimer::new(
                        &handle,
                        local_id,
                        Arc::clone(&tables),
                        Arc::clone(&raw_payload_spill_requested),
                    ),
                ));
                Some(name)
            } else {
                None
            };
            let state_spill_name =
                if hash_aggregate_state_spill_supported(&self.spec, &aggregate_objects)
                    && query_has_temporary_directory(ctx.query)
                {
                    let state_width = AggregateStateLayout::new(&aggregate_objects)?.total_size();
                    let state_encoding = hash_aggregate_state_spill_encoding(&aggregate_objects);
                    let name = AggregateLocalStateSpillReclaimer::name_for(&handle, local_id);
                    ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                        AggregateLocalStateSpillReclaimer::new(
                            &handle,
                            local_id,
                            Arc::clone(&tables),
                            Arc::clone(&state_spill),
                            Arc::clone(&raw_payload_spill_requested),
                            ctx.query.session.buffer_pool().clone(),
                            group_types(&self.spec)?,
                            state_width,
                            state_encoding,
                            aggregate_payload_spill_radix_bits(
                                ctx.query.session.number_of_threads(),
                            ),
                            query_hash_table_memory(ctx.query),
                        ),
                    ));
                    Some(name)
                } else {
                    None
                };
            (
                Some(build_name),
                payload_spill_name,
                state_spill_name,
                Some(Arc::clone(&ctx.query.memory)),
            )
        };
        Ok(SinkLocal::HashAggregateBuild(HashAggregateBuildSinkLocal {
            aggregate_objects: Arc::clone(&aggregate_objects),
            projection_executor: (!self.spec.projection_exprs.is_empty()).then(|| {
                ExpressionExecutor::with_expressions_for_session(
                    &self.spec.projection_exprs,
                    ctx.query.session.as_ref(),
                )
            }),
            payload_chunk: (!self.spec.projection_exprs.is_empty())
                .then(|| {
                    Chunk::try_initialize(
                        &self.spec.payload_types,
                        VECTOR_SIZE,
                        ctx.query.allocator(MemoryTag::BaseTable),
                    )
                })
                .transpose()?,
            group_refs: group_payload_refs(&self.spec)?.into_boxed_slice(),
            group_key_encoder: GroupKeyEncoder::try_new(
                &self.spec,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            grouping_sets: normalized_grouping_sets(&self.spec)?
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            addresses: Vector::try_new(
                LogicalType::BigInt,
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            new_groups: SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.query.allocator(MemoryTag::HashTable),
            )?,
            tables,
            local_build_reclaimer_name,
            local_payload_spill_reclaimer_name,
            local_state_spill_reclaimer_name,
            query_memory,
            raw_payload_spill_enabled,
            raw_payload_spill_requested,
            payload_spill: None,
            state_spill,
            ordered_collectors: empty_ordered_collectors_with_memory(
                &self.spec,
                &group_payload_refs(&self.spec)?,
                ctx.query.session.buffer_pool().clone(),
                query_modifier_memory(ctx.query),
            )?,
            modifier_memory: query_modifier_memory(ctx.query),
            distinct: DistinctAggregateState::new(aggregate_objects.len()),
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
        let SinkLocal::HashAggregateBuild(local) = local else {
            return Err(paro_error::internal(
                "hash aggregate sink local state mismatch",
            ));
        };
        let payload = if let Some(executor) = local.projection_executor.as_mut() {
            projected_payload_chunk(
                &self.spec,
                executor,
                &mut local.payload_chunk,
                input,
                ctx.query,
            )?
        } else {
            input
        };
        let groups = local
            .group_key_encoder
            .encode_payload(payload, &local.group_refs)?;
        if local.raw_payload_spill_enabled
            || local.raw_payload_spill_requested.load(Ordering::Acquire)
        {
            append_payload_to_local_spill(ctx, payload, groups, &mut local.payload_spill)?;
            local.activate_raw_payload_spill_if_requested();
            return Ok(SinkPoll::NeedMoreInput);
        }
        if has_aggregate_distinct(&self.spec) {
            collect_distinct_rows(
                &self.spec,
                &local.aggregate_objects,
                payload,
                groups,
                ctx.query.session.number_of_threads(),
                ctx.query.memory.capacity_bytes(),
                &local.modifier_memory,
                &mut local.distinct,
            )?;
        }
        if has_aggregate_ordered(&self.spec) {
            collect_ordered_rows(
                &self.spec,
                &local.aggregate_objects,
                payload,
                &local.group_refs,
                &mut local.ordered_collectors,
            )?;
        }
        if can_skip_regular_aggregate_sink(&self.spec, &local.aggregate_objects) {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let tables_ref = Arc::clone(&local.tables);
        let mut tables = tables_ref.lock();
        if local.raw_payload_spill_enabled
            || local.raw_payload_spill_requested.load(Ordering::Acquire)
        {
            drop(tables);
            append_payload_to_local_spill(ctx, payload, groups, &mut local.payload_spill)?;
            local.activate_raw_payload_spill_if_requested();
            return Ok(SinkPoll::NeedMoreInput);
        }
        update_hash_aggregate_tables(
            &self.spec,
            &local.aggregate_objects,
            payload,
            groups,
            &local.grouping_sets,
            &mut tables,
            &mut local.addresses,
            &mut local.new_groups,
        )?;
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        _ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::HashAggregateBuild(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate sink global state mismatch",
            ));
        };
        let SinkLocal::HashAggregateBuild(local) = local else {
            return Err(paro_error::internal(
                "hash aggregate sink local state mismatch",
            ));
        };
        local.activate_raw_payload_spill_if_requested();
        if local.raw_payload_spill_enabled() {
            global.handle.with_state_mut(|state| {
                let AggregateRuntimeState::Hash(global) = state else {
                    return Err(paro_error::internal(
                        "aggregate handle does not contain hash aggregate state",
                    ));
                };
                if let Some(spill) = local.payload_spill.take() {
                    global.spilled_payloads.push(spill.seal());
                }
                if let Some(spill) = local.state_spill.lock().take() {
                    global.spilled_states.push(spill.seal());
                }
                Ok(())
            })?;
            return Ok(MergePoll::Done);
        }
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            let mut local_tables = local.tables.lock();
            global.distinct.merge_from(&mut local.distinct)?;
            if can_skip_regular_aggregate_sink(&self.spec, &local.aggregate_objects) {
                merge_ordered_collectors(
                    &mut global.ordered_collectors,
                    &mut local.ordered_collectors,
                )?;
                return Ok(());
            }
            if global.tables.len() != local_tables.len() {
                return Err(paro_error::internal(format!(
                    "hash aggregate table count mismatch: global={} local={}",
                    global.tables.len(),
                    local_tables.len()
                )));
            }
            for (global_table, local_table) in global.tables.iter_mut().zip(local_tables.iter_mut())
            {
                global_table.combine(local_table)?;
            }
            if let Some(spill) = local.payload_spill.take() {
                global.spilled_payloads.push(spill.seal());
            }
            if let Some(spill) = local.state_spill.lock().take() {
                global.spilled_states.push(spill.seal());
            }
            merge_ordered_collectors(
                &mut global.ordered_collectors,
                &mut local.ordered_collectors,
            )?;
            Ok(())
        })?;
        local.unregister_local_reclaimers();
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
        let SinkGlobal::HashAggregateBuild(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate sink global state mismatch",
            ));
        };
        Ok(
            match prepare_parallel_distinct_finalize(global.handle.clone(), &self.spec)? {
                Some(group) => FinishWork::Parallel(group),
                None => FinishWork::None,
            },
        )
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::HashAggregateBuild(global) = global else {
            return Err(paro_error::internal(
                "hash aggregate sink global state mismatch",
            ));
        };
        global.handle.with_state_mut(|state| {
            let AggregateRuntimeState::Hash(global) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain hash aggregate state",
                ));
            };
            let aggregate_objects = aggregate_objects(&self.spec)?;
            let group_refs = group_payload_refs(&self.spec)?;
            let grouping_sets = normalized_grouping_sets(&self.spec)?
                .into_iter()
                .map(Vec::into_boxed_slice)
                .collect::<Vec<_>>();
            if global.spilled_payloads.is_empty() {
                replay_spilled_states(ctx, &self.spec, &aggregate_objects, global)?;
            }
            replay_spilled_payloads(
                ctx,
                &self.spec,
                &aggregate_objects,
                &group_refs,
                &grouping_sets,
                global,
            )?;
            if global.spilled_outputs.is_some() {
                return Ok(());
            }
            finalize_distinct_into_tables(
                &self.spec,
                &aggregate_objects,
                &group_refs,
                &grouping_sets,
                &query_modifier_memory(ctx.query),
                &mut global.distinct,
                &mut global.tables,
            )?;
            finalize_ordered_into_hash_tables(
                &self.spec,
                &aggregate_objects,
                &group_refs,
                &grouping_sets,
                &query_modifier_memory(ctx.query),
                &mut global.ordered_collectors,
                &mut global.tables,
            )
        })?;
        global.handle.mark_finalized();
        global.handle.enable_state_reclaim();
        Ok(FinishPoll::Done)
    }
}

fn hash_aggregate_payload_spill_supported(spec: &AggregateSpec) -> bool {
    spec.grouping_key_count > 0
        && spec.grouping_sets.is_empty()
        && spec.grouping_functions.is_empty()
        && !has_aggregate_distinct(spec)
        && !has_aggregate_ordered(spec)
}

fn hash_aggregate_state_spill_supported(
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
) -> bool {
    hash_aggregate_payload_spill_supported(spec)
        && aggregate_state_spill_supported(aggregate_objects)
}

fn hash_aggregate_state_spill_encoding(
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
) -> AggregateStateEncoding {
    if aggregate_state_spill_requires_serialization(aggregate_objects) {
        AggregateStateEncoding::FunctionSerialized
    } else {
        AggregateStateEncoding::RawBytes
    }
}

fn hash_aggregate_external_payload_spill_requested(
    query: &crate::runtime::context::QueryRuntimeContext,
    spec: &AggregateSpec,
) -> bool {
    query.session.limits.force_external && hash_aggregate_payload_spill_supported(spec)
}

fn hash_aggregate_external_payload_spill_enabled(
    query: &crate::runtime::context::QueryRuntimeContext,
    spec: &AggregateSpec,
) -> bool {
    if !hash_aggregate_payload_spill_supported(spec) || !query_has_temporary_directory(query) {
        return false;
    }
    hash_aggregate_external_payload_spill_requested(query, spec)
        || hash_aggregate_preemptive_payload_spill_enabled(query)
}

fn hash_aggregate_local_payload_spill_enabled(
    query: &crate::runtime::context::QueryRuntimeContext,
    spec: &AggregateSpec,
) -> bool {
    hash_aggregate_external_payload_spill_enabled(query, spec)
}

fn hash_aggregate_preemptive_payload_spill_enabled(
    query: &crate::runtime::context::QueryRuntimeContext,
) -> bool {
    let capacity = query.memory.capacity_bytes();
    if capacity >= usize::MAX / 8 {
        return false;
    }
    let threshold = HASH_AGGREGATE_PREEMPTIVE_SPILL_CAP_PER_THREAD
        .saturating_mul(query.session.number_of_threads().max(1));
    capacity <= threshold
}

fn aggregate_payload_spill_radix_bits(parallelism: usize) -> usize {
    parallelism.next_power_of_two().trailing_zeros().clamp(1, 4) as usize
}

fn append_payload_to_local_spill(
    ctx: &mut OperatorCallContext,
    payload: &Chunk,
    groups: &Chunk,
    payload_spill: &mut Option<AggregatePayloadSpillBuffer>,
) -> Result<()> {
    let hashes = hash_group_columns(groups)?;
    if payload_spill.is_none() {
        *payload_spill = Some(AggregatePayloadSpillBuffer::new(
            ctx.query.session.buffer_pool().clone(),
            payload.types(),
            aggregate_payload_spill_radix_bits(ctx.query.session.number_of_threads()),
            query_hash_table_memory(ctx.query),
        )?);
    }
    payload_spill
        .as_mut()
        .expect("aggregate payload spill initialized above")
        .append_payload(payload, &hashes)
}

fn replay_spilled_states(
    ctx: &mut OperatorFinishContext,
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    state: &mut HashAggregateRuntimeState,
) -> Result<()> {
    if state.spilled_states.is_empty() {
        return Ok(());
    }

    if state.tables.is_empty() {
        state.tables = create_hash_aggregate_tables(
            spec,
            ctx.query.allocator(MemoryTag::HashTable),
            query_hash_table_memory(ctx.query),
            ctx.query.session.number_of_threads(),
        )?;
    }
    let spilled_bytes = state
        .spilled_states
        .iter()
        .map(|spilled| spilled.size_in_bytes())
        .sum::<usize>();
    let spilled_states = std::mem::take(&mut state.spilled_states);
    replay_spilled_states_into_tables(ctx, aggregate_objects, &mut state.tables, &spilled_states)?;

    ctx.profiler.record_runtime(
        ctx.operator.index() as u64,
        ExplainRuntimeStats {
            spilled: Some(true),
            spilled_bytes: (spilled_bytes > 0).then_some(spilled_bytes as u64),
            repartition_depth: Some(1),
            ..ExplainRuntimeStats::default()
        },
    );
    Ok(())
}

fn replay_spilled_states_into_tables(
    ctx: &mut OperatorFinishContext,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    target_tables: &mut [AggregateHashTable],
    spilled_states: &[crate::operators::aggregate::payload_spill::AggregateSpilledState],
) -> Result<()> {
    let Some(first_state) = spilled_states.first() else {
        return Ok(());
    };
    let partition_count = first_state.partition_count();
    if partition_count == 0 {
        return Ok(());
    }
    if spilled_states
        .iter()
        .any(|state| state.partition_count() != partition_count)
    {
        return Err(paro_error::internal(
            "aggregate spilled state partition count mismatch",
        ));
    }
    if target_tables.len() != 1 {
        return Err(paro_error::internal(format!(
            "aggregate spilled state replay currently supports one grouping table, got {}",
            target_tables.len()
        )));
    }

    let mut addresses = Vector::try_new(
        LogicalType::BigInt,
        VECTOR_SIZE,
        ctx.query.allocator(MemoryTag::HashTable),
    )?;
    let mut new_groups =
        SelectionVector::try_with_capacity(VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;
    for partition_idx in 0..partition_count {
        for spilled_state in spilled_states {
            spilled_state.replay_partition_state_rows(
                partition_idx,
                ctx.query.allocator(MemoryTag::HashTable),
                |hashes, groups, state_blobs| {
                    combine_spilled_state_batch(
                        ctx,
                        aggregate_objects,
                        spilled_state.state_width(),
                        spilled_state.encoding(),
                        hashes,
                        groups,
                        state_blobs,
                        &mut target_tables[0],
                        &mut addresses,
                        &mut new_groups,
                    )
                },
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn combine_spilled_state_batch(
    ctx: &mut OperatorFinishContext,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    state_width: usize,
    state_encoding: AggregateStateEncoding,
    hashes: &Vector,
    groups: &Chunk,
    state_blobs: &Vector,
    target_table: &mut AggregateHashTable,
    addresses: &mut Vector,
    new_groups: &mut SelectionVector,
) -> Result<()> {
    let row_count = groups.size();
    if row_count == 0 {
        return Ok(());
    }
    if hashes.len() < row_count || state_blobs.len() < row_count {
        return Err(paro_error::internal(format!(
            "aggregate spilled state batch width mismatch: rows={row_count} hashes={} states={}",
            hashes.len(),
            state_blobs.len()
        )));
    }

    target_table.find_or_create_groups(groups, hashes, addresses, new_groups)?;
    if aggregate_objects.is_empty() {
        return Ok(());
    }

    let (mut state_words, source_addresses) = materialize_spilled_state_addresses(
        ctx.query.allocator(MemoryTag::HashTable),
        state_blobs,
        row_count,
        state_width,
        state_encoding,
        aggregate_objects,
    )?;
    let mut aggregate_allocator = ArenaAllocator::new(ctx.query.allocator(MemoryTag::HashTable));
    let mut input_data = AggregateInputData::new(
        None,
        &mut aggregate_allocator,
        AggregateCombineType::PreserveInput,
    );
    let combine_result = combine_states(
        aggregate_objects,
        &mut input_data,
        &source_addresses,
        addresses,
        row_count,
    );
    let destroy_result = destroy_states(
        aggregate_objects,
        &mut input_data,
        &source_addresses,
        row_count,
    );
    state_words.clear();
    combine_result?;
    destroy_result?;
    Ok(())
}

fn materialize_spilled_state_addresses(
    allocator: Arc<dyn paro_common::allocator::Allocator>,
    state_blobs: &Vector,
    row_count: usize,
    state_width: usize,
    state_encoding: AggregateStateEncoding,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
) -> Result<(Vec<u64>, Vector)> {
    let words_per_state = state_width.div_ceil(std::mem::size_of::<u64>());
    let mut state_words = vec![0u64; row_count.saturating_mul(words_per_state)];
    for row_idx in 0..row_count {
        let blob = state_blobs.get_blob(row_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "Spilled aggregate state is NULL or not a blob at row {row_idx}"
            ))
        })?;
        let offset_words = row_idx * words_per_state;
        let dest = unsafe {
            std::slice::from_raw_parts_mut(
                state_words.as_mut_ptr().add(offset_words) as *mut u8,
                state_width,
            )
        };
        match state_encoding {
            AggregateStateEncoding::RawBytes => {
                if blob.len() != state_width {
                    return Err(paro_error::internal(format!(
                        "Spilled aggregate state width mismatch: expected={} actual={}",
                        state_width,
                        blob.len()
                    )));
                }
                dest.copy_from_slice(blob);
            }
            AggregateStateEncoding::FunctionSerialized => {
                let layout = AggregateStateLayout::new(aggregate_objects)?;
                let mut aggregate_allocator = ArenaAllocator::new(allocator.clone());
                let mut input_data = AggregateInputData::new(
                    None,
                    &mut aggregate_allocator,
                    AggregateCombineType::PreserveInput,
                );
                deserialize_aggregate_state_blob(
                    aggregate_objects,
                    &layout,
                    blob,
                    dest.as_mut_ptr(),
                    &mut input_data,
                )?;
            }
        }
    }

    let mut source_addresses = Vector::try_new(LogicalType::BigInt, row_count, allocator)?;
    source_addresses.try_set_count(row_count)?;
    let address_data = unsafe { source_addresses.flat_data_mut::<*mut u8>() };
    for row_idx in 0..row_count {
        let ptr = if state_width == 0 {
            std::ptr::NonNull::<u8>::dangling().as_ptr()
        } else {
            unsafe {
                state_words
                    .as_mut_ptr()
                    .add(row_idx * words_per_state)
                    .cast::<u8>()
            }
        };
        unsafe {
            *address_data.add(row_idx) = ptr;
        }
    }
    Ok((state_words, source_addresses))
}

fn replay_spilled_payloads(
    ctx: &mut OperatorFinishContext,
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    state: &mut HashAggregateRuntimeState,
) -> Result<()> {
    if state.spilled_payloads.is_empty() {
        return Ok(());
    }

    let payload_spilled_bytes = state
        .spilled_payloads
        .iter()
        .map(|payload| payload.size_in_bytes())
        .sum::<usize>();
    let state_spilled_bytes = state
        .spilled_states
        .iter()
        .map(|state| state.size_in_bytes())
        .sum::<usize>();
    let spilled_payloads = std::mem::take(&mut state.spilled_payloads);
    let spilled_states = std::mem::take(&mut state.spilled_states);
    let output_spilled_bytes = spill_payload_partitions_to_outputs(
        ctx,
        spec,
        aggregate_objects,
        group_refs,
        grouping_sets,
        state,
        &spilled_payloads,
        &spilled_states,
    )?;
    let spilled_bytes = payload_spilled_bytes
        .saturating_add(state_spilled_bytes)
        .saturating_add(output_spilled_bytes);

    ctx.profiler.record_runtime(
        ctx.operator.index() as u64,
        ExplainRuntimeStats {
            spilled: Some(true),
            spilled_bytes: (spilled_bytes > 0).then_some(spilled_bytes as u64),
            repartition_depth: Some(1),
            ..ExplainRuntimeStats::default()
        },
    );
    Ok(())
}

fn spill_payload_partitions_to_outputs(
    ctx: &mut OperatorFinishContext,
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    state: &mut HashAggregateRuntimeState,
    spilled_payloads: &[crate::operators::aggregate::payload_spill::AggregateSpilledPayload],
    spilled_states: &[crate::operators::aggregate::payload_spill::AggregateSpilledState],
) -> Result<usize> {
    let Some(first_payload) = spilled_payloads.first() else {
        return Ok(0);
    };
    let partition_count = first_payload.partition_count();
    if partition_count == 0 {
        return Ok(0);
    }
    if spilled_payloads
        .iter()
        .any(|payload| payload.partition_count() != partition_count)
    {
        return Err(paro_error::internal(
            "aggregate spilled payload partition count mismatch",
        ));
    }
    if spilled_states
        .iter()
        .any(|state| state.partition_count() != partition_count)
    {
        return Err(paro_error::internal(
            "aggregate spilled state/payload partition count mismatch",
        ));
    }

    if state.tables.iter().any(|table| table.count() > 0) {
        if !hash_aggregate_state_spill_supported(spec, aggregate_objects) {
            replay_spilled_states_into_tables(
                ctx,
                aggregate_objects,
                &mut state.tables,
                spilled_states,
            )?;
            replay_spilled_payloads_into_tables(
                ctx,
                spec,
                aggregate_objects,
                group_refs,
                grouping_sets,
                &mut state.tables,
                spilled_payloads,
                partition_count,
            )?;
            return Ok(0);
        }
    }

    let in_memory_state_spill = spill_in_memory_tables_to_state_partitions(
        ctx,
        spec,
        aggregate_objects,
        &mut state.tables,
        partition_count,
    )?;
    let mut writers: Option<Vec<Option<RowStoreSpillWriter<AggregateGroupFormat>>>> = None;
    let mut addresses = Vector::try_new(
        LogicalType::BigInt,
        VECTOR_SIZE,
        ctx.query.allocator(MemoryTag::HashTable),
    )?;
    let mut new_groups =
        SelectionVector::try_with_capacity(VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;
    let mut group_key_encoder =
        GroupKeyEncoder::try_new(spec, VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;

    for partition_idx in 0..partition_count {
        let mut partition_tables = create_hash_aggregate_tables(
            spec,
            ctx.query.allocator(MemoryTag::HashTable),
            query_hash_table_memory(ctx.query),
            ctx.query.session.number_of_threads(),
        )?;
        for spilled_state in spilled_states {
            spilled_state.replay_partition_state_rows(
                partition_idx,
                ctx.query.allocator(MemoryTag::HashTable),
                |hashes, groups, state_blobs| {
                    combine_spilled_state_batch(
                        ctx,
                        aggregate_objects,
                        spilled_state.state_width(),
                        spilled_state.encoding(),
                        hashes,
                        groups,
                        state_blobs,
                        &mut partition_tables[0],
                        &mut addresses,
                        &mut new_groups,
                    )
                },
            )?;
        }
        if let Some(spilled_state) = &in_memory_state_spill {
            spilled_state.replay_partition_state_rows(
                partition_idx,
                ctx.query.allocator(MemoryTag::HashTable),
                |hashes, groups, state_blobs| {
                    combine_spilled_state_batch(
                        ctx,
                        aggregate_objects,
                        spilled_state.state_width(),
                        spilled_state.encoding(),
                        hashes,
                        groups,
                        state_blobs,
                        &mut partition_tables[0],
                        &mut addresses,
                        &mut new_groups,
                    )
                },
            )?;
        }
        for spilled_payload in spilled_payloads {
            spilled_payload.replay_partition_payloads(
                partition_idx,
                ctx.query.allocator(MemoryTag::HashTable),
                |payload_batch| {
                    let groups = group_key_encoder.encode_payload(payload_batch, group_refs)?;
                    update_hash_aggregate_tables(
                        spec,
                        aggregate_objects,
                        payload_batch,
                        groups,
                        grouping_sets,
                        &mut partition_tables,
                        &mut addresses,
                        &mut new_groups,
                    )
                },
            )?;
        }
        append_partition_tables_to_output_writers(
            &mut writers,
            &mut partition_tables,
            ctx.query.session.buffer_pool().clone(),
            query_hash_table_memory(ctx.query),
        )?;
    }

    for table in &mut state.tables {
        table.destroy()?;
    }
    state.tables.clear();
    state.spilled_outputs = Some(finish_output_spill_writers(writers.unwrap_or_default())?);
    Ok(state
        .spilled_outputs
        .as_ref()
        .map(|outputs| {
            outputs
                .iter()
                .filter_map(|output| output.as_ref())
                .map(AggregateSpilledOutput::size_in_bytes)
                .sum()
        })
        .unwrap_or(0))
}

fn spill_in_memory_tables_to_state_partitions(
    ctx: &mut OperatorFinishContext,
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    tables: &mut Vec<AggregateHashTable>,
    expected_partition_count: usize,
) -> Result<Option<crate::operators::aggregate::payload_spill::AggregateSpilledState>> {
    if tables.iter().all(|table| table.count() == 0) {
        for table in tables.iter_mut() {
            table.destroy()?;
        }
        tables.clear();
        return Ok(None);
    }
    if tables.len() != 1 {
        return Err(paro_error::internal(format!(
            "aggregate in-memory state spill currently supports one grouping table, got {}",
            tables.len()
        )));
    }

    let state_width = AggregateStateLayout::new(aggregate_objects)?.total_size();
    let mut state_spill = AggregateStateSpillBuffer::new(
        ctx.query.session.buffer_pool().clone(),
        group_types(spec)?,
        state_width,
        hash_aggregate_state_spill_encoding(aggregate_objects),
        aggregate_payload_spill_radix_bits(ctx.query.session.number_of_threads()),
        query_hash_table_memory(ctx.query),
    )?;
    for table in tables.iter() {
        if table.count() > 0 {
            state_spill.append_table(table)?;
        }
    }
    let spilled_state = state_spill.seal();
    if spilled_state.partition_count() != expected_partition_count {
        return Err(paro_error::internal(format!(
            "aggregate in-memory state spill partition count mismatch: expected={} actual={}",
            expected_partition_count,
            spilled_state.partition_count()
        )));
    }

    for table in tables.iter_mut() {
        table.destroy()?;
    }
    tables.clear();
    Ok(Some(spilled_state))
}

fn replay_spilled_payloads_into_tables(
    ctx: &mut OperatorFinishContext,
    spec: &AggregateSpec,
    aggregate_objects: &[crate::operators::aggregate::aggregate_object::AggregateObject],
    group_refs: &[usize],
    grouping_sets: &[Box<[usize]>],
    target_tables: &mut [AggregateHashTable],
    spilled_payloads: &[crate::operators::aggregate::payload_spill::AggregateSpilledPayload],
    partition_count: usize,
) -> Result<()> {
    let mut addresses = Vector::try_new(
        LogicalType::BigInt,
        VECTOR_SIZE,
        ctx.query.allocator(MemoryTag::HashTable),
    )?;
    let mut new_groups =
        SelectionVector::try_with_capacity(VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;
    let mut group_key_encoder =
        GroupKeyEncoder::try_new(spec, VECTOR_SIZE, ctx.query.allocator(MemoryTag::HashTable))?;
    for partition_idx in 0..partition_count {
        let mut partition_tables = create_hash_aggregate_tables(
            spec,
            ctx.query.allocator(MemoryTag::HashTable),
            query_hash_table_memory(ctx.query),
            ctx.query.session.number_of_threads(),
        )?;
        for spilled_payload in spilled_payloads {
            spilled_payload.replay_partition_payloads(
                partition_idx,
                ctx.query.allocator(MemoryTag::HashTable),
                |payload_batch| {
                    let groups = group_key_encoder.encode_payload(payload_batch, group_refs)?;
                    update_hash_aggregate_tables(
                        spec,
                        aggregate_objects,
                        payload_batch,
                        groups,
                        grouping_sets,
                        &mut partition_tables,
                        &mut addresses,
                        &mut new_groups,
                    )
                },
            )?;
        }
        if target_tables.len() != partition_tables.len() {
            return Err(paro_error::internal(format!(
                "aggregate replay table count mismatch: target={} partition={}",
                target_tables.len(),
                partition_tables.len()
            )));
        }
        for (target, partition) in target_tables.iter_mut().zip(partition_tables.iter_mut()) {
            target.combine(partition)?;
        }
    }
    Ok(())
}

fn append_partition_tables_to_output_writers(
    writers: &mut Option<Vec<Option<RowStoreSpillWriter<AggregateGroupFormat>>>>,
    tables: &mut [AggregateHashTable],
    buffer_pool: Arc<BufferPool>,
    memory: MemoryAccountingContext,
) -> Result<()> {
    if writers.is_none() {
        *writers = Some(
            tables
                .iter()
                .map(|table| {
                    let output_types = table.scan_output_types();
                    let format = AggregateGroupFormat::finalized_output(
                        output_types.clone(),
                        output_types.len().saturating_sub(table.aggregate_count()),
                        table.aggregate_count(),
                    );
                    Some(RowStoreSpillWriter::new(
                        Arc::clone(&buffer_pool),
                        format,
                        MemoryTag::HashTable,
                        memory.clone(),
                    ))
                })
                .collect(),
        );
    }
    let writers = writers
        .as_mut()
        .expect("aggregate output spill writers initialized above");
    if writers.len() != tables.len() {
        return Err(paro_error::internal(format!(
            "aggregate output spill writer count mismatch: writers={} tables={}",
            writers.len(),
            tables.len()
        )));
    }
    for (table_idx, table) in tables.iter_mut().enumerate() {
        let output_types = table.scan_output_types();
        let mut chunk = Chunk::try_initialize(&output_types, VECTOR_SIZE, table.allocator())?;
        let mut position = Default::default();
        while table.scan(&mut position, &mut chunk)? {
            let writer = writers
                .get_mut(table_idx)
                .and_then(Option::as_mut)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "missing aggregate output spill writer for table {table_idx}"
                    ))
                })?;
            writer.append_chunk(&chunk)?;
        }
    }
    Ok(())
}

fn finish_output_spill_writers(
    writers: Vec<Option<RowStoreSpillWriter<AggregateGroupFormat>>>,
) -> Result<Vec<Option<AggregateSpilledOutput>>> {
    writers
        .into_iter()
        .map(|writer| {
            let Some(writer) = writer else {
                return Ok(None);
            };
            if writer.count() == 0 {
                return Ok(None);
            }
            let format = writer.format().clone();
            let rows = writer.finish()?;
            Ok(Some(AggregateSpilledOutput::new(format, rows)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
    use paro_common::runtime_value::Value;
    use paro_context::test_support::TestStatementContextBuilder;
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::aggregate::distributive::string_agg::get_string_agg_function;
    use paro_planner::expression::{AggregateExpression, Expression, ReferenceExpression};
    use paro_storage::row::RowSpillReader;

    use crate::explain::profiler::OperatorProfiler;
    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::specs::GroupKeyEncoding;
    use crate::pipeline::graph::PipelineId;
    use crate::runtime::parameter::ParameterBindings;
    use crate::runtime::scratch::TaskMemoryGrants;
    use crate::runtime::{
        OperatorWakeScope, PipelineTaskId, QueryOutputPort, QueryRuntimeContext, RuntimeOperatorId,
        WakeGeneration,
    };
    use crate::thread_context::ThreadContext;

    fn reference(index: usize, ty: LogicalType) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, ty))
    }

    fn grouped_count_spec() -> AggregateSpec {
        AggregateSpec {
            grouping_key_count: 1,
            estimated_input_rows: None,
            projection_exprs: Box::new([]),
            payload_types: Box::new([LogicalType::Integer]),
            groups: Box::new([reference(0, LogicalType::Integer)]),
            group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([Expression::Aggregate(AggregateExpression::new(
                get_count_star_function(),
                vec![],
                LogicalType::BigInt,
            ))]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([])]),
            aggregate_filters: Box::new([None]),
            aggregate_orders: Box::new([Box::new([])]),
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["k".to_string(), "count".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::BigInt]),
        }
    }

    fn grouped_string_agg_spec() -> AggregateSpec {
        let (string_agg, _) = get_string_agg_function()
            .bind(&[LogicalType::Varchar])
            .expect("bind string_agg");
        AggregateSpec {
            grouping_key_count: 1,
            estimated_input_rows: None,
            projection_exprs: Box::new([]),
            payload_types: Box::new([LogicalType::Integer, LogicalType::Varchar]),
            groups: Box::new([reference(0, LogicalType::Integer)]),
            group_key_encodings: Box::new([GroupKeyEncoding::Identity]),
            grouping_sets: Box::new([]),
            aggregates: Box::new([Expression::Aggregate(AggregateExpression::new(
                string_agg,
                vec![reference(1, LogicalType::Varchar)],
                LogicalType::Varchar,
            ))]),
            grouping_functions: Box::new([]),
            aggregate_inputs: Box::new([Box::new([1])]),
            aggregate_filters: Box::new([None]),
            aggregate_orders: Box::new([Box::new([])]),
            having_filter: Box::new([]),
            perfect_hash: None,
            output_names: Box::new(["k".to_string(), "items".to_string()]),
            output_types: Box::new([LogicalType::Integer, LogicalType::Varchar]),
        }
    }

    fn int_payload(values: &[i32], allocator: Arc<dyn paro_common::allocator::Allocator>) -> Chunk {
        let mut payload = Chunk::try_initialize(&[LogicalType::Integer], values.len(), allocator)
            .expect("payload");
        payload.set_cardinality(values.len());
        for (row_idx, value) in values.iter().enumerate() {
            payload
                .column_mut(0)
                .expect("payload column")
                .set_value(row_idx, &Value::Integer(*value));
        }
        payload
    }

    fn string_agg_payload(
        rows: &[(i32, &str)],
        allocator: Arc<dyn paro_common::allocator::Allocator>,
    ) -> Chunk {
        let mut payload = Chunk::try_initialize(
            &[LogicalType::Integer, LogicalType::Varchar],
            rows.len(),
            allocator,
        )
        .expect("payload");
        payload.set_cardinality(rows.len());
        for (row_idx, (key, value)) in rows.iter().enumerate() {
            payload
                .column_mut(0)
                .expect("key column")
                .set_value(row_idx, &Value::Integer(*key));
            payload
                .column_mut(1)
                .expect("value column")
                .set_value(row_idx, &Value::Varchar((*value).to_string()));
        }
        payload
    }

    fn query_context() -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        )
    }

    #[test]
    fn mixed_spilled_payload_and_global_state_writes_bounded_output() {
        let allocator = paro_common::test_utils::test_allocator();
        let query = query_context();
        let thread = ThreadContext::single_threaded();
        let memory = TaskMemoryGrants::detached(allocator.clone());
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(42),
            generation: WakeGeneration(0),
        };
        let mut profiler = OperatorProfiler::disabled();
        let mut ctx = OperatorFinishContext {
            query: &query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            finish_task: None,
            thread: &thread,
            memory: memory.call_scope(),
            cancel: &query.cancellation,
            wake: &wake,
            profiler: &mut profiler,
        };

        let spec = grouped_count_spec();
        let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
        let group_refs = group_payload_refs(&spec).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>();
        let table_memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let mut tables =
            create_hash_aggregate_tables(&spec, allocator.clone(), table_memory.clone(), 1)
                .expect("tables");
        let global_payload = int_payload(&[1, 2, 1], allocator.clone());
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
        let global_groups = build_groups_chunk(&global_payload, &group_refs).expect("groups");
        update_hash_aggregate_tables(
            &spec,
            &aggregate_objects,
            &global_payload,
            &global_groups,
            &grouping_sets,
            &mut tables,
            &mut addresses,
            &mut new_groups,
        )
        .expect("build global table");

        let spilled_payload = int_payload(&[1, 3, 2], allocator.clone());
        let groups = build_groups_chunk(&spilled_payload, &group_refs).expect("groups");
        let hashes = hash_group_columns(&groups).expect("hashes");
        let mut payload_spill = AggregatePayloadSpillBuffer::new(
            query.session.buffer_pool().clone(),
            spilled_payload.types(),
            aggregate_payload_spill_radix_bits(1),
            table_memory,
        )
        .expect("payload spill");
        payload_spill
            .append_payload(&spilled_payload, &hashes)
            .expect("append payload spill");
        let spilled_payloads = vec![payload_spill.seal()];

        let mut state = HashAggregateRuntimeState {
            tables,
            distinct: Default::default(),
            spilled_payloads: Vec::new(),
            spilled_states: Vec::new(),
            spilled_outputs: None,
            ordered_collectors: Vec::new(),
        };
        let spilled_bytes = spill_payload_partitions_to_outputs(
            &mut ctx,
            &spec,
            &aggregate_objects,
            &group_refs,
            &grouping_sets,
            &mut state,
            &spilled_payloads,
            &[],
        )
        .expect("spill mixed replay output");
        assert!(spilled_bytes > 0);
        assert!(state.tables.is_empty());

        let outputs = state.spilled_outputs.take().expect("spilled outputs");
        let mut reader = outputs
            .into_iter()
            .flatten()
            .next()
            .expect("first spilled output")
            .into_reader();
        let mut output =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::BigInt], 8, allocator)
                .expect("output chunk");
        let mut actual = Vec::new();
        loop {
            let count = reader.read_next(&mut output).expect("read output");
            if count == 0 {
                break;
            }
            actual.extend((0..output.size()).map(|row| {
                (
                    output.column(0).unwrap().get_i32(row).unwrap(),
                    output.column(1).unwrap().get_i64(row).unwrap(),
                )
            }));
        }
        actual.sort_unstable();
        assert_eq!(actual, vec![(1, 3), (2, 2), (3, 1)]);
    }

    #[test]
    fn mixed_spilled_payload_and_serialized_string_state_writes_bounded_output() {
        let allocator = paro_common::test_utils::test_allocator();
        let query = query_context();
        let thread = ThreadContext::single_threaded();
        let memory = TaskMemoryGrants::detached(allocator.clone());
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(43),
            generation: WakeGeneration(0),
        };
        let mut profiler = OperatorProfiler::disabled();
        let mut ctx = OperatorFinishContext {
            query: &query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            finish_task: None,
            thread: &thread,
            memory: memory.call_scope(),
            cancel: &query.cancellation,
            wake: &wake,
            profiler: &mut profiler,
        };

        let spec = grouped_string_agg_spec();
        let aggregate_objects = aggregate_objects(&spec).expect("aggregate objects");
        assert!(hash_aggregate_state_spill_supported(
            &spec,
            &aggregate_objects
        ));
        assert_eq!(
            hash_aggregate_state_spill_encoding(&aggregate_objects),
            AggregateStateEncoding::FunctionSerialized
        );

        let group_refs = group_payload_refs(&spec).expect("group refs");
        let grouping_sets = normalized_grouping_sets(&spec)
            .expect("grouping sets")
            .into_iter()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>();
        let table_memory = MemoryAccountingContext::detached(
            MemoryTag::HashTable,
            MemoryAccountingClass::Revocable,
        );
        let mut tables =
            create_hash_aggregate_tables(&spec, allocator.clone(), table_memory.clone(), 1)
                .expect("tables");
        let global_payload =
            string_agg_payload(&[(1, "alpha"), (2, "solo"), (1, "beta")], allocator.clone());
        let mut addresses =
            paro_common::test_utils::test_vector_with_capacity(LogicalType::BigInt, VECTOR_SIZE);
        let mut new_groups = paro_common::test_utils::test_selection_with_capacity(VECTOR_SIZE);
        let global_groups = build_groups_chunk(&global_payload, &group_refs).expect("groups");
        update_hash_aggregate_tables(
            &spec,
            &aggregate_objects,
            &global_payload,
            &global_groups,
            &grouping_sets,
            &mut tables,
            &mut addresses,
            &mut new_groups,
        )
        .expect("build global table");

        let spilled_payload = string_agg_payload(&[(1, "gamma"), (3, "fresh")], allocator.clone());
        let groups = build_groups_chunk(&spilled_payload, &group_refs).expect("groups");
        let hashes = hash_group_columns(&groups).expect("hashes");
        let mut payload_spill = AggregatePayloadSpillBuffer::new(
            query.session.buffer_pool().clone(),
            spilled_payload.types(),
            aggregate_payload_spill_radix_bits(1),
            table_memory,
        )
        .expect("payload spill");
        payload_spill
            .append_payload(&spilled_payload, &hashes)
            .expect("append payload spill");
        let spilled_payloads = vec![payload_spill.seal()];

        let mut state = HashAggregateRuntimeState {
            tables,
            distinct: Default::default(),
            spilled_payloads: Vec::new(),
            spilled_states: Vec::new(),
            spilled_outputs: None,
            ordered_collectors: Vec::new(),
        };
        let spilled_bytes = spill_payload_partitions_to_outputs(
            &mut ctx,
            &spec,
            &aggregate_objects,
            &group_refs,
            &grouping_sets,
            &mut state,
            &spilled_payloads,
            &[],
        )
        .expect("spill mixed replay output");
        assert!(spilled_bytes > 0);
        assert!(state.tables.is_empty());

        let outputs = state.spilled_outputs.take().expect("spilled outputs");
        let mut reader = outputs
            .into_iter()
            .flatten()
            .next()
            .expect("first spilled output")
            .into_reader();
        let mut output =
            Chunk::try_initialize(&[LogicalType::Integer, LogicalType::Varchar], 8, allocator)
                .expect("output chunk");
        let mut actual = Vec::new();
        loop {
            let count = reader.read_next(&mut output).expect("read output");
            if count == 0 {
                break;
            }
            actual.extend((0..output.size()).map(|row| {
                (
                    output.column(0).unwrap().get_i32(row).unwrap(),
                    output
                        .column(1)
                        .unwrap()
                        .get_string(row)
                        .unwrap()
                        .to_string(),
                )
            }));
        }
        actual.sort_unstable_by_key(|(key, _)| *key);
        assert_eq!(
            actual,
            vec![
                (1, "alpha,beta,gamma".to_string()),
                (2, "solo".to_string()),
                (3, "fresh".to_string()),
            ]
        );
    }
}
