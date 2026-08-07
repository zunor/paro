// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::memory::{MemoryAccountingClass, MemoryAccountingContext};
use paro_common::types::LogicalType;

use crate::explain::types::ExplainRuntimeStats;
use crate::physical::properties::{MemoryClass, RequiredProperties};
use crate::runtime::breaker::{HandleRef, SortHandle, SortPendingRunsReclaimer};
use crate::runtime::context::{
    OperatorCallContext, OperatorFinishContext, PipelineInitContext, QueryRuntimeContext,
};
use crate::runtime::sink::{
    FinishPoll, FinishTaskGroupRunner, FinishWork, MergePoll, PrepareFinishPoll, SinkPoll,
};
use crate::runtime::state::{BreakerHandleGlobal, SinkGlobal, SinkLocal, SortBuildSinkLocal};
use crate::sorting::sort_descriptor::{
    build_key_chunk_into as build_sort_key_chunk_into,
    build_payload_chunk_into as build_sort_payload_chunk_into, Sort,
};
use crate::sorting::sorted_run::RunBuilder;

use super::finalize::prepare_parallel_sort_finalize;

// ---------------------------------------------------------------------------
// Sort build sink
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SortBuildSinkExec {
    pub handle: HandleRef<SortHandle>,
    pub orders: Box<[paro_planner::binder::ir::OrderByNode]>,
    pub projection_map: Box<[usize]>,
    pub input_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub force_external: bool,
    pub required: RequiredProperties,
}

impl SortBuildSinkExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SinkGlobal> {
        let handle = ctx.handles.get(self.handle)?;
        let force_external = self.force_external || ctx.query.session.limits.force_external;
        if force_external && !query_has_temporary_directory(ctx.query) {
            return Err(paro_error::out_of_memory(
                "force_external sort requires a temporary directory",
            ));
        }
        let sort = Arc::new(Sort::new(
            self.orders.to_vec(),
            self.input_types.to_vec(),
            self.projection_map.to_vec(),
            false,
        )?);
        handle.initialize(sort, self.output_types.clone(), force_external)?;
        if query_has_temporary_directory(ctx.query) {
            ctx.query.memory.register_reclaimer_once_by_name(Arc::new(
                SortPendingRunsReclaimer::for_query(
                    handle.clone(),
                    ctx.query.session.buffer_pool().clone(),
                    ctx.query.memory.clone(),
                ),
            ));
        }
        Ok(SinkGlobal::SortBuild(Arc::new(BreakerHandleGlobal {
            handle,
        })))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        global: &SinkGlobal,
    ) -> Result<SinkLocal> {
        let SinkGlobal::SortBuild(global) = global else {
            return Err(paro_error::internal(
                "sort build sink global state mismatch",
            ));
        };
        Ok(SinkLocal::SortBuild(SortBuildSinkLocal {
            sort: Some(global.handle.sort()?),
            run_builder: None,
            maximum_run_size: sort_run_target_bytes(ctx.query, global.handle.is_external()),
            external: global.handle.is_external(),
            key_chunk: None,
            payload_chunk: None,
        }))
    }

    pub(crate) fn consume(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
        input: &mut Chunk,
    ) -> Result<SinkPoll> {
        ctx.cancel.check()?;
        if input.is_empty() {
            return Ok(SinkPoll::NeedMoreInput);
        }
        let SinkGlobal::SortBuild(global) = global else {
            return Err(paro_error::internal(
                "sort build sink global state mismatch",
            ));
        };
        let SinkLocal::SortBuild(local) = local else {
            return Err(paro_error::internal("sort build sink local state mismatch"));
        };
        let sort = local
            .sort
            .as_ref()
            .ok_or_else(|| paro_error::internal("sort build local missing descriptor"))?;
        if local.run_builder.is_none() {
            local.run_builder = Some(new_sort_run_builder(sort, ctx)?);
            local.maximum_run_size = sort_run_target_bytes(ctx.query, global.handle.is_external());
            local.external = global.handle.is_external();
        }
        let key_chunk =
            build_sort_key_chunk_into(input, sort.key_column_indices(), &mut local.key_chunk)?;
        let payload_chunk = build_sort_payload_chunk_into(
            input,
            sort.input_projection_map(),
            &mut local.payload_chunk,
        )?;
        if let Some(run_builder) = local.run_builder.as_mut() {
            run_builder.sink(&key_chunk, &payload_chunk)?;
            record_sort_runtime(
                ctx,
                run_builder.size_in_bytes(),
                0,
                global.handle.is_external(),
            );
        }
        self.try_finish_run(ctx, global.handle.as_ref(), local)?;
        Ok(SinkPoll::NeedMoreInput)
    }

    pub(crate) fn merge_local(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SinkGlobal,
        local: &mut SinkLocal,
    ) -> Result<MergePoll> {
        let SinkGlobal::SortBuild(global) = global else {
            return Err(paro_error::internal(
                "sort build sink global state mismatch",
            ));
        };
        let SinkLocal::SortBuild(local) = local else {
            return Err(paro_error::internal("sort build sink local state mismatch"));
        };
        if let Some(run_builder) = local.run_builder.take() {
            let peak_memory_bytes = run_builder.size_in_bytes();
            let spilled_bytes = if global.handle.is_external() {
                peak_memory_bytes
            } else {
                0
            };
            global
                .handle
                .add_run(run_builder.finish(global.handle.is_external())?)?;
            record_sort_runtime(
                ctx,
                peak_memory_bytes,
                spilled_bytes,
                global.handle.is_external(),
            );
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
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishWork> {
        let SinkGlobal::SortBuild(global) = global else {
            return Err(paro_error::internal(
                "sort build sink global state mismatch",
            ));
        };
        let handle = global.handle.clone();
        let num_threads = ctx.query.session.number_of_threads();
        if let Some(work) = prepare_parallel_sort_finalize(
            Arc::clone(&handle),
            num_threads,
            ctx.query.memory.available_bytes(),
        )? {
            return Ok(FinishWork::Parallel(work));
        }
        let memory_class = if handle.is_external() {
            MemoryClass::External
        } else {
            MemoryClass::Blocking
        };
        Ok(FinishWork::Parallel(FinishTaskGroupRunner::group(
            "sort_seal",
            memory_class,
            move |_ctx| handle.seal_streaming(),
        )))
    }

    pub(crate) fn finish(
        &self,
        ctx: &mut OperatorFinishContext,
        global: &SinkGlobal,
    ) -> Result<FinishPoll> {
        let SinkGlobal::SortBuild(global) = global else {
            return Err(paro_error::internal(
                "sort build sink global state mismatch",
            ));
        };
        if !global.handle.is_sealed() {
            global.handle.seal_streaming()?;
        }
        ctx.query
            .memory
            .unregister_reclaimer_by_name(&SortPendingRunsReclaimer::name_for(&global.handle));
        Ok(FinishPoll::Done)
    }

    fn try_finish_run(
        &self,
        ctx: &mut OperatorCallContext,
        handle: &SortHandle,
        local: &mut SortBuildSinkLocal,
    ) -> Result<()> {
        let Some(run_builder) = local.run_builder.as_ref() else {
            return Ok(());
        };
        let run_size = run_builder.size_in_bytes();
        if run_size < local.maximum_run_size {
            return Ok(());
        }
        if query_has_temporary_directory(ctx.query) {
            local.external = true;
            handle.mark_external();
        }
        if !local.external {
            return Ok(());
        }
        let sort = local
            .sort
            .as_ref()
            .ok_or_else(|| paro_error::internal("sort build local missing descriptor"))?;
        let spilled_bytes = run_size;
        let run = local
            .run_builder
            .take()
            .expect("sort run builder checked above")
            .finish(true)?;
        handle.add_run(run)?;
        record_sort_runtime(ctx, run_size, spilled_bytes, true);
        local.run_builder = Some(new_sort_run_builder(sort, ctx)?);
        local.maximum_run_size = sort_run_target_bytes(ctx.query, true);
        Ok(())
    }
}

fn record_sort_runtime(
    ctx: &mut OperatorCallContext,
    peak_memory_bytes: usize,
    spilled_bytes: usize,
    external: bool,
) {
    ctx.profiler.record_runtime(
        ctx.operator.index() as u64,
        ExplainRuntimeStats {
            spilled: external.then_some(true),
            peak_memory_bytes: (peak_memory_bytes > 0).then_some(peak_memory_bytes as u64),
            spilled_bytes: (spilled_bytes > 0).then_some(spilled_bytes as u64),
            ..ExplainRuntimeStats::default()
        },
    );
}

// ---------------------------------------------------------------------------
// Sort helpers
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_SORT_RUN_TARGET_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn query_has_temporary_directory(query: &QueryRuntimeContext) -> bool {
    query.session.limits.use_temporary_directory
        && query.session.buffer_pool().has_temporary_directory()
}

pub(crate) fn sort_run_target_bytes(query: &QueryRuntimeContext, force_external: bool) -> usize {
    if force_external {
        return 1;
    }
    if !query_has_temporary_directory(query) {
        return usize::MAX / 4;
    }

    let query_cap = query
        .session
        .query_governance()
        .memory_quota
        .unwrap_or_else(|| query.memory.capacity_bytes())
        .min(query.session.limits.max_memory);
    let target = if query_cap >= usize::MAX / 8 {
        DEFAULT_SORT_RUN_TARGET_BYTES
    } else {
        query_cap / query.session.number_of_threads().max(1)
    };
    target.max(paro_storage::buffer::DEFAULT_BLOCK_SIZE)
}

pub(crate) fn new_sort_run_builder(
    sort: &Sort,
    ctx: &mut OperatorCallContext,
) -> Result<RunBuilder> {
    let grant = ctx
        .memory
        .grant_allocator_for(MemoryTag::OrderBy, MemoryAccountingClass::Revocable);
    Ok(RunBuilder::new_with_memory(
        ctx.query.session.buffer_pool().clone(),
        Arc::clone(sort.key_layout()),
        Arc::clone(sort.payload_layout()),
        Arc::clone(sort.sort_key_encoding()),
        MemoryAccountingContext::from_grant_allocator(&grant),
    ))
}
