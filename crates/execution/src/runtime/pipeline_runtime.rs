// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Per-execution pipeline runtime state.

use std::sync::Arc;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};

use crate::pipeline::graph::SinkSharing;
use crate::pipeline::handles::BreakerHandleCatalog;
use crate::pipeline::program::PipelineProgram;

use super::breaker::{BreakerHandleRegistry, SharedSinkCoordinator};
use super::context::{PipelineInitContext, QueryRuntimeContext};
use super::parameter::ParameterBindings;
use super::scratch::{PipelineTaskState, TaskMemoryGrants};
use super::state::{SinkGlobal, SourceGlobal, TransformGlobalSlots};

/// Runtime state created for one execution attempt of one immutable
/// `PipelineProgram`.
///
/// The hot path reaches concrete operator state through enum slots owned here.
/// There is no outer `Mutex<Option<Arc<dyn ...>>>`; synchronization is pushed
/// into concrete shared structures such as morsel cursors and hash table
/// partitions.
#[derive(Debug)]
pub struct PipelineRuntime {
    pub program: Arc<PipelineProgram>,
    pub source_global: SourceGlobal,
    pub transform_globals: TransformGlobalSlots,
    pub sink_global: SinkGlobal,
    pub breaker_handles: Arc<BreakerHandleRegistry>,
    pub params: Arc<ParameterBindings>,
    pub shared_sink: Option<Arc<SharedSinkCoordinator>>,
}

impl PipelineRuntime {
    pub fn new(
        program: Arc<PipelineProgram>,
        breaker_handles: Arc<BreakerHandleRegistry>,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
    ) -> Result<Self> {
        Self::with_registry(program, breaker_handles, params, query)
    }

    pub fn from_catalog(
        program: Arc<PipelineProgram>,
        handle_catalog: &BreakerHandleCatalog,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
    ) -> Result<Self> {
        let breaker_handles = Arc::new(BreakerHandleRegistry::from_catalog(handle_catalog)?);
        Self::with_registry(program, breaker_handles, params, query)
    }

    pub fn with_registry(
        program: Arc<PipelineProgram>,
        breaker_handles: Arc<BreakerHandleRegistry>,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
    ) -> Result<Self> {
        Self::with_registry_and_shared_sink(program, breaker_handles, params, query, None)
    }

    pub fn with_registry_and_shared_sink(
        program: Arc<PipelineProgram>,
        breaker_handles: Arc<BreakerHandleRegistry>,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
        shared_sink: Option<Arc<SharedSinkCoordinator>>,
    ) -> Result<Self> {
        if shared_sink.is_some() && matches!(program.sink_sharing, SinkSharing::Exclusive) {
            return Err(paro_common::error::internal(
                "exclusive pipeline cannot bind a shared sink coordinator",
            ));
        }
        let source_global = {
            let mut ctx = init_context(
                query,
                &program,
                &params,
                breaker_handles.as_ref(),
                program.source.operator_id,
            );
            program.source.exec.create_global(&mut ctx)?
        };

        let mut transform_globals = Vec::new();
        transform_globals
            .try_reserve_exact(program.transforms.len())
            .map_err(|error| {
                paro_error::out_of_memory(format!(
                    "failed to allocate {} transform global slots: {error}",
                    program.transforms.len()
                ))
            })?;
        for transform in program.transforms.iter() {
            let mut ctx = init_context(
                query,
                &program,
                &params,
                breaker_handles.as_ref(),
                transform.operator_id,
            );
            transform_globals.push(transform.exec.create_global(&mut ctx)?);
        }

        let sink_global = {
            let mut ctx = init_context(
                query,
                &program,
                &params,
                breaker_handles.as_ref(),
                program.sink.operator_id,
            );
            program.sink.exec.create_global(&mut ctx)?
        };

        Ok(Self {
            program,
            source_global,
            transform_globals: TransformGlobalSlots::new(transform_globals),
            sink_global,
            breaker_handles,
            params,
            shared_sink,
        })
    }

    pub fn create_task_state(
        &self,
        query: &QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<PipelineTaskState> {
        let source = {
            let mut ctx = self.init_context(query, self.program.source.operator_id);
            self.program
                .source
                .exec
                .create_local(&mut ctx, &self.source_global)?
        };

        let transforms = self
            .program
            .transforms
            .iter()
            .enumerate()
            .map(|(idx, transform)| {
                let mut ctx = self.init_context(query, transform.operator_id);
                let global = self
                    .transform_globals
                    .get(idx)
                    .expect("transform global slot must match program transform slot");
                transform.exec.create_local(&mut ctx, global)
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();

        let sink = {
            let mut ctx = self.init_context(query, self.program.sink.operator_id);
            self.program
                .sink
                .exec
                .create_local(&mut ctx, &self.sink_global)?
        };

        let scratch = self.program.scratch.create_scratch(allocator.clone())?;
        let memory = TaskMemoryGrants::query_accounted(query.memory.clone(), allocator.clone())?;

        Ok(PipelineTaskState::new_data(
            source, transforms, sink, memory, scratch,
        ))
    }

    /// Create global-completion state without data-path operator locals or vector scratch.
    pub(crate) fn create_finish_task_state(
        &self,
        query: &QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<PipelineTaskState> {
        PipelineTaskState::new_finish(query.memory.clone(), allocator)
    }

    /// Prove that an empty source can bypass all data-path local state.
    pub(crate) fn can_complete_empty_without_data_task(&self) -> bool {
        self.program.transforms.is_empty() && self.program.sink.exec.empty_local_merge_is_identity()
    }

    fn init_context<'a>(
        &'a self,
        query: &'a QueryRuntimeContext,
        operator: super::ids::RuntimeOperatorId,
    ) -> PipelineInitContext<'a> {
        init_context(
            query,
            &self.program,
            &self.params,
            self.breaker_handles.as_ref(),
            operator,
        )
    }
}

fn init_context<'a>(
    query: &'a QueryRuntimeContext,
    program: &'a PipelineProgram,
    params: &'a Arc<ParameterBindings>,
    handles: &'a BreakerHandleRegistry,
    operator: super::ids::RuntimeOperatorId,
) -> PipelineInitContext<'a> {
    PipelineInitContext {
        query,
        pipeline: program.id,
        operator,
        params: params.as_ref(),
        handles,
        properties: &program.properties,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::types::LogicalType;
    use paro_context::TestStatementContextBuilder;

    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::physical::specs::EmptyResultSpec;
    use crate::pipeline::graph::{
        ClientResultSpec, PipelineId, PipelineSpec, SinkSharing, SinkSpec, SourceSpec,
    };
    use crate::pipeline::program::PipelineProgramBuilder;

    use super::*;
    use crate::runtime::{ParameterBindings, QueryOutputPort, SinkGlobal, SourceGlobal};

    fn empty_program() -> Arc<PipelineProgram> {
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Empty(EmptyResultSpec),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: RowType::new(Vec::new(), Vec::<LogicalType>::new()),
        };
        Arc::new(
            PipelineProgramBuilder::default()
                .build_program(&spec)
                .expect("program build"),
        )
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
    fn pipeline_runtime_initializes_global_and_local_state_without_dyn_containers() {
        let query = query_context();
        let runtime = PipelineRuntime::from_catalog(
            empty_program(),
            &BreakerHandleCatalog::default(),
            query.params.clone(),
            &query,
        )
        .expect("runtime init");

        assert!(matches!(runtime.source_global, SourceGlobal::Empty(_)));
        assert!(matches!(runtime.sink_global, SinkGlobal::ClientResult(_)));
        assert_eq!(runtime.transform_globals.len(), 0);

        let task = runtime
            .create_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("task state");
        assert!(task.pending.is_empty());
        assert_eq!(task.data().scratch.transform_chunks.len(), 0);

        let finish = runtime
            .create_finish_task_state(&query, paro_common::test_utils::test_allocator())
            .expect("finish task state");
        assert!(finish.is_finish_only());
        assert!(finish.pending.is_empty());
    }
}
