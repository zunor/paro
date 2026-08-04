// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Measures the sealed materialized-breaker source path across many chunks.

use std::sync::Arc;

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::test_support::TestStatementContextBuilder;
use paro_execution::explain::profiler::OperatorProfiler;
use paro_execution::memory_runtime::QueryMemoryPool;
use paro_execution::physical::properties::PipelineProperties;
use paro_execution::physical::row_type::RowType;
use paro_execution::pipeline::graph::PipelineId;
use paro_execution::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleKind};
use paro_execution::runtime::{
    BreakerHandleRegistry, ExpressionScratchArena, HandleRef, MaterializedHandle,
    MaterializedSourceExec, OperatorCallContext, OperatorScratchScope, OperatorWakeScope,
    ParameterBindings, PipelineInitContext, PipelineTaskId, QueryOutputPort, QueryRuntimeContext,
    RuntimeOperatorId, SourceExec, SourceGlobal, SourceLocal, SourcePoll, TaskMemoryGrants,
    WakeGeneration,
};
use paro_execution::thread_context::ThreadContext;

const SOURCE_CHUNKS: usize = 16_384;

fn main() {
    divan::main();
}

#[divan::bench(sample_count = 50, sample_size = 1)]
fn scan_sealed_chunks(bencher: Bencher) {
    bencher
        .counter(SOURCE_CHUNKS)
        .with_inputs(MaterializedSourceBench::new)
        .bench_local_refs(|state| divan::black_box(state.run_once()));
}

struct MaterializedSourceBench {
    query: QueryRuntimeContext,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    memory: TaskMemoryGrants,
    scratch: ExpressionScratchArena,
    profiler: OperatorProfiler,
    output: Chunk,
    exec: SourceExec,
    global: SourceGlobal,
    local: SourceLocal,
}

impl MaterializedSourceBench {
    fn new() -> Self {
        let allocator = paro_common::test_utils::test_allocator();
        let query = QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::discarding(),
        );
        let mut catalog = BreakerHandleCatalogBuilder::default();
        let id = catalog.register(
            BreakerHandleKind::Materialized,
            RowType::new(vec!["v".to_string()], vec![LogicalType::Integer]),
            PipelineProperties::default(),
        );
        let registry = BreakerHandleRegistry::from_catalog(&catalog.finish())
            .expect("benchmark breaker registry");
        let handle = registry
            .get(HandleRef::<MaterializedHandle>::new(id))
            .expect("benchmark materialized handle");
        let template = Chunk::from_vectors(
            vec![Vector::try_from_i32(&[7], Arc::clone(&allocator)).expect("benchmark vector")],
            Arc::clone(&allocator),
        );
        let mut chunks = (0..SOURCE_CHUNKS)
            .map(|_| template.clone_referencing_vectors())
            .collect::<Vec<_>>();
        handle
            .append_chunks(&mut chunks)
            .expect("benchmark materialized append");
        handle.seal().expect("benchmark materialized seal");

        let exec = SourceExec::Materialized(MaterializedSourceExec {
            handle: HandleRef::new(id),
        });
        let properties = PipelineProperties::default();
        let mut init_ctx = PipelineInitContext {
            query: &query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            params: query.params.as_ref(),
            handles: &registry,
            properties: &properties,
        };
        let global = exec
            .create_global(&mut init_ctx)
            .expect("benchmark materialized source global");
        let local = exec
            .create_local(&mut init_ctx, &global)
            .expect("benchmark materialized source local");

        Self {
            query,
            thread: ThreadContext::single_threaded(),
            wake: OperatorWakeScope {
                task_id: PipelineTaskId(0),
                generation: WakeGeneration(0),
            },
            memory: TaskMemoryGrants::detached(Arc::clone(&allocator)),
            scratch: ExpressionScratchArena::default(),
            profiler: OperatorProfiler::disabled(),
            output: Chunk::try_initialize(&[LogicalType::Integer], 1, allocator)
                .expect("benchmark output chunk"),
            exec,
            global,
            local,
        }
    }

    fn run_once(&mut self) -> usize {
        let mut rows = 0;
        loop {
            let mut ctx = OperatorCallContext {
                query: &self.query,
                pipeline: PipelineId::new(0),
                operator: RuntimeOperatorId::new(0),
                thread: &self.thread,
                memory: self.memory.call_scope(),
                scratch: OperatorScratchScope::from_expression(&mut self.scratch),
                cancel: &self.query.cancellation,
                wake: &self.wake,
                profiler: &mut self.profiler,
            };
            match self
                .exec
                .poll_next(&mut ctx, &self.global, &mut self.local, &mut self.output)
                .expect("benchmark materialized source poll")
            {
                SourcePoll::Output => rows += self.output.size(),
                SourcePoll::Finished => return rows,
                SourcePoll::Pending(_) => panic!("materialized source should not block"),
            }
        }
    }
}
