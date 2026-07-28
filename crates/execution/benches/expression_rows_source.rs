// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Measures repeated execution of row-oriented expression sources such as
//! SQL `VALUES` lists.

use std::sync::Arc;

use divan::Bencher;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::test_support::TestStatementContextBuilder;
use paro_execution::explain::profiler::OperatorProfiler;
use paro_execution::memory_runtime::QueryMemoryPool;
use paro_execution::physical::properties::PipelineProperties;
use paro_execution::physical::specs::ValuesSpec;
use paro_execution::pipeline::graph::PipelineId;
use paro_execution::pipeline::handles::BreakerHandleCatalog;
use paro_execution::runtime::{
    BreakerHandleRegistry, ExpressionScratchArena, OperatorCallContext, OperatorScratchScope,
    OperatorWakeScope, ParameterBindings, PipelineInitContext, PipelineTaskId, QueryOutputPort,
    QueryRuntimeContext, RuntimeOperatorId, SourceExec, SourceGlobal, SourcePoll, TaskMemoryGrants,
    ValuesSourceExec, WakeGeneration,
};
use paro_execution::thread_context::ThreadContext;
use paro_planner::expression::{ConstantExpression, Expression};

const ROWS: usize = 2_048;
const COLUMNS: usize = 4;

fn main() {
    divan::main();
}

#[divan::bench(sample_count = 20)]
fn scan_constant_rows(bencher: Bencher) {
    let mut state = ExpressionRowsBench::new();
    bencher.counter(ROWS * COLUMNS).bench_local(|| {
        divan::black_box(state.run_once());
    });
}

struct ExpressionRowsBench {
    query: QueryRuntimeContext,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    memory: TaskMemoryGrants,
    scratch: ExpressionScratchArena,
    profiler: OperatorProfiler,
    output: Chunk,
    handles: BreakerHandleRegistry,
    properties: PipelineProperties,
    exec: SourceExec,
    global: SourceGlobal,
}

impl ExpressionRowsBench {
    fn new() -> Self {
        let allocator = paro_common::test_utils::test_allocator();
        let query = QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::discarding(),
        );
        let output_types = vec![LogicalType::BigInt; COLUMNS].into_boxed_slice();
        let expressions = (0..ROWS)
            .map(|row| {
                (0..COLUMNS)
                    .map(|column| {
                        Expression::Constant(ConstantExpression::new(
                            Value::BigInt((row * COLUMNS + column) as i64),
                            LogicalType::BigInt,
                        ))
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let exec = SourceExec::Values(ValuesSourceExec {
            spec: ValuesSpec {
                table_index: 0,
                expressions,
                output_names: (0..COLUMNS)
                    .map(|column| format!("c{column}"))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                output_types: output_types.clone(),
            },
        });
        let handles = BreakerHandleRegistry::from_catalog(&BreakerHandleCatalog::default())
            .expect("empty breaker registry");
        let properties = PipelineProperties::default();
        let mut init_ctx = PipelineInitContext {
            query: &query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            params: query.params.as_ref(),
            handles: &handles,
            properties: &properties,
        };
        let global = exec
            .create_global(&mut init_ctx)
            .expect("VALUES global state");

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
            output: Chunk::try_initialize(&output_types, 2_048, allocator)
                .expect("VALUES output chunk"),
            handles,
            properties,
            exec,
            global,
        }
    }

    fn run_once(&mut self) -> usize {
        let mut init_ctx = PipelineInitContext {
            query: &self.query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            params: self.query.params.as_ref(),
            handles: &self.handles,
            properties: &self.properties,
        };
        let mut local = self
            .exec
            .create_local(&mut init_ctx, &self.global)
            .expect("VALUES local state");
        let mut checksum = 0usize;
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
                .poll_next(&mut ctx, &self.global, &mut local, &mut self.output)
                .expect("VALUES scan")
            {
                SourcePoll::Output => {
                    checksum = checksum.wrapping_add(self.output.size());
                    checksum = checksum.wrapping_add(
                        self.output
                            .column(COLUMNS - 1)
                            .and_then(|column| column.get_i64(self.output.size() - 1))
                            .unwrap_or_default() as usize,
                    );
                }
                SourcePoll::Finished => break,
                SourcePoll::Pending(_) => panic!("VALUES source must not block"),
            }
        }
        checksum
    }
}
