// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Measures scheduler overhead for a parallel source with many small morsels.

use std::sync::Arc;

use divan::Bencher;
use paro_common::allocator::Allocator;
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_context::{RuntimeLimits, StatementContext, TestStatementContextBuilder};
use paro_execution::memory_runtime::QueryMemoryPool;
use paro_execution::physical::properties::{Parallelism, PipelineProperties};
use paro_execution::physical::row_type::RowType;
use paro_execution::physical::specs::ChunkScanSpec;
use paro_execution::pipeline::graph::{
    ClientResultSpec, PipelineGraph, PipelineId, PipelineRoot, PipelineSpec, SinkSharing, SinkSpec,
    SourceSpec,
};
use paro_execution::pipeline::handles::BreakerHandleCatalogBuilder;
use paro_execution::pipeline::{PipelineProgramBuilder, PipelineProgramSet};
use paro_execution::runtime::{
    BreakerHandleRegistry, ParameterBindings, PipelineScheduler, QueryOutputPort,
    QueryRuntimeContext,
};

const SOURCE_CHUNKS: usize = 1_024;
const WORKER_THREADS: usize = 4;

fn main() {
    divan::main();
}

#[divan::bench(sample_count = 100)]
fn parallel_small_morsels(bencher: Bencher) {
    let state = SchedulerMorselBench::new();
    bencher.bench_local(|| divan::black_box(state.run_once()));
}

struct SchedulerMorselBench {
    session: Arc<StatementContext>,
    graph: PipelineGraph,
    programs: PipelineProgramSet,
    allocator: Arc<dyn Allocator>,
}

impl SchedulerMorselBench {
    fn new() -> Self {
        let allocator = paro_common::test_utils::test_allocator();
        let chunks = (0..SOURCE_CHUNKS)
            .map(|value| {
                let vector = Vector::try_from_i32(&[value as i32], Arc::clone(&allocator))
                    .expect("benchmark vector");
                Chunk::from_vectors(vec![vector], Arc::clone(&allocator))
            })
            .collect::<Vec<_>>();
        let output_type = LogicalType::Integer;
        let chunk_spec = ChunkScanSpec {
            chunks: Arc::from(chunks.into_boxed_slice()),
            output_names: vec!["v".to_string()].into_boxed_slice(),
            output_types: vec![output_type.clone()].into_boxed_slice(),
        };
        let mut properties = PipelineProperties::default();
        properties.capabilities.parallelism = Parallelism::unbounded();
        let graph = PipelineGraph {
            pipelines: vec![PipelineSpec {
                id: PipelineId::new(0),
                source: SourceSpec::Chunk(chunk_spec),
                transforms: Vec::new(),
                sink: SinkSpec::ClientResult(ClientResultSpec::default()),
                sink_sharing: SinkSharing::Exclusive,
                properties,
                output: RowType::new(vec!["v".to_string()], vec![output_type]),
            }],
            dependencies: Vec::new(),
            handles: BreakerHandleCatalogBuilder::default().finish(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(PipelineId::new(0)),
        };
        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("benchmark pipeline programs");
        let session = TestStatementContextBuilder::minimal()
            .with_limits(RuntimeLimits {
                max_threads: WORKER_THREADS,
                max_memory: 64 * 1024 * 1024,
                use_temporary_directory: false,
                temporary_directory: String::new(),
                max_temp_directory_size: None,
                force_external: false,
                rowset_scan_pushdown: true,
                parallel_scheduler: true,
            })
            .build();
        session
            .scheduler()
            .set_threads(WORKER_THREADS)
            .expect("benchmark worker threads");

        Self {
            session,
            graph,
            programs,
            allocator,
        }
    }

    fn run_once(&self) -> usize {
        let query = QueryRuntimeContext::new(
            Arc::clone(&self.session),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        );
        let handles = Arc::new(
            BreakerHandleRegistry::from_catalog(&self.graph.handles)
                .expect("benchmark breaker handles"),
        );
        PipelineScheduler::run_to_completion_with_registry(
            &self.graph,
            &self.programs,
            handles,
            &query,
            Arc::clone(&self.allocator),
        )
        .expect("benchmark pipeline execution");
        query.output.len()
    }
}
