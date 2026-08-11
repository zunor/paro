// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime hot-path micro benchmarks for the typed physical-operator runtime.
//!
//! These benches intentionally use real runtime enum slots, scratch arenas, and
//! pending sink behavior. Earlier toy transforms could be optimized away, which
//! made the dispatch numbers misleading.

use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use divan::Bencher;
use paro_common::allocator::{
    allocator_tracking_event_count, reset_allocator_tracking_event_count, Allocator,
};
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_context::test_support::TestStatementContextBuilder;
use paro_execution::explain::profiler::{ExplainProfiler, OperatorProfiler};
use paro_execution::memory_runtime::QueryMemoryPool;
use paro_execution::physical::properties::PipelineProperties;
use paro_execution::physical::row_type::RowType;
use paro_execution::physical::specs::{ChunkScanSpec, FilterSpec, ProjectSpec};
use paro_execution::pipeline::graph::{
    ClientResultSpec, HashJoinBuildSinkSpec, PipelineGraph, PipelineId, PipelineRoot, PipelineSpec,
    SinkSharing, SinkSpec, SourceSpec, TransformSpec,
};
use paro_execution::pipeline::handles::{
    BreakerHandleCatalog, BreakerHandleCatalogBuilder, BreakerHandleKind,
};
use paro_execution::pipeline::program::{PipelineProgram, PipelineProgramBuilder, TransformSlot};
use paro_execution::runtime::{
    BreakerHandleRegistry, ExpressionScratchArena, FinishPoll, FinishTaskPoll, FinishWork,
    MergePoll, NextFinishTask, OperatorCallContext, OperatorFinishContext, OperatorScratchScope,
    OperatorWakeScope, ParameterBindings, PipelineInitContext, PipelineRuntime, PipelineScratch,
    PipelineScratchLayout, PipelineTaskId, PipelineTaskState, PrepareFinishPoll, QueryOutputPort,
    QueryOutputWrite, RuntimeOperatorId, SinkPoll, SourceExec, SourceGlobal, SourceLocal,
    SourcePoll, TaskMemoryGrants, TransformGlobal, TransformLocal, TransformPoll, WakeGeneration,
};
use paro_execution::thread_context::ThreadContext;
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::join::{JoinCondition, JoinType};

const CHAIN_ITERS: usize = 256;
const SCRATCH_ITERS: usize = 4096;
const SOURCE_CHUNKS: usize = 1024;
const PENDING_ITERS: usize = 512;
const JOIN_BUILD_CHUNKS: usize = 64;
const STRUCTURED_SAMPLE_COUNT: usize = 50;

fn main() {
    if let Ok(path) = env::var("PARO_DIVAN_JSON_OUT") {
        write_structured_results(Path::new(&path));
        return;
    }
    divan::main();
}

fn write_structured_results(path: &Path) {
    let sample_count = structured_sample_count();
    let selected = structured_bench_filter();
    let mut benches = Vec::new();

    if structured_bench_selected(&selected, "enum_dispatch_multi_transform_chain") {
        let mut state = TransformChainBench::new();
        benches.push(measure_structured_bench(
            "enum_dispatch_multi_transform_chain",
            CHAIN_ITERS * VECTOR_SIZE,
            sample_count,
            || state.run_once(),
        ));
    }
    if structured_bench_selected(&selected, "scratch_selection_lease_reuse") {
        let allocator = bench_allocator();
        let mut arena = ExpressionScratchArena::default();
        benches.push(measure_structured_bench(
            "scratch_selection_lease_reuse",
            SCRATCH_ITERS,
            sample_count,
            || scratch_selection_lease_reuse_once(&mut arena, &allocator),
        ));
    }
    if structured_bench_selected(&selected, "source_atomic_cursor_morsel_scan") {
        let mut state = SourceCursorBench::new();
        benches.push(measure_structured_bench(
            "source_atomic_cursor_morsel_scan",
            SOURCE_CHUNKS,
            sample_count,
            || state.run_once(),
        ));
    }
    if structured_bench_selected(&selected, "hash_join_build_merge_finish") {
        let mut state = HashJoinBuildFinishBench::new();
        benches.push(measure_structured_bench(
            "hash_join_build_merge_finish",
            JOIN_BUILD_CHUNKS * VECTOR_SIZE,
            sample_count,
            || state.run_once(),
        ));
    }
    if structured_bench_selected(&selected, "client_result_sink_pending_resume") {
        let mut state = SinkPendingBench::new();
        benches.push(measure_structured_bench(
            "client_result_sink_pending_resume",
            PENDING_ITERS * VECTOR_SIZE,
            sample_count,
            || state.run_once(),
        ));
    }

    let payload = serde_json::json!({
        "schema_version": 1,
        "kind": "divan_bench_result",
        "crate": "paro-execution",
        "bench": "operator_runtime_dispatch",
        "sample_count": sample_count,
        "benches": benches,
    });
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("structured Divan result directory should be created");
    }
    let bytes =
        serde_json::to_vec_pretty(&payload).expect("structured Divan result should serialize");
    fs::write(path, bytes).expect("structured Divan result should be written");
}

fn measure_structured_bench<F>(
    id: &str,
    items: usize,
    sample_count: usize,
    mut run_once: F,
) -> serde_json::Value
where
    F: FnMut() -> usize,
{
    let warmup = structured_warmup_count();
    for _ in 0..warmup {
        divan::black_box(run_once());
    }

    let mut samples_ms = Vec::with_capacity(sample_count);
    let mut tracking_events = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        reset_allocator_tracking_event_count();
        let start = Instant::now();
        let checksum = run_once();
        divan::black_box(checksum);
        samples_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        tracking_events.push(allocator_tracking_event_count());
    }

    let chunks = items.div_ceil(VECTOR_SIZE).max(1);
    let tracking_events_per_chunk = tracking_events
        .iter()
        .map(|value| *value as f64 / chunks as f64)
        .collect::<Vec<_>>();
    serde_json::json!({
        "id": id,
        "items": items,
        "samples_ms": samples_ms,
        "audit": {
            "allocator_tracking_event_counts": tracking_events,
            "allocator_tracking_events_per_chunk": tracking_events_per_chunk,
            "chunk_count": chunks,
        },
    })
}

fn structured_sample_count() -> usize {
    env::var("PARO_DIVAN_SAMPLE_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(STRUCTURED_SAMPLE_COUNT)
}

fn structured_warmup_count() -> usize {
    env::var("PARO_DIVAN_WARMUP")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1)
}

fn structured_bench_filter() -> Option<Vec<String>> {
    let items = env::var("PARO_DIVAN_BENCH_FILTER").ok()?;
    let selected = items
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        None
    } else {
        Some(selected)
    }
}

fn structured_bench_selected(selected: &Option<Vec<String>>, id: &str) -> bool {
    match selected {
        Some(items) => items.iter().any(|item| item == id),
        None => true,
    }
}

fn query_context(output: QueryOutputPort) -> paro_execution::runtime::QueryRuntimeContext {
    paro_execution::runtime::QueryRuntimeContext::new(
        TestStatementContextBuilder::minimal().build(),
        Arc::new(ParameterBindings::empty()),
        Arc::new(QueryMemoryPool::unbounded()),
        output,
    )
}

fn runtime_from_spec(
    query: &paro_execution::runtime::QueryRuntimeContext,
    spec: PipelineSpec,
) -> Arc<PipelineRuntime> {
    let program = Arc::new(
        PipelineProgramBuilder::default()
            .build_program(&spec)
            .expect("benchmark pipeline program should build"),
    );
    Arc::new(
        PipelineRuntime::from_catalog(
            program,
            &BreakerHandleCatalog::default(),
            query.params.clone(),
            query,
        )
        .expect("benchmark pipeline runtime should initialize"),
    )
}

fn program_from_graph_pipeline(
    graph: &PipelineGraph,
    pipeline: PipelineId,
) -> (Arc<PipelineProgram>, BreakerHandleCatalog) {
    let programs = PipelineProgramBuilder::default()
        .build_program_set(graph)
        .expect("benchmark pipeline graph should build");
    let program = Arc::clone(
        programs
            .get(pipeline)
            .expect("benchmark pipeline graph should contain the requested pipeline"),
    );
    (program, graph.handles.clone())
}

fn reference(index: usize, ty: LogicalType) -> Expression {
    Expression::Reference(ReferenceExpression::new(index, ty))
}

fn row_type(names: &[&str], types: &[LogicalType]) -> RowType {
    RowType::new(
        names.iter().map(|name| (*name).to_string()).collect(),
        types.to_vec(),
    )
}

fn bench_allocator() -> Arc<dyn Allocator> {
    paro_common::test_utils::test_allocator()
}

fn input_chunk(phase: usize) -> Chunk {
    let allocator = bench_allocator();
    let flags = (0..VECTOR_SIZE)
        .map(|idx| ((idx + phase) & 1) == 0)
        .collect::<Vec<_>>();
    let values = (0..VECTOR_SIZE)
        .map(|idx| idx as i32 + phase as i32)
        .collect::<Vec<_>>();
    Chunk::from_vectors(
        vec![
            paro_common::test_utils::test_bool_vector_with_allocator(&flags, allocator.clone()),
            paro_common::test_utils::test_i32_vector_with_allocator(&values, allocator.clone()),
        ],
        allocator,
    )
}

fn single_i32_chunk(seed: i32) -> Chunk {
    let allocator = bench_allocator();
    let values = (0..VECTOR_SIZE)
        .map(|idx| seed.wrapping_add(idx as i32))
        .collect::<Vec<_>>();
    Chunk::from_vectors(
        vec![paro_common::test_utils::test_i32_vector_with_allocator(
            &values,
            allocator.clone(),
        )],
        allocator,
    )
}

fn join_build_chunk(seed: i32) -> Chunk {
    let allocator = bench_allocator();
    let keys = (0..VECTOR_SIZE)
        .map(|idx| seed.wrapping_add(idx as i32))
        .collect::<Vec<_>>();
    let payloads = (0..VECTOR_SIZE)
        .map(|idx| seed.wrapping_mul(31).wrapping_add(idx as i32))
        .collect::<Vec<_>>();
    Chunk::from_vectors(
        vec![
            paro_common::test_utils::test_i32_vector_with_allocator(&keys, allocator.clone()),
            paro_common::test_utils::test_i32_vector_with_allocator(&payloads, allocator.clone()),
        ],
        allocator,
    )
}

enum TransformInput {
    BenchInput(usize),
    PreviousOutput,
}

#[allow(clippy::too_many_arguments)]
fn call_transform(
    query: &paro_execution::runtime::QueryRuntimeContext,
    pipeline: PipelineId,
    thread: &ThreadContext,
    wake: &OperatorWakeScope,
    profiler: &mut OperatorProfiler,
    memory: &TaskMemoryGrants,
    expression_scratch: &mut ExpressionScratchArena,
    transform: &TransformSlot,
    global: &TransformGlobal,
    local: &mut TransformLocal,
    input: &Chunk,
    output: &mut Chunk,
) {
    let mut ctx = OperatorCallContext {
        query,
        pipeline,
        operator: transform.operator_id,
        thread,
        memory: memory.call_scope(),
        scratch: OperatorScratchScope::from_expression(expression_scratch),
        cancel: &query.cancellation,
        wake,
        profiler,
    };
    let poll = divan::black_box(&transform.exec)
        .transform(&mut ctx, global, local, input, output)
        .expect("transform should run");
    assert!(matches!(poll, TransformPoll::Output));
}

struct TransformChainBench {
    query: paro_execution::runtime::QueryRuntimeContext,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    runtime: Arc<PipelineRuntime>,
    task: PipelineTaskState,
    profiler: OperatorProfiler,
    inputs: [Chunk; 2],
    iteration: usize,
}

impl TransformChainBench {
    fn new() -> Self {
        let query = query_context(QueryOutputPort::unbounded());
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Chunk(ChunkScanSpec {
                chunks: Arc::from(Vec::<Chunk>::new().into_boxed_slice()),
                output_names: vec!["keep".to_string(), "v".to_string()].into_boxed_slice(),
                output_types: vec![LogicalType::Boolean, LogicalType::Integer].into_boxed_slice(),
            }),
            transforms: vec![
                TransformSpec::Filter(FilterSpec {
                    expressions: vec![reference(0, LogicalType::Boolean)].into_boxed_slice(),
                    projection_map: vec![1].into_boxed_slice(),
                }),
                TransformSpec::Project(ProjectSpec {
                    expressions: vec![reference(0, LogicalType::Integer)].into_boxed_slice(),
                    output_names: vec!["v1".to_string()].into_boxed_slice(),
                }),
                TransformSpec::Project(ProjectSpec {
                    expressions: vec![reference(0, LogicalType::Integer)].into_boxed_slice(),
                    output_names: vec!["v2".to_string()].into_boxed_slice(),
                }),
            ],
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type(&["v2"], &[LogicalType::Integer]),
        };
        let runtime = runtime_from_spec(&query, spec);
        let task = runtime
            .create_task_state(&query, bench_allocator())
            .expect("benchmark task state should initialize");
        Self {
            query,
            thread: ThreadContext::single_threaded(),
            wake: OperatorWakeScope {
                task_id: PipelineTaskId(1),
                generation: WakeGeneration(0),
            },
            runtime,
            task,
            profiler: OperatorProfiler::new(ExplainProfiler::new()),
            inputs: [input_chunk(0), input_chunk(1)],
            iteration: 0,
        }
    }

    fn run_once(&mut self) -> usize {
        let mut checksum = 0usize;
        for _ in 0..CHAIN_ITERS {
            let input_idx = self.iteration & 1;
            self.iteration = self.iteration.wrapping_add(1);
            self.run_transform(0, TransformInput::BenchInput(input_idx));
            self.run_transform(1, TransformInput::PreviousOutput);
            self.run_transform(2, TransformInput::PreviousOutput);

            let output = self
                .task
                .scratch
                .transform_chunks
                .last()
                .expect("transform chain has an output chunk");
            checksum = checksum.wrapping_add(output.size());
            if output.size() > 0 {
                checksum = checksum.wrapping_add(
                    output
                        .column(0)
                        .and_then(|column| column.get_i32(output.size() - 1))
                        .unwrap_or_default() as usize,
                );
            }
        }
        divan::black_box(checksum)
    }

    fn run_transform(&mut self, idx: usize, input: TransformInput) {
        let transform = &self.runtime.program.transforms[idx];
        let global = self
            .runtime
            .transform_globals
            .get(idx)
            .expect("transform global should exist");
        let task = &mut self.task;
        let scratch_state = &mut task.scratch;
        match input {
            TransformInput::BenchInput(input_idx) => {
                let input = divan::black_box(&self.inputs[input_idx]);
                let output = &mut scratch_state.transform_chunks[idx];
                call_transform(
                    &self.query,
                    self.runtime.program.id,
                    &self.thread,
                    &self.wake,
                    &mut self.profiler,
                    &task.memory,
                    &mut scratch_state.expression,
                    transform,
                    global,
                    &mut task.transforms[idx],
                    input,
                    output,
                );
            }
            TransformInput::PreviousOutput => {
                let (input, output) = {
                    let (left, right) = scratch_state.transform_chunks.split_at_mut(idx);
                    (&left[idx - 1], &mut right[0])
                };
                call_transform(
                    &self.query,
                    self.runtime.program.id,
                    &self.thread,
                    &self.wake,
                    &mut self.profiler,
                    &task.memory,
                    &mut scratch_state.expression,
                    transform,
                    global,
                    &mut task.transforms[idx],
                    input,
                    output,
                );
            }
        }
    }
}

#[divan::bench(sample_count = 10)]
fn enum_dispatch_multi_transform_chain(bencher: Bencher) {
    let mut state = TransformChainBench::new();
    bencher.counter(CHAIN_ITERS * VECTOR_SIZE).bench_local(|| {
        state.run_once();
    });
}

#[divan::bench(sample_count = 10)]
fn scratch_selection_lease_reuse(bencher: Bencher) {
    let allocator = bench_allocator();
    let mut arena = ExpressionScratchArena::default();
    bencher.counter(SCRATCH_ITERS).bench_local(|| {
        scratch_selection_lease_reuse_once(&mut arena, &allocator);
    });
}

fn scratch_selection_lease_reuse_once(
    arena: &mut ExpressionScratchArena,
    allocator: &Arc<dyn Allocator>,
) -> usize {
    let mut checksum = 0usize;
    for idx in 0..SCRATCH_ITERS {
        let count = divan::black_box(VECTOR_SIZE - (idx & 7));
        let mut lease = arena.lease();
        let generation = lease.generation();
        let selection = lease
            .selection(count, Arc::clone(allocator))
            .expect("selection scratch should lease");
        checksum = checksum
            .wrapping_add(selection.len())
            .wrapping_add(selection.capacity())
            .wrapping_add(generation as usize);
    }
    divan::black_box(checksum)
}

struct SourceCursorBench {
    query: paro_execution::runtime::QueryRuntimeContext,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    memory: TaskMemoryGrants,
    scratch: PipelineScratch,
    profiler: OperatorProfiler,
    exec: SourceExec,
    global: SourceGlobal,
    local: SourceLocal,
}

impl SourceCursorBench {
    fn new() -> Self {
        let query = query_context(QueryOutputPort::unbounded());
        let template = single_i32_chunk(7);
        let chunks = (0..SOURCE_CHUNKS)
            .map(|_| template.clone_referencing_vectors())
            .collect::<Vec<_>>();
        let scratch = PipelineScratchLayout::new(
            paro_execution::runtime::ChunkLayout::new(vec![LogicalType::Integer], VECTOR_SIZE),
            Vec::new(),
            1,
        )
        .create_scratch(bench_allocator())
        .expect("source scratch should initialize");
        let handles = BreakerHandleRegistry::from_catalog(&BreakerHandleCatalog::default())
            .expect("empty handle registry should initialize");
        let properties = PipelineProperties::default();
        let exec = SourceExec::Chunk(paro_execution::runtime::ChunkSourceExec {
            spec: ChunkScanSpec {
                chunks: Arc::from(chunks.into_boxed_slice()),
                output_names: vec!["v".to_string()].into_boxed_slice(),
                output_types: vec![LogicalType::Integer].into_boxed_slice(),
            },
        });
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
            .expect("source global should initialize");
        let mut init_ctx = PipelineInitContext {
            query: &query,
            pipeline: PipelineId::new(0),
            operator: RuntimeOperatorId::new(0),
            params: query.params.as_ref(),
            handles: &handles,
            properties: &properties,
        };
        let local = exec
            .create_local(&mut init_ctx, &global)
            .expect("source local should initialize");
        Self {
            query,
            thread: ThreadContext::single_threaded(),
            wake: OperatorWakeScope {
                task_id: PipelineTaskId(2),
                generation: WakeGeneration(0),
            },
            memory: TaskMemoryGrants::detached(bench_allocator()),
            scratch,
            profiler: OperatorProfiler::new(ExplainProfiler::new()),
            exec,
            global,
            local,
        }
    }

    fn run_once(&mut self) -> usize {
        self.local
            .chunk_mut()
            .expect("source local should be chunk scan")
            .next_chunk = 0;

        let mut rows = 0usize;
        loop {
            let mut ctx = OperatorCallContext {
                query: &self.query,
                pipeline: PipelineId::new(0),
                operator: RuntimeOperatorId::new(0),
                thread: &self.thread,
                memory: self.memory.call_scope(),
                scratch: OperatorScratchScope::from_expression(&mut self.scratch.expression),
                cancel: &self.query.cancellation,
                wake: &self.wake,
                profiler: &mut self.profiler,
            };
            match divan::black_box(&self.exec)
                .poll_next(
                    &mut ctx,
                    &self.global,
                    &mut self.local,
                    &mut self.scratch.source_chunk,
                )
                .expect("source poll should run")
            {
                SourcePoll::Output => {
                    rows = rows.wrapping_add(self.scratch.source_chunk.size());
                }
                SourcePoll::Finished => break,
                SourcePoll::Pending(_) => panic!("chunk source should not block"),
            }
        }
        divan::black_box(rows)
    }
}

#[divan::bench(sample_count = 10)]
fn source_atomic_cursor_morsel_scan(bencher: Bencher) {
    let mut state = SourceCursorBench::new();
    bencher.counter(SOURCE_CHUNKS).bench_local(|| {
        state.run_once();
    });
}

struct HashJoinBuildFinishBench {
    query: paro_execution::runtime::QueryRuntimeContext,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    program: Arc<PipelineProgram>,
    handles: BreakerHandleCatalog,
    inputs: Box<[Chunk]>,
    profiler: OperatorProfiler,
}

impl HashJoinBuildFinishBench {
    fn new() -> Self {
        let query = query_context(QueryOutputPort::unbounded());
        let build_id = PipelineId::new(0);
        let build_row_type = row_type(
            &["k", "payload"],
            &[LogicalType::Integer, LogicalType::Integer],
        );
        let mut handles = BreakerHandleCatalogBuilder::default();
        let handle = handles.register(
            BreakerHandleKind::HashJoinBuild,
            build_row_type.clone(),
            PipelineProperties::default(),
        );
        handles
            .set_producer(handle, build_id)
            .expect("hash join build producer should register");
        let graph = PipelineGraph {
            pipelines: vec![PipelineSpec {
                id: build_id,
                source: SourceSpec::Chunk(ChunkScanSpec {
                    chunks: Arc::from(Vec::<Chunk>::new().into_boxed_slice()),
                    output_names: vec!["k".to_string(), "payload".to_string()].into_boxed_slice(),
                    output_types: vec![LogicalType::Integer, LogicalType::Integer]
                        .into_boxed_slice(),
                }),
                transforms: Vec::new(),
                sink: SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                    handle,
                    join_type: JoinType::Inner,
                    conditions: vec![JoinCondition::equality(
                        reference(0, LogicalType::Integer),
                        reference(0, LogicalType::Integer),
                    )]
                    .into_boxed_slice(),
                    build_projection: vec![1].into_boxed_slice(),
                    build_payload_types: vec![LogicalType::Integer].into_boxed_slice(),
                    build_output_count: 1,
                    required: Default::default(),
                    force_external: false,
                }),
                sink_sharing: SinkSharing::Exclusive,
                properties: PipelineProperties::default(),
                output: build_row_type,
            }],
            dependencies: Vec::new(),
            handles: handles.finish(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(build_id),
        };
        let (program, handles) = program_from_graph_pipeline(&graph, build_id);
        Self {
            query,
            thread: ThreadContext::single_threaded(),
            wake: OperatorWakeScope {
                task_id: PipelineTaskId(4),
                generation: WakeGeneration(0),
            },
            program,
            handles,
            inputs: (0..JOIN_BUILD_CHUNKS)
                .map(|idx| join_build_chunk((idx * VECTOR_SIZE) as i32))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            profiler: OperatorProfiler::new(ExplainProfiler::new()),
        }
    }

    fn runtime_for_run(&self) -> PipelineRuntime {
        PipelineRuntime::from_catalog(
            Arc::clone(&self.program),
            &self.handles,
            self.query.params.clone(),
            &self.query,
        )
        .expect("hash join build runtime should initialize")
    }

    fn call_context<'a>(
        &'a mut self,
        runtime: &'a PipelineRuntime,
        memory: &'a TaskMemoryGrants,
        expression_scratch: &'a mut ExpressionScratchArena,
    ) -> OperatorCallContext<'a> {
        OperatorCallContext {
            query: &self.query,
            pipeline: runtime.program.id,
            operator: runtime.program.sink.operator_id,
            thread: &self.thread,
            memory: memory.call_scope(),
            scratch: OperatorScratchScope::from_expression(expression_scratch),
            cancel: &self.query.cancellation,
            wake: &self.wake,
            profiler: &mut self.profiler,
        }
    }

    fn finish_context<'a>(
        &'a mut self,
        runtime: &'a PipelineRuntime,
        memory: &'a TaskMemoryGrants,
    ) -> OperatorFinishContext<'a> {
        OperatorFinishContext {
            query: &self.query,
            pipeline: runtime.program.id,
            operator: runtime.program.sink.operator_id,
            finish_task: None,
            thread: &self.thread,
            memory: memory.call_scope(),
            cancel: &self.query.cancellation,
            wake: &self.wake,
            profiler: &mut self.profiler,
        }
    }

    fn run_once(&mut self) -> usize {
        let runtime = self.runtime_for_run();
        let mut task = runtime
            .create_task_state(&self.query, bench_allocator())
            .expect("hash join build task should initialize");
        let mut input = Chunk::try_init_empty(
            &[LogicalType::Integer, LogicalType::Integer],
            bench_allocator(),
        )
        .expect("input chunk should allocate");
        let mut checksum = 0usize;

        for idx in 0..self.inputs.len() {
            let template = &self.inputs[idx];
            input.reference(divan::black_box(template));
            checksum = checksum.wrapping_add(input.size());
            let mut ctx = self.call_context(&runtime, &task.memory, &mut task.scratch.expression);
            let poll = divan::black_box(&runtime.program.sink.exec)
                .consume(&mut ctx, &runtime.sink_global, &mut task.sink, &mut input)
                .expect("hash join build consume should run");
            assert!(matches!(poll, SinkPoll::NeedMoreInput));
        }

        {
            let mut ctx = self.call_context(&runtime, &task.memory, &mut task.scratch.expression);
            let poll = divan::black_box(&runtime.program.sink.exec)
                .merge_local(&mut ctx, &runtime.sink_global, &mut task.sink)
                .expect("hash join build merge should run");
            assert!(matches!(poll, MergePoll::Done));
        }
        {
            let mut ctx = self.finish_context(&runtime, &task.memory);
            let poll = divan::black_box(&runtime.program.sink.exec)
                .prepare_finish(&mut ctx, &runtime.sink_global)
                .expect("hash join build prepare_finish should run");
            assert!(matches!(poll, PrepareFinishPoll::Done));
        }
        {
            let work = {
                let mut ctx = self.finish_context(&runtime, &task.memory);
                divan::black_box(&runtime.program.sink.exec)
                    .finish_work(&mut ctx, &runtime.sink_global)
                    .expect("hash join build finish_work should run")
            };
            self.drive_finish_work(&runtime, &task.memory, work);
        }
        {
            let mut ctx = self.finish_context(&runtime, &task.memory);
            let poll = divan::black_box(&runtime.program.sink.exec)
                .finish(&mut ctx, &runtime.sink_global)
                .expect("hash join build finish should run");
            assert!(matches!(poll, FinishPoll::Done));
        }

        divan::black_box(checksum)
    }

    fn drive_finish_work(
        &mut self,
        runtime: &PipelineRuntime,
        memory: &TaskMemoryGrants,
        work: FinishWork,
    ) {
        let FinishWork::Parallel(group) = work else {
            return;
        };
        loop {
            let next = {
                let mut ctx = self.finish_context(runtime, memory);
                group
                    .driver
                    .next_task(&mut ctx)
                    .expect("finish task discovery should run")
            };
            match next {
                NextFinishTask::Task(task_id) => {
                    let poll = {
                        let mut ctx = self.finish_context(runtime, memory);
                        group
                            .driver
                            .run_task(task_id, &mut ctx)
                            .expect("finish task should run")
                    };
                    match poll {
                        FinishTaskPoll::Done => {}
                        FinishTaskPoll::Pending(_) => {
                            panic!("hash join bench finish task should not block")
                        }
                    }
                }
                NextFinishTask::Drained => break,
                NextFinishTask::Pending(_) => {
                    panic!("hash join bench finish task discovery should not block")
                }
            }
        }
    }
}

#[divan::bench(sample_count = 10)]
fn hash_join_build_merge_finish(bencher: Bencher) {
    let mut state = HashJoinBuildFinishBench::new();
    bencher
        .counter(JOIN_BUILD_CHUNKS * VECTOR_SIZE)
        .bench_local(|| {
            state.run_once();
        });
}

struct SinkPendingBench {
    query: paro_execution::runtime::QueryRuntimeContext,
    thread: ThreadContext,
    wake: OperatorWakeScope,
    runtime: Arc<PipelineRuntime>,
    task: PipelineTaskState,
    input: Chunk,
    template: Chunk,
    profiler: OperatorProfiler,
}

impl SinkPendingBench {
    fn new() -> Self {
        let output = QueryOutputPort::bounded(1);
        let query = query_context(output);
        let spec = PipelineSpec {
            id: PipelineId::new(0),
            source: SourceSpec::Chunk(ChunkScanSpec {
                chunks: Arc::from(Vec::<Chunk>::new().into_boxed_slice()),
                output_names: vec!["v".to_string()].into_boxed_slice(),
                output_types: vec![LogicalType::Integer].into_boxed_slice(),
            }),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: row_type(&["v"], &[LogicalType::Integer]),
        };
        let runtime = runtime_from_spec(&query, spec);
        let task = runtime
            .create_task_state(&query, bench_allocator())
            .expect("sink benchmark task state should initialize");
        Self {
            query,
            thread: ThreadContext::single_threaded(),
            wake: OperatorWakeScope {
                task_id: PipelineTaskId(3),
                generation: WakeGeneration(0),
            },
            runtime,
            task,
            input: Chunk::try_init_empty(&[LogicalType::Integer], bench_allocator())
                .expect("input chunk should allocate"),
            template: single_i32_chunk(19),
            profiler: OperatorProfiler::new(ExplainProfiler::new()),
        }
    }

    fn consume_once(&mut self) -> SinkPoll {
        let task = &mut self.task;
        let mut ctx = OperatorCallContext {
            query: &self.query,
            pipeline: self.runtime.program.id,
            operator: self.runtime.program.sink.operator_id,
            thread: &self.thread,
            memory: task.memory.call_scope(),
            scratch: OperatorScratchScope::from_expression(&mut task.scratch.expression),
            cancel: &self.query.cancellation,
            wake: &self.wake,
            profiler: &mut self.profiler,
        };
        divan::black_box(&self.runtime.program.sink.exec)
            .consume(
                &mut ctx,
                &self.runtime.sink_global,
                &mut task.sink,
                &mut self.input,
            )
            .expect("sink consume should run")
    }

    fn run_once(&mut self) -> usize {
        let mut checksum = 0usize;
        for _ in 0..PENDING_ITERS {
            while self.query.output.pop_front().is_some() {}

            let filler = self.template.clone_referencing_vectors();
            match self.query.output.try_push(filler) {
                QueryOutputWrite::Written => {}
                QueryOutputWrite::Blocked(_) => panic!("output should accept filler chunk"),
            }

            self.input = divan::black_box(&self.template).clone_referencing_vectors();
            let blocked = self.consume_once();
            assert!(matches!(blocked, SinkPoll::Pending(_)));
            checksum = checksum.wrapping_add(self.input.size());

            let drained = self
                .query
                .output
                .pop_front()
                .expect("filler chunk should drain");
            checksum = checksum.wrapping_add(drained.size());

            let resumed = self.consume_once();
            assert!(matches!(resumed, SinkPoll::NeedMoreInput));
            let written = self
                .query
                .output
                .pop_front()
                .expect("resumed chunk should be written");
            checksum = checksum.wrapping_add(written.size());
        }
        divan::black_box(checksum)
    }
}

#[divan::bench(sample_count = 10)]
fn client_result_sink_pending_resume(bencher: Bencher) {
    let mut state = SinkPendingBench::new();
    bencher
        .counter(PENDING_ITERS * VECTOR_SIZE)
        .bench_local(|| {
            state.run_once();
        });
}
