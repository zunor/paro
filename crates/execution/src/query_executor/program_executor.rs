// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fetch-driven driver for typed runtime programs.
//!
//! Query pipelines can now be returned to pgwire before root output is fully
//! drained. The result handler advances this driver on fetch, while bounded
//! `QueryOutputPort` backpressure parks the active pipeline task and resumes it
//! after the client consumes a chunk.

use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_context::StatementContext;
use paro_planner::operator::ExplainSpec;

use crate::explain::analyze_render::render_explain_analyze;
use crate::explain::profiler::ExplainProfiler;
use crate::memory_runtime::QueryMemoryPool;
use crate::pipeline::graph::{
    ControlRegion, ControlRegionId, PipelineGraph, PipelineId, PipelineRoot, PipelineSubgraphRoot,
    SinkSharing,
};
use crate::pipeline::{PipelineProgramSet, StatementProgram, UtilityProgram};
use crate::runtime::scheduler::run_bound_pipeline_runtime;
use crate::runtime::{
    BreakerHandleRegistry, ControlRegionRuntime, ControlRegionRuntimeSet, ParameterBindings,
    PipelineDependencyGates, PipelineRuntime, PipelineScheduler, QueryOutputPort,
    QueryRuntimeContext, RecursiveCteGateAction, SharedSinkRuntimeSet, UtilityContext,
};

use super::cleanup::{
    cleanup_handles, cleanup_reason_for_error, merge_execution_and_cleanup_result,
};
use super::explain_output::push_explain_lines;
use super::pipeline_driver::{supports_fetch_driven_pipeline, PipelineExecutionDriver};

/// Shared context for control-region pipeline execution, reducing parameter
/// count in recursive dispatch functions.
struct GraphExecutionContext<'a> {
    graph: &'a PipelineGraph,
    programs: &'a PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    shared_sinks: &'a SharedSinkRuntimeSet,
    query: &'a QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
}

const ROOT_OUTPUT_QUEUE_CHUNKS: usize = 2;

pub struct ProgramExecution {
    pub query: QueryRuntimeContext,
    pub driver: Option<PipelineExecutionDriver>,
    pub background: Option<BackgroundExecutionDriver>,
}

pub fn execute_program(
    session: Arc<StatementContext>,
    program: &StatementProgram,
    params: Arc<ParameterBindings>,
    memory: Arc<QueryMemoryPool>,
    allocator: Arc<dyn Allocator>,
) -> Result<ProgramExecution> {
    let mut execution = start_program_with_output(
        session,
        program,
        params,
        memory,
        allocator,
        QueryOutputPort::unbounded(),
        QueryOutputPort::unbounded(),
        false,
    )?;
    if let Some(driver) = execution.driver.as_mut() {
        driver.run_to_completion(&execution.query)?;
    }
    if let Some(background) = execution.background.as_mut() {
        background.join()?;
    }
    execution.driver = None;
    execution.background = None;
    Ok(execution)
}

pub fn start_program(
    session: Arc<StatementContext>,
    program: &StatementProgram,
    params: Arc<ParameterBindings>,
    memory: Arc<QueryMemoryPool>,
    allocator: Arc<dyn Allocator>,
) -> Result<ProgramExecution> {
    start_program_with_output(
        session,
        program,
        params,
        memory,
        allocator,
        QueryOutputPort::bounded(ROOT_OUTPUT_QUEUE_CHUNKS),
        QueryOutputPort::unbounded(),
        true,
    )
}

fn start_program_with_output(
    session: Arc<StatementContext>,
    program: &StatementProgram,
    params: Arc<ParameterBindings>,
    memory: Arc<QueryMemoryPool>,
    allocator: Arc<dyn Allocator>,
    streaming_output: QueryOutputPort,
    completed_output: QueryOutputPort,
    fetch_driven: bool,
) -> Result<ProgramExecution> {
    let requires_background_input = session.input.requires_background_execution();
    let output = match program {
        StatementProgram::Pipeline { .. } if fetch_driven && requires_background_input => {
            QueryOutputPort::with_blocking_writes(&streaming_output)
        }
        StatementProgram::Pipeline { graph, .. }
            if fetch_driven
                && PipelineScheduler::should_use_parallel_scheduler_for_session(
                    graph,
                    session.as_ref(),
                ) =>
        {
            QueryOutputPort::with_blocking_writes(&streaming_output)
        }
        StatementProgram::Pipeline { graph, .. }
            if fetch_driven && supports_fetch_driven_pipeline(graph.as_ref()) =>
        {
            streaming_output
        }
        StatementProgram::Pipeline { graph, .. }
            if fetch_driven && has_control_region_execution(graph.as_ref()) =>
        {
            QueryOutputPort::with_blocking_writes(&streaming_output)
        }
        _ => completed_output,
    };
    let query = QueryRuntimeContext::new(session, params, memory, output);
    match program {
        StatementProgram::Utility(utility) => run_utility(utility, &query)?,
        StatementProgram::ExplainAnalyze { target, spec } => {
            run_explain_analyze(target, *spec, &query, allocator)?
        }
        StatementProgram::Pipeline {
            graph, programs, ..
        } if fetch_driven && requires_background_input => {
            let background = BackgroundExecutionDriver::spawn(
                graph.clone(),
                programs.clone(),
                query.clone(),
                allocator,
            )?;
            return Ok(ProgramExecution {
                query,
                driver: None,
                background: Some(background),
            });
        }
        StatementProgram::Pipeline {
            graph, programs, ..
        } if fetch_driven && PipelineScheduler::should_use_parallel_scheduler(graph, &query) => {
            let background = BackgroundExecutionDriver::spawn(
                graph.clone(),
                programs.clone(),
                query.clone(),
                allocator,
            )?;
            return Ok(ProgramExecution {
                query,
                driver: None,
                background: Some(background),
            });
        }
        StatementProgram::Pipeline {
            graph, programs, ..
        } if fetch_driven && supports_fetch_driven_pipeline(graph.as_ref()) => {
            let driver =
                PipelineExecutionDriver::new(graph.clone(), programs.clone(), &query, allocator)?;
            return Ok(ProgramExecution {
                query,
                driver: Some(driver),
                background: None,
            });
        }
        StatementProgram::Pipeline {
            graph, programs, ..
        } if fetch_driven && has_control_region_execution(graph.as_ref()) => {
            let background = BackgroundExecutionDriver::spawn(
                graph.clone(),
                programs.clone(),
                query.clone(),
                allocator,
            )?;
            return Ok(ProgramExecution {
                query,
                driver: None,
                background: Some(background),
            });
        }
        StatementProgram::Pipeline {
            graph, programs, ..
        } => run_pipeline_graph(graph.as_ref(), programs, &query, allocator)?,
    }
    Ok(ProgramExecution {
        query,
        driver: None,
        background: None,
    })
}

fn has_control_region_execution(graph: &PipelineGraph) -> bool {
    matches!(graph.root, PipelineRoot::ControlRegion(_)) || !graph.control_regions.is_empty()
}

pub struct BackgroundExecutionDriver {
    state: Arc<BackgroundExecutionState>,
    output: QueryOutputPort,
    handle: Option<JoinHandle<()>>,
}

struct BackgroundExecutionState {
    result: Mutex<Option<Result<()>>>,
    cv: Condvar,
}

impl BackgroundExecutionDriver {
    fn spawn(
        graph: Arc<PipelineGraph>,
        programs: PipelineProgramSet,
        query: QueryRuntimeContext,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Self> {
        let state = Arc::new(BackgroundExecutionState {
            result: Mutex::new(None),
            cv: Condvar::new(),
        });
        let worker_state = state.clone();
        let worker_query = query.clone();
        let handle = thread::Builder::new()
            .name("paro-control-region-driver".to_string())
            .spawn(move || {
                let result = match panic::catch_unwind(AssertUnwindSafe(|| {
                    run_pipeline_graph(graph.as_ref(), &programs, &worker_query, allocator)
                })) {
                    Ok(result) => result,
                    Err(_) => Err(paro_error::internal("background query driver panicked")),
                };
                worker_state.finish(result);
                worker_query.output.close();
            })
            .map_err(|error| {
                paro_error::internal(format!("failed to spawn background query driver: {error}"))
            })?;
        Ok(Self {
            state,
            output: query.output.clone(),
            handle: Some(handle),
        })
    }

    pub fn is_finished(&self) -> bool {
        self.state
            .result
            .lock()
            .expect("background execution result lock poisoned")
            .is_some()
    }

    pub fn join(&mut self) -> Result<()> {
        self.output.close();
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| paro_error::internal("background query driver panicked"))?;
        }
        self.state.take_result().unwrap_or(Ok(()))
    }
}

impl Drop for BackgroundExecutionDriver {
    fn drop(&mut self) {
        self.output.close();
    }
}

impl BackgroundExecutionState {
    fn finish(&self, result: Result<()>) {
        let mut slot = self
            .result
            .lock()
            .expect("background execution result lock poisoned");
        *slot = Some(result);
        self.cv.notify_all();
    }

    fn take_result(&self) -> Option<Result<()>> {
        self.result
            .lock()
            .expect("background execution result lock poisoned")
            .take()
    }
}

#[cfg(test)]
pub(super) fn start_program_with_output_for_test(
    session: Arc<StatementContext>,
    program: &StatementProgram,
    params: Arc<ParameterBindings>,
    memory: Arc<QueryMemoryPool>,
    allocator: Arc<dyn Allocator>,
    streaming_output: QueryOutputPort,
    completed_output: QueryOutputPort,
    fetch_driven: bool,
) -> Result<ProgramExecution> {
    start_program_with_output(
        session,
        program,
        params,
        memory,
        allocator,
        streaming_output,
        completed_output,
        fetch_driven,
    )
}

fn run_utility(program: &UtilityProgram, query: &QueryRuntimeContext) -> Result<()> {
    let mut ctx = UtilityContext {
        session: query.session.as_ref(),
        catalog: &query.catalog,
        transaction: &query.transaction,
        params: query.params.as_ref(),
        cancel: &query.cancellation,
        errors: &query.errors,
    };
    program.run_once(&mut ctx)?;
    Ok(())
}

fn run_explain_analyze(
    target: &StatementProgram,
    spec: ExplainSpec,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let profiler = ExplainProfiler::new();
    let target_output = QueryOutputPort::discarding();
    let target_query = QueryRuntimeContext::new(
        query.session.clone(),
        query.params.clone(),
        query.memory.clone(),
        target_output.clone(),
    )
    .with_explain_profiler(profiler.clone());

    let started_at = Instant::now();
    match target {
        StatementProgram::Utility(utility) => run_utility(utility, &target_query)?,
        StatementProgram::Pipeline {
            graph, programs, ..
        } => run_pipeline_graph(graph.as_ref(), programs, &target_query, allocator.clone())?,
        StatementProgram::ExplainAnalyze { .. } => {
            return Err(paro_error::not_supported(
                "nested EXPLAIN ANALYZE is not supported",
            ));
        }
    }
    let elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0;
    let rows_returned = target_output.stats().pushed_rows as u64;
    profiler.record_query_memory_stats(target_query.memory.runtime_stats());
    let lines = render_explain_analyze(target, spec, profiler.as_ref(), elapsed_ms, rows_returned);
    push_explain_lines(&query.output, &lines, allocator)
}

fn run_pipeline_graph(
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    if matches!(graph.root, PipelineRoot::Utility(_)) {
        return Err(paro_error::internal(
            "utility roots should be represented as StatementProgram::Utility",
        ));
    }

    let handles = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles)?);
    if PipelineScheduler::should_use_parallel_scheduler(graph, query) {
        let result = PipelineScheduler::run_to_completion_with_registry(
            graph,
            programs,
            handles.clone(),
            query,
            allocator.clone(),
        );
        return cleanup_graph_result(result, handles, query, allocator);
    }
    run_pipeline_graph_with_cleanup(graph, programs, handles, query, allocator)
}

fn run_pipeline_graph_with_cleanup(
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let result =
        run_pipeline_graph_inner(graph, programs, handles.clone(), query, allocator.clone());
    cleanup_graph_result(result, handles, query, allocator)
}

#[cfg(test)]
pub(super) fn run_pipeline_graph_with_registry_for_test(
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    run_pipeline_graph_with_cleanup(graph, programs, handles, query, allocator)
}

fn run_pipeline_graph_inner(
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    if let PipelineRoot::ControlRegion(id) = graph.root {
        return run_control_region_root(id, graph, programs, handles, query, allocator);
    }

    if !graph.control_regions.is_empty() {
        return run_pipeline_graph_with_control_regions(graph, programs, handles, query, allocator);
    }

    let shared_sinks = SharedSinkRuntimeSet::from_graph(graph)?;
    let ctx = GraphExecutionContext {
        graph,
        programs,
        handles,
        shared_sinks: &shared_sinks,
        query,
        allocator,
    };
    run_pipeline_dag(&ctx, None)
}

fn cleanup_graph_result(
    result: Result<()>,
    handles: Arc<BreakerHandleRegistry>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let reason = match result.as_ref() {
        Ok(()) => crate::runtime::CleanupReason::Finished,
        Err(error) => cleanup_reason_for_error(query, error),
    };
    let cleanup_result = cleanup_handles(handles.as_ref(), query, allocator, reason);
    merge_execution_and_cleanup_result(result, cleanup_result)
}

fn run_pipeline_graph_with_control_regions(
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let mut regions = ControlRegionRuntimeSet::new(
        graph,
        programs,
        handles.clone(),
        query.params.clone(),
        query,
    )?;
    let shared_sinks = regions.shared_sinks.clone();
    let region_roots = control_region_root_pipelines(graph)?;
    let region_members = control_region_pipeline_members(graph, &region_roots)?;
    let mut pipeline_region = vec![None; programs.pipeline_count()];
    for (region_idx, members) in region_members.iter().enumerate() {
        let region_id = ControlRegionId::new(region_idx);
        for pipeline in members {
            let slot = pipeline_region.get_mut(pipeline.index()).ok_or_else(|| {
                paro_error::internal("control region references invalid pipeline id")
            })?;
            *slot = Some(region_id);
        }
    }

    let ctx = GraphExecutionContext {
        graph,
        programs,
        handles,
        shared_sinks: &shared_sinks,
        query,
        allocator,
    };
    run_pipeline_dag(
        &ctx,
        Some(ControlRegionDispatch {
            regions: &mut regions,
            pipeline_region: &pipeline_region,
            region_members: &region_members,
        }),
    )
}

/// Optional control-region dispatch state for the pipeline DAG runner.
struct ControlRegionDispatch<'a> {
    regions: &'a mut ControlRegionRuntimeSet,
    pipeline_region: &'a [Option<ControlRegionId>],
    region_members: &'a [Vec<PipelineId>],
}

/// Unified pipeline DAG execution loop. When `region_dispatch` is `None`, all
/// pipelines are plain acyclic pipelines. When present, pipelines that belong
/// to a control region are dispatched through the region controller.
fn run_pipeline_dag(
    ctx: &GraphExecutionContext<'_>,
    mut region_dispatch: Option<ControlRegionDispatch<'_>>,
) -> Result<()> {
    let pipeline_count = ctx.programs.pipeline_count();
    let mut gates = PipelineDependencyGates::from_graph(ctx.graph);
    let mut finished = vec![false; pipeline_count];
    let mut finished_count = 0usize;
    let mut ready = gates.ready_pipelines().into_iter().collect::<VecDeque<_>>();
    let mut region_finished = region_dispatch
        .as_ref()
        .map(|_| vec![false; ctx.graph.control_regions.len()]);

    while finished_count < finished.len() {
        let Some(pipeline_id) = ready.pop_front() else {
            return Err(paro_error::internal(
                "typed pipeline driver could not find a ready pipeline",
            ));
        };
        if finished.get(pipeline_id.index()).copied().unwrap_or(false) {
            continue;
        }
        if !gates.is_ready(pipeline_id) {
            return Err(paro_error::internal(
                "typed pipeline driver dequeued a pipeline before its gates opened",
            ));
        }

        // Check if this pipeline belongs to a control region.
        if let Some(dispatch) = region_dispatch.as_mut() {
            if let Some(region_id) = dispatch.pipeline_region[pipeline_id.index()] {
                let rf = region_finished.as_mut().expect("region_finished exists");
                if rf[region_id.index()] {
                    continue;
                }
                if pipeline_id != control_region_entry_pipeline(ctx.graph, region_id)? {
                    continue;
                }
                let mut completed = finished
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, done)| done.then_some(PipelineId::new(idx)))
                    .collect::<HashSet<_>>();
                run_control_region(region_id, dispatch.regions, ctx, &mut completed)?;
                rf[region_id.index()] = true;
                // The set was seeded with globally finished pipelines so region
                // dependencies are not replayed. Publish only newly completed work,
                // together with every member covered by the completed controller.
                let mut newly_finished = completed
                    .into_iter()
                    .filter(|pipeline| !finished[pipeline.index()])
                    .collect::<HashSet<_>>();
                newly_finished.extend(
                    dispatch.region_members[region_id.index()]
                        .iter()
                        .copied()
                        .filter(|member| !finished[member.index()]),
                );
                let mut newly_finished = newly_finished.into_iter().collect::<Vec<_>>();
                newly_finished.sort_unstable();
                for pipeline in newly_finished {
                    mark_pipeline_finished(
                        pipeline,
                        ctx.handles.as_ref(),
                        &mut finished,
                        &mut finished_count,
                        &mut gates,
                        &mut ready,
                    );
                }
                continue;
            }
        }

        run_pipeline(pipeline_id, ctx)?;
        mark_pipeline_finished(
            pipeline_id,
            ctx.handles.as_ref(),
            &mut finished,
            &mut finished_count,
            &mut gates,
            &mut ready,
        );
    }

    Ok(())
}

fn mark_pipeline_finished(
    pipeline_id: PipelineId,
    handles: &BreakerHandleRegistry,
    finished: &mut [bool],
    finished_count: &mut usize,
    gates: &mut PipelineDependencyGates,
    ready: &mut VecDeque<PipelineId>,
) {
    if finished[pipeline_id.index()] {
        return;
    }
    finished[pipeline_id.index()] = true;
    *finished_count += 1;
    handles.pipeline_finished(pipeline_id);
    for event in gates.mark_finished(pipeline_id) {
        if !finished[event.pipeline.index()] && gates.is_ready(event.pipeline) {
            ready.push_back(event.pipeline);
        }
    }
}

pub(super) fn control_region_pipeline_members(
    graph: &PipelineGraph,
    region_roots: &HashMap<PipelineId, ControlRegionId>,
) -> Result<Vec<Vec<PipelineId>>> {
    let mut all_members = Vec::with_capacity(graph.control_regions.len());
    for idx in 0..graph.control_regions.len() {
        let mut members = Vec::new();
        let mut visiting = HashSet::new();
        collect_control_region_pipeline_members(
            graph,
            ControlRegionId::new(idx),
            region_roots,
            &mut visiting,
            &mut members,
        )?;
        members.sort_unstable();
        members.dedup();
        all_members.push(members);
    }
    Ok(all_members)
}

fn collect_control_region_pipeline_members(
    graph: &PipelineGraph,
    id: ControlRegionId,
    region_roots: &HashMap<PipelineId, ControlRegionId>,
    visiting: &mut HashSet<usize>,
    members: &mut Vec<PipelineId>,
) -> Result<()> {
    if !visiting.insert(id.index()) {
        return Err(paro_error::internal(
            "control region dependency cycle is not supported",
        ));
    }

    let Some(region) = graph.control_regions.get(id.index()) else {
        return Err(paro_error::internal("control region id is invalid"));
    };
    match region {
        ControlRegion::RecursiveCte(region) => {
            collect_pipeline_member(graph, id, region.anchor, region_roots, visiting, members)?;
            for pipeline in region.recursive.iter().copied() {
                collect_pipeline_member(graph, id, pipeline, region_roots, visiting, members)?;
            }
            collect_pipeline_member(graph, id, region.emit, region_roots, visiting, members)?;
        }
        ControlRegion::CorrelatedSubquery(region) => {
            collect_pipeline_member(graph, id, region.capture, region_roots, visiting, members)?;
            for root in &region.dependent_roots {
                match root {
                    PipelineSubgraphRoot::Pipeline(pipeline) => {
                        collect_pipeline_member(
                            graph,
                            id,
                            *pipeline,
                            region_roots,
                            visiting,
                            members,
                        )?;
                    }
                    PipelineSubgraphRoot::ControlRegion(region) => {
                        collect_control_region_pipeline_members(
                            graph,
                            *region,
                            region_roots,
                            visiting,
                            members,
                        )?;
                    }
                }
            }
            collect_pipeline_member(graph, id, region.join, region_roots, visiting, members)?;
        }
    }

    visiting.remove(&id.index());
    Ok(())
}

fn collect_pipeline_member(
    graph: &PipelineGraph,
    owner: ControlRegionId,
    pipeline: PipelineId,
    region_roots: &HashMap<PipelineId, ControlRegionId>,
    visiting: &mut HashSet<usize>,
    members: &mut Vec<PipelineId>,
) -> Result<()> {
    members.push(pipeline);
    let Some(nested) = region_roots.get(&pipeline).copied() else {
        return Ok(());
    };
    if nested == owner {
        return Ok(());
    }
    collect_control_region_pipeline_members(graph, nested, region_roots, visiting, members)
}

pub(super) fn control_region_root_pipelines(
    graph: &PipelineGraph,
) -> Result<HashMap<PipelineId, ControlRegionId>> {
    let mut roots = HashMap::with_capacity(graph.control_regions.len());
    for (idx, region) in graph.control_regions.iter().enumerate() {
        let id = ControlRegionId::new(idx);
        let root = match region {
            ControlRegion::RecursiveCte(region) => region.emit,
            ControlRegion::CorrelatedSubquery(region) => region.join,
        };
        if roots.insert(root, id).is_some() {
            return Err(paro_error::internal(
                "multiple control regions share the same root pipeline",
            ));
        }
    }
    Ok(roots)
}

fn run_pipeline(pipeline: PipelineId, ctx: &GraphExecutionContext<'_>) -> Result<()> {
    let program = ctx
        .programs
        .get(pipeline)
        .cloned()
        .ok_or_else(|| paro_error::internal("pipeline program missing"))?;
    let spec = ctx
        .graph
        .pipeline(pipeline)
        .ok_or_else(|| paro_error::internal("pipeline spec missing"))?;
    let shared_sink = match spec.sink_sharing {
        SinkSharing::Exclusive => None,
        SinkSharing::Shared(id) => ctx.shared_sinks.get(id),
    };
    let runtime = Arc::new(PipelineRuntime::with_registry_and_shared_sink(
        program,
        ctx.handles.clone(),
        ctx.query.params.clone(),
        ctx.query,
        shared_sink,
    )?);
    run_runtime(runtime, ctx)
}

fn run_runtime(runtime: Arc<PipelineRuntime>, ctx: &GraphExecutionContext<'_>) -> Result<()> {
    let pipeline = runtime.program.id;
    let spec = ctx
        .graph
        .pipeline(pipeline)
        .ok_or_else(|| paro_error::internal("control-region pipeline spec missing"))?;
    run_bound_pipeline_runtime(
        runtime,
        spec.properties.capabilities.parallelism,
        Arc::new(ctx.query.clone()),
        ctx.allocator.clone(),
    )
}

fn run_control_region_root(
    root: ControlRegionId,
    graph: &PipelineGraph,
    programs: &PipelineProgramSet,
    handles: Arc<BreakerHandleRegistry>,
    query: &QueryRuntimeContext,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let mut regions = ControlRegionRuntimeSet::new(
        graph,
        programs,
        handles.clone(),
        query.params.clone(),
        query,
    )?;
    let shared_sinks = regions.shared_sinks.clone();
    let ctx = GraphExecutionContext {
        graph,
        programs,
        handles,
        shared_sinks: &shared_sinks,
        query,
        allocator,
    };
    run_control_region(root, &mut regions, &ctx, &mut HashSet::new())
}

fn run_control_region(
    id: ControlRegionId,
    regions: &mut ControlRegionRuntimeSet,
    ctx: &GraphExecutionContext<'_>,
    completed: &mut HashSet<PipelineId>,
) -> Result<()> {
    match regions.regions.get(id.index()) {
        Some(ControlRegionRuntime::RecursiveCte(_)) => {
            run_recursive_cte_region(id, regions, ctx, completed)
        }
        Some(ControlRegionRuntime::CorrelatedSubquery(_)) => {
            run_correlated_subquery_region(id, regions, ctx, completed)
        }
        None => Err(paro_error::internal("control-region id is invalid")),
    }
}

fn recursive_cte_controller_mut(
    regions: &mut ControlRegionRuntimeSet,
    id: ControlRegionId,
) -> Result<&mut crate::runtime::RecursiveCteControllerState> {
    let region = regions
        .regions
        .get_mut(id.index())
        .ok_or_else(|| paro_error::internal("control-region id is invalid"))?;
    match region {
        ControlRegionRuntime::RecursiveCte(controller) => Ok(controller),
        ControlRegionRuntime::CorrelatedSubquery(_) => Err(paro_error::internal(
            "control-region id does not reference a recursive CTE",
        )),
    }
}

fn run_recursive_cte_region(
    id: ControlRegionId,
    regions: &mut ControlRegionRuntimeSet,
    ctx: &GraphExecutionContext<'_>,
    completed: &mut HashSet<PipelineId>,
) -> Result<()> {
    let anchor = recursive_cte_controller_mut(regions, id)?.start_anchor()?;
    let anchor_id = anchor.program.id;
    run_pipeline_dependencies(anchor_id, id, regions, ctx, completed)?;
    run_runtime(anchor, ctx)?;
    completed.insert(anchor_id);

    let mut action = recursive_cte_controller_mut(regions, id)?.finish_anchor()?;
    loop {
        match action {
            RecursiveCteGateAction::RunPipelines(_) => {
                recursive_cte_controller_mut(regions, id)?.start_recursive_iteration(ctx.query)?;
                let runtimes = recursive_cte_controller_mut(regions, id)?
                    .iteration_runtime
                    .as_ref()
                    .ok_or_else(|| paro_error::internal("recursive CTE iteration runtime missing"))?
                    .recursive_pipelines
                    .to_vec();
                for runtime in &runtimes {
                    completed.remove(&runtime.program.id);
                }
                for runtime in runtimes {
                    let pipeline_id = runtime.program.id;
                    if completed.contains(&pipeline_id) {
                        continue;
                    }
                    run_pipeline_dependencies(pipeline_id, id, regions, ctx, completed)?;
                    run_runtime(runtime, ctx)?;
                    completed.insert(pipeline_id);
                }
                action = recursive_cte_controller_mut(regions, id)?.finish_recursive_iteration()?;
            }
            RecursiveCteGateAction::RunPipeline(pipeline) => {
                let emit_pipeline = recursive_cte_controller_mut(regions, id)?.programs.emit.id;
                if pipeline != emit_pipeline {
                    return Err(paro_error::internal(
                        "recursive CTE controller requested unknown pipeline",
                    ));
                }
                let emit = recursive_cte_controller_mut(regions, id)?.start_emit(ctx.query)?;
                let emit_id = emit.program.id;
                run_pipeline_dependencies(emit_id, id, regions, ctx, completed)?;
                run_runtime(emit, ctx)?;
                completed.insert(emit_id);
                recursive_cte_controller_mut(regions, id)?.finish_emit()?;
                return Ok(());
            }
            RecursiveCteGateAction::Done => return Ok(()),
        }
    }
}

fn run_correlated_subquery_region(
    id: ControlRegionId,
    regions: &mut ControlRegionRuntimeSet,
    ctx: &GraphExecutionContext<'_>,
    completed: &mut HashSet<PipelineId>,
) -> Result<()> {
    let capture_id = correlated_subquery_controller_mut(regions, id)?.capture.id;
    if control_region_root_for_pipeline(ctx.graph, capture_id, Some(id))?.is_none() {
        run_pipeline_dependencies(capture_id, id, regions, ctx, completed)?;
    }
    let capture = correlated_subquery_controller_mut(regions, id)?.start_capture(ctx.query)?;
    run_pipeline_runtime_or_nested_region(capture_id, capture, id, regions, ctx, completed)?;
    completed.insert(capture_id);

    let dependents = {
        let controller = correlated_subquery_controller_mut(regions, id)?;
        controller.finish_capture()?
    };
    for root in dependents.iter() {
        match root {
            PipelineSubgraphRoot::Pipeline(pipeline) => {
                run_control_region_pipeline(*pipeline, id, regions, ctx, completed)?;
            }
            PipelineSubgraphRoot::ControlRegion(region) => {
                run_control_region(*region, regions, ctx, completed)?;
            }
        }
    }

    let join_id = {
        let controller = correlated_subquery_controller_mut(regions, id)?;
        controller.finish_dependents()?;
        controller.join.id
    };
    run_pipeline_dependencies(join_id, id, regions, ctx, completed)?;
    let join = correlated_subquery_controller_mut(regions, id)?.start_join(ctx.query)?;
    run_runtime(join, ctx)?;
    completed.insert(join_id);

    let controller = correlated_subquery_controller_mut(regions, id)?;
    controller.finish_join()
}

fn run_control_region_pipeline(
    pipeline: PipelineId,
    current_region: ControlRegionId,
    regions: &mut ControlRegionRuntimeSet,
    ctx: &GraphExecutionContext<'_>,
    completed: &mut HashSet<PipelineId>,
) -> Result<()> {
    if completed.contains(&pipeline) {
        return Ok(());
    }
    if let Some(nested) =
        control_region_root_for_pipeline(ctx.graph, pipeline, Some(current_region))?
    {
        run_control_region(nested, regions, ctx, completed)?;
    } else {
        run_pipeline_dependencies(pipeline, current_region, regions, ctx, completed)?;
        run_pipeline(pipeline, ctx)?;
    }
    completed.insert(pipeline);
    Ok(())
}

fn run_pipeline_dependencies(
    consumer: PipelineId,
    current_region: ControlRegionId,
    regions: &mut ControlRegionRuntimeSet,
    ctx: &GraphExecutionContext<'_>,
    completed: &mut HashSet<PipelineId>,
) -> Result<()> {
    let producers = ctx
        .graph
        .dependencies
        .iter()
        .filter(|dependency| {
            dependency.consumer == consumer
                && !matches!(
                    dependency.kind,
                    crate::pipeline::graph::DependencyKind::LoopEntry(_)
                        | crate::pipeline::graph::DependencyKind::LoopBack(_)
                )
        })
        .map(|dependency| dependency.producer)
        .collect::<Vec<_>>();
    for producer in producers {
        run_control_region_pipeline(producer, current_region, regions, ctx, completed)?;
    }
    Ok(())
}

fn run_pipeline_runtime_or_nested_region(
    pipeline: PipelineId,
    runtime: Arc<PipelineRuntime>,
    current_region: ControlRegionId,
    regions: &mut ControlRegionRuntimeSet,
    ctx: &GraphExecutionContext<'_>,
    completed: &mut HashSet<PipelineId>,
) -> Result<()> {
    if let Some(nested) =
        control_region_root_for_pipeline(ctx.graph, pipeline, Some(current_region))?
    {
        return run_control_region(nested, regions, ctx, completed);
    }
    run_runtime(runtime, ctx)
}

fn control_region_entry_pipeline(
    graph: &PipelineGraph,
    region: ControlRegionId,
) -> Result<PipelineId> {
    let entry = match graph.control_regions.get(region.index()) {
        Some(ControlRegion::RecursiveCte(region)) => Ok(region.anchor),
        Some(ControlRegion::CorrelatedSubquery(region)) => Ok(region.capture),
        None => Err(paro_error::internal("control-region id is invalid")),
    }?;
    if let Some(nested) = control_region_root_for_pipeline(graph, entry, Some(region))? {
        return control_region_entry_pipeline(graph, nested);
    }
    Ok(entry)
}

fn control_region_root_for_pipeline(
    graph: &PipelineGraph,
    pipeline: PipelineId,
    exclude: Option<ControlRegionId>,
) -> Result<Option<ControlRegionId>> {
    let mut found = None;
    for (idx, region) in graph.control_regions.iter().enumerate() {
        let id = ControlRegionId::new(idx);
        if Some(id) == exclude {
            continue;
        }
        let root = match region {
            ControlRegion::RecursiveCte(region) => region.emit,
            ControlRegion::CorrelatedSubquery(region) => region.join,
        };
        if root != pipeline {
            continue;
        }
        if found.replace(id).is_some() {
            return Err(paro_error::internal(
                "multiple control regions share the same root pipeline",
            ));
        }
    }
    Ok(found)
}

fn correlated_subquery_controller_mut(
    regions: &mut ControlRegionRuntimeSet,
    id: ControlRegionId,
) -> Result<&mut crate::runtime::CorrelatedSubqueryControllerState> {
    let region = regions
        .regions
        .get_mut(id.index())
        .ok_or_else(|| paro_error::internal("control-region id is invalid"))?;
    match region {
        ControlRegionRuntime::CorrelatedSubquery(controller) => Ok(controller),
        ControlRegionRuntime::RecursiveCte(_) => Err(paro_error::internal(
            "control-region id does not reference a correlated subquery",
        )),
    }
}
