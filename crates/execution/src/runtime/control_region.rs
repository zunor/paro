// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Scheduler-visible control-region runtime state.

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};

use crate::explain::profiler::ExplainProfiler;
use crate::explain::types::{
    ExplainControlRegionStats, ExplainRecursiveCteIterationStats, ExplainRecursiveCteStats,
    ExplainRecursiveCteTermination,
};
use crate::pipeline::graph::{
    ControlRegion, ControlRegionId, CorrelatedSubqueryRegion, DelimJoinSide, DependencyKind,
    PipelineDependency, PipelineGraph, PipelineId, PipelineSubgraphRoot, RecursiveCteDedup,
    RecursiveCteRegion, RecursiveTermination, SharedSinkId, SinkSharing,
};
use crate::pipeline::program::{PipelineProgram, PipelineProgramSet};

use super::breaker::{
    DelimHandle, HandleRef, RecursiveDedupSet, RecursiveTableHandle, SharedSinkCoordinator,
};
use super::context::QueryRuntimeContext;
use super::parameter::ParameterBindings;
use super::pipeline_runtime::PipelineRuntime;
use super::BreakerHandleRegistry;

#[derive(Debug)]
pub struct ControlRegionRuntimeSet {
    pub regions: Box<[ControlRegionRuntime]>,
    pub shared_sinks: SharedSinkRuntimeSet,
    pub dependency_gates: PipelineDependencyGates,
}

impl ControlRegionRuntimeSet {
    pub fn new(
        graph: &PipelineGraph,
        programs: &PipelineProgramSet,
        handles: Arc<BreakerHandleRegistry>,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
    ) -> Result<Self> {
        let shared_sinks = SharedSinkRuntimeSet::from_graph(graph)?;
        let regions = graph
            .control_regions
            .iter()
            .enumerate()
            .map(|(idx, region)| {
                let id = ControlRegionId::new(idx);
                match region {
                    ControlRegion::RecursiveCte(region) => {
                        RecursiveCteControllerState::from_region(
                            id,
                            region,
                            programs,
                            handles.clone(),
                            params.clone(),
                            query,
                            &shared_sinks,
                        )
                        .map(ControlRegionRuntime::RecursiveCte)
                    }
                    ControlRegion::CorrelatedSubquery(region) => {
                        CorrelatedSubqueryControllerState::from_region(
                            id,
                            region,
                            programs,
                            handles.clone(),
                            params.clone(),
                            query,
                            &shared_sinks,
                        )
                        .map(ControlRegionRuntime::CorrelatedSubquery)
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();

        Ok(Self {
            regions,
            shared_sinks,
            dependency_gates: PipelineDependencyGates::from_graph(graph),
        })
    }
}

#[derive(Debug)]
pub enum ControlRegionRuntime {
    RecursiveCte(RecursiveCteControllerState),
    CorrelatedSubquery(CorrelatedSubqueryControllerState),
}

#[derive(Debug)]
pub struct RecursiveCteControllerState {
    pub id: ControlRegionId,
    pub programs: RecursiveCtePrograms,
    pub persistent: RecursiveCtePersistentState,
    pub anchor_runtime: Option<Arc<PipelineRuntime>>,
    pub iteration_runtime: Option<RecursiveCteIterationRuntime>,
    pub emit_runtime: Option<Arc<PipelineRuntime>>,
    pub phase: RecursiveCtePhase,
    handles: Arc<BreakerHandleRegistry>,
    params: Arc<ParameterBindings>,
    shared_sinks: SharedSinkRuntimeSet,
    profile: Option<RecursiveCteProfileState>,
}

impl RecursiveCteControllerState {
    pub fn from_region(
        id: ControlRegionId,
        region: &RecursiveCteRegion,
        programs: &PipelineProgramSet,
        handles: Arc<BreakerHandleRegistry>,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
        shared_sinks: &SharedSinkRuntimeSet,
    ) -> Result<Self> {
        let programs = RecursiveCtePrograms::from_region(region, programs)?;
        let intermediate =
            handles.get(HandleRef::<RecursiveTableHandle>::new(region.intermediate))?;
        let dedup = match region.dedup {
            RecursiveCteDedup::None => RecursiveCteDedupRuntime::None,
            RecursiveCteDedup::HashSet => {
                let set = Arc::new(RecursiveDedupSet::new());
                intermediate.set_dedup(set.clone());
                RecursiveCteDedupRuntime::HashSet { set }
            }
        };
        let persistent = RecursiveCtePersistentState {
            working: handles.get(HandleRef::<RecursiveTableHandle>::new(region.working))?,
            intermediate,
            accumulated: region
                .accumulated
                .map(|handle| handles.get(HandleRef::<RecursiveTableHandle>::new(handle)))
                .transpose()?,
            dedup,
            termination: region.termination,
            iteration: 0,
        };

        let anchor_runtime = Some(Arc::new(pipeline_runtime_with_shared_sink(
            programs.anchor.clone(),
            handles.clone(),
            params.clone(),
            query,
            shared_sinks,
        )?));

        Ok(Self {
            id,
            programs,
            persistent,
            anchor_runtime,
            iteration_runtime: None,
            emit_runtime: None,
            phase: RecursiveCtePhase::AnchorReady,
            handles,
            params,
            shared_sinks: shared_sinks.clone(),
            profile: query
                .explain_profiler
                .as_ref()
                .map(|profiler| RecursiveCteProfileState::new(id, profiler.clone())),
        })
    }

    pub fn start_anchor(&mut self) -> Result<Arc<PipelineRuntime>> {
        match self.phase {
            RecursiveCtePhase::AnchorReady => {
                self.phase = RecursiveCtePhase::AnchorRunning;
                self.anchor_runtime
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| paro_error::internal("recursive CTE anchor runtime missing"))
            }
            _ => Err(paro_error::internal(
                "recursive CTE anchor can only start from AnchorReady",
            )),
        }
    }

    pub fn finish_anchor(&mut self) -> Result<RecursiveCteGateAction> {
        match self.phase {
            RecursiveCtePhase::AnchorRunning | RecursiveCtePhase::AnchorReady => {
                self.advance_loop_or_emit()
            }
            _ => Err(paro_error::internal(
                "recursive CTE anchor finished in invalid phase",
            )),
        }
    }

    pub fn start_recursive_iteration(
        &mut self,
        query: &QueryRuntimeContext,
    ) -> Result<RecursiveCteGateAction> {
        match self.phase {
            RecursiveCtePhase::LoopEntry | RecursiveCtePhase::LoopBack => {}
            _ => {
                return Err(paro_error::internal(
                    "recursive CTE iteration can only start from a loop gate",
                ));
            }
        }

        self.handles
            .reset_materialized_handles_produced_by(&self.programs.recursive_pipeline_ids);
        let runtimes = self
            .programs
            .recursive
            .iter()
            .map(|program| {
                pipeline_runtime_with_shared_sink(
                    program.clone(),
                    self.handles.clone(),
                    self.params.clone(),
                    query,
                    &self.shared_sinks,
                )
                .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        self.iteration_runtime = Some(RecursiveCteIterationRuntime {
            recursive_pipelines: runtimes,
        });
        self.phase = RecursiveCtePhase::RecursiveRunning;
        Ok(RecursiveCteGateAction::RunPipelines(
            self.programs.recursive_pipeline_ids.clone(),
        ))
    }

    pub fn finish_recursive_iteration(&mut self) -> Result<RecursiveCteGateAction> {
        if self.phase != RecursiveCtePhase::RecursiveRunning {
            return Err(paro_error::internal(
                "recursive CTE recursive branch finished in invalid phase",
            ));
        }
        self.iteration_runtime = None;
        self.phase = RecursiveCtePhase::LoopBack;
        self.advance_loop_or_emit()
    }

    pub fn start_emit(&mut self, query: &QueryRuntimeContext) -> Result<Arc<PipelineRuntime>> {
        if self.phase != RecursiveCtePhase::EmitReady {
            return Err(paro_error::internal(
                "recursive CTE emit can only start from EmitReady",
            ));
        }
        let runtime = Arc::new(pipeline_runtime_with_shared_sink(
            self.programs.emit.clone(),
            self.handles.clone(),
            self.params.clone(),
            query,
            &self.shared_sinks,
        )?);
        self.emit_runtime = Some(runtime.clone());
        self.phase = RecursiveCtePhase::EmitRunning;
        Ok(runtime)
    }

    pub fn finish_emit(&mut self) -> Result<()> {
        if self.phase != RecursiveCtePhase::EmitRunning {
            return Err(paro_error::internal(
                "recursive CTE emit finished in invalid phase",
            ));
        }
        self.emit_runtime = None;
        if let Some(profile) = self.profile.as_mut() {
            profile.flush();
        }
        self.phase = RecursiveCtePhase::Done;
        Ok(())
    }

    pub fn cancel(&mut self) {
        self.anchor_runtime = None;
        self.iteration_runtime = None;
        self.emit_runtime = None;
        if let Some(profile) = self.profile.as_mut() {
            profile.finish(ExplainRecursiveCteTermination::Cancelled);
            profile.flush();
        }
        self.phase = RecursiveCtePhase::Cancelled;
    }

    fn advance_loop_or_emit(&mut self) -> Result<RecursiveCteGateAction> {
        let has_delta = !self.persistent.intermediate.is_empty();
        let next_iteration = self.persistent.iteration + 1;
        if self
            .persistent
            .termination
            .allows_next_iteration(next_iteration, has_delta)
        {
            let chunks = self.persistent.intermediate.take_chunks();
            if let Some(profile) = self.profile.as_mut() {
                let rows = chunks.iter().map(|chunk| chunk.size()).sum::<usize>() as u64;
                profile.record_iteration(next_iteration, rows, rows);
            }
            if let Some(accumulated) = self.persistent.accumulated.as_ref() {
                accumulated.append_snapshot(&chunks);
            }
            self.persistent.working.replace_chunks(chunks);
            self.persistent.iteration = next_iteration;
            self.phase = RecursiveCtePhase::LoopEntry;
            return Ok(RecursiveCteGateAction::RunPipelines(
                self.programs.recursive_pipeline_ids.clone(),
            ));
        }

        if let Some(profile) = self.profile.as_mut() {
            profile.finish(recursive_cte_termination_reason(
                self.persistent.termination,
                has_delta,
            ));
        }
        self.phase = RecursiveCtePhase::EmitReady;
        Ok(RecursiveCteGateAction::RunPipeline(self.programs.emit.id))
    }
}

#[derive(Debug)]
struct RecursiveCteProfileState {
    profiler: Arc<ExplainProfiler>,
    region_id: ControlRegionId,
    iteration_stats: Vec<ExplainRecursiveCteIterationStats>,
    termination: Option<ExplainRecursiveCteTermination>,
    flushed: bool,
}

impl RecursiveCteProfileState {
    fn new(region_id: ControlRegionId, profiler: Arc<ExplainProfiler>) -> Self {
        Self {
            profiler,
            region_id,
            iteration_stats: Vec::new(),
            termination: None,
            flushed: false,
        }
    }

    fn record_iteration(&mut self, iteration: usize, delta_rows: u64, working_rows: u64) {
        self.iteration_stats
            .push(ExplainRecursiveCteIterationStats {
                iteration,
                delta_rows,
                working_rows,
            });
    }

    fn finish(&mut self, termination: ExplainRecursiveCteTermination) {
        if self.termination.is_none() {
            self.termination = Some(termination);
        }
    }

    fn flush(&mut self) {
        if self.flushed {
            return;
        }
        let Some(termination) = self.termination else {
            return;
        };
        self.profiler
            .record_control_region(ExplainControlRegionStats::RecursiveCte(
                ExplainRecursiveCteStats {
                    region_id: self.region_id.index(),
                    iterations: self.iteration_stats.len(),
                    termination,
                    iteration_stats: self.iteration_stats.clone(),
                },
            ));
        self.flushed = true;
    }
}

fn recursive_cte_termination_reason(
    termination: RecursiveTermination,
    has_delta: bool,
) -> ExplainRecursiveCteTermination {
    match termination {
        RecursiveTermination::UntilEmpty => ExplainRecursiveCteTermination::EmptyDelta,
        RecursiveTermination::MaxIterations(_) => ExplainRecursiveCteTermination::MaxIterations,
        RecursiveTermination::UntilEmptyOrMaxIterations(_) if has_delta => {
            ExplainRecursiveCteTermination::MaxIterations
        }
        RecursiveTermination::UntilEmptyOrMaxIterations(_) => {
            ExplainRecursiveCteTermination::EmptyDelta
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecursiveCtePrograms {
    pub anchor: Arc<PipelineProgram>,
    pub recursive: Box<[Arc<PipelineProgram>]>,
    pub recursive_pipeline_ids: Box<[PipelineId]>,
    pub emit: Arc<PipelineProgram>,
}

impl RecursiveCtePrograms {
    fn from_region(region: &RecursiveCteRegion, programs: &PipelineProgramSet) -> Result<Self> {
        let recursive: Box<[Arc<PipelineProgram>]> = region
            .recursive
            .iter()
            .map(|id| {
                programs
                    .get(*id)
                    .cloned()
                    .ok_or_else(|| paro_error::internal("recursive CTE recursive program missing"))
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        let recursive_pipeline_ids: Box<[PipelineId]> =
            recursive.iter().map(|p| p.id).collect::<Vec<_>>().into();
        Ok(Self {
            anchor: programs
                .get(region.anchor)
                .cloned()
                .ok_or_else(|| paro_error::internal("recursive CTE anchor program missing"))?,
            recursive,
            recursive_pipeline_ids,
            emit: programs
                .get(region.emit)
                .cloned()
                .ok_or_else(|| paro_error::internal("recursive CTE emit program missing"))?,
        })
    }
}

#[derive(Debug)]
pub struct RecursiveCtePersistentState {
    pub working: Arc<RecursiveTableHandle>,
    pub intermediate: Arc<RecursiveTableHandle>,
    pub accumulated: Option<Arc<RecursiveTableHandle>>,
    pub dedup: RecursiveCteDedupRuntime,
    pub termination: RecursiveTermination,
    pub iteration: usize,
}

#[derive(Debug)]
pub enum RecursiveCteDedupRuntime {
    None,
    HashSet { set: Arc<RecursiveDedupSet> },
}

#[derive(Debug)]
pub struct RecursiveCteIterationRuntime {
    pub recursive_pipelines: Box<[Arc<PipelineRuntime>]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecursiveCtePhase {
    AnchorReady,
    AnchorRunning,
    LoopEntry,
    RecursiveRunning,
    LoopBack,
    EmitReady,
    EmitRunning,
    Done,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecursiveCteGateAction {
    RunPipeline(PipelineId),
    RunPipelines(Box<[PipelineId]>),
    Done,
}

#[derive(Debug)]
pub struct CorrelatedSubqueryControllerState {
    pub id: ControlRegionId,
    pub side: DelimJoinSide,
    pub capture: Arc<PipelineRuntime>,
    pub dependent_roots: Box<[PipelineSubgraphRoot]>,
    pub join: Arc<PipelineRuntime>,
    pub delim_values: Arc<DelimHandle>,
    pub cached_outer: Option<Arc<DelimHandle>>,
    pub phase: CorrelatedSubqueryPhase,
}

impl CorrelatedSubqueryControllerState {
    pub fn from_region(
        id: ControlRegionId,
        region: &CorrelatedSubqueryRegion,
        programs: &PipelineProgramSet,
        handles: Arc<BreakerHandleRegistry>,
        params: Arc<ParameterBindings>,
        query: &QueryRuntimeContext,
        shared_sinks: &SharedSinkRuntimeSet,
    ) -> Result<Self> {
        let capture = programs
            .get(region.capture)
            .cloned()
            .ok_or_else(|| paro_error::internal("correlated subquery capture program missing"))?;
        let join = programs
            .get(region.join)
            .cloned()
            .ok_or_else(|| paro_error::internal("correlated subquery join program missing"))?;
        Ok(Self {
            id,
            side: region.side,
            capture: Arc::new(pipeline_runtime_with_shared_sink(
                capture,
                handles.clone(),
                params.clone(),
                query,
                shared_sinks,
            )?),
            dependent_roots: region.dependent_roots.clone().into_boxed_slice(),
            join: Arc::new(pipeline_runtime_with_shared_sink(
                join,
                handles.clone(),
                params,
                query,
                shared_sinks,
            )?),
            delim_values: handles.get(HandleRef::<DelimHandle>::new(region.delim_values))?,
            cached_outer: region
                .cached_outer
                .map(|handle| handles.get(HandleRef::<DelimHandle>::new(handle)))
                .transpose()?,
            phase: CorrelatedSubqueryPhase::CaptureReady,
        })
    }

    pub fn start_capture(&mut self) -> Result<PipelineId> {
        if self.phase != CorrelatedSubqueryPhase::CaptureReady {
            return Err(paro_error::internal(
                "correlated subquery capture can only start from CaptureReady",
            ));
        }
        self.phase = CorrelatedSubqueryPhase::CaptureRunning;
        Ok(self.capture.program.id)
    }

    pub fn finish_capture(&mut self) -> Result<Box<[PipelineSubgraphRoot]>> {
        if self.phase != CorrelatedSubqueryPhase::CaptureRunning {
            return Err(paro_error::internal(
                "correlated subquery capture finished in invalid phase",
            ));
        }
        self.delim_values.seal_capture()?;
        if let Some(cached_outer) = self.cached_outer.as_ref() {
            cached_outer.seal_capture()?;
        }
        self.phase = CorrelatedSubqueryPhase::DependentReady;
        Ok(self.dependent_roots.clone())
    }

    pub fn finish_dependents(&mut self) -> Result<PipelineId> {
        match self.phase {
            CorrelatedSubqueryPhase::DependentReady | CorrelatedSubqueryPhase::DependentRunning => {
                self.phase = CorrelatedSubqueryPhase::JoinReady;
                Ok(self.join.program.id)
            }
            _ => Err(paro_error::internal(
                "correlated subquery dependents finished in invalid phase",
            )),
        }
    }

    pub fn start_join(&mut self) -> Result<PipelineId> {
        if self.phase != CorrelatedSubqueryPhase::JoinReady {
            return Err(paro_error::internal(
                "correlated subquery join can only start from JoinReady",
            ));
        }
        self.phase = CorrelatedSubqueryPhase::JoinRunning;
        Ok(self.join.program.id)
    }

    pub fn finish_join(&mut self) -> Result<()> {
        if self.phase != CorrelatedSubqueryPhase::JoinRunning {
            return Err(paro_error::internal(
                "correlated subquery join finished in invalid phase",
            ));
        }
        self.phase = CorrelatedSubqueryPhase::Done;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelatedSubqueryPhase {
    CaptureReady,
    CaptureRunning,
    DependentReady,
    DependentRunning,
    JoinReady,
    JoinRunning,
    Done,
}

#[derive(Debug, Default, Clone)]
pub struct SharedSinkRuntimeSet {
    coordinators: HashMap<SharedSinkId, Arc<SharedSinkCoordinator>>,
}

impl SharedSinkRuntimeSet {
    pub fn from_graph(graph: &PipelineGraph) -> Result<Self> {
        let mut coordinators = HashMap::<SharedSinkId, Arc<SharedSinkCoordinator>>::new();
        for pipeline in &graph.pipelines {
            let SinkSharing::Shared(id) = pipeline.sink_sharing else {
                continue;
            };
            let coordinator = coordinators
                .entry(id)
                .or_insert_with(|| Arc::new(SharedSinkCoordinator::new(id)));
            coordinator.register_producer()?;
        }
        for coordinator in coordinators.values() {
            coordinator.freeze_producer_count()?;
        }
        Ok(Self { coordinators })
    }

    pub fn get(&self, id: SharedSinkId) -> Option<Arc<SharedSinkCoordinator>> {
        self.coordinators.get(&id).cloned()
    }

    pub fn len(&self) -> usize {
        self.coordinators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.coordinators.is_empty()
    }
}

fn pipeline_runtime_with_shared_sink(
    program: Arc<PipelineProgram>,
    handles: Arc<BreakerHandleRegistry>,
    params: Arc<ParameterBindings>,
    query: &QueryRuntimeContext,
    shared_sinks: &SharedSinkRuntimeSet,
) -> Result<PipelineRuntime> {
    let shared_sink = match program.sink_sharing {
        SinkSharing::Exclusive => None,
        SinkSharing::Shared(id) => Some(shared_sinks.get(id).ok_or_else(|| {
            paro_error::internal("shared sink coordinator missing for pipeline runtime")
        })?),
    };
    PipelineRuntime::with_registry_and_shared_sink(program, handles, params, query, shared_sink)
}

#[derive(Debug)]
pub struct PipelineDependencyGates {
    remaining: Vec<usize>,
    consumers: Vec<Vec<PipelineDependency>>,
    ready: Vec<bool>,
    side_effects: SideEffectOrderGate,
}

impl PipelineDependencyGates {
    pub fn from_graph(graph: &PipelineGraph) -> Self {
        let mut remaining = vec![0usize; graph.pipelines.len()];
        let mut consumers = vec![Vec::<PipelineDependency>::new(); graph.pipelines.len()];
        for dependency in &graph.dependencies {
            if matches!(
                dependency.kind,
                DependencyKind::LoopEntry(_) | DependencyKind::LoopBack(_)
            ) {
                continue;
            }
            remaining[dependency.consumer.index()] += 1;
            consumers[dependency.producer.index()].push(*dependency);
        }
        let side_effects = SideEffectOrderGate::from_dependencies(&graph.dependencies);
        let ready = remaining.iter().map(|count| *count == 0).collect();
        Self {
            remaining,
            consumers,
            ready,
            side_effects,
        }
    }

    pub fn is_ready(&self, pipeline: PipelineId) -> bool {
        self.ready.get(pipeline.index()).copied().unwrap_or(false)
            && self.side_effects.can_start(pipeline)
    }

    pub fn ready_pipelines(&self) -> Vec<PipelineId> {
        self.ready
            .iter()
            .enumerate()
            .filter_map(|(idx, ready)| {
                let pipeline = PipelineId::new(idx);
                (*ready && self.side_effects.can_start(pipeline)).then_some(pipeline)
            })
            .collect()
    }

    pub fn mark_finished(&mut self, pipeline: PipelineId) -> Vec<PipelineGateEvent> {
        self.side_effects.mark_finished(pipeline);
        let mut events = Vec::new();
        let Some(consumers) = self.consumers.get(pipeline.index()) else {
            return events;
        };
        for dependency in consumers {
            let consumer = dependency.consumer.index();
            if let Some(remaining) = self.remaining.get_mut(consumer) {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    self.ready[consumer] = true;
                    events.push(PipelineGateEvent {
                        pipeline: dependency.consumer,
                        dependency: dependency.kind,
                    });
                }
            }
        }
        events
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineGateEvent {
    pub pipeline: PipelineId,
    pub dependency: DependencyKind,
}

#[derive(Debug, Default)]
pub struct SideEffectOrderGate {
    ordered: Vec<PipelineId>,
    positions: HashMap<PipelineId, usize>,
    finished: Vec<bool>,
}

impl SideEffectOrderGate {
    pub fn from_dependencies(dependencies: &[PipelineDependency]) -> Self {
        let mut next_by_producer = HashMap::<PipelineId, PipelineId>::new();
        let mut has_predecessor = HashMap::<PipelineId, bool>::new();
        for dep in dependencies {
            if dep.kind != DependencyKind::SideEffectOrder {
                continue;
            }
            has_predecessor.entry(dep.producer).or_insert(false);
            has_predecessor.insert(dep.consumer, true);
            next_by_producer.insert(dep.producer, dep.consumer);
        }
        if has_predecessor.is_empty() {
            return Self::default();
        }

        let start = has_predecessor
            .iter()
            .find_map(|(id, has_prev)| (!*has_prev).then_some(*id))
            .unwrap_or_else(|| *has_predecessor.keys().min().expect("side effects exist"));

        let mut ordered = Vec::with_capacity(has_predecessor.len());
        let mut current = Some(start);
        while let Some(pipeline) = current {
            ordered.push(pipeline);
            current = next_by_producer.remove(&pipeline);
        }

        let positions = ordered
            .iter()
            .enumerate()
            .map(|(idx, pipeline)| (*pipeline, idx))
            .collect::<HashMap<_, _>>();
        let finished = vec![false; ordered.len()];
        Self {
            ordered,
            positions,
            finished,
        }
    }

    pub fn can_start(&self, pipeline: PipelineId) -> bool {
        let Some(position) = self.positions.get(&pipeline).copied() else {
            return true;
        };
        self.finished[..position].iter().all(|finished| *finished)
    }

    pub fn mark_finished(&mut self, pipeline: PipelineId) {
        if let Some(position) = self.positions.get(&pipeline).copied() {
            self.finished[position] = true;
        }
    }

    pub fn ordered(&self) -> &[PipelineId] {
        &self.ordered
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_context::TestStatementContextBuilder;

    use crate::memory_runtime::QueryMemoryPool;
    use crate::physical::properties::PipelineProperties;
    use crate::physical::row_type::RowType;
    use crate::physical::specs::DummyScanSpec;
    use crate::pipeline::graph::{
        ClientResultSpec, ControlRegion, PipelineRoot, PipelineSpec, SinkSpec, SourceSpec,
    };
    use crate::pipeline::handles::{BreakerHandleCatalogBuilder, BreakerHandleKind};
    use crate::pipeline::program::PipelineProgramBuilder;
    use crate::runtime::{ParameterBindings, QueryOutputPort, SharedSinkMergeEvent};

    use super::*;

    fn row_type() -> RowType {
        RowType::new(vec!["v".to_string()], vec![LogicalType::Integer])
    }

    fn simple_pipeline(id: usize) -> PipelineSpec {
        PipelineSpec {
            id: PipelineId::new(id),
            source: SourceSpec::Dummy(DummyScanSpec),
            transforms: Vec::new(),
            sink: SinkSpec::ClientResult(ClientResultSpec::default()),
            sink_sharing: SinkSharing::Exclusive,
            properties: PipelineProperties::default(),
            output: RowType::new(Vec::new(), Vec::<LogicalType>::new()),
        }
    }

    fn query_context() -> QueryRuntimeContext {
        QueryRuntimeContext::new(
            TestStatementContextBuilder::minimal().build(),
            Arc::new(ParameterBindings::empty()),
            Arc::new(QueryMemoryPool::unbounded()),
            QueryOutputPort::unbounded(),
        )
    }

    fn one_row_chunk(value: i32) -> paro_common::chunk::Chunk {
        paro_common::test_utils::test_chunk_from_vectors(vec![Vector::try_from_i32(
            &[value],
            paro_common::test_utils::test_allocator(),
        )
        .expect("vector")])
    }

    #[test]
    fn recursive_cte_controller_moves_delta_between_handles_without_relowering() {
        let mut handles = BreakerHandleCatalogBuilder::default();
        let working = handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type(),
            PipelineProperties::default(),
        );
        let intermediate = handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type(),
            PipelineProperties::default(),
        );
        let accumulated = handles.register(
            BreakerHandleKind::RecursiveTable,
            row_type(),
            PipelineProperties::default(),
        );
        let graph = PipelineGraph {
            pipelines: vec![simple_pipeline(0), simple_pipeline(1), simple_pipeline(2)],
            dependencies: vec![
                PipelineDependency {
                    producer: PipelineId::new(0),
                    consumer: PipelineId::new(1),
                    kind: DependencyKind::LoopEntry(ControlRegionId::new(0)),
                },
                PipelineDependency {
                    producer: PipelineId::new(1),
                    consumer: PipelineId::new(1),
                    kind: DependencyKind::LoopBack(ControlRegionId::new(0)),
                },
            ],
            handles: handles.finish(),
            control_regions: vec![ControlRegion::RecursiveCte(RecursiveCteRegion {
                anchor: PipelineId::new(0),
                recursive: vec![PipelineId::new(1)],
                emit: PipelineId::new(2),
                working,
                intermediate,
                accumulated: Some(accumulated),
                termination: RecursiveTermination::UntilEmpty,
                dedup: RecursiveCteDedup::None,
            })],
            root: PipelineRoot::ControlRegion(ControlRegionId::new(0)),
        };
        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("program set");
        let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).unwrap());
        let query = query_context();
        let shared_sinks = SharedSinkRuntimeSet::from_graph(&graph).unwrap();
        let mut controller = RecursiveCteControllerState::from_region(
            ControlRegionId::new(0),
            match &graph.control_regions[0] {
                ControlRegion::RecursiveCte(region) => region,
                _ => unreachable!(),
            },
            &programs,
            registry,
            query.params.clone(),
            &query,
            &shared_sinks,
        )
        .expect("controller");

        let mut delta = vec![one_row_chunk(7)];
        controller.persistent.intermediate.append_chunks(&mut delta);
        controller.start_anchor().expect("anchor starts");
        let action = controller.finish_anchor().expect("anchor finish");

        assert_eq!(
            action,
            RecursiveCteGateAction::RunPipelines(vec![PipelineId::new(1)].into_boxed_slice())
        );
        assert_eq!(controller.phase, RecursiveCtePhase::LoopEntry);
        assert_eq!(controller.persistent.iteration, 1);
        assert_eq!(controller.persistent.working.row_count(), 1);
        assert_eq!(controller.persistent.intermediate.row_count(), 0);
        assert_eq!(
            controller
                .persistent
                .accumulated
                .as_ref()
                .expect("accumulated")
                .row_count(),
            1
        );

        controller
            .start_recursive_iteration(&query)
            .expect("recursive runtime");
        let mut next_delta = vec![one_row_chunk(8)];
        controller
            .persistent
            .intermediate
            .append_chunks(&mut next_delta);
        let action = controller
            .finish_recursive_iteration()
            .expect("non-empty recursive iteration loops");
        assert_eq!(
            action,
            RecursiveCteGateAction::RunPipelines(vec![PipelineId::new(1)].into_boxed_slice())
        );
        assert_eq!(controller.phase, RecursiveCtePhase::LoopEntry);
        assert_eq!(controller.persistent.iteration, 2);
        assert_eq!(controller.persistent.working.row_count(), 1);
        assert_eq!(controller.persistent.intermediate.row_count(), 0);
        assert_eq!(
            controller
                .persistent
                .accumulated
                .as_ref()
                .expect("accumulated")
                .row_count(),
            2
        );

        controller
            .start_recursive_iteration(&query)
            .expect("final recursive runtime");
        let action = controller
            .finish_recursive_iteration()
            .expect("empty recursive iteration emits");
        assert_eq!(
            action,
            RecursiveCteGateAction::RunPipeline(PipelineId::new(2))
        );
        assert_eq!(controller.phase, RecursiveCtePhase::EmitReady);
    }

    #[test]
    fn correlated_subquery_controller_seals_delim_before_dependents() {
        let mut handles = BreakerHandleCatalogBuilder::default();
        let delim = handles.register(
            BreakerHandleKind::Delim,
            row_type(),
            PipelineProperties::default(),
        );
        let cached_outer = handles.register(
            BreakerHandleKind::Delim,
            row_type(),
            PipelineProperties::default(),
        );
        let graph = PipelineGraph {
            pipelines: vec![simple_pipeline(0), simple_pipeline(1), simple_pipeline(2)],
            dependencies: Vec::new(),
            handles: handles.finish(),
            control_regions: vec![ControlRegion::CorrelatedSubquery(
                CorrelatedSubqueryRegion {
                    side: DelimJoinSide::Left,
                    capture: PipelineId::new(0),
                    dependent_roots: vec![PipelineSubgraphRoot::Pipeline(PipelineId::new(1))],
                    join: PipelineId::new(2),
                    delim_values: delim,
                    cached_outer: Some(cached_outer),
                },
            )],
            root: PipelineRoot::ControlRegion(ControlRegionId::new(0)),
        };
        let programs = PipelineProgramBuilder::default()
            .build_program_set(&graph)
            .expect("program set");
        let registry = Arc::new(BreakerHandleRegistry::from_catalog(&graph.handles).unwrap());
        let query = query_context();
        let shared_sinks = SharedSinkRuntimeSet::from_graph(&graph).unwrap();
        let mut controller = CorrelatedSubqueryControllerState::from_region(
            ControlRegionId::new(0),
            match &graph.control_regions[0] {
                ControlRegion::CorrelatedSubquery(region) => region,
                _ => unreachable!(),
            },
            &programs,
            registry,
            query.params.clone(),
            &query,
            &shared_sinks,
        )
        .expect("controller");

        assert_eq!(controller.start_capture().unwrap(), PipelineId::new(0));
        let dependents = controller.finish_capture().unwrap();
        assert_eq!(
            dependents.as_ref(),
            &[PipelineSubgraphRoot::Pipeline(PipelineId::new(1))]
        );
        assert!(controller.delim_values.is_capture_sealed());
        assert!(controller
            .cached_outer
            .as_ref()
            .expect("cached outer")
            .is_capture_sealed());
        assert_eq!(controller.finish_dependents().unwrap(), PipelineId::new(2));
        assert_eq!(controller.start_join().unwrap(), PipelineId::new(2));
        controller.finish_join().unwrap();
        assert_eq!(controller.phase, CorrelatedSubqueryPhase::Done);
    }

    #[test]
    fn shared_sink_runtime_set_freezes_registered_producers() {
        let shared = SharedSinkId::new(0);
        let mut first = simple_pipeline(0);
        first.sink_sharing = SinkSharing::Shared(shared);
        let mut second = simple_pipeline(1);
        second.sink_sharing = SinkSharing::Shared(shared);
        let graph = PipelineGraph {
            pipelines: vec![first, second],
            dependencies: Vec::new(),
            handles: BreakerHandleCatalogBuilder::default().finish(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(PipelineId::new(0)),
        };

        let set = SharedSinkRuntimeSet::from_graph(&graph).expect("shared sinks");
        let coordinator = set.get(shared).expect("coordinator");
        assert_eq!(coordinator.frozen_producer_count(), Some(2));
        assert_eq!(
            coordinator.mark_producer_merged().unwrap(),
            SharedSinkMergeEvent::WaitingForProducers { remaining: 1 }
        );
        assert_eq!(
            coordinator.mark_producer_merged().unwrap(),
            SharedSinkMergeEvent::ReadyToFinish
        );
        assert!(coordinator.try_begin_finish().unwrap());
    }

    #[test]
    fn side_effect_order_gate_serializes_pipeline_start() {
        let dependencies = vec![
            PipelineDependency {
                producer: PipelineId::new(0),
                consumer: PipelineId::new(1),
                kind: DependencyKind::SideEffectOrder,
            },
            PipelineDependency {
                producer: PipelineId::new(1),
                consumer: PipelineId::new(2),
                kind: DependencyKind::SideEffectOrder,
            },
        ];
        let gate = SideEffectOrderGate::from_dependencies(&dependencies);
        assert_eq!(
            gate.ordered(),
            &[PipelineId::new(0), PipelineId::new(1), PipelineId::new(2)]
        );
        assert!(gate.can_start(PipelineId::new(0)));
        assert!(!gate.can_start(PipelineId::new(1)));

        let graph = PipelineGraph {
            pipelines: vec![simple_pipeline(0), simple_pipeline(1), simple_pipeline(2)],
            dependencies,
            handles: BreakerHandleCatalogBuilder::default().finish(),
            control_regions: Vec::new(),
            root: PipelineRoot::Pipeline(PipelineId::new(2)),
        };
        let mut gates = PipelineDependencyGates::from_graph(&graph);
        assert!(gates.is_ready(PipelineId::new(0)));
        assert!(!gates.is_ready(PipelineId::new(1)));
        assert_eq!(gates.ready_pipelines(), vec![PipelineId::new(0)]);
        let events = gates.mark_finished(PipelineId::new(0));
        assert_eq!(events[0].pipeline, PipelineId::new(1));
        assert!(gates.is_ready(PipelineId::new(1)));
        assert_eq!(
            gates
                .ready_pipelines()
                .into_iter()
                .filter(|pipeline| *pipeline == PipelineId::new(1))
                .collect::<Vec<_>>(),
            vec![PipelineId::new(1)]
        );
    }
}
