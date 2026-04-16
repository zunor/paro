// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query executor that builds, schedules, and coordinates pipelines.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use paro_common::allocator::{BufferAllocator, MemoryTag};
use paro_common::error::{self as paro_error, Result};
use paro_common::logging::targets;
use paro_common::types::LogicalType;
use paro_context::StatementContext;

use crate::execution_context::ExecutionContext;
use crate::operator::result::result_collector::PhysicalResultCollector;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::pipeline::events::complete::PipelineCompleteEventBuilder;
use crate::pipeline::events::execute::PipelineEvent;
use crate::pipeline::events::finish::{PipelineFinishEvent, PipelineFinishTask};
use crate::pipeline::events::hash_join_finalize::{HashJoinFinalizeEvent, HashJoinFinalizeTask};
use crate::pipeline::events::initialize::{PipelineInitializeEvent, PipelineInitializeTask};
use crate::pipeline::events::prepare_finish::{
    PipelinePrepareFinishEvent, PipelinePrepareFinishTask,
};
use crate::pipeline::meta_pipeline::{MetaPipeline, MetaPipelineType};
use crate::pipeline::pipeline::Pipeline;
use crate::pipeline::scheduler::PipelineEventStack;
use crate::query_executor::compiled::CompiledStatement;

use paro_scheduler::coordinator::EventCoordinator;
use paro_scheduler::event::Event;
use paro_scheduler::scheduler::TaskScheduler;

use super::result::ExecutorConfig;
use super::stream::ResultHandler;
use std::time::Instant;
use tracing::debug;

struct ScheduledPipeline {
    pipeline: Arc<Pipeline>,
    meta_key: usize,
    stack: PipelineEventStack,
}

#[inline]
fn pipeline_key(pipeline: &Arc<Pipeline>) -> usize {
    Arc::as_ptr(pipeline) as usize
}

#[inline]
fn meta_pipeline_key(meta_pipeline: &Arc<MetaPipeline>) -> usize {
    Arc::as_ptr(meta_pipeline) as usize
}

#[inline]
fn event_key(event: &Arc<paro_scheduler::event::Event>) -> usize {
    Arc::as_ptr(event) as usize
}

#[inline]
fn task_key(task: &Arc<Mutex<dyn paro_scheduler::task::Task>>) -> usize {
    Arc::as_ptr(task) as *const () as usize
}

#[inline]
fn pipeline_needs_hash_join_finalize_event(pipeline: &Arc<Pipeline>) -> bool {
    pipeline
        .get_sink()
        .is_some_and(|sink| matches!(sink.operator_type(), PhysicalOperatorType::HashJoin))
}

#[cfg(debug_assertions)]
type EventDependencyGraph = HashMap<usize, Vec<usize>>;
#[cfg(not(debug_assertions))]
type EventDependencyGraph = ();

#[cfg(debug_assertions)]
fn new_dependency_graph() -> EventDependencyGraph {
    HashMap::new()
}

#[cfg(not(debug_assertions))]
fn new_dependency_graph() -> EventDependencyGraph {}

fn add_event_dependency(
    dependent: &Arc<Event>,
    dependency: &Arc<Event>,
    _dependency_graph: &mut EventDependencyGraph,
) {
    dependent.add_dependency(dependency);
    #[cfg(debug_assertions)]
    {
        _dependency_graph
            .entry(event_key(dependent))
            .or_default()
            .push(event_key(dependency));
    }
}

#[cfg(debug_assertions)]
fn verify_scheduled_events(
    events: &[Arc<Event>],
    dependency_graph: &EventDependencyGraph,
) -> Result<()> {
    let mut vertices: Vec<Arc<Event>> = Vec::new();
    let mut vertex_map: HashMap<usize, usize> = HashMap::new();

    for event in events {
        let key = event_key(event);
        if vertex_map.contains_key(&key) {
            continue;
        }
        vertex_map.insert(key, vertices.len());
        vertices.push(event.clone());
    }

    let mut visited = vec![false; vertices.len()];
    let mut recursion_stack = vec![false; vertices.len()];
    for vertex in 0..vertices.len() {
        verify_scheduled_events_internal(
            vertex,
            &vertices,
            &vertex_map,
            dependency_graph,
            &mut visited,
            &mut recursion_stack,
        )?;
    }

    Ok(())
}

#[cfg(debug_assertions)]
fn verify_scheduled_events_internal(
    vertex: usize,
    vertices: &[Arc<Event>],
    vertex_map: &HashMap<usize, usize>,
    dependency_graph: &EventDependencyGraph,
    visited: &mut [bool],
    recursion_stack: &mut [bool],
) -> Result<()> {
    if recursion_stack[vertex] {
        return Err(paro_error::internal(
            "Circular dependency detected in scheduled event DAG".to_string(),
        ));
    }
    if visited[vertex] {
        return Ok(());
    }

    visited[vertex] = true;
    recursion_stack[vertex] = true;

    let vertex_key = event_key(&vertices[vertex]);
    if let Some(adjacent) = dependency_graph.get(&vertex_key) {
        for dependency_key in adjacent {
            let Some(&dependency_idx) = vertex_map.get(dependency_key) else {
                return Err(paro_error::internal(format!(
                    "Event dependency {} was not found in scheduled events",
                    dependency_key
                )));
            };
            verify_scheduled_events_internal(
                dependency_idx,
                vertices,
                vertex_map,
                dependency_graph,
                visited,
                recursion_stack,
            )?;
        }
    }

    recursion_stack[vertex] = false;
    Ok(())
}

#[cfg(not(debug_assertions))]
fn verify_scheduled_events(
    _events: &[Arc<Event>],
    _dependency_graph: &EventDependencyGraph,
) -> Result<()> {
    Ok(())
}
///
///
/// Executor holds a StatementContext Arc to avoid lifetime pollution.
pub struct Executor {
    /// Session context for accessing cluster and other resources.
    session: Arc<StatementContext>,
    /// Executor configuration.
    config: ExecutorConfig,
    /// Blocked tasks awaiting explicit reschedule.
    to_be_rescheduled_tasks: Mutex<HashMap<usize, Arc<Mutex<dyn paro_scheduler::task::Task>>>>,
}

impl Executor {
    /// Create a new Executor for a query.
    ///
    pub fn new(session: Arc<StatementContext>) -> Self {
        let max_threads = session
            .number_of_threads()
            .min(session.scheduler().number_of_threads().max(1) as usize)
            .max(1);
        Self {
            session,
            config: ExecutorConfig { max_threads },
            to_be_rescheduled_tasks: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new Executor with custom configuration.
    pub fn with_config(session: Arc<StatementContext>, config: ExecutorConfig) -> Self {
        Self {
            session,
            config,
            to_be_rescheduled_tasks: Mutex::new(HashMap::new()),
        }
    }

    /// Get the session context.
    ///
    pub fn session_context(&self) -> &StatementContext {
        self.session.as_ref()
    }

    /// Get the task scheduler.
    #[inline]
    pub fn task_scheduler(&self) -> &Arc<TaskScheduler> {
        self.session.scheduler()
    }

    /// Execute a physical plan and return a streaming result handler.
    ///
    /// Pipelines are started without waiting for completion.
    /// `ResultHandler` drives progress as chunks are fetched.
    pub fn execute(&self, compiled: CompiledStatement) -> Result<ResultHandler> {
        let result_names = compiled.result_names();
        let result_types = compiled.result_types();
        let physical_plan = compiled.physical_plan;
        let is_query = !result_names.is_empty();
        let started_at = Instant::now();
        debug!(
            target: targets::EXECUTOR,
            is_query,
            result_columns = result_names.len(),
            "Execution started"
        );

        // Get allocator from session's buffer pool for memory management
        let allocator = Arc::new(BufferAllocator::new(
            self.session.buffer_pool().clone(),
            MemoryTag::Allocator,
        )) as Arc<dyn paro_common::allocator::Allocator>;

        // Create result collector
        let collector_types = if is_query {
            result_types.clone()
        } else {
            // For DML operations (INSERT/UPDATE/DELETE), the result is a single BIGINT (row count)
            vec![LogicalType::BigInt]
        };
        let (collector, buffer) =
            PhysicalResultCollector::with_shared_result(collector_types, allocator.clone());
        let collector = Arc::new(collector);

        // Build pipelines
        let pipelines = self.build_pipelines(physical_plan, collector)?;

        // Start pipelines (non-blocking)
        let coordinator = self.start_pipelines(pipelines)?;
        self.session
            .bind_execution_coordinator(Arc::clone(&coordinator));

        // Return streaming result handler with buffer and coordinator
        let handler = ResultHandler::new(
            result_names,
            result_types,
            buffer,
            coordinator,
            self.session.cancellation.clone(),
            allocator,
        );
        debug!(
            target: targets::EXECUTOR,
            is_query,
            elapsed_ms = started_at.elapsed().as_millis(),
            "Execution pipelines started"
        );
        Ok(handler)
    }

    /// Build pipelines from a physical plan using the meta-pipeline graph.
    fn build_pipelines(
        &self,
        physical_plan: Arc<dyn PhysicalOperator>,
        result_collector: Arc<PhysicalResultCollector>,
    ) -> Result<Vec<Arc<MetaPipeline>>> {
        use crate::pipeline::build_state::PipelineBuildState;

        // Determine the actual sink for the root MetaPipeline
        // If the root operator is a sink-only operator (like INSERT), use it as the sink
        // Otherwise, use the result collector
        let (root_sink, build_from): (Arc<dyn PhysicalOperator>, Arc<dyn PhysicalOperator>) =
            if physical_plan.is_sink() && !physical_plan.is_source() {
                // Root is a sink-only operator (e.g., INSERT)
                // Use it as the sink and build from its child
                let child = physical_plan.child_arc(0).ok_or_else(|| {
                    paro_error::internal("Sink-only operator has no child".to_string())
                })?;
                (physical_plan.clone(), child)
            } else {
                // Root is not a sink-only operator, use result collector
                (
                    result_collector as Arc<dyn PhysicalOperator>,
                    physical_plan.clone(),
                )
            };

        // Create root MetaPipeline with the determined sink
        let root_meta = MetaPipeline::new(Some(root_sink), MetaPipelineType::Regular);

        // Build the pipeline tree
        let mut state = PipelineBuildState::new();
        root_meta.build(&build_from, &mut state);

        // Mark all pipelines as ready (wires dependencies)
        root_meta.ready();

        // Keep the full MetaPipeline graph for scheduling.
        Ok(root_meta.get_meta_pipelines_recursive(true))
    }

    /// Reschedule an existing set of MetaPipelines and return a fresh coordinator.
    ///
    /// This is used by recursive CTE execution to repeatedly run the same
    /// recursive branch pipelines after updating the working table.
    pub fn reschedule_pipelines(
        &self,
        meta_pipelines: Vec<Arc<MetaPipeline>>,
    ) -> Result<Arc<EventCoordinator>> {
        self.start_pipelines(meta_pipelines)
    }

    /// Execute an existing set of MetaPipelines to completion.
    ///
    /// Unlike `execute()`, this does not rebuild pipelines from a physical plan.
    /// It reuses already-built MetaPipeline graphs.
    pub fn execute_meta_pipelines_blocking(
        &self,
        meta_pipelines: Vec<Arc<MetaPipeline>>,
    ) -> Result<()> {
        let coordinator = self.start_pipelines(meta_pipelines)?;
        coordinator.wait_for_completion()?;
        Ok(())
    }

    /// Start pipeline execution and return immediately.
    ///
    /// The returned coordinator can be used to inspect status, cancel execution,
    /// or wait for completion.
    fn start_pipelines(
        &self,
        meta_pipelines: Vec<Arc<MetaPipeline>>,
    ) -> Result<Arc<EventCoordinator>> {
        if meta_pipelines.is_empty() {
            // Return a completed coordinator for empty pipelines
            let scheduler = self.task_scheduler();
            let coordinator = EventCoordinator::new(scheduler.clone());
            return Ok(Arc::new(coordinator));
        }

        self.to_be_rescheduled_tasks.lock().clear();
        debug!(
            target: targets::PIPELINE,
            pipeline_count = meta_pipelines.len(),
            max_threads = self.config.max_threads,
            "Pipeline scheduling started"
        );

        let scheduler = self.task_scheduler();
        let producer = scheduler.create_producer();

        // For now, use single-threaded context. In parallel execution,
        // each PipelineTask will create its own ThreadContext.
        let thread_ctx = crate::thread_context::ThreadContext::single_threaded();

        // Create ExecutionContext for pipeline scheduling
        // Note: pipeline field is None here, will be set in PipelineExecutor
        let exec_ctx = ExecutionContext::new(self.session.clone(), &thread_ctx, None);

        let scheduled = self.schedule_events_internal(&meta_pipelines, &exec_ctx)?;
        debug!(
            target: targets::PIPELINE,
            pipeline_count = scheduled.len(),
            "Pipeline events scheduled"
        );

        // Create event coordinator
        let coordinator = Arc::new(EventCoordinator::with_producer(
            scheduler.clone(),
            producer.clone(),
        ));
        let mut registered_events = HashSet::new();
        for scheduled_pipeline in &scheduled {
            for event in scheduled_pipeline.stack.all_events() {
                if registered_events.insert(event_key(&event)) {
                    coordinator.add_event(event);
                }
            }
        }

        // Set up schedule callbacks for each event stack.
        let mut configured_execute_events = HashSet::new();
        let mut configured_prepare_events = HashSet::new();
        let mut configured_hash_finalize_events = HashSet::new();
        let mut configured_finish_events = HashSet::new();
        let mut configured_complete_events = HashSet::new();
        for scheduled_pipeline in &scheduled {
            let stack = &scheduled_pipeline.stack;

            // Execute event: creates PipelineTasks via Pipeline::schedule()
            let exec_event = stack.execute_event.clone();
            let exec_inner_event = stack.execute_event.event().clone();
            if configured_execute_events.insert(event_key(&exec_inner_event)) {
                let scheduler_clone = scheduler.clone();
                let producer_clone = producer.clone();
                exec_inner_event.set_schedule_callback(move || {
                    exec_event.schedule_with_producer(&scheduler_clone, &producer_clone)
                });
            }

            // Prepare finish event: creates PipelinePrepareFinishTask
            let prep_finish_event = stack.prepare_finish_event.clone();
            let prep_finish_inner_event = stack.prepare_finish_event.event().clone();
            if configured_prepare_events.insert(event_key(&prep_finish_inner_event)) {
                let scheduler_clone = scheduler.clone();
                let producer_clone = producer.clone();
                prep_finish_inner_event.set_schedule_callback(move || {
                    let task = PipelinePrepareFinishTask::new(prep_finish_event.clone());
                    let tasks: Vec<Arc<Mutex<dyn paro_scheduler::task::Task>>> =
                        vec![Arc::new(Mutex::new(task))];
                    prep_finish_event
                        .event()
                        .schedule_tasks_to_scheduler_with_producer(
                            tasks,
                            &scheduler_clone,
                            &producer_clone,
                        );
                    Ok(())
                });
            }

            // Hash-join finalize event: creates HashJoinFinalizeTask
            if let Some(hash_finalize_event) = stack.hash_join_finalize_event.clone() {
                let hash_finalize_inner_event = hash_finalize_event.event().clone();
                if configured_hash_finalize_events.insert(event_key(&hash_finalize_inner_event)) {
                    let scheduler_clone = scheduler.clone();
                    let producer_clone = producer.clone();
                    hash_finalize_inner_event.set_schedule_callback(move || {
                        let task = HashJoinFinalizeTask::new(hash_finalize_event.clone());
                        let tasks: Vec<Arc<Mutex<dyn paro_scheduler::task::Task>>> =
                            vec![Arc::new(Mutex::new(task))];
                        hash_finalize_event
                            .event()
                            .schedule_tasks_to_scheduler_with_producer(
                                tasks,
                                &scheduler_clone,
                                &producer_clone,
                            );
                        Ok(())
                    });
                }
            }

            // Finish event: creates PipelineFinishTask
            let finish_event = stack.finish_event.clone();
            let finish_inner_event = stack.finish_event.event().clone();
            if configured_finish_events.insert(event_key(&finish_inner_event)) {
                let scheduler_clone = scheduler.clone();
                let producer_clone = producer.clone();
                finish_inner_event.set_schedule_callback(move || {
                    let task = PipelineFinishTask::new(finish_event.clone());
                    let tasks: Vec<Arc<Mutex<dyn paro_scheduler::task::Task>>> =
                        vec![Arc::new(Mutex::new(task))];
                    finish_event
                        .event()
                        .schedule_tasks_to_scheduler_with_producer(
                            tasks,
                            &scheduler_clone,
                            &producer_clone,
                        );
                    Ok(())
                });
            }

            // Complete event: just signals completion
            let complete_event = stack.complete_event.clone();
            let complete_inner_event = stack.complete_event.event().clone();
            if configured_complete_events.insert(event_key(&complete_inner_event)) {
                complete_inner_event.set_schedule_callback(move || {
                    complete_event.mark_complete();
                    Ok(())
                });
            }
        }

        // Install init callbacks for all pipelines before root kickoff. This keeps
        // dependent init events safe from fast upstream completion races while also
        // allowing root init events to use the same activation path.
        let mut configured_init_events = HashSet::new();
        for scheduled_pipeline in &scheduled {
            let init_event = scheduled_pipeline.stack.initialize_event.clone();
            let init_inner_event = init_event.event().clone();
            if !configured_init_events.insert(event_key(&init_inner_event)) {
                continue;
            }

            let scheduler_clone = scheduler.clone();
            let init_event_clone = init_event.clone();
            let producer_clone = producer.clone();
            init_inner_event.set_schedule_callback(move || {
                let task = PipelineInitializeTask::new(init_event_clone.clone());
                let tasks: Vec<Arc<Mutex<dyn paro_scheduler::task::Task>>> =
                    vec![Arc::new(Mutex::new(task))];
                init_event_clone
                    .event()
                    .schedule_tasks_to_scheduler_with_producer(
                        tasks,
                        &scheduler_clone,
                        &producer_clone,
                    );
                Ok(())
            });
        }

        coordinator.activate_ready_events()?;

        debug!(
            target: targets::PIPELINE,
            pipeline_count = scheduled.len(),
            "Pipeline scheduling completed"
        );
        Ok(coordinator)
    }

    /// Build event DAG from MetaPipeline graph and apply cross-pipeline ordering.
    ///
    /// - Keep MetaPipeline structure during scheduling
    /// - Add cross-Meta dependencies through complete events
    /// - Add explicit MetaPipeline internal dependencies through execute events
    /// - Add JoinBuild sibling ordering for PrepareFinalize/Finalize
    fn schedule_events_internal(
        &self,
        meta_pipelines: &[Arc<MetaPipeline>],
        exec_ctx: &ExecutionContext,
    ) -> Result<Vec<ScheduledPipeline>> {
        let mut scheduled = Vec::new();
        let mut pipeline_to_entry: HashMap<usize, usize> = HashMap::new();
        let mut pipeline_to_meta: HashMap<usize, usize> = HashMap::new();
        let mut meta_to_base_entry: HashMap<usize, usize> = HashMap::new();
        let mut dependency_graph = new_dependency_graph();
        debug!(
            target: targets::PIPELINE,
            meta_pipeline_count = meta_pipelines.len(),
            "Pipeline event DAG construction started"
        );

        // 1) Create event stacks for every pipeline while keeping MetaPipeline ownership.
        for meta_pipeline in meta_pipelines {
            let meta_key = meta_pipeline_key(meta_pipeline);
            let pipelines = meta_pipeline.pipelines();
            if pipelines.is_empty() {
                continue;
            }

            let base_pipeline = pipelines[0].clone();
            base_pipeline.initialize(exec_ctx)?;
            let base_global_states = base_pipeline.get_global_states().ok_or_else(|| {
                paro_error::internal("Pipeline should be initialized".to_string())
            })?;

            // The initialize task only materializes the base sink state; each pipeline
            // will populate its own source/operator/sink states when scheduled.
            let base_initialize_event = PipelineInitializeEvent::new(base_pipeline.clone());
            let base_execute_event =
                PipelineEvent::new(base_pipeline.clone(), self.config.max_threads);
            let base_prepare_finish_event =
                PipelinePrepareFinishEvent::new(base_pipeline.clone(), base_global_states.clone());
            let base_hash_join_finalize_event =
                pipeline_needs_hash_join_finalize_event(&base_pipeline).then(|| {
                    HashJoinFinalizeEvent::new(base_pipeline.clone(), base_global_states.clone())
                });
            let base_finish_event =
                PipelineFinishEvent::new(base_pipeline.clone(), base_global_states);
            let base_complete_event = PipelineCompleteEventBuilder::new()
                .complete_pipeline(true)
                .build();

            add_event_dependency(
                base_execute_event.event(),
                base_initialize_event.event(),
                &mut dependency_graph,
            );
            add_event_dependency(
                base_prepare_finish_event.event(),
                base_execute_event.event(),
                &mut dependency_graph,
            );
            if let Some(hash_finalize_event) = &base_hash_join_finalize_event {
                add_event_dependency(
                    hash_finalize_event.event(),
                    base_prepare_finish_event.event(),
                    &mut dependency_graph,
                );
                add_event_dependency(
                    base_finish_event.event(),
                    hash_finalize_event.event(),
                    &mut dependency_graph,
                );
            } else {
                add_event_dependency(
                    base_finish_event.event(),
                    base_prepare_finish_event.event(),
                    &mut dependency_graph,
                );
            }
            add_event_dependency(
                base_complete_event.event(),
                base_finish_event.event(),
                &mut dependency_graph,
            );

            let base_entry_idx = scheduled.len();
            let base_stack = PipelineEventStack {
                initialize_event: base_initialize_event.clone(),
                execute_event: base_execute_event.clone(),
                prepare_finish_event: base_prepare_finish_event.clone(),
                hash_join_finalize_event: base_hash_join_finalize_event.clone(),
                finish_event: base_finish_event.clone(),
                complete_event: base_complete_event.clone(),
            };

            scheduled.push(ScheduledPipeline {
                pipeline: base_pipeline.clone(),
                meta_key,
                stack: base_stack,
            });
            pipeline_to_entry.insert(pipeline_key(&base_pipeline), base_entry_idx);
            pipeline_to_meta.insert(pipeline_key(&base_pipeline), meta_key);
            meta_to_base_entry.insert(meta_key, base_entry_idx);

            for pipeline in pipelines.into_iter().skip(1) {
                // Non-base pipelines only allocate empty PipelineGlobalStates here.
                // Full reset() happens per-pipeline inside Pipeline::schedule().
                pipeline.initialize(exec_ctx)?;

                let execute_event = PipelineEvent::new(pipeline.clone(), self.config.max_threads);
                let stack = if let Some(finish_group) = meta_pipeline.get_finish_group(&pipeline) {
                    let Some(group_idx) =
                        pipeline_to_entry.get(&pipeline_key(&finish_group)).copied()
                    else {
                        return Err(paro_error::internal(
                            "Finish group root pipeline has not been scheduled".to_string(),
                        ));
                    };
                    let group_stack = &scheduled[group_idx].stack;

                    add_event_dependency(
                        execute_event.event(),
                        base_finish_event.event(),
                        &mut dependency_graph,
                    );
                    add_event_dependency(
                        group_stack.prepare_finish_event.event(),
                        execute_event.event(),
                        &mut dependency_graph,
                    );

                    PipelineEventStack {
                        initialize_event: base_initialize_event.clone(),
                        execute_event,
                        prepare_finish_event: group_stack.prepare_finish_event.clone(),
                        hash_join_finalize_event: group_stack.hash_join_finalize_event.clone(),
                        finish_event: group_stack.finish_event.clone(),
                        complete_event: base_complete_event.clone(),
                    }
                } else if meta_pipeline.has_finish_event(&pipeline) {
                    let global_states = pipeline.get_global_states().ok_or_else(|| {
                        paro_error::internal("Pipeline should be initialized".to_string())
                    })?;
                    let prepare_finish_event =
                        PipelinePrepareFinishEvent::new(pipeline.clone(), global_states.clone());
                    let hash_join_finalize_event =
                        pipeline_needs_hash_join_finalize_event(&pipeline).then(|| {
                            HashJoinFinalizeEvent::new(pipeline.clone(), global_states.clone())
                        });
                    let finish_event = PipelineFinishEvent::new(pipeline.clone(), global_states);

                    add_event_dependency(
                        execute_event.event(),
                        base_finish_event.event(),
                        &mut dependency_graph,
                    );
                    add_event_dependency(
                        prepare_finish_event.event(),
                        execute_event.event(),
                        &mut dependency_graph,
                    );
                    if let Some(hash_finalize_event) = &hash_join_finalize_event {
                        add_event_dependency(
                            hash_finalize_event.event(),
                            prepare_finish_event.event(),
                            &mut dependency_graph,
                        );
                        add_event_dependency(
                            finish_event.event(),
                            hash_finalize_event.event(),
                            &mut dependency_graph,
                        );
                    } else {
                        add_event_dependency(
                            finish_event.event(),
                            prepare_finish_event.event(),
                            &mut dependency_graph,
                        );
                    }
                    add_event_dependency(
                        base_complete_event.event(),
                        finish_event.event(),
                        &mut dependency_graph,
                    );

                    PipelineEventStack {
                        initialize_event: base_initialize_event.clone(),
                        execute_event,
                        prepare_finish_event,
                        hash_join_finalize_event,
                        finish_event,
                        complete_event: base_complete_event.clone(),
                    }
                } else {
                    add_event_dependency(
                        execute_event.event(),
                        base_initialize_event.event(),
                        &mut dependency_graph,
                    );
                    add_event_dependency(
                        base_prepare_finish_event.event(),
                        execute_event.event(),
                        &mut dependency_graph,
                    );

                    PipelineEventStack {
                        initialize_event: base_initialize_event.clone(),
                        execute_event,
                        prepare_finish_event: base_prepare_finish_event.clone(),
                        hash_join_finalize_event: base_hash_join_finalize_event.clone(),
                        finish_event: base_finish_event.clone(),
                        complete_event: base_complete_event.clone(),
                    }
                };

                let key = pipeline_key(&pipeline);
                let entry_idx = scheduled.len();
                scheduled.push(ScheduledPipeline {
                    pipeline: pipeline.clone(),
                    meta_key,
                    stack,
                });

                pipeline_to_entry.insert(key, entry_idx);
                pipeline_to_meta.insert(key, meta_key);
            }
        }

        // 2) Link cross-Meta dependencies: this pipeline waits for dependency complete.
        for entry_idx in 0..scheduled.len() {
            let pipeline = scheduled[entry_idx].pipeline.clone();
            let this_meta_key = scheduled[entry_idx].meta_key;

            for dependency in pipeline.get_dependencies() {
                let dep_key = pipeline_key(&dependency);
                let Some(dep_idx) = pipeline_to_entry.get(&dep_key).copied() else {
                    continue;
                };
                let dep_meta_key = pipeline_to_meta
                    .get(&dep_key)
                    .copied()
                    .unwrap_or(this_meta_key);
                if dep_meta_key == this_meta_key {
                    continue;
                }

                let dep_complete_event = scheduled[dep_idx].stack.complete_event.event().clone();
                add_event_dependency(
                    scheduled[entry_idx].stack.initialize_event.event(),
                    &dep_complete_event,
                    &mut dependency_graph,
                );
                add_event_dependency(
                    scheduled[entry_idx].stack.execute_event.event(),
                    &dep_complete_event,
                    &mut dependency_graph,
                );
            }
        }

        // 3) Link explicit dependencies inside each MetaPipeline.
        for meta_pipeline in meta_pipelines {
            for (pipeline, dependencies) in meta_pipeline.explicit_dependencies() {
                let Some(entry_idx) = pipeline_to_entry.get(&pipeline_key(&pipeline)).copied()
                else {
                    continue;
                };

                for dependency in dependencies {
                    let dep_key = pipeline_key(&dependency);
                    let Some(dep_idx) = pipeline_to_entry.get(&dep_key).copied() else {
                        continue;
                    };

                    let dep_execute_event = scheduled[dep_idx].stack.execute_event.event().clone();
                    // Pipelines inside the same MetaPipeline can share initialize events.
                    // Delaying initialize here can create a self-cycle for source child
                    // pipelines (e.g. RIGHT/FULL hash join source phase). What needs
                    // ordering is execution: the dependent pipeline must not start
                    // executing until the dependency pipeline has finished its own
                    // execute phase.
                    add_event_dependency(
                        scheduled[entry_idx].stack.execute_event.event(),
                        &dep_execute_event,
                        &mut dependency_graph,
                    );
                }
            }
        }

        // 4) JoinBuild ordering:
        // - all sibling JoinBuild prepare_finish wait for all sibling execute
        // - all sibling JoinBuild finish wait for all sibling prepare_finish
        for meta_pipeline in meta_pipelines {
            let children = meta_pipeline.get_meta_pipelines_recursive(false);
            for child1 in &children {
                if child1.meta_type() != MetaPipelineType::JoinBuild {
                    continue;
                }
                let Some(parent1) = child1.parent() else {
                    continue;
                };
                let Some(child1_base_idx) =
                    meta_to_base_entry.get(&meta_pipeline_key(child1)).copied()
                else {
                    continue;
                };

                for child2 in &children {
                    if child2.meta_type() != MetaPipelineType::JoinBuild {
                        continue;
                    }
                    if Arc::ptr_eq(child1, child2) {
                        continue;
                    }

                    let Some(parent2) = child2.parent() else {
                        continue;
                    };
                    if !Arc::ptr_eq(&parent1, &parent2) {
                        continue;
                    }

                    let Some(child2_base_idx) =
                        meta_to_base_entry.get(&meta_pipeline_key(child2)).copied()
                    else {
                        continue;
                    };

                    let child2_execute_event = scheduled[child2_base_idx]
                        .stack
                        .execute_event
                        .event()
                        .clone();
                    let child2_prepare_event = scheduled[child2_base_idx]
                        .stack
                        .prepare_finish_event
                        .event()
                        .clone();

                    add_event_dependency(
                        scheduled[child1_base_idx]
                            .stack
                            .prepare_finish_event
                            .event(),
                        &child2_execute_event,
                        &mut dependency_graph,
                    );
                    add_event_dependency(
                        scheduled[child1_base_idx].stack.finish_event.event(),
                        &child2_prepare_event,
                        &mut dependency_graph,
                    );
                }
            }
        }

        let mut unique_events = HashSet::new();
        let mut all_events = Vec::new();
        for scheduled_pipeline in &scheduled {
            for event in scheduled_pipeline.stack.all_events() {
                if unique_events.insert(event_key(&event)) {
                    all_events.push(event);
                }
            }
        }
        verify_scheduled_events(&all_events, &dependency_graph)?;
        debug!(
            target: targets::PIPELINE,
            scheduled_pipeline_count = scheduled.len(),
            "Pipeline event DAG construction completed"
        );

        Ok(scheduled)
    }

    /// Register a blocked task so it can be rescheduled later.
    pub fn add_to_be_rescheduled(&self, task: Arc<Mutex<dyn paro_scheduler::task::Task>>) {
        let key = task_key(&task);
        let mut blocked = self.to_be_rescheduled_tasks.lock();
        blocked.entry(key).or_insert(task);
    }

    /// Reschedule a previously blocked task.
    ///
    /// Returns `true` when the task was found and queued again.
    pub fn reschedule_task(&self, task: Arc<Mutex<dyn paro_scheduler::task::Task>>) -> bool {
        let key = task_key(&task);
        let mut blocked = self.to_be_rescheduled_tasks.lock();
        let Some(task) = blocked.remove(&key) else {
            return false;
        };
        drop(blocked);
        self.task_scheduler().schedule_task(task);
        true
    }

    /// Reschedule all blocked tasks that are currently tracked by the executor.
    ///
    /// Returns the number of tasks that were queued.
    pub fn reschedule_all_blocked_tasks(&self) -> usize {
        let tasks = {
            let mut blocked = self.to_be_rescheduled_tasks.lock();
            blocked.drain().map(|(_, task)| task).collect::<Vec<_>>()
        };
        let count = tasks.len();
        if count > 0 {
            self.task_scheduler().schedule_tasks(tasks);
        }
        count
    }

    /// Number of blocked tasks currently tracked by the executor.
    pub fn blocked_task_count(&self) -> usize {
        self.to_be_rescheduled_tasks.lock().len()
    }

    /// Get the executor configuration.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::{
        add_event_dependency, new_dependency_graph, verify_scheduled_events, EventDependencyGraph,
    };
    use paro_scheduler::event::Event;

    #[test]
    fn verify_scheduled_events_accepts_acyclic_graph() {
        let a = Event::new();
        let b = Event::new();
        let c = Event::new();

        let mut dependency_graph: EventDependencyGraph = new_dependency_graph();
        add_event_dependency(&b, &a, &mut dependency_graph);
        add_event_dependency(&c, &b, &mut dependency_graph);

        let events = vec![a, b, c];
        let result = verify_scheduled_events(&events, &dependency_graph);
        assert!(result.is_ok());
    }

    #[test]
    fn verify_scheduled_events_detects_cycle() {
        let a = Event::new();
        let b = Event::new();
        let c = Event::new();

        let mut dependency_graph: EventDependencyGraph = new_dependency_graph();
        add_event_dependency(&a, &c, &mut dependency_graph);
        add_event_dependency(&b, &a, &mut dependency_graph);
        add_event_dependency(&c, &b, &mut dependency_graph);

        let events = vec![a, b, c];
        let result = verify_scheduled_events(&events, &dependency_graph);
        #[cfg(debug_assertions)]
        assert!(result.is_err());
        #[cfg(not(debug_assertions))]
        assert!(result.is_ok());
    }
}
