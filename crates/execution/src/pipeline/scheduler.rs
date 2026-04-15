// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Schedules pipeline execution through an event chain.
//!
//! Pipeline execution is coordinated through a chain of events:
//! ```text
//! PipelineInitializeEvent (reset sink state)
//!         ↓
//! PipelineEvent (schedules PipelineTasks)
//!         ↓
//! PipelineFinishEvent (calls Finalize on sink)
//!         ↓
//! PipelineCompleteEvent (signals completion)
//! ```

use paro_scheduler::event::Event;
use std::sync::Arc;

use super::events::complete::{PipelineCompleteEvent, PipelineCompleteEventBuilder};
use super::events::execute::PipelineEvent;
use super::events::finish::PipelineFinishEvent;
use super::events::hash_join_finalize::HashJoinFinalizeEvent;
use super::events::initialize::PipelineInitializeEvent;
use super::events::prepare_finish::PipelinePrepareFinishEvent;
use super::pipeline::Pipeline;
use crate::execution_context::ExecutionContext;
use crate::operator_type::PhysicalOperatorType;
use paro_common::error::Result;

/// Holds all events for a single pipeline's execution lifecycle.
///
#[derive(Debug)]
pub struct PipelineEventStack {
    /// Event that initializes the sink state
    pub initialize_event: Arc<PipelineInitializeEvent>,
    /// Event that schedules and executes PipelineTasks
    pub execute_event: Arc<PipelineEvent>,
    /// Event that prepares the sink for finalization
    pub prepare_finish_event: Arc<PipelinePrepareFinishEvent>,
    /// Optional hash join finalize event (between prepare_finish and finish).
    pub hash_join_finalize_event: Option<Arc<HashJoinFinalizeEvent>>,
    /// Event that finalizes the sink
    pub finish_event: Arc<PipelineFinishEvent>,
    /// Event that signals completion
    pub complete_event: Arc<PipelineCompleteEvent>,
}

impl PipelineEventStack {
    /// Get all events as a vector for registration with EventCoordinator.
    pub fn all_events(&self) -> Vec<Arc<Event>> {
        let mut events = vec![
            self.initialize_event.event().clone(),
            self.execute_event.event().clone(),
            self.prepare_finish_event.event().clone(),
        ];
        if let Some(hash_finalize_event) = &self.hash_join_finalize_event {
            events.push(hash_finalize_event.event().clone());
        }
        events.push(self.finish_event.event().clone());
        events.push(self.complete_event.event().clone());
        events
    }

    /// Check if all events have completed.
    pub fn is_complete(&self) -> bool {
        self.complete_event.is_finished()
    }
}

/// Schedules pipeline execution through event chains.
///
pub struct PipelineScheduler {
    /// Default max threads for parallel execution
    default_max_threads: usize,
}

impl PipelineScheduler {
    fn needs_hash_join_finalize_event(pipeline: &Arc<Pipeline>) -> bool {
        pipeline
            .get_sink()
            .is_some_and(|sink| matches!(sink.operator_type(), PhysicalOperatorType::HashJoin))
    }

    /// Create a new PipelineScheduler with default settings.
    pub fn new() -> Self {
        Self {
            default_max_threads: 1,
        }
    }

    /// Create a new PipelineScheduler with specified max threads.
    pub fn with_max_threads(max_threads: usize) -> Self {
        Self {
            default_max_threads: max_threads.max(1),
        }
    }

    /// Schedule a single pipeline for execution.
    ///
    /// Creates the event chain: Initialize -> Execute -> Finish -> Complete
    ///
    /// # Arguments
    /// * `pipeline` - The pipeline to schedule (must be ready)
    /// * `initial_schedule` - Whether this is the initial schedule (affects completion tracking)
    /// * `ctx` - The execution context
    ///
    /// # Returns
    /// A `PipelineEventStack` containing all events for this pipeline.
    pub fn schedule_pipeline(
        &self,
        pipeline: Arc<Pipeline>,
        initial_schedule: bool,
        ctx: &ExecutionContext,
    ) -> Result<PipelineEventStack> {
        self.schedule_pipeline_with_threads(
            pipeline,
            self.default_max_threads,
            initial_schedule,
            ctx,
        )
    }

    /// Schedule a pipeline with a specific thread count.
    pub fn schedule_pipeline_with_threads(
        &self,
        pipeline: Arc<Pipeline>,
        max_threads: usize,
        initial_schedule: bool,
        ctx: &ExecutionContext,
    ) -> Result<PipelineEventStack> {
        // Ensure pipeline is ready
        if !pipeline.is_ready() {
            pipeline.set_ready();
        }

        // Create events
        let initialize_event = PipelineInitializeEvent::new(pipeline.clone());
        let execute_event = PipelineEvent::new(pipeline.clone(), max_threads);

        // Initialize global states for finish event
        pipeline.initialize(ctx)?;
        let global_states = pipeline.get_global_states().ok_or_else(|| {
            paro_common::error::internal("Pipeline should be initialized".to_string())
        })?;
        let prepare_finish_event =
            PipelinePrepareFinishEvent::new(pipeline.clone(), global_states.clone());
        let hash_join_finalize_event = Self::needs_hash_join_finalize_event(&pipeline)
            .then(|| HashJoinFinalizeEvent::new(pipeline.clone(), global_states.clone()));
        let finish_event = PipelineFinishEvent::new(pipeline.clone(), global_states);

        let complete_event = PipelineCompleteEventBuilder::new()
            .complete_pipeline(initial_schedule)
            .build();

        // Set up dependency chain:
        // initialize -> execute -> prepare_finish -> finish -> complete
        execute_event.add_dependency(initialize_event.event());
        prepare_finish_event.add_dependency(execute_event.event());
        if let Some(hash_finalize_event) = &hash_join_finalize_event {
            hash_finalize_event.add_dependency(prepare_finish_event.event());
            finish_event.add_dependency(hash_finalize_event.event());
        } else {
            finish_event.add_dependency(prepare_finish_event.event());
        }
        complete_event.add_dependency(finish_event.event());

        Ok(PipelineEventStack {
            initialize_event,
            execute_event,
            prepare_finish_event,
            hash_join_finalize_event,
            finish_event,
            complete_event,
        })
    }

    /// Schedule multiple pipelines that share a completion event.
    ///
    /// All pipelines will execute in parallel, and the shared complete event
    /// will only finish when all pipelines have completed.
    pub fn schedule_pipelines(
        &self,
        pipelines: Vec<Arc<Pipeline>>,
        initial_schedule: bool,
        ctx: &ExecutionContext,
    ) -> Result<(Vec<PipelineEventStack>, Arc<PipelineCompleteEvent>)> {
        if pipelines.is_empty() {
            let empty_complete = PipelineCompleteEvent::new(initial_schedule);
            return Ok((vec![], empty_complete));
        }

        // Create shared completion event
        let shared_complete = PipelineCompleteEventBuilder::new()
            .complete_pipeline(initial_schedule)
            .total_pipelines(pipelines.len())
            .build();

        let mut stacks = Vec::with_capacity(pipelines.len());

        for pipeline in pipelines {
            // Ensure pipeline is ready
            if !pipeline.is_ready() {
                pipeline.set_ready();
            }

            // Create events
            let initialize_event = PipelineInitializeEvent::new(pipeline.clone());
            let execute_event = PipelineEvent::new(pipeline.clone(), self.default_max_threads);
            pipeline.initialize(ctx)?;
            let global_states = pipeline.get_global_states().ok_or_else(|| {
                paro_common::error::internal("Pipeline should be initialized".to_string())
            })?;
            let prepare_finish_event =
                PipelinePrepareFinishEvent::new(pipeline.clone(), global_states.clone());
            let hash_join_finalize_event = Self::needs_hash_join_finalize_event(&pipeline)
                .then(|| HashJoinFinalizeEvent::new(pipeline.clone(), global_states.clone()));
            let finish_event = PipelineFinishEvent::new(pipeline.clone(), global_states);

            // Each pipeline has its own init/execute/prepare_finish/finish, but shares complete
            execute_event.add_dependency(initialize_event.event());
            prepare_finish_event.add_dependency(execute_event.event());
            if let Some(hash_finalize_event) = &hash_join_finalize_event {
                hash_finalize_event.add_dependency(prepare_finish_event.event());
                finish_event.add_dependency(hash_finalize_event.event());
            } else {
                finish_event.add_dependency(prepare_finish_event.event());
            }
            shared_complete.add_dependency(finish_event.event());

            // Create a per-pipeline complete event that references the shared one
            let pipeline_complete = PipelineCompleteEventBuilder::new()
                .complete_pipeline(false) // Don't signal individually
                .build();
            pipeline_complete.add_dependency(finish_event.event());

            stacks.push(PipelineEventStack {
                initialize_event,
                execute_event,
                prepare_finish_event,
                hash_join_finalize_event,
                finish_event,
                complete_event: pipeline_complete,
            });
        }

        Ok((stacks, shared_complete))
    }

    /// Schedule a pipeline with dependencies on other pipelines.
    ///
    /// The new pipeline will only start executing after all dependencies complete.
    pub fn schedule_pipeline_with_dependencies(
        &self,
        pipeline: Arc<Pipeline>,
        dependencies: &[&PipelineEventStack],
        initial_schedule: bool,
        ctx: &ExecutionContext,
    ) -> Result<PipelineEventStack> {
        let stack = self.schedule_pipeline(pipeline, initial_schedule, ctx)?;

        // Add dependencies: this pipeline's init/execute waits for all dependency completes
        for dep_stack in dependencies {
            stack
                .initialize_event
                .add_dependency(dep_stack.complete_event.event());
            stack
                .execute_event
                .add_dependency(dep_stack.complete_event.event());
        }

        Ok(stack)
    }
}

impl Default for PipelineScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PipelineScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineScheduler")
            .field("default_max_threads", &self.default_max_threads)
            .finish()
    }
}
