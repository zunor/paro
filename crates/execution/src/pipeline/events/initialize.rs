// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # PipelineInitializeEvent
//!
//! Event for initializing the base pipeline sink state before execution.

use parking_lot::Mutex;
use paro_scheduler::event::Event;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use std::sync::Arc;

use super::event_base::BasePipelineEvent;
use crate::pipeline::pipeline::Pipeline;

/// PipelineInitializeEvent initializes the base pipeline sink state.
pub struct PipelineInitializeEvent {
    /// Base event with pipeline reference
    base: BasePipelineEvent,
}

impl PipelineInitializeEvent {
    /// Create a new PipelineInitializeEvent for the given pipeline.
    pub fn new(pipeline: Arc<Pipeline>) -> Arc<Self> {
        Arc::new(Self {
            base: BasePipelineEvent::new(pipeline),
        })
    }

    /// Get the underlying event for dependency management.
    pub fn event(&self) -> &Arc<Event> {
        self.base.event()
    }

    /// Get the pipeline.
    pub fn pipeline(&self) -> &Arc<Pipeline> {
        self.base.pipeline()
    }

    /// Create the initialization task for external scheduling.
    pub fn create_task(self: &Arc<Self>) -> Arc<Mutex<dyn Task>> {
        let task = PipelineInitializeTask::new(self.clone());
        self.base.set_tasks(1);
        Arc::new(Mutex::new(task))
    }

    /// Check if the event is finished.
    pub fn is_finished(&self) -> bool {
        self.base.is_finished()
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.base.add_dependency(dependency);
    }

    /// Set the number of tasks for this event (for testing).
    #[cfg(test)]
    pub fn set_tasks(&self, count: usize) {
        self.base.set_tasks(count);
    }
}

impl std::fmt::Debug for PipelineInitializeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineInitializeEvent")
            .field("is_finished", &self.is_finished())
            .finish()
    }
}

/// Task that initializes the base pipeline sink state.
pub struct PipelineInitializeTask {
    /// Reference to the parent event
    event: Arc<PipelineInitializeEvent>,
    /// Whether the task has completed
    finished: bool,
    /// Result of reset_sink (stored for later use)
    sink_state: Option<Arc<dyn crate::operator::state::GlobalSinkState>>,
}

impl PipelineInitializeTask {
    /// Create a new PipelineInitializeTask.
    pub fn new(event: Arc<PipelineInitializeEvent>) -> Self {
        Self {
            event,
            finished: false,
            sink_state: None,
        }
    }

    /// Get the initialized sink state after execution.
    pub fn sink_state(&self) -> Option<&Arc<dyn crate::operator::state::GlobalSinkState>> {
        self.sink_state.as_ref()
    }
}

impl Task for PipelineInitializeTask {
    fn execute(
        &mut self,
        _mode: TaskExecutionMode,
    ) -> paro_common::error::Result<TaskExecutionResult> {
        if self.finished {
            return Ok(TaskExecutionResult::Finished);
        }

        // Initialize the sink state
        let pipeline = self.event.pipeline();

        // We need an ExecutionContext to initialize.
        // We can create a temporary one from the session.
        // If gstates is not yet initialized, we might need to get session from somewhere else.
        let gstates = pipeline.get_global_states().ok_or_else(|| {
            paro_common::error::internal("Pipeline global states not initialized".to_string())
        })?;
        let session = gstates.client.clone();
        let thread = crate::thread_context::ThreadContext::single_threaded();
        let ctx = crate::execution_context::ExecutionContext::new(session, &thread, Some(pipeline));

        // Every pipeline performs its own full reset() right before scheduling.
        self.sink_state = pipeline.reset_sink(&ctx)?;

        // Keep the sink state for inspection if needed by tests.
        if self.sink_state.is_none() {
            let gstates = pipeline.get_global_states().unwrap();
            self.sink_state = gstates.sink.lock().clone();
        }

        self.finished = true;
        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "PipelineInitializeTask"
    }
}
