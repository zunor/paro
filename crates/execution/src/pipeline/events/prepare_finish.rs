// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # PipelinePrepareFinishEvent
//!
//! Event for preparing the sink for finalization.

use parking_lot::Mutex;
use paro_scheduler::event::Event;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use std::sync::Arc;

use super::event_base::BasePipelineEvent;
use crate::pipeline::pipeline::{Pipeline, PipelineGlobalStates};
use paro_common::error::Result;

/// PipelinePrepareFinishEvent prepares the sink for finalization.
pub struct PipelinePrepareFinishEvent {
    /// Base event with pipeline reference
    base: BasePipelineEvent,
    /// Global states from the execution phase
    global_states: Arc<PipelineGlobalStates>,
}

impl PipelinePrepareFinishEvent {
    /// Create a new PipelinePrepareFinishEvent.
    pub fn new(pipeline: Arc<Pipeline>, global_states: Arc<PipelineGlobalStates>) -> Arc<Self> {
        Arc::new(Self {
            base: BasePipelineEvent::new(pipeline),
            global_states,
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

    /// Get the global states.
    pub fn global_states(&self) -> &Arc<PipelineGlobalStates> {
        &self.global_states
    }

    /// Schedule the preparation task.
    pub fn schedule(self: &Arc<Self>) -> Arc<Mutex<dyn Task>> {
        let task = PipelinePrepareFinishTask::new(self.clone());
        self.base.set_tasks(1);
        Arc::new(Mutex::new(task))
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.base.add_dependency(dependency);
    }
}

impl std::fmt::Debug for PipelinePrepareFinishEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelinePrepareFinishEvent")
            .field("is_finished", &self.base.is_finished())
            .finish()
    }
}

/// Task that prepares the sink for finalization.
pub struct PipelinePrepareFinishTask {
    /// Reference to the parent event
    event: Arc<PipelinePrepareFinishEvent>,
    /// Whether the task has completed
    finished: bool,
}

impl PipelinePrepareFinishTask {
    /// Create a new PipelinePrepareFinishTask.
    pub fn new(event: Arc<PipelinePrepareFinishEvent>) -> Self {
        Self {
            event,
            finished: false,
        }
    }
}

impl Task for PipelinePrepareFinishTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        if self.finished {
            return Ok(TaskExecutionResult::Finished);
        }

        let pipeline = self.event.pipeline();
        let global_states = self.event.global_states();

        if let Some(sink) = pipeline.get_sink() {
            let sink_guard = global_states.sink.lock();
            if let Some(sink_state) = sink_guard.as_ref() {
                sink.prepare_finalize(sink_state.as_ref())?;
            }
        }

        self.finished = true;
        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "PipelinePrepareFinishTask"
    }
}
