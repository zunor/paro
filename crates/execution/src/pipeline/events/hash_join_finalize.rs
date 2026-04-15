// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # HashJoinFinalizeEvent
//!
//! Event for running hash-join sink finalize in its own scheduler stage.

use paro_scheduler::event::Event;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use std::sync::Arc;

use crate::operator::state::OperatorSinkFinalizeInput;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SinkFinalizeType;

use super::event_base::BasePipelineEvent;
use crate::pipeline::pipeline::{Pipeline, PipelineGlobalStates};
use paro_common::error::Result;

/// Event that runs hash-join finalize between PrepareFinish and Finish.
pub struct HashJoinFinalizeEvent {
    /// Base event with pipeline reference.
    base: BasePipelineEvent,
    /// Global states from execution phase.
    global_states: Arc<PipelineGlobalStates>,
}

impl HashJoinFinalizeEvent {
    /// Create a new HashJoinFinalizeEvent.
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

    /// Get global states.
    pub fn global_states(&self) -> &Arc<PipelineGlobalStates> {
        &self.global_states
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.base.add_dependency(dependency);
    }
}

impl std::fmt::Debug for HashJoinFinalizeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashJoinFinalizeEvent")
            .field("is_finished", &self.base.is_finished())
            .finish()
    }
}

/// Task that performs hash-join sink finalize.
pub struct HashJoinFinalizeTask {
    /// Parent event.
    event: Arc<HashJoinFinalizeEvent>,
    /// Whether task has completed.
    finished: bool,
}

impl HashJoinFinalizeTask {
    /// Create a new HashJoinFinalizeTask.
    pub fn new(event: Arc<HashJoinFinalizeEvent>) -> Self {
        Self {
            event,
            finished: false,
        }
    }
}

impl Task for HashJoinFinalizeTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        if self.finished {
            return Ok(TaskExecutionResult::Finished);
        }

        let pipeline = self.event.pipeline();
        let global_states = self.event.global_states();

        if let Some(sink) = pipeline.get_sink() {
            if sink.operator_type() != PhysicalOperatorType::HashJoin {
                self.finished = true;
                return Ok(TaskExecutionResult::Finished);
            }

            let sink_guard = global_states.sink.lock();
            if let Some(sink_state) = sink_guard.as_ref() {
                let interrupt_state = paro_scheduler::task::InterruptState::new();
                let finalize_input =
                    OperatorSinkFinalizeInput::new(sink_state.as_ref(), &interrupt_state);
                match sink.finalize(&finalize_input)? {
                    SinkFinalizeType::Ready | SinkFinalizeType::NoOutputPossible => {}
                    SinkFinalizeType::Blocked => return Ok(TaskExecutionResult::Blocked),
                }
            }
        }

        self.finished = true;
        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "HashJoinFinalizeTask"
    }
}
