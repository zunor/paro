// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # BasePipelineEvent
//!
//! Base structure for pipeline-related events.

use paro_scheduler::event::Event;
use std::sync::Arc;

use crate::pipeline::pipeline::Pipeline;

/// BasePipelineEvent associates a scheduler Event with a Pipeline.
pub struct BasePipelineEvent {
    /// The underlying scheduler event for dependency management
    event: Arc<Event>,
    /// The pipeline this event belongs to
    pipeline: Arc<Pipeline>,
}

impl BasePipelineEvent {
    /// Create a new BasePipelineEvent for the given pipeline.
    pub fn new(pipeline: Arc<Pipeline>) -> Self {
        Self {
            event: Event::new(),
            pipeline,
        }
    }

    /// Create a new BasePipelineEvent with an existing event.
    pub fn with_event(event: Arc<Event>, pipeline: Arc<Pipeline>) -> Self {
        Self { event, pipeline }
    }

    /// Get the underlying scheduler event.
    pub fn event(&self) -> &Arc<Event> {
        &self.event
    }

    /// Get the pipeline this event belongs to.
    pub fn pipeline(&self) -> &Arc<Pipeline> {
        &self.pipeline
    }

    /// Check if this event has any dependencies.
    pub fn has_dependencies(&self) -> bool {
        self.event.has_dependencies()
    }

    /// Check if this event is finished.
    pub fn is_finished(&self) -> bool {
        self.event.is_finished()
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.event.add_dependency(dependency);
    }

    /// Add a dependency on another BasePipelineEvent.
    pub fn add_pipeline_dependency(&self, dependency: &BasePipelineEvent) {
        self.event.add_dependency(&dependency.event);
    }

    /// Set the number of tasks for this event.
    pub fn set_tasks(&self, task_count: usize) {
        self.event.set_tasks(task_count);
    }

    /// Mark a task as finished.
    pub fn finish_task(&self) {
        self.event.finish_task();
    }
}

impl std::fmt::Debug for BasePipelineEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasePipelineEvent")
            .field("has_dependencies", &self.event.has_dependencies())
            .field("is_finished", &self.event.is_finished())
            .field("operator_count", &self.pipeline.operator_count())
            .finish()
    }
}
