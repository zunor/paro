// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # PipelineCompleteEvent
//!
//! Event for signaling pipeline completion.

use paro_scheduler::event::Event;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// PipelineCompleteEvent signals pipeline completion.
pub struct PipelineCompleteEvent {
    /// The underlying scheduler event
    event: Arc<Event>,
    /// Whether to signal completion when finalized
    complete_pipeline: bool,
    /// Completion callback (called when event finishes)
    on_complete: Option<Box<dyn Fn() + Send + Sync>>,
    /// Counter for completed pipelines (for multi-pipeline coordination)
    completed_count: Arc<AtomicUsize>,
    /// Total pipelines to wait for (for multi-pipeline coordination)
    total_count: usize,
}

impl PipelineCompleteEvent {
    /// Create a new PipelineCompleteEvent.
    pub fn new(complete_pipeline: bool) -> Arc<Self> {
        let event = Event::new();
        // Complete events model completion as a single synthetic task so the
        // scheduler event does not auto-finish during activation.
        event.set_tasks(1);

        Arc::new(Self {
            event,
            complete_pipeline,
            on_complete: None,
            completed_count: Arc::new(AtomicUsize::new(0)),
            total_count: 1,
        })
    }

    /// Create a PipelineCompleteEvent for coordinating multiple pipelines.
    pub fn for_multiple_pipelines(total: usize) -> Arc<Self> {
        let event = Event::new();
        // Complete events model completion as a single synthetic task so the
        // scheduler event does not auto-finish during activation.
        event.set_tasks(1);

        Arc::new(Self {
            event,
            complete_pipeline: true,
            on_complete: None,
            completed_count: Arc::new(AtomicUsize::new(0)),
            total_count: total,
        })
    }

    /// Get the underlying event for dependency management.
    pub fn event(&self) -> &Arc<Event> {
        &self.event
    }

    /// Check if this event should signal completion.
    pub fn should_complete(&self) -> bool {
        self.complete_pipeline
    }

    /// Get the number of completed pipelines.
    pub fn completed_count(&self) -> usize {
        self.completed_count.load(Ordering::SeqCst)
    }

    /// Get the total number of pipelines to wait for.
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    /// Check if all pipelines have completed.
    pub fn all_complete(&self) -> bool {
        self.completed_count() >= self.total_count
    }

    /// Signal that a pipeline has completed.
    pub fn signal_pipeline_complete(&self) -> bool {
        let prev = self.completed_count.fetch_add(1, Ordering::SeqCst);
        prev + 1 >= self.total_count
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.event.add_dependency(dependency);
    }

    /// Check if the event is finished.
    pub fn is_finished(&self) -> bool {
        self.event.is_finished()
    }

    /// Check if this event has any dependencies.
    pub fn has_dependencies(&self) -> bool {
        self.event.has_dependencies()
    }

    /// Mark this completion event as done once its dependencies have activated it.
    pub fn mark_complete(&self) {
        debug_assert!(
            self.event.total_task_count() > 0,
            "PipelineCompleteEvent requires a synthetic task before completion"
        );
        self.event.finish_task();

        if self.complete_pipeline {
            if let Some(ref callback) = self.on_complete {
                callback();
            }
        }
    }
}

impl std::fmt::Debug for PipelineCompleteEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineCompleteEvent")
            .field("complete_pipeline", &self.complete_pipeline)
            .field("is_finished", &self.is_finished())
            .field("completed_count", &self.completed_count())
            .field("total_count", &self.total_count)
            .finish()
    }
}

/// Builder for PipelineCompleteEvent with callbacks.
pub struct PipelineCompleteEventBuilder {
    complete_pipeline: bool,
    on_complete: Option<Box<dyn Fn() + Send + Sync>>,
    total_count: usize,
}

impl PipelineCompleteEventBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            complete_pipeline: true,
            on_complete: None,
            total_count: 1,
        }
    }

    /// Set whether to signal completion when finalized.
    pub fn complete_pipeline(mut self, complete: bool) -> Self {
        self.complete_pipeline = complete;
        self
    }

    /// Set a callback to be called when the event completes.
    pub fn on_complete<F>(mut self, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_complete = Some(Box::new(callback));
        self
    }

    /// Set the total number of pipelines to wait for.
    pub fn total_pipelines(mut self, total: usize) -> Self {
        self.total_count = total;
        self
    }

    /// Build the PipelineCompleteEvent.
    pub fn build(self) -> Arc<PipelineCompleteEvent> {
        let event = Event::new();
        // Complete events model completion as a single synthetic task so the
        // scheduler event does not auto-finish during activation.
        event.set_tasks(1);

        Arc::new(PipelineCompleteEvent {
            event,
            complete_pipeline: self.complete_pipeline,
            on_complete: self.on_complete,
            completed_count: Arc::new(AtomicUsize::new(0)),
            total_count: self.total_count,
        })
    }
}

impl Default for PipelineCompleteEventBuilder {
    fn default() -> Self {
        Self::new()
    }
}
