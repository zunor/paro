//! # PipelineEvent
//!
//! Event for scheduling and executing pipeline tasks.

use paro_scheduler::event::Event;
use std::sync::Arc;

use super::event_base::BasePipelineEvent;
use crate::pipeline::pipeline::Pipeline;
use paro_common::error::Result;

/// PipelineEvent schedules and coordinates pipeline execution.
pub struct PipelineEvent {
    /// Base event with pipeline reference
    base: BasePipelineEvent,
    /// Maximum number of threads for parallel execution
    max_threads: usize,
}

impl PipelineEvent {
    /// Create a new PipelineEvent for the given pipeline.
    pub fn new(pipeline: Arc<Pipeline>, max_threads: usize) -> Arc<Self> {
        Arc::new(Self {
            base: BasePipelineEvent::new(pipeline),
            max_threads,
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

    /// Schedule pipeline execution.
    pub fn schedule(
        self: &Arc<Self>,
        scheduler: &Arc<paro_scheduler::scheduler::TaskScheduler>,
    ) -> Result<()> {
        self.pipeline().schedule(self.event(), scheduler, None)?;
        Ok(())
    }

    /// Schedule pipeline execution using a specific producer token.
    pub fn schedule_with_producer(
        self: &Arc<Self>,
        scheduler: &Arc<paro_scheduler::scheduler::TaskScheduler>,
        producer: &paro_scheduler::task::ProducerToken,
    ) -> Result<()> {
        self.pipeline()
            .schedule(self.event(), scheduler, Some(producer))?;
        Ok(())
    }

    /// Check if the event is finished.
    pub fn is_finished(&self) -> bool {
        self.base.is_finished()
    }

    /// Add a dependency on another event.
    pub fn add_dependency(&self, dependency: &Arc<Event>) {
        self.base.add_dependency(dependency);
    }

    /// Get max threads setting.
    pub fn max_threads(&self) -> usize {
        self.max_threads
    }
}

impl std::fmt::Debug for PipelineEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineEvent")
            .field("max_threads", &self.max_threads)
            .field("is_finished", &self.is_finished())
            .finish()
    }
}
