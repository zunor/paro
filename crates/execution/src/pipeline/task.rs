// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Task wrapper for [`Pipeline`](super::pipeline::Pipeline) execution in the
//! [`TaskScheduler`](paro_scheduler::scheduler::TaskScheduler).
//!
//! Each task owns a [`PipelineExecutor`](super::executor::PipelineExecutor); parallel
//! scans use multiple tasks sharing the same pipeline global state.

use parking_lot::Mutex;
use paro_scheduler::task::InterruptState;
use paro_scheduler::task::ProducerToken;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::executor::{ExecutionBudget, PipelineExecuteResult, PipelineExecutor};
use paro_common::error::Result;

/// Owns a [`PipelineExecutor`] for single-threaded or ad-hoc stepping.
pub struct BorrowedPipelineTask {
    executor: PipelineExecutor,
    finished: bool,
}

impl BorrowedPipelineTask {
    pub fn new(executor: PipelineExecutor) -> Self {
        Self {
            executor,
            finished: false,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn execute_step(&mut self) -> Result<PipelineExecuteResult> {
        if self.finished {
            return Ok(PipelineExecuteResult::Finished);
        }

        let result = self.executor.execute()?;

        if result == PipelineExecuteResult::Finished {
            self.finished = true;
        }

        Ok(result)
    }
}

/// A Task that executes part of a Pipeline in parallel.
///
/// This is the "owned" version of the task that can be passed to the TaskScheduler.
/// It holds an Arc to the Pipeline and the Event it belongs to.
pub struct PipelineTask {
    /// The pipeline to execute
    pipeline: Arc<super::pipeline::Pipeline>,
    /// The event that owns this task (used for dependency tracking in full execution)
    event: Arc<paro_scheduler::event::Event>,
    /// Task index (worker ID, used for parallel execution coordination)
    task_idx: usize,
    /// Whether the task is finished
    finished: bool,
    /// Whether the task is blocked
    blocked: AtomicBool,
    /// Producer token for rescheduling
    token: Mutex<Option<ProducerToken>>,
    /// Interrupt state linked to the scheduled task.
    interrupt_state: InterruptState,
    /// persistent executor
    executor: Option<PipelineExecutor>,
}

impl PipelineTask {
    const PARTIAL_CHUNK_COUNT: usize = 50;

    /// Create a new PipelineTask.
    pub fn new(
        pipeline: Arc<super::pipeline::Pipeline>,
        event: Arc<paro_scheduler::event::Event>,
        task_idx: usize,
    ) -> Self {
        Self {
            pipeline,
            event,
            task_idx,
            finished: false,
            blocked: AtomicBool::new(false),
            token: Mutex::new(None),
            interrupt_state: InterruptState::new(),
            executor: None,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }
}

impl Task for PipelineTask {
    fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        if self.finished {
            return Ok(TaskExecutionResult::Finished);
        }

        // Keep the owning event alive for the lifetime of the task.
        let _ = &self.event;

        let gstates = self
            .pipeline
            .get_global_states()
            .ok_or_else(|| paro_common::error::internal("Pipeline not initialized".to_string()))?;

        // Execute with context
        if self.task_blocked_on_result() {
            return Ok(TaskExecutionResult::Blocked);
        }

        let Some(_admission) = self
            .pipeline
            .query_memory_pool()
            .admission_controller()
            .try_acquire(self.interrupt_state.clone())
        else {
            return Ok(TaskExecutionResult::Blocked);
        };

        // Initialize executor if needed
        if self.executor.is_none() {
            let total_threads = self.pipeline.compute_max_threads();
            self.executor = Some(PipelineExecutor::with_interrupt_state(
                gstates.client.clone(),
                self.task_idx,
                total_threads,
                self.pipeline.clone(),
                gstates.clone(),
                self.interrupt_state.clone(),
            )?);
        }

        let executor = self.executor.as_mut().unwrap();

        if matches!(mode, TaskExecutionMode::ProcessPartial) {
            executor.set_budget(ExecutionBudget::new(Self::PARTIAL_CHUNK_COUNT));
            return match executor.execute()? {
                PipelineExecuteResult::Finished => {
                    self.finished = true;
                    Ok(TaskExecutionResult::Finished)
                }
                PipelineExecuteResult::Blocked => Ok(TaskExecutionResult::Blocked),
                PipelineExecuteResult::Interrupted | PipelineExecuteResult::NotFinished => {
                    Ok(TaskExecutionResult::NotFinished)
                }
            };
        }
        executor.budget = None;

        // Execute one or more steps
        loop {
            let res = executor.execute()?;
            match res {
                PipelineExecuteResult::Finished => {
                    self.finished = true;
                    // Note: Do NOT call finish_task() here!
                    // When this task is scheduled via Event::schedule_tasks_to_scheduler(),
                    // it gets wrapped in EventTask which automatically calls finish_task()
                    // when the inner task returns Finished.
                    return Ok(TaskExecutionResult::Finished);
                }
                PipelineExecuteResult::Blocked => {
                    // Task is blocked, will be descheduled by TaskScheduler
                    return Ok(TaskExecutionResult::Blocked);
                }
                PipelineExecuteResult::Interrupted => {
                    // Budget exceeded, yield
                    return Ok(TaskExecutionResult::NotFinished);
                }
                PipelineExecuteResult::NotFinished => {
                    // Continue processing
                }
            }
        }
    }

    fn set_token(&mut self, token: ProducerToken) {
        *self.token.lock() = Some(token);
    }

    fn set_interrupt_state(&mut self, interrupt_state: InterruptState) {
        self.interrupt_state = interrupt_state.clone();
        if let Some(executor) = self.executor.as_mut() {
            executor.interrupt_state = interrupt_state;
        }
    }

    fn clear_interrupt_state(&mut self) {
        self.interrupt_state = InterruptState::new();
        if let Some(executor) = self.executor.as_mut() {
            executor.interrupt_state = InterruptState::new();
        }
    }

    fn get_token(&self) -> Option<ProducerToken> {
        self.token.lock().clone()
    }

    fn deschedule(&mut self) -> Result<()> {
        self.blocked.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn reschedule(&mut self) -> Result<()> {
        self.blocked.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn task_blocked_on_result(&self) -> bool {
        self.blocked.load(Ordering::SeqCst)
    }

    fn task_type(&self) -> &str {
        "PipelineTask"
    }
}

impl PipelineTask {
    /// Execute the task with a given context.
    ///
    /// 由于 PipelineExecutor 现在内部持有 ThreadContext，
    /// 这个方法需要从 ExecutionContext 提取 client 信息。
    pub fn execute_with_context(
        &mut self,
        ctx: &crate::execution_context::ExecutionContext,
    ) -> Result<TaskExecutionResult> {
        if self.finished {
            return Ok(TaskExecutionResult::Finished);
        }
        if self.task_blocked_on_result() {
            return Ok(TaskExecutionResult::Blocked);
        }

        let Some(_admission) = self
            .pipeline
            .query_memory_pool()
            .admission_controller()
            .try_acquire(self.interrupt_state.clone())
        else {
            return Ok(TaskExecutionResult::Blocked);
        };

        let gstates = self
            .pipeline
            .get_global_states()
            .ok_or_else(|| paro_common::error::internal("Pipeline not initialized".to_string()))?;

        // Initialize executor if needed
        if self.executor.is_none() {
            // Use thread_id from context, and gstates.client for session
            let total_threads = self.pipeline.compute_max_threads();
            self.executor = Some(PipelineExecutor::with_interrupt_state(
                gstates.client.clone(),
                ctx.thread_id(),
                total_threads,
                self.pipeline.clone(),
                gstates.clone(),
                self.interrupt_state.clone(),
            )?);
        }

        let executor = self.executor.as_mut().unwrap();
        executor.budget = None;

        // Execute one or more steps
        loop {
            let res = executor.execute()?;
            match res {
                PipelineExecuteResult::Finished => {
                    self.finished = true;
                    // Note: Do NOT call finish_task() here!
                    // When this task is scheduled via Event::schedule_tasks_to_scheduler(),
                    // it gets wrapped in EventTask which automatically calls finish_task()
                    // when the inner task returns Finished.
                    return Ok(TaskExecutionResult::Finished);
                }
                PipelineExecuteResult::Blocked => {
                    // Task is blocked, will be descheduled by TaskScheduler
                    return Ok(TaskExecutionResult::Blocked);
                }
                PipelineExecuteResult::Interrupted => {
                    // Budget exceeded, yield
                    return Ok(TaskExecutionResult::NotFinished);
                }
                PipelineExecuteResult::NotFinished => {
                    // For now, just loop.
                }
            }
        }
    }
}

/// Implement From trait for idiomatic conversion.
impl From<PipelineExecuteResult> for TaskExecutionResult {
    fn from(result: PipelineExecuteResult) -> Self {
        match result {
            PipelineExecuteResult::NotFinished => TaskExecutionResult::NotFinished,
            PipelineExecuteResult::Blocked => TaskExecutionResult::Blocked,
            PipelineExecuteResult::Interrupted => TaskExecutionResult::NotFinished,
            PipelineExecuteResult::Finished => TaskExecutionResult::Finished,
        }
    }
}

/// Helper function for explicit conversion.
#[inline]
pub fn pipeline_result_to_task_result(result: PipelineExecuteResult) -> TaskExecutionResult {
    result.into()
}
