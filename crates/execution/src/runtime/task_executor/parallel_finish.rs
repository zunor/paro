// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use parking_lot::Mutex as ParkingMutex;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use paro_scheduler::task::{ProducerToken, Task, TaskExecutionMode, TaskExecutionResult};

use crate::explain::profiler::{OperatorProfiler, ProfileWorkerContext};
use crate::runtime::{
    FinishTaskGroup, FinishTaskId, FinishTaskPoll, OperatorWakeScope, PipelineRuntime,
    PipelineTaskId, PipelineTaskStepContext, QueryRuntimeContext, WorkGroupCompletion,
};
use crate::thread_context::ThreadContext;

use super::helpers::finish_context;

pub(super) fn run_parallel_finish_tasks(
    runtime: Arc<PipelineRuntime>,
    query: &QueryRuntimeContext,
    group: FinishTaskGroup,
    task_ids: Vec<FinishTaskId>,
    allocator: Arc<dyn Allocator>,
) -> Result<()> {
    let total_threads = query.session.number_of_threads().max(1);
    let scheduler = query.session.scheduler().clone();
    let producer = scheduler.create_producer_with_priority(0);
    let coordinator = Arc::new(WorkGroupCompletion::new(task_ids.len()));
    let query = Arc::new(query.clone());
    let tasks = task_ids
        .into_iter()
        .enumerate()
        .map(|(idx, task_id)| {
            Arc::new(ParkingMutex::new(ScheduledFinishTask {
                runtime: runtime.clone(),
                query: query.clone(),
                group: group.clone(),
                allocator: allocator.clone(),
                task_id,
                thread_id: idx % total_threads,
                total_threads,
                coordinator: coordinator.clone(),
                token: None,
            })) as Arc<ParkingMutex<dyn Task>>
        })
        .collect::<Vec<_>>();
    producer.schedule_tasks(tasks);
    wait_for_parallel_finish_group(scheduler.as_ref(), &producer, &coordinator, query.as_ref())
}

fn wait_for_parallel_finish_group(
    scheduler: &paro_scheduler::scheduler::TaskScheduler,
    producer: &ProducerToken,
    coordinator: &WorkGroupCompletion,
    query: &QueryRuntimeContext,
) -> Result<()> {
    let marker = std::sync::atomic::AtomicBool::new(true);
    loop {
        if let Err(error) = query.cancellation.check() {
            cancel_parallel_finish_and_drain(scheduler, producer, coordinator);
            return Err(error);
        }
        match coordinator.snapshot() {
            Ok(None) => return Ok(()),
            Ok(Some(_)) => {}
            Err(error) => {
                cancel_parallel_finish_and_drain(scheduler, producer, coordinator);
                return Err(error);
            }
        }
        scheduler.execute_tasks_for_producer(producer, &marker, usize::MAX);
        if let Some(error) = scheduler.get_error_for_producer(producer) {
            cancel_parallel_finish_and_drain(scheduler, producer, coordinator);
            return Err(paro_error::internal(error.to_string()));
        }
        match coordinator.snapshot() {
            Ok(None) => return Ok(()),
            Ok(Some(_)) => {}
            Err(error) => {
                cancel_parallel_finish_and_drain(scheduler, producer, coordinator);
                return Err(error);
            }
        }
        if !scheduler.wait_for_task_for_producer(producer) {
            coordinator.wait_for_worker_completion_with_timeout();
        }
    }
}

fn cancel_parallel_finish_and_drain(
    scheduler: &paro_scheduler::scheduler::TaskScheduler,
    producer: &ProducerToken,
    coordinator: &WorkGroupCompletion,
) {
    let cancelled = scheduler.cancel_tasks_for_producer(producer);
    coordinator.cancel_queued(cancelled);
    wait_for_finish_workers(coordinator);
}

fn wait_for_finish_workers(coordinator: &WorkGroupCompletion) {
    while coordinator.remaining() > 0 {
        coordinator.wait_for_worker_completion_with_timeout();
    }
}

struct ScheduledFinishTask {
    runtime: Arc<PipelineRuntime>,
    query: Arc<QueryRuntimeContext>,
    group: FinishTaskGroup,
    allocator: Arc<dyn Allocator>,
    task_id: FinishTaskId,
    thread_id: usize,
    total_threads: usize,
    coordinator: Arc<WorkGroupCompletion>,
    token: Option<ProducerToken>,
}

impl ScheduledFinishTask {
    fn run(&mut self) -> Result<()> {
        let task = self
            .runtime
            .create_task_state(self.query.as_ref(), self.allocator.clone())?;
        let thread = ThreadContext::new(self.thread_id, self.total_threads);
        let wake = OperatorWakeScope {
            task_id: PipelineTaskId(
                ((self.runtime.program.id.index() as u64) << 32) | self.task_id.0 as u64,
            ),
            generation: self.query.output.wake_generation(),
        };
        let mut profiler = self.query.explain_profiler.as_ref().map_or_else(
            OperatorProfiler::disabled,
            |profiler| {
                OperatorProfiler::new_with_context(
                    profiler.clone(),
                    ProfileWorkerContext::new(
                        Some(self.runtime.program.id.index() as u64),
                        Some(
                            ((self.runtime.program.id.index() as u64) << 32)
                                | self.task_id.0 as u64,
                        ),
                        Some(self.thread_id as u64),
                        Some(self.total_threads as u64),
                        None,
                    ),
                )
            },
        );
        let mut step_ctx = PipelineTaskStepContext {
            query: self.query.as_ref(),
            thread: &thread,
            wake: &wake,
            profiler: &mut profiler,
        };
        let poll = {
            let mut finish_ctx = finish_context(
                &mut step_ctx,
                self.runtime.program.id,
                self.runtime.program.sink.operator_id,
                Some(self.task_id),
                &task,
            );
            self.group.driver.run_task(self.task_id, &mut finish_ctx)
        }?;
        profiler.flush();
        match poll {
            FinishTaskPoll::Done => Ok(()),
            FinishTaskPoll::Pending(blocker) => Err(paro_error::internal(format!(
                "parallel finish sub-task {} blocked on {:?}; async finish blockers require scheduler-level finish waiters",
                self.task_id.0, blocker.reason
            ))),
        }
    }
}

impl Task for ScheduledFinishTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        let result = match panic::catch_unwind(AssertUnwindSafe(|| self.run())) {
            Ok(result) => result,
            Err(_) => Err(paro_error::internal("parallel finish task panicked")),
        };
        self.coordinator.finish(result);
        Ok(TaskExecutionResult::Finished)
    }

    fn set_token(&mut self, token: ProducerToken) {
        self.token = Some(token);
    }

    fn get_token(&self) -> Option<ProducerToken> {
        self.token.clone()
    }

    fn task_type(&self) -> &str {
        "ScheduledFinishTask"
    }
}
