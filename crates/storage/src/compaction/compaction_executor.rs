// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::compaction_task::CompactionTask;
use crate::metrics::storage_metrics;
use crate::tablet::Tablet;
use parking_lot::Mutex;
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::ProducerToken;
use paro_scheduler::task::Task;
use paro_scheduler::task::TaskExecutionMode;
use paro_scheduler::task::TaskExecutionResult;
use std::any::Any;
use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

const COMPACTION_TASK_PRIORITY: i32 = -10;

type CompactionCallback = Box<dyn FnOnce(std::result::Result<(), String>) + Send + Sync + 'static>;

struct ScheduledCompaction {
    tablet: Arc<Tablet>,
    task: Box<dyn CompactionTask>,
    callback: Option<CompactionCallback>,
}

struct SchedulerCompactionTask {
    scheduled: Option<ScheduledCompaction>,
    state: Arc<SchedulerCompactionState>,
}

impl SchedulerCompactionTask {
    fn new(scheduled: ScheduledCompaction, state: Arc<SchedulerCompactionState>) -> Self {
        Self {
            scheduled: Some(scheduled),
            state,
        }
    }
}

impl Task for SchedulerCompactionTask {
    fn execute(
        &mut self,
        _mode: TaskExecutionMode,
    ) -> paro_common::error::Result<TaskExecutionResult> {
        let Some(mut scheduled) = self.scheduled.take() else {
            return Ok(TaskExecutionResult::Finished);
        };

        let result = run_compaction_task(scheduled.tablet.clone(), scheduled.task.as_mut());

        if let Some(callback) = scheduled.callback.take() {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(result)));
        }

        self.state.on_task_complete();
        Ok(TaskExecutionResult::Finished)
    }

    fn task_type(&self) -> &str {
        "CompactionSchedulerTask"
    }
}

struct SchedulerCompactionState {
    producer: ProducerToken,
    max_concurrency: usize,
    running: AtomicUsize,
    pending: Mutex<VecDeque<ScheduledCompaction>>,
}

impl SchedulerCompactionState {
    fn new(scheduler: Arc<TaskScheduler>, max_concurrency: usize) -> Arc<Self> {
        let cap = max_concurrency.max(1);
        Arc::new(Self {
            producer: scheduler.create_producer_with_priority(COMPACTION_TASK_PRIORITY),
            max_concurrency: cap,
            running: AtomicUsize::new(0),
            pending: Mutex::new(VecDeque::new()),
        })
    }

    fn submit(self: &Arc<Self>, scheduled: ScheduledCompaction) {
        self.pending.lock().push_back(scheduled);
        self.dispatch_pending();
    }

    fn on_task_complete(self: &Arc<Self>) {
        self.running.fetch_sub(1, Ordering::AcqRel);
        self.dispatch_pending();
    }

    fn dispatch_pending(self: &Arc<Self>) {
        loop {
            let next = {
                let mut pending = self.pending.lock();
                if self.running.load(Ordering::Acquire) >= self.max_concurrency {
                    None
                } else if let Some(task) = pending.pop_front() {
                    self.running.fetch_add(1, Ordering::AcqRel);
                    Some(task)
                } else {
                    None
                }
            };

            let Some(task) = next else {
                break;
            };

            self.producer
                .schedule_task(Arc::new(parking_lot::Mutex::new(
                    SchedulerCompactionTask::new(task, self.clone()),
                )));
        }
    }
}

/// Executor for compaction tasks with concurrency control.
pub struct CompactionExecutor {
    /// Limit concurrent compaction tasks.
    semaphore: Arc<Semaphore>,
    /// Optional scheduler-backed mode for unified query/maintenance scheduling.
    scheduler_state: Option<Arc<SchedulerCompactionState>>,
}

impl CompactionExecutor {
    /// Create a new executor with the given max concurrency.
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
            scheduler_state: None,
        }
    }

    /// Create a new executor that runs compaction tasks on TaskScheduler.
    pub fn new_with_scheduler(max_concurrency: usize, scheduler: Arc<TaskScheduler>) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
            scheduler_state: Some(SchedulerCompactionState::new(scheduler, max_concurrency)),
        }
    }

    /// Submit a compaction task for execution.
    pub fn submit(&self, tablet: Arc<Tablet>, task: Box<dyn CompactionTask>) {
        self.submit_with_callback(tablet, task, |_| {});
    }

    /// Submit a compaction task with a completion callback.
    pub fn submit_with_callback<F>(
        &self,
        tablet: Arc<Tablet>,
        mut task: Box<dyn CompactionTask>,
        callback: F,
    ) where
        F: FnOnce(std::result::Result<(), String>) + Send + Sync + 'static,
    {
        if let Some(state) = &self.scheduler_state {
            state.submit(ScheduledCompaction {
                tablet,
                task,
                callback: Some(Box::new(callback)),
            });
            return;
        }

        let sem = self.semaphore.clone();

        tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            callback(run_compaction_task(tablet, task.as_mut()));
        });
    }
}

fn run_compaction_task(
    tablet: Arc<Tablet>,
    task: &mut dyn CompactionTask,
) -> std::result::Result<(), String> {
    let start = std::time::Instant::now();
    let metrics = storage_metrics();
    metrics.inc_compaction_tasks();

    match panic::catch_unwind(AssertUnwindSafe(|| task.run())) {
        Ok(Ok(())) => {
            metrics.inc_compaction_success();
            metrics.add_compaction_duration(start.elapsed());
            info!("Compaction successful for tablet {}", tablet.tablet_id());
            Ok(())
        }
        Ok(Err(err)) => {
            metrics.inc_compaction_failed();
            let message = err.to_string();
            error!(
                "Compaction failed for tablet {}: {}",
                tablet.tablet_id(),
                message
            );
            Err(message)
        }
        Err(payload) => {
            metrics.inc_compaction_failed();
            let message = panic_message(payload);
            error!(
                "Compaction panicked for tablet {}: {}",
                tablet.tablet_id(),
                message
            );
            Err(message)
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    let any = payload.as_ref();
    if let Some(message) = any.downcast_ref::<&'static str>() {
        return format!("panic: {}", message);
    }
    if let Some(message) = any.downcast_ref::<String>() {
        return format!("panic: {}", message);
    }
    "panic: unknown payload".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::compaction_task::CompactionTaskState;
    use crate::compaction::plan::types::{
        CompactionPlan, CompactionPlanId, CompactionReason, CumulativePointAction, ExecutionLayout,
        MergeSemantics, PolicyKind, ReadSnapshot,
    };
    use crate::tablet::Version;
    use crate::tablet::{KeysType, Tablet, TabletColumn, TabletSchema};
    use paro_common::types::LogicalType;
    use std::time::Duration;

    fn create_test_tablet(id: u64, data_dir: &std::path::Path) -> Arc<Tablet> {
        let mut columns = Vec::new();
        columns.push(TabletColumn::new(0, "pk".to_string(), LogicalType::Integer));
        columns[0].is_key = true;
        columns.push(TabletColumn::new(1, "v".to_string(), LogicalType::Integer));
        let schema = Arc::new(TabletSchema::new(1, columns, KeysType::PrimaryKeys).unwrap());
        let tablet = Tablet::new(id, 100, 0, schema, data_dir, None).unwrap();
        tablet.init().unwrap();
        Arc::new(tablet)
    }

    struct SleepTask {
        state: CompactionTaskState,
        plan: CompactionPlan,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    impl SleepTask {
        fn new(tablet_id: u64, active: Arc<AtomicUsize>, max_active: Arc<AtomicUsize>) -> Self {
            Self {
                state: CompactionTaskState::Init,
                plan: CompactionPlan {
                    plan_id: CompactionPlanId(1),
                    tablet_id,
                    policy_kind: PolicyKind::Cumulative,
                    cumulative_point_action: CumulativePointAction::AdvanceToOutputEndExclusive,
                    execution_layout: ExecutionLayout::Horizontal,
                    merge_semantics: MergeSemantics::Deduplicate,
                    input_rowsets: Vec::new(),
                    read_snapshot: ReadSnapshot {
                        visible_version: 0,
                        layout_epoch: 0,
                        schema_epoch: None,
                    },
                    output_version: Version::singleton(0),
                    output_rowset_id: 1,
                    score: 1.0,
                    reason: CompactionReason::CumulativePolicy,
                    pk_delta_guard: None,
                },
                active,
                max_active,
            }
        }
    }

    impl CompactionTask for SleepTask {
        fn run(&mut self) -> paro_common::error::Result<()> {
            self.state = CompactionTaskState::Running;
            let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            loop {
                let max_seen = self.max_active.load(Ordering::SeqCst);
                if current <= max_seen {
                    break;
                }
                if self
                    .max_active
                    .compare_exchange(max_seen, current, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(30));
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.state = CompactionTaskState::Success;
            Ok(())
        }

        fn stop(&mut self) {}

        fn state(&self) -> CompactionTaskState {
            self.state
        }

        fn context(&self) -> &CompactionPlan {
            &self.plan
        }
    }

    #[test]
    fn scheduler_executor_respects_max_concurrency() {
        let scheduler = Arc::new(TaskScheduler::new());
        scheduler.set_threads(2).unwrap();

        let executor = CompactionExecutor::new_with_scheduler(1, scheduler.clone());
        let dir = tempfile::tempdir().unwrap();
        let tablet = create_test_tablet(1, dir.path());

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = std::sync::mpsc::channel();

        for _ in 0..3 {
            let tx = tx.clone();
            let task = Box::new(SleepTask::new(
                tablet.tablet_id(),
                active.clone(),
                max_active.clone(),
            ));
            executor.submit_with_callback(tablet.clone(), task, move |_| {
                let _ = tx.send(());
            });
        }
        drop(tx);

        for _ in 0..3 {
            rx.recv_timeout(Duration::from_secs(3))
                .expect("compaction task should finish");
        }

        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }
}
