// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Instance-governed background authentication for immutable HNSW artifacts.
//!
//! Lazy range authentication remains the correctness boundary for foreground
//! reads. This service only removes its steady-state per-range overhead. One
//! low-priority scheduler task processes bounded sequential slices, so checksum
//! I/O is cancellable with the instance, cannot spawn process-global threads,
//! and yields between slices to foreground work.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use paro_common::error::Result;
use paro_scheduler::scheduler::TaskScheduler;
use paro_scheduler::task::{ProducerToken, Task, TaskExecutionMode, TaskExecutionResult};

use crate::metrics::storage_metrics;

use super::hnsw_builder::HnswQueryActivity;
use super::HnswIndex;

const DEFAULT_INTEGRITY_TASK_PRIORITY: i32 = -20;
const DEFAULT_MAX_PENDING_ARTIFACTS: usize = 8;
pub(crate) const HNSW_INTEGRITY_CHUNKS_PER_SLICE: usize = 256;
/// Automatic sweeping is a latency optimization, not the correctness
/// boundary: foreground reads authenticate every immutable range before use.
/// Scanning a multi-gigabyte mmap can evict the external vector working set
/// and make the optimization net-negative. Keep automatic residency work
/// bounded; large artifacts remain demand-verified and can be exhaustively
/// checked through the explicit integrity tooling.
const DEFAULT_MAX_AUTOMATIC_VERIFY_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_DEFINITION_IDLE: std::time::Duration = std::time::Duration::from_millis(25);

/// Explicit instance policy for optional whole-artifact authentication.
///
/// Lazy range verification is always the correctness boundary. These knobs
/// govern only how aggressively the instance makes immutable checksum state
/// resident in advance of a query; publication never waits for this service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HnswIntegritySchedulerConfig {
    pub task_priority: i32,
    pub max_pending_artifacts: usize,
    pub chunks_per_slice: usize,
    pub max_automatic_verify_bytes: usize,
    pub definition_idle: std::time::Duration,
}

impl Default for HnswIntegritySchedulerConfig {
    fn default() -> Self {
        Self {
            task_priority: DEFAULT_INTEGRITY_TASK_PRIORITY,
            max_pending_artifacts: DEFAULT_MAX_PENDING_ARTIFACTS,
            chunks_per_slice: HNSW_INTEGRITY_CHUNKS_PER_SLICE,
            max_automatic_verify_bytes: DEFAULT_MAX_AUTOMATIC_VERIFY_BYTES,
            definition_idle: DEFAULT_DEFINITION_IDLE,
        }
    }
}

struct IntegrityJob {
    index: Weak<HnswIndex>,
    query_activity: Option<Weak<HnswQueryActivity>>,
    on_failure: Arc<dyn Fn() + Send + Sync>,
    cursor: usize,
}

struct IntegritySchedulerState {
    producer: ProducerToken,
    queue: Mutex<VecDeque<IntegrityJob>>,
    outstanding: AtomicUsize,
    worker_scheduled: AtomicBool,
    config: HnswIntegritySchedulerConfig,
}

/// One instance-owned integrity service shared by every database and table in
/// that instance. The service does not retain artifacts: a retired generation
/// disappears before its queued job runs and is counted as stale work.
#[derive(Clone)]
pub struct HnswIntegrityScheduler {
    state: Arc<IntegritySchedulerState>,
}

impl std::fmt::Debug for HnswIntegrityScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswIntegrityScheduler")
            .field(
                "outstanding",
                &self.state.outstanding.load(Ordering::Acquire),
            )
            .finish()
    }
}

impl HnswIntegrityScheduler {
    pub fn new(scheduler: Arc<TaskScheduler>) -> Self {
        Self::with_config(scheduler, HnswIntegritySchedulerConfig::default())
    }

    pub fn with_config(
        scheduler: Arc<TaskScheduler>,
        config: HnswIntegritySchedulerConfig,
    ) -> Self {
        Self {
            state: Arc::new(IntegritySchedulerState {
                producer: scheduler.create_producer_with_priority(config.task_priority),
                queue: Mutex::new(VecDeque::new()),
                outstanding: AtomicUsize::new(0),
                worker_scheduled: AtomicBool::new(false),
                config,
            }),
        }
    }

    pub fn config(&self) -> HnswIntegritySchedulerConfig {
        self.state.config
    }

    #[cfg(test)]
    fn new_without_definition_idle(scheduler: Arc<TaskScheduler>) -> Self {
        Self {
            state: Arc::new(IntegritySchedulerState {
                producer: scheduler.create_producer_with_priority(DEFAULT_INTEGRITY_TASK_PRIORITY),
                queue: Mutex::new(VecDeque::new()),
                outstanding: AtomicUsize::new(0),
                worker_scheduled: AtomicBool::new(false),
                config: HnswIntegritySchedulerConfig {
                    definition_idle: std::time::Duration::ZERO,
                    ..HnswIntegritySchedulerConfig::default()
                },
            }),
        }
    }

    pub(crate) fn schedule(
        &self,
        index: &Arc<HnswIndex>,
        query_activity: Option<Arc<HnswQueryActivity>>,
        on_failure: Arc<dyn Fn() + Send + Sync>,
    ) {
        let Some(integrity) = index.artifact_integrity() else {
            return;
        };
        if integrity.is_fully_verified() {
            return;
        }
        if integrity.is_corrupt() {
            if index.try_mark_integrity_scheduled() {
                storage_metrics().record_hnsw_integrity_failed();
                on_failure();
            }
            return;
        }
        if integrity.protected_len() > self.state.config.max_automatic_verify_bytes {
            return;
        }
        if !index.try_mark_integrity_scheduled() {
            return;
        }
        if self
            .state
            .outstanding
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.state.config.max_pending_artifacts).then_some(current + 1)
            })
            .is_err()
        {
            index.clear_integrity_scheduled();
            storage_metrics().record_hnsw_integrity_deferred();
            return;
        }

        self.state.queue.lock().push_back(IntegrityJob {
            index: Arc::downgrade(index),
            query_activity: query_activity.map(|activity| Arc::downgrade(&activity)),
            on_failure,
            cursor: 0,
        });
        storage_metrics().record_hnsw_integrity_scheduled();
        if !self.state.worker_scheduled.swap(true, Ordering::AcqRel) {
            let task: Arc<Mutex<dyn Task>> = Arc::new(Mutex::new(IntegritySweepTask {
                state: Arc::clone(&self.state),
                current: None,
            }));
            self.state.producer.schedule_task(task);
        }
    }
}

struct IntegritySweepTask {
    state: Arc<IntegritySchedulerState>,
    current: Option<IntegrityJob>,
}

impl IntegritySweepTask {
    fn take_next_job(&mut self) -> bool {
        if self.current.is_some() {
            return true;
        }
        let mut queue = self.state.queue.lock();
        if let Some(job) = queue.pop_front() {
            self.current = Some(job);
            true
        } else {
            // `schedule` also takes `queue` before testing this flag. Publishing
            // false while holding the queue lock closes the empty-to-enqueue
            // race without another process-global wakeup primitive.
            self.state.worker_scheduled.store(false, Ordering::Release);
            false
        }
    }

    fn finish_job(&mut self) {
        self.current = None;
        let previous = self.state.outstanding.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "HNSW integrity outstanding count underflow");
    }

    fn defer_current_job(&mut self) {
        if let Some(job) = self.current.take() {
            self.state.queue.lock().push_back(job);
        }
    }
}

impl Task for IntegritySweepTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        loop {
            if !self.take_next_job() {
                return Ok(TaskExecutionResult::Finished);
            }
            let Some(job) = self.current.as_mut() else {
                unreachable!("HNSW integrity task lost its current job")
            };
            let Some(index) = job.index.upgrade() else {
                storage_metrics().record_hnsw_integrity_stale();
                self.finish_job();
                continue;
            };
            let Some(integrity) = index.artifact_integrity() else {
                storage_metrics().record_hnsw_integrity_stale();
                self.finish_job();
                continue;
            };
            if let Some(activity) = job
                .query_activity
                .as_ref()
                .and_then(Weak::upgrade)
                .filter(|activity| !activity.quiet_for(self.state.config.definition_idle))
            {
                // Rotate instead of parking the process-wide worker. A hot
                // definition must not prevent unrelated artifacts from using
                // the same instance-governed bandwidth lane.
                let has_other_work = !self.state.queue.lock().is_empty();
                if !has_other_work {
                    let _ = activity.wait_for_quiet(
                        self.state.config.definition_idle,
                        self.state.config.definition_idle,
                    );
                }
                self.defer_current_job();
                return Ok(TaskExecutionResult::NotFinished);
            }

            match integrity.verify_batch(&mut job.cursor, self.state.config.chunks_per_slice.max(1))
            {
                Ok(progress) => {
                    storage_metrics()
                        .record_hnsw_integrity_verified_bytes(progress.bytes_covered as u64);
                    if progress.complete {
                        storage_metrics().record_hnsw_integrity_completed();
                        self.finish_job();
                    }
                    // Requeue after every physical slice, including the final
                    // slice when another artifact is waiting. This is the I/O
                    // governance boundary; high-priority work can run first.
                    return Ok(TaskExecutionResult::NotFinished);
                }
                Err(error) => {
                    storage_metrics().record_hnsw_integrity_failed();
                    (job.on_failure)();
                    tracing::error!(
                        error = %error,
                        "governed HNSW artifact integrity verification failed"
                    );
                    self.finish_job();
                    return Ok(TaskExecutionResult::NotFinished);
                }
            }
        }
    }

    fn task_type(&self) -> &str {
        "HnswIntegritySweepTask"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::index::hnsw::{DistanceMetric, HnswConfig, InMemoryVectorStorage, VectorStorage};

    fn loaded_index() -> Arc<HnswIndex> {
        let storage: Arc<dyn VectorStorage> = Arc::new(InMemoryVectorStorage::new(
            vec![0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0],
            2,
        ));
        let built = HnswIndex::build(storage, HnswConfig::default(), DistanceMetric::Euclidean);
        Arc::new(HnswIndex::deserialize(&built.serialize().unwrap()).unwrap())
    }

    fn ignore_failure() -> Arc<dyn Fn() + Send + Sync> {
        Arc::new(|| {})
    }

    #[test]
    fn automatic_verification_has_a_bounded_residency_budget() {
        let config = HnswIntegritySchedulerConfig::default();
        assert_eq!(
            config.max_automatic_verify_bytes,
            DEFAULT_MAX_AUTOMATIC_VERIFY_BYTES
        );

        let scheduler = Arc::new(TaskScheduler::new());
        let service = HnswIntegrityScheduler::with_config(
            Arc::clone(&scheduler),
            HnswIntegritySchedulerConfig {
                max_automatic_verify_bytes: 0,
                ..config
            },
        );
        let index = loaded_index();
        service.schedule(&index, None, ignore_failure());
        scheduler.execute_tasks(&AtomicBool::new(true), 1);
        assert!(!index.artifact_integrity().unwrap().is_fully_verified());
    }

    #[test]
    #[serial_test::serial]
    fn governed_sweep_completes_without_retaining_retired_artifacts() {
        let scheduler = Arc::new(TaskScheduler::new());
        let service = HnswIntegrityScheduler::new_without_definition_idle(Arc::clone(&scheduler));
        let marker = AtomicBool::new(true);
        let before = storage_metrics().snapshot();

        let index = loaded_index();
        let integrity = index.artifact_integrity().unwrap();
        service.schedule(&index, None, ignore_failure());
        for _ in 0..16 {
            scheduler.execute_tasks(&marker, 1);
            if integrity.is_fully_verified() {
                break;
            }
        }
        assert!(integrity.is_fully_verified());

        let retired = loaded_index();
        service.schedule(&retired, None, ignore_failure());
        drop(retired);
        scheduler.execute_tasks(&marker, 4);

        let after = storage_metrics().snapshot();
        assert_eq!(
            after.search_hnsw_integrity_scheduled_total,
            before.search_hnsw_integrity_scheduled_total + 2
        );
        assert_eq!(
            after.search_hnsw_integrity_completed_total,
            before.search_hnsw_integrity_completed_total + 1
        );
        assert_eq!(
            after.search_hnsw_integrity_stale_total,
            before.search_hnsw_integrity_stale_total + 1
        );
    }

    #[test]
    #[serial_test::serial]
    fn governed_sweep_defers_only_the_active_definition() {
        let scheduler = Arc::new(TaskScheduler::new());
        let service = HnswIntegrityScheduler::new(Arc::clone(&scheduler));
        let marker = AtomicBool::new(true);
        let index = loaded_index();
        let integrity = index.artifact_integrity().unwrap();
        let activity = Arc::new(HnswQueryActivity::default());
        service.schedule(&index, Some(Arc::clone(&activity)), ignore_failure());

        let query = activity.enter();
        scheduler.execute_tasks(&marker, 1);
        assert!(!integrity.is_fully_verified());
        drop(query);
    }

    #[test]
    #[serial_test::serial]
    fn active_definition_does_not_block_an_unrelated_artifact() {
        let scheduler = Arc::new(TaskScheduler::new());
        let service = HnswIntegrityScheduler::with_config(
            Arc::clone(&scheduler),
            HnswIntegritySchedulerConfig {
                definition_idle: std::time::Duration::from_secs(1),
                ..HnswIntegritySchedulerConfig::default()
            },
        );
        let marker = AtomicBool::new(true);
        let active_index = loaded_index();
        let unrelated_index = loaded_index();
        let active_integrity = active_index.artifact_integrity().unwrap();
        let unrelated_integrity = unrelated_index.artifact_integrity().unwrap();
        let activity = Arc::new(HnswQueryActivity::default());
        let query = activity.enter();
        service.schedule(&active_index, Some(Arc::clone(&activity)), ignore_failure());
        service.schedule(&unrelated_index, None, ignore_failure());

        scheduler.execute_tasks(&marker, 2);
        assert!(!active_integrity.is_fully_verified());
        assert!(unrelated_integrity.is_fully_verified());
        drop(query);
    }
}
