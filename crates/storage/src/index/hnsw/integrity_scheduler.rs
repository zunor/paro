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

use super::hnsw_builder::hnsw_wait_for_foreground_quiet;
use super::HnswIndex;

const INTEGRITY_TASK_PRIORITY: i32 = -20;
const MAX_PENDING_ARTIFACTS: usize = 8;
const VERIFY_CHUNKS_PER_SLICE: usize = 256;
/// Automatic sweeping is a latency optimization, not the correctness
/// boundary: foreground reads authenticate every immutable range before use.
/// Scanning a multi-gigabyte mmap can evict the external vector working set
/// and make the optimization net-negative. Keep automatic residency work
/// bounded; large artifacts remain demand-verified and can be exhaustively
/// checked through the explicit integrity tooling.
const MAX_AUTOMATIC_VERIFY_BYTES: usize = 32 * 1024 * 1024;
const FOREGROUND_QUIET_WAIT: std::time::Duration = std::time::Duration::from_millis(25);

fn should_automatically_verify(protected_len: usize) -> bool {
    protected_len <= MAX_AUTOMATIC_VERIFY_BYTES
}

struct IntegrityJob {
    index: Weak<HnswIndex>,
    cursor: usize,
}

struct IntegritySchedulerState {
    producer: ProducerToken,
    queue: Mutex<VecDeque<IntegrityJob>>,
    outstanding: AtomicUsize,
    worker_scheduled: AtomicBool,
    respect_foreground_pressure: bool,
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
        Self {
            state: Arc::new(IntegritySchedulerState {
                producer: scheduler.create_producer_with_priority(INTEGRITY_TASK_PRIORITY),
                queue: Mutex::new(VecDeque::new()),
                outstanding: AtomicUsize::new(0),
                worker_scheduled: AtomicBool::new(false),
                respect_foreground_pressure: true,
            }),
        }
    }

    #[cfg(test)]
    fn new_without_foreground_governor(scheduler: Arc<TaskScheduler>) -> Self {
        Self {
            state: Arc::new(IntegritySchedulerState {
                producer: scheduler.create_producer_with_priority(INTEGRITY_TASK_PRIORITY),
                queue: Mutex::new(VecDeque::new()),
                outstanding: AtomicUsize::new(0),
                worker_scheduled: AtomicBool::new(false),
                // Completion/lifetime tests need a deterministic scheduler
                // seam: unrelated parallel HNSW tests legitimately hold the
                // process-wide foreground reservation.
                respect_foreground_pressure: false,
            }),
        }
    }

    pub(crate) fn schedule(&self, index: &Arc<HnswIndex>) {
        let Some(integrity) = index.artifact_integrity() else {
            return;
        };
        if !should_automatically_verify(integrity.protected_len()) {
            return;
        }
        if !index.try_mark_integrity_scheduled() {
            return;
        }
        if self
            .state
            .outstanding
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < MAX_PENDING_ARTIFACTS).then_some(current + 1)
            })
            .is_err()
        {
            index.clear_integrity_scheduled();
            storage_metrics().record_hnsw_integrity_deferred();
            return;
        }

        self.state.queue.lock().push_back(IntegrityJob {
            index: Arc::downgrade(index),
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
}

impl Task for IntegritySweepTask {
    fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        if self.state.respect_foreground_pressure
            && !hnsw_wait_for_foreground_quiet(FOREGROUND_QUIET_WAIT)
        {
            return Ok(TaskExecutionResult::NotFinished);
        }
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

            match integrity.verify_batch(&mut job.cursor, VERIFY_CHUNKS_PER_SLICE) {
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

    #[test]
    fn automatic_verification_has_a_bounded_residency_budget() {
        assert!(should_automatically_verify(MAX_AUTOMATIC_VERIFY_BYTES));
        assert!(!should_automatically_verify(MAX_AUTOMATIC_VERIFY_BYTES + 1));
    }

    #[test]
    #[serial_test::serial]
    fn governed_sweep_completes_without_retaining_retired_artifacts() {
        let scheduler = Arc::new(TaskScheduler::new());
        let service =
            HnswIntegrityScheduler::new_without_foreground_governor(Arc::clone(&scheduler));
        let marker = AtomicBool::new(true);
        let before = storage_metrics().snapshot();

        let index = loaded_index();
        let integrity = index.artifact_integrity().unwrap();
        service.schedule(&index);
        for _ in 0..16 {
            scheduler.execute_tasks(&marker, 1);
            if integrity.is_fully_verified() {
                break;
            }
        }
        assert!(integrity.is_fully_verified());

        let retired = loaded_index();
        service.schedule(&retired);
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
    fn governed_sweep_parks_while_a_foreground_query_is_reserved() {
        let scheduler = Arc::new(TaskScheduler::new());
        let service = HnswIntegrityScheduler::new(Arc::clone(&scheduler));
        let marker = AtomicBool::new(true);
        let index = loaded_index();
        let integrity = index.artifact_integrity().unwrap();
        service.schedule(&index);

        let query = super::super::hnsw_builder::HnswForegroundQueryGuard::enter();
        scheduler.execute_tasks(&marker, 1);
        assert!(!integrity.is_fully_verified());
        drop(query);

        for _ in 0..16 {
            scheduler.execute_tasks(&marker, 1);
            if integrity.is_fully_verified() {
                break;
            }
        }
        assert!(integrity.is_fully_verified());
    }
}
