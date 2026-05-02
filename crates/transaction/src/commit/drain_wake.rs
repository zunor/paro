// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Instance-level commit drain wake handle.

use super::DrainSignalReason;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;

pub type CommitDrainWakeCallback = Arc<dyn Fn(DrainSignalReason, usize) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct CommitDrainWakeHandle {
    inner: Weak<CommitDrainWakePoolInner>,
    callback: CommitDrainWakeCallback,
}

impl CommitDrainWakeHandle {
    pub fn signal(&self, reason: DrainSignalReason) {
        if let Some(inner) = self.inner.upgrade() {
            inner.enqueue(WakeTask {
                reason,
                callback: Arc::clone(&self.callback),
            });
        }
    }

    pub fn metrics(&self) -> Option<CommitDrainWakePoolMetrics> {
        self.inner.upgrade().map(|inner| inner.metrics())
    }
}

impl fmt::Debug for CommitDrainWakeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitDrainWakeHandle")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitDrainWakePoolOptions {
    pub worker_threads: usize,
    pub blocking_spare_threads: usize,
    pub drain_batches_per_turn: usize,
}

impl Default for CommitDrainWakePoolOptions {
    fn default() -> Self {
        Self {
            worker_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .clamp(1, 4),
            blocking_spare_threads: 1,
            drain_batches_per_turn: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitDrainWakePoolMetrics {
    pub enqueued_count: u64,
    pub dequeued_count: u64,
    pub dropped_count: u64,
    pub ready_depth: u64,
}

#[derive(Clone)]
pub struct CommitDrainWakePool {
    inner: Arc<CommitDrainWakePoolInner>,
}

impl CommitDrainWakePool {
    pub fn new(options: CommitDrainWakePoolOptions) -> Self {
        let worker_threads = options
            .worker_threads
            .max(1)
            .saturating_add(options.blocking_spare_threads);
        let inner = Arc::new(CommitDrainWakePoolInner {
            state: Mutex::new(WakePoolState::default()),
            wake: Condvar::new(),
            options,
            metrics: CommitDrainWakePoolMetricsCells::default(),
        });
        for worker_id in 0..worker_threads {
            let worker_inner = Arc::clone(&inner);
            thread::Builder::new()
                .name(format!("paro-commit-drain-wake-{worker_id}"))
                .spawn(move || run_wake_worker(worker_inner))
                .expect("spawn commit drain wake worker");
        }
        Self { inner }
    }

    pub fn handle(&self, callback: CommitDrainWakeCallback) -> CommitDrainWakeHandle {
        CommitDrainWakeHandle {
            inner: Arc::downgrade(&self.inner),
            callback,
        }
    }

    pub fn metrics(&self) -> CommitDrainWakePoolMetrics {
        self.inner.metrics()
    }
}

impl Drop for CommitDrainWakePool {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("commit wake pool mutex poisoned");
        state.shutdown = true;
        drop(state);
        self.inner.wake.notify_all();
    }
}

impl fmt::Debug for CommitDrainWakePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitDrainWakePool")
            .field("options", &self.inner.options)
            .finish_non_exhaustive()
    }
}

struct CommitDrainWakePoolInner {
    state: Mutex<WakePoolState>,
    wake: Condvar,
    options: CommitDrainWakePoolOptions,
    metrics: CommitDrainWakePoolMetricsCells,
}

impl CommitDrainWakePoolInner {
    fn enqueue(&self, task: WakeTask) {
        let mut state = self.state.lock().expect("commit wake pool mutex poisoned");
        if state.shutdown {
            self.metrics.dropped_count.fetch_add(1, Ordering::Relaxed);
            return;
        }
        state.ready.push_back(task);
        self.metrics.enqueued_count.fetch_add(1, Ordering::Relaxed);
        self.wake.notify_one();
    }

    fn metrics(&self) -> CommitDrainWakePoolMetrics {
        let ready_depth = self
            .state
            .lock()
            .expect("commit wake pool mutex poisoned")
            .ready
            .len() as u64;
        CommitDrainWakePoolMetrics {
            enqueued_count: self.metrics.enqueued_count.load(Ordering::Relaxed),
            dequeued_count: self.metrics.dequeued_count.load(Ordering::Relaxed),
            dropped_count: self.metrics.dropped_count.load(Ordering::Relaxed),
            ready_depth,
        }
    }
}

#[derive(Default)]
struct WakePoolState {
    ready: VecDeque<WakeTask>,
    shutdown: bool,
}

#[derive(Default)]
struct CommitDrainWakePoolMetricsCells {
    enqueued_count: AtomicU64,
    dequeued_count: AtomicU64,
    dropped_count: AtomicU64,
}

struct WakeTask {
    reason: DrainSignalReason,
    callback: CommitDrainWakeCallback,
}

fn run_wake_worker(inner: Arc<CommitDrainWakePoolInner>) {
    loop {
        let task = {
            let mut state = inner.state.lock().expect("commit wake pool mutex poisoned");
            loop {
                if let Some(task) = state.ready.pop_front() {
                    inner.metrics.dequeued_count.fetch_add(1, Ordering::Relaxed);
                    break task;
                }
                if state.shutdown {
                    return;
                }
                state = inner
                    .wake
                    .wait(state)
                    .expect("commit wake pool condvar poisoned");
            }
        };
        (task.callback)(task.reason, inner.options.drain_batches_per_turn.max(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[test]
    fn wake_pool_spare_worker_runs_fast_database_while_one_callback_blocks() {
        let pool = CommitDrainWakePool::new(CommitDrainWakePoolOptions {
            worker_threads: 1,
            blocking_spare_threads: 1,
            drain_batches_per_turn: 1,
        });
        let first_started = Arc::new(AtomicBool::new(false));
        let release_first = Arc::new(AtomicBool::new(false));
        let second_ran = Arc::new(AtomicUsize::new(0));

        let slow = pool.handle({
            let first_started = Arc::clone(&first_started);
            let release_first = Arc::clone(&release_first);
            Arc::new(move |_, _| {
                first_started.store(true, Ordering::Release);
                while !release_first.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        });
        let fast = pool.handle({
            let second_ran = Arc::clone(&second_ran);
            Arc::new(move |_, _| {
                second_ran.fetch_add(1, Ordering::AcqRel);
            })
        });

        slow.signal(DrainSignalReason::Enqueued);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !first_started.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(first_started.load(Ordering::Acquire));

        fast.signal(DrainSignalReason::Enqueued);
        let deadline = Instant::now() + Duration::from_secs(1);
        while second_ran.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        release_first.store(true, Ordering::Release);

        assert_eq!(second_ran.load(Ordering::Acquire), 1);
        let metrics = pool.metrics();
        assert!(metrics.enqueued_count >= 2);
        assert!(metrics.dequeued_count >= 2);
    }

    #[test]
    fn wake_handle_does_not_run_callback_after_pool_shutdown() {
        let callback_count = Arc::new(AtomicUsize::new(0));
        let handle = {
            let pool = CommitDrainWakePool::new(CommitDrainWakePoolOptions {
                worker_threads: 1,
                blocking_spare_threads: 0,
                drain_batches_per_turn: 1,
            });
            pool.handle({
                let callback_count = Arc::clone(&callback_count);
                Arc::new(move |_, _| {
                    callback_count.fetch_add(1, Ordering::AcqRel);
                })
            })
        };

        handle.signal(DrainSignalReason::Shutdown);
        let deadline = Instant::now() + Duration::from_secs(1);
        while handle.metrics().is_some() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(callback_count.load(Ordering::Acquire), 0);
        assert!(handle.metrics().is_none());
    }

    #[test]
    fn wake_pool_passes_drain_turn_budget_to_callback() {
        let pool = CommitDrainWakePool::new(CommitDrainWakePoolOptions {
            worker_threads: 1,
            blocking_spare_threads: 0,
            drain_batches_per_turn: 7,
        });
        let observed = Arc::new(AtomicUsize::new(0));
        let handle = pool.handle({
            let observed = Arc::clone(&observed);
            Arc::new(move |_, budget| {
                observed.store(budget, Ordering::Release);
            })
        });

        handle.signal(DrainSignalReason::Enqueued);
        let deadline = Instant::now() + Duration::from_secs(1);
        while observed.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(observed.load(Ordering::Acquire), 7);
    }
}
