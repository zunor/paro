// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-wide task permits.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_scheduler::task::InterruptState;

/// Query-local concurrency gate shared by pipeline, finish, replay, and
/// operator-internal tasks.
pub struct QueryTaskPermitPool {
    max_permits: AtomicUsize,
    used_permits: AtomicUsize,
    next_waiter_id: AtomicU64,
    waiters: Mutex<VecDeque<TaskPermitWaiter>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskPermitWaiterId(pub u64);

struct TaskPermitWaiter {
    id: TaskPermitWaiterId,
    interrupt: InterruptState,
}

impl fmt::Debug for QueryTaskPermitPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryTaskPermitPool")
            .field("max_permits", &self.max_permits())
            .field("used_permits", &self.used_permits())
            .field("blocked_waiters", &self.blocked_waiters())
            .finish()
    }
}

impl QueryTaskPermitPool {
    pub fn new(max_permits: usize) -> Self {
        Self {
            max_permits: AtomicUsize::new(max_permits.max(1)),
            used_permits: AtomicUsize::new(0),
            next_waiter_id: AtomicU64::new(0),
            waiters: Mutex::new(VecDeque::new()),
        }
    }

    pub fn for_current_parallelism() -> Self {
        let slots = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Self::new(slots)
    }

    pub fn max_permits(&self) -> usize {
        self.max_permits.load(Ordering::Acquire)
    }

    pub fn set_max_permits(&self, max_permits: usize) {
        self.max_permits
            .store(max_permits.max(1), Ordering::Release);
        self.wake_waiters();
    }

    pub fn used_permits(&self) -> usize {
        self.used_permits.load(Ordering::Acquire)
    }

    pub fn blocked_waiters(&self) -> usize {
        self.waiters
            .lock()
            .expect("query task-permit waiters lock poisoned")
            .len()
    }

    pub fn try_acquire(self: &Arc<Self>, interrupt: InterruptState) -> Option<QueryTaskPermit> {
        let id = TaskPermitWaiterId(self.next_waiter_id.fetch_add(1, Ordering::Relaxed));
        self.try_acquire_for(id, interrupt)
    }

    pub fn try_acquire_for(
        self: &Arc<Self>,
        waiter_id: TaskPermitWaiterId,
        interrupt: InterruptState,
    ) -> Option<QueryTaskPermit> {
        if let Some(permit) = self.try_acquire_available() {
            return Some(permit);
        }
        let mut waiters = self
            .waiters
            .lock()
            .expect("query task-permit waiters lock poisoned");
        if let Some(permit) = self.try_acquire_available() {
            return Some(permit);
        }
        if let Some(waiter) = waiters.iter_mut().find(|waiter| waiter.id == waiter_id) {
            waiter.interrupt = interrupt;
        } else {
            waiters.push_back(TaskPermitWaiter {
                id: waiter_id,
                interrupt,
            });
        }
        None
    }

    /// Acquire a permit without registering a waiter. Parallel finish uses
    /// this to form bounded waves from the permits available beside its
    /// already-admitted coordinator task.
    pub fn try_acquire_available(self: &Arc<Self>) -> Option<QueryTaskPermit> {
        let mut used = self.used_permits.load(Ordering::Acquire);
        loop {
            if used >= self.max_permits() {
                return None;
            }

            match self.used_permits.compare_exchange_weak(
                used,
                used + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(QueryTaskPermit {
                        pool: Arc::clone(self),
                    })
                }
                Err(actual) => used = actual,
            }
        }
    }

    fn release_permit(&self) {
        let _ = self
            .used_permits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            });
        self.wake_waiters();
    }

    fn wake_waiters(&self) {
        let waiter = {
            self.waiters
                .lock()
                .expect("query task-permit waiters lock poisoned")
                .pop_front()
        };
        if let Some(waiter) = waiter {
            let _ = waiter.interrupt.callback();
        }
    }
}

#[derive(Debug)]
pub struct QueryTaskPermit {
    pool: Arc<QueryTaskPermitPool>,
}

impl Drop for QueryTaskPermit {
    fn drop(&mut self) {
        self.pool.release_permit();
    }
}
