// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline worker-slot admission.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_scheduler::task::InterruptState;

/// Query-local worker-slot gate.
pub struct PipelineAdmissionController {
    max_slots: AtomicUsize,
    used_slots: AtomicUsize,
    next_waiter_id: AtomicU64,
    waiters: Mutex<VecDeque<AdmissionWaiter>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdmissionWaiterId(pub u64);

struct AdmissionWaiter {
    id: AdmissionWaiterId,
    interrupt: InterruptState,
}

impl fmt::Debug for PipelineAdmissionController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipelineAdmissionController")
            .field("max_slots", &self.max_slots())
            .field("used_slots", &self.used_slots())
            .field("blocked_waiters", &self.blocked_waiters())
            .finish()
    }
}

impl PipelineAdmissionController {
    pub fn new(max_slots: usize) -> Self {
        Self {
            max_slots: AtomicUsize::new(max_slots.max(1)),
            used_slots: AtomicUsize::new(0),
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

    pub fn max_slots(&self) -> usize {
        self.max_slots.load(Ordering::Acquire)
    }

    pub fn set_max_slots(&self, max_slots: usize) {
        self.max_slots.store(max_slots.max(1), Ordering::Release);
        self.wake_waiters();
    }

    pub fn used_slots(&self) -> usize {
        self.used_slots.load(Ordering::Acquire)
    }

    pub fn blocked_waiters(&self) -> usize {
        self.waiters
            .lock()
            .expect("pipeline admission waiters lock poisoned")
            .len()
    }

    pub fn try_acquire(
        self: &Arc<Self>,
        interrupt: InterruptState,
    ) -> Option<PipelineAdmissionGuard> {
        let id = AdmissionWaiterId(self.next_waiter_id.fetch_add(1, Ordering::Relaxed));
        self.try_acquire_for(id, interrupt)
    }

    pub fn try_acquire_for(
        self: &Arc<Self>,
        waiter_id: AdmissionWaiterId,
        interrupt: InterruptState,
    ) -> Option<PipelineAdmissionGuard> {
        let mut used = self.used_slots.load(Ordering::Acquire);
        loop {
            if used >= self.max_slots() {
                let mut waiters = self
                    .waiters
                    .lock()
                    .expect("pipeline admission waiters lock poisoned");
                if self.used_slots() < self.max_slots() {
                    used = self.used_slots.load(Ordering::Acquire);
                    continue;
                }
                if let Some(waiter) = waiters.iter_mut().find(|waiter| waiter.id == waiter_id) {
                    waiter.interrupt = interrupt;
                } else {
                    waiters.push_back(AdmissionWaiter {
                        id: waiter_id,
                        interrupt,
                    });
                }
                return None;
            }

            match self.used_slots.compare_exchange_weak(
                used,
                used + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(PipelineAdmissionGuard {
                        controller: Arc::clone(self),
                    })
                }
                Err(actual) => used = actual,
            }
        }
    }

    fn release_slot(&self) {
        let _ = self
            .used_slots
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            });
        self.wake_waiters();
    }

    fn wake_waiters(&self) {
        let waiter = {
            self.waiters
                .lock()
                .expect("pipeline admission waiters lock poisoned")
                .pop_front()
        };
        if let Some(waiter) = waiter {
            let _ = waiter.interrupt.callback();
        }
    }
}

#[derive(Debug)]
pub struct PipelineAdmissionGuard {
    controller: Arc<PipelineAdmissionController>,
}

impl Drop for PipelineAdmissionGuard {
    fn drop(&mut self) {
        self.controller.release_slot();
    }
}
