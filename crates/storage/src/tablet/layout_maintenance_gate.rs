// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Coordination between immutable-layout builds and physical rowset rewrites.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use paro_common::error::{self as paro_error, Result};

const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Default)]
struct GateState {
    shared_holders: usize,
    exclusive_owner: Option<u64>,
    exclusive_holders: usize,
    exclusive_waiters: usize,
}

#[derive(Debug, Default)]
struct GateInner {
    state: Mutex<GateState>,
    changed: Condvar,
}

/// Per-tablet gate for operations whose artifact embeds physical rowset ids.
///
/// Compaction owns a shared lease from snapshot through durable publication.
/// A staged search-generation build owns the exclusive lease until its
/// transaction publishes or aborts. This makes the artifact's rowset mapping
/// a structural invariant instead of a post-build best-effort validation.
#[derive(Debug, Clone, Default)]
pub struct LayoutMaintenanceGate {
    inner: Arc<GateInner>,
}

impl LayoutMaintenanceGate {
    /// Foreground storage publication waits for the stable-layout owner.
    ///
    /// DML acquires this lease during commit preparation, before transaction
    /// locks are released, and keeps it through physical tablet publication.
    /// This bridges the durable-append/apply window in which SQL locks no
    /// longer protect the rowset layout.
    pub fn acquire_shared(&self, should_stop: impl Fn() -> bool) -> Result<LayoutMaintenanceLease> {
        let mut state = self.lock_state("acquire shared layout lease");
        while state.exclusive_owner.is_some() || state.exclusive_waiters != 0 {
            if should_stop() {
                return Err(paro_error::query_canceled());
            }
            state = match self.inner.changed.wait_timeout(state, CANCEL_POLL_INTERVAL) {
                Ok(waited) => waited.0,
                Err(poisoned) => {
                    tracing::error!(
                        "recovering poisoned layout maintenance gate while waiting for shared lease"
                    );
                    poisoned.into_inner().0
                }
            };
        }
        if should_stop() {
            return Err(paro_error::query_canceled());
        }
        state.shared_holders = state.shared_holders.checked_add(1).ok_or_else(|| {
            paro_error::internal("layout maintenance shared-holder count overflow")
        })?;
        Ok(LayoutMaintenanceLease {
            inner: Arc::clone(&self.inner),
            mode: LeaseMode::Shared,
        })
    }

    /// Compaction is background work and must yield rather than queue behind a
    /// potentially long foreground CREATE INDEX build.
    pub fn try_acquire_shared(&self) -> Result<Option<LayoutMaintenanceLease>> {
        let mut state = self.lock_state("try acquire shared layout lease");
        if state.exclusive_owner.is_some() || state.exclusive_waiters != 0 {
            return Ok(None);
        }
        state.shared_holders = state.shared_holders.checked_add(1).ok_or_else(|| {
            paro_error::internal("layout maintenance shared-holder count overflow")
        })?;
        Ok(Some(LayoutMaintenanceLease {
            inner: Arc::clone(&self.inner),
            mode: LeaseMode::Shared,
        }))
    }

    pub fn acquire_exclusive(
        &self,
        owner_id: u64,
        should_stop: impl Fn() -> bool,
    ) -> Result<LayoutMaintenanceLease> {
        let mut state = self.lock_state("acquire exclusive layout lease");
        if state.exclusive_owner == Some(owner_id) {
            state.exclusive_holders = state.exclusive_holders.checked_add(1).ok_or_else(|| {
                paro_error::internal("layout maintenance exclusive-holder count overflow")
            })?;
            return Ok(LayoutMaintenanceLease {
                inner: Arc::clone(&self.inner),
                mode: LeaseMode::Exclusive { owner_id },
            });
        }
        state.exclusive_waiters = state.exclusive_waiters.saturating_add(1);
        while state.exclusive_owner.is_some() || state.shared_holders != 0 {
            if should_stop() {
                state.exclusive_waiters = state.exclusive_waiters.saturating_sub(1);
                self.inner.changed.notify_all();
                return Err(paro_error::query_canceled());
            }
            state = match self.inner.changed.wait_timeout(state, CANCEL_POLL_INTERVAL) {
                Ok(waited) => waited.0,
                Err(poisoned) => {
                    tracing::error!(
                        "recovering poisoned layout maintenance gate while waiting for exclusive lease"
                    );
                    poisoned.into_inner().0
                }
            };
        }
        if should_stop() {
            state.exclusive_waiters = state.exclusive_waiters.saturating_sub(1);
            self.inner.changed.notify_all();
            return Err(paro_error::query_canceled());
        }
        state.exclusive_waiters = state.exclusive_waiters.saturating_sub(1);
        state.exclusive_owner = Some(owner_id);
        state.exclusive_holders = 1;
        Ok(LayoutMaintenanceLease {
            inner: Arc::clone(&self.inner),
            mode: LeaseMode::Exclusive { owner_id },
        })
    }

    fn lock_state(&self, operation: &'static str) -> MutexGuard<'_, GateState> {
        match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                tracing::error!(operation, "recovering poisoned layout maintenance gate");
                poisoned.into_inner()
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LeaseMode {
    Shared,
    Exclusive { owner_id: u64 },
}

#[derive(Debug)]
pub struct LayoutMaintenanceLease {
    inner: Arc<GateInner>,
    mode: LeaseMode,
}

impl Drop for LayoutMaintenanceLease {
    fn drop(&mut self) {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                tracing::error!(
                    "recovering poisoned layout maintenance gate while releasing lease"
                );
                poisoned.into_inner()
            }
        };
        match self.mode {
            LeaseMode::Shared => {
                state.shared_holders = state.shared_holders.saturating_sub(1);
            }
            LeaseMode::Exclusive { owner_id } => {
                if state.exclusive_owner != Some(owner_id) || state.exclusive_holders == 0 {
                    tracing::error!(
                        owner_id,
                        actual_owner = ?state.exclusive_owner,
                        holders = state.exclusive_holders,
                        "layout maintenance exclusive lease ownership invariant violated"
                    );
                } else {
                    state.exclusive_holders -= 1;
                    if state.exclusive_holders == 0 {
                        state.exclusive_owner = None;
                    }
                }
            }
        }
        self.inner.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn exclusive_waiter_prevents_new_compaction_admission() {
        let gate = LayoutMaintenanceGate::default();
        let shared = gate
            .try_acquire_shared()
            .unwrap()
            .expect("initial shared lease");
        let gate_for_thread = gate.clone();
        let started = Arc::new(AtomicBool::new(false));
        let started_for_thread = Arc::clone(&started);
        let join = std::thread::spawn(move || {
            started_for_thread.store(true, Ordering::Release);
            gate_for_thread.acquire_exclusive(7, || false).unwrap()
        });
        while !started.load(Ordering::Acquire) {
            std::thread::yield_now();
        }
        while gate.try_acquire_shared().unwrap().is_some() {
            std::thread::yield_now();
        }
        drop(shared);
        let exclusive = join.join().unwrap();
        assert!(gate.try_acquire_shared().unwrap().is_none());
        drop(exclusive);
        assert!(gate.try_acquire_shared().unwrap().is_some());
    }

    #[test]
    fn exclusive_wait_is_cancellable() {
        let gate = LayoutMaintenanceGate::default();
        let _shared = gate.try_acquire_shared().unwrap().unwrap();
        let err = gate.acquire_exclusive(7, || true).unwrap_err();
        assert!(err.is_query_canceled());
    }

    #[test]
    fn shared_wait_is_cancellable() {
        let gate = LayoutMaintenanceGate::default();
        let _exclusive = gate.acquire_exclusive(7, || false).unwrap();
        let err = gate.acquire_shared(|| true).unwrap_err();
        assert!(err.is_query_canceled());
    }

    #[test]
    fn poisoned_state_is_recovered_consistently() {
        let gate = LayoutMaintenanceGate::default();
        let inner = Arc::clone(&gate.inner);
        let _ = std::thread::spawn(move || {
            let _state = inner.state.lock().unwrap();
            panic!("poison gate for recovery test");
        })
        .join();

        let lease = gate
            .try_acquire_shared()
            .unwrap()
            .expect("poison recovery must not permanently close the gate");
        drop(lease);
        let exclusive = gate.acquire_exclusive(9, || false).unwrap();
        drop(exclusive);
    }

    #[test]
    fn exclusive_lease_is_reentrant_for_the_same_transaction_owner() {
        let gate = LayoutMaintenanceGate::default();
        let first = gate.acquire_exclusive(11, || false).unwrap();
        let second = gate.acquire_exclusive(11, || false).unwrap();
        assert!(gate.try_acquire_shared().unwrap().is_none());
        drop(first);
        assert!(gate.try_acquire_shared().unwrap().is_none());
        drop(second);
        assert!(gate.try_acquire_shared().unwrap().is_some());
    }

    #[test]
    fn foreground_shared_publish_waits_for_exclusive_layout_owner() {
        let gate = LayoutMaintenanceGate::default();
        let exclusive = gate.acquire_exclusive(11, || false).unwrap();
        let admitted = Arc::new(AtomicBool::new(false));
        let admitted_for_thread = Arc::clone(&admitted);
        let gate_for_thread = gate.clone();
        let join = std::thread::spawn(move || {
            let lease = gate_for_thread.acquire_shared(|| false).unwrap();
            admitted_for_thread.store(true, Ordering::Release);
            lease
        });

        std::thread::yield_now();
        assert!(!admitted.load(Ordering::Acquire));
        drop(exclusive);
        let shared = join.join().unwrap();
        assert!(admitted.load(Ordering::Acquire));
        drop(shared);
    }
}
