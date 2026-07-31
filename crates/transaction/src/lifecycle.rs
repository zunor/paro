// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Linearization boundary shared by sharded lifecycle registries.

use crate::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct RegistryLifecycle {
    barrier: RwLock<()>,
    active_mutations: AtomicU64,
    dirty_epoch: AtomicU64,
    confirmed_epoch: AtomicU64,
}

pub(crate) struct RegistryMutation<'a> {
    lifecycle: &'a RegistryLifecycle,
    _barrier: RwLockReadGuard<'a, ()>,
    changed: bool,
}

pub(crate) struct RegistrySnapshot<'a> {
    _barrier: RwLockWriteGuard<'a, ()>,
}

impl RegistryLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            barrier: RwLock::new(()),
            active_mutations: AtomicU64::new(0),
            dirty_epoch: AtomicU64::new(0),
            confirmed_epoch: AtomicU64::new(u64::MAX),
        }
    }

    #[inline]
    pub(crate) fn begin_mutation(&self) -> RegistryMutation<'_> {
        let barrier = self.barrier.read();
        self.active_mutations.fetch_add(1, Ordering::AcqRel);
        RegistryMutation {
            lifecycle: self,
            _barrier: barrier,
            changed: false,
        }
    }

    /// Read a cached multi-atomic value without serializing concurrent
    /// mutations. The shared barrier excludes full registry scans; the mutation
    /// counter and dirty epoch detect an overlapping incremental publication.
    pub(crate) fn read_consistent<T>(&self, mut read: impl FnMut() -> T) -> T {
        const OPTIMISTIC_ATTEMPTS: usize = 64;

        let observation = self.barrier.read();
        for _ in 0..OPTIMISTIC_ATTEMPTS {
            if self.active_mutations.load(Ordering::Acquire) != 0 {
                std::thread::yield_now();
                continue;
            }

            let dirty_epoch = self.dirty_epoch();
            let value = read();
            if self.active_mutations.load(Ordering::Acquire) == 0
                && self.dirty_epoch() == dirty_epoch
            {
                return value;
            }
        }
        drop(observation);

        // A steady mutation stream should not starve correctness-sensitive
        // readers. Fall back to the same exclusive boundary used by a scan.
        let _snapshot = self.barrier.write();
        read()
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> RegistrySnapshot<'_> {
        RegistrySnapshot {
            _barrier: self.barrier.write(),
        }
    }

    #[inline]
    pub(crate) fn dirty_epoch(&self) -> u64 {
        self.dirty_epoch.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn is_confirmed(&self, dirty_epoch: u64) -> bool {
        self.confirmed_epoch.load(Ordering::Acquire) == dirty_epoch
    }

    #[inline]
    pub(crate) fn confirm(&self) {
        let dirty_epoch = self.dirty_epoch();
        self.confirmed_epoch.store(dirty_epoch, Ordering::Release);
    }
}

impl Default for RegistryLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RegistryLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryLifecycle")
            .field(
                "active_mutations",
                &self.active_mutations.load(Ordering::Acquire),
            )
            .field("dirty_epoch", &self.dirty_epoch())
            .finish_non_exhaustive()
    }
}

impl RegistryMutation<'_> {
    #[inline]
    pub(crate) fn mark_changed(&mut self) {
        self.changed = true;
    }
}

impl Drop for RegistryMutation<'_> {
    fn drop(&mut self) {
        if self.changed {
            self.lifecycle.dirty_epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.lifecycle
            .active_mutations
            .fetch_sub(1, Ordering::Release);
    }
}
