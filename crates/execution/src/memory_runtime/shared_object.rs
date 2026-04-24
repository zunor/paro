// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared retained memory object for build-side state that crosses pipelines.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use paro_common::allocator::MemoryTag;
use paro_common::memory::{MemoryAccountingClass, MemoryDomain, MemoryOwner, MemoryResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedRetainedObjectState {
    Dormant,
    Active,
}

/// Memory owner wrapper for retained structures that survive build/source handoff.
///
/// Allocations published as `Revocable` are dormant/non-revocable until the
/// consumer side binds a reclaimer. Other accounting classes pass through as-is.
pub struct SharedRetainedObject {
    name: &'static str,
    owner: Arc<dyn MemoryOwner>,
    domain: MemoryDomain,
    tag: MemoryTag,
    state: Mutex<SharedRetainedObjectState>,
    stateful_bytes: AtomicUsize,
    retained_bytes: AtomicUsize,
}

impl SharedRetainedObject {
    pub fn new(
        name: &'static str,
        owner: Arc<dyn MemoryOwner>,
        domain: MemoryDomain,
        tag: MemoryTag,
    ) -> Self {
        Self {
            name,
            owner,
            domain,
            tag,
            state: Mutex::new(SharedRetainedObjectState::Dormant),
            stateful_bytes: AtomicUsize::new(0),
            retained_bytes: AtomicUsize::new(0),
        }
    }

    pub fn state(&self) -> SharedRetainedObjectState {
        *self
            .state
            .lock()
            .expect("shared retained object state lock poisoned")
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    pub fn reclaimable_bytes(&self) -> usize {
        match self.state() {
            SharedRetainedObjectState::Dormant => 0,
            SharedRetainedObjectState::Active => self.stateful_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn rebind_reclaimer(&self) {
        self.transition_to(SharedRetainedObjectState::Active);
    }

    pub fn mark_dormant(&self) {
        self.transition_to(SharedRetainedObjectState::Dormant);
    }

    fn transition_to(&self, next: SharedRetainedObjectState) {
        let mut state = self
            .state
            .lock()
            .expect("shared retained object state lock poisoned");
        if *state == next {
            return;
        }

        let bytes = self.stateful_bytes.load(Ordering::Acquire);
        if bytes > 0 {
            self.owner.reclassify_allocation(
                self.domain,
                self.tag,
                Self::state_class(*state),
                Self::state_class(next),
                bytes,
            );
        }
        *state = next;
    }

    fn class_for(&self, class: MemoryAccountingClass) -> MemoryAccountingClass {
        if class == MemoryAccountingClass::Revocable {
            Self::state_class(self.state())
        } else {
            class
        }
    }

    fn state_class(state: SharedRetainedObjectState) -> MemoryAccountingClass {
        match state {
            SharedRetainedObjectState::Dormant => MemoryAccountingClass::NonRevocable,
            SharedRetainedObjectState::Active => MemoryAccountingClass::Revocable,
        }
    }
}

impl MemoryOwner for SharedRetainedObject {
    fn acquire_capacity(&self, domain: MemoryDomain, bytes: usize) -> MemoryResult<()> {
        self.owner.acquire_capacity(domain, bytes)
    }

    fn release_capacity(&self, domain: MemoryDomain, bytes: usize) {
        self.owner.release_capacity(domain, bytes);
    }

    fn record_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        if class == MemoryAccountingClass::Revocable {
            self.stateful_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        self.retained_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.owner
            .record_allocation(domain, tag, self.class_for(class), bytes);
    }

    fn release_allocation(
        &self,
        domain: MemoryDomain,
        tag: MemoryTag,
        class: MemoryAccountingClass,
        bytes: usize,
    ) {
        if class == MemoryAccountingClass::Revocable {
            saturating_sub(&self.stateful_bytes, bytes);
        }
        saturating_sub(&self.retained_bytes, bytes);
        self.owner
            .release_allocation(domain, tag, self.class_for(class), bytes);
    }
}

fn saturating_sub(counter: &AtomicUsize, bytes: usize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(bytes))
    });
}

impl fmt::Debug for SharedRetainedObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedRetainedObject")
            .field("name", &self.name)
            .field("domain", &self.domain)
            .field("tag", &self.tag)
            .field("state", &self.state())
            .field("retained_bytes", &self.retained_bytes())
            .field("reclaimable_bytes", &self.reclaimable_bytes())
            .finish()
    }
}
