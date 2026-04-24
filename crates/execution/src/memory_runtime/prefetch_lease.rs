// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query-accounted prefetch budget lease.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::memory::{MemoryAccountingClass, MemoryDomain};
use paro_storage::buffer::PrefetchBudget;

use super::OperatorMemoryAccount;

/// Best-effort prefetch lease backed by an operator memory account.
#[derive(Debug)]
pub struct PrefetchLease {
    account: Arc<OperatorMemoryAccount>,
    target_bytes: AtomicUsize,
    inflight_bytes: AtomicUsize,
}

impl PrefetchLease {
    pub fn new(account: Arc<OperatorMemoryAccount>, target_bytes: usize) -> Self {
        Self {
            account,
            target_bytes: AtomicUsize::new(target_bytes),
            inflight_bytes: AtomicUsize::new(0),
        }
    }

    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes.load(Ordering::Acquire)
    }
}

impl PrefetchBudget for PrefetchLease {
    fn target_bytes(&self) -> usize {
        self.target_bytes.load(Ordering::Acquire)
    }

    fn update_target_bytes(&self, bytes: usize) {
        self.target_bytes.store(bytes, Ordering::Release);
    }

    fn try_acquire(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }

        let mut current = self.inflight_bytes.load(Ordering::Acquire);
        loop {
            let target = self.target_bytes();
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > target {
                return false;
            }
            match self.inflight_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self
                        .account
                        .retain_external_allocation(
                            MemoryDomain::Host,
                            MemoryTag::ExternalFileCache,
                            MemoryAccountingClass::Prefetch,
                            bytes,
                        )
                        .is_ok()
                    {
                        return true;
                    }
                    self.release_inflight_only(bytes);
                    return false;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .inflight_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
        self.account.release_external_allocation(
            MemoryDomain::Host,
            MemoryTag::ExternalFileCache,
            MemoryAccountingClass::Prefetch,
            bytes,
        );
    }
}

impl PrefetchLease {
    fn release_inflight_only(&self, bytes: usize) {
        let _ = self
            .inflight_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }
}
