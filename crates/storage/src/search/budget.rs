// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchBatchConfig {
    pub row_limit: usize,
    pub preferred_bytes: usize,
}

impl Default for SearchBatchConfig {
    fn default() -> Self {
        Self {
            row_limit: 1024,
            preferred_bytes: 1 << 20,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceContext {
    pub tenant: Option<String>,
    pub workload: Option<String>,
}

pub trait SearchMemoryAccountant: Debug + Send + Sync {
    fn try_reserve(&self, bytes: usize) -> Result<()>;
    fn release(&self, bytes: usize);
}

/// RAII logical charge for provider-owned memory allocated outside Paro's
/// typed vectors. Dropping the search working set releases the query grant.
#[derive(Debug)]
pub struct SearchMemoryReservation {
    tracker: Arc<SearchMemoryTracker>,
    accountant: Option<Arc<dyn SearchMemoryAccountant>>,
    bytes: usize,
}

impl Drop for SearchMemoryReservation {
    fn drop(&mut self) {
        if let Some(accountant) = &self.accountant {
            accountant.release(self.bytes);
        }
        self.tracker.release(self.bytes);
    }
}

/// Query-local retained-memory ledger shared by every clone of a resource
/// budget. Providers reserve through [`ResourceBudget::try_reserve_memory`];
/// callers never need a second ad-hoc atomic or a separate interpretation of
/// the limit.
#[derive(Debug, Default)]
pub struct SearchMemoryTracker {
    retained: AtomicUsize,
}

impl SearchMemoryTracker {
    fn try_reserve(&self, bytes: usize, limit: usize) -> Result<()> {
        self.retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes).filter(|next| *next <= limit)
            })
            .map(|_| ())
            .map_err(|_| {
                paro_error::out_of_memory(format!(
                    "search working set exceeds query budget of {limit} bytes"
                ))
            })
    }

    fn release(&self, bytes: usize) {
        let previous = self.retained.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "search memory reservation underflow");
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
pub struct ResourceBudget {
    pub memory_limit_bytes: usize,
    pub heap_budget_items: usize,
    pub parallelism_slots: usize,
    pub cpu_step_budget: Option<usize>,
    pub context: Option<ResourceContext>,
    pub memory_accountant: Option<Arc<dyn SearchMemoryAccountant>>,
    /// Shared local ledger enforcing `memory_limit_bytes`, including when no
    /// engine-level memory accountant is installed.
    pub memory_tracker: Arc<SearchMemoryTracker>,
}

impl ResourceBudget {
    pub fn try_reserve_memory(&self, bytes: usize) -> Result<SearchMemoryReservation> {
        self.memory_tracker
            .try_reserve(bytes, self.memory_limit_bytes)?;
        if let Some(accountant) = &self.memory_accountant {
            if let Err(error) = accountant.try_reserve(bytes) {
                self.memory_tracker.release(bytes);
                return Err(error);
            }
        }
        Ok(SearchMemoryReservation {
            tracker: self.memory_tracker.clone(),
            accountant: self.memory_accountant.clone(),
            bytes,
        })
    }
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 64 * 1024 * 1024,
            heap_budget_items: 1024,
            parallelism_slots: 1,
            cpu_step_budget: None,
            context: None,
            memory_accountant: None,
            memory_tracker: Arc::new(SearchMemoryTracker::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct CountingAccountant {
        retained: AtomicUsize,
    }

    impl SearchMemoryAccountant for CountingAccountant {
        fn try_reserve(&self, bytes: usize) -> Result<()> {
            self.retained.fetch_add(bytes, Ordering::AcqRel);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.retained.fetch_sub(bytes, Ordering::AcqRel);
        }
    }

    #[test]
    fn reservation_releases_query_charge_on_drop() {
        let accountant = Arc::new(CountingAccountant::default());
        let budget = ResourceBudget {
            memory_accountant: Some(accountant.clone()),
            ..ResourceBudget::default()
        };
        {
            let _reservation = budget.try_reserve_memory(4096).unwrap();
            assert_eq!(accountant.retained.load(Ordering::Acquire), 4096);
            assert_eq!(budget.memory_tracker.retained_bytes(), 4096);
        }
        assert_eq!(accountant.retained.load(Ordering::Acquire), 0);
        assert_eq!(budget.memory_tracker.retained_bytes(), 0);
    }

    #[test]
    fn reservation_enforces_local_limit_without_accountant() {
        let budget = ResourceBudget {
            memory_limit_bytes: 4096,
            ..ResourceBudget::default()
        };
        let reservation = budget.try_reserve_memory(4096).unwrap();
        assert!(budget.try_reserve_memory(1).is_err());
        drop(reservation);
        assert!(budget.try_reserve_memory(4096).is_ok());
    }
}
