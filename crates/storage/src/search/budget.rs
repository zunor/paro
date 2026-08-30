// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use paro_common::allocator::{default_allocator, Allocator};
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

pub trait SearchCancellation: Debug + Send + Sync {
    fn check(&self) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoopSearchCancellation;

impl SearchCancellation for NoopSearchCancellation {
    fn check(&self) -> Result<()> {
        Ok(())
    }
}

/// Query-wide work control shared by every segment dispatch. It is deliberately
/// independent of HNSW so sparse/full-text providers can adopt the same CPU
/// and cancellation contract.
#[derive(Debug)]
pub struct SearchWorkBudget {
    remaining_steps: Option<AtomicUsize>,
    cancellation: Arc<dyn SearchCancellation>,
}

impl SearchWorkBudget {
    fn new(cpu_step_budget: Option<usize>, cancellation: Arc<dyn SearchCancellation>) -> Self {
        Self {
            remaining_steps: cpu_step_budget.map(AtomicUsize::new),
            cancellation,
        }
    }

    pub fn check_and_consume(&self, steps: usize) -> Result<()> {
        self.cancellation.check()?;
        self.consume(steps)
    }

    /// Charge work after a nearby cancellation check. Hot graph loops use
    /// this to account every inspected edge without repeating the atomic
    /// cancellation read for the same bounded adjacency list.
    pub fn consume(&self, steps: usize) -> Result<()> {
        let Some(remaining) = &self.remaining_steps else {
            return Ok(());
        };
        remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(steps)
            })
            .map(|_| ())
            .map_err(|_| {
                paro_error::configuration_limit_exceeded(
                    "search CPU step budget exceeded during provider traversal",
                )
            })
    }
}

/// RAII logical charge for provider-owned memory allocated outside Paro's
/// typed vectors. Dropping the search working set releases the query grant.
#[derive(Debug)]
pub struct SearchMemoryReservation {
    accountant: Arc<dyn SearchMemoryAccountant>,
    bytes: usize,
}

impl Drop for SearchMemoryReservation {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

/// Standalone retained-memory ledger used by embedded and test callers that do
/// not own the engine's query memory pool.
#[derive(Debug)]
pub struct BoundedSearchMemoryAccountant {
    retained: AtomicUsize,
    limit: usize,
}

impl BoundedSearchMemoryAccountant {
    pub fn new(limit: usize) -> Self {
        Self {
            retained: AtomicUsize::new(0),
            limit,
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained.load(Ordering::Acquire)
    }
}

impl SearchMemoryAccountant for BoundedSearchMemoryAccountant {
    fn try_reserve(&self, bytes: usize) -> Result<()> {
        self.retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| {
                paro_error::out_of_memory(format!(
                    "search working set exceeds query budget of {} bytes",
                    self.limit
                ))
            })
    }

    fn release(&self, bytes: usize) {
        let previous = self.retained.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "search memory reservation underflow");
    }
}

#[derive(Clone)]
pub struct ResourceBudget {
    pub heap_budget_items: usize,
    pub parallelism_slots: usize,
    pub context: Option<ResourceContext>,
    pub memory_accountant: Arc<dyn SearchMemoryAccountant>,
    /// Allocator used for provider materialization that must participate in
    /// the engine's physical memory runtime. The logical accountant above
    /// owns query admission; this allocator owns the corresponding buffers.
    pub materialization_allocator: Arc<dyn Allocator>,
    pub work: Arc<SearchWorkBudget>,
}

impl Debug for ResourceBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceBudget")
            .field("heap_budget_items", &self.heap_budget_items)
            .field("parallelism_slots", &self.parallelism_slots)
            .field("context", &self.context)
            .field(
                "materialization_allocator",
                &self.materialization_allocator.name(),
            )
            .field("work", &self.work)
            .finish_non_exhaustive()
    }
}

impl ResourceBudget {
    pub fn standalone(
        memory_limit_bytes: usize,
        heap_budget_items: usize,
        parallelism_slots: usize,
    ) -> Self {
        Self {
            heap_budget_items,
            parallelism_slots,
            context: None,
            memory_accountant: Arc::new(BoundedSearchMemoryAccountant::new(memory_limit_bytes)),
            materialization_allocator: Arc::new(default_allocator()),
            work: Arc::new(SearchWorkBudget::new(
                None,
                Arc::new(NoopSearchCancellation),
            )),
        }
    }

    pub fn managed(
        heap_budget_items: usize,
        parallelism_slots: usize,
        memory_accountant: Arc<dyn SearchMemoryAccountant>,
        materialization_allocator: Arc<dyn Allocator>,
    ) -> Self {
        Self {
            heap_budget_items,
            parallelism_slots,
            context: None,
            memory_accountant,
            materialization_allocator,
            work: Arc::new(SearchWorkBudget::new(
                None,
                Arc::new(NoopSearchCancellation),
            )),
        }
    }

    pub fn with_work_controls(
        mut self,
        cpu_step_budget: Option<usize>,
        cancellation: Arc<dyn SearchCancellation>,
    ) -> Self {
        self.work = Arc::new(SearchWorkBudget::new(cpu_step_budget, cancellation));
        self
    }

    pub fn try_reserve_memory(&self, bytes: usize) -> Result<SearchMemoryReservation> {
        self.memory_accountant.try_reserve(bytes)?;
        Ok(SearchMemoryReservation {
            accountant: self.memory_accountant.clone(),
            bytes,
        })
    }
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self::standalone(64 * 1024 * 1024, 1024, 1)
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
            memory_accountant: accountant.clone(),
            ..ResourceBudget::default()
        };
        {
            let _reservation = budget.try_reserve_memory(4096).unwrap();
            assert_eq!(accountant.retained.load(Ordering::Acquire), 4096);
        }
        assert_eq!(accountant.retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn reservation_enforces_local_limit_without_accountant() {
        let accountant = Arc::new(BoundedSearchMemoryAccountant::new(4096));
        let budget =
            ResourceBudget::managed(1024, 1, accountant.clone(), Arc::new(default_allocator()));
        let reservation = budget.try_reserve_memory(4096).unwrap();
        assert!(budget.try_reserve_memory(1).is_err());
        drop(reservation);
        assert_eq!(accountant.retained_bytes(), 0);
        assert!(budget.try_reserve_memory(4096).is_ok());
    }

    #[test]
    fn cpu_steps_are_shared_by_every_clone_of_the_query_budget() {
        let budget =
            ResourceBudget::default().with_work_controls(Some(2), Arc::new(NoopSearchCancellation));
        let clone = budget.clone();
        budget.work.check_and_consume(1).unwrap();
        clone.work.check_and_consume(1).unwrap();
        assert!(budget.work.check_and_consume(1).is_err());
    }
}
