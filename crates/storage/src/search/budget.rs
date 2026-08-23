// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;
use std::sync::Arc;

use paro_common::error::Result;

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
    accountant: Option<Arc<dyn SearchMemoryAccountant>>,
    bytes: usize,
}

impl Drop for SearchMemoryReservation {
    fn drop(&mut self) {
        if let Some(accountant) = &self.accountant {
            accountant.release(self.bytes);
        }
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
}

impl ResourceBudget {
    pub fn try_reserve_memory(&self, bytes: usize) -> Result<SearchMemoryReservation> {
        if let Some(accountant) = &self.memory_accountant {
            accountant.try_reserve(bytes)?;
        }
        Ok(SearchMemoryReservation {
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
        }
        assert_eq!(accountant.retained.load(Ordering::Acquire), 0);
    }
}
