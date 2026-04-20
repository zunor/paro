// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBudget {
    pub memory_limit_bytes: usize,
    pub heap_budget_items: usize,
    pub parallelism_slots: usize,
    pub cpu_step_budget: Option<usize>,
    pub context: Option<ResourceContext>,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 64 * 1024 * 1024,
            heap_budget_items: 1024,
            parallelism_slots: 1,
            cpu_step_budget: None,
            context: None,
        }
    }
}
