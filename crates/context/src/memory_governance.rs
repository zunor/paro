// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query memory governance contracts shared across context and execution.

use std::fmt;
use std::sync::{Arc, Weak};

use paro_common::memory::MemoryResult;

/// Memory budget requested by one executable statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemoryBudgetSpec {
    pub query_id: u64,
    pub query_group: String,
    pub requested_bytes: usize,
    pub hard_quota_bytes: Option<usize>,
    pub priority_weight: usize,
}

impl QueryMemoryBudgetSpec {
    pub fn new(
        query_id: u64,
        query_group: Option<String>,
        requested_bytes: usize,
        hard_quota_bytes: Option<usize>,
    ) -> Self {
        Self {
            query_id,
            query_group: query_group
                .filter(|group| !group.trim().is_empty())
                .unwrap_or_else(|| "default".to_string()),
            requested_bytes,
            hard_quota_bytes,
            priority_weight: 1,
        }
    }

    pub fn desired_bytes(&self) -> usize {
        self.hard_quota_bytes.unwrap_or(self.requested_bytes)
    }
}

/// Query-pool surface used by a process-level memory coordinator.
pub trait QueryMemoryTarget: Send + Sync + fmt::Debug {
    fn capacity_bytes(&self) -> usize;
    fn set_capacity_bytes(&self, bytes: usize);
    fn issued_bytes(&self) -> usize;
    fn reclaimable_bytes(&self) -> usize;
    fn reclaim(&self, target_bytes: usize) -> MemoryResult<usize>;
}

/// Registration handle retained by a query pool until the pool is dropped.
#[derive(Clone)]
pub struct QueryMemoryRegistration {
    coordinator: Arc<dyn QueryMemoryCoordinator>,
    query_id: u64,
}

impl QueryMemoryRegistration {
    pub fn new(coordinator: Arc<dyn QueryMemoryCoordinator>, query_id: u64) -> Self {
        Self {
            coordinator,
            query_id,
        }
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    pub fn coordinator(&self) -> &Arc<dyn QueryMemoryCoordinator> {
        &self.coordinator
    }
}

impl fmt::Debug for QueryMemoryRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryMemoryRegistration")
            .field("query_id", &self.query_id)
            .finish_non_exhaustive()
    }
}

/// Process-level coordinator for query memory pools.
pub trait QueryMemoryCoordinator: Send + Sync + fmt::Debug {
    fn next_query_id(&self) -> u64;

    fn register_query(
        self: Arc<Self>,
        spec: QueryMemoryBudgetSpec,
        target: Weak<dyn QueryMemoryTarget>,
    ) -> QueryMemoryRegistration;

    fn unregister_query(&self, query_id: u64);

    fn reclaim_for_query(
        &self,
        requester_query_id: u64,
        target_bytes: usize,
    ) -> MemoryResult<usize>;

    fn available_for_queries(&self) -> usize;

    fn session_retained_bytes(&self) -> usize {
        0
    }
}
