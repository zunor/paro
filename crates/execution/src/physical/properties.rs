// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Execution contracts consumed by pipeline scheduling and memory admission.

use paro_common::vector::VECTOR_SIZE;
pub type ColumnId = usize;
pub type MemoryBytes = u64;

#[derive(Debug, Clone, Default)]
pub struct PlanPropertyMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineProperties {
    pub capabilities: ExecutionCapabilities,
    pub memory: MemoryRequirement,
    pub tuning: ExecutionTuning,
}

impl Default for PipelineProperties {
    fn default() -> Self {
        Self {
            capabilities: ExecutionCapabilities::default(),
            memory: MemoryRequirement::default(),
            tuning: ExecutionTuning::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCapabilities {
    pub parallelism: Parallelism,
    pub morsel: MorselCapability,
    pub supports_backpressure: bool,
    pub supports_spill: bool,
    pub supports_late_materialization: bool,
}

impl Default for ExecutionCapabilities {
    fn default() -> Self {
        Self {
            parallelism: Parallelism::unbounded(),
            morsel: MorselCapability::None,
            supports_backpressure: true,
            supports_spill: false,
            supports_late_materialization: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRequirement {
    pub class: MemoryClass,
    pub revocable: bool,
    pub spillable: bool,
    pub min_grant: MemoryBytes,
    pub preferred_grant: MemoryBytes,
    pub per_task_grant: MemoryBytes,
}

impl Default for MemoryRequirement {
    fn default() -> Self {
        Self {
            class: MemoryClass::Streaming,
            revocable: false,
            spillable: false,
            min_grant: 0,
            preferred_grant: 0,
            per_task_grant: 0,
        }
    }
}

impl MemoryRequirement {
    pub fn combine_with(&mut self, other: &Self) {
        self.class = self.class.max(other.class);
        self.revocable |= other.revocable;
        self.spillable |= other.spillable;
        self.min_grant = self.min_grant.max(other.min_grant);
        self.preferred_grant = self.preferred_grant.max(other.preferred_grant);
        self.per_task_grant = self.per_task_grant.max(other.per_task_grant);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionTuning {
    pub vector_size: usize,
    pub batch_size_hint: Option<usize>,
    pub prefetch: PrefetchPolicy,
    pub decompression_budget: DecompressionBudget,
}

impl Default for ExecutionTuning {
    fn default() -> Self {
        Self {
            vector_size: VECTOR_SIZE,
            batch_size_hint: None,
            prefetch: PrefetchPolicy::None,
            decompression_budget: DecompressionBudget::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderingSpec {
    pub columns: Vec<OrderingColumn>,
}

impl OrderingSpec {
    pub fn new(columns: Vec<OrderingColumn>) -> Self {
        Self { columns }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderingColumn {
    pub column: ColumnId,
    pub direction: OrderingDirection,
    pub nulls: NullOrdering,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderingDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrdering {
    First,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parallelism {
    pub min: usize,
    pub max: usize,
    pub saturates_threads: bool,
}

impl Parallelism {
    pub fn single() -> Self {
        Self {
            min: 1,
            max: 1,
            saturates_threads: false,
        }
    }

    pub fn unbounded() -> Self {
        Self {
            min: 1,
            max: usize::MAX,
            saturates_threads: true,
        }
    }

    pub fn bounded(max: usize) -> Self {
        Self {
            min: 1,
            max: max.max(1),
            saturates_threads: true,
        }
    }

    pub fn merge(self, other: Self) -> Self {
        Self {
            min: self.min.max(other.min),
            max: self.max.min(other.max),
            saturates_threads: self.saturates_threads || other.saturates_threads,
        }
    }

    /// Whether this capability is no more restrictive than `baseline`.
    /// `min` is a lower scheduling requirement, while `max` and thread
    /// saturation describe available concurrency.
    pub fn dominates(self, baseline: Self) -> bool {
        self.min <= baseline.min
            && self.max >= baseline.max
            && (!baseline.saturates_threads || self.saturates_threads)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MorselCapability {
    None,
    Source,
    Transform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemoryClass {
    Streaming,
    Blocking,
    External,
    Utility,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchPolicy {
    None,
    RowsetSegments { distance: usize },
    SearchPartitions { distance: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecompressionBudget {
    pub bytes_per_task: MemoryBytes,
}

impl Default for DecompressionBudget {
    fn default() -> Self {
        Self { bytes_per_task: 0 }
    }
}
