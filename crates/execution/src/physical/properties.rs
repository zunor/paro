// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Canonical physical property types shared by plan and pipeline lowering.
//!
//! This module intentionally contains only immutable property descriptions and
//! repair decisions. Pipeline-specific accumulation lives in
//! `crate::pipeline::properties` so `physical` does not depend on `pipeline`.

use paro_common::vector::VECTOR_SIZE;
use paro_planner::plan::CardinalityEstimate;

pub type ColumnId = usize;
pub type MemoryBytes = u64;

#[derive(Debug, Clone, Default)]
pub struct PlanPropertyMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineProperties {
    pub placement: Placement,
    pub required: RequiredProperties,
    pub provided: ProvidedProperties,
    pub capabilities: ExecutionCapabilities,
    pub memory: MemoryRequirement,
    pub tuning: ExecutionTuning,
}

impl Default for PipelineProperties {
    fn default() -> Self {
        Self {
            placement: Placement::Local,
            required: RequiredProperties::default(),
            provided: ProvidedProperties::default(),
            capabilities: ExecutionCapabilities::default(),
            memory: MemoryRequirement::default(),
            tuning: ExecutionTuning::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredProperties {
    pub ordering: OrderingRequirement,
    pub partitioning: PartitioningRequirement,
    pub batch_index: BatchIndexRequirement,
    pub cardinality: CardinalityRequirement,
}

impl Default for RequiredProperties {
    fn default() -> Self {
        Self {
            ordering: OrderingRequirement::Any,
            partitioning: PartitioningRequirement::Any,
            batch_index: BatchIndexRequirement::Any,
            cardinality: CardinalityRequirement::Any,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvidedProperties {
    pub ordering: OrderingProperty,
    pub partitioning: PartitioningProperty,
    pub uniqueness: UniqueKeyProperty,
    pub cardinality: Option<CardinalityEstimate>,
    pub late_materialization: LateMaterializationProperty,
}

impl Default for ProvidedProperties {
    fn default() -> Self {
        Self {
            ordering: OrderingProperty::Any,
            partitioning: PartitioningProperty::None,
            uniqueness: UniqueKeyProperty::Unknown,
            cardinality: None,
            late_materialization: LateMaterializationProperty::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCapabilities {
    pub parallelism: Parallelism,
    pub morsel: MorselCapability,
    pub preserves_order: bool,
    pub supports_backpressure: bool,
    pub supports_spill: bool,
    pub supports_late_materialization: bool,
}

impl Default for ExecutionCapabilities {
    fn default() -> Self {
        Self {
            parallelism: Parallelism::unbounded(),
            morsel: MorselCapability::None,
            preserves_order: true,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    Local,
    SingleTask,
    Partitioned(MorselPartitioning),
}

impl Placement {
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::SingleTask, _) | (_, Self::SingleTask) => Self::SingleTask,
            (Self::Partitioned(left), Self::Partitioned(right)) => {
                if left.is_compatible_with(right) {
                    Self::Partitioned(left)
                } else {
                    Self::Local
                }
            }
            (Self::Partitioned(partitioning), Self::Local)
            | (Self::Local, Self::Partitioned(partitioning)) => Self::Partitioned(partitioning),
            (Self::Local, Self::Local) => Self::Local,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderingProperty {
    Any,
    Preserved,
    Fixed(OrderingSpec),
    Unordered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderingRequirement {
    Any,
    PreserveInput,
    Fixed(OrderingSpec),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitioningProperty {
    None,
    BatchIndex,
    Columns(Vec<ColumnId>),
    Morsel(MorselPartitioning),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitioningRequirement {
    Any,
    SingleTask,
    BatchIndex,
    Columns(Vec<ColumnId>),
    CompatibleWith(MorselPartitioning),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchIndexRequirement {
    Any,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityRequirement {
    Any,
    AtMost(u64),
    Exact(u64),
}

impl Default for CardinalityRequirement {
    fn default() -> Self {
        Self::Any
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniqueKeyProperty {
    Unknown,
    None,
    Columns(Vec<ColumnId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LateMaterializationProperty {
    None,
    RowIds { columns: Vec<ColumnId> },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselPartitioning {
    pub key_count: usize,
}

impl MorselPartitioning {
    pub fn rowset_segments() -> Self {
        Self { key_count: 0 }
    }

    pub fn with_key_count(key_count: usize) -> Self {
        Self { key_count }
    }

    pub fn is_compatible_with(self, other: Self) -> bool {
        self == other
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PropertyRepair {
    pub repairs: Vec<PropertyRepairKind>,
}

impl PropertyRepair {
    pub fn none() -> Self {
        Self {
            repairs: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.repairs.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRepairKind {
    Sort(OrderingSpec),
    BatchIndexAdapter,
    SingleTaskFallback,
    MaterializationAdapter,
}

pub struct PhysicalPropertySolver;

impl PhysicalPropertySolver {
    pub fn reconcile(
        required: &RequiredProperties,
        provided: &ProvidedProperties,
        capabilities: &ExecutionCapabilities,
    ) -> PropertyRepair {
        let mut repair = PropertyRepair::none();

        match &required.ordering {
            OrderingRequirement::Any => {}
            OrderingRequirement::PreserveInput => {
                if matches!(
                    provided.ordering,
                    OrderingProperty::Unordered | OrderingProperty::Any
                ) {
                    repair
                        .repairs
                        .push(PropertyRepairKind::MaterializationAdapter);
                }
            }
            OrderingRequirement::Fixed(spec) => {
                if !matches!(&provided.ordering, OrderingProperty::Fixed(existing) if existing == spec)
                {
                    repair.repairs.push(PropertyRepairKind::Sort(spec.clone()));
                }
            }
        }

        match &required.partitioning {
            PartitioningRequirement::Any => {}
            PartitioningRequirement::SingleTask => {
                if capabilities.parallelism.max != 1 {
                    repair.repairs.push(PropertyRepairKind::SingleTaskFallback);
                }
            }
            PartitioningRequirement::BatchIndex => {
                if !matches!(provided.partitioning, PartitioningProperty::BatchIndex) {
                    repair.repairs.push(PropertyRepairKind::BatchIndexAdapter);
                }
            }
            PartitioningRequirement::Columns(columns) => {
                if !matches!(&provided.partitioning, PartitioningProperty::Columns(existing) if existing == columns)
                {
                    repair
                        .repairs
                        .push(PropertyRepairKind::MaterializationAdapter);
                }
            }
            PartitioningRequirement::CompatibleWith(partitioning) => {
                if !matches!(&provided.partitioning, PartitioningProperty::Morsel(existing) if existing == partitioning)
                {
                    repair
                        .repairs
                        .push(PropertyRepairKind::MaterializationAdapter);
                }
            }
        }

        if matches!(required.batch_index, BatchIndexRequirement::Required)
            && !matches!(provided.partitioning, PartitioningProperty::BatchIndex)
            && !repair
                .repairs
                .contains(&PropertyRepairKind::BatchIndexAdapter)
        {
            repair.repairs.push(PropertyRepairKind::BatchIndexAdapter);
        }

        repair
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_merge_preserves_compatible_partitioning() {
        let partitioning = MorselPartitioning::rowset_segments();

        assert_eq!(
            Placement::Partitioned(partitioning).merge(Placement::Partitioned(partitioning)),
            Placement::Partitioned(partitioning)
        );
        assert_eq!(
            Placement::Partitioned(partitioning).merge(Placement::Local),
            Placement::Partitioned(partitioning)
        );
    }

    #[test]
    fn placement_merge_drops_incompatible_partitioning_claim() {
        let left = MorselPartitioning::with_key_count(1);
        let right = MorselPartitioning::with_key_count(2);

        assert_eq!(
            Placement::Partitioned(left).merge(Placement::Partitioned(right)),
            Placement::Local
        );
        assert_eq!(
            Placement::Partitioned(left).merge(Placement::SingleTask),
            Placement::SingleTask
        );
    }
}
