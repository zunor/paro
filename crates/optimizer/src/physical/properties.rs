// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Optimizer/execution property contracts carried by every physical node.

use std::collections::BTreeMap;

use paro_common::vector::VECTOR_SIZE;
use paro_planner::plan::CardinalityEstimate;

use crate::physical::cost::SearchCost;
use crate::physical::identity::{AdmissibleGrantSetId, Fingerprint, ResourceGrantClassId};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::requirements::{ProvidedProperties, RequiredProperties};

pub type ExecutionColumnId = usize;
pub type MemoryBytes = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalGrantContract {
    Invariant(AdmissibleGrantSetId),
    Class(ResourceGrantClassId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanOrigin {
    Direct,
    Memo,
    SpecializedRegion(Fingerprint),
    Enforcer(Fingerprint),
    StatementLowering,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AuxiliaryArtifactKind {
    RuntimeFilter,
    SharedSpool,
    WorkTable,
    ExactRowset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnedAuxiliaryArtifact {
    pub fingerprint: Fingerprint,
    pub kind: AuxiliaryArtifactKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalCharacteristics {
    pub blocking: bool,
    pub spillable: bool,
    pub parallel: bool,
    pub supports_early_stop: bool,
}

#[derive(Debug, Clone)]
pub struct PhysicalNodeProperties {
    pub required_from_parent: RequiredProperties,
    pub provided: ProvidedProperties,
    pub characteristics: PhysicalCharacteristics,
    pub output_estimate: Option<CardinalityEstimate>,
    pub cumulative_cost: SearchCost,
    pub grant_contract: PhysicalGrantContract,
    pub auxiliary_dependencies: Box<[u32]>,
    pub region_owner: Option<Fingerprint>,
    pub owned_artifacts: Box<[OwnedAuxiliaryArtifact]>,
    pub origin: PlanOrigin,
    pub winner_goal: Fingerprint,
}

#[derive(Debug, Clone, Default)]
pub struct PlanPropertyMap {
    entries: BTreeMap<PhysicalPlanNodeId, PhysicalNodeProperties>,
}

impl PlanPropertyMap {
    pub fn insert(
        &mut self,
        node: PhysicalPlanNodeId,
        properties: PhysicalNodeProperties,
    ) -> Option<PhysicalNodeProperties> {
        self.entries.insert(node, properties)
    }

    pub fn get(&self, node: PhysicalPlanNodeId) -> Option<&PhysicalNodeProperties> {
        self.entries.get(&node)
    }

    pub fn get_mut(&mut self, node: PhysicalPlanNodeId) -> Option<&mut PhysicalNodeProperties> {
        self.entries.get_mut(&node)
    }

    pub fn iter(&self) -> impl Iterator<Item = (PhysicalPlanNodeId, &PhysicalNodeProperties)> {
        self.entries
            .iter()
            .map(|(id, properties)| (*id, properties))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn retain_remapped(&mut self, reachable: &[bool], remap: &[PhysicalPlanNodeId]) {
        self.entries = std::mem::take(&mut self.entries)
            .into_iter()
            .filter_map(|(old, properties)| {
                reachable
                    .get(old.index())
                    .copied()
                    .unwrap_or(false)
                    .then(|| (remap[old.index()], properties))
            })
            .collect();
    }

    pub(crate) fn remap_auxiliary_edges(
        &mut self,
        edge_remap: &[Option<crate::physical::edges::PhysicalEdgeId>],
    ) {
        for properties in self.entries.values_mut() {
            properties.auxiliary_dependencies = properties
                .auxiliary_dependencies
                .iter()
                .filter_map(|old| {
                    edge_remap
                        .get(*old as usize)
                        .and_then(|mapped| *mapped)
                        .map(|mapped| mapped.0)
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineProperties {
    pub capabilities: ExecutionCapabilities,
    pub memory: MemoryRequirement,
    pub tuning: ExecutionTuning,
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
    pub column: ExecutionColumnId,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecompressionBudget {
    pub bytes_per_task: MemoryBytes,
}
