// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical-plan IR and extraction from verified Memo winners.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

pub mod children;
pub mod cost;
pub mod dependencies;
pub mod edges;
pub mod explain;
pub mod identity;
pub mod ids;
pub mod lineage;
pub mod node;
pub mod plan;
pub mod portfolio;
pub mod properties;
pub mod requirements;
pub mod resources;
pub mod row_type;
pub mod specs;
pub mod verifier;

pub use children::{InlinePlanChildren, PlanChildren, PlanChildrenArena};
pub use cost::SearchCost;
pub use dependencies::PlanDependencies;
pub use edges::{PhysicalEdge, PhysicalEdgeArena, PhysicalEdgeId, PhysicalEdgeKind};
pub use identity::*;
pub use ids::{PhysicalPlanNodeId, PlanChildrenId};
pub use node::{OperatorLabel, PhysicalPlanNode};
pub use plan::{PhysicalPlan, PhysicalPlanNodeArena};
pub use portfolio::*;
pub use properties::*;
pub use requirements::{ProvidedProperties, RequiredProperties};
pub use resources::{
    ExecutionMemoryContract, RuntimeFilterCapability, RuntimeFilterKeyRepresentation,
    RuntimeFilterResourceContract,
};
pub use row_type::{ColumnIdentity, RowType};
pub use specs::*;
pub use verifier::PhysicalPlanVerifier;

pub(crate) mod aggregate_planning;

pub mod extraction;
mod rewrite;

pub use extraction::{ExtractionContext, PhysicalPlanExtractor};

pub(crate) mod slot_assignment;

/// Concrete implementation selected by the optimizer.  This is deliberately
/// distinct from a logical operator tag: extraction must never choose an
/// algorithm a second time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhysicalImplementationFlavor {
    /// Operators without an algorithm family, plus structural operators whose
    /// specialized semantics are already fixed by their logical payload.
    Structural,
    HashJoin,
    /// Hash join plus an AuxiliaryPlanRegion-owned build-to-scan filter.
    HashJoinRuntimeFilter,
    NestedLoopJoin,
    /// Cross-product build retained entirely in the query memory pool.
    CrossProductInMemory,
    /// Cross-product build written to one external row-store domain.
    CrossProductExternal,
    /// Full ordering with an adaptive in-memory/external run representation.
    AdaptiveSort,
    /// Bounded heap retaining at most LIMIT + OFFSET candidates.
    HeapTopN,
    SortRangeJoin,
    ClassicIeJoin,
    HashAggregate,
    PerfectHashAggregate,
    /// Projection implementation admitted only by a validated at-most-one-row
    /// proof for every grouping key.
    SingletonAggregateProjection,
    Window,
    PartitionAggregateWindow,
    /// Capability-bound access selected for logical Filter/Rank/TopK
    /// semantics. The capability token exists only in its physical payload.
    SearchProvider,
}

#[derive(Debug, Clone)]
pub(crate) struct WinnerPhysicalContract {
    pub required: RequiredProperties,
    pub provided: ProvidedProperties,
    pub cost: SearchCost,
    pub grant: PhysicalGrantContract,
    pub origin: PlanOrigin,
    pub goal_fingerprint: Fingerprint,
    pub physical_fingerprint: Fingerprint,
    pub implementation: PhysicalImplementationFlavor,
    pub region_owner: Option<Fingerprint>,
    pub owned_artifacts: Box<[OwnedAuxiliaryArtifact]>,
}

/// Extraction-local contract lookup. Planner node ids never participate in
/// Memo equivalence; they only reconnect a verified winner with its bound
/// semantic payload during the optimizer's two-stage extraction.
pub(crate) type WinnerPhysicalContracts =
    Arc<HashMap<paro_planner::plan::PlanNodeId, WinnerPhysicalContract>>;

/// Property conversions selected during winner extraction. They are physical
/// nodes, never logical Memo expressions.
#[derive(Debug, Clone)]
pub(crate) enum ExtractedPhysicalEnforcer {
    Sort {
        orders: Box<[paro_planner::binder::ir::OrderByNode]>,
    },
    MutationInputSpool {
        barrier: MutationBarrierId,
        targets: BTreeSet<BaseRelationId>,
        snapshot: SnapshotId,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ExtractedEnforcerContract {
    pub enforcer: ExtractedPhysicalEnforcer,
    pub contract: WinnerPhysicalContract,
}

pub(crate) type ExtractedEnforcerContracts =
    Arc<HashMap<paro_planner::plan::PlanNodeId, Box<[ExtractedEnforcerContract]>>>;

pub(crate) type StatementWriteContracts =
    Arc<HashMap<paro_planner::plan::PlanNodeId, WriteContract>>;
