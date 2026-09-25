// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Implementation selection and construction of planner-owned physical contracts.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

pub(crate) use paro_planner::physical::*;
pub mod enforcer;

pub(crate) mod aggregate_planning;

pub(crate) mod implementation;
pub mod lower;
mod rewrite;
pub(crate) mod select;
pub(crate) mod selected;

pub use lower::{PhysicalBuildContext, PhysicalPlanBuilder};

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
    /// Hash join with the logical left input selected as the physical build
    /// side. Extraction inverts the physical join and records its logical
    /// output layout in the immutable hash-join spec.
    HashJoinBuildLeft,
    /// Build-left hash join plus a region-owned filter from the preserved
    /// build input into the non-preserved physical probe input.
    HashJoinBuildLeftRuntimeFilter,
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

pub mod access;
