// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical-plan contracts shared by planning and execution.
//! Algorithm selection, estimation and search state stay in the optimizer.

pub mod artifact;
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
mod predicate_identity;
pub mod properties;
pub mod requirements;
pub mod resources;
pub mod row_type;
pub mod specs;
pub mod verifier;

pub use artifact::*;
pub use children::{InlinePlanChildren, PlanChildren, PlanChildrenArena};
pub use cost::{MemoryCompletion, PhysicalCost, UncappedMemoryDemand};
pub use dependencies::PlanDependencies;
pub use edges::{PhysicalEdge, PhysicalEdgeArena, PhysicalEdgeId, PhysicalEdgeKind};
pub use identity::*;
pub use ids::{PhysicalPlanNodeId, PlanChildrenId};
pub use node::{OperatorLabel, PhysicalPlanNode};
pub use plan::{PhysicalIdentityError, PhysicalPlan, PhysicalPlanNodeArena};
pub use properties::*;
pub use requirements::{ProvidedProperties, RequiredProperties};
pub use resources::{
    ExecutionMemoryContract, RuntimeFilterCapability, RuntimeFilterKeyRepresentation,
    RuntimeFilterResourceContract,
};
pub use row_type::{ColumnIdentity, RowType};
pub use specs::*;
pub use verifier::PhysicalPlanVerifier;

pub mod access_identity;
pub mod scalar_identity;
