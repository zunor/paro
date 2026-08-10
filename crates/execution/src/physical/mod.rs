// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable arena-backed physical plan image.
//!
//! This module stores the immutable plan image for the operator runtime. It
//! models what should run without allocating runtime operator objects, pipeline
//! state, or breaker handles.

pub mod children;
pub mod generator;
pub mod ids;
pub mod node;
pub mod plan;
pub mod properties;
mod rewrite;
pub mod row_type;
pub mod specs;

pub use children::{InlinePlanChildren, PlanChildren, PlanChildrenArena};
pub use generator::{PhysicalPlanGenerator, PlanBuildContext};
pub use ids::{PhysicalPlanNodeId, PlanChildrenId};
pub use node::{OperatorLabel, PhysicalPlanNode};
pub use plan::{PhysicalPlan, PhysicalPlanNodeArena};
pub use properties::PlanPropertyMap;
pub use row_type::RowType;
pub use specs::*;
