// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query optimizer passes and supporting infrastructure.

pub(crate) mod context;
pub(crate) mod cost;
mod optimizer;
pub(crate) mod physical;
pub(crate) mod statement;
pub(crate) mod verify;

pub use optimizer::{OptimizedStatement, Optimizer};

pub(crate) mod estimate;
pub(crate) mod region;
pub(crate) mod rewrite;

pub(crate) mod diagnostics;

pub use diagnostics::work::begin as begin_optimizer_observation;

/// Explicit test/benchmark entry points, absent from normal dependencies.
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use crate::diagnostics::profile::{
        publish_optimizer_profile_snapshot, OptimizerComponent, OptimizerProfileSnapshot,
        OptimizerProfileSnapshotEntry,
    };
    pub use crate::physical::{PhysicalBuildContext, PhysicalPlanBuilder};
    pub use crate::rewrite::expr::rewriter::ExpressionRewriter;
    pub use crate::rewrite::expr::rules::expression_matcher::{
        AnyExpressionMatcher, ExpressionMatcher,
    };
    pub use crate::rewrite::expr::rules::rule::{Rule, RuleResult};
}
