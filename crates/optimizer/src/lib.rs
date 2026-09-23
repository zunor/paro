// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query optimizer passes and supporting infrastructure.

pub mod cascades;
mod construction;
pub mod context;
pub mod cost_model;
pub mod optimizer;
pub mod physical;
pub mod profiler;
pub mod statement;
pub mod transformation_rejection;
pub(crate) mod verify;
pub mod work_partition;

pub mod aggregate;
pub mod column;
pub mod cte;
pub mod expression;
pub mod external;
pub mod filter;
pub mod graph;
pub mod join;
pub mod join_order;
pub mod limit;
pub mod rules;
pub mod search;
pub mod statistics;
pub mod subquery;

pub use optimizer::{OptimizedStatement, Optimizer};
pub use statement::{ReturningImageContract, WriteContract};
