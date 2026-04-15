// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query optimizer passes and supporting infrastructure.

pub mod context;
pub mod cost_model;
pub mod optimizer;
pub mod optimizer_type;
pub(crate) mod pipeline_passes;
pub mod profiler;
pub mod rewriter;
pub(crate) mod verify;

pub mod aggregate;
pub mod column;
pub mod cte;
pub mod expression;
pub mod filter;
pub mod graph;
pub mod join;
pub mod join_order;
pub mod limit;
pub mod rules;
pub mod search;
pub mod statistics;
pub mod subquery;
