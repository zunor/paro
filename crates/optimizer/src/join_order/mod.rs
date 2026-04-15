// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cost-based join order optimization using dynamic programming.

pub mod cardinality;
pub mod cost_model;
pub mod enumerator;
pub mod optimizer;
pub mod query_graph;
pub mod relation;
pub mod relation_manager;
