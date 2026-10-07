// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cost-based join order optimization using dynamic programming.

pub(crate) mod candidate;
pub(crate) mod connected;
pub mod enumerator;
pub mod planner;
mod predicate_inference;
pub mod query_graph;
pub mod relation;
pub mod relation_manager;
