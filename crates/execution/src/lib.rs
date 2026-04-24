// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical operators and the pipeline-based query execution engine.
//!
//! Import types from their submodules; this crate does not expose crate-root re-exports.

pub mod column_binding_resolver;
pub mod execution_context;
pub mod explain;
pub mod expression_executor;
pub mod join_hashtable;
pub mod memory_runtime;
pub mod operator;
pub mod operator_type;
pub mod physical_plan;
pub mod pipeline;
pub mod query_executor;
pub mod result_type;
pub mod sorting;
pub mod spill;
pub mod thread_context;
