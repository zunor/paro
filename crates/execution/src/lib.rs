// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical operators and the pipeline-based query execution engine.
//!
//! Import types from their submodules; this crate does not expose crate-root re-exports.

pub mod explain;
pub mod expression_executor;
pub mod join_hashtable;
pub mod memory_runtime;
pub mod operators;
pub(crate) mod physical;
pub mod pipeline;
pub mod query_executor;
pub mod result_type;
pub mod runtime;
pub mod sorting;
pub mod spill;
pub mod thread_context;
