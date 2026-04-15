// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pipeline construction, scheduling, and execution primitives.
//!
//! The high-level query coordinator lives in `crate::query_executor`.

pub mod build_pipelines;
pub mod build_state;
pub mod builder;
pub mod events;
pub mod executor;
pub mod meta_pipeline;
pub mod parallel_executor;
pub mod pipeline;
pub mod scheduler;
pub mod task;
