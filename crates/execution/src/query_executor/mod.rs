// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! High-level query execution coordination.

mod cleanup;
pub mod compiled;
#[cfg(test)]
mod control_region_executor_tests;
mod direct_dense_topk;
pub mod executor;
mod explain_output;
mod pipeline_driver;
mod program_executor;
#[cfg(test)]
mod program_executor_tests;
pub mod result;
pub mod stream;
