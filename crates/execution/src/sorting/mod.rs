// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Sorting operators, run storage, and merge helpers.

pub mod sort_descriptor;
pub mod sort_key_store;
pub mod sort_projection_column;
pub mod sorted_run;
pub mod sorted_run_merger;

#[cfg(test)]
mod tests;
