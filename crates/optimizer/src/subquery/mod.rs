// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Subquery optimization passes.

pub mod delim_join_elimination;
pub mod empty_result;
pub(crate) mod output_contract;
pub mod partition_aggregate;
pub mod scalar_aggregate_window;

#[cfg(test)]
mod partition_aggregate_tests;
#[cfg(test)]
mod scalar_aggregate_window_tests;
