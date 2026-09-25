// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate optimization passes.

pub mod common;
pub mod dimension_deferral;
pub mod dimension_sharing;
pub mod distinct_decomposition;
pub mod input_materialization;
pub mod join_preaggregation;
pub mod join_subsumption;
pub mod non_null_inputs;
pub mod post_reduction;
pub(crate) mod semantic_kernels;
pub mod singleton_groups;

#[cfg(test)]
mod dimension_deferral_tests;
#[cfg(test)]
mod input_materialization_tests;
