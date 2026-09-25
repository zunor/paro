// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate optimization passes.

pub mod deduplicate;
pub mod dimension_deferral;
#[cfg(test)]
pub mod dimension_sharing;
pub mod distinct_decomposition;
#[cfg(test)]
pub mod input_materialization;
#[cfg(test)]
pub mod join_preaggregation;
#[cfg(test)]
pub mod join_subsumption;
#[cfg(test)]
pub mod non_null_inputs;
#[cfg(test)]
pub mod post_reduction;
#[cfg(test)]
pub(crate) mod semantic_kernels;
pub mod singleton_groups;

#[cfg(test)]
mod dimension_deferral_tests;
#[cfg(test)]
mod input_materialization_tests;
