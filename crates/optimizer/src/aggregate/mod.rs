// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate optimization passes.

pub mod common;
pub mod dimension_deferral;
pub mod join_preaggregation;
pub mod join_subsumption;
pub mod late_payload;
pub mod post_reduction;
pub(crate) mod semantic_kernels;
pub mod statistics_exec;

#[cfg(test)]
mod dimension_deferral_tests;
#[cfg(test)]
mod late_payload_tests;
