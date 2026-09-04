// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statistics propagation and cost estimation.

pub(crate) mod aggregate_filter;
pub(crate) mod cardinality_bound;
pub mod cost;
pub mod gathering;
pub mod propagator;
pub(crate) mod unique_keys;
