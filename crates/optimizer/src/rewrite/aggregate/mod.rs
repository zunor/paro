// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate optimization passes.

pub mod deduplicate;
pub mod dimension_deferral;
pub mod distinct_decomposition;
pub mod singleton_groups;
