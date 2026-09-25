// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Crate-local vocabulary for the optimizer-produced immutable physical plan.
//!
//! The public owner is `paro_planner::physical`; execution keeps this alias
//! only so runtime implementation modules can use a concise path.

pub use paro_planner::physical::*;
