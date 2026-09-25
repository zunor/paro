// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic logical normalization. Costed alternatives belong to regions.

pub mod column;
pub mod cte;
pub mod expr;
pub mod external;
pub mod limit;
pub(crate) mod normalize;
pub mod predicate;
pub mod subquery;

pub mod aggregate;
pub mod graph;
pub mod join;
