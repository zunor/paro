// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binding and shared plan contracts: expressions, logical plans, and immutable
//! physical plans. Optimization decisions and execution machinery live in their
//! respective crates; this crate must not depend on either implementation.
//!
//! Entry points: [`crate::binder::Planner`], [`crate::binder::Binder`], [`crate::logical::operator::LogicalOperator`].
//! Types live in submodules (for example [`crate::logical::visitor::LogicalOperatorVisitor`]), not at the crate root.

pub mod binder;
pub mod expression;
pub mod logical;
mod stack;

pub mod physical;
