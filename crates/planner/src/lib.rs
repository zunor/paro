// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binding and shared plan contracts: expressions, logical plans, and immutable
//! physical plans. Optimization decisions and execution machinery live in their
//! respective crates; this crate must not depend on either implementation.
//!
//! Entry points: [`crate::planner::Planner`], [`crate::binder::Binder`], [`crate::operator::LogicalOperator`].
//! Types live in submodules (for example [`crate::visitor::LogicalOperatorVisitor`]), not at the crate root.

pub mod binder;
pub mod expression;
mod logical_properties;
pub mod operator;
pub mod plan;
pub mod planner;
mod stack;
pub mod verify;
pub mod visitor;

pub mod physical;
