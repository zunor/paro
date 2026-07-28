// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! SQL AST → logical plan: binding, name resolution, and logical operator trees.
//!
//! Entry points: [`crate::planner::Planner`], [`crate::binder::Binder`], [`crate::operator::LogicalOperator`].
//! Types live in submodules (for example [`crate::visitor::LogicalOperatorVisitor`]), not at the crate root.

pub mod binder;
pub mod expression;
pub mod operator;
pub mod plan;
pub mod planner;
mod stack;
pub mod verify;
pub mod visitor;
