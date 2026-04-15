// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Traverses and rewrites logical operator trees (`LogicalOperatorVisitor`).

mod logical_operator_visitor;

pub use logical_operator_visitor::{enumerate_expressions, LogicalOperatorVisitor};
