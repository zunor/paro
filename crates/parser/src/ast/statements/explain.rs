// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainKind {
    Ast(String),
    Syntax(String),
    // The display string will be filled by optimizer, as we
    // don't want to expose `Memo` to other crates.
    Memo(String),
    Graph,
    Pipeline,
    Fragments,

    /// `EXPLAIN RAW` will be deprecated in the future, use EXPLAIN(LOGICAL) instead
    Raw,
    /// `EXPLAIN DECORRELATED` will show the plan after subquery decorrelation
    /// `EXPLAIN DECORRELATED` will be deprecated in the future, use `EXPLAIN(LOGICAL, DECORRELATED)` instead
    Decorrelated,
    /// `EXPLAIN OPTIMIZED` will be deprecated in the future, use `EXPLAIN(LOGICAL, OPTIMIZED)` instead
    Optimized,

    Plan,

    Join,

    // Explain analyze plan
    AnalyzePlan,

    Graphical,

    Perf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainOption {
    Verbose,
    Logical,
    Optimized,
    Decorrelated,
}
