// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Expr;
use crate::ast::Query;

// settings: set a = xxx
// variable: set variable a = xxx
#[derive(Debug, Copy, Default, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SetType {
    #[default]
    SettingsSession,
    SettingsLocal,
    SettingsGlobal,
    Variable,
    SettingsQuery,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum SetValues {
    Expr(Vec<Box<Expr>>),
    Query(Box<Query>),
    // None means Unset Stmt
    None,
}
