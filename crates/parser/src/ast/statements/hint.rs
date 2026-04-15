// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Expr;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Hint {
    pub hints_list: Vec<HintItem>,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct HintItem {
    pub name: Identifier,
    pub expr: Expr,
}

impl Display for Hint {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "/*+ ")?;
        for hint in &self.hints_list {
            write!(f, "SET_VAR(")?;
            write!(f, "{}", hint.name)?;
            write!(f, "=")?;
            write!(f, "{}", hint.expr)?;
            write!(f, ")")?;
        }
        write!(f, "*/")
    }
}
