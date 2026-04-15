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
use crate::ast::Hint;
use crate::ast::TableReference;
use crate::ast::With;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DeleteStmt {
    pub hints: Option<Hint>,
    pub table: TableReference,
    pub selection: Option<Expr>,
    // With clause, common table expression
    pub with: Option<With>,
}

impl Display for DeleteStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "DELETE ")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write!(f, "FROM {}", self.table)?;
        if let Some(conditions) = &self.selection {
            write!(f, " WHERE {conditions}")?;
        }
        Ok(())
    }
}
