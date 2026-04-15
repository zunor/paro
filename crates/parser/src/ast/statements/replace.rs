// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::Expr;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::InsertSource;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ReplaceStmt {
    pub hints: Option<Hint>,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
    pub is_conflict: bool,
    pub on_conflict_columns: Vec<Identifier>,
    pub columns: Vec<Identifier>,
    pub source: InsertSource,
    pub delete_when: Option<Expr>,
}

impl Display for ReplaceStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REPLACE")?;
        if let Some(hints) = &self.hints {
            write!(f, " {}", hints)?;
        }
        write!(f, " INTO ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.table)),
        )?;

        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }

        // on_conflict_columns must be non-empty
        write!(f, " ON")?;
        if self.is_conflict {
            write!(f, " CONFLICT")?;
        }
        write!(f, " (")?;
        write_comma_separated_list(f, &self.on_conflict_columns)?;
        write!(f, ")")?;

        if let Some(expr) = &self.delete_when {
            write!(f, " DELETE WHEN {expr}")?;
        }

        write!(f, " {}", self.source)
    }
}
