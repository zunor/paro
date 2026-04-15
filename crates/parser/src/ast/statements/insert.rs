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
use crate::ast::FileFormatOptions;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::MutationUpdateExpr;
use crate::ast::Query;
use crate::ast::With;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InsertStmt {
    pub hints: Option<Hint>,
    // With clause, common table expression
    pub with: Option<With>,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
    pub columns: Vec<Identifier>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflictClause>,
    pub overwrite: bool,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct OnConflictClause {
    pub columns: Vec<Identifier>,
    pub action: OnConflictAction,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate {
        update_list: Vec<MutationUpdateExpr>,
    },
}

impl Display for InsertStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "INSERT ")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        if self.overwrite {
            write!(f, "OVERWRITE ")?;
        } else {
            write!(f, "INTO ")?;
        }
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
        write!(f, " {}", self.source)?;
        if let Some(on_conflict) = &self.on_conflict {
            write!(f, " {}", on_conflict)?;
        }
        Ok(())
    }
}

impl Display for OnConflictClause {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ON CONFLICT (")?;
        write_comma_separated_list(f, &self.columns)?;
        write!(f, ") {}", self.action)
    }
}

impl Display for OnConflictAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::DoNothing => write!(f, "DO NOTHING"),
            Self::DoUpdate { update_list } => {
                write!(f, "DO UPDATE SET ")?;
                write_comma_separated_list(f, update_list)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum InsertSource {
    Values {
        rows: Vec<Vec<Expr>>,
    },
    RawValues {
        rest_str: String,
        start: usize,
    },
    Select {
        query: Box<Query>,
    },
    LoadFile {
        format_options: FileFormatOptions,
        value: Option<Vec<Expr>>,

        // '_databend_upload' => read from streaming upload handler body
        // _ => read from stage
        location: String,
    },
}

impl Display for InsertSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            InsertSource::Values { rows } => {
                write!(f, "VALUES ")?;
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(")?;
                    write_comma_separated_list(f, row)?;
                    write!(f, ")")?;
                }
                Ok(())
            }
            InsertSource::RawValues { rest_str, .. } => write!(f, "VALUES {rest_str}"),
            InsertSource::Select { query } => write!(f, "{query}"),
            InsertSource::LoadFile {
                value,
                format_options,
                location,
            } => {
                if let Some(value) = value {
                    write!(f, "VALUES (")?;
                    write_comma_separated_list(f, value)?;
                    write!(f, ")")?;
                }
                write!(f, " FROM @{location}",)?;
                write!(f, " FILE_FORMAT = ({})", format_options)?;
                Ok(())
            }
        }
    }
}
