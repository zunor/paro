// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct IntoClause {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
    pub target_columns: Vec<Identifier>,
    pub source_columns: Vec<SourceExpr>,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
#[allow(clippy::large_enum_variant)]
pub enum SourceExpr {
    Expr(Expr),
    Default,
}

impl Display for SourceExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SourceExpr::Expr(expr) => expr.fmt(f),
            SourceExpr::Default => write!(f, "DEFAULT"),
        }
    }
}

impl Display for IntoClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "INTO ")?;
        if let Some(database) = &self.database {
            write!(f, "{}.", database)?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{}.", schema)?;
        }
        write!(f, "{}", self.table)?;
        if !self.target_columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.target_columns)?;
            write!(f, ")")?;
        }
        if !self.source_columns.is_empty() {
            write!(f, " VALUES ")?;
            write!(f, " (")?;
            write_comma_separated_list(f, &self.source_columns)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WhenClause {
    pub condition: Expr,
    pub into_clauses: Vec<IntoClause>,
}

impl Display for WhenClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WHEN ")?;
        self.condition.fmt(f)?;
        write!(f, " THEN ")?;
        for into_clause in &self.into_clauses {
            write!(f, "{} ", into_clause)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ElseClause {
    pub into_clauses: Vec<IntoClause>,
}

impl Display for ElseClause {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ELSE ")?;
        for into_clause in &self.into_clauses {
            write!(f, "{} ", into_clause)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct InsertMultiTableStmt {
    pub overwrite: bool,
    pub is_first: bool,
    pub when_clauses: Vec<WhenClause>,
    pub else_clause: Option<ElseClause>,
    pub into_clauses: Vec<IntoClause>,
    pub source: Query,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum InsertMultiTableKind {
    First,
    All,
}

impl Display for InsertMultiTableStmt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "INSERT ")?;
        if self.overwrite {
            write!(f, "OVERWRITE ")?;
        }
        match &self.is_first {
            true => write!(f, "FIRST ")?,
            false => write!(f, "ALL ")?,
        }
        for when in &self.when_clauses {
            write!(f, "{} ", when)?;
        }
        if let Some(else_clause) = &self.else_clause {
            write!(f, "{} ", else_clause)?;
        }
        for into_clause in &self.into_clauses {
            write!(f, "{} ", into_clause)?;
        }
        write!(f, "{}", self.source)
    }
}
