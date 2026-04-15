// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_dot_separated_list;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RefreshVirtualColumnStmt {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
    pub selection: Option<Box<Expr>>,
    pub limit: Option<u64>,
    pub overwrite: bool,
}

impl Display for RefreshVirtualColumnStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH VIRTUAL COLUMN ON ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.table)),
        )?;
        if let Some(selection) = &self.selection {
            write!(f, " WHERE {selection}")?;
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if self.overwrite {
            write!(f, " OVERWRITE")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowVirtualColumnsStmt {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Option<Identifier>,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowVirtualColumnsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW VIRTUAL COLUMNS")?;
        if let Some(table) = &self.table {
            write!(f, " FROM {}", table)?;
        }
        if let Some(schema) = &self.schema {
            write!(f, " FROM ")?;
            if let Some(database) = &self.database {
                write!(f, "{database}.",)?;
            }
            write!(f, "{schema}")?;
        } else if let Some(database) = &self.database {
            write!(f, " FROM {database}")?;
        }

        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}
