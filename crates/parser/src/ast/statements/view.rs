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
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::ShowLimit;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateViewStmt {
    pub create_option: CreateOption,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub view: Identifier,
    pub columns: Vec<Identifier>,
    pub query: Box<Query>,
}

impl Display for CreateViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "VIEW ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.view)),
        )?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterViewStmt {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub view: Identifier,
    pub columns: Vec<Identifier>,
    pub query: Box<Query>,
}

impl Display for AlterViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER VIEW ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.view)),
        )?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.columns)?;
            write!(f, ")")?;
        }
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropViewStmt {
    pub if_exists: bool,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub view: Identifier,
}

impl Display for DropViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP VIEW ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.view)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowViewsStmt {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub full: bool,
    pub limit: Option<ShowLimit>,
    pub with_history: bool,
}

impl Display for ShowViewsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW")?;
        if self.full {
            write!(f, " FULL")?;
        }
        write!(f, " VIEWS")?;
        if self.with_history {
            write!(f, " HISTORY")?;
        }
        if let Some(schema) = &self.schema {
            write!(f, " FROM ")?;
            if let Some(database) = &self.database {
                write!(f, "{database}.",)?;
            }
            write!(f, "{schema}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescribeViewStmt {
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub view: Identifier,
}

impl Display for DescribeViewStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE VIEW ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(self.schema.iter().chain(Some(&self.view))),
        )
    }
}
