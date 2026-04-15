// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::write_space_separated_string_map;
use crate::ast::CreateOption;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateAggregatingIndexStmt {
    pub index_kind: IndexKind,
    pub create_option: CreateOption,
    pub index_name: Identifier,
    pub query: Box<Query>,
    pub sync_creation: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum IndexKind {
    Aggregating,
    Inverted,
    Ngram,
    Vector,
}

impl Display for IndexKind {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            IndexKind::Aggregating => write!(f, "AGGREGATING"),
            IndexKind::Inverted => write!(f, "INVERTED"),
            IndexKind::Ngram => write!(f, "NGRAM"),
            IndexKind::Vector => write!(f, "VECTOR"),
        }
    }
}

impl Display for CreateAggregatingIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        if !self.sync_creation {
            write!(f, "ASYNC ")?;
        }
        write!(f, "{} INDEX", self.index_kind)?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.index_name)?;
        write!(f, " AS {}", self.query)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropIndexStmt {
    pub if_exists: bool,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub index: Identifier,
}

impl Display for DropIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP INDEX")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.index)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RefreshAggregatingIndexStmt {
    pub index: Identifier,
    pub limit: Option<u64>,
}

impl Display for RefreshAggregatingIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH AGGREGATING INDEX {}", self.index)?;
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateIndexStmt {
    pub create_option: CreateOption,
    pub is_unique: bool,
    pub index_name: Identifier,
    pub index_kind: Option<IndexKind>,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
    pub using_method: Option<Identifier>,
    pub expressions: Vec<Expr>,
    pub columns: Vec<Identifier>,
    pub sync_creation: bool,
    pub index_options: BTreeMap<String, String>,
}

impl Display for CreateIndexStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        if !self.sync_creation {
            write!(f, "ASYNC ")?;
        }
        if self.is_unique {
            write!(f, "UNIQUE ")?;
        }
        if self.using_method.is_some() {
            write!(f, "INDEX")?;
        } else if let Some(index_kind) = self.index_kind {
            write!(f, "{} INDEX", index_kind)?;
        } else {
            write!(f, "INDEX")?;
        }
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.index_name)?;
        write!(f, " ON ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.table)),
        )?;
        if let Some(method) = &self.using_method {
            write!(f, " USING {method}")?;
        }
        write!(f, " (")?;
        if self.expressions.is_empty() {
            write_comma_separated_list(f, &self.columns)?;
        } else {
            write_comma_separated_list(f, &self.expressions)?;
        }
        write!(f, ")")?;
        if !self.index_options.is_empty() {
            write!(f, " ")?;
            write_space_separated_string_map(f, &self.index_options)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropIndexOnTableStmt {
    pub if_exists: bool,
    pub index_name: Identifier,
    pub index_kind: IndexKind,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
}

impl Display for DropIndexOnTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP {} INDEX", self.index_kind)?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.index_name)?;
        write!(f, " ON ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.table)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct RefreshIndexOnTableStmt {
    pub index_name: Identifier,
    pub index_kind: IndexKind,
    pub database: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub table: Identifier,
    pub limit: Option<u64>,
}

impl Display for RefreshIndexOnTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "REFRESH {} INDEX {}", self.index_kind, self.index_name)?;
        write!(f, " ON ")?;
        write_dot_separated_list(
            f,
            self.database
                .iter()
                .chain(&self.schema)
                .chain(Some(&self.table)),
        )?;
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}
