// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::quote::QuotedString;
use crate::ast::statements::show::ShowLimit;
use crate::ast::CreateOption;
use crate::ast::Identifier;
use crate::ast::Literal;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateTagStmt {
    pub create_option: CreateOption,
    pub name: Identifier,
    pub allowed_values: Option<Vec<Literal>>,
    pub comment: Option<String>,
}

impl Display for CreateTagStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        write!(f, "TAG ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        if let Some(values) = &self.allowed_values {
            write!(f, " ALLOWED_VALUES = (")?;
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{v}")?;
            }
            write!(f, ")")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropTagStmt {
    pub if_exists: bool,
    pub name: Identifier,
}

impl Display for DropTagStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP TAG ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowTagsStmt {
    pub filter: Option<ShowLimit>,
    pub limit: Option<u64>,
}

impl Display for ShowTagsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW TAGS")?;
        if let Some(filter) = &self.filter {
            write!(f, " {filter}")?;
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}
