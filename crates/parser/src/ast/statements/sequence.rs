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
use crate::ast::CreateOption;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateSequenceStmt {
    pub create_option: CreateOption,
    pub sequence: Identifier,
    pub start: Option<u64>,
    pub increment: Option<u64>,
    pub comment: Option<String>,
}

impl Display for CreateSequenceStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "SEQUENCE ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.sequence)?;

        if let Some(s) = &self.start {
            write!(f, " START = {}", s)?;
        }

        if let Some(i) = &self.increment {
            write!(f, " INCREMENT = {}", i)?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropSequenceStmt {
    pub if_exists: bool,
    pub sequence: Identifier,
}

impl Display for DropSequenceStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SEQUENCE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.sequence)?;
        Ok(())
    }
}
