// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterRoleStmt {
    pub if_exists: bool,
    pub name: String,
    pub action: AlterRoleAction,
}

impl Display for AlterRoleStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER ROLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;
        write!(f, "{}", self.action)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterRoleAction {
    Comment(Option<String>),
}

impl Display for AlterRoleAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterRoleAction::Comment(Some(comment)) => {
                write!(f, " SET COMMENT = '{}'", comment)?;
            }
            AlterRoleAction::Comment(None) => {
                write!(f, " UNSET COMMENT")?;
            }
        }
        Ok(())
    }
}
