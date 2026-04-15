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

use crate::ast::CreateOption;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateConnectionStmt {
    pub name: Identifier,
    pub storage_type: String,
    pub storage_params: BTreeMap<String, String>,
    pub create_option: CreateOption,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropConnectionStmt {
    pub if_exists: bool,
    pub name: Identifier,
}

impl Display for DropConnectionStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP CONNECTION ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{} ", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescribeConnectionStmt {
    pub name: Identifier,
}

impl Display for CreateConnectionStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " CONNECTION ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", self.name)?;
        write!(f, "STORAGE_TYPE = '{}'", self.storage_type)?;
        for (k, v) in &self.storage_params {
            write!(f, " {k} = '{v}'")?;
        }
        Ok(())
    }
}

impl Display for DescribeConnectionStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE CONNECTION {} ", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowConnectionsStmt {}

impl Display for ShowConnectionsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW CONNECTIONS")
    }
}
