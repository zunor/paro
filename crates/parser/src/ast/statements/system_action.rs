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
pub struct SystemStmt {
    pub action: SystemAction,
}

impl Display for SystemStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SYSTEM {}", self.action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SystemAction {
    Backtrace(bool),
    FlushPrivileges,
}

impl Display for SystemAction {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SystemAction::Backtrace(switch) => match switch {
                true => write!(f, "ENABLE EXCEPTION_BACKTRACE"),
                false => write!(f, "DISABLE EXCEPTION_BACKTRACE"),
            },
            SystemAction::FlushPrivileges => write!(f, "FLUSH PRIVILEGES"),
        }
    }
}
