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
use crate::ast::Expr;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
#[allow(clippy::large_enum_variant)]
pub enum ConstraintType {
    Check(Expr),
    PrimaryKey(Vec<Identifier>),
}

impl Display for ConstraintType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ConstraintType::Check(expr) => {
                write!(f, "CHECK ({})", expr)
            }
            ConstraintType::PrimaryKey(columns) => {
                write!(f, "PRIMARY KEY (")?;
                write_comma_separated_list(f, columns)?;
                write!(f, ")")
            }
        }
    }
}
