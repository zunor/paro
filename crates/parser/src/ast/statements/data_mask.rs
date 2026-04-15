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
use crate::ast::Expr;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DataMaskArg {
    pub arg_name: String,
    pub arg_type: TypeName,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DataMaskPolicy {
    pub args: Vec<DataMaskArg>,
    pub return_type: TypeName,
    pub body: Expr,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateDatamaskPolicyStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub policy: DataMaskPolicy,
}

impl Display for CreateDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE MASKING POLICY ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} AS (", self.name)?;
        let mut flag = false;
        for arg in &self.policy.args {
            if flag {
                write!(f, ",")?;
            }
            flag = true;
            write!(f, "{} {}", arg.arg_name, arg.arg_type)?;
        }
        write!(
            f,
            ") RETURNS {} -> {}",
            self.policy.return_type, self.policy.body
        )?;
        if let Some(comment) = &self.policy.comment {
            write!(f, " COMMENT = {}", QuotedString(comment, '\''))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropDatamaskPolicyStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP MASKING POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescDatamaskPolicyStmt {
    pub name: String,
}

impl Display for DescDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE MASKING POLICY {}", self.name)?;

        Ok(())
    }
}
