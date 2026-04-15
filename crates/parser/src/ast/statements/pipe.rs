// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::CopyStmt;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreatePipeStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub auto_ingest: bool,
    pub comments: String,
    pub copy_stmt: CopyStmt,
}

impl Display for CreatePipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE PIPE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name)?;

        if self.auto_ingest {
            write!(f, " AUTO_INGEST = TRUE")?;
        }

        if !self.comments.is_empty() {
            write!(f, " COMMENTS = '{}'", self.comments)?;
        }

        write!(f, " AS {}", self.copy_stmt)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropPipeStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropPipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP PIPE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescribePipeStmt {
    pub name: String,
}

impl Display for DescribePipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE PIPE {}", self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterPipeStmt {
    pub if_exists: bool,
    pub name: String,
    pub options: AlterPipeOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum AlterPipeOptions {
    Set {
        execution_paused: Option<bool>,
        comments: Option<String>,
    },
    Refresh {
        prefix: Option<String>,
        modified_after: Option<String>,
    },
}

impl Display for AlterPipeOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AlterPipeOptions::Set {
                execution_paused,
                comments,
            } => {
                if let Some(execution_paused) = execution_paused {
                    write!(f, " SET PIPE_EXECUTION_PAUSED = {}", execution_paused)?;
                }
                if let Some(comments) = comments {
                    write!(f, " SET COMMENTS = '{}'", comments)?;
                }
                Ok(())
            }
            AlterPipeOptions::Refresh {
                prefix,
                modified_after,
            } => {
                write!(f, " REFRESH")?;
                if let Some(prefix) = prefix {
                    write!(f, " PREFIX = '{}'", prefix)?;
                }
                if let Some(modified_after) = modified_after {
                    write!(f, " MODIFIED_AFTER = '{}'", modified_after)?;
                }
                Ok(())
            }
        }
    }
}

impl Display for AlterPipeStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER PIPE")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        write!(f, "{}", self.options)?;
        Ok(())
    }
}
