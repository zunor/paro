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
use crate::ast::Settings;
use crate::ast::Statement;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct TransactionStmt {
    pub kind: TransactionKind,
}

impl Display for TransactionStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            TransactionKind::Begin(options) => {
                write!(f, "BEGIN")?;
                options.write_suffix(f)
            }
            TransactionKind::Start(options) => {
                write!(f, "START TRANSACTION")?;
                options.write_suffix(f)
            }
            TransactionKind::Commit => write!(f, "COMMIT"),
            TransactionKind::Rollback => write!(f, "ROLLBACK"),
            TransactionKind::SetTransaction(options) => {
                write!(f, "SET TRANSACTION")?;
                options.write_suffix(f)
            }
            TransactionKind::SetSessionCharacteristics(options) => {
                write!(f, "SET SESSION CHARACTERISTICS AS TRANSACTION")?;
                options.write_suffix(f)
            }
            TransactionKind::Savepoint(name) => write!(f, "SAVEPOINT {name}"),
            TransactionKind::ReleaseSavepoint(name) => write!(f, "RELEASE SAVEPOINT {name}"),
            TransactionKind::RollbackToSavepoint(name) => {
                write!(f, "ROLLBACK TO SAVEPOINT {name}")
            }
            TransactionKind::PrepareTransaction(gid) => write!(f, "PREPARE TRANSACTION '{gid}'"),
            TransactionKind::CommitPrepared(gid) => write!(f, "COMMIT PREPARED '{gid}'"),
            TransactionKind::RollbackPrepared(gid) => write!(f, "ROLLBACK PREPARED '{gid}'"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum TransactionKind {
    Begin(TransactionOptions),
    Start(TransactionOptions),
    Commit,
    Rollback,
    SetTransaction(TransactionOptions),
    SetSessionCharacteristics(TransactionOptions),
    Savepoint(Identifier),
    ReleaseSavepoint(Identifier),
    RollbackToSavepoint(Identifier),
    PrepareTransaction(String),
    CommitPrepared(String),
    RollbackPrepared(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Drive, DriveMut)]
pub struct TransactionOptions {
    pub isolation_level: Option<TransactionIsolationLevel>,
    pub read_mode: Option<TransactionReadMode>,
}

impl TransactionOptions {
    pub fn is_empty(self) -> bool {
        self.isolation_level.is_none() && self.read_mode.is_none()
    }

    fn write_suffix(self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        if let Some(isolation_level) = self.isolation_level {
            write!(f, " ISOLATION LEVEL {isolation_level}")?;
        }
        if let Some(read_mode) = self.read_mode {
            write!(f, " {read_mode}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum TransactionIsolationLevel {
    Serializable,
    Snapshot,
}

impl Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionIsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
            TransactionIsolationLevel::Snapshot => write!(f, "SNAPSHOT"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum TransactionReadMode {
    ReadOnly,
    ReadWrite,
}

impl Display for TransactionReadMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionReadMode::ReadOnly => write!(f, "READ ONLY"),
            TransactionReadMode::ReadWrite => write!(f, "READ WRITE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct VariableSetStmt {
    pub kind: VariableSetKind,
    pub settings: Settings,
}

impl Display for VariableSetStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            VariableSetKind::Set => write!(f, "SET {}", self.settings),
            VariableSetKind::Reset => write!(f, "RESET {}", self.settings),
            VariableSetKind::ResetAll => write!(f, "RESET ALL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum VariableSetKind {
    Set,
    Reset,
    ResetAll,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct VariableShowStmt {
    pub target: VariableShowTarget,
}

impl Display for VariableShowStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.target {
            VariableShowTarget::All => write!(f, "SHOW ALL"),
            VariableShowTarget::Name(variable) => write!(f, "SHOW {variable}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum VariableShowTarget {
    All,
    Name(Identifier),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct PrepareStmt {
    pub name: Identifier,
    pub parameter_types: Vec<TypeName>,
    pub statement: Box<Statement>,
}

impl Display for PrepareStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PREPARE {}", self.name)?;
        if !self.parameter_types.is_empty() {
            write!(f, " (")?;
            write_comma_separated_list(f, &self.parameter_types)?;
            write!(f, ")")?;
        }
        write!(f, " AS {}", self.statement)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ExecuteStmt {
    pub name: Identifier,
    pub args: Vec<Box<Expr>>,
}

impl Display for ExecuteStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EXECUTE {}", self.name)?;
        if !self.args.is_empty() {
            write!(f, "(")?;
            write_comma_separated_list(f, &self.args)?;
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DeallocateStmt {
    pub name: Option<Identifier>,
}

impl Display for DeallocateStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "DEALLOCATE {name}"),
            None => write!(f, "DEALLOCATE ALL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DeclareCursorStmt {
    pub name: Identifier,
    pub scroll: CursorScrollMode,
    pub hold: bool,
    pub query: Box<Statement>,
}

impl Display for DeclareCursorStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DECLARE {} ", self.name)?;
        match self.scroll {
            CursorScrollMode::Unspecified => {}
            CursorScrollMode::Scroll => write!(f, "SCROLL ")?,
            CursorScrollMode::NoScroll => write!(f, "NO SCROLL ")?,
        }
        write!(f, "CURSOR ")?;
        if self.hold {
            write!(f, "WITH HOLD ")?;
        }
        write!(f, "FOR {}", self.query)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum CursorScrollMode {
    Unspecified,
    Scroll,
    NoScroll,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct FetchStmt {
    pub ismove: bool,
    pub direction: FetchDirection,
    pub cursor: Identifier,
}

impl Display for FetchStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.ismove {
            write!(f, "MOVE {}", self.direction)?;
        } else {
            write!(f, "FETCH {}", self.direction)?;
        }
        write!(f, " FROM {}", self.cursor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    ForwardAll,
    BackwardAll,
    Count(i64),
    ForwardCount(i64),
    BackwardCount(i64),
    Absolute(i64),
    Relative(i64),
}

impl Display for FetchDirection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchDirection::Next => write!(f, "NEXT"),
            FetchDirection::Prior => write!(f, "PRIOR"),
            FetchDirection::First => write!(f, "FIRST"),
            FetchDirection::Last => write!(f, "LAST"),
            FetchDirection::ForwardAll => write!(f, "FORWARD ALL"),
            FetchDirection::BackwardAll => write!(f, "BACKWARD ALL"),
            FetchDirection::Count(count) => write!(f, "{count}"),
            FetchDirection::ForwardCount(count) => write!(f, "FORWARD {count}"),
            FetchDirection::BackwardCount(count) => write!(f, "BACKWARD {count}"),
            FetchDirection::Absolute(count) => write!(f, "ABSOLUTE {count}"),
            FetchDirection::Relative(count) => write!(f, "RELATIVE {count}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CloseCursorStmt {
    pub name: Option<Identifier>,
}

impl Display for CloseCursorStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.name {
            Some(name) => write!(f, "CLOSE {name}"),
            None => write!(f, "CLOSE ALL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DiscardStmt {
    pub target: DiscardTarget,
}

impl Display for DiscardStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DISCARD {}", self.target)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Drive, DriveMut)]
pub enum DiscardTarget {
    All,
    Plans,
    Temp,
    Sequences,
}

impl Display for DiscardTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscardTarget::All => write!(f, "ALL"),
            DiscardTarget::Plans => write!(f, "PLANS"),
            DiscardTarget::Temp => write!(f, "TEMP"),
            DiscardTarget::Sequences => write!(f, "SEQUENCES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CheckpointStmt;

impl Display for CheckpointStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CHECKPOINT")
    }
}
