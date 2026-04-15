// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Expr;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum ShowLimit {
    Like { pattern: String },
    Where { selection: Box<Expr> },
}

impl Display for ShowLimit {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ShowLimit::Like { pattern } => write!(f, "LIKE '{pattern}'"),
            ShowLimit::Where { selection } => write!(f, "WHERE {selection}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowOptions {
    pub show_limit: Option<ShowLimit>,
    pub limit: Option<u64>,
}

impl Display for ShowOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(limit_option) = self.show_limit.clone() {
            write!(f, "{}", limit_option)?;
        }
        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        Ok(())
    }
}
