// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum KillTarget {
    Query,
    Connection,
}

impl Display for KillTarget {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            KillTarget::Query => write!(f, "QUERY"),
            KillTarget::Connection => write!(f, "CONNECTION"),
        }
    }
}
