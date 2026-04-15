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
pub enum Priority {
    HIGH,
    MEDIUM,
    LOW,
}

impl Display for Priority {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Priority::HIGH => write!(f, "HIGH"),
            Priority::MEDIUM => write!(f, "MEDIUM"),
            Priority::LOW => write!(f, "LOW"),
        }
    }
}
