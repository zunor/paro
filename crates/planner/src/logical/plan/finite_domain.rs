// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Small SQL-derived domains used by relational proofs. These are not
//! statistics: absence means unknown, including unsupported value types.

use crate::logical::operator::ColumnBinding;
use paro_common::runtime_value::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DomainValue {
    Null,
    Boolean(bool),
    Integer(i128),
    String(String),
}

impl DomainValue {
    pub fn from_value(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Null(_) => Self::Null,
            Value::Boolean(v) => Self::Boolean(*v),
            Value::TinyInt(v) => Self::Integer(*v as i128),
            Value::SmallInt(v) => Self::Integer(*v as i128),
            Value::Integer(v) => Self::Integer(*v as i128),
            Value::BigInt(v) => Self::Integer(*v as i128),
            Value::HugeInt(v) => Self::Integer(*v),
            Value::UTinyInt(v) => Self::Integer(*v as i128),
            Value::USmallInt(v) => Self::Integer(*v as i128),
            Value::UInteger(v) => Self::Integer(*v as i128),
            Value::UBigInt(v) => Self::Integer(*v as i128),
            Value::Varchar(v) if v.len() <= 256 => Self::String(v.clone()),
            // No float/decimal coercion, collation or prefix assumptions.
            _ => return None,
        })
    }
}

pub type FiniteDomains = BTreeMap<ColumnBinding, BTreeSet<DomainValue>>;
pub const MAX_DOMAIN_VALUES: usize = 32;
