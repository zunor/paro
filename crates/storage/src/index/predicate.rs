// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Predicate Definitions
//!
//! Predicate types used by index evaluation.

use std::cmp::Ordering;
use std::collections::HashSet;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use crate::index::ColumnId;

/// Predicate over a single column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// column = value
    Eq { column_id: ColumnId, value: Value },
    /// column != value
    NotEq { column_id: ColumnId, value: Value },
    /// column < value
    Lt { column_id: ColumnId, value: Value },
    /// column <= value
    Le { column_id: ColumnId, value: Value },
    /// column > value
    Gt { column_id: ColumnId, value: Value },
    /// column >= value
    Ge { column_id: ColumnId, value: Value },
    /// column IN (values...)
    In {
        column_id: ColumnId,
        values: Vec<Value>,
    },
    /// column BETWEEN lower AND upper (inclusive)
    Range {
        column_id: ColumnId,
        lower: Value,
        upper: Value,
    },
    /// column IS NULL
    IsNull { column_id: ColumnId },
    /// column IS NOT NULL
    IsNotNull { column_id: ColumnId },
}

impl Predicate {
    /// Column id referenced by this predicate.
    pub fn column_id(&self) -> ColumnId {
        match self {
            Predicate::Eq { column_id, .. }
            | Predicate::NotEq { column_id, .. }
            | Predicate::Lt { column_id, .. }
            | Predicate::Le { column_id, .. }
            | Predicate::Gt { column_id, .. }
            | Predicate::Ge { column_id, .. }
            | Predicate::In { column_id, .. }
            | Predicate::Range { column_id, .. }
            | Predicate::IsNull { column_id }
            | Predicate::IsNotNull { column_id } => *column_id,
        }
    }
}

/// Predicate tree with AND/OR composition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateTree {
    /// Leaf predicate
    Leaf(Predicate),
    /// AND of children
    And(Vec<PredicateTree>),
    /// OR of children
    Or(Vec<PredicateTree>),
}

impl PredicateTree {
    /// Convenience constructor for a leaf.
    pub fn leaf(predicate: Predicate) -> Self {
        PredicateTree::Leaf(predicate)
    }
}

/// Collect unique column IDs referenced by a predicate tree (in first-seen order).
pub fn collect_predicate_columns(tree: &PredicateTree) -> Vec<ColumnId> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();

    fn visit(tree: &PredicateTree, seen: &mut HashSet<ColumnId>, columns: &mut Vec<ColumnId>) {
        match tree {
            PredicateTree::Leaf(predicate) => {
                let col = predicate.column_id();
                if seen.insert(col) {
                    columns.push(col);
                }
            }
            PredicateTree::And(children) | PredicateTree::Or(children) => {
                for child in children {
                    visit(child, seen, columns);
                }
            }
        }
    }

    visit(tree, &mut seen, &mut columns);
    columns
}

// =============================================================================
// Value Encoding Helpers
// =============================================================================

/// Convert a typed value into raw bytes for index evaluation.
///
/// For numeric types, this uses little-endian encoding to match existing
/// index writers. The comparator must decode accordingly for ordering.
pub fn value_to_bytes(value: &Value, logical_type: &LogicalType) -> Result<Vec<u8>> {
    match (logical_type, value) {
        (LogicalType::Boolean, Value::Boolean(v)) => Ok(vec![*v as u8]),
        (LogicalType::TinyInt, Value::TinyInt(v)) => Ok(vec![*v as u8]),
        (LogicalType::SmallInt, Value::SmallInt(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::Integer, Value::Integer(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::BigInt, Value::BigInt(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::HugeInt, Value::HugeInt(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::UTinyInt, Value::UTinyInt(v)) => Ok(vec![*v]),
        (LogicalType::USmallInt, Value::USmallInt(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::UInteger, Value::UInteger(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::UBigInt, Value::UBigInt(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::UHugeInt, Value::UHugeInt(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::Uuid, Value::Uuid(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::Float, Value::Float(v)) => Ok(v.to_le_bytes().to_vec()),
        (LogicalType::Double, Value::Double(v)) => Ok(v.to_le_bytes().to_vec()),
        (
            LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb,
            Value::Varchar(v),
        ) => Ok(v.as_bytes().to_vec()),
        (LogicalType::Blob, Value::Blob(v)) => Ok(v.clone()),
        (LogicalType::Null, Value::Null(_)) => Ok(Vec::new()),
        _ => Err(paro_error::not_implemented(format!(
            "Unsupported value/logical type pair: {:?} / {:?}",
            value, logical_type
        ))),
    }
}

/// Compare two raw byte values according to the logical type.
///
/// This is required because many index encodings use little-endian bytes
/// that are not order-preserving for numeric types.
pub fn compare_bytes(logical_type: &LogicalType, left: &[u8], right: &[u8]) -> Result<Ordering> {
    match logical_type {
        LogicalType::Boolean => {
            let l = left.first().copied().unwrap_or(0) != 0;
            let r = right.first().copied().unwrap_or(0) != 0;
            Ok(l.cmp(&r))
        }
        LogicalType::TinyInt => {
            let l = *left.first().unwrap_or(&0) as i8;
            let r = *right.first().unwrap_or(&0) as i8;
            Ok(l.cmp(&r))
        }
        LogicalType::SmallInt => {
            let l = i16::from_le_bytes(
                left.get(..2)
                    .ok_or_else(|| paro_error::type_mismatch("SmallInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = i16::from_le_bytes(
                right
                    .get(..2)
                    .ok_or_else(|| paro_error::type_mismatch("SmallInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::Integer => {
            let l = i32::from_le_bytes(
                left.get(..4)
                    .ok_or_else(|| paro_error::type_mismatch("Integer: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = i32::from_le_bytes(
                right
                    .get(..4)
                    .ok_or_else(|| paro_error::type_mismatch("Integer: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::BigInt => {
            let l = i64::from_le_bytes(
                left.get(..8)
                    .ok_or_else(|| paro_error::type_mismatch("BigInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = i64::from_le_bytes(
                right
                    .get(..8)
                    .ok_or_else(|| paro_error::type_mismatch("BigInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::HugeInt => {
            let l = i128::from_le_bytes(
                left.get(..16)
                    .ok_or_else(|| paro_error::type_mismatch("HugeInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = i128::from_le_bytes(
                right
                    .get(..16)
                    .ok_or_else(|| paro_error::type_mismatch("HugeInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::UTinyInt => {
            let l = *left.first().unwrap_or(&0);
            let r = *right.first().unwrap_or(&0);
            Ok(l.cmp(&r))
        }
        LogicalType::USmallInt => {
            let l = u16::from_le_bytes(
                left.get(..2)
                    .ok_or_else(|| paro_error::type_mismatch("USmallInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = u16::from_le_bytes(
                right
                    .get(..2)
                    .ok_or_else(|| paro_error::type_mismatch("USmallInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::UInteger => {
            let l = u32::from_le_bytes(
                left.get(..4)
                    .ok_or_else(|| paro_error::type_mismatch("UInteger: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = u32::from_le_bytes(
                right
                    .get(..4)
                    .ok_or_else(|| paro_error::type_mismatch("UInteger: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::UBigInt => {
            let l = u64::from_le_bytes(
                left.get(..8)
                    .ok_or_else(|| paro_error::type_mismatch("UBigInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = u64::from_le_bytes(
                right
                    .get(..8)
                    .ok_or_else(|| paro_error::type_mismatch("UBigInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::UHugeInt => {
            let l = u128::from_le_bytes(
                left.get(..16)
                    .ok_or_else(|| paro_error::type_mismatch("UHugeInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = u128::from_le_bytes(
                right
                    .get(..16)
                    .ok_or_else(|| paro_error::type_mismatch("UHugeInt: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::Uuid => {
            let l = u128::from_le_bytes(
                left.get(..16)
                    .ok_or_else(|| paro_error::type_mismatch("UUID: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = u128::from_le_bytes(
                right
                    .get(..16)
                    .ok_or_else(|| paro_error::type_mismatch("UUID: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.cmp(&r))
        }
        LogicalType::Float => {
            let l = f32::from_le_bytes(
                left.get(..4)
                    .ok_or_else(|| paro_error::type_mismatch("Float: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = f32::from_le_bytes(
                right
                    .get(..4)
                    .ok_or_else(|| paro_error::type_mismatch("Float: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.partial_cmp(&r).unwrap_or(Ordering::Equal))
        }
        LogicalType::Double => {
            let l = f64::from_le_bytes(
                left.get(..8)
                    .ok_or_else(|| paro_error::type_mismatch("Double: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            let r = f64::from_le_bytes(
                right
                    .get(..8)
                    .ok_or_else(|| paro_error::type_mismatch("Double: insufficient bytes"))?
                    .try_into()
                    .unwrap(),
            );
            Ok(l.partial_cmp(&r).unwrap_or(Ordering::Equal))
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::Blob => Ok(left.cmp(right)),
        _ => Err(paro_error::not_implemented(format!(
            "Unsupported logical type for compare: {:?}",
            logical_type
        ))),
    }
}
