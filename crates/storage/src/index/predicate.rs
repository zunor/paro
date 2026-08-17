// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Predicate Definitions
//!
//! Predicate types used by index evaluation.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

use crate::index::ColumnId;

use super::fixed_membership::FixedMembership;

/// Ordering operation used by a row-level column comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateComparison {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl fmt::Display for PredicateComparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
        })
    }
}

/// Predicate evaluated by indexes and/or by the storage row verifier.
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
    /// Physical fixed-width membership. Runtime filters use this form to
    /// preserve their frozen dense/sorted representation across scan readers.
    FixedIn {
        column_id: ColumnId,
        values: FixedMembership,
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
    /// Binary VARCHAR prefix test. `negated` represents `NOT LIKE 'prefix%'`.
    StringPrefix {
        column_id: ColumnId,
        prefix: String,
        negated: bool,
    },
    /// Binary VARCHAR value starts with at least one prefix.
    ///
    /// This is the storage form for exact rewrites such as a fixed-width
    /// leading substring membership predicate. Prefixes are alternatives, not
    /// a conjunction.
    StringPrefixIn {
        column_id: ColumnId,
        prefixes: Vec<String>,
    },
    /// Constant ASCII SQL LIKE pattern compiled by the scan reader. Patterns
    /// containing `_` remain on the general expression path.
    StringLike {
        column_id: ColumnId,
        pattern: String,
        negated: bool,
    },
    /// left_column op right_column. This cannot be answered by a single-column
    /// index and is always verified against rows selected by other predicates.
    ColumnComparison {
        left_column_id: ColumnId,
        right_column_id: ColumnId,
        comparison: PredicateComparison,
    },
}

impl Predicate {
    /// Single column eligible for index lookup, or `None` for a multi-column
    /// row-verification predicate.
    pub fn index_column_id(&self) -> Option<ColumnId> {
        match self {
            Predicate::Eq { column_id, .. }
            | Predicate::NotEq { column_id, .. }
            | Predicate::Lt { column_id, .. }
            | Predicate::Le { column_id, .. }
            | Predicate::Gt { column_id, .. }
            | Predicate::Ge { column_id, .. }
            | Predicate::In { column_id, .. }
            | Predicate::FixedIn { column_id, .. }
            | Predicate::Range { column_id, .. }
            | Predicate::IsNull { column_id }
            | Predicate::IsNotNull { column_id }
            | Predicate::StringPrefix { column_id, .. }
            | Predicate::StringPrefixIn { column_id, .. }
            | Predicate::StringLike { column_id, .. } => Some(*column_id),
            Predicate::ColumnComparison { .. } => None,
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Predicate::Eq { column_id, value } => write!(f, "col#{column_id} = {value}"),
            Predicate::NotEq { column_id, value } => write!(f, "col#{column_id} != {value}"),
            Predicate::Lt { column_id, value } => write!(f, "col#{column_id} < {value}"),
            Predicate::Le { column_id, value } => write!(f, "col#{column_id} <= {value}"),
            Predicate::Gt { column_id, value } => write!(f, "col#{column_id} > {value}"),
            Predicate::Ge { column_id, value } => write!(f, "col#{column_id} >= {value}"),
            Predicate::In { column_id, values } => {
                let values = values
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "col#{column_id} IN ({values})")
            }
            Predicate::FixedIn { column_id, values } => {
                write!(f, "col#{column_id} IN ({} fixed values)", values.len())
            }
            Predicate::Range {
                column_id,
                lower,
                upper,
            } => write!(f, "col#{column_id} BETWEEN {lower} AND {upper}"),
            Predicate::IsNull { column_id } => write!(f, "col#{column_id} IS NULL"),
            Predicate::IsNotNull { column_id } => write!(f, "col#{column_id} IS NOT NULL"),
            Predicate::StringPrefix {
                column_id,
                prefix,
                negated,
            } => write!(
                f,
                "col#{column_id} {} PREFIX {prefix:?}",
                if *negated { "NOT" } else { "HAS" }
            ),
            Predicate::StringPrefixIn {
                column_id,
                prefixes,
            } => write!(
                f,
                "col#{column_id} HAS PREFIX IN ({})",
                prefixes
                    .iter()
                    .map(|prefix| format!("{prefix:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Predicate::StringLike {
                column_id,
                pattern,
                negated,
            } => write!(
                f,
                "col#{column_id} {} {pattern:?}",
                if *negated { "NOT LIKE" } else { "LIKE" }
            ),
            Predicate::ColumnComparison {
                left_column_id,
                right_column_id,
                comparison,
            } => write!(f, "col#{left_column_id} {comparison} col#{right_column_id}"),
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

    /// Prove that every row admitted by this tree has the requested ASCII
    /// prefix width on `column_id`.  A witness under conjunction is sufficient;
    /// disjunction requires branch-wise reasoning and is deliberately rejected.
    pub fn proves_ascii_prefix_width(&self, column_id: ColumnId, byte_width: usize) -> bool {
        match self {
            Self::Leaf(Predicate::StringPrefixIn {
                column_id: candidate,
                prefixes,
            }) => {
                *candidate == column_id
                    && byte_width > 0
                    && !prefixes.is_empty()
                    && prefixes
                        .iter()
                        .all(|prefix| prefix.is_ascii() && prefix.len() == byte_width)
            }
            Self::And(children) => children
                .iter()
                .any(|child| child.proves_ascii_prefix_width(column_id, byte_width)),
            Self::Or(_) | Self::Leaf(_) => false,
        }
    }
}

impl fmt::Display for PredicateTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PredicateTree::Leaf(predicate) => write!(f, "{predicate}"),
            PredicateTree::And(children) => write_predicate_children(f, children, " AND "),
            PredicateTree::Or(children) => write_predicate_children(f, children, " OR "),
        }
    }
}

fn write_predicate_children(
    f: &mut fmt::Formatter<'_>,
    children: &[PredicateTree],
    separator: &str,
) -> fmt::Result {
    for (idx, child) in children.iter().enumerate() {
        if idx > 0 {
            f.write_str(separator)?;
        }
        write!(f, "{child}")?;
    }
    Ok(())
}

/// Collect unique column IDs referenced by a predicate tree (in first-seen order).
pub fn collect_predicate_columns(tree: &PredicateTree) -> Vec<ColumnId> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();

    fn visit(tree: &PredicateTree, seen: &mut HashSet<ColumnId>, columns: &mut Vec<ColumnId>) {
        match tree {
            PredicateTree::Leaf(predicate) => match predicate {
                Predicate::ColumnComparison {
                    left_column_id,
                    right_column_id,
                    ..
                } => {
                    for column_id in [*left_column_id, *right_column_id] {
                        if seen.insert(column_id) {
                            columns.push(column_id);
                        }
                    }
                }
                predicate => {
                    let column_id = predicate
                        .index_column_id()
                        .expect("single-column predicate must expose its index column");
                    if seen.insert(column_id) {
                        columns.push(column_id);
                    }
                }
            },
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
