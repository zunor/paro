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

impl PredicateComparison {
    pub fn with_value<V>(self, column_id: ColumnId, value: V) -> Predicate<V> {
        match self {
            Self::Equal => Predicate::Eq { column_id, value },
            Self::NotEqual => Predicate::NotEq { column_id, value },
            Self::LessThan => Predicate::Lt { column_id, value },
            Self::LessThanOrEqual => Predicate::Le { column_id, value },
            Self::GreaterThan => Predicate::Gt { column_id, value },
            Self::GreaterThanOrEqual => Predicate::Ge { column_id, value },
        }
    }
}

/// Predicate evaluated by indexes and/or by the storage row verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate<V = Value> {
    /// column = value
    Eq { column_id: ColumnId, value: V },
    /// column != value
    NotEq { column_id: ColumnId, value: V },
    /// column < value
    Lt { column_id: ColumnId, value: V },
    /// column <= value
    Le { column_id: ColumnId, value: V },
    /// column > value
    Gt { column_id: ColumnId, value: V },
    /// column >= value
    Ge { column_id: ColumnId, value: V },
    /// column IN (values...)
    In { column_id: ColumnId, values: Vec<V> },
    /// Physical fixed-width membership. Runtime filters use this form to
    /// preserve their frozen dense/sorted representation across scan readers.
    FixedIn {
        column_id: ColumnId,
        values: FixedMembership,
    },
    /// column BETWEEN lower AND upper (inclusive)
    Range {
        column_id: ColumnId,
        lower: V,
        upper: V,
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

impl<V> Predicate<V> {
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

    pub fn try_map_values<U, E>(
        self,
        map: &mut impl FnMut(V) -> std::result::Result<U, E>,
    ) -> std::result::Result<Predicate<U>, E> {
        Ok(match self {
            Self::Eq { column_id, value } => Predicate::Eq {
                column_id,
                value: map(value)?,
            },
            Self::NotEq { column_id, value } => Predicate::NotEq {
                column_id,
                value: map(value)?,
            },
            Self::Lt { column_id, value } => Predicate::Lt {
                column_id,
                value: map(value)?,
            },
            Self::Le { column_id, value } => Predicate::Le {
                column_id,
                value: map(value)?,
            },
            Self::Gt { column_id, value } => Predicate::Gt {
                column_id,
                value: map(value)?,
            },
            Self::Ge { column_id, value } => Predicate::Ge {
                column_id,
                value: map(value)?,
            },
            Self::In { column_id, values } => Predicate::In {
                column_id,
                values: values
                    .into_iter()
                    .map(map)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            },
            Self::FixedIn { column_id, values } => Predicate::FixedIn { column_id, values },
            Self::Range {
                column_id,
                lower,
                upper,
            } => Predicate::Range {
                column_id,
                lower: map(lower)?,
                upper: map(upper)?,
            },
            Self::IsNull { column_id } => Predicate::IsNull { column_id },
            Self::IsNotNull { column_id } => Predicate::IsNotNull { column_id },
            Self::StringPrefix {
                column_id,
                prefix,
                negated,
            } => Predicate::StringPrefix {
                column_id,
                prefix,
                negated,
            },
            Self::StringPrefixIn {
                column_id,
                prefixes,
            } => Predicate::StringPrefixIn {
                column_id,
                prefixes,
            },
            Self::StringLike {
                column_id,
                pattern,
                negated,
            } => Predicate::StringLike {
                column_id,
                pattern,
                negated,
            },
            Self::ColumnComparison {
                left_column_id,
                right_column_id,
                comparison,
            } => Predicate::ColumnComparison {
                left_column_id,
                right_column_id,
                comparison,
            },
        })
    }

    pub fn map_values<U>(self, map: &mut impl FnMut(V) -> U) -> Predicate<U> {
        self.try_map_values(&mut |value| Ok::<U, std::convert::Infallible>(map(value)))
            .unwrap_or_else(|never| match never {})
    }
}

impl<V: fmt::Display> fmt::Display for Predicate<V> {
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
pub enum PredicateTree<V = Value> {
    /// Leaf predicate
    Leaf(Predicate<V>),
    /// AND of children
    And(Vec<PredicateTree<V>>),
    /// OR of children
    Or(Vec<PredicateTree<V>>),
}

impl<V> PredicateTree<V> {
    /// Convenience constructor for a leaf.
    pub fn leaf(predicate: Predicate<V>) -> Self {
        PredicateTree::Leaf(predicate)
    }

    /// Flatten one conjunction without introducing a parallel predicate-tree
    /// representation in reusable plans.
    pub fn and(children: impl IntoIterator<Item = Self>) -> Option<Self> {
        combine_predicate_trees(children, true)
    }

    /// Flatten one disjunction without introducing a parallel predicate-tree
    /// representation in reusable plans.
    pub fn or(children: impl IntoIterator<Item = Self>) -> Option<Self> {
        combine_predicate_trees(children, false)
    }

    /// Whether any scalar value in the tree satisfies `predicate`.
    pub fn any_value(&self, predicate: &impl Fn(&V) -> bool) -> bool {
        match self {
            Self::Leaf(leaf) => match leaf {
                Predicate::Eq { value, .. }
                | Predicate::NotEq { value, .. }
                | Predicate::Lt { value, .. }
                | Predicate::Le { value, .. }
                | Predicate::Gt { value, .. }
                | Predicate::Ge { value, .. } => predicate(value),
                Predicate::In { values, .. } => values.iter().any(predicate),
                Predicate::Range { lower, upper, .. } => predicate(lower) || predicate(upper),
                Predicate::FixedIn { .. }
                | Predicate::IsNull { .. }
                | Predicate::IsNotNull { .. }
                | Predicate::StringPrefix { .. }
                | Predicate::StringPrefixIn { .. }
                | Predicate::StringLike { .. }
                | Predicate::ColumnComparison { .. } => false,
            },
            Self::And(children) | Self::Or(children) => {
                children.iter().any(|child| child.any_value(predicate))
            }
        }
    }

    pub fn try_map_values<U, E>(
        self,
        map: &mut impl FnMut(V) -> std::result::Result<U, E>,
    ) -> std::result::Result<PredicateTree<U>, E> {
        Ok(match self {
            Self::Leaf(predicate) => PredicateTree::Leaf(predicate.try_map_values(map)?),
            Self::And(children) => PredicateTree::And(
                children
                    .into_iter()
                    .map(|child| child.try_map_values(map))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            ),
            Self::Or(children) => PredicateTree::Or(
                children
                    .into_iter()
                    .map(|child| child.try_map_values(map))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            ),
        })
    }

    /// Transform every leaf while preserving the tree's boolean structure.
    /// The callback may change the predicate variant (for example, folding an
    /// out-of-domain comparison into `IN ()` or `IS NOT NULL`).
    pub fn try_map_predicates<U, E>(
        self,
        map: &mut impl FnMut(Predicate<V>) -> std::result::Result<Predicate<U>, E>,
    ) -> std::result::Result<PredicateTree<U>, E> {
        Ok(match self {
            Self::Leaf(predicate) => PredicateTree::Leaf(map(predicate)?),
            Self::And(children) => PredicateTree::And(
                children
                    .into_iter()
                    .map(|child| child.try_map_predicates(map))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            ),
            Self::Or(children) => PredicateTree::Or(
                children
                    .into_iter()
                    .map(|child| child.try_map_predicates(map))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            ),
        })
    }

    pub fn map_values<U>(self, map: &mut impl FnMut(V) -> U) -> PredicateTree<U> {
        self.try_map_values(&mut |value| Ok::<U, std::convert::Infallible>(map(value)))
            .unwrap_or_else(|never| match never {})
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

impl<V: fmt::Display> fmt::Display for PredicateTree<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PredicateTree::Leaf(predicate) => write!(f, "{predicate}"),
            PredicateTree::And(children) => write_predicate_children(f, children, " AND "),
            PredicateTree::Or(children) => write_predicate_children(f, children, " OR "),
        }
    }
}

fn write_predicate_children<V: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    children: &[PredicateTree<V>],
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

fn combine_predicate_trees<V>(
    children: impl IntoIterator<Item = PredicateTree<V>>,
    conjunction: bool,
) -> Option<PredicateTree<V>> {
    let mut combined = Vec::new();
    for child in children {
        match (conjunction, child) {
            (true, PredicateTree::And(mut nested)) | (false, PredicateTree::Or(mut nested)) => {
                combined.append(&mut nested)
            }
            (_, child) => combined.push(child),
        }
    }
    match combined.len() {
        0 => None,
        1 => combined.pop(),
        _ if conjunction => Some(PredicateTree::And(combined)),
        _ => Some(PredicateTree::Or(combined)),
    }
}

/// Collect unique column IDs referenced by a predicate tree (in first-seen order).
pub fn collect_predicate_columns<V>(tree: &PredicateTree<V>) -> Vec<ColumnId> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();

    fn visit<V>(
        tree: &PredicateTree<V>,
        seen: &mut HashSet<ColumnId>,
        columns: &mut Vec<ColumnId>,
    ) {
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

/// Whether the durable scalar encoding has a total ordering understood by
/// [`compare_bytes`]. Predicate-local graph topology must use exactly this
/// capability boundary so definition validation and artifact construction
/// cannot drift.
pub fn supports_ordered_bytes(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Boolean
            | LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::HugeInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
            | LogicalType::Uuid
            | LogicalType::Float
            | LogicalType::Double
            | LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    )
}
