// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared physical dispatch and SQL comparison semantics for hash-join kernels.

use paro_common::types::PhysicalType;
use paro_planner::expression::ComparisonType;
use paro_planner::operator::join::JoinComparisonType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FixedKind {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
}

impl FixedKind {
    pub(super) fn from_physical_type(physical_type: PhysicalType) -> Option<Self> {
        match physical_type {
            PhysicalType::Int8 => Some(Self::I8),
            PhysicalType::Int16 => Some(Self::I16),
            PhysicalType::Int32 => Some(Self::I32),
            PhysicalType::Int64 => Some(Self::I64),
            PhysicalType::Int128 => Some(Self::I128),
            PhysicalType::UInt8 => Some(Self::U8),
            PhysicalType::UInt16 => Some(Self::U16),
            PhysicalType::UInt32 => Some(Self::U32),
            PhysicalType::UInt64 => Some(Self::U64),
            PhysicalType::UInt128 => Some(Self::U128),
            PhysicalType::Float => Some(Self::F32),
            PhysicalType::Double => Some(Self::F64),
            PhysicalType::Bool
            | PhysicalType::Varchar
            | PhysicalType::Bit
            | PhysicalType::List
            | PhysicalType::Struct
            | PhysicalType::Array => None,
        }
    }

    pub(super) fn supports(self, comparison: FixedComparison) -> bool {
        // Ordinary floating-point comparisons use Rust's partial ordering in
        // the canonical vector executor. DISTINCT FROM instead compares the
        // runtime Value representation (including NaN payloads and signed
        // zero), so that pair must stay on the generic path.
        !matches!(self, Self::F32 | Self::F64)
            || !matches!(
                comparison,
                FixedComparison::DistinctFrom | FixedComparison::NotDistinctFrom
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FixedComparison {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    DistinctFrom,
    NotDistinctFrom,
}

impl From<ComparisonType> for FixedComparison {
    fn from(comparison: ComparisonType) -> Self {
        match comparison {
            ComparisonType::Equal => Self::Equal,
            ComparisonType::NotEqual => Self::NotEqual,
            ComparisonType::LessThan => Self::LessThan,
            ComparisonType::LessThanOrEqual => Self::LessThanOrEqual,
            ComparisonType::GreaterThan => Self::GreaterThan,
            ComparisonType::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            ComparisonType::DistinctFrom => Self::DistinctFrom,
            ComparisonType::NotDistinctFrom => Self::NotDistinctFrom,
        }
    }
}

impl From<JoinComparisonType> for FixedComparison {
    fn from(comparison: JoinComparisonType) -> Self {
        match comparison {
            JoinComparisonType::Equal => Self::Equal,
            JoinComparisonType::NotEqual => Self::NotEqual,
            JoinComparisonType::LessThan => Self::LessThan,
            JoinComparisonType::LessThanOrEqual => Self::LessThanOrEqual,
            JoinComparisonType::GreaterThan => Self::GreaterThan,
            JoinComparisonType::GreaterThanOrEqual => Self::GreaterThanOrEqual,
            JoinComparisonType::DistinctFrom => Self::DistinctFrom,
            JoinComparisonType::NotDistinctFrom => Self::NotDistinctFrom,
        }
    }
}

pub(super) fn expression_comparison(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
    }
}

#[inline]
pub(super) fn fixed_comparison_matches<T>(
    left: Option<T>,
    right: Option<T>,
    comparison: FixedComparison,
) -> bool
where
    T: PartialEq + PartialOrd,
{
    match comparison {
        FixedComparison::DistinctFrom => left != right,
        FixedComparison::NotDistinctFrom => left == right,
        FixedComparison::Equal => {
            matches!((left, right), (Some(left), Some(right)) if left == right)
        }
        FixedComparison::NotEqual => {
            matches!((left, right), (Some(left), Some(right)) if left != right)
        }
        FixedComparison::LessThan => {
            matches!((left, right), (Some(left), Some(right)) if left < right)
        }
        FixedComparison::LessThanOrEqual => {
            matches!((left, right), (Some(left), Some(right)) if left <= right)
        }
        FixedComparison::GreaterThan => {
            matches!((left, right), (Some(left), Some(right)) if left > right)
        }
        FixedComparison::GreaterThanOrEqual => {
            matches!((left, right), (Some(left), Some(right)) if left >= right)
        }
    }
}
