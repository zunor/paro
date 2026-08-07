// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

/// Lossless physical representation used for a materialized group key.
///
/// Logical expressions and operator output retain their SQL types. Only the
/// rows owned by the aggregate operator use this representation, allowing
/// hashing and equality checks to operate on compact fixed-width values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKeyEncoding {
    Identity,
    PackedString {
        physical_type: LogicalType,
        max_length: usize,
    },
    OffsetInteger {
        physical_type: LogicalType,
        minimum: i128,
    },
}

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub grouping_key_count: usize,
    /// Estimated rows entering the aggregate before local parallelism.
    /// Runtime hash tables treat this as a bounded capacity hint, never as a
    /// correctness constraint.
    pub estimated_input_rows: Option<u64>,
    pub projection_exprs: Box<[Expression]>,
    pub payload_types: Box<[LogicalType]>,
    pub groups: Box<[Expression]>,
    pub group_key_encodings: Box<[GroupKeyEncoding]>,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub aggregates: Box<[Expression]>,
    pub grouping_functions: Box<[Box<[usize]>]>,
    pub aggregate_inputs: Box<[Box<[usize]>]>,
    pub aggregate_filters: Box<[Option<usize>]>,
    pub aggregate_orders: Box<[Box<[usize]>]>,
    /// HAVING predicate restricted to finalized aggregate outputs. Reference
    /// indices are rebased so column zero is the first aggregate value.
    pub having_filter: Box<[Expression]>,
    pub perfect_hash: Option<PerfectHashAggregatePlan>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct PerfectHashAggregatePlan {
    pub group_minima: Box<[i128]>,
    pub required_bits: Box<[usize]>,
}
