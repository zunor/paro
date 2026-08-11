// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{AntiJoinMode, JoinCondition, JoinType};

#[derive(Debug, Clone)]
pub struct HashJoinSpec {
    pub join_type: JoinType,
    pub anti_join_mode: AntiJoinMode,
    /// Equality predicates used to locate a candidate hash chain.
    pub key_conditions: Box<[JoinCondition]>,
    /// Remaining predicates evaluated vectorially against candidate rows.
    pub residual_conditions: Box<[JoinCondition]>,
    pub left_projection: Box<[usize]>,
    /// Columns copied from the build input into the hash-table payload.
    /// Once materialized, the payload is already dense and is never projected
    /// by source-column indexes again.
    pub build_input_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    /// Visible build-side payload prefix in `build_payload_types`.
    pub build_output_count: usize,
    /// Visible build output followed by hidden residual-expression values.
    pub build_payload_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub force_external: bool,
    /// Multiple existential reductions over one preserved build relation and
    /// one equivalent filtering scan. Each step owns one match bit; the emit
    /// phase applies the required/forbidden masks after the shared probe.
    pub reduction_cascade: Option<HashReductionCascadeSpec>,
}

#[derive(Debug, Clone)]
pub struct HashReductionCascadeSpec {
    pub predicates: Box<[HashReductionPredicateSpec]>,
    pub source_predicates: Box<[HashReductionSourcePredicateSpec]>,
    pub steps: Box<[HashReductionStepSpec]>,
    pub required_mask: u8,
    pub forbidden_mask: u8,
    /// Optional grouped summary for repeated `source_value <> build_value`
    /// reductions. Exact integer equality indexes can update one extrema state
    /// per key instead of walking every duplicate build row per source row.
    pub grouped_extrema: Option<HashReductionGroupedExtremaSpec>,
}

#[derive(Debug, Clone)]
pub struct HashReductionGroupedExtremaSpec {
    /// Column index in the merged filtering scan containing the summarized
    /// `BIGINT` value.
    pub source_value_index: usize,
    pub build_residual_offset: usize,
    pub channels: Box<[HashReductionExtremaChannelSpec]>,
}

#[derive(Debug, Clone, Copy)]
pub struct HashReductionExtremaChannelSpec {
    /// Conjunction of source-local predicate bits guarding this summary.
    pub source_predicate_mask: u8,
    /// Reduction match bits established when the channel contains a value
    /// unequal to the current preserved-row value.
    pub match_mask: u8,
}

#[derive(Debug, Clone)]
pub struct HashReductionStepSpec {
    /// All predicate bits that must accept a candidate for this step to match.
    pub predicate_mask: u8,
    pub match_mask: u8,
}

#[derive(Debug, Clone)]
pub struct HashReductionPredicateSpec {
    pub condition: JoinCondition,
    /// Offset of this predicate's RHS value in the hidden build payload suffix.
    pub build_residual_offset: usize,
    pub predicate_mask: u8,
}

#[derive(Debug, Clone)]
pub struct HashReductionSourcePredicateSpec {
    pub expression: Expression,
    pub predicate_mask: u8,
}

#[derive(Debug, Clone)]
pub struct NestedLoopJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub arbitrary_condition: Option<Expression>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct SortRangeJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ClassicIeJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub mark_null_condition_start: Option<usize>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct CrossProductSpec {
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct DelimJoinSpec {
    pub side: DelimJoinSideSpec,
    pub duplicate_keys: Box<[Expression]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimJoinSideSpec {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct DelimScanSpec {
    pub target: DelimScanTarget,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimScanTarget {
    Values { table_index: usize },
    CachedOuter,
}
