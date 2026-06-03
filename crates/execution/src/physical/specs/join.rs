// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::expression::Expression;
use paro_planner::operator::join::{JoinCondition, JoinType};

#[derive(Debug, Clone)]
pub struct HashJoinSpec {
    pub join_type: JoinType,
    pub conditions: Box<[JoinCondition]>,
    pub left_projection: Box<[usize]>,
    pub right_projection: Box<[usize]>,
    pub left_output_types: Box<[LogicalType]>,
    pub right_output_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub force_external: bool,
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
