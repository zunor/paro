// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub grouping_key_count: usize,
    pub projection_exprs: Box<[Expression]>,
    pub payload_types: Box<[LogicalType]>,
    pub groups: Box<[Expression]>,
    pub grouping_sets: Box<[Box<[usize]>]>,
    pub aggregates: Box<[Expression]>,
    pub grouping_functions: Box<[Box<[usize]>]>,
    pub aggregate_inputs: Box<[Box<[usize]>]>,
    pub aggregate_filters: Box<[Option<usize>]>,
    pub aggregate_orders: Box<[Box<[usize]>]>,
    pub perfect_hash: Option<PerfectHashAggregatePlan>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct PerfectHashAggregatePlan {
    pub group_minima: Box<[i128]>,
    pub required_bits: Box<[usize]>,
}
