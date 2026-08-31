// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_external::routine::identity::RoutineCallIdentity;
use paro_external::routine::spec::{RoutineSemantics, RoutineSpec};
use paro_planner::operator::external_project::{ExternalCostEstimate, ExternalProjectExpression};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRoutineDescriptor {
    pub label: String,
    pub identity: RoutineCallIdentity,
    pub semantics: RoutineSemantics,
    pub spec: Option<RoutineSpec>,
}

#[derive(Debug, Clone)]
pub struct ExternalProjectSpec {
    pub routines: Box<[ExternalRoutineDescriptor]>,
    pub expressions: Box<[ExternalProjectExpression]>,
    pub cost: ExternalCostEstimate,
    pub input_names: Box<[String]>,
    pub input_types: Box<[LogicalType]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ExternalTableSpec {
    pub routine: ExternalRoutineDescriptor,
    pub worker_output_types: Box<[LogicalType]>,
    pub emitted_output_types: Box<[LogicalType]>,
    pub argument_count: usize,
    pub lateral: bool,
    pub parameterized: bool,
    pub estimated_cardinality: usize,
    pub cost: ExternalCostEstimate,
}
