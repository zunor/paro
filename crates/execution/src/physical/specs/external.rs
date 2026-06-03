// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::types::LogicalType;
use paro_planner::operator::external_project::{ExternalCostEstimate, ExternalProjectExpression};

use crate::operators::external::runtime_bridge::{
    ExternalRoutineDescriptor, ExternalRuntimeBridge,
};

#[derive(Debug, Clone)]
pub struct ExternalProjectSpec {
    pub routines: Box<[ExternalRoutineDescriptor]>,
    pub expressions: Box<[ExternalProjectExpression]>,
    pub cost: ExternalCostEstimate,
    pub bridge: Arc<ExternalRuntimeBridge>,
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
    pub bridge: Arc<ExternalRuntimeBridge>,
}
