// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::expression::Expression;
use crate::plan::LogicalPlan;
use paro_common::types::LogicalType;
use paro_external::routine::bound::BoundRoutineCallMeta;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ExternalCostEstimate {
    pub startup_cost: f64,
    pub per_row_cost: f64,
    pub bytes_cost: f64,
    pub queue_risk: f64,
}

#[derive(Debug, Clone)]
pub struct ExternalProjectExpression {
    pub output_name: String,
    pub expression: Expression,
    pub routine_meta: BoundRoutineCallMeta,
}

#[derive(Debug)]
pub struct LogicalExternalProject {
    pub project_index: usize,
    pub expressions: Vec<ExternalProjectExpression>,
    pub child: Box<LogicalPlan>,
    pub output_names: Vec<String>,
    pub returned_types: Vec<LogicalType>,
    pub cost: ExternalCostEstimate,
}

impl LogicalExternalProject {
    pub fn new(
        project_index: usize,
        child: LogicalPlan,
        expressions: Vec<ExternalProjectExpression>,
    ) -> Self {
        let mut output_names = child.output_names();
        output_names.extend(expressions.iter().map(|expr| expr.output_name.clone()));

        let mut returned_types = child.types();
        returned_types.extend(expressions.iter().map(|expr| expr.expression.return_type()));

        Self {
            project_index,
            expressions,
            child: Box::new(child),
            output_names,
            returned_types,
            cost: ExternalCostEstimate::default(),
        }
    }

    pub fn with_cost(mut self, cost: ExternalCostEstimate) -> Self {
        self.cost = cost;
        self
    }

    pub fn explain_name(&self) -> &'static str {
        "LOGICAL_EXTERNAL_PROJECT"
    }
}
