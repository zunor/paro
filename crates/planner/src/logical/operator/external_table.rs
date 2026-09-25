// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::expression::Expression;
use crate::logical::plan::OwnedLogicalPlan;
use paro_common::types::LogicalType;
use paro_external::routine::bound::BoundRoutineCallMeta;

use super::external_project::ExternalCostEstimate;

#[derive(Debug, Clone)]
pub struct LogicalExternalTable<Child = Box<OwnedLogicalPlan>> {
    pub table_index: usize,
    pub output_columns: Vec<String>,
    pub returned_types: Vec<LogicalType>,
    pub call_expression: Expression,
    pub call: BoundRoutineCallMeta,
    pub child: Option<Child>,
    pub lateral: bool,
    pub parameterized: bool,
    pub cost: ExternalCostEstimate,
}

impl LogicalExternalTable {
    pub fn new(
        table_index: usize,
        output_columns: Vec<String>,
        returned_types: Vec<LogicalType>,
        call_expression: Expression,
        call: BoundRoutineCallMeta,
    ) -> Self {
        Self {
            table_index,
            output_columns,
            returned_types,
            call_expression,
            call,
            child: None,
            lateral: false,
            parameterized: false,
            cost: ExternalCostEstimate::default(),
        }
    }

    pub fn with_child(
        mut self,
        child: OwnedLogicalPlan,
        lateral: bool,
        parameterized: bool,
    ) -> Self {
        self.child = Some(Box::new(child));
        self.lateral = lateral;
        self.parameterized = parameterized;
        self
    }

    pub fn with_cost(mut self, cost: ExternalCostEstimate) -> Self {
        self.cost = cost;
        self
    }

    pub fn explain_name(&self) -> &'static str {
        "LOGICAL_EXTERNAL_TABLE"
    }
}
