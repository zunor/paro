// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical CTE operators.

use paro_common::types::LogicalType;

use crate::binder::ir::CTEMaterialize;
use crate::plan::LogicalPlan;

/// Non-recursive materialized CTE wrapper.
#[derive(Debug)]
pub struct MaterializedCTE {
    pub cte_index: usize,
    pub cte_name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub materialized: CTEMaterialize,
    pub ref_count: usize,
    pub cte_query: Box<LogicalPlan>,
    pub child: Box<LogicalPlan>,
}

impl MaterializedCTE {
    pub fn new(
        cte_index: usize,
        cte_name: String,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
        materialized: CTEMaterialize,
        cte_query: LogicalPlan,
        child: LogicalPlan,
    ) -> Self {
        Self {
            cte_index,
            cte_name,
            column_names,
            column_types,
            materialized,
            ref_count: 0,
            cte_query: Box::new(cte_query),
            child: Box::new(child),
        }
    }

    pub fn with_ref_count(mut self, ref_count: usize) -> Self {
        self.ref_count = ref_count;
        self
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.child.types()
    }
}

/// Recursive CTE producer.
#[derive(Debug)]
pub struct RecursiveCTE {
    pub cte_index: usize,
    pub cte_name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub union_all: bool,
    pub anchor: Box<LogicalPlan>,
    pub recursive: Box<LogicalPlan>,
}

impl RecursiveCTE {
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.column_types.clone()
    }
}

/// CTE reference (leaf node).
#[derive(Debug, Clone)]
pub struct CTERef {
    pub cte_index: usize,
    pub table_index: usize,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
}

impl CTERef {
    pub fn new(
        cte_index: usize,
        table_index: usize,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
    ) -> Self {
        Self {
            cte_index,
            table_index,
            column_names,
            column_types,
        }
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.column_types.clone()
    }
}
