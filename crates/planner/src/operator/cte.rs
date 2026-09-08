// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical CTE operators.

use paro_common::types::LogicalType;

use super::ColumnBinding;
use crate::binder::ir::CTEMaterialize;
use crate::plan::OwnedLogicalPlan;

/// Column identity inside a lexical CTE definition/domain. Unlike an output
/// slot this identity survives pruning and reordering of a producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CteColumnId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CteOutputColumn {
    pub definition: CteColumnId,
    pub binding: ColumnBinding,
}

/// Non-recursive materialized CTE wrapper.
#[derive(Debug, Clone)]
pub struct MaterializedCTE<Child = Box<OwnedLogicalPlan>> {
    pub cte_index: usize,
    pub cte_name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    /// Explicit definition-to-producer correspondence; never reconstruct it
    /// from the compacted producer's output ordinal.
    pub output_columns: Vec<CteOutputColumn>,
    pub materialized: CTEMaterialize,
    pub ref_count: usize,
    pub cte_query: Child,
    pub child: Child,
}

impl MaterializedCTE {
    pub fn new(
        cte_index: usize,
        cte_name: String,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
        materialized: CTEMaterialize,
        mut cte_query: OwnedLogicalPlan,
        child: OwnedLogicalPlan,
    ) -> Self {
        if let crate::operator::LogicalOperator::ExpressionGet(values) = &mut cte_query.operator {
            values.names.clone_from(&column_names);
            values.relation_alias = Some(cte_name.clone());
        }
        let output_columns = cte_query
            .get_column_bindings()
            .into_iter()
            .enumerate()
            .map(|(ordinal, binding)| CteOutputColumn {
                definition: CteColumnId(ordinal),
                binding,
            })
            .collect();
        Self {
            cte_index,
            cte_name,
            column_names,
            column_types,
            output_columns,
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
#[derive(Debug, Clone)]
pub struct RecursiveCTE<Child = Box<OwnedLogicalPlan>> {
    pub cte_index: usize,
    pub cte_name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub union_all: bool,
    pub anchor: Child,
    pub recursive: Child,
}

impl RecursiveCTE {
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.column_types.clone()
    }
}

/// CTE reference (leaf node).
#[derive(Debug, Clone)]
pub struct CTERef {
    /// Lexical producer-domain symbol, not a display name. Optimizer demand
    /// rewrites must rebind this symbol when changing the producer's domain;
    /// scans of different domains cannot share expression-independent facts.
    pub cte_index: usize,
    pub table_index: usize,
    pub relation_alias: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub definition_columns: Vec<CteColumnId>,
}

impl CTERef {
    pub fn new(
        cte_index: usize,
        table_index: usize,
        relation_alias: String,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
    ) -> Self {
        let definition_columns = (0..column_types.len()).map(CteColumnId).collect();
        Self {
            cte_index,
            table_index,
            relation_alias,
            column_names,
            column_types,
            definition_columns,
        }
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.column_types.clone()
    }
}
