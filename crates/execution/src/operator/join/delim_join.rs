// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared delim-join contract.

use std::collections::HashSet;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::Expression;

use crate::execution_context::ExecutionContext;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::scan::column_data_scan::ColumnDataScanBinding;
use crate::operator::PhysicalOperator;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DelimKey(pub Vec<Value>);

/// Shared metadata owned by left/right delim join implementations.
#[derive(Debug)]
pub struct DelimJoin {
    /// Outer input that must be materialized before the wrapped join runs.
    pub input: Arc<dyn PhysicalOperator>,
    /// The original join plan with placeholder column-data scans already injected.
    pub join: Arc<dyn PhysicalOperator>,
    /// Expressions to evaluate on the duplicate-eliminated side.
    pub duplicate_eliminated_columns: Vec<Expression>,
    /// Placeholder scan binding for the fully materialized input side.
    pub cached_input_scan: Arc<ColumnDataScanBinding>,
    /// Placeholder scan bindings for RHS delim scans.
    pub delim_scans: Vec<Arc<ColumnDataScanBinding>>,
    /// Final output types.
    pub types: Vec<LogicalType>,
}

impl DelimJoin {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        join: Arc<dyn PhysicalOperator>,
        duplicate_eliminated_columns: Vec<Expression>,
        cached_input_scan: Arc<ColumnDataScanBinding>,
        delim_scans: Vec<Arc<ColumnDataScanBinding>>,
        types: Vec<LogicalType>,
    ) -> Self {
        Self {
            input,
            join,
            duplicate_eliminated_columns,
            cached_input_scan,
            delim_scans,
            types,
        }
    }

    pub fn delim_types(&self) -> Vec<LogicalType> {
        self.duplicate_eliminated_columns
            .iter()
            .map(|expr| expr.return_type())
            .collect()
    }

    pub fn explain_params(&self) -> Vec<String> {
        vec![
            format!("Delim Keys: {}", self.duplicate_eliminated_columns.len()),
            format!("Delim Scans: {}", self.delim_scans.len()),
        ]
    }

    pub fn duplicate_eliminated_executors(&self) -> Vec<ExpressionExecutor> {
        self.duplicate_eliminated_columns
            .iter()
            .map(ExpressionExecutor::new)
            .collect()
    }

    pub fn evaluate_delim_chunk(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        executors: &mut [ExpressionExecutor],
    ) -> Result<Chunk> {
        let mut vectors = Vec::with_capacity(self.duplicate_eliminated_columns.len());
        for executor in executors {
            vectors.push(executor.execute_expression(0, input, None, input.size(), ctx)?);
        }
        Ok(Chunk::from_arc_vectors(vectors))
    }

    pub(crate) fn select_new_delim_rows(
        &self,
        delim_chunk: &Chunk,
        seen: &mut HashSet<DelimKey>,
    ) -> Chunk {
        let types = delim_chunk.types();
        let mut output = Chunk::initialize_with_allocator(
            &types,
            delim_chunk.size(),
            delim_chunk.allocator().clone(),
        );

        let mut count = 0;
        for row_idx in 0..delim_chunk.size() {
            let key = DelimKey(
                (0..delim_chunk.column_count())
                    .map(|col_idx| {
                        delim_chunk
                            .column(col_idx)
                            .expect("delim column must exist")
                            .get_value(row_idx)
                    })
                    .collect(),
            );
            if !seen.insert(key) {
                continue;
            }
            for col_idx in 0..delim_chunk.column_count() {
                let source = delim_chunk
                    .column(col_idx)
                    .expect("delim column must exist")
                    .get_value(row_idx);
                output
                    .column_mut(col_idx)
                    .expect("output column must exist")
                    .set_value(count, &source);
            }
            count += 1;
        }

        output.set_cardinality(count);
        output
    }
}
