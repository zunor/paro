// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Graph Project Operator
//!
//! Late materialization: reads actual column values from vertex/edge tables
//! using rowids produced by the graph scan/expand chain, then evaluates
//! the COLUMNS expressions to produce the final output.
//!
//! ## Column Layout from Expand Chain
//!
//! After GraphScan: `[v0_local, v0_rowid]`
//! After 1st expand: `[v0_local, v0_rowid, e0_rowid, v1_local, v1_rowid]`
//! After 2nd expand: `[v0_local, v0_rowid, e0_rowid, v1_local, v1_rowid, e1_rowid, v2_local, v2_rowid]`
//!
//! Pattern: vertex_i rowid at column `1 + 3*i`, edge_i rowid at column `2 + 3*i`

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_catalog::entry::CatalogEntryEnum;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::ColumnBinding;
use paro_storage::tablet::TabletReaderParams;

use crate::execution_context::ExecutionContext;
use crate::explain::explain_node::format_bound_expression;
use crate::expression_executor::executor::ExpressionExecutor;
use crate::operator::state::{GlobalOperatorState, OperatorState};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::OperatorResultType;

/// Describes which rowid column in the expand output corresponds to which table.
#[derive(Debug, Clone)]
pub struct RowidMapping {
    /// table_index from the planner (used to match ColumnBinding.table_index)
    pub table_index: usize,
    /// Column index in the expand output Chunk that holds the rowid.
    pub rowid_col_idx: usize,
    /// Table name for catalog lookup.
    pub table_name: String,
    /// Schema name.
    pub schema_name: String,
}

/// Physical graph project operator.
#[derive(Debug)]
pub struct PhysicalGraphProject {
    /// The COLUMNS expressions to evaluate.
    expressions: Vec<Expression>,
    /// Optional filter expressions (from vertex/edge WHERE clauses).
    /// Applied after late materialization but before COLUMNS evaluation.
    filters: Vec<Expression>,
    /// Output types (one per COLUMNS expression).
    output_types: Vec<LogicalType>,
    /// Mapping from table_index to rowid column position in the expand output.
    rowid_mappings: Vec<RowidMapping>,
    /// Child operator (graph scan/expand chain).
    child: Arc<dyn PhysicalOperator>,
}

#[derive(Debug)]
struct GraphProjectOperatorState;

impl OperatorState for GraphProjectOperatorState {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalGraphProject {
    pub fn new(
        expressions: Vec<Expression>,
        filters: Vec<Expression>,
        rowid_mappings: Vec<RowidMapping>,
        child: Arc<dyn PhysicalOperator>,
    ) -> Self {
        let output_types = expressions.iter().map(|e| e.return_type()).collect();
        Self {
            expressions,
            filters,
            output_types,
            rowid_mappings,
            child,
        }
    }
}

impl PhysicalOperator for PhysicalGraphProject {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::GraphProject
    }

    fn types(&self) -> &[LogicalType] {
        &self.output_types
    }

    fn explain_params(&self) -> Vec<String> {
        let mut params = Vec::new();
        if !self.expressions.is_empty() {
            params.push(format!(
                "Output: {}",
                self.expressions
                    .iter()
                    .map(format_bound_expression)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        params
    }

    fn estimated_cardinality(&self) -> usize {
        self.child.estimated_cardinality()
    }

    fn children_count(&self) -> usize {
        1
    }

    fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
        if index == 0 {
            Some(self.child.as_ref())
        } else {
            None
        }
    }

    fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
        if index == 0 {
            Some(self.child.clone())
        } else {
            None
        }
    }

    fn get_operator_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn OperatorState>> {
        Ok(Box::new(GraphProjectOperatorState))
    }

    fn execute(
        &self,
        ctx: &ExecutionContext,
        input: &Chunk,
        chunk: &mut Chunk,
        _gstate: &dyn GlobalOperatorState,
        _state: &mut dyn OperatorState,
    ) -> Result<OperatorResultType> {
        if input.is_empty() {
            *chunk = Chunk::init_empty(&self.output_types);
            return Ok(OperatorResultType::NeedMoreInput);
        }

        let count = input.size();
        let catalog = ctx.catalog();
        let txn = ctx.catalog_txn_view();
        let visible_version = i64::try_from(ctx.transaction_visible_version()).unwrap_or(i64::MAX);

        // Collect required columns per table from expressions and filters.
        let mut required_cols: HashMap<usize, Vec<usize>> = HashMap::new();
        for expr in self.expressions.iter().chain(self.filters.iter()) {
            collect_table_column_refs(expr, &mut required_cols);
        }
        for cols in required_cols.values_mut() {
            cols.sort();
            cols.dedup();
        }

        // Step 1: For each rowid mapping, read column values from the table.
        // table_index -> Vec<Arc<Vector>> (one per table column)
        let mut table_columns: HashMap<usize, Vec<Arc<Vector>>> = HashMap::new();

        for mapping in &self.rowid_mappings {
            // Skip if we already loaded this table (same table_index)
            if table_columns.contains_key(&mapping.table_index) {
                continue;
            }
            // Skip tables that are not referenced by expressions/filters.
            let Some(required) = required_cols.get(&mapping.table_index) else {
                continue;
            };
            if required.is_empty() {
                continue;
            }

            let rowid_col = input.column(mapping.rowid_col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing rowid column at index {} for table \"{}\"",
                    mapping.rowid_col_idx, mapping.table_name
                ))
            })?;

            // Get the table from catalog
            let table_entry = catalog.get_table(&txn, &mapping.schema_name, &mapping.table_name)?;
            let table = match table_entry.as_ref() {
                CatalogEntryEnum::Table(t) => t,
                _ => {
                    return Err(paro_error::wrong_object_type("table", &mapping.table_name));
                }
            };
            let storage = table.get_storage().ok_or_else(|| {
                paro_error::internal(format!("Table \"{}\" has no storage", mapping.table_name))
            })?;

            let num_columns = table.columns.len();

            // Collect rowids from the input chunk for bulk lookup.
            let mut rowids: Vec<u64> = Vec::with_capacity(count);
            for row_idx in 0..count {
                rowids.push(rowid_col.get_u64(row_idx).unwrap_or(0));
            }

            // Build column_ids list (only required columns) for the storage layer.
            let column_ids: Vec<u32> = required.iter().map(|&c| c as u32).collect();

            // Bulk rowid lookup: O(n log n) sort + O(n) read, vs old O(table_size) scan.
            let params = TabletReaderParams::with_version(visible_version);
            let mut reader = storage.create_reader(params)?;
            reader.prepare()?;
            let fetched = reader.get_by_rowids(&rowids, &column_ids)?;
            let mut fetched_columns: Vec<Arc<Vector>> = Vec::with_capacity(fetched.column_count());
            for col_idx in 0..fetched.column_count() {
                if let Some(col) = fetched.column(col_idx) {
                    fetched_columns.push(col.clone());
                }
            }

            // The fetched Chunk has columns in column_ids order and rows
            // in the same order as the input rowids — one row per input rowid.
            // Reconstruct a full-width column vector list, filling missing columns with NULLs.
            let mut full_cols: Vec<Option<Arc<Vector>>> = vec![None; num_columns];
            for (pos, &col_idx) in required.iter().enumerate() {
                if let Some(v) = fetched_columns.get(pos) {
                    full_cols[col_idx] = Some(v.clone());
                }
            }
            let mut arc_vectors: Vec<Arc<Vector>> = Vec::with_capacity(num_columns);
            for col_idx in 0..num_columns {
                if let Some(v) = full_cols[col_idx].take() {
                    arc_vectors.push(v);
                } else {
                    arc_vectors.push(null_vector(&table.columns[col_idx].logical_type, count));
                }
            }

            table_columns.insert(mapping.table_index, arc_vectors);
        }

        // Step 2: Build a combined Chunk for expression evaluation.
        // Sort tables by table_index for deterministic column ordering.
        let mut combined_columns: Vec<Arc<Vector>> = Vec::new();
        let mut table_col_offset: HashMap<usize, usize> = HashMap::new();

        let mut sorted_tables: Vec<usize> = table_columns.keys().copied().collect();
        sorted_tables.sort();

        for &table_idx in &sorted_tables {
            table_col_offset.insert(table_idx, combined_columns.len());
            if let Some(cols) = table_columns.get(&table_idx) {
                combined_columns.extend(cols.iter().cloned());
            }
        }

        // Add path metadata columns from the raw expand chain output.
        // Path columns use table_index == usize::MAX as a sentinel.
        // The column_index in the ColumnBinding refers to the column position
        // in the raw expand chain output (the `input` Chunk).
        // Collect path columns (table_index == usize::MAX) from expression trees.
        let mut path_cols: Vec<usize> = Vec::new();
        for expr in &self.expressions {
            collect_path_column_indices(expr, &mut path_cols);
        }
        // Dedup while preserving order
        let mut seen = HashSet::new();
        path_cols.retain(|idx| seen.insert(*idx));

        let mut path_col_map: HashMap<usize, usize> = HashMap::new();
        if !path_cols.is_empty() {
            for raw_col_idx in path_cols {
                let raw_col = input.column(raw_col_idx).ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing path column at index {} in expand output",
                        raw_col_idx
                    ))
                })?;
                let new_idx = combined_columns.len();
                combined_columns.push(raw_col.clone());
                path_col_map.insert(raw_col_idx, new_idx);
            }
        }

        let combined_chunk = if combined_columns.is_empty() {
            let mut empty = Chunk::initialize(&[], count.max(1));
            empty.set_cardinality(count);
            empty
        } else {
            let mut cc = Chunk::from_arc_vectors(combined_columns);
            cc.set_cardinality(count);
            cc
        };

        // Step 3: Remap expressions to use combined chunk column indices.
        let remapped_expressions: Vec<Expression> = self
            .expressions
            .iter()
            .map(|expr| remap_expression(expr, &table_col_offset, &path_col_map))
            .collect();

        // Step 3.5: Apply filters (vertex/edge WHERE clauses) if any.
        let filtered_chunk = if self.filters.is_empty() {
            combined_chunk
        } else {
            let remapped_filters: Vec<Expression> = self
                .filters
                .iter()
                .map(|expr| remap_expression(expr, &table_col_offset, &path_col_map))
                .collect();

            // Evaluate each filter and build a selection mask
            let mut mask = vec![true; count];
            for filter_expr in &remapped_filters {
                let filter_exprs = vec![filter_expr.clone()];
                let mut filter_executor = ExpressionExecutor::with_expressions(&filter_exprs);
                let mut filter_result = Chunk::initialize(&[LogicalType::Boolean], count);
                filter_executor.execute_all_into(&combined_chunk, ctx, &mut filter_result)?;

                if let Some(bool_col) = filter_result.column(0) {
                    for row in 0..count {
                        if mask[row] {
                            let passes = bool_col.get_bool(row).unwrap_or(false);
                            if !passes {
                                mask[row] = false;
                            }
                        }
                    }
                }
            }

            // Build filtered combined chunk
            let selected_count = mask.iter().filter(|&&b| b).count();
            if selected_count == 0 {
                *chunk = Chunk::init_empty(&self.output_types);
                return Ok(OperatorResultType::NeedMoreInput);
            }

            if selected_count == count {
                combined_chunk
            } else {
                let mut new_vectors: Vec<Arc<Vector>> =
                    Vec::with_capacity(combined_chunk.column_count());
                for col_idx in 0..combined_chunk.column_count() {
                    if let Some(src_col) = combined_chunk.column(col_idx) {
                        let logical_type = src_col.logical_type().clone();
                        let mut new_vec = Vector::with_capacity(logical_type, selected_count);
                        new_vec.set_len(selected_count);
                        let mut dst_idx = 0;
                        for (src_idx, &keep) in mask.iter().enumerate() {
                            if keep {
                                new_vec.copy_at(dst_idx, src_col, src_idx);
                                dst_idx += 1;
                            }
                        }
                        new_vectors.push(Arc::new(new_vec));
                    }
                }
                let mut filtered = Chunk::from_arc_vectors(new_vectors);
                filtered.set_cardinality(selected_count);
                filtered
            }
        };

        let filtered_count = filtered_chunk.size();

        // Step 4: Evaluate expressions.
        let mut executor = ExpressionExecutor::with_expressions(&remapped_expressions);
        let mut output_chunk = Chunk::initialize(&self.output_types, filtered_count);
        executor.execute_all_into(&filtered_chunk, ctx, &mut output_chunk)?;

        *chunk = output_chunk;
        Ok(OperatorResultType::NeedMoreInput)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Remap column references in an expression to use combined chunk column indices.
fn remap_expression(
    expr: &Expression,
    table_col_offset: &HashMap<usize, usize>,
    path_col_map: &HashMap<usize, usize>,
) -> Expression {
    expr.clone().replace_column_ref(&|col_ref| {
        let binding = col_ref.binding;
        let new_index = if binding.table_index == usize::MAX {
            path_col_map
                .get(&binding.column_index)
                .copied()
                .unwrap_or(binding.column_index)
        } else {
            let offset = table_col_offset
                .get(&binding.table_index)
                .copied()
                .unwrap_or(0);
            offset + binding.column_index
        };
        let new_binding = ColumnBinding::new(binding.table_index, new_index);
        Some(Expression::ColumnRef(ColumnRefExpression::new(
            new_binding,
            col_ref.return_type.clone(),
        )))
    })
}

/// Collect table column references from an expression tree.
fn collect_table_column_refs(expr: &Expression, out: &mut HashMap<usize, Vec<usize>>) {
    match expr {
        Expression::ColumnRef(col_ref) => {
            let binding = col_ref.binding;
            if binding.table_index != usize::MAX {
                out.entry(binding.table_index)
                    .or_default()
                    .push(binding.column_index);
            }
        }
        Expression::Function(func) => {
            for child in &func.children {
                collect_table_column_refs(child, out);
            }
        }
        Expression::Cast(cast) => {
            collect_table_column_refs(&cast.child, out);
        }
        Expression::Comparison(cmp) => {
            collect_table_column_refs(&cmp.left, out);
            collect_table_column_refs(&cmp.right, out);
        }
        Expression::Conjunction(conj) => {
            for child in &conj.children {
                collect_table_column_refs(child, out);
            }
        }
        Expression::Operator(op) => {
            for child in &op.children {
                collect_table_column_refs(child, out);
            }
        }
        Expression::Case(case) => {
            collect_table_column_refs(&case.check, out);
            collect_table_column_refs(&case.result_if_true, out);
            collect_table_column_refs(&case.result_if_false, out);
        }
        Expression::Aggregate(agg) => {
            for child in &agg.children {
                collect_table_column_refs(child, out);
            }
            if let Some(filter) = &agg.filter {
                collect_table_column_refs(filter, out);
            }
            for order in &agg.order_bys {
                collect_table_column_refs(&order.expression, out);
            }
        }
        Expression::Subquery(subq) => {
            for child in &subq.children {
                collect_table_column_refs(child, out);
            }
        }
        Expression::Window(win) => {
            for child in &win.children {
                collect_table_column_refs(child, out);
            }
            for part in &win.partitions {
                collect_table_column_refs(part, out);
            }
            for order in &win.orders {
                collect_table_column_refs(&order.expression, out);
            }
        }
        Expression::Reference(_) | Expression::Constant(_) => {}
    }
}

/// Collect path column indices (table_index == usize::MAX) from an expression tree.
fn collect_path_column_indices(expr: &Expression, out: &mut Vec<usize>) {
    match expr {
        Expression::ColumnRef(col_ref) => {
            let binding = col_ref.binding;
            if binding.table_index == usize::MAX {
                out.push(binding.column_index);
            }
        }
        Expression::Function(func) => {
            for child in &func.children {
                collect_path_column_indices(child, out);
            }
        }
        Expression::Cast(cast) => {
            collect_path_column_indices(&cast.child, out);
        }
        Expression::Comparison(cmp) => {
            collect_path_column_indices(&cmp.left, out);
            collect_path_column_indices(&cmp.right, out);
        }
        Expression::Conjunction(conj) => {
            for child in &conj.children {
                collect_path_column_indices(child, out);
            }
        }
        Expression::Operator(op) => {
            for child in &op.children {
                collect_path_column_indices(child, out);
            }
        }
        Expression::Case(case) => {
            collect_path_column_indices(&case.check, out);
            collect_path_column_indices(&case.result_if_true, out);
            collect_path_column_indices(&case.result_if_false, out);
        }
        Expression::Aggregate(agg) => {
            for child in &agg.children {
                collect_path_column_indices(child, out);
            }
            if let Some(filter) = &agg.filter {
                collect_path_column_indices(filter, out);
            }
            for order in &agg.order_bys {
                collect_path_column_indices(&order.expression, out);
            }
        }
        Expression::Subquery(subq) => {
            for child in &subq.children {
                collect_path_column_indices(child, out);
            }
        }
        Expression::Window(win) => {
            for child in &win.children {
                collect_path_column_indices(child, out);
            }
            for part in &win.partitions {
                collect_path_column_indices(part, out);
            }
            for order in &win.orders {
                collect_path_column_indices(&order.expression, out);
            }
        }
        Expression::Reference(_) | Expression::Constant(_) => {}
    }
}

fn null_vector(logical_type: &LogicalType, count: usize) -> Arc<Vector> {
    let mut v = Vector::with_capacity(logical_type.clone(), count);
    v.set_count(count);
    for j in 0..count {
        v.set_null(j, true);
    }
    Arc::new(v)
}
