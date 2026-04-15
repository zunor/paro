// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical plan generation for `CREATE INDEX`.
//!
//! Current supported CREATE INDEX types are metadata-only, so the common shape
//! is `DUMMY_SCAN -> CREATE_INDEX`.
//!
//! The scan/projection/filter/sort helpers remain here for future runtime-build
//! index types, but ART no longer goes through that path.
//!
//! ## Known Limitations
//! - Runtime-build index plans are not implemented in this phase
//! - ALTER TABLE ADD CONSTRAINT not yet supported
//! - Custom index build plans not yet supported

use super::generator::PhysicalPlanGenerator;
use crate::operator::ddl::create_index::CreateIndex;
use crate::operator::filter::Filter;
use crate::operator::helper::order::Order;
use crate::operator::projection::Projection;
use crate::operator::scan::dummy_scan::PhysicalDummyScan;
use crate::operator::scan::rowset_scan::{PhysicalRowsetScan, RowsetScanBindData};
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::expression::{OperatorExpression, OperatorType};
use paro_planner::operator::CreateIndex as LogicalCreateIndex;
use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for CreateIndex.
    ///
    /// This method builds the complete execution plan for CREATE INDEX:
    /// 1. Validates the table and index expressions
    /// 2. Creates a table scan for the source data
    /// 3. Adds projection for indexed columns + rowid
    /// 4. Adds filter to exclude NULL values (optional)
    /// 5. Adds sort if the index type requires it
    /// 6. Creates the CreateIndex operator
    ///
    /// # Arguments
    /// * `op` - The logical CREATE INDEX operator
    ///
    /// # Returns
    /// * `Ok(PhysicalOperator)` - The physical plan tree
    /// * `Err` - If validation fails or plan cannot be created
    pub fn create_plan_create_index(
        &self,
        op: &LogicalCreateIndex,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // 1. Validate the table is a base table (DuckTable)
        // For now, we assume all tables are valid base tables

        // 2. Validate index expressions don't contain side effects
        // E.g., random(), get_current_timestamp(), sequence values cannot be index keys
        for expr in &op.unbound_expressions {
            self.validate_index_expression(expr)?;
        }

        // 3-6. Build phase split:
        // - Future runtime-build index types: full scan/projection/filter/sort pipeline
        // - Current supported index types (ART/HNSW/Sparse/FullText): metadata-only
        let child = if op.info.index_type.requires_runtime_build() {
            let scan = self.create_index_table_scan(op)?;
            let projection = self.add_index_projection(op, scan)?;

            let need_filter = true; // Always filter for CREATE INDEX
            let filtered = if need_filter {
                self.add_index_filter(op, projection)?
            } else {
                projection
            };

            let need_sort = self.index_needs_sort(&op.info.index_type);
            if need_sort {
                self.add_index_sort(op, filtered)?
            } else {
                filtered
            }
        } else {
            Arc::new(PhysicalDummyScan::new()) as Arc<dyn PhysicalOperator>
        };

        // 7. Create CreateIndex operator
        let create_index = CreateIndex::new(op.table.clone(), op.info.clone());

        // Wrap in Arc and set child
        // Note: CreateIndex is a Sink operator, it receives data from child
        // The child relationship is implicit through the pipeline builder

        Ok(Arc::new(CreateIndexWithChild::new(create_index, child)))
    }

    /// Validate that an index expression doesn't contain side effects.
    fn validate_index_expression(&self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::ColumnRef(_) | Expression::Reference(_) => Ok(()),
            Expression::Constant(_) => Ok(()),
            Expression::Function(func) => {
                // Check if function has side effects
                // For MVP, we allow all functions except known problematic ones
                let problematic_functions = [
                    "random",
                    "uuid",
                    "gen_random_uuid",
                    "now",
                    "current_timestamp",
                    "current_date",
                    "current_time",
                    "nextval",
                ];
                let func_name = func.function.name.to_lowercase();
                if problematic_functions.contains(&func_name.as_str()) {
                    return Err(paro_error::syntax(format!(
                        "Index keys cannot contain expressions with side effects: {}",
                        func.function.name
                    )));
                }
                // Recursively check arguments
                for arg in &func.children {
                    self.validate_index_expression(arg)?;
                }
                Ok(())
            }
            Expression::Operator(op_expr) => {
                // Recursively check all children
                for child in &op_expr.children {
                    self.validate_index_expression(child)?;
                }
                Ok(())
            }
            Expression::Comparison(cmp) => {
                self.validate_index_expression(&cmp.left)?;
                self.validate_index_expression(&cmp.right)?;
                Ok(())
            }
            Expression::Cast(cast) => self.validate_index_expression(&cast.child),
            Expression::Case(case) => {
                // CaseExpression has check, result_if_true, result_if_false
                self.validate_index_expression(&case.check)?;
                self.validate_index_expression(&case.result_if_true)?;
                self.validate_index_expression(&case.result_if_false)?;
                Ok(())
            }
            Expression::Conjunction(conj) => {
                for child in &conj.children {
                    self.validate_index_expression(child)?;
                }
                Ok(())
            }
            // Subqueries and aggregates are not allowed in index expressions
            Expression::Subquery(_) => Err(paro_error::syntax(
                "Index keys cannot contain subqueries".to_string(),
            )),
            Expression::Aggregate(_) => Err(paro_error::syntax(
                "Index keys cannot contain aggregate functions".to_string(),
            )),
            Expression::Window(_) => Err(paro_error::syntax(
                "Index keys cannot contain window functions".to_string(),
            )),
        }
    }

    /// Create a table scan for the index source data.
    fn create_index_table_scan(
        &self,
        op: &LogicalCreateIndex,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let table_data = op
            .table
            .get_storage()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Table '{}' has no storage. Cannot create index.",
                    op.table.base.base.name
                ))
            })?
            .clone();

        // Scan all columns + virtual rowid.
        // For MVP, we scan all columns and let projection handle key filtering.
        let mut output_types = table_data.types().to_vec();
        output_types.push(LogicalType::BigInt);
        let bind_data = RowsetScanBindData::from_table_data(table_data)
            .with_output_types(output_types)
            .with_emit_row_id(true);
        let scan = PhysicalRowsetScan::new(bind_data);

        Ok(Arc::new(scan))
    }

    /// Add projection for indexed columns + rowid.
    ///
    /// The projection outputs:
    /// - Indexed columns (from expressions)
    /// - Row ID (as the last column)
    fn add_index_projection(
        &self,
        op: &LogicalCreateIndex,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let mut projection_exprs = Vec::new();

        // Add indexed column expressions
        for (i, col_id) in op.info.column_ids.iter().enumerate() {
            let col_type = op
                .info
                .column_types
                .get(i)
                .cloned()
                .unwrap_or(LogicalType::Unknown);
            projection_exprs.push(Expression::Reference(ReferenceExpression {
                index: col_id.index as usize,
                return_type: col_type,
            }));
        }

        // Add rowid column (virtual column at the end)
        // that will be populated by the table scan with row identifiers.
        let rowid_index = child.types().len().saturating_sub(1);
        projection_exprs.push(Expression::Reference(ReferenceExpression {
            index: rowid_index,
            return_type: LogicalType::BigInt,
        }));

        let projection = Projection::new(projection_exprs, child);
        Ok(Arc::new(projection))
    }

    /// Add filter to exclude NULL values from indexed columns.
    ///
    /// Creates a filter expression: col0 IS NOT NULL AND col1 IS NOT NULL AND ...
    fn add_index_filter(
        &self,
        op: &LogicalCreateIndex,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let child_types = child.types();
        let num_key_columns = op.info.column_ids.len();

        // If no key columns, skip filter
        if num_key_columns == 0 {
            return Ok(child);
        }

        // Build IS NOT NULL expressions for each key column
        let mut not_null_exprs: Vec<Expression> = Vec::new();

        for i in 0..num_key_columns {
            let col_type = child_types.get(i).cloned().unwrap_or(LogicalType::Unknown);

            // Create: column IS NOT NULL
            let col_ref = Expression::Reference(ReferenceExpression {
                index: i,
                return_type: col_type,
            });

            // Create IS NOT NULL operator expression
            let is_not_null = Expression::Operator(OperatorExpression::new_unary(
                OperatorType::IsNotNull,
                col_ref,
                LogicalType::Boolean,
            ));
            not_null_exprs.push(is_not_null);
        }

        // Combine with AND
        let filter_expr = if not_null_exprs.len() == 1 {
            not_null_exprs.pop().unwrap()
        } else {
            Expression::Conjunction(ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: not_null_exprs,
            })
        };

        let filter = Filter::new(filter_expr, child);
        Ok(Arc::new(filter))
    }

    /// Check if the index type requires sorted input.
    fn index_needs_sort(&self, index_type: &paro_catalog::entry::IndexType) -> bool {
        use paro_catalog::entry::IndexType;
        match index_type {
            IndexType::ART => true,       // Reserved for future runtime ART builds
            IndexType::HNSW => false,     // staged metadata-only in current phase
            IndexType::Sparse => false,   // staged metadata-only in current phase
            IndexType::FullText => false, // staged metadata-only in current phase
            IndexType::BPlusTree => true, // B+Tree requires sorted input
            IndexType::Hash => false,     // Hash index doesn't need sorting
            IndexType::Custom => false,   // Custom indexes: assume no sorting needed
        }
    }

    /// Add sort operator for index construction.
    ///
    /// Sorts by all indexed columns in ascending order with NULLS FIRST.
    fn add_index_sort(
        &self,
        op: &LogicalCreateIndex,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let child_types = child.types().to_vec();
        let num_key_columns = op.info.column_ids.len();

        // Create ORDER BY for each key column
        let mut orders = Vec::new();
        for i in 0..num_key_columns {
            let col_type = child_types.get(i).cloned().unwrap_or(LogicalType::Unknown);
            orders.push(OrderByNode {
                expression: Expression::Reference(ReferenceExpression {
                    index: i,
                    return_type: col_type,
                }),
                ascending: true,
                nulls_first: true, // NULLS FIRST for consistent ordering
            });
        }

        // Create projection map (all columns)
        let projections: Vec<usize> = (0..child_types.len()).collect();

        let sort = Order::new(
            child_types,
            orders,
            projections,
            child,
            true, // is_index_sort = true for index creation
        )?;
        Ok(Arc::new(sort))
    }
}

/// Wrapper for CreateIndex that holds a child operator.
///
/// This is needed because CreateIndex is a Sink operator that
/// receives data from a child operator through the pipeline.
#[derive(Debug)]
pub struct CreateIndexWithChild {
    /// The actual CREATE INDEX operator
    inner: CreateIndex,
    /// Child operator providing data
    child: Arc<dyn PhysicalOperator>,
}

impl CreateIndexWithChild {
    /// Create a new wrapper with child.
    pub fn new(inner: CreateIndex, child: Arc<dyn PhysicalOperator>) -> Self {
        Self { inner, child }
    }
}

impl PhysicalOperator for CreateIndexWithChild {
    fn operator_type(&self) -> crate::operator_type::PhysicalOperatorType {
        self.inner.operator_type()
    }

    fn types(&self) -> &[LogicalType] {
        self.inner.types()
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

    fn is_source(&self) -> bool {
        self.inner.is_source()
    }

    fn is_sink(&self) -> bool {
        self.inner.is_sink()
    }

    fn parallel_sink(&self) -> bool {
        self.inner.parallel_sink()
    }

    fn get_global_sink_state(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
    ) -> Result<Box<dyn crate::operator::state::GlobalSinkState>> {
        self.inner.get_global_sink_state(ctx)
    }

    fn get_local_sink_state(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
    ) -> Result<Box<dyn crate::operator::state::LocalSinkState>> {
        self.inner.get_local_sink_state(ctx)
    }

    fn sink(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
        chunk: &paro_common::chunk::Chunk,
        input: &mut crate::operator::state::OperatorSinkInput,
    ) -> Result<crate::result_type::SinkResultType> {
        self.inner.sink(ctx, chunk, input)
    }

    fn combine(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
        input: &mut crate::operator::state::OperatorSinkCombineInput,
    ) -> Result<crate::result_type::SinkCombineResultType> {
        self.inner.combine(ctx, input)
    }

    fn finalize(
        &self,
        input: &crate::operator::state::OperatorSinkFinalizeInput,
    ) -> Result<crate::result_type::SinkFinalizeType> {
        self.inner.finalize(input)
    }

    fn get_global_source_state(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
        sink_state: Option<&dyn crate::operator::state::GlobalSinkState>,
    ) -> Result<Box<dyn crate::operator::state::GlobalSourceState>> {
        self.inner.get_global_source_state(ctx, sink_state)
    }

    fn get_local_source_state(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
        gstate: &dyn crate::operator::state::GlobalSourceState,
    ) -> Result<Box<dyn crate::operator::state::LocalSourceState>> {
        self.inner.get_local_source_state(ctx, gstate)
    }

    fn get_data(
        &self,
        ctx: &crate::execution_context::ExecutionContext,
        chunk: &mut paro_common::chunk::Chunk,
        input: &mut crate::operator::state::OperatorSourceInput,
    ) -> Result<crate::result_type::SourceResultType> {
        self.inner.get_data(ctx, chunk, input)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
