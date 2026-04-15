// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Update - Convert Update to PhysicalUpdate
//!
//!
//! ## Dependencies Check
//! - PhysicalUpdate: ✅
//!
//! ## Implementation Notes
//! - The scan/filter child produces original rows with a trailing row_id column
//! - PlanUpdate injects a projection to build full updated rows: `[all table columns..., row_id]`
//! - UPDATE returns a single BIGINT with the count of updated rows
//!
//! ## Input Chunk Format
//! The child operator produces chunks with:
//! - Columns 0..N-1: Full updated row values for the target table
//! - Column N (last): row_id identifying which rows to update

use super::generator::PhysicalPlanGenerator;
use crate::operator::persistent::update::PhysicalUpdate;
use crate::operator::projection::Projection;
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::{Expression, ReferenceExpression};
use paro_planner::operator::update::Update;

use std::sync::Arc;

impl PhysicalPlanGenerator {
    /// Create physical plan for Update.
    ///
    /// The child operator should produce rows with:
    /// - Full table row values (updated columns overlaid on original row)
    /// - A row_id column (last column) identifying which rows to update
    pub fn create_plan_update(&self, update: &Update) -> Result<Arc<dyn PhysicalOperator>> {
        // Create plan for child scan/filter.
        let child = self.create_plan_from_logical_plan(update.child.as_ref())?;

        let child_types = child.types();
        if child_types.is_empty() {
            return Err(paro_error::internal(
                "UPDATE child operator has no output columns".to_string(),
            ));
        }
        if update.columns.len() != update.expressions.len() {
            return Err(paro_error::internal(format!(
                "UPDATE has {} target columns but {} expressions",
                update.columns.len(),
                update.expressions.len()
            )));
        }

        let table_column_count = update.table.columns.len();
        let minimum_child_columns = table_column_count + 1; // +1 for row_id
        if child_types.len() < minimum_child_columns {
            return Err(paro_error::internal(format!(
                "UPDATE child operator has {} columns, expected at least {} (full row + row_id)",
                child_types.len(),
                minimum_child_columns
            )));
        }

        // Build a projection that overlays SET expressions on top of the original full row.
        // Output layout is always `[col_0, col_1, ..., col_N, row_id]`.
        let mut assignment_positions = vec![None; table_column_count];
        for (expr_idx, &column_idx) in update.columns.iter().enumerate() {
            if column_idx >= table_column_count {
                return Err(paro_error::internal(format!(
                    "UPDATE target column index {} out of range for {} columns",
                    column_idx, table_column_count
                )));
            }
            if assignment_positions[column_idx].replace(expr_idx).is_some() {
                return Err(paro_error::internal(format!(
                    "UPDATE target column {} specified multiple times",
                    column_idx
                )));
            }
        }

        let mut projection_exprs = Vec::with_capacity(table_column_count + 1);
        for table_col_idx in 0..table_column_count {
            if let Some(expr_idx) = assignment_positions[table_col_idx] {
                projection_exprs.push(update.expressions[expr_idx].clone());
            } else {
                projection_exprs.push(Expression::Reference(ReferenceExpression::new(
                    table_col_idx,
                    child_types[table_col_idx].clone(),
                )));
            }
        }
        let scan_row_id_index = child_types.len() - 1;
        projection_exprs.push(Expression::Reference(ReferenceExpression::new(
            scan_row_id_index,
            child_types[scan_row_id_index].clone(),
        )));

        let projected_child =
            Arc::new(Projection::new(projection_exprs, child)) as Arc<dyn PhysicalOperator>;
        let row_id_index = table_column_count;

        let physical_update = PhysicalUpdate::new(
            update.table.clone(),
            update.columns.clone(),
            row_id_index,
            projected_child,
        );

        Ok(Arc::new(physical_update))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_context::ExecutionContext;
    use crate::operator::persistent::update::PhysicalUpdate;
    use crate::operator::state::OperatorSourceInput;
    use crate::result_type::SourceResultType;
    use crate::thread_context::ThreadContext;
    use paro_catalog::entry::{ColumnDefinition, TableCatalogEntry};
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_planner::expression::{ConstantExpression, ReferenceExpression};
    use paro_planner::operator::{ExpressionGet, LogicalOperator};
    use paro_scheduler::task::InterruptState;
    use paro_storage::table::table_factory::TableFactory;

    fn create_storage(types: &[LogicalType]) -> paro_storage::table::table_handle::TableHandle {
        TableFactory::default().create_table(types).unwrap()
    }

    fn test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    #[test]
    fn update_plan_projects_full_row_and_rowid() {
        let storage = Arc::new(create_storage(&[
            LogicalType::Integer,
            LogicalType::Integer,
        ]));
        let table = Arc::new(TableCatalogEntry::new(
            "paro".to_string(),
            "public".to_string(),
            "t".to_string(),
            vec![
                ColumnDefinition::new("id".to_string(), LogicalType::Integer),
                ColumnDefinition::new("score".to_string(), LogicalType::Integer),
            ],
            storage,
            0,
        ));

        // Child row is [id=1, score=10, row_id=42].
        let child = LogicalOperator::ExpressionGet(ExpressionGet::new(
            0,
            vec![vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(10),
                    LogicalType::Integer,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::BigInt(42),
                    LogicalType::BigInt,
                )),
            ]],
            vec!["id".to_string(), "score".to_string(), "rowid".to_string()],
            vec![
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::BigInt,
            ],
        ));

        // UPDATE t SET score = id
        let update = Update::new(
            table,
            1,
            vec![1],
            vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::Integer,
            ))],
            paro_planner::plan::LogicalPlan::new(
                &paro_planner::binder::context::BindContext::new(),
                child,
            ),
        );

        let session: Arc<StatementContext> = test_session();
        let generator = PhysicalPlanGenerator::new(session.clone());
        let plan = generator
            .create_plan_update(&update)
            .expect("plan update should succeed");
        let update_op = plan
            .as_any()
            .downcast_ref::<PhysicalUpdate>()
            .expect("plan root should be PhysicalUpdate");

        // row_id should be moved to the last column after full-row projection.
        assert_eq!(update_op.row_id_index, 2);
        assert_eq!(update_op.child.types().len(), 3);

        let thread = ThreadContext::single_threaded();
        let exec_ctx = ExecutionContext::new(session, &thread, None);
        let gsource = update_op
            .child
            .get_global_source_state(&exec_ctx, None)
            .expect("global source state");
        let mut lsource = update_op
            .child
            .get_local_source_state(&exec_ctx, gsource.as_ref())
            .expect("local source state");
        let interrupt = InterruptState::new();
        let mut input = OperatorSourceInput::new(gsource.as_ref(), lsource.as_mut(), &interrupt);
        let mut chunk = paro_common::chunk::Chunk::initialize(update_op.child.types(), 8);

        let result = update_op
            .child
            .get_data(&exec_ctx, &mut chunk, &mut input)
            .expect("projection should produce chunk");
        assert_eq!(result, SourceResultType::HaveMoreOutput);
        assert_eq!(chunk.size(), 1);
        // Unchanged id column remains original value.
        assert_eq!(chunk.column(0).unwrap().get_value(0), Value::Integer(1));
        // Updated score column uses SET expression (score = id).
        assert_eq!(chunk.column(1).unwrap().get_value(0), Value::Integer(1));
        // row_id remains the trailing column.
        assert_eq!(chunk.column(2).unwrap().get_value(0), Value::BigInt(42));
    }
}
