//! Lower logical `Get` operators to physical rowset scans.

use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::Expression;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
use paro_planner::operator::get::Get;
use std::sync::Arc;

use crate::operator::filter::Filter;
use crate::operator::scan::rowset_scan::{PhysicalRowsetScan, RowsetScanBindData};
use crate::operator::PhysicalOperator;

use super::generator::PhysicalPlanGenerator;
use super::predicate_builder;

impl PhysicalPlanGenerator {
    /// Create physical plan for Get (table scan).
    ///
    /// This method converts a logical get operation into a physical table scan.
    /// It uses the TableCatalogEntry reference stored in Get to access
    /// the table's storage (TableHandle).
    ///
    /// # Arguments
    /// * `get` - The logical get operator containing table reference and column info
    ///
    /// # Returns
    /// * `Ok(PhysicalRowsetScan)` - The physical rowset scan operator
    /// * `Err` - If the table reference is missing or storage is unavailable
    pub fn create_plan_get(&self, get: &Get) -> Result<Arc<dyn PhysicalOperator>> {
        let table_entry = get.get_table().ok_or_else(|| {
            paro_error::internal(
                "Get missing table reference. This is required for physical plan generation."
                    .to_string(),
            )
        })?;

        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| {
                paro_error::internal(format!(
                    "Table '{}' has no storage. Cannot create table scan.",
                    table_entry.base.base.name
                ))
            })?
            .clone();

        let physical_cols = table_data.types().len();
        let mut emit_row_id = false;
        let projected_columns = if get.column_ids.is_empty() {
            Vec::new()
        } else {
            let mut cols = Vec::new();
            for &col_id in &get.column_ids {
                if col_id < physical_cols {
                    cols.push(col_id);
                } else if col_id == physical_cols {
                    emit_row_id = true;
                } else {
                    return Err(paro_error::invalid_input(format!(
                        "Get column_id {} out of range (physical columns: {})",
                        col_id, physical_cols
                    )));
                }
            }
            cols
        };

        let mut bind_data = if get.column_ids.is_empty() {
            RowsetScanBindData::from_table_data(table_data)
        } else {
            RowsetScanBindData::from_table_data_with_projection(table_data, projected_columns)
        }
        .with_output_types(get.returned_types.clone())
        .with_emit_row_id(emit_row_id)
        .with_relation(get.relation_name.clone(), get.relation_alias.clone());

        let (runtime_tree, runtime_residual) =
            predicate_builder::build_predicate_tree(&get.runtime_filter_expressions, get)?;
        if let Some(tree) = runtime_tree {
            bind_data = bind_data.with_predicate(tree);
        }

        let scan: Arc<dyn PhysicalOperator> = Arc::new(PhysicalRowsetScan::new(bind_data));
        let mut current_op: Arc<dyn PhysicalOperator> = self.annotate_schema(
            scan,
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        if !runtime_residual.is_empty() {
            let predicate = if runtime_residual.len() == 1 {
                runtime_residual[0].clone()
            } else {
                Expression::Conjunction(ConjunctionExpression {
                    conjunction_type: ConjunctionType::And,
                    children: runtime_residual,
                })
            };
            let filter = Arc::new(Filter::new(predicate, current_op.clone()));
            current_op = self.annotate_schema(
                filter,
                self.passthrough_schema(&current_op, get.names.clone()),
            );
        }
        Ok(current_op)
    }
}
