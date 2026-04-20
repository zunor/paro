// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan Filter - Convert logical Filter to physical filter/scan operators.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::Expression;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType};
use paro_planner::operator::Filter as LogicalFilter;
use paro_planner::operator::LogicalOperator;

use super::generator::PhysicalPlanGenerator;
use super::predicate_builder;
use crate::operator::filter::Filter as PhysicalFilter;
use crate::operator::scan::rowset_scan::{PhysicalRowsetScan, RowsetScanBindData};
use crate::operator::PhysicalOperator;

impl PhysicalPlanGenerator {
    /// Create physical plan for Filter.
    pub fn create_plan_filter(
        &self,
        filter: &LogicalFilter,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        if filter.expressions.is_empty() {
            return Ok(child);
        }

        if let LogicalOperator::Get(get) = &filter.child.operator {
            let (predicate_tree, mut residual) =
                predicate_builder::build_predicate_tree(&filter.expressions, get)?;
            let (runtime_tree, mut runtime_residual) =
                predicate_builder::build_predicate_tree(&get.runtime_filter_expressions, get)?;
            let predicate_tree =
                predicate_builder::combine_predicate_trees(predicate_tree, runtime_tree);
            residual.append(&mut runtime_residual);

            if let Some(tree) = predicate_tree {
                let table_entry = get
                    .get_table()
                    .ok_or_else(|| paro_error::internal("Get missing table reference"))?;
                let table_data = table_entry
                    .get_storage()
                    .ok_or_else(|| paro_error::internal("Table has no storage"))?
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
                    RowsetScanBindData::from_table_data_with_projection(
                        table_data,
                        projected_columns,
                    )
                }
                .with_output_types(get.returned_types.clone())
                .with_emit_row_id(emit_row_id)
                .with_relation(get.relation_name.clone(), get.relation_alias.clone());
                bind_data = bind_data.with_predicate(tree);

                let scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
                    Arc::new(PhysicalRowsetScan::new(bind_data)),
                    crate::explain::types::ExplainSchema {
                        output_names: get.names.clone(),
                        relation_name: get.relation_name.clone(),
                        relation_alias: get.relation_alias.clone(),
                    },
                );
                let mut current_op: Arc<dyn PhysicalOperator> = scan;

                if !residual.is_empty() {
                    let predicate = if residual.len() == 1 {
                        residual[0].clone()
                    } else {
                        Expression::Conjunction(ConjunctionExpression {
                            conjunction_type: ConjunctionType::And,
                            children: residual,
                        })
                    };
                    let filter_op: Arc<dyn PhysicalOperator> =
                        Arc::new(PhysicalFilter::new(predicate, current_op.clone()));
                    current_op = self.annotate_schema(
                        filter_op,
                        self.passthrough_schema(&current_op, filter.child.output_names()),
                    );
                }

                return Ok(current_op);
            }
        }

        let predicate = if filter.expressions.len() == 1 {
            filter.expressions[0].clone()
        } else {
            Expression::Conjunction(ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: filter.expressions.clone(),
            })
        };

        let physical_filter = if filter.projection_map.is_empty() {
            PhysicalFilter::new(predicate, child)
        } else {
            PhysicalFilter::with_projection_map(predicate, filter.projection_map.clone(), child)
        };
        Ok(Arc::new(physical_filter))
    }
}
