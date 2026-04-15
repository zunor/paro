// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deduplicate aggregates inside `Aggregate` and remap
//! `ColumnBinding` references in parent operators.

use std::collections::HashMap;

use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{Aggregate, ColumnBinding, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_planner::visitor::LogicalOperatorVisitor;

pub struct CommonAggregateOptimizer {
    aggregate_map: HashMap<ColumnBinding, ColumnBinding>,
}

impl CommonAggregateOptimizer {
    pub fn new() -> Self {
        Self {
            aggregate_map: HashMap::new(),
        }
    }

    pub fn optimize(&mut self, plan: &mut LogicalPlan) {
        self.visit_logical_plan(plan);
    }

    fn standard_visit_operator(&mut self, op: &mut LogicalOperator) {
        self.visit_operator_children(op);
        if !self.aggregate_map.is_empty() {
            self.visit_operator_expressions(op);
        }
    }

    fn extract_common_aggregates(&mut self, aggr: &mut Aggregate) {
        if aggr.aggregates.len() < 2 {
            return;
        }

        let mut deduped: Vec<Expression> = Vec::with_capacity(aggr.aggregates.len());
        let mut changed = false;

        for (old_idx, expr) in std::mem::take(&mut aggr.aggregates).into_iter().enumerate() {
            if let Some((new_idx, _)) = deduped
                .iter()
                .enumerate()
                .find(|(_, existing)| existing.equals(&expr))
            {
                changed = true;
                self.aggregate_map.insert(
                    ColumnBinding::new(aggr.aggregate_index, old_idx),
                    ColumnBinding::new(aggr.aggregate_index, new_idx),
                );
                continue;
            }

            let new_idx = deduped.len();
            if new_idx != old_idx {
                changed = true;
                self.aggregate_map.insert(
                    ColumnBinding::new(aggr.aggregate_index, old_idx),
                    ColumnBinding::new(aggr.aggregate_index, new_idx),
                );
            }
            deduped.push(expr);
        }

        if changed {
            aggr.aggregates = deduped;
            aggr.recompute_returned_types();
        } else {
            aggr.aggregates = deduped;
        }
    }
}

impl Default for CommonAggregateOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalOperatorVisitor for CommonAggregateOptimizer {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        match op {
            LogicalOperator::Projection(_)
            | LogicalOperator::SetOperation(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_) => {
                // Projection/set-op/CTE boundaries create fresh bindings, so keep
                // remap local to this subtree.
                let mut child_optimizer = CommonAggregateOptimizer::new();
                child_optimizer.standard_visit_operator(op);
                return;
            }
            _ => {}
        }

        self.standard_visit_operator(op);
        if let LogicalOperator::Aggregate(aggr) = op {
            self.extract_common_aggregates(aggr);
        }
    }

    fn visit_replace_column_ref(&mut self, expr: &mut ColumnRefExpression) -> Option<Expression> {
        if let Some(new_binding) = self.aggregate_map.get(&expr.binding) {
            expr.binding = *new_binding;
        }
        None
    }
}
