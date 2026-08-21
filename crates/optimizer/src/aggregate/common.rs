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
            if expr.evaluation_properties().can_share_evaluation() {
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
            // `standard_visit_operator` necessarily visits this operator
            // before its own aggregate-output remap exists. Post-reduction
            // expressions are unusual in that they consume outputs owned by
            // the same Aggregate, so apply the newly established ordinal map
            // once more to that local annotation. Parent expressions will be
            // remapped naturally as traversal unwinds.
            if let Some(reduction) = aggr.post_reduction.as_mut() {
                for reducer in &mut reduction.reducers {
                    self.visit_expression(reducer);
                }
                for scalar in &mut reduction.scalar_expressions {
                    self.visit_expression(scalar);
                }
                self.visit_expression(&mut reduction.predicate);
            }
        }
    }

    fn visit_replace_column_ref(&mut self, expr: &mut ColumnRefExpression) -> Option<Expression> {
        if let Some(new_binding) = self.aggregate_map.get(&expr.binding) {
            expr.binding = *new_binding;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use paro_common::types::LogicalType;
    use paro_function::aggregate::distributive::count::get_count_function;
    use paro_function::aggregate::distributive::minmax::get_max_function;
    use paro_function::scalar::math::get_random_function;
    use paro_planner::expression::{
        AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType, Expression,
        FunctionExpression, ReferenceExpression,
    };
    use paro_planner::operator::{
        Aggregate, ColumnBinding, LogicalOperator, PostAggregateReduction,
    };
    use paro_planner::plan::LogicalPlan;

    use super::CommonAggregateOptimizer;

    fn count_of(expression: Expression) -> Expression {
        let function = get_count_function()
            .functions
            .into_iter()
            .find(|function| function.arguments == [LogicalType::Double])
            .expect("count(double) overload");
        Expression::Aggregate(AggregateExpression::new(
            function,
            vec![expression],
            LogicalType::BigInt,
        ))
    }

    fn random_call() -> Expression {
        let function = get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(
            function,
            vec![],
            LogicalType::Double,
        ))
    }

    #[test]
    fn volatile_aggregate_inputs_are_not_deduplicated() {
        let aggregate = count_of(random_call());
        let mut operator = Aggregate::new(
            1,
            2,
            3,
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![],
            vec![],
            vec![aggregate.clone(), aggregate],
            vec![],
        );

        CommonAggregateOptimizer::new().extract_common_aggregates(&mut operator);

        assert_eq!(operator.aggregates.len(), 2);
    }

    #[test]
    fn deduplication_remaps_aggregate_local_post_reduction_ordinals() {
        let aggregate_index = 2;
        let reduction_index = 4;
        let duplicate = count_of(Expression::Constant(
            paro_planner::expression::ConstantExpression::new(
                paro_common::runtime_value::Value::Double(1.0),
                LogicalType::Double,
            ),
        ));
        let (max, _) = get_max_function()
            .bind(&[LogicalType::BigInt])
            .expect("bind max(bigint)");
        let reducer = Expression::Aggregate(AggregateExpression::new(
            max,
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(aggregate_index, 1),
                LogicalType::BigInt,
            ))],
            LogicalType::BigInt,
        ));
        let predicate = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(aggregate_index, 1),
                LogicalType::BigInt,
            )),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(reduction_index, 0),
                LogicalType::BigInt,
            )),
        ));
        let aggregate = Aggregate::new(
            1,
            aggregate_index,
            3,
            LogicalPlan::synthetic(LogicalOperator::DummyScan),
            vec![Expression::Constant(
                paro_planner::expression::ConstantExpression::new(
                    paro_common::runtime_value::Value::Integer(1),
                    LogicalType::Integer,
                ),
            )],
            vec![],
            vec![duplicate.clone(), duplicate],
            vec![],
        )
        .with_post_reduction(PostAggregateReduction {
            reduction_index,
            reducers: vec![reducer],
            scalar_expressions: vec![Expression::Reference(ReferenceExpression::new(
                0,
                LogicalType::BigInt,
            ))],
            predicate,
        });
        let mut plan = LogicalPlan::synthetic(LogicalOperator::Aggregate(aggregate));

        CommonAggregateOptimizer::new().optimize(&mut plan);

        let LogicalOperator::Aggregate(aggregate) = &plan.operator else {
            panic!("expected aggregate root");
        };
        assert_eq!(aggregate.aggregates.len(), 1);
        let reduction = aggregate
            .post_reduction
            .as_ref()
            .expect("post reduction survives deduplication");
        let Expression::Aggregate(reducer) = &reduction.reducers[0] else {
            panic!("expected reducer aggregate");
        };
        assert!(matches!(
            reducer.children.as_slice(),
            [Expression::ColumnRef(column)]
                if column.binding == ColumnBinding::new(aggregate_index, 0)
        ));
        let Expression::Comparison(predicate) = &reduction.predicate else {
            panic!("expected post predicate comparison");
        };
        assert!(matches!(
            predicate.left.as_ref(),
            Expression::ColumnRef(column)
                if column.binding == ColumnBinding::new(aggregate_index, 0)
        ));
        assert!(matches!(
            predicate.right.as_ref(),
            Expression::ColumnRef(column)
                if column.binding == ColumnBinding::new(reduction_index, 0)
        ));
        aggregate
            .verify_post_reduction()
            .expect("remapped post-reduction domains remain valid");
    }
}
