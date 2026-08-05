// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Normalize predicates around inner joins.
//!
//! Cross products with equality filters and comparison joins with mixed predicates are two
//! representations of the same logical operation. This pass gives the physical planner one
//! canonical form: equality predicates live on an inner comparison join, while non-hashable
//! predicates remain as a filter over the joined rows.

use paro_common::error::Result;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{
    ComparisonExpression, ComparisonType, ConjunctionType, Expression, ExpressionIterator,
};
use paro_planner::operator::{
    ColumnBinding, ComparisonJoin, Filter, Join, JoinComparisonType, JoinCondition, JoinSide,
    JoinType, LogicalOperator,
};
use paro_planner::plan::LogicalPlan;

use crate::expression::traversal::{expression_join_side, into_associative_terms};

pub struct JoinPredicateNormalizer<'a> {
    bind_context: &'a BindContext,
}

impl<'a> JoinPredicateNormalizer<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn optimize_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = plan.try_map_children(|child| self.optimize_plan(child))?;
        Ok(self.normalize_join(self.normalize_cross_product(plan)))
    }

    fn normalize_cross_product(&self, plan: LogicalPlan) -> LogicalPlan {
        let LogicalOperator::Filter(filter) = &plan.operator else {
            return plan;
        };
        let LogicalOperator::Join(Join::Cross(cross)) = &filter.child.operator else {
            return plan;
        };
        if filter
            .expressions
            .iter()
            .any(|expression| expression.evaluation_properties().is_reorder_fence())
        {
            return plan;
        }

        let left_bindings = cross.left.get_column_bindings();
        let right_bindings = cross.right.get_column_bindings();
        let left_width = left_bindings.len();
        plan.map_operator(|operator| {
            Self::normalize_cross_filter_operator(
                operator,
                left_width,
                &left_bindings,
                &right_bindings,
            )
        })
    }

    fn normalize_cross_filter_operator(
        operator: LogicalOperator,
        left_width: usize,
        left_bindings: &[ColumnBinding],
        right_bindings: &[ColumnBinding],
    ) -> LogicalOperator {
        let LogicalOperator::Filter(mut filter) = operator else {
            return operator;
        };
        let LogicalPlan {
            id: cross_id,
            stats: cross_stats,
            operator: cross_operator,
        } = *filter.child;
        let LogicalOperator::Join(Join::Cross(cross)) = cross_operator else {
            filter.child = Box::new(LogicalPlan {
                id: cross_id,
                stats: cross_stats,
                operator: cross_operator,
            });
            return LogicalOperator::Filter(filter);
        };

        let mut conditions = Vec::new();
        let mut residuals = Vec::new();
        for expression in filter.expressions {
            for term in into_associative_terms(expression, ConjunctionType::And) {
                match cross_product_hash_condition(term, left_width, left_bindings, right_bindings)
                {
                    HashCondition::Join(condition) => conditions.push(*condition),
                    HashCondition::Residual(expression) => residuals.push(*expression),
                }
            }
        }
        if conditions.is_empty() {
            filter.expressions = residuals;
            filter.child = Box::new(LogicalPlan {
                id: cross_id,
                stats: cross_stats,
                operator: LogicalOperator::Join(Join::Cross(cross)),
            });
            return LogicalOperator::Filter(filter);
        }

        let join = ComparisonJoin::new(JoinType::Inner, *cross.left, *cross.right, conditions);
        if residuals.is_empty() {
            LogicalOperator::Join(Join::Comparison(join))
        } else {
            LogicalOperator::Filter(Filter::new(
                LogicalPlan {
                    id: cross_id,
                    // The equality join has a different cardinality from the cross product it
                    // replaces. Preserve the enclosing filter estimate, but make the new child
                    // explicitly unknown until statistics are derived for its actual predicate.
                    stats: Default::default(),
                    operator: LogicalOperator::Join(Join::Comparison(join)),
                },
                residuals,
            ))
        }
    }

    fn normalize_join(&self, plan: LogicalPlan) -> LogicalPlan {
        plan.map_operator(|operator| self.normalize_join_operator(operator))
    }

    fn normalize_join_operator(&self, operator: LogicalOperator) -> LogicalOperator {
        let LogicalOperator::Join(Join::Comparison(mut join)) = operator else {
            return operator;
        };
        if join.join_type != JoinType::Inner {
            return LogicalOperator::Join(Join::Comparison(join));
        }

        let mut hash_keys = Vec::new();
        let mut residuals = Vec::new();
        for condition in std::mem::take(&mut join.conditions) {
            if is_hash_key(condition.comparison) {
                hash_keys.push(condition);
            } else {
                residuals.push(condition);
            }
        }

        if hash_keys.is_empty() || residuals.is_empty() {
            join.conditions = hash_keys;
            join.conditions.extend(residuals);
            return LogicalOperator::Join(Join::Comparison(join));
        }

        join.conditions = hash_keys;
        let residuals = residuals
            .into_iter()
            .map(|condition| {
                Expression::Comparison(ComparisonExpression::new(
                    comparison_type(condition.comparison),
                    condition.left,
                    condition.right,
                ))
            })
            .collect();
        let join_plan = LogicalPlan {
            id: self.bind_context.next_plan_id(),
            // The original estimate includes the residual predicate. It belongs to the outer
            // filter, not to this less selective hash-join child.
            stats: Default::default(),
            operator: LogicalOperator::Join(Join::Comparison(join)),
        };
        LogicalOperator::Filter(Filter::new(join_plan, residuals))
    }
}

enum HashCondition {
    Join(Box<JoinCondition>),
    Residual(Box<Expression>),
}

fn cross_product_hash_condition(
    expression: Expression,
    left_width: usize,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
) -> HashCondition {
    let Expression::Comparison(comparison) = expression else {
        return HashCondition::Residual(Box::new(expression));
    };
    let Some(comparison_type) = hash_comparison_type(comparison.comparison_type) else {
        return HashCondition::Residual(Box::new(Expression::Comparison(comparison)));
    };
    let left_input = expression_input(&comparison.left, left_width, left_bindings, right_bindings);
    let right_input =
        expression_input(&comparison.right, left_width, left_bindings, right_bindings);
    let (left, right) = match (left_input, right_input) {
        (JoinSide::Left, JoinSide::Right) => (*comparison.left, *comparison.right),
        (JoinSide::Right, JoinSide::Left) => (*comparison.right, *comparison.left),
        _ => return HashCondition::Residual(Box::new(Expression::Comparison(comparison))),
    };
    HashCondition::Join(Box::new(JoinCondition::new(
        left,
        rebase_right_expression(right, left_width),
        comparison_type,
    )))
}

fn expression_input(
    expression: &Expression,
    left_width: usize,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
) -> JoinSide {
    expression_join_side(expression, &mut |expression| match expression {
        Expression::Reference(reference) => {
            if reference.index < left_width {
                Some(JoinSide::Left)
            } else {
                Some(JoinSide::Right)
            }
        }
        Expression::ColumnRef(column_ref) if column_ref.depth == 0 => Some(
            match (
                left_bindings.contains(&column_ref.binding),
                right_bindings.contains(&column_ref.binding),
            ) {
                (true, false) => JoinSide::Left,
                (false, true) => JoinSide::Right,
                _ => JoinSide::Both,
            },
        ),
        Expression::ColumnRef(_) => Some(JoinSide::Both),
        _ => None,
    })
}

fn rebase_right_expression(mut expression: Expression, left_width: usize) -> Expression {
    fn rebase(expression: &mut Expression, left_width: usize) {
        if let Expression::Reference(reference) = expression {
            reference.index -= left_width;
            return;
        }
        ExpressionIterator::enumerate_children_mut(expression, |child| rebase(child, left_width));
    }
    rebase(&mut expression, left_width);
    expression
}

fn hash_comparison_type(comparison: ComparisonType) -> Option<JoinComparisonType> {
    match comparison {
        ComparisonType::Equal => Some(JoinComparisonType::Equal),
        ComparisonType::NotDistinctFrom => Some(JoinComparisonType::NotDistinctFrom),
        _ => None,
    }
}

fn is_hash_key(comparison: JoinComparisonType) -> bool {
    matches!(
        comparison,
        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
    )
}

fn comparison_type(comparison: JoinComparisonType) -> ComparisonType {
    match comparison {
        JoinComparisonType::Equal => ComparisonType::Equal,
        JoinComparisonType::NotEqual => ComparisonType::NotEqual,
        JoinComparisonType::LessThan => ComparisonType::LessThan,
        JoinComparisonType::GreaterThan => ComparisonType::GreaterThan,
        JoinComparisonType::LessThanOrEqual => ComparisonType::LessThanOrEqual,
        JoinComparisonType::GreaterThanOrEqual => ComparisonType::GreaterThanOrEqual,
        JoinComparisonType::NotDistinctFrom => ComparisonType::NotDistinctFrom,
        JoinComparisonType::DistinctFrom => ComparisonType::DistinctFrom,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, ReferenceExpression};
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, CrossProduct, ExpressionGet, JoinCondition,
    };
    use paro_planner::plan::CardinalityEstimate;

    fn column(table: usize, column: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table, column),
            LogicalType::Integer,
        ))
    }

    fn reference(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer))
    }

    fn input(context: &BindContext, table: usize) -> LogicalPlan {
        LogicalPlan::new(
            context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table,
                Vec::<Vec<Expression>>::new(),
                vec!["key".to_string(), "value".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        )
    }

    fn join(context: &BindContext, join_type: JoinType) -> LogicalPlan {
        let conditions = vec![
            JoinCondition::new(column(1, 0), column(2, 0), JoinComparisonType::Equal),
            JoinCondition::new(column(1, 1), column(2, 1), JoinComparisonType::NotEqual),
        ];
        LogicalPlan::new(
            context,
            LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                join_type,
                input(context, 1),
                input(context, 2),
                conditions,
            ))),
        )
    }

    #[test]
    fn inner_join_keeps_hash_keys_and_moves_residuals_to_filter() {
        let context = BindContext::new();
        let mut plan = join(&context, JoinType::Inner);
        plan.stats.estimated_cardinality = Some(CardinalityEstimate::exact(7));
        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize inner join");

        assert_eq!(
            optimized.stats.estimated_cardinality,
            Some(CardinalityEstimate::exact(7))
        );
        let LogicalOperator::Filter(filter) = optimized.operator else {
            panic!("expected residual filter");
        };
        assert_eq!(filter.expressions.len(), 1);
        assert_eq!(filter.child.stats.estimated_cardinality, None);
        let LogicalOperator::Join(Join::Comparison(join)) = filter.child.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 1);
        assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);
    }

    #[test]
    fn preserving_join_does_not_move_residuals() {
        let context = BindContext::new();
        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(join(&context, JoinType::Left))
            .expect("normalize preserving join");

        let LogicalOperator::Join(Join::Comparison(join)) = optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 2);
    }

    #[test]
    fn equality_filter_over_cross_product_becomes_hash_join() {
        let context = BindContext::new();
        let cross = LogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Cross(CrossProduct {
                left: Box::new(input(&context, 1)),
                right: Box::new(input(&context, 2)),
            })),
        );
        let equality = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            reference(1),
            reference(2),
        ));
        let plan = LogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(cross, vec![equality])),
        );

        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize cross product");

        let LogicalOperator::Join(Join::Comparison(join)) = optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 1);
        assert!(join.conditions[0].left.equals(&reference(1)));
        assert!(join.conditions[0].right.equals(&reference(0)));
        assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);
    }

    #[test]
    fn binding_equality_filter_over_cross_product_becomes_hash_join() {
        let context = BindContext::new();
        let cross = LogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Cross(CrossProduct {
                left: Box::new(input(&context, 1)),
                right: Box::new(input(&context, 2)),
            })),
        );
        let equality = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            column(1, 1),
            column(2, 0),
        ));
        let plan = LogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(cross, vec![equality])),
        );

        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize binding cross product");

        let LogicalOperator::Join(Join::Comparison(join)) = optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 1);
        assert!(join.conditions[0].left.equals(&column(1, 1)));
        assert!(join.conditions[0].right.equals(&column(2, 0)));
    }

    #[test]
    fn cross_product_residual_stays_above_normalized_hash_join() {
        let context = BindContext::new();
        let cross = LogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Cross(CrossProduct {
                left: Box::new(input(&context, 1)),
                right: Box::new(input(&context, 2)),
            })),
        );
        let equality = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            reference(0),
            reference(2),
        ));
        let residual = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            reference(1),
            reference(3),
        ));
        let plan = LogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(cross, vec![equality, residual.clone()])),
        );

        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize cross product residual");

        let LogicalOperator::Filter(filter) = optimized.operator else {
            panic!("expected residual filter");
        };
        assert_eq!(filter.expressions.len(), 1);
        assert!(filter.expressions[0].equals(&residual));
        assert!(matches!(
            filter.child.operator,
            LogicalOperator::Join(Join::Comparison(_))
        ));
    }
}
