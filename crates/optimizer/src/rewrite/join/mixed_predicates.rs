// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Normalize predicates around inner joins.
//!
//! Cross products with equality filters and comparison joins with mixed predicates are two
//! representations of the same logical operation. This pass gives the physical planner one
//! canonical form: orientable comparisons live on the inner comparison join.
//! Hash extraction partitions equality keys from residual comparisons; moving
//! residuals back above that join would materialize rejected matches.

use paro_common::error::Result;
use paro_planner::binder::context::BindContext;
use paro_planner::expression::{ComparisonType, ConjunctionType, Expression, ExpressionIterator};
use paro_planner::logical::operator::{
    ColumnBinding, ComparisonJoin, Join, JoinComparisonType, JoinCondition, JoinSide, JoinType,
    LogicalOperator,
};
use paro_planner::logical::plan::OwnedLogicalPlan;

use crate::rewrite::expr::traversal::{expression_join_side, into_associative_terms};

pub struct JoinPredicateNormalizer<'a> {
    bind_context: &'a BindContext,
}

impl<'a> JoinPredicateNormalizer<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn optimize_plan(&self, plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
        plan.try_map_post_order(|plan| Ok(self.normalize_filter(plan)))
    }

    fn normalize_filter(&self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        let LogicalOperator::Filter(filter) = &plan.operator else {
            return plan;
        };
        let (left, right) = match &filter.child.operator {
            LogicalOperator::Join(Join::Cross(cross)) => (&cross.left, &cross.right),
            LogicalOperator::Join(Join::Comparison(join))
                if join.join_type == JoinType::Inner
                    && join.mark_index.is_none()
                    && join.duplicate_eliminated_columns.is_empty()
                    && !join.delim_flipped
                    // Explicit maps belong to the preceding output namespace
                    // until column-demand settlement rebinds them. Only All
                    // certifies that Filter positions are child positions here.
                    && join.left_projection_map.is_all()
                    && join.right_projection_map.is_all() =>
            {
                (&join.left, &join.right)
            }
            _ => return plan,
        };
        if filter
            .expressions
            .iter()
            .any(|expression| !movable(expression))
            || crate::rewrite::expr::join_region_has_evaluation_fence(&filter.child.operator)
        {
            return plan;
        }

        let left_bindings = left.get_column_bindings();
        let right_bindings = right.get_column_bindings();
        let left_width = left_bindings.len();
        plan.map_operator(|operator| {
            self.normalize_filter_operator(operator, left_width, &left_bindings, &right_bindings)
        })
    }

    fn normalize_filter_operator(
        &self,
        operator: LogicalOperator,
        left_width: usize,
        left_bindings: &[ColumnBinding],
        right_bindings: &[ColumnBinding],
    ) -> LogicalOperator {
        let LogicalOperator::Filter(mut filter) = operator else {
            return operator;
        };
        let mut conditions = Vec::new();
        let mut residuals = Vec::new();
        for expression in filter.expressions {
            for term in into_associative_terms(expression, ConjunctionType::And) {
                match oriented_condition(term, left_width, left_bindings, right_bindings) {
                    OrientedCondition::Join(condition) => conditions.push(*condition),
                    OrientedCondition::Residual(expression) => residuals.push(*expression),
                }
            }
        }
        if conditions.is_empty() {
            filter.expressions = residuals;
            return LogicalOperator::Filter(filter);
        }

        let child_width = filter.child.types().len();
        let join = match (*filter.child).into_parts().2 {
            LogicalOperator::Join(Join::Cross(cross)) => {
                let mut join =
                    ComparisonJoin::new(JoinType::Inner, *cross.left, *cross.right, conditions);
                join.build_side_constraint = cross.build_side_constraint;
                join
            }
            LogicalOperator::Join(Join::Comparison(mut join)) => {
                join.conditions.extend(conditions);
                join
            }
            _ => unreachable!("normalize_filter established an inner join"),
        };
        if residuals.is_empty() && filter.projection_map.is_identity(child_width) {
            LogicalOperator::Join(Join::Comparison(join))
        } else {
            filter.child = Box::new(OwnedLogicalPlan {
                id: self.bind_context.next_plan_id(),
                stats: Default::default(),
                operator: LogicalOperator::Join(Join::Comparison(join)),
            });
            filter.expressions = residuals;
            LogicalOperator::Filter(filter)
        }
    }
}

enum OrientedCondition {
    Join(Box<JoinCondition>),
    Residual(Box<Expression>),
}

fn oriented_condition(
    expression: Expression,
    left_width: usize,
    left_bindings: &[ColumnBinding],
    right_bindings: &[ColumnBinding],
) -> OrientedCondition {
    let Expression::Comparison(comparison) = expression else {
        return OrientedCondition::Residual(Box::new(expression));
    };
    let mut comparison_type = join_comparison_type(comparison.comparison_type);
    let left_input = expression_input(&comparison.left, left_width, left_bindings, right_bindings);
    let right_input =
        expression_input(&comparison.right, left_width, left_bindings, right_bindings);
    let (left, right) = match (left_input, right_input) {
        (JoinSide::Left, JoinSide::Right) => {
            let comparison = comparison.into_inner();
            (*comparison.left, *comparison.right)
        }
        (JoinSide::Right, JoinSide::Left) => {
            comparison_type = comparison_type.flip();
            let comparison = comparison.into_inner();
            (*comparison.right, *comparison.left)
        }
        _ => return OrientedCondition::Residual(Box::new(Expression::Comparison(comparison))),
    };
    OrientedCondition::Join(Box::new(JoinCondition::new(
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

pub(crate) fn movable(expression: &Expression) -> bool {
    let properties = expression.evaluation_properties();
    properties.can_share_evaluation() && !properties.is_reorder_fence()
}

pub(crate) fn join_comparison_type(comparison: ComparisonType) -> JoinComparisonType {
    match comparison {
        ComparisonType::Equal => JoinComparisonType::Equal,
        ComparisonType::NotEqual => JoinComparisonType::NotEqual,
        ComparisonType::LessThan => JoinComparisonType::LessThan,
        ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
        ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
        ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
        ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
        ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ReferenceExpression,
    };
    use paro_planner::logical::operator::{
        ColumnBinding, ComparisonJoin, CrossProduct, ExpressionGet, Filter, JoinCondition,
    };
    use paro_planner::logical::plan::CardinalityEstimate;

    fn column(table: usize, column: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(ColumnBinding::new(table, column), LogicalType::Integer)
                .into(),
        )
    }

    fn reference(index: usize) -> Expression {
        Expression::Reference(ReferenceExpression::new(index, LogicalType::Integer).into())
    }

    fn input(context: &BindContext, table: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            context,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table,
                Vec::<Vec<Expression>>::new(),
                vec!["key".to_string(), "value".to_string()],
                vec![LogicalType::Integer, LogicalType::Integer],
            )),
        )
    }

    fn join(context: &BindContext, join_type: JoinType) -> OwnedLogicalPlan {
        let conditions = vec![
            JoinCondition::new(column(1, 0), column(2, 0), JoinComparisonType::Equal),
            JoinCondition::new(column(1, 1), column(2, 1), JoinComparisonType::NotEqual),
        ];
        OwnedLogicalPlan::new(
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
    fn inner_join_keeps_hash_keys_and_residuals_in_the_join() {
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
        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 2);
        assert_eq!(join.conditions[0].comparison, JoinComparisonType::Equal);
    }

    #[test]
    fn preserving_join_does_not_move_residuals() {
        let context = BindContext::new();
        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(join(&context, JoinType::Left))
            .expect("normalize preserving join");

        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 2);
    }

    #[test]
    fn outer_where_is_not_on_and_projecting_filter_keeps_its_layout() {
        let context = BindContext::new();
        for kind in [
            JoinType::Left,
            JoinType::Outer,
            JoinType::Semi,
            JoinType::Anti,
        ] {
            let plan = OwnedLogicalPlan::new(
                &context,
                LogicalOperator::Filter(Filter::new(
                    join(&context, kind),
                    vec![Expression::Comparison(
                        ComparisonExpression::new(
                            ComparisonType::Equal,
                            column(1, 0),
                            column(2, 0),
                        )
                        .into(),
                    )],
                )),
            );
            let actual = JoinPredicateNormalizer::new(&context)
                .optimize_plan(plan)
                .unwrap();
            assert!(
                matches!(actual.operator, LogicalOperator::Filter(_)),
                "{kind:?}"
            );
        }
        let mut filter = Filter::new(
            join(&context, JoinType::Inner),
            vec![Expression::Comparison(
                ComparisonExpression::new(ComparisonType::LessThan, column(2, 1), column(1, 1))
                    .into(),
            )],
        );
        filter.projection_map = paro_planner::logical::operator::ProjectionMap::new(vec![3, 0]);
        let plan = OwnedLogicalPlan::new(&context, LogicalOperator::Filter(filter));
        let expected = plan.output_layout();
        let actual = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .unwrap();
        assert_eq!(actual.output_layout(), expected);
        let LogicalOperator::Filter(filter) = actual.into_parts().2 else {
            panic!("projection is not disposable")
        };
        assert!(filter.expressions.is_empty());
        let LogicalOperator::Join(Join::Comparison(join)) = &filter.child.operator else {
            panic!()
        };
        assert_eq!(join.conditions.len(), 3);
        assert_eq!(
            join.conditions[2].comparison,
            JoinComparisonType::GreaterThan
        );
        assert!(join.conditions[2].left.equals(&column(1, 1)));
        assert!(join.conditions[2].right.equals(&column(2, 1)));
    }

    #[test]
    fn equality_filter_over_cross_product_becomes_hash_join() {
        let context = BindContext::new();
        let cross = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Cross(CrossProduct {
                left: Box::new(input(&context, 1)),
                right: Box::new(input(&context, 2)),
                build_side_constraint: Default::default(),
            })),
        );
        let equality = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, reference(1), reference(2)).into(),
        );
        let plan = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(cross, vec![equality])),
        );

        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize cross product");

        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
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
        let cross = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Cross(CrossProduct {
                left: Box::new(input(&context, 1)),
                right: Box::new(input(&context, 2)),
                build_side_constraint: Default::default(),
            })),
        );
        let equality = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, column(1, 1), column(2, 0)).into(),
        );
        let plan = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(cross, vec![equality])),
        );

        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize binding cross product");

        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 1);
        assert!(join.conditions[0].left.equals(&column(1, 1)));
        assert!(join.conditions[0].right.equals(&column(2, 0)));
    }

    #[test]
    fn cross_product_residual_is_consumed_inside_normalized_hash_join() {
        let context = BindContext::new();
        let cross = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Join(Join::Cross(CrossProduct {
                left: Box::new(input(&context, 1)),
                right: Box::new(input(&context, 2)),
                build_side_constraint: Default::default(),
            })),
        );
        let equality = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::Equal, reference(0), reference(2)).into(),
        );
        let residual = Expression::Comparison(
            ComparisonExpression::new(ComparisonType::GreaterThan, reference(1), reference(3))
                .into(),
        );
        let plan = OwnedLogicalPlan::new(
            &context,
            LogicalOperator::Filter(Filter::new(cross, vec![equality, residual.clone()])),
        );

        let optimized = JoinPredicateNormalizer::new(&context)
            .optimize_plan(plan)
            .expect("normalize cross product residual");

        let LogicalOperator::Join(Join::Comparison(join)) = &optimized.operator else {
            panic!("expected comparison join");
        };
        assert_eq!(join.conditions.len(), 2);
        assert_eq!(
            join.conditions[1].comparison,
            JoinComparisonType::GreaterThan
        );
        assert!(join.conditions[1].left.equals(&reference(1)));
        assert!(join.conditions[1].right.equals(&reference(1)));
    }
}
