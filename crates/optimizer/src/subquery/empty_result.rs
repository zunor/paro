// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Empty-result pullup optimizer.
//!
//! Keep `EmptyResult` as a schema-preserving marker and bubble it upward
//! through operators that cannot produce rows once one of their inputs is empty.

use paro_planner::operator::empty_result::EmptyResult;
use paro_planner::operator::{Join, JoinType, LogicalOperator};
use paro_planner::plan::LogicalPlan;

/// Pull empty-result markers upward through the logical plan.
pub struct EmptyResultPullup;

impl EmptyResultPullup {
    pub fn new() -> Self {
        Self
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_recursive_plan(plan)
    }

    fn optimize_recursive_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize_recursive_plan(child));
        plan.map_operator(|operator| self.pull_up(operator))
    }

    fn pull_up(&self, plan: LogicalOperator) -> LogicalOperator {
        match plan {
            LogicalOperator::Projection(proj) => {
                if proj.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Projection(proj))
                } else {
                    LogicalOperator::Projection(proj)
                }
            }
            LogicalOperator::Filter(filter) => {
                if filter.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Filter(filter))
                } else {
                    LogicalOperator::Filter(filter)
                }
            }
            LogicalOperator::Order(order) => {
                if order.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Order(order))
                } else {
                    LogicalOperator::Order(order)
                }
            }
            LogicalOperator::TopN(topn) => {
                if topn.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::TopN(topn))
                } else {
                    LogicalOperator::TopN(topn)
                }
            }
            LogicalOperator::Limit(limit) => {
                if limit.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Limit(limit))
                } else {
                    LogicalOperator::Limit(limit)
                }
            }
            LogicalOperator::Distinct(distinct) => {
                if distinct.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Distinct(distinct))
                } else {
                    LogicalOperator::Distinct(distinct)
                }
            }
            LogicalOperator::Window(window) => {
                if window.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Window(window))
                } else {
                    LogicalOperator::Window(window)
                }
            }
            LogicalOperator::Explain(explain) => {
                if explain.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::Explain(explain))
                } else {
                    LogicalOperator::Explain(explain)
                }
            }
            LogicalOperator::Join(join) => self.pull_up_join(join),
            LogicalOperator::SetOperation(setop) => {
                if setop.left.is_empty_result() || setop.right.is_empty_result() {
                    match setop.setop_type {
                        paro_planner::operator::SetOpType::Union => {
                            if setop.left.is_empty_result() && setop.right.is_empty_result() {
                                Self::empty_result(LogicalOperator::SetOperation(setop))
                            } else {
                                LogicalOperator::SetOperation(setop)
                            }
                        }
                        paro_planner::operator::SetOpType::Intersect => {
                            Self::empty_result(LogicalOperator::SetOperation(setop))
                        }
                        paro_planner::operator::SetOpType::Except => {
                            if setop.right.is_empty_result() {
                                let left = *setop.left;
                                left.operator
                            } else {
                                Self::empty_result(LogicalOperator::SetOperation(setop))
                            }
                        }
                    }
                } else {
                    LogicalOperator::SetOperation(setop)
                }
            }
            LogicalOperator::MaterializedCTE(cte) => {
                if cte.child.is_empty_result() {
                    Self::empty_result(LogicalOperator::MaterializedCTE(cte))
                } else {
                    LogicalOperator::MaterializedCTE(cte)
                }
            }
            LogicalOperator::RecursiveCTE(cte) => {
                if cte.anchor.is_empty_result() && cte.recursive.is_empty_result() {
                    Self::empty_result(LogicalOperator::RecursiveCTE(cte))
                } else {
                    LogicalOperator::RecursiveCTE(cte)
                }
            }
            other => other,
        }
    }

    fn pull_up_join(&self, join: Join) -> LogicalOperator {
        match join {
            Join::Comparison(comp) => {
                let left_empty = comp.left.is_empty_result();
                let right_empty = comp.right.is_empty_result();

                match comp.join_type {
                    JoinType::Inner | JoinType::Semi => {
                        if left_empty || right_empty {
                            Self::empty_result(LogicalOperator::Join(Join::Comparison(comp)))
                        } else {
                            LogicalOperator::Join(Join::Comparison(comp))
                        }
                    }
                    JoinType::Anti => {
                        if right_empty {
                            let left = *comp.left;
                            left.operator
                        } else if left_empty {
                            Self::empty_result(LogicalOperator::Join(Join::Comparison(comp)))
                        } else {
                            LogicalOperator::Join(Join::Comparison(comp))
                        }
                    }
                    JoinType::Mark | JoinType::Single | JoinType::Left => {
                        if left_empty {
                            Self::empty_result(LogicalOperator::Join(Join::Comparison(comp)))
                        } else {
                            LogicalOperator::Join(Join::Comparison(comp))
                        }
                    }
                    _ => LogicalOperator::Join(Join::Comparison(comp)),
                }
            }
            Join::Any(any) => {
                let left_empty = any.left.is_empty_result();
                let right_empty = any.right.is_empty_result();

                match any.join_type {
                    JoinType::Inner | JoinType::Semi => {
                        if left_empty || right_empty {
                            Self::empty_result(LogicalOperator::Join(Join::Any(any)))
                        } else {
                            LogicalOperator::Join(Join::Any(any))
                        }
                    }
                    JoinType::Anti => {
                        if right_empty {
                            let left = *any.left;
                            left.operator
                        } else if left_empty {
                            Self::empty_result(LogicalOperator::Join(Join::Any(any)))
                        } else {
                            LogicalOperator::Join(Join::Any(any))
                        }
                    }
                    JoinType::Mark | JoinType::Single | JoinType::Left => {
                        if left_empty {
                            Self::empty_result(LogicalOperator::Join(Join::Any(any)))
                        } else {
                            LogicalOperator::Join(Join::Any(any))
                        }
                    }
                    _ => LogicalOperator::Join(Join::Any(any)),
                }
            }
            Join::Cross(cross) => {
                if cross.left.is_empty_result() || cross.right.is_empty_result() {
                    Self::empty_result(LogicalOperator::Join(Join::Cross(cross)))
                } else {
                    LogicalOperator::Join(Join::Cross(cross))
                }
            }
        }
    }

    fn empty_result(op: LogicalOperator) -> LogicalOperator {
        LogicalOperator::EmptyResult(EmptyResult::new(LogicalPlan::synthetic(op)))
    }
}

#[cfg(test)]
mod tests {
    use super::EmptyResultPullup;
    use paro_common::types::LogicalType;
    use paro_planner::binder::context::BindContext;
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::empty_result::EmptyResult;
    use paro_planner::operator::{
        ColumnBinding, ComparisonJoin, ExpressionGet, Join, JoinComparisonType, JoinCondition,
        JoinType, LogicalOperator,
    };
    use paro_planner::plan::LogicalPlan;

    fn expression_get(ctx: &BindContext, table_index: usize) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                table_index,
                vec![vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(table_index, 0),
                    LogicalType::Integer,
                ))]],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn empty(ctx: &BindContext, op: LogicalOperator) -> LogicalPlan {
        LogicalPlan::new(
            ctx,
            LogicalOperator::EmptyResult(EmptyResult::new(LogicalPlan::new(ctx, op))),
        )
    }

    #[test]
    fn pullup_collapses_left_empty_delim_single_join() {
        let ctx = BindContext::new();
        let left = empty(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                vec![vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                ))]],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let right = expression_get(&ctx, 1);
        let mut join = ComparisonJoin::new(
            JoinType::Single,
            left,
            right,
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
        ))];

        let result = EmptyResultPullup::new().optimize_plan(LogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(join)),
        ));
        assert!(matches!(result.operator, LogicalOperator::EmptyResult(_)));
    }

    #[test]
    fn pullup_keeps_mark_join_with_empty_rhs() {
        let ctx = BindContext::new();
        let left = expression_get(&ctx, 0);
        let right = empty(
            &ctx,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                1,
                vec![vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                ))]],
                vec!["v".to_string()],
                vec![LogicalType::Integer],
            )),
        );
        let mut join = ComparisonJoin::new(
            JoinType::Mark,
            left,
            right,
            vec![JoinCondition::new(
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Integer,
                )),
                JoinComparisonType::Equal,
            )],
        );
        join.duplicate_eliminated_columns = vec![Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(0, 0),
            LogicalType::Integer,
        ))];

        let result = EmptyResultPullup::new().optimize_plan(LogicalPlan::synthetic(
            LogicalOperator::Join(Join::Comparison(join)),
        ));
        match result.operator {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(matches!(
                    join.right.operator,
                    LogicalOperator::EmptyResult(_)
                ));
            }
            _ => panic!("expected join to remain"),
        }
    }
}
