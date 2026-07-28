// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plans subqueries: uncorrelated (cross product, aggregates, mark joins) and correlated (`DependentJoin` + decorrelation).

use crate::binder::Binder;
use crate::expression::*;
use crate::operator::{
    Aggregate, ColumnBinding, ComparisonJoin, CrossProduct, Join, JoinComparisonType,
    JoinCondition, JoinType, Limit, LogicalOperator, Projection,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::distributive::count::get_count_star_function;
use paro_function::aggregate::distributive::first_last::get_first_function;

impl Binder {
    fn subquery_output_column_ref(
        subquery_plan: &LogicalOperator,
        index: usize,
        return_type: LogicalType,
    ) -> Result<Expression> {
        let bindings = subquery_plan.get_column_bindings();
        let Some(binding) = bindings.get(index).copied() else {
            return Err(paro_error::internal(format!(
                "Subquery output binding {} out of range (available: {})",
                index,
                bindings.len()
            )));
        };

        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            binding,
            return_type,
        )))
    }

    pub(crate) fn plan_current_layer_subqueries_in_list(
        &mut self,
        exprs: &mut [Expression],
        root: &mut LogicalOperator,
    ) -> Result<bool> {
        if !self.delayed_subquery_planning_enabled() {
            return Ok(false);
        }

        let mut planned_any = false;
        for expr in exprs {
            planned_any |= self.plan_current_layer_subqueries(expr, root)?;
        }
        Ok(planned_any)
    }

    pub(crate) fn plan_current_layer_subqueries(
        &mut self,
        expr: &mut Expression,
        root: &mut LogicalOperator,
    ) -> Result<bool> {
        if !self.delayed_subquery_planning_enabled() {
            return Ok(false);
        }

        match expr {
            Expression::Subquery(subquery) => {
                if subquery.planning_state != crate::expression::SubqueryPlanningState::Unplanned {
                    return Ok(false);
                }
                if !subquery.correlated_columns.is_empty()
                    && !subquery
                        .correlated_columns
                        .iter()
                        .any(|corr| corr.depth == 1)
                {
                    return Ok(false);
                }
                subquery.planning_state = crate::expression::SubqueryPlanningState::Planning;
                if !subquery.correlated_columns.is_empty() {
                    let replacement = self.plan_correlated_subquery(subquery, root)?;
                    *expr = replacement;
                } else {
                    let replacement = self.plan_uncorrelated_subquery(subquery, root)?;
                    *expr = replacement;
                }
                Ok(true)
            }
            _ => {
                let mut planned_any = false;
                let mut recurse_result = Ok(());
                ExpressionIterator::enumerate_children_mut(expr, |child| {
                    if recurse_result.is_ok() {
                        match self.plan_current_layer_subqueries(child, root) {
                            Ok(found) => planned_any |= found,
                            Err(err) => recurse_result = Err(err),
                        }
                    }
                });
                recurse_result?;
                Ok(planned_any)
            }
        }
    }

    pub fn plan_subqueries(
        &mut self,
        expr: &mut Expression,
        root: &mut LogicalOperator,
    ) -> Result<()> {
        let _ = self.plan_current_layer_subqueries(expr, root)?;
        Ok(())
    }

    fn plan_uncorrelated_subquery(
        &mut self,
        subquery: &SubqueryExpression,
        root: &mut LogicalOperator,
    ) -> Result<Expression> {
        let subquery_plan = self.copy_subquery_plan_into_current_context(
            &subquery.subquery,
            subquery.bind_snapshot.as_ref(),
        )?;

        match subquery.subquery_type {
            SubqueryType::Exists | SubqueryType::NotExists => {
                self.plan_uncorrelated_exists(subquery_plan, root, subquery.subquery_type)
            }
            SubqueryType::Scalar => {
                self.plan_uncorrelated_scalar(subquery_plan, root, &subquery.return_type)
            }
            SubqueryType::Any => self.plan_uncorrelated_any(
                subquery_plan,
                root,
                &subquery.children,
                subquery.comparison_type,
            ),
            SubqueryType::All => self.plan_uncorrelated_all(
                subquery_plan,
                root,
                &subquery.children,
                subquery.comparison_type,
            ),
        }
    }

    fn plan_uncorrelated_exists(
        &mut self,
        subquery_plan: LogicalOperator,
        root: &mut LogicalOperator,
        subquery_type: SubqueryType,
    ) -> Result<Expression> {
        let limit_expr = Expression::Constant(ConstantExpression {
            value: Value::BigInt(1),
            return_type: LogicalType::BigInt,
        });
        let limited = Limit::new(self.wrap_plan(subquery_plan), Some(limit_expr), None);
        let plan = LogicalOperator::Limit(limited);

        let count_star = get_count_star_function();
        let count_agg = AggregateExpression::new(count_star, vec![], LogicalType::BigInt);
        let group_index = self.bind_context.generate_table_index();
        let aggregate_index = self.bind_context.generate_table_index();
        let groupings_index = self.bind_context.generate_table_index();
        let aggregate = Aggregate::new(
            group_index,
            aggregate_index,
            groupings_index,
            self.wrap_plan(plan),
            Vec::new(),
            Vec::new(),
            vec![Expression::Aggregate(count_agg)],
            vec![],
        );
        let plan = LogicalOperator::Aggregate(aggregate);

        let count_ref = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(aggregate_index, 0),
            LogicalType::BigInt,
        ));
        let one = Expression::Constant(ConstantExpression {
            value: Value::BigInt(1),
            return_type: LogicalType::BigInt,
        });

        let comparison_type = match subquery_type {
            SubqueryType::Exists => ComparisonType::Equal,
            SubqueryType::NotExists => ComparisonType::NotEqual,
            _ => unreachable!(),
        };

        let comparison =
            Expression::Comparison(ComparisonExpression::new(comparison_type, count_ref, one));

        let projection_index = self.bind_context.generate_table_index();
        let projection = Projection::new(projection_index, self.wrap_plan(plan), vec![comparison]);
        let plan = LogicalOperator::Projection(projection);

        let old_root = std::mem::replace(root, LogicalOperator::DummyScan);
        *root = LogicalOperator::Join(Join::Cross(CrossProduct::new(
            self.wrap_plan(old_root),
            self.wrap_plan(plan),
        )));

        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(projection_index, 0),
            LogicalType::Boolean,
        )))
    }

    fn plan_uncorrelated_scalar(
        &mut self,
        subquery_plan: LogicalOperator,
        root: &mut LogicalOperator,
        return_type: &LogicalType,
    ) -> Result<Expression> {
        let subquery_types = subquery_plan.types();
        if subquery_types.len() != 1 {
            return Err(paro_error::syntax(format!(
                "Scalar subquery must return exactly one column, got {}",
                subquery_types.len()
            )));
        }

        let subquery_col_type = subquery_types[0].clone();

        let first_func_set = get_first_function();
        let (first_func, _) = first_func_set
            .bind(std::slice::from_ref(&subquery_col_type))
            .map_err(|e| paro_error::internal(format!("Failed to bind FIRST function: {}", e)))?;

        let subquery_ref =
            Self::subquery_output_column_ref(&subquery_plan, 0, subquery_col_type.clone())?;

        let first_agg =
            AggregateExpression::new(first_func, vec![subquery_ref], return_type.clone());
        let count_star = get_count_star_function();
        let count_agg = AggregateExpression::new(count_star, vec![], LogicalType::BigInt);

        let group_index = self.bind_context.generate_table_index();
        let aggregate_index = self.bind_context.generate_table_index();
        let groupings_index = self.bind_context.generate_table_index();
        let aggregate = Aggregate::new(
            group_index,
            aggregate_index,
            groupings_index,
            self.wrap_plan(subquery_plan),
            Vec::new(),
            Vec::new(),
            vec![
                Expression::Aggregate(first_agg),
                Expression::Aggregate(count_agg),
            ],
            vec![],
        );
        let plan = LogicalOperator::Aggregate(aggregate);

        let projection_index = self.bind_context.generate_table_index();
        let first_ref = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(aggregate_index, 0),
            return_type.clone(),
        ));
        let count_ref = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(aggregate_index, 1),
            LogicalType::BigInt,
        ));
        let checked_value = Expression::Operator(OperatorExpression::new(
            OperatorType::ErrorIfMultipleRows,
            vec![first_ref, count_ref],
            return_type.clone(),
        ));
        let projection =
            Projection::new(projection_index, self.wrap_plan(plan), vec![checked_value]);
        let plan = LogicalOperator::Projection(projection);

        let old_root = std::mem::replace(root, LogicalOperator::DummyScan);
        *root = LogicalOperator::Join(Join::Cross(CrossProduct::new(
            self.wrap_plan(old_root),
            self.wrap_plan(plan),
        )));

        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(projection_index, 0),
            return_type.clone(),
        )))
    }

    fn plan_uncorrelated_any(
        &mut self,
        subquery_plan: LogicalOperator,
        root: &mut LogicalOperator,
        children: &[Expression],
        comparison_type: ComparisonType,
    ) -> Result<Expression> {
        if children.is_empty() {
            return Err(paro_error::syntax(
                "ANY/IN subquery requires at least one comparison expression",
            ));
        }

        let subquery_types = subquery_plan.types();
        if subquery_types.len() != children.len() {
            return Err(paro_error::syntax(format!(
                "ANY/IN subquery column count mismatch: expected {}, got {}",
                children.len(),
                subquery_types.len()
            )));
        }

        let mark_index = self.bind_context.generate_table_index();

        let mut conditions = Vec::new();
        for (i, child) in children.iter().enumerate() {
            let right =
                Self::subquery_output_column_ref(&subquery_plan, i, subquery_types[i].clone())?;

            let join_comparison = match comparison_type {
                ComparisonType::Equal => JoinComparisonType::Equal,
                ComparisonType::NotEqual => JoinComparisonType::NotEqual,
                ComparisonType::LessThan => JoinComparisonType::LessThan,
                ComparisonType::GreaterThan => JoinComparisonType::GreaterThan,
                ComparisonType::LessThanOrEqual => JoinComparisonType::LessThanOrEqual,
                ComparisonType::GreaterThanOrEqual => JoinComparisonType::GreaterThanOrEqual,
                ComparisonType::DistinctFrom => JoinComparisonType::DistinctFrom,
                ComparisonType::NotDistinctFrom => JoinComparisonType::NotDistinctFrom,
            };

            conditions.push(JoinCondition::new(child.clone(), right, join_comparison));
        }

        let old_root = std::mem::replace(root, LogicalOperator::DummyScan);
        let mut mark_join = ComparisonJoin::new(
            JoinType::Mark,
            self.wrap_plan(old_root),
            self.wrap_plan(subquery_plan),
            conditions,
        );
        mark_join.mark_index = Some(mark_index);

        *root = LogicalOperator::Join(Join::Comparison(mark_join));

        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(mark_index, 0),
            LogicalType::Boolean,
        )))
    }

    fn plan_uncorrelated_all(
        &mut self,
        subquery_plan: LogicalOperator,
        root: &mut LogicalOperator,
        children: &[Expression],
        comparison_type: ComparisonType,
    ) -> Result<Expression> {
        let inverted_comparison = match comparison_type {
            ComparisonType::Equal => ComparisonType::NotEqual,
            ComparisonType::NotEqual => ComparisonType::Equal,
            ComparisonType::LessThan => ComparisonType::GreaterThanOrEqual,
            ComparisonType::GreaterThan => ComparisonType::LessThanOrEqual,
            ComparisonType::LessThanOrEqual => ComparisonType::GreaterThan,
            ComparisonType::GreaterThanOrEqual => ComparisonType::LessThan,
            ComparisonType::DistinctFrom => ComparisonType::NotDistinctFrom,
            ComparisonType::NotDistinctFrom => ComparisonType::DistinctFrom,
        };

        let any_result =
            self.plan_uncorrelated_any(subquery_plan, root, children, inverted_comparison)?;

        let false_const = Expression::Constant(ConstantExpression {
            value: Value::Boolean(false),
            return_type: LogicalType::Boolean,
        });

        Ok(Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            any_result,
            false_const,
        )))
    }

    pub fn contains_subquery(expr: &Expression) -> bool {
        if matches!(expr, Expression::Subquery(_)) {
            return true;
        }

        let mut has_subquery = false;
        ExpressionIterator::enumerate_children(expr, |child| {
            if !has_subquery && Self::contains_subquery(child) {
                has_subquery = true;
            }
        });
        has_subquery
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_session as binder_test_session;
    use crate::planner::Planner;
    use paro_context::StatementContext;
    use std::process::Output;

    fn test_session() -> std::sync::Arc<StatementContext> {
        binder_test_session(Vec::new())
    }

    fn raw_logical_plan_debug(sql: &str) -> String {
        let session = test_session();
        let mut binder = Binder::new(session);
        let statement = paro_parser::parse_one(sql).expect("parse").stmt;
        let bound = binder.bind(statement).expect("bind");
        format!("{:?}", bound.plan)
    }

    fn planned_logical_operator(sql: &str) -> LogicalOperator {
        let session = test_session();
        let mut planner = Planner::new(session);
        let statement = paro_parser::parse_one(sql).expect("parse").stmt;
        planner.create_plan(statement).expect("planner create_plan");
        planner.take_plan().expect("planned logical plan").operator
    }

    fn binder_planned_logical_operator(sql: &str) -> LogicalOperator {
        let session = test_session();
        let mut binder = Binder::new(session);
        let statement = paro_parser::parse_one(sql).expect("parse").stmt;
        let bound = binder
            .bind_statement_kind(statement)
            .expect("bind statement kind");
        binder
            .create_plan(bound)
            .expect("binder create_plan without final flatten")
            .operator
    }

    fn flattened_logical_operator(sql: &str) -> LogicalOperator {
        let session = test_session();
        let mut binder = Binder::new(session);
        let statement = paro_parser::parse_one(sql).expect("parse").stmt;
        let bound = binder
            .bind_statement_kind(statement)
            .expect("bind statement kind");
        let plan = binder
            .create_plan(bound)
            .expect("binder create_plan before flatten");
        binder
            .flatten_dependent_joins(plan)
            .expect("flatten dependent joins")
            .operator
    }

    fn nested_case_sql(case: &str) -> &'static str {
        match case {
            "three_level_scalar" => {
                "SELECT o.id, \
                 (SELECT (SELECT (SELECT d.score \
                                   FROM (VALUES (10, 9), (20, 5)) AS d(grp, score) \
                                   WHERE d.grp = o.grp))) \
                 FROM (VALUES (1, 10), (2, 20)) AS o(id, grp)"
            }
            "two_level_exists" => {
                "SELECT o.id, \
                 EXISTS(SELECT 1 \
                        WHERE EXISTS(SELECT 1 \
                                     FROM (VALUES (10, 4), (20, 5)) AS d(grp, score) \
                                     WHERE d.grp = o.grp)) \
                 FROM (VALUES (1, 10), (2, 30)) AS o(id, grp)"
            }
            "setop_nested_outer" => {
                "SELECT o.id \
                 FROM (VALUES (1, 10), (2, 20)) AS o(id, grp) \
                 WHERE EXISTS( \
                   SELECT 1 \
                   FROM ( \
                     SELECT d.score \
                     FROM (VALUES (10, 4), (10, 9), (20, 5)) AS d(grp, score) \
                     WHERE d.grp = o.grp \
                     UNION \
                     SELECT 0 \
                     WHERE EXISTS( \
                       SELECT 1 \
                       FROM (VALUES (10, 1), (20, 1)) AS x(grp, flag) \
                       WHERE x.grp = o.grp \
                     ) \
                   ) AS s(score) \
                 )"
            }
            "setop_union_outer" => {
                "SELECT o.id \
                 FROM (VALUES (1, 10, 6), (2, 20, 5)) AS o(id, grp, threshold) \
                 WHERE EXISTS( \
                   SELECT d.seq \
                   FROM (VALUES (10, 1, 4), (10, 2, 9), (20, 1, 5), (20, 2, 8)) \
                        AS d(grp, seq, score) \
                   WHERE d.grp = o.grp AND d.seq <= 1 \
                   UNION \
                   SELECT d.seq \
                   FROM (VALUES (10, 1, 4), (10, 2, 9), (20, 1, 5), (20, 2, 8)) \
                        AS d(grp, seq, score) \
                   WHERE d.grp = o.grp AND d.score >= o.threshold \
                 )"
            }
            "setop_intersect_outer" => {
                "SELECT o.id \
                 FROM (VALUES (1, 10, 6), (2, 20, 5)) AS o(id, grp, threshold) \
                 WHERE EXISTS( \
                   SELECT d.seq \
                   FROM (VALUES (10, 1, 4), (10, 2, 9), (20, 1, 5), (20, 2, 8)) \
                        AS d(grp, seq, score) \
                   WHERE d.grp = o.grp AND d.seq <= 2 \
                   INTERSECT \
                   SELECT d.seq \
                   FROM (VALUES (10, 1, 4), (10, 2, 9), (20, 1, 5), (20, 2, 8)) \
                        AS d(grp, seq, score) \
                   WHERE d.grp = o.grp AND d.score >= o.threshold \
                 )"
            }
            "setop_except_outer" => {
                "SELECT o.id \
                 FROM (VALUES (1, 10, 6), (2, 20, 5)) AS o(id, grp, threshold) \
                 WHERE EXISTS( \
                   SELECT d.seq \
                   FROM (VALUES (10, 1, 4), (10, 2, 9), (20, 1, 5), (20, 2, 8)) \
                        AS d(grp, seq, score) \
                   WHERE d.grp = o.grp AND d.seq <= 2 \
                   EXCEPT \
                   SELECT d.seq \
                   FROM (VALUES (10, 1, 4), (10, 2, 9), (20, 1, 5), (20, 2, 8)) \
                        AS d(grp, seq, score) \
                   WHERE d.grp = o.grp AND d.score >= o.threshold \
                 )"
            }
            "having_nested_outer" => {
                "SELECT o.grp \
                 FROM (VALUES (1, 10, 6), (2, 20, 5)) AS o(id, grp, threshold) \
                 GROUP BY o.grp, o.threshold \
                 HAVING EXISTS( \
                   SELECT 1 \
                   FROM ( \
                     SELECT (SELECT d.score \
                             FROM (VALUES (10, 9), (20, 5)) AS d(grp, score) \
                             WHERE d.grp = o.grp) AS top_score \
                   ) AS nested \
                   WHERE nested.top_score >= o.threshold \
                 )"
            }
            "outer_first_visible" => {
                "SELECT o.id, \
                 EXISTS(SELECT 1 \
                        WHERE EXISTS(SELECT 1 \
                                     FROM (VALUES (10, 4), (20, 5)) AS d(grp, score) \
                                     WHERE d.grp = o.grp)) \
                 FROM (VALUES (1, 10), (2, 30)) AS o(id, grp)"
            }
            "join_on_nested_outer" => {
                "SELECT o.id \
                 FROM (VALUES (1, 10), (2, 20)) AS o(id, grp) \
                 JOIN (VALUES (1), (2)) AS p(id) \
                   ON p.id = o.id \
                  AND EXISTS( \
                        SELECT 1 \
                        WHERE EXISTS( \
                              SELECT 1 \
                              FROM (VALUES (10, 4), (20, 5)) AS d(grp, score) \
                              WHERE d.grp = o.grp \
                        ) \
                  )"
            }
            other => panic!("unknown nested case: {other}"),
        }
    }

    fn run_subquery_probe(case: &str, mode: &str) -> Output {
        let exe = std::env::current_exe().expect("current test binary");
        std::process::Command::new(exe)
            .arg("--exact")
            .arg("binder::plan::subquery::dispatcher::tests::subquery_probe_harness")
            .arg("--nocapture")
            .env("PARO_SUBQUERY_CASE", case)
            .env("PARO_SUBQUERY_MODE", mode)
            .output()
            .expect("run subquery probe subprocess")
    }

    fn assert_planned_probe_succeeds(case: &str) {
        let output = run_subquery_probe(case, "planned");
        assert!(output.status.success(), "{output:?}");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.contains("Subquery("));
        assert!(!stdout.contains("planning_state: Unplanned"));
    }

    #[test]
    fn scalar_subquery_plan_inserts_multi_row_error_check() {
        let session = test_session();
        let mut binder = Binder::new(session);
        let statement = paro_parser::parse_one("SELECT (SELECT * FROM (VALUES (1), (2)) AS t(x))")
            .expect("parse")
            .stmt;
        let bound = binder.bind(statement).expect("bind");
        let plan_debug = format!("{:?}", bound.plan);

        assert!(plan_debug.contains("ErrorIfMultipleRows"));
    }

    #[test]
    fn uncorrelated_scalar_plan_keeps_logical_column_ref_for_aggregate_input() {
        let session = test_session();
        let mut binder = Binder::new(session);
        let statement = paro_parser::parse_one("SELECT (SELECT x FROM (VALUES (1)) AS t(x))")
            .expect("parse")
            .stmt;
        let bound = binder.bind(statement).expect("bind");

        let LogicalOperator::Projection(root_projection) = &bound.plan.operator else {
            panic!("expected root projection, got {:?}", bound.plan);
        };
        let LogicalOperator::Join(Join::Cross(cross)) = &root_projection.child.operator else {
            panic!(
                "expected cross product below root projection, got {:?}",
                root_projection.child
            );
        };
        let LogicalOperator::Projection(projection) = &cross.right.operator else {
            panic!(
                "expected scalar subquery projection on rhs, got {:?}",
                cross.right
            );
        };
        let LogicalOperator::Aggregate(aggregate) = &projection.child.operator else {
            panic!(
                "expected aggregate below scalar projection, got {:?}",
                projection.child
            );
        };
        let Expression::Aggregate(first_agg) = &aggregate.aggregates[0] else {
            panic!(
                "expected FIRST aggregate, got {:?}",
                aggregate.aggregates[0]
            );
        };

        assert!(matches!(
            first_agg.children.first(),
            Some(Expression::ColumnRef(_))
        ));
    }

    #[test]
    fn subquery_probe_harness() {
        let Ok(case) = std::env::var("PARO_SUBQUERY_CASE") else {
            return;
        };
        let mode = std::env::var("PARO_SUBQUERY_MODE").expect("probe mode");
        let sql = nested_case_sql(&case);
        let plan_debug = match mode.as_str() {
            "raw" => raw_logical_plan_debug(sql),
            "binder_planned" => format!("{:?}", binder_planned_logical_operator(sql)),
            "flattened" => format!("{:?}", flattened_logical_operator(sql)),
            "planned" => format!("{:?}", planned_logical_operator(sql)),
            other => panic!("unknown probe mode: {other}"),
        };
        tracing::info!(target: "paro_planner.subquery_probe", "{plan_debug}");
    }

    #[test]
    fn three_level_nested_correlated_scalar_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("three_level_scalar");
    }

    #[test]
    fn two_level_nested_correlated_exists_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("two_level_exists");
    }

    #[test]
    fn set_operation_with_nested_outer_correlation_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("setop_nested_outer");
    }

    #[test]
    fn union_set_operation_with_outer_correlation_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("setop_union_outer");
    }

    #[test]
    fn intersect_set_operation_with_outer_correlation_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("setop_intersect_outer");
    }

    #[test]
    fn except_set_operation_with_outer_correlation_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("setop_except_outer");
    }

    #[test]
    fn having_with_nested_outer_correlation_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("having_nested_outer");
    }

    #[test]
    fn recursive_planner_consumes_outer_first_subquery_layers() {
        assert_planned_probe_succeeds("outer_first_visible");
    }

    #[test]
    fn join_on_nested_outer_correlation_plans_without_remaining_subquery() {
        assert_planned_probe_succeeds("join_on_nested_outer");
    }
}
