// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Correlated subquery planning: build [`DependentJoin`] and related helpers.
//!
//! Decorrelation lives in [`crate::binder::plan::subquery::decorrelate`].

use crate::binder::context::BindSnapshot;
use crate::binder::plan::subquery::{
    copy_subquery_top_level, copy_subquery_top_level_plan, flatten_dependent_join,
};
use crate::expression::{
    ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    SubqueryExpression, SubqueryType,
};
use crate::operator::{AnyAllPayload, ColumnBinding, DependentJoin, LogicalOperator};
use crate::plan::PlannedStatement;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;

impl crate::binder::Binder {
    fn build_correlated_dependent_join(
        &mut self,
        root: LogicalOperator,
        subquery_plan: LogicalOperator,
        subquery: &SubqueryExpression,
    ) -> DependentJoin {
        let left = self.wrap_plan(root);
        let right = self.wrap_plan(subquery_plan);
        let correlated_columns = subquery.correlated_columns.clone();

        match subquery.subquery_type {
            SubqueryType::Scalar => DependentJoin::scalar(left, right, correlated_columns),
            SubqueryType::Exists => DependentJoin::mark_exists(
                left,
                right,
                correlated_columns,
                self.bind_context.generate_table_index(),
            ),
            SubqueryType::NotExists => DependentJoin::mark_not_exists(
                left,
                right,
                correlated_columns,
                self.bind_context.generate_table_index(),
            ),
            SubqueryType::Any => DependentJoin::mark_any(
                left,
                right,
                correlated_columns,
                self.bind_context.generate_table_index(),
                AnyAllPayload {
                    comparison_type: subquery.comparison_type,
                    expression_children: subquery.children.clone(),
                    child_types: subquery.child_types.clone(),
                    child_targets: subquery.child_targets.clone(),
                },
            ),
            SubqueryType::All => DependentJoin::mark_all(
                left,
                right,
                correlated_columns,
                self.bind_context.generate_table_index(),
                AnyAllPayload {
                    comparison_type: subquery.comparison_type,
                    expression_children: subquery.children.clone(),
                    child_types: subquery.child_types.clone(),
                    child_targets: subquery.child_targets.clone(),
                },
            ),
        }
    }

    pub fn plan_correlated_subquery(
        &mut self,
        subquery: &SubqueryExpression,
        root: &mut LogicalOperator,
    ) -> Result<Expression> {
        if subquery
            .correlated_columns
            .iter()
            .any(|corr| corr.depth != 1)
        {
            return Err(paro_error::internal(format!(
                "Current-layer correlated planning only accepts depth==1 columns, got {:?}",
                subquery.correlated_columns
            )));
        }

        let copied_statement = copy_subquery_top_level_plan(
            subquery.subquery.as_ref(),
            subquery.bind_snapshot.as_ref(),
        );
        let subquery_plan = copied_statement.plan.operator;

        let old_root = std::mem::replace(root, LogicalOperator::DummyScan);
        let dependent_join = match subquery.subquery_type {
            SubqueryType::Scalar
            | SubqueryType::Exists
            | SubqueryType::NotExists
            | SubqueryType::Any
            | SubqueryType::All => {
                self.build_correlated_dependent_join(old_root, subquery_plan, subquery)
            }
        };
        let planned_mark_index = dependent_join.mark_index();

        let flattened = flatten_dependent_join(self, dependent_join)?;
        let result_bindings = flattened.get_column_bindings();
        let result_col_index = result_bindings.len().saturating_sub(1);
        *root = flattened;
        match subquery.subquery_type {
            SubqueryType::Exists | SubqueryType::Any => {
                let result_binding = ColumnBinding::new(
                    planned_mark_index.ok_or_else(|| {
                        paro_error::internal(
                            "Planner must assign mark_index before flattening correlated MARK subqueries",
                        )
                    })?,
                    0,
                );
                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    result_binding,
                    LogicalType::Boolean,
                )))
            }
            SubqueryType::NotExists | SubqueryType::All => {
                let mark_index = planned_mark_index.ok_or_else(|| {
                    paro_error::internal(
                        "Planner must assign mark_index before flattening negated MARK subqueries",
                    )
                })?;
                Ok(Self::negated_mark_expression(mark_index))
            }
            SubqueryType::Scalar => {
                let result_binding =
                    result_bindings
                        .get(result_col_index)
                        .copied()
                        .ok_or_else(|| {
                            paro_error::internal(
                                "Flattened correlated subquery must expose a result binding",
                            )
                        })?;
                let result_types = root.types();
                let result_type = result_types
                    .get(result_col_index)
                    .cloned()
                    .unwrap_or_else(|| subquery.return_type.clone());
                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    result_binding,
                    result_type,
                )))
            }
        }
    }

    pub(crate) fn copy_subquery_plan_into_current_context(
        &mut self,
        statement: &PlannedStatement,
        bind_snapshot: &BindSnapshot,
    ) -> Result<LogicalOperator> {
        Ok(copy_subquery_top_level(
            &statement.plan.operator,
            bind_snapshot,
        ))
    }

    fn negated_mark_expression(mark_index: usize) -> Expression {
        let mark_ref = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(mark_index, 0),
            LogicalType::Boolean,
        ));
        let false_const = Expression::Constant(ConstantExpression {
            value: Value::Boolean(false),
            return_type: LogicalType::Boolean,
        });
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            mark_ref,
            false_const,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::context::BindContext;
    use crate::binder::ir::OrderByNode;
    use crate::binder::test_utils::test_binder;
    use crate::binder::CorrelatedColumnInfo;
    use crate::expression::{
        AggregateExpression, CastExpression, ColumnRefExpression, OperatorExpression, OperatorType,
        OrderByExpression, SubqueryExpression, SubqueryPlanningState, WindowExpression,
        WindowFrame,
    };
    use crate::operator::{
        Aggregate, ColumnBinding, CrossProduct, DependentJoinKind, Distinct, ExpressionGet, Join,
        JoinComparisonType, JoinType, MarkSubqueryKind, Projection, SetOpType, SetOperation,
        Window,
    };
    use crate::plan::LogicalPlan;
    use crate::plan::PlannedStatement;
    use paro_function::aggregate::distributive::first_last::get_first_function;
    use paro_function::window::WindowFunction;
    use std::sync::Arc;

    fn wrapped(binder: &crate::binder::Binder, op: LogicalOperator) -> LogicalPlan {
        binder.wrap_plan(op)
    }

    fn expression_get(table_index: usize, types: Vec<LogicalType>) -> LogicalOperator {
        let names = (0..types.len()).map(|idx| format!("c{}", idx)).collect();
        LogicalOperator::ExpressionGet(ExpressionGet::new(
            table_index,
            Vec::<Vec<Expression>>::new(),
            names,
            types,
        ))
    }

    fn correlated_column(
        table_index: usize,
        column_index: usize,
        return_type: LogicalType,
    ) -> CorrelatedColumnInfo {
        CorrelatedColumnInfo {
            table_index,
            column_index,
            return_type,
            name: "corr".to_string(),
            depth: 1,
        }
    }

    fn subquery_expression(
        subquery_type: SubqueryType,
        subquery_plan: LogicalOperator,
        children: Vec<Expression>,
        child_types: Vec<LogicalType>,
        child_targets: Vec<LogicalType>,
        correlated_columns: Vec<CorrelatedColumnInfo>,
        return_type: LogicalType,
        comparison_type: ComparisonType,
    ) -> SubqueryExpression {
        SubqueryExpression {
            subquery_type,
            subquery: Arc::new(PlannedStatement {
                types: subquery_plan.types(),
                names: vec!["subq".to_string()],
                plan: LogicalPlan::new(&BindContext::new(), subquery_plan),
            }),
            children,
            child_types,
            child_targets,
            comparison_type,
            return_type,
            correlated_columns,
            bind_snapshot: BindContext::new().snapshot(),
            planning_state: SubqueryPlanningState::Unplanned,
        }
    }

    fn extract_binding(expr: &Expression) -> Option<ColumnBinding> {
        match expr {
            Expression::ColumnRef(col_ref) => Some(col_ref.binding),
            Expression::Cast(cast) => extract_binding(&cast.child),
            _ => None,
        }
    }

    /// Walks common single-child wrappers to the leaf `ExpressionGet` (test plans only).
    fn expression_get_table_index_root(plan: &LogicalPlan) -> usize {
        match &plan.operator {
            LogicalOperator::ExpressionGet(eg) => eg.table_index,
            LogicalOperator::Filter(f) => expression_get_table_index_root(&f.child),
            LogicalOperator::Projection(p) => expression_get_table_index_root(&p.child),
            LogicalOperator::Aggregate(a) => expression_get_table_index_root(&a.child),
            other => panic!("expected ExpressionGet leaf, found {other:?}"),
        }
    }

    fn int_col(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(table_index, column_index),
            LogicalType::Integer,
        ))
    }

    #[test]
    fn build_correlated_dependent_join_populates_planner_owned_metadata() {
        let mut binder = test_binder();
        let outer = expression_get(10, vec![LogicalType::Integer]);
        let inner = expression_get(20, vec![LogicalType::Integer]);
        let correlated = vec![correlated_column(10, 0, LogicalType::Integer)];
        let child = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(10, 0),
            LogicalType::Integer,
        ));
        let subquery = subquery_expression(
            SubqueryType::Any,
            inner,
            vec![child],
            vec![LogicalType::Integer],
            vec![LogicalType::BigInt],
            correlated.clone(),
            LogicalType::Boolean,
            ComparisonType::GreaterThan,
        );

        let dependent_join = binder.build_correlated_dependent_join(
            outer,
            expression_get(30, vec![LogicalType::Integer]),
            &subquery,
        );

        match &dependent_join.kind {
            DependentJoinKind::Mark {
                mark_index,
                subquery: MarkSubqueryKind::Any(payload),
            } => {
                assert_eq!(
                    *mark_index,
                    dependent_join.mark_index().expect("mark index")
                );
                assert_eq!(payload.expression_children.len(), 1);
                assert_eq!(payload.child_types, vec![LogicalType::Integer]);
                assert_eq!(payload.child_targets, vec![LogicalType::BigInt]);
                assert_eq!(payload.comparison_type, ComparisonType::GreaterThan);
            }
            other => panic!("expected any-mark dependent join, got {other:?}"),
        }
        assert_eq!(dependent_join.correlated_columns, correlated);
    }

    #[test]
    fn build_correlated_dependent_join_marks_scalar_as_single() {
        let mut binder = test_binder();
        let subquery = subquery_expression(
            SubqueryType::Scalar,
            expression_get(31, vec![LogicalType::Integer]),
            vec![],
            vec![],
            vec![],
            vec![correlated_column(30, 0, LogicalType::Integer)],
            LogicalType::Integer,
            ComparisonType::Equal,
        );
        let dependent_join = binder.build_correlated_dependent_join(
            expression_get(30, vec![LogicalType::Integer]),
            expression_get(31, vec![LogicalType::Integer]),
            &subquery,
        );

        assert!(matches!(dependent_join.kind, DependentJoinKind::Scalar));
        assert!(dependent_join.mark_index().is_none());
    }

    #[test]
    fn plan_correlated_scalar_subquery_uses_flattened_output_binding() {
        let mut binder = test_binder();
        for _ in 0..3 {
            binder.bind_context.generate_table_index();
        }

        let correlated = vec![correlated_column(40, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::Scalar,
            expression_get(50, vec![LogicalType::BigInt]),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::BigInt,
            ComparisonType::Equal,
        );
        let mut root = expression_get(40, vec![LogicalType::Integer, LogicalType::Varchar]);
        let outer_bindings = root.get_column_bindings();

        let result = binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated scalar");
        let result_bindings = root.get_column_bindings();
        let expected_binding = *result_bindings.last().expect("scalar binding");
        assert_eq!(&result_bindings[..outer_bindings.len()], &outer_bindings);

        match result {
            Expression::ColumnRef(col_ref) => {
                assert_eq!(col_ref.binding, expected_binding);
                assert_ne!(
                    col_ref.binding,
                    ColumnBinding::new(0, result_bindings.len() - 1)
                );
            }
            other => panic!("expected column ref, got {other:?}"),
        }
    }

    #[test]
    fn plan_correlated_exists_subquery_uses_planned_mark_index_binding() {
        let mut binder = test_binder();
        for _ in 0..3 {
            binder.bind_context.generate_table_index();
        }

        let correlated = vec![correlated_column(60, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::Exists,
            expression_get(70, vec![LogicalType::Integer]),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::Boolean,
            ComparisonType::Equal,
        );
        let mut root = expression_get(60, vec![LogicalType::Integer]);

        let result = binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated exists");
        let result_bindings = root.get_column_bindings();
        let expected_binding = *result_bindings.last().expect("mark binding");

        match result {
            Expression::ColumnRef(col_ref) => {
                assert_eq!(col_ref.binding, expected_binding);
                match &root {
                    LogicalOperator::Join(Join::Comparison(join)) => {
                        assert_eq!(join.mark_index, Some(col_ref.binding.table_index));
                        assert_eq!(join.duplicate_eliminated_columns.len(), 1);
                    }
                    other => panic!("expected mark join, got {other:?}"),
                }
            }
            other => panic!("expected column ref, got {other:?}"),
        }
    }

    #[test]
    fn plan_correlated_not_exists_subquery_returns_negated_mark_expression() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(61, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::NotExists,
            expression_get(71, vec![LogicalType::Integer]),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::Boolean,
            ComparisonType::Equal,
        );
        let mut root = expression_get(61, vec![LogicalType::Integer, LogicalType::Varchar]);

        let result = binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated not exists");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert!(matches!(join.join_type, JoinType::Mark));
                let mark_table_index = join.mark_index.expect("planned mark index");
                match result {
                    Expression::Comparison(comp) => {
                        assert!(matches!(comp.comparison_type, ComparisonType::Equal));
                        assert_eq!(
                            extract_binding(&comp.left),
                            Some(ColumnBinding::new(mark_table_index, 0))
                        );
                        assert!(matches!(
                            comp.right.as_ref(),
                            Expression::Constant(ConstantExpression {
                                value: Value::Boolean(false),
                                ..
                            })
                        ));
                    }
                    other => panic!("expected negated mark comparison, got {other:?}"),
                }
            }
            other => panic!("expected mark join root, got {other:?}"),
        }
    }

    #[test]
    fn correlated_scalar_join_tracks_duplicate_eliminated_columns() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(80, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::Scalar,
            expression_get(81, vec![LogicalType::BigInt]),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::BigInt,
            ComparisonType::Equal,
        );
        let mut root = expression_get(80, vec![LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated scalar");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Single);
                assert_eq!(join.duplicate_eliminated_columns.len(), 1);
            }
            other => panic!("expected single join root, got {other:?}"),
        }
    }

    #[test]
    fn correlated_exists_uses_not_distinct_from_for_delim_keys() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(100, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::Exists,
            expression_get(110, vec![LogicalType::Integer]),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::Boolean,
            ComparisonType::Equal,
        );
        let mut root = expression_get(100, vec![LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated exists");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.conditions.len(), 1);
                assert!(matches!(
                    join.conditions[0].comparison,
                    JoinComparisonType::NotDistinctFrom
                ));
            }
            other => panic!("expected mark join, got {other:?}"),
        }
    }

    #[test]
    fn correlated_any_adds_actual_predicate_with_rhs_binding_and_cast() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(120, 0, LogicalType::Integer)];
        let any_left = CastExpression::add_cast_if_needed(
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(120, 1),
                LogicalType::Integer,
            )),
            LogicalType::BigInt,
            binder.cast_functions.as_ref(),
        )
        .expect("cast left predicate");
        let subquery = subquery_expression(
            SubqueryType::Any,
            expression_get(130, vec![LogicalType::Integer]),
            vec![any_left],
            vec![LogicalType::Integer],
            vec![LogicalType::BigInt],
            correlated,
            LogicalType::Boolean,
            ComparisonType::GreaterThan,
        );
        let mut root = expression_get(120, vec![LogicalType::Integer, LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated any");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.conditions.len(), 2);
                assert!(matches!(
                    join.conditions[0].comparison,
                    JoinComparisonType::NotDistinctFrom
                ));
                assert!(matches!(
                    join.conditions[1].comparison,
                    JoinComparisonType::GreaterThan
                ));
                let outer_tbl = expression_get_table_index_root(join.left.as_ref());
                let lb = extract_binding(&join.conditions[1].left).expect("any pred left");
                assert_eq!(lb, ColumnBinding::new(outer_tbl, 1));
                let rb = extract_binding(&join.conditions[1].right).expect("any pred right");
                match &join.right.operator {
                    LogicalOperator::Projection(proj) => {
                        assert_eq!(rb, ColumnBinding::new(proj.table_index, 0));
                        assert_eq!(proj.expressions.len(), 2);
                    }
                    other => panic!("expected compacting projection on ANY rhs, got {other:?}"),
                }
                assert_eq!(join.conditions[1].right.return_type(), LogicalType::BigInt);
            }
            other => panic!("expected mark join, got {other:?}"),
        }
    }

    #[test]
    fn correlated_all_inverts_any_predicate_comparison() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(140, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::All,
            expression_get(150, vec![LogicalType::Integer]),
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(140, 1),
                LogicalType::Integer,
            ))],
            vec![LogicalType::Integer],
            vec![LogicalType::Integer],
            correlated,
            LogicalType::Boolean,
            ComparisonType::GreaterThan,
        );
        let mut root = expression_get(140, vec![LogicalType::Integer, LogicalType::Integer]);

        let result = binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated all");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.conditions.len(), 2);
                assert!(matches!(
                    join.conditions[0].comparison,
                    JoinComparisonType::NotDistinctFrom
                ));
                assert!(matches!(
                    join.conditions[1].comparison,
                    JoinComparisonType::LessThanOrEqual
                ));
                let mark_table_index = join.mark_index.expect("planned mark index");
                match result {
                    Expression::Comparison(comp) => {
                        assert!(matches!(comp.comparison_type, ComparisonType::Equal));
                        assert_eq!(
                            extract_binding(&comp.left),
                            Some(ColumnBinding::new(mark_table_index, 0))
                        );
                    }
                    other => panic!("expected negated mark comparison, got {other:?}"),
                }
            }
            other => panic!("expected mark join for ALL subquery, got {other:?}"),
        }
    }

    #[test]
    fn correlated_exists_pushes_filter_to_delim_cross_product_leaf() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(200, 0, LogicalType::Integer)];
        let filter_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::ColumnRef(ColumnRefExpression::with_depth(
                ColumnBinding::new(200, 0),
                LogicalType::Integer,
                1,
            )),
            Expression::Constant(ConstantExpression {
                value: Value::Integer(10),
                return_type: LogicalType::Integer,
            }),
        ));
        let subquery = subquery_expression(
            SubqueryType::Exists,
            LogicalOperator::Filter(crate::operator::Filter::new(
                wrapped(&binder, expression_get(210, vec![LogicalType::Integer])),
                vec![filter_expr],
            )),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::Boolean,
            ComparisonType::Equal,
        );
        let mut root = expression_get(200, vec![LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated filtered exists");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => match &join.right.operator {
                LogicalOperator::Projection(proj) => {
                    assert_eq!(proj.expressions.len(), 1);
                    assert_eq!(
                        extract_binding(&join.conditions[0].right),
                        Some(ColumnBinding::new(proj.table_index, 0))
                    );
                    let LogicalOperator::Filter(filter) = &proj.child.operator else {
                        panic!(
                            "expected filter below compacting projection, got {:?}",
                            proj.child
                        );
                    };
                    assert_eq!(filter.expressions.len(), 1);
                    match &filter.expressions[0] {
                        Expression::Comparison(comp) => {
                            let rewritten_binding =
                                extract_binding(&comp.left).expect("rewritten correlated binding");
                            let projected_binding = extract_binding(&proj.expressions[0])
                                .expect("projected delimiter binding");
                            assert_eq!(rewritten_binding, projected_binding);
                        }
                        other => panic!("expected rewritten comparison filter, got {other:?}"),
                    }
                    match &filter.child.operator {
                        LogicalOperator::Join(Join::Cross(_)) => {}
                        other => panic!("expected delim cross product under filter, got {other:?}"),
                    }
                }
                other => panic!("expected compacting projection on rhs, got {other:?}"),
            },
            other => panic!("expected mark join root, got {other:?}"),
        }
    }

    #[test]
    fn correlated_exists_projection_appends_delim_columns_and_updates_join_binding() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(220, 0, LogicalType::Integer)];
        let subquery = subquery_expression(
            SubqueryType::Exists,
            LogicalOperator::Projection(Projection::new(
                221,
                wrapped(&binder, expression_get(230, vec![LogicalType::Integer])),
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(230, 0),
                    LogicalType::Integer,
                ))],
            )),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::Boolean,
            ComparisonType::Equal,
        );
        let mut root = expression_get(220, vec![LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated projected exists");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => match &join.right.operator {
                LogicalOperator::Projection(proj) => {
                    assert_eq!(proj.expressions.len(), 1);
                    assert_eq!(proj.returned_types.len(), 1);
                    assert_eq!(
                        extract_binding(&join.conditions[0].right),
                        Some(ColumnBinding::new(proj.table_index, 0))
                    );
                }
                other => panic!("expected projection rhs, got {other:?}"),
            },
            other => panic!("expected mark join root, got {other:?}"),
        }
    }

    #[test]
    fn correlated_scalar_aggregate_adds_correlated_group_key_for_delim_binding() {
        let mut binder = test_binder();
        let first_set = get_first_function();
        let (first_func, _) = first_set
            .bind(&[LogicalType::Integer])
            .expect("bind first aggregate");
        let inner_ref = Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(250, 0),
            LogicalType::Integer,
        ));
        let aggregate = LogicalOperator::Aggregate(Aggregate::new(
            251,
            252,
            253,
            wrapped(&binder, expression_get(250, vec![LogicalType::Integer])),
            Vec::new(),
            Vec::new(),
            vec![Expression::Aggregate(AggregateExpression::new(
                first_func,
                vec![inner_ref],
                LogicalType::Integer,
            ))],
            vec![],
        ));
        let subquery = subquery_expression(
            SubqueryType::Scalar,
            aggregate,
            vec![],
            vec![],
            vec![],
            vec![correlated_column(240, 0, LogicalType::Integer)],
            LogicalType::Integer,
            ComparisonType::Equal,
        );
        let mut root = expression_get(240, vec![LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated scalar aggregate");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => match &join.right.operator {
                LogicalOperator::Aggregate(inner_agg) => {
                    assert_eq!(join.join_type, JoinType::Single);
                    assert_eq!(inner_agg.groups.len(), 1);
                    assert_eq!(
                        extract_binding(&join.conditions[0].right),
                        Some(ColumnBinding::new(inner_agg.group_index, 0))
                    );
                }
                other => panic!("expected aggregate rhs, got {other:?}"),
            },
            other => panic!("expected single join root, got {other:?}"),
        }
    }

    #[test]
    fn correlated_exists_cross_product_pushes_into_correlated_side_only() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(260, 0, LogicalType::Integer)];
        let correlated_projection = LogicalOperator::Projection(Projection::new(
            261,
            wrapped(&binder, expression_get(262, vec![LogicalType::Integer])),
            vec![Expression::ColumnRef(ColumnRefExpression::with_depth(
                ColumnBinding::new(260, 0),
                LogicalType::Integer,
                1,
            ))],
        ));
        let subquery = subquery_expression(
            SubqueryType::Exists,
            LogicalOperator::Join(Join::Cross(CrossProduct::new(
                wrapped(&binder, expression_get(263, vec![LogicalType::Integer])),
                wrapped(&binder, correlated_projection),
            ))),
            vec![],
            vec![],
            vec![],
            correlated,
            LogicalType::Boolean,
            ComparisonType::Equal,
        );
        let mut root = expression_get(260, vec![LogicalType::Integer]);

        binder
            .plan_correlated_subquery(&subquery, &mut root)
            .expect("plan correlated cross-product exists");

        match &root {
            LogicalOperator::Join(Join::Comparison(join)) => match &join.right.operator {
                LogicalOperator::Projection(proj) => {
                    assert_eq!(proj.expressions.len(), 1);
                    assert_eq!(
                        extract_binding(&join.conditions[0].right),
                        Some(ColumnBinding::new(proj.table_index, 0))
                    );
                    let LogicalOperator::Join(Join::Cross(cross)) = &proj.child.operator else {
                        panic!(
                            "expected cross-product child below compacting projection, got {:?}",
                            proj.child
                        );
                    };
                    match &cross.right.operator {
                        LogicalOperator::Projection(proj) => {
                            assert_eq!(proj.expressions.len(), 2);
                        }
                        other => panic!(
                            "expected correlated projection on cross-product rhs, got {other:?}"
                        ),
                    }
                    assert!(matches!(
                        &cross.left.operator,
                        LogicalOperator::ExpressionGet(_)
                    ));
                }
                other => panic!("expected compacting projection on rhs, got {other:?}"),
            },
            other => panic!("expected mark join root, got {other:?}"),
        }
    }

    #[test]
    fn flatten_inner_lateral_join_uses_comparison_join_and_duplicate_elimination() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(300, 0, LogicalType::Integer)];
        let join_condition = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            int_col(300, 0),
            int_col(301, 0),
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(300, vec![LogicalType::Integer])),
            wrapped(&binder, expression_get(301, vec![LogicalType::Integer])),
            correlated,
            JoinType::Inner,
            Some(join_condition),
        );

        let plan =
            flatten_dependent_join(&mut binder, dependent_join).expect("flatten lateral join");

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Inner);
                assert_eq!(join.duplicate_eliminated_columns.len(), 1);
                assert_eq!(join.conditions.len(), 2);
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_lateral_join_hides_correlated_group_keys_from_aggregate_rhs() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(330, 0, LogicalType::Integer)];
        let aggregate = LogicalOperator::Aggregate(Aggregate::new(
            331,
            332,
            333,
            wrapped(&binder, expression_get(334, vec![LogicalType::Integer])),
            vec![int_col(334, 0)],
            Vec::new(),
            Vec::new(),
            vec![],
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(330, vec![LogicalType::Integer])),
            wrapped(&binder, aggregate),
            correlated,
            JoinType::Inner,
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })),
        );

        let plan =
            flatten_dependent_join(&mut binder, dependent_join).expect("flatten aggregate lateral");
        let output_types = plan.types();
        let output_names = plan.output_names();

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.right_projection_map, vec![0]);
                assert_eq!(output_types.len(), 2);
                assert_eq!(output_names.len(), 2);
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_lateral_join_hides_correlated_columns_from_window_rhs() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(340, 0, LogicalType::Integer)];
        let row_number_function = WindowFunction::row_number();
        let window = LogicalOperator::Window(Window::new(
            341,
            vec![WindowExpression {
                function: row_number_function.clone(),
                children: vec![],
                partitions: vec![],
                orders: vec![OrderByExpression {
                    expression: int_col(342, 0),
                    ascending: true,
                    nulls_first: false,
                }],
                frame: WindowFrame::get_default_frame(&row_number_function),
                ignore_nulls: false,
                return_type: LogicalType::BigInt,
            }],
            wrapped(&binder, expression_get(342, vec![LogicalType::Integer])),
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(340, vec![LogicalType::Integer])),
            wrapped(&binder, window),
            correlated,
            JoinType::Inner,
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })),
        );

        let plan =
            flatten_dependent_join(&mut binder, dependent_join).expect("flatten window lateral");
        let output_types = plan.types();
        let output_names = plan.output_names();

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.right_projection_map, vec![0, 2]);
                assert_eq!(output_types.len(), 3);
                assert_eq!(output_names.len(), 3);
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_lateral_join_hides_internal_columns_from_distinct_on_rhs() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(350, 0, LogicalType::Integer)];
        let distinct = LogicalOperator::Distinct(Distinct::distinct_on_with_order(
            vec![int_col(351, 0)],
            vec![OrderByNode {
                expression: int_col(351, 0),
                ascending: true,
                nulls_first: false,
            }],
            wrapped(&binder, expression_get(351, vec![LogicalType::Integer])),
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(350, vec![LogicalType::Integer])),
            wrapped(&binder, distinct),
            correlated,
            JoinType::Inner,
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })),
        );

        let plan =
            flatten_dependent_join(&mut binder, dependent_join).expect("flatten distinct lateral");
        let output_types = plan.types();

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.right_projection_map, vec![0]);
                assert_eq!(output_types.len(), 2);
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_lateral_join_rebases_setop_correlation_keys_to_setop_table_index() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(360, 0, LogicalType::Integer)];
        let setop = LogicalOperator::SetOperation(SetOperation::union(
            361,
            wrapped(&binder, expression_get(362, vec![LogicalType::Integer])),
            wrapped(&binder, expression_get(363, vec![LogicalType::Integer])),
            true,
            vec![LogicalType::Integer],
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(360, vec![LogicalType::Integer])),
            wrapped(&binder, setop),
            correlated,
            JoinType::Inner,
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })),
        );

        let plan =
            flatten_dependent_join(&mut binder, dependent_join).expect("flatten setop lateral");

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.right_projection_map, vec![0]);
                assert_eq!(
                    extract_binding(&join.conditions[0].right),
                    Some(ColumnBinding::new(361, 1))
                );
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_lateral_join_rebases_all_setop_variants() {
        let mut binder = test_binder();
        let variants = [
            (SetOpType::Union, false),
            (SetOpType::Union, true),
            (SetOpType::Intersect, false),
            (SetOpType::Intersect, true),
            (SetOpType::Except, false),
            (SetOpType::Except, true),
        ];

        for (idx, (setop_type, setop_all)) in variants.into_iter().enumerate() {
            let outer_table = 380 + idx * 10;
            let setop_table = 381 + idx * 10;
            let left_table = 382 + idx * 10;
            let right_table = 383 + idx * 10;
            let correlated = vec![correlated_column(outer_table, 0, LogicalType::Integer)];
            let setop = LogicalOperator::SetOperation(SetOperation::new(
                setop_table,
                wrapped(
                    &binder,
                    expression_get(left_table, vec![LogicalType::Integer]),
                ),
                wrapped(
                    &binder,
                    expression_get(right_table, vec![LogicalType::Integer]),
                ),
                setop_type,
                setop_all,
                vec![LogicalType::Integer],
            ));
            let dependent_join = DependentJoin::lateral(
                wrapped(
                    &binder,
                    expression_get(outer_table, vec![LogicalType::Integer]),
                ),
                wrapped(&binder, setop),
                correlated,
                JoinType::Inner,
                Some(Expression::Constant(ConstantExpression {
                    value: Value::Boolean(true),
                    return_type: LogicalType::Boolean,
                })),
            );

            let plan =
                flatten_dependent_join(&mut binder, dependent_join).expect("flatten setop variant");

            match plan {
                LogicalOperator::Join(Join::Comparison(join)) => {
                    assert_eq!(join.right_projection_map, vec![0]);
                    assert_eq!(
                        extract_binding(&join.conditions[0].right),
                        Some(ColumnBinding::new(setop_table, 1)),
                        "unexpected binding for {:?} all={}",
                        setop_type,
                        setop_all
                    );
                }
                other => panic!("expected comparison join, got {other:?}"),
            }
        }
    }

    #[test]
    fn flatten_lateral_join_composes_visible_columns_for_window_over_aggregate() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(370, 0, LogicalType::Integer)];
        let aggregate = Aggregate::new(
            371,
            372,
            373,
            wrapped(&binder, expression_get(374, vec![LogicalType::Integer])),
            vec![int_col(374, 0)],
            Vec::new(),
            Vec::new(),
            vec![],
        );
        let row_number_function = WindowFunction::row_number();
        let window = LogicalOperator::Window(Window::new(
            375,
            vec![WindowExpression {
                function: row_number_function.clone(),
                children: vec![],
                partitions: vec![],
                orders: vec![OrderByExpression {
                    expression: Expression::ColumnRef(ColumnRefExpression::new(
                        ColumnBinding::new(371, 0),
                        LogicalType::Integer,
                    )),
                    ascending: true,
                    nulls_first: false,
                }],
                frame: WindowFrame::get_default_frame(&row_number_function),
                ignore_nulls: false,
                return_type: LogicalType::BigInt,
            }],
            wrapped(&binder, LogicalOperator::Aggregate(aggregate)),
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(370, vec![LogicalType::Integer])),
            wrapped(&binder, window),
            correlated,
            JoinType::Inner,
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })),
        );

        let plan = flatten_dependent_join(&mut binder, dependent_join)
            .expect("flatten window-over-aggregate lateral");
        let output_types = plan.types();

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.right_projection_map, vec![0, 2]);
                assert_eq!(output_types.len(), 3);
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_left_lateral_join_allows_on_true() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(310, 0, LogicalType::Integer)];
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(310, vec![LogicalType::Integer])),
            wrapped(&binder, expression_get(311, vec![LogicalType::Integer])),
            correlated,
            JoinType::Left,
            Some(Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            })),
        );

        let plan = flatten_dependent_join(&mut binder, dependent_join)
            .expect("flatten left lateral join with ON true");

        match plan {
            LogicalOperator::Join(Join::Comparison(join)) => {
                assert_eq!(join.join_type, JoinType::Left);
                assert_eq!(join.conditions.len(), 1);
                assert_eq!(join.duplicate_eliminated_columns.len(), 1);
            }
            other => panic!("expected comparison join, got {other:?}"),
        }
    }

    #[test]
    fn flatten_left_lateral_join_rejects_arbitrary_residuals() {
        let mut binder = test_binder();
        let correlated = vec![correlated_column(320, 0, LogicalType::Integer)];
        let arbitrary_condition = Expression::Operator(OperatorExpression::new_unary(
            OperatorType::Not,
            Expression::Comparison(ComparisonExpression::new(
                ComparisonType::Equal,
                int_col(320, 0),
                int_col(321, 0),
            )),
            LogicalType::Boolean,
        ));
        let dependent_join = DependentJoin::lateral(
            wrapped(&binder, expression_get(320, vec![LogicalType::Integer])),
            wrapped(&binder, expression_get(321, vec![LogicalType::Integer])),
            correlated,
            JoinType::Left,
            Some(arbitrary_condition),
        );

        let error = flatten_dependent_join(&mut binder, dependent_join)
            .expect_err("left lateral join with arbitrary residual must fail");

        assert!(error
            .to_string()
            .contains("non-inner LATERAL JOIN must be a comparison"));
    }
}
