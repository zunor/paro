// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binds subquery expressions: scalar, `EXISTS`, `IN`, `ANY`/`ALL`.

use crate::binder::plan::subquery::{
    split_child_correlated_columns, CorrelationBoundaryMode, CorrelationProjectionMode,
};
use crate::binder::Binder;
use crate::expression::{
    CastExpression, ComparisonType, ConstantExpression, Expression, OperatorExpression,
    OperatorType, SubqueryExpression, SubqueryPlanningState, SubqueryType,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::{Query, Statement};
use std::sync::Arc;

fn should_extract_struct_children(child: &Expression, subquery_types: &[LogicalType]) -> bool {
    if !matches!(child.return_type(), LogicalType::Struct(_)) {
        return false;
    }
    let Expression::Operator(OperatorExpression {
        operator_type: OperatorType::StructConstructor,
        children,
        ..
    }) = child
    else {
        return false;
    };
    if subquery_types.len() == 1
        && matches!(subquery_types.first(), Some(LogicalType::Struct(_)))
        && children.len() != subquery_types.len()
    {
        return false;
    }

    true
}

fn extract_subquery_children(child: Expression, subquery_types: &[LogicalType]) -> Vec<Expression> {
    if should_extract_struct_children(&child, subquery_types) {
        match child {
            Expression::Operator(OperatorExpression { children, .. }) => children,
            _ => unreachable!(),
        }
    } else if let Expression::Constant(ConstantExpression {
        value: Value::Struct(children, fields),
        ..
    }) = child
    {
        // Tuple constants get constant-folded during binding. Re-expand them here so
        // multi-column IN/ANY can still line up with subquery output columns.
        if subquery_types.len() == 1
            && matches!(subquery_types.first(), Some(LogicalType::Struct(_)))
        {
            vec![Expression::Constant(ConstantExpression {
                value: Value::Struct(children, fields.clone()),
                return_type: LogicalType::Struct(fields),
            })]
        } else {
            children
                .into_iter()
                .zip(fields)
                .map(|(value, (_name, ty))| {
                    Expression::Constant(ConstantExpression {
                        value,
                        return_type: ty,
                    })
                })
                .collect()
        }
    } else {
        vec![child]
    }
}

/// Bind a subquery expression.
pub fn bind_subquery_expression(
    binder: &mut Binder,
    subquery: Query,
    subquery_type: SubqueryType,
    child: Option<Expression>,
    comparison_type: Option<ComparisonType>,
) -> Result<Expression> {
    // 1. Create a child binder
    let mut child_binder = binder.create_child();

    // 2. Bind the subquery
    let bound_node = child_binder.with_delayed_subquery_planning_disabled(|binder| {
        binder.bind(Statement::Query(Box::new(subquery)))
    })?;
    let bind_snapshot = child_binder.bind_context.snapshot();

    // Collect correlated columns before dropping the child binder
    let correlated_columns_from_child = std::mem::take(&mut child_binder.correlated_columns);
    drop(child_binder);

    // 3. Determine return type, child extraction, and validate column counts
    let return_types = bound_node.types();
    let mut children = if let Some(c) = child {
        extract_subquery_children(c, &return_types)
    } else {
        Vec::new()
    };

    if subquery_type != SubqueryType::Exists && subquery_type != SubqueryType::NotExists {
        let expected_columns = if children.is_empty() {
            1
        } else {
            children.len()
        };
        if return_types.len() != expected_columns {
            return Err(paro_error::syntax(format!(
                "Subquery returns {} columns - expected {}",
                return_types.len(),
                expected_columns
            )));
        }
    }

    let return_type = match subquery_type {
        SubqueryType::Scalar => return_types
            .first()
            .cloned()
            .unwrap_or(LogicalType::Integer),
        SubqueryType::Exists | SubqueryType::NotExists | SubqueryType::Any | SubqueryType::All => {
            LogicalType::Boolean
        }
    };

    // 4. Propagate correlated columns and collect for the expression
    let split = split_child_correlated_columns(
        correlated_columns_from_child,
        CorrelationBoundaryMode::ScopeBoundary,
    );
    let correlated_columns =
        split.projected_correlations(CorrelationProjectionMode::IncludeAllPropagated);
    binder.correlated_columns.extend(split.propagate_to_parent);

    // 5. Handle ANY/ALL/IN type alignment and preserve original RHS types.
    let mut child_types = Vec::new();
    let mut child_targets = Vec::new();
    if matches!(subquery_type, SubqueryType::Any | SubqueryType::All) {
        for (idx, child_expr) in children.iter_mut().enumerate() {
            let child_sql_type = child_expr.get_expression_return_type();
            let subquery_child_type = return_types[idx].clone();
            let target_type = LogicalType::max_logical_type(&child_sql_type, &subquery_child_type)
                .normalize_type();

            if target_type == LogicalType::Unknown {
                return Err(paro_error::syntax(format!(
                    "Cannot compare values of type {} and {} in IN/ANY/ALL clause - an explicit cast is required",
                    child_sql_type, subquery_child_type
                )));
            }

            if child_expr.return_type() != target_type {
                *child_expr = CastExpression::add_cast_if_needed(
                    child_expr.clone(),
                    target_type.clone(),
                    &binder.cast_functions,
                )?;
            }

            child_types.push(subquery_child_type);
            child_targets.push(target_type);
        }
    }

    let comparison = comparison_type.unwrap_or(ComparisonType::Equal);

    Ok(Expression::Subquery(SubqueryExpression {
        subquery_type,
        subquery: Arc::new(bound_node),
        children,
        child_types,
        child_targets,
        comparison_type: comparison,
        return_type,
        correlated_columns,
        bind_snapshot,
        planning_state: SubqueryPlanningState::Unplanned,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::bind::expr;
    use crate::binder::test_utils::test_binder;
    use crate::expression::{CastExpression, ColumnRefExpression};
    use paro_common::types::LogicalType;

    fn parse_expr_sql(sql: &str) -> paro_parser::ast::Expr {
        let tokens = paro_parser::tokenize_sql(sql).expect("tokenize");
        paro_parser::parse_expr_tokens(&tokens).expect("parse expr")
    }

    #[test]
    fn comparison_with_any_subquery_uses_subquery_metadata() {
        let mut binder = test_binder();
        let bound =
            expr::bind_expression(&mut binder, parse_expr_sql("1 > ANY (SELECT 1)")).expect("bind");

        match bound {
            Expression::Subquery(subquery) => {
                assert_eq!(subquery.subquery_type, SubqueryType::Any);
                assert_eq!(subquery.comparison_type, ComparisonType::GreaterThan);
                assert_eq!(subquery.children.len(), 1);
            }
            other => panic!("expected subquery expression, got {other:?}"),
        }
    }

    #[test]
    fn nested_child_binder_resolves_grandparent_columns_with_depth_two() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "outer_tbl".to_string(),
            42,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let child = binder.create_child();
        let mut grandchild = child.create_child();
        let bound = expr::bind_expression(&mut grandchild, parse_expr_sql("outer_tbl.x"))
            .expect("bind correlated column");

        match bound {
            Expression::ColumnRef(ColumnRefExpression { depth, .. }) => {
                assert_eq!(depth, 2);
            }
            other => panic!("expected column ref, got {other:?}"),
        }
        assert_eq!(grandchild.correlated_columns.len(), 1);
        assert_eq!(grandchild.correlated_columns[0].depth, 2);
    }

    #[test]
    fn in_subquery_extracts_tuple_children_and_records_cast_targets() {
        let mut binder = test_binder();
        let bound =
            expr::bind_expression(&mut binder, parse_expr_sql("(1, 'a') IN (SELECT 1.0, 'a')"))
                .expect("bind");

        match bound {
            Expression::Subquery(subquery) => {
                assert_eq!(subquery.subquery_type, SubqueryType::Any);
                assert_eq!(subquery.children.len(), 2);
                assert_eq!(subquery.child_types.len(), 2);
                assert_eq!(subquery.child_targets.len(), 2);
                assert_eq!(
                    subquery.child_targets[0],
                    LogicalType::Decimal {
                        precision: 2,
                        scale: 1
                    }
                );
                assert_eq!(subquery.child_targets[1], LogicalType::Varchar);
                assert!(matches!(
                    subquery.children[0],
                    Expression::Cast(CastExpression { .. })
                ));
            }
            other => panic!("expected subquery expression, got {other:?}"),
        }
    }

    #[test]
    fn correlated_subquery_keeps_scope_metadata() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "outer_tbl".to_string(),
            7,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let bound = expr::bind_expression(&mut binder, parse_expr_sql("(SELECT outer_tbl.x)"))
            .expect("bind");

        match bound {
            Expression::Subquery(subquery) => {
                assert_eq!(subquery.subquery_type, SubqueryType::Scalar);
                assert_eq!(subquery.correlated_columns.len(), 1);
                assert!(subquery.bind_snapshot.has_parent());
            }
            other => panic!("expected scalar subquery, got {other:?}"),
        }
    }

    #[test]
    fn nested_correlated_subquery_preserves_outer_scope_metadata() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "outer_tbl".to_string(),
            7,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let bound = expr::bind_expression(
            &mut binder,
            parse_expr_sql("(SELECT EXISTS(SELECT 1 WHERE outer_tbl.x = 1))"),
        )
        .expect("bind nested correlated subquery");

        match bound {
            Expression::Subquery(subquery) => {
                assert_eq!(subquery.subquery_type, SubqueryType::Scalar);
                assert_eq!(subquery.correlated_columns.len(), 1);
                assert_eq!(subquery.correlated_columns[0].table_index, 7);
                assert_eq!(subquery.correlated_columns[0].depth, 1);
            }
            other => panic!("expected scalar subquery, got {other:?}"),
        }
    }
}
