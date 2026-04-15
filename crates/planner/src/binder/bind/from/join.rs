//! Join TableRef Binder
//!
//!
//!
//! ## Supported
//! - INNER JOIN
//! - LEFT [OUTER] JOIN
//! - RIGHT [OUTER] JOIN
//! - FULL [OUTER] JOIN
//! - CROSS JOIN
//! - ON condition
//! - USING clause
//!
//! ## Not Supported Yet
//! - NATURAL JOIN
//! - SEMI/ANTI JOIN
//! - LATERAL table functions
//! - FULL/RIGHT LATERAL JOIN
use crate::binder::bind::expr;
use crate::binder::ir::{BoundFromItem, BoundJoin, JoinType};
use crate::binder::{Binder, CorrelatedColumnInfo};
use crate::expression::*;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{Join, JoinCondition, JoinOperator};

/// Bind a JOIN clause.
///
/// This function handles the binding of JOIN operations in FROM clauses.
/// It recursively binds left and right table references, then binds the
/// join condition in the combined context.
pub fn bind_join(binder: &mut Binder, join: Join) -> Result<BoundFromItem> {
    // 1. Convert JoinOperator to JoinType
    let join_type = extract_join_type(&join.op)?;

    // 2. Bind the left side table reference
    let left = binder.bind_table_ref((*join.left).clone())?;

    // 3. Bind the right side table reference
    let right = binder.bind_table_ref((*join.right).clone())?;

    let (lateral, correlated_columns) = extract_lateral_metadata(&right);
    if lateral
        && !matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::Cross
        )
    {
        return Err(paro_error::syntax(
            "The combining JOIN type must be INNER, CROSS, or LEFT for a LATERAL reference",
        ));
    }
    let condition = bind_join_condition(binder, &join.condition, &left, &right, join_type)?;

    // SEMI/ANTI joins only expose the preserved side to the outer query scope.
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => {
            let hidden_indices = collect_tableref_indices(&right);
            binder
                .bind_context
                .remove_bindings_by_index(&hidden_indices);
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            let hidden_indices = collect_tableref_indices(&left);
            binder
                .bind_context
                .remove_bindings_by_index(&hidden_indices);
        }
        _ => {}
    }

    // 5. Create the bound join reference
    Ok(BoundFromItem::Join(BoundJoin {
        left: Box::new(left),
        right: Box::new(right),
        condition,
        join_type,
        lateral,
        correlated_columns,
    }))
}

fn extract_lateral_metadata(table_ref: &BoundFromItem) -> (bool, Vec<CorrelatedColumnInfo>) {
    match table_ref {
        BoundFromItem::Subquery(subquery) => {
            let is_lateral = subquery.lateral
                && subquery
                    .correlated_columns
                    .iter()
                    .any(|corr| corr.depth == 1);
            (is_lateral, subquery.correlated_columns.clone())
        }
        _ => (false, Vec::new()),
    }
}

fn collect_tableref_indices(table_ref: &BoundFromItem) -> Vec<usize> {
    let mut indices = Vec::new();
    collect_tableref_indices_recursive(table_ref, &mut indices);
    indices
}

fn collect_tableref_indices_recursive(table_ref: &BoundFromItem, indices: &mut Vec<usize>) {
    match table_ref {
        BoundFromItem::BaseTable(base) => indices.push(base.table_index),
        BoundFromItem::Subquery(subquery) => indices.push(subquery.subquery_index),
        BoundFromItem::TableFunction(function) => indices.push(function.table_index),
        BoundFromItem::CTE(cte) => indices.push(cte.table_index),
        BoundFromItem::GraphTable(graph) => indices.push(graph.table_index),
        BoundFromItem::Join(join) => {
            collect_tableref_indices_recursive(&join.left, indices);
            collect_tableref_indices_recursive(&join.right, indices);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder;
    use paro_parser::{ast::Statement, parse_one};

    #[test]
    fn right_join_lateral_is_rejected() {
        let mut binder = test_binder();
        let statement = parse_one(
            "SELECT * FROM (SELECT 1 AS x) t RIGHT JOIN LATERAL (SELECT t.x AS y) s ON true",
        )
        .expect("parse")
        .stmt;

        let error = binder
            .bind(statement)
            .expect_err("RIGHT JOIN LATERAL must fail");
        assert!(error
            .to_string()
            .contains("must be INNER, CROSS, or LEFT for a LATERAL reference"));
    }

    #[test]
    fn inner_join_lateral_marks_bound_join_as_lateral() {
        let mut binder = test_binder();
        let statement =
            parse_one("SELECT * FROM (SELECT 1 AS x) t JOIN LATERAL (SELECT t.x AS y) s ON true")
                .expect("parse")
                .stmt;

        let Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        let bound = binder.bind_query(*query).expect("bind");
        let from_table = match bound {
            crate::binder::ir::BoundQuery::Select(select) => select.from_table,
            other => panic!("expected select node, got {other:?}"),
        }
        .expect("from table");

        match from_table {
            BoundFromItem::Join(join) => {
                assert!(join.lateral);
                assert_eq!(join.correlated_columns.len(), 1);
                assert_eq!(join.join_type, JoinType::Inner);
            }
            other => panic!("expected bound join, got {other:?}"),
        }
    }

    #[test]
    fn cross_join_lateral_marks_bound_join_as_lateral() {
        let mut binder = test_binder();
        let statement =
            parse_one("SELECT * FROM (SELECT 1 AS x) t CROSS JOIN LATERAL (SELECT t.x AS y) s")
                .expect("parse")
                .stmt;

        let Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        let bound = binder.bind_query(*query).expect("bind");
        let from_table = match bound {
            crate::binder::ir::BoundQuery::Select(select) => select.from_table,
            other => panic!("expected select node, got {other:?}"),
        }
        .expect("from table");

        match from_table {
            BoundFromItem::Join(join) => {
                assert!(join.lateral);
                assert_eq!(join.correlated_columns.len(), 1);
                assert_eq!(join.join_type, JoinType::Cross);
            }
            other => panic!("expected bound join, got {other:?}"),
        }
    }
}

/// Extract JoinType from parser's JoinOperator.
fn extract_join_type(operator: &JoinOperator) -> Result<JoinType> {
    match operator {
        JoinOperator::Inner => Ok(JoinType::Inner),
        JoinOperator::LeftOuter => Ok(JoinType::Left),
        JoinOperator::RightOuter => Ok(JoinType::Right),
        JoinOperator::FullOuter => Ok(JoinType::Full),
        JoinOperator::CrossJoin => Ok(JoinType::Cross),
        JoinOperator::LeftSemi => Ok(JoinType::LeftSemi),
        JoinOperator::RightSemi => Ok(JoinType::RightSemi),
        JoinOperator::LeftAnti => Ok(JoinType::LeftAnti),
        JoinOperator::RightAnti => Ok(JoinType::RightAnti),
        JoinOperator::Asof | JoinOperator::LeftAsof | JoinOperator::RightAsof => {
            Err(paro_error::not_implemented("ASOF JOIN"))
        }
        JoinOperator::InnerAny | JoinOperator::LeftAny | JoinOperator::RightAny => {
            Err(paro_error::not_implemented("ANY JOIN"))
        }
    }
}

/// Bind the join condition (ON or USING clause).
fn bind_join_condition(
    binder: &mut Binder,
    condition: &JoinCondition,
    left: &BoundFromItem,
    right: &BoundFromItem,
    join_type: JoinType,
) -> Result<Option<Expression>> {
    match condition {
        JoinCondition::On(expr) => {
            // Bind the ON expression in the current context
            // (which now includes both left and right table bindings)
            let bound_expr = expr::bind_expression(binder, (**expr).clone())?;

            // Verify the condition returns boolean
            if bound_expr.return_type() != paro_common::types::LogicalType::Boolean {
                return Err(paro_error::syntax(format!(
                    "JOIN ON condition must be boolean, got {}",
                    bound_expr.return_type()
                )));
            }

            Ok(Some(bound_expr))
        }
        JoinCondition::Using(columns) => {
            // USING clause: create equality conditions for each column
            bind_using_clause(binder, columns, left, right)
        }
        JoinCondition::Natural => {
            // NATURAL JOIN: find common columns and create USING-like condition
            Err(paro_error::not_implemented("NATURAL JOIN"))
        }
        JoinCondition::None => {
            // No constraint - valid for CROSS JOIN
            if join_type == JoinType::Cross {
                Ok(None)
            } else {
                // For other join types, no condition means CROSS JOIN behavior
                // This is technically valid SQL but unusual
                Ok(None)
            }
        }
    }
}

/// Bind USING clause by creating equality conditions.
///
/// USING(col1, col2) is equivalent to ON left.col1 = right.col1 AND left.col2 = right.col2
fn bind_using_clause(
    binder: &mut Binder,
    columns: &[paro_parser::ast::Identifier],
    left: &BoundFromItem,
    right: &BoundFromItem,
) -> Result<Option<Expression>> {
    if columns.is_empty() {
        return Err(paro_error::syntax("USING clause cannot be empty"));
    }

    let mut conditions = Vec::new();

    for col_ident in columns {
        let col_str = col_ident.name.clone();

        // Find column in left table
        let left_col = find_column_in_tableref(binder, left, &col_str)?;

        // Find column in right table
        let right_col = find_column_in_tableref(binder, right, &col_str)?;

        // Create equality condition
        let eq_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(left_col),
            Expression::ColumnRef(right_col),
        ));

        conditions.push(eq_expr);
    }

    // Combine all conditions with AND
    if conditions.len() == 1 {
        Ok(Some(conditions.pop().unwrap()))
    } else {
        Ok(Some(Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: conditions,
        })))
    }
}

/// Find a column by name in a BoundFromItem.
fn find_column_in_tableref(
    binder: &mut Binder,
    table_ref: &BoundFromItem,
    column_name: &str,
) -> Result<ColumnRefExpression> {
    use paro_common::types::LogicalType;

    // Helper to create ColumnRefExpression using new constructor
    fn create_col_ref(
        _binder: &mut Binder,
        table_index: usize,
        column_index: usize,
        return_type: LogicalType,
    ) -> ColumnRefExpression {
        use crate::operator::ColumnBinding;
        ColumnRefExpression::new(ColumnBinding::new(table_index, column_index), return_type)
    }

    match table_ref {
        BoundFromItem::BaseTable(base) => {
            // Find the binding for this table
            if let Some(binding) = binder.bind_context.find_binding_by_index(base.table_index) {
                for (col_idx, col_name_in_table) in binding.column_names.iter().enumerate() {
                    if col_name_in_table.eq_ignore_ascii_case(column_name) {
                        return Ok(create_col_ref(
                            binder,
                            base.table_index,
                            col_idx,
                            binding.column_types[col_idx].clone(),
                        ));
                    }
                }
            }
            Err(paro_error::syntax(format!(
                "Column '{}' not found in table",
                column_name
            )))
        }
        BoundFromItem::Join(join_ref) => {
            // Try left first, then right
            if let Ok(col) = find_column_in_tableref(binder, &join_ref.left, column_name) {
                return Ok(col);
            }
            find_column_in_tableref(binder, &join_ref.right, column_name)
        }
        BoundFromItem::Subquery(sub_ref) => {
            // Find the column in the subquery's binding
            for (col_idx, col_name_in_sub) in sub_ref.column_names.iter().enumerate() {
                if col_name_in_sub.eq_ignore_ascii_case(column_name) {
                    return Ok(create_col_ref(
                        binder,
                        sub_ref.subquery_index,
                        col_idx,
                        sub_ref.column_types[col_idx].clone(),
                    ));
                }
            }
            Err(paro_error::syntax(format!(
                "Column '{}' not found in subquery '{}'",
                column_name, sub_ref.alias
            )))
        }
        BoundFromItem::TableFunction(tf_ref) => {
            // Find the column in the table function's binding
            for (col_idx, col_name_in_tf) in tf_ref.column_names.iter().enumerate() {
                if col_name_in_tf.eq_ignore_ascii_case(column_name) {
                    return Ok(create_col_ref(
                        binder,
                        tf_ref.table_index,
                        col_idx,
                        tf_ref.column_types[col_idx].clone(),
                    ));
                }
            }
            Err(paro_error::syntax(format!(
                "Column '{}' not found in table function '{}'",
                column_name, tf_ref.alias
            )))
        }
        BoundFromItem::CTE(cte_ref) => {
            // Find the column in the CTE's binding
            for (col_idx, col_name_in_cte) in cte_ref.column_names.iter().enumerate() {
                if col_name_in_cte.eq_ignore_ascii_case(column_name) {
                    return Ok(create_col_ref(
                        binder,
                        cte_ref.table_index,
                        col_idx,
                        cte_ref.column_types[col_idx].clone(),
                    ));
                }
            }
            Err(paro_error::syntax(format!(
                "Column '{}' not found in CTE '{}'",
                column_name, cte_ref.alias
            )))
        }
        BoundFromItem::GraphTable(graph_ref) => {
            for (col_idx, col_name_in_graph) in graph_ref.output_names.iter().enumerate() {
                if col_name_in_graph.eq_ignore_ascii_case(column_name) {
                    return Ok(create_col_ref(
                        binder,
                        graph_ref.table_index,
                        col_idx,
                        graph_ref.output_types[col_idx].clone(),
                    ));
                }
            }
            Err(paro_error::syntax(format!(
                "Column '{}' not found in GRAPH_TABLE output",
                column_name
            )))
        }
    }
}
