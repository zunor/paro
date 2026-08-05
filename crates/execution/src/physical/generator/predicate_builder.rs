// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Predicate Builder - Build PredicateTree from Expression
//!
//! Shared by plan_topn and plan_filter to push down predicates to RowsetScan.

use std::sync::Arc;

use paro_common::allocator::default_allocator;
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::scalar::cast::{CastContextDependency, CastExecCtx};
use paro_function::scalar::FunctionExecContext;
use paro_planner::expression::{ComparisonType, ConjunctionType, Expression, OperatorType};
use paro_planner::operator::get::Get;
use paro_storage::index::{Predicate, PredicateTree};

pub fn build_predicate_tree(
    filters: &[Expression],
    get: &Get,
) -> Result<(Option<PredicateTree>, Vec<Expression>)> {
    if filters.is_empty() {
        return Ok((None, Vec::new()));
    }

    let mut pushed_trees = Vec::new();
    let mut residual_exprs = Vec::new();

    for expr in filters {
        let (tree, mut residual) = extract_predicate(expr, get)?;
        if let Some(tree) = tree {
            pushed_trees.push(tree);
        }
        residual_exprs.append(&mut residual);
    }

    Ok((combine_with_and(pushed_trees), residual_exprs))
}

fn extract_predicate(
    expr: &Expression,
    get: &Get,
) -> Result<(Option<PredicateTree>, Vec<Expression>)> {
    match expr {
        Expression::Conjunction(conj) if conj.conjunction_type == ConjunctionType::And => {
            let mut pushed_trees = Vec::new();
            let mut residual_exprs = Vec::new();

            for child in &conj.children {
                let (tree, mut residual) = extract_predicate(child, get)?;
                if let Some(tree) = tree {
                    pushed_trees.push(tree);
                }
                residual_exprs.append(&mut residual);
            }

            Ok((combine_with_and(pushed_trees), residual_exprs))
        }
        _ => match build_predicate(expr, get)? {
            Some(tree) => Ok((Some(tree), Vec::new())),
            None => Ok((None, vec![expr.clone()])),
        },
    }
}

pub fn combine_predicate_trees(
    left: Option<PredicateTree>,
    right: Option<PredicateTree>,
) -> Option<PredicateTree> {
    combine_with_and([left, right].into_iter().flatten())
}

fn combine_with_and(trees: impl IntoIterator<Item = PredicateTree>) -> Option<PredicateTree> {
    let mut combined = Vec::new();
    let mut pending = trees.into_iter().collect::<Vec<_>>();
    pending.reverse();
    while let Some(tree) = pending.pop() {
        if let PredicateTree::And(children) = tree {
            pending.extend(children.into_iter().rev());
        } else if !combined.contains(&tree) {
            combined.push(tree);
        }
    }

    match combined.len() {
        0 => None,
        1 => combined.pop(),
        _ => Some(PredicateTree::And(combined)),
    }
}

pub fn build_predicate(expr: &Expression, get: &Get) -> Result<Option<PredicateTree>> {
    match expr {
        Expression::Conjunction(conj) => {
            let mut children = Vec::new();
            for child in &conj.children {
                let Some(tree) = build_predicate(child, get)? else {
                    return Ok(None);
                };
                children.push(tree);
            }
            let tree = match conj.conjunction_type {
                ConjunctionType::And => PredicateTree::And(children),
                ConjunctionType::Or => PredicateTree::Or(children),
            };
            Ok(Some(tree))
        }
        Expression::Comparison(cmp) => build_comparison_predicate(cmp, get),
        Expression::Operator(op) => build_operator_predicate(op, get),
        _ => Ok(None),
    }
}

fn build_comparison_predicate(
    cmp: &paro_planner::expression::ComparisonExpression,
    get: &Get,
) -> Result<Option<PredicateTree>> {
    let left_col = extract_scan_column_index(&cmp.left);
    let right_col = extract_scan_column_index(&cmp.right);

    let (col_idx, value, comparison) = match (left_col, right_col) {
        (Some(col), None) => {
            let Some(value) = extract_constant_value(&cmp.right, get, col)? else {
                return Ok(None);
            };
            (col, value, cmp.comparison_type)
        }
        (None, Some(col)) => {
            let Some(value) = extract_constant_value(&cmp.left, get, col)? else {
                return Ok(None);
            };
            let Some(flipped) = flip_comparison(cmp.comparison_type) else {
                return Ok(None);
            };
            (col, value, flipped)
        }
        _ => return Ok(None),
    };

    let column_id = match get.column_ids.get(col_idx) {
        Some(id) => *id as u32,
        None => return Ok(None),
    };

    let predicate = match comparison {
        ComparisonType::Equal => Predicate::Eq { column_id, value },
        ComparisonType::NotEqual => Predicate::NotEq { column_id, value },
        ComparisonType::LessThan => Predicate::Lt { column_id, value },
        ComparisonType::LessThanOrEqual => Predicate::Le { column_id, value },
        ComparisonType::GreaterThan => Predicate::Gt { column_id, value },
        ComparisonType::GreaterThanOrEqual => Predicate::Ge { column_id, value },
        _ => return Ok(None),
    };

    Ok(Some(PredicateTree::Leaf(predicate)))
}

fn build_operator_predicate(
    op: &paro_planner::expression::OperatorExpression,
    get: &Get,
) -> Result<Option<PredicateTree>> {
    match op.operator_type {
        OperatorType::IsNull | OperatorType::IsNotNull => {
            let child = match op.children.get(0) {
                Some(child) => child,
                None => return Ok(None),
            };
            let col_idx = match extract_scan_column_index(child) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            let column_id = match get.column_ids.get(col_idx) {
                Some(id) => *id as u32,
                None => return Ok(None),
            };
            let predicate = match op.operator_type {
                OperatorType::IsNull => Predicate::IsNull { column_id },
                OperatorType::IsNotNull => Predicate::IsNotNull { column_id },
                _ => return Ok(None),
            };
            Ok(Some(PredicateTree::Leaf(predicate)))
        }
        OperatorType::In => {
            if op.children.len() < 2 {
                return Ok(None);
            }
            let col_idx = match extract_scan_column_index(&op.children[0]) {
                Some(idx) => idx,
                None => return Ok(None),
            };
            let mut values = Vec::with_capacity(op.children.len() - 1);
            for child in &op.children[1..] {
                let Some(value) = extract_constant_value(child, get, col_idx)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            let column_id = match get.column_ids.get(col_idx) {
                Some(id) => *id as u32,
                None => return Ok(None),
            };
            Ok(Some(PredicateTree::Leaf(Predicate::In {
                column_id,
                values,
            })))
        }
        _ => Ok(None),
    }
}

fn extract_constant_value(expr: &Expression, get: &Get, col_idx: usize) -> Result<Option<Value>> {
    let col_type = match get.column_types.get(col_idx) {
        Some(ty) => ty,
        None => return Ok(None),
    };
    if matches!(col_type, LogicalType::Array(_, _)) {
        return Ok(None);
    }

    let Some(value) = evaluate_bound_constant(expr)? else {
        return Ok(None);
    };

    if value.is_null() {
        return Ok(None);
    }

    if value.logical_type() == *col_type {
        return Ok(Some(value));
    }

    match value.cast(col_type) {
        Ok(v) => Ok(Some(v)),
        Err(_) => Ok(None),
    }
}

fn evaluate_bound_constant(expr: &Expression) -> Result<Option<Value>> {
    match expr {
        Expression::Constant(constant) => Ok(Some(constant.value.clone())),
        Expression::Cast(cast) => {
            if cast.cast_info.context_dependency() == CastContextDependency::Runtime {
                return Ok(None);
            }
            let Some(value) = evaluate_bound_constant(cast.child.as_ref())? else {
                return Ok(None);
            };
            if value.is_null() {
                return Ok(None);
            }

            let allocator = Arc::new(default_allocator());
            let mut source = Vector::try_new(value.logical_type(), 1, allocator.clone())?;
            source.set_count(1);
            source.set_value(0, &value);
            let mut result = Vector::try_new(cast.target_type.clone(), 1, allocator)?;
            let ctx = CastExecCtx {
                runtime: &ConstantCastContext,
                try_cast: cast.try_cast,
                cast_data: cast.cast_info.cast_data.as_deref(),
            };
            cast.cast_info.execute(&source, &mut result, 1, &ctx)?;
            let value = result.get_value(0);
            Ok((!value.is_null()).then_some(value))
        }
        _ => Ok(None),
    }
}

struct ConstantCastContext;

impl FunctionExecContext for ConstantCastContext {
    fn current_database(&self) -> Option<&str> {
        None
    }

    fn current_schema(&self) -> Option<&str> {
        None
    }

    fn current_user(&self) -> Option<&str> {
        None
    }
}

fn flip_comparison(cmp: ComparisonType) -> Option<ComparisonType> {
    match cmp {
        ComparisonType::Equal => Some(ComparisonType::Equal),
        ComparisonType::NotEqual => Some(ComparisonType::NotEqual),
        ComparisonType::LessThan => Some(ComparisonType::GreaterThan),
        ComparisonType::LessThanOrEqual => Some(ComparisonType::GreaterThanOrEqual),
        ComparisonType::GreaterThan => Some(ComparisonType::LessThan),
        ComparisonType::GreaterThanOrEqual => Some(ComparisonType::LessThanOrEqual),
        _ => None,
    }
}

pub fn extract_scan_column_index(expr: &Expression) -> Option<usize> {
    match expr {
        Expression::Reference(r) => Some(r.index),
        Expression::ColumnRef(c) => Some(c.binding.column_index),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::scalar::cast::date_casts::{parse_date_text, varchar_to_date};
    use paro_function::scalar::cast::decimal_casts::bind_decimal_casts;
    use paro_function::scalar::cast::{BindCastInput, BoundCastInfo, CastFunctionSet};
    use paro_planner::expression::{CastExpression, ConstantExpression};

    #[test]
    fn combining_predicates_flattens_and_removes_duplicates() {
        let lower = PredicateTree::leaf(Predicate::Ge {
            column_id: 3,
            value: Value::Integer(2),
        });
        let upper = PredicateTree::leaf(Predicate::Le {
            column_id: 3,
            value: Value::Integer(8),
        });
        let combined = combine_predicate_trees(
            Some(PredicateTree::And(vec![lower.clone(), upper.clone()])),
            Some(PredicateTree::And(vec![lower, upper])),
        )
        .unwrap();

        let PredicateTree::And(children) = combined else {
            panic!("expected AND tree");
        };
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn bound_date_constant_is_evaluated_for_scan_pushdown() {
        let expr = Expression::Cast(CastExpression::new(
            Expression::Constant(ConstantExpression::new(
                Value::Varchar("1994-01-01".to_string()),
                LogicalType::Varchar,
            )),
            LogicalType::Date,
            BoundCastInfo::fixed(varchar_to_date),
            false,
        ));

        assert_eq!(
            evaluate_bound_constant(&expr).unwrap(),
            Some(Value::Date(parse_date_text("1994-01-01").unwrap()))
        );
    }

    #[test]
    fn bound_decimal_constant_is_rescaled_for_scan_pushdown() {
        let source_type = LogicalType::Decimal {
            precision: 2,
            scale: 2,
        };
        let target_type = LogicalType::Decimal {
            precision: 15,
            scale: 2,
        };
        let cast_functions = CastFunctionSet::new();
        let cast_info = bind_decimal_casts(
            &BindCastInput::new(&cast_functions),
            &source_type,
            &target_type,
        )
        .unwrap()
        .unwrap();
        let expr = Expression::Cast(CastExpression::new(
            Expression::Constant(ConstantExpression::new(
                Value::Decimal(5, 2, 2),
                source_type,
            )),
            target_type,
            cast_info,
            false,
        ));

        assert_eq!(
            evaluate_bound_constant(&expr).unwrap(),
            Some(Value::Decimal(5, 15, 2))
        );
    }

    #[test]
    fn runtime_context_dependent_cast_is_not_folded_for_scan_pushdown() {
        let expr = Expression::Cast(CastExpression::new(
            Expression::Constant(ConstantExpression::new(
                Value::Varchar("session-dependent".to_string()),
                LogicalType::Varchar,
            )),
            LogicalType::Date,
            BoundCastInfo::fixed(varchar_to_date).requiring_runtime_context(),
            false,
        ));

        assert_eq!(evaluate_bound_constant(&expr).unwrap(), None);
    }
}
