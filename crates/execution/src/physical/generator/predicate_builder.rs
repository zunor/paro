// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Predicate Builder - Build PredicateTree from Expression
//!
//! Shared by plan_topn and plan_filter to push down predicates to RowsetScan.

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
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
        match build_predicate(expr, get)? {
            Some(tree) => pushed_trees.push(tree),
            None => residual_exprs.push(expr.clone()),
        }
    }

    let tree = if pushed_trees.is_empty() {
        None
    } else if pushed_trees.len() == 1 {
        Some(pushed_trees.remove(0))
    } else {
        Some(PredicateTree::And(pushed_trees))
    };

    Ok((tree, residual_exprs))
}

pub fn combine_predicate_trees(
    left: Option<PredicateTree>,
    right: Option<PredicateTree>,
) -> Option<PredicateTree> {
    match (left, right) {
        (Some(left), Some(right)) => Some(PredicateTree::And(vec![left, right])),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
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

    let value = match expr {
        Expression::Constant(c) => c.value.clone(),
        Expression::Cast(cast) => {
            if let Expression::Constant(c) = cast.child.as_ref() {
                c.value.clone()
            } else {
                return Ok(None);
            }
        }
        _ => return Ok(None),
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
