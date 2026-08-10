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
use paro_storage::index::{Predicate, PredicateComparison, PredicateTree};

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
    let left_col = extract_scan_column_operand(&cmp.left);
    let right_col = extract_scan_column_operand(&cmp.right);

    if let (Some(left_col), Some(right_col)) = (left_col, right_col) {
        let (ScanColumnTransform::Identity, ScanColumnTransform::Identity) =
            (left_col.transform, right_col.transform)
        else {
            return Ok(None);
        };
        return build_column_comparison_predicate(
            cmp.comparison_type,
            get,
            left_col.column_idx,
            right_col.column_idx,
        );
    }

    let (col_idx, value, comparison) = match (left_col, right_col) {
        (Some(col), None) => {
            let Some(value) =
                extract_comparison_constant(&cmp.right, get, col, cmp.comparison_type)?
            else {
                return Ok(None);
            };
            (col.column_idx, value, cmp.comparison_type)
        }
        (None, Some(col)) => {
            let Some(flipped) = flip_comparison(cmp.comparison_type) else {
                return Ok(None);
            };
            let Some(value) = extract_comparison_constant(&cmp.left, get, col, flipped)? else {
                return Ok(None);
            };
            (col.column_idx, value, flipped)
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

#[derive(Debug, Clone, Copy)]
struct ScanColumnOperand {
    column_idx: usize,
    transform: ScanColumnTransform,
}

#[derive(Debug, Clone, Copy)]
enum ScanColumnTransform {
    Identity,
    /// SQL DATE values are represented by midnight timestamps under this
    /// widening cast. A timestamp constant can be mapped back only when it is
    /// exactly representable as a DATE; otherwise the comparison stays in the
    /// execution layer.
    DateToTimestamp,
}

fn extract_scan_column_operand(expr: &Expression) -> Option<ScanColumnOperand> {
    if let Some(column_idx) = extract_scan_column_index(expr) {
        return Some(ScanColumnOperand {
            column_idx,
            transform: ScanColumnTransform::Identity,
        });
    }
    let Expression::Cast(cast) = expr else {
        return None;
    };
    if cast.cast_info.context_dependency() != CastContextDependency::Independent
        || cast.child.return_type() != LogicalType::Date
        || cast.target_type != LogicalType::Timestamp
    {
        return None;
    }
    Some(ScanColumnOperand {
        column_idx: extract_scan_column_index(cast.child.as_ref())?,
        transform: ScanColumnTransform::DateToTimestamp,
    })
}

fn extract_comparison_constant(
    expr: &Expression,
    get: &Get,
    operand: ScanColumnOperand,
    _comparison: ComparisonType,
) -> Result<Option<Value>> {
    match operand.transform {
        ScanColumnTransform::Identity => extract_constant_value(expr, get, operand.column_idx),
        ScanColumnTransform::DateToTimestamp => {
            const MICROS_PER_DAY: i64 = 86_400_000_000;
            if get.column_types.get(operand.column_idx) != Some(&LogicalType::Date) {
                return Ok(None);
            }
            let Some(Value::Timestamp(timestamp)) = evaluate_bound_constant(expr)? else {
                return Ok(None);
            };
            if timestamp.rem_euclid(MICROS_PER_DAY) != 0 {
                return Ok(None);
            }
            let Ok(days) = i32::try_from(timestamp.div_euclid(MICROS_PER_DAY)) else {
                return Ok(None);
            };
            Ok(Some(Value::Date(days)))
        }
    }
}

fn build_column_comparison_predicate(
    comparison: ComparisonType,
    get: &Get,
    left_col: usize,
    right_col: usize,
) -> Result<Option<PredicateTree>> {
    let (Some(left_type), Some(right_type)) = (
        get.column_types.get(left_col),
        get.column_types.get(right_col),
    ) else {
        return Ok(None);
    };
    if left_type != right_type || !supports_raw_column_comparison(left_type) {
        return Ok(None);
    }
    let comparison = match comparison {
        ComparisonType::Equal => PredicateComparison::Equal,
        ComparisonType::NotEqual => PredicateComparison::NotEqual,
        ComparisonType::LessThan => PredicateComparison::LessThan,
        ComparisonType::LessThanOrEqual => PredicateComparison::LessThanOrEqual,
        ComparisonType::GreaterThan => PredicateComparison::GreaterThan,
        ComparisonType::GreaterThanOrEqual => PredicateComparison::GreaterThanOrEqual,
        ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom => return Ok(None),
    };
    let (Some(left_column_id), Some(right_column_id)) =
        (get.column_ids.get(left_col), get.column_ids.get(right_col))
    else {
        return Ok(None);
    };
    Ok(Some(PredicateTree::Leaf(Predicate::ColumnComparison {
        left_column_id: *left_column_id as u32,
        right_column_id: *right_column_id as u32,
        comparison,
    })))
}

fn supports_raw_column_comparison(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Date
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::Timestamp
            | LogicalType::TimestampTz
            | LogicalType::Time
            | LogicalType::Decimal { .. }
            | LogicalType::Interval
            | LogicalType::Uuid
    )
}

fn build_operator_predicate(
    op: &paro_planner::expression::OperatorExpression,
    get: &Get,
) -> Result<Option<PredicateTree>> {
    match op.operator_type {
        OperatorType::Like => build_like_prefix_predicate(op, get, false),
        OperatorType::Not => {
            let Some(Expression::Operator(child)) = op.children.first() else {
                return Ok(None);
            };
            if child.operator_type != OperatorType::Like || op.children.len() != 1 {
                return Ok(None);
            }
            build_like_prefix_predicate(child, get, true)
        }
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

fn build_like_prefix_predicate(
    op: &paro_planner::expression::OperatorExpression,
    get: &Get,
    negated: bool,
) -> Result<Option<PredicateTree>> {
    let [value, pattern] = op.children.as_slice() else {
        return Ok(None);
    };
    let Some(col_idx) = extract_scan_column_index(value) else {
        return Ok(None);
    };
    if get.column_types.get(col_idx) != Some(&LogicalType::Varchar) {
        return Ok(None);
    }
    let Some(Value::Varchar(pattern)) = evaluate_bound_constant(pattern)? else {
        return Ok(None);
    };
    let Some(prefix) = extract_like_prefix(&pattern) else {
        return Ok(None);
    };
    let Some(column_id) = get.column_ids.get(col_idx) else {
        return Ok(None);
    };
    Ok(Some(PredicateTree::leaf(Predicate::StringPrefix {
        column_id: *column_id as u32,
        prefix,
        negated,
    })))
}

/// Return the literal prefix when a LIKE pattern consists only of literal
/// characters followed by one or more unescaped `%` wildcards.
fn extract_like_prefix(pattern: &str) -> Option<String> {
    let mut chars = pattern.chars();
    let mut prefix = String::with_capacity(pattern.len());
    let mut saw_suffix_wildcard = false;
    while let Some(token) = chars.next() {
        match token {
            '\\' => {
                let literal = chars.next().unwrap_or('\\');
                if saw_suffix_wildcard {
                    return None;
                }
                prefix.push(literal);
            }
            '%' => saw_suffix_wildcard = true,
            '_' => return None,
            literal => {
                if saw_suffix_wildcard {
                    return None;
                }
                prefix.push(literal);
            }
        }
    }
    saw_suffix_wildcard.then_some(prefix)
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

pub(crate) fn evaluate_bound_constant(expr: &Expression) -> Result<Option<Value>> {
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
    use paro_function::scalar::cast::date_casts::{
        date_to_timestamp, parse_date_text, varchar_to_date,
    };
    use paro_function::scalar::cast::decimal_casts::bind_decimal_casts;
    use paro_function::scalar::cast::{BindCastInput, BoundCastInfo, CastFunctionSet};
    use paro_planner::expression::{
        CastExpression, ConstantExpression, OperatorExpression, ReferenceExpression,
    };
    use paro_planner::operator::Get;

    #[test]
    fn fixed_width_column_comparison_is_pushed_to_storage() {
        let get = Get::new_without_table(
            7,
            vec!["commit_date".to_string(), "receipt_date".to_string()],
            vec![LogicalType::Date, LogicalType::Date],
        );
        let expression =
            Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
                ComparisonType::LessThan,
                Expression::Reference(paro_planner::expression::ReferenceExpression::new(
                    0,
                    LogicalType::Date,
                )),
                Expression::Reference(paro_planner::expression::ReferenceExpression::new(
                    1,
                    LogicalType::Date,
                )),
            ));

        let predicate = build_predicate(&expression, &get).unwrap();

        assert_eq!(
            predicate,
            Some(PredicateTree::leaf(Predicate::ColumnComparison {
                left_column_id: 0,
                right_column_id: 1,
                comparison: PredicateComparison::LessThan,
            }))
        );
    }

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
    fn exactly_representable_date_timestamp_comparison_is_pushed() {
        const MICROS_PER_DAY: i64 = 86_400_000_000;
        let get = Get::new_without_table(7, vec!["shipdate".to_string()], vec![LogicalType::Date]);
        let date_column = Expression::Reference(ReferenceExpression::new(0, LogicalType::Date));
        let timestamp_column = Expression::Cast(CastExpression::new(
            date_column,
            LogicalType::Timestamp,
            BoundCastInfo::fixed(date_to_timestamp),
            false,
        ));
        let comparison = |timestamp| {
            Expression::Comparison(paro_planner::expression::ComparisonExpression::new(
                ComparisonType::LessThanOrEqual,
                timestamp_column.clone(),
                Expression::Constant(ConstantExpression::new(
                    Value::Timestamp(timestamp),
                    LogicalType::Timestamp,
                )),
            ))
        };

        assert_eq!(
            build_predicate(&comparison(10_000 * MICROS_PER_DAY), &get).unwrap(),
            Some(PredicateTree::leaf(Predicate::Le {
                column_id: 0,
                value: Value::Date(10_000),
            }))
        );
        assert_eq!(
            build_predicate(&comparison(10_000 * MICROS_PER_DAY + 1), &get).unwrap(),
            None,
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

    fn like_expression(pattern: &str, negated: bool) -> Expression {
        let like = Expression::Operator(OperatorExpression::new(
            OperatorType::Like,
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar(pattern.to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Boolean,
        ));
        if negated {
            Expression::Operator(OperatorExpression::new_unary(
                OperatorType::Not,
                like,
                LogicalType::Boolean,
            ))
        } else {
            like
        }
    }

    #[test]
    fn literal_suffix_like_is_pushed_as_an_exact_prefix_predicate() {
        let get = Get::new_without_table(7, vec!["type".to_string()], vec![LogicalType::Varchar]);

        assert_eq!(
            build_predicate(&like_expression("MEDIUM POLISHED%", true), &get).unwrap(),
            Some(PredicateTree::leaf(Predicate::StringPrefix {
                column_id: 0,
                prefix: "MEDIUM POLISHED".to_string(),
                negated: true,
            }))
        );
        assert_eq!(
            build_predicate(&like_expression(r"MEDIUM\%%", false), &get).unwrap(),
            Some(PredicateTree::leaf(Predicate::StringPrefix {
                column_id: 0,
                prefix: "MEDIUM%".to_string(),
                negated: false,
            }))
        );
    }

    #[test]
    fn non_prefix_like_remains_an_execution_predicate() {
        let get = Get::new_without_table(7, vec!["type".to_string()], vec![LogicalType::Varchar]);

        assert_eq!(
            build_predicate(&like_expression("MEDIUM%POLISHED%", false), &get).unwrap(),
            None
        );
        assert_eq!(
            build_predicate(&like_expression("MEDIUM_POLISHED%", false), &get).unwrap(),
            None
        );
    }
}
