// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::bind::expr as bind;
use crate::expression::{
    CastExpression, ColumnRefExpression, ComparisonType, ConjunctionType, Expression,
    OperatorExpression, OperatorType, SubqueryType,
};
use crate::operator::ColumnBinding;
use paro_common::error::{self as paro_error, ParoError, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{BinaryOperator, ColumnRef, Expr, JsonOperator, SubqueryModifier};
/// Maximum expression depth to prevent stack overflow.
const DEFAULT_MAX_EXPRESSION_DEPTH: usize = 1000;

/// Initial stack depth increment when creating nested binders.
const INITIAL_DEPTH_INCREMENT: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AtAtBindTarget {
    FullText { swap_args: bool },
    JsonFallback,
}

fn resolve_atat_bind_target(
    left_type: &LogicalType,
    right_type: &LogicalType,
) -> Result<AtAtBindTarget> {
    match (left_type, right_type) {
        (LogicalType::TsVector, LogicalType::TsQuery) => {
            Ok(AtAtBindTarget::FullText { swap_args: false })
        }
        (LogicalType::TsQuery, LogicalType::TsVector) => {
            Ok(AtAtBindTarget::FullText { swap_args: true })
        }
        _ if matches!(left_type, LogicalType::TsVector | LogicalType::TsQuery)
            || matches!(right_type, LogicalType::TsVector | LogicalType::TsQuery) =>
        {
            Err(paro_error::syntax(format!(
                "operator @@ expects TSVECTOR and TSQUERY operands (in either order), got {} @@ {}",
                left_type, right_type
            )))
        }
        _ => Ok(AtAtBindTarget::JsonFallback),
    }
}

/// Main entry point for binding expressions.
pub fn bind_expression(binder: &mut crate::binder::Binder, expr: Expr) -> Result<Expression> {
    ExpressionBinder::new(binder).bind(expr)
}

/// Information about a bound column reference.
#[derive(Debug, Clone)]
pub struct BoundColumnReferenceInfo {
    pub name: String,
    pub query_location: Option<usize>,
}

/// Result of binding an expression.
pub struct BindResult {
    pub expression: Option<Expression>,
    pub error: Option<paro_common::error::ParoError>,
}

impl BindResult {
    pub fn success(expr: Expression) -> Self {
        Self {
            expression: Some(expr),
            error: None,
        }
    }

    pub fn error(err: paro_common::error::ParoError) -> Self {
        Self {
            expression: None,
            error: Some(err),
        }
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    pub fn into_result(self) -> Result<Expression> {
        match self.error {
            Some(err) => Err(err),
            None => Ok(self.expression.unwrap()),
        }
    }
}

/// Base class for all expression binders.
pub struct ExpressionBinder<'a> {
    pub binder: &'a mut crate::binder::Binder,
    pub target_type: LogicalType,
    stack_depth: usize,
    max_expression_depth: usize,
    bound_columns: Vec<BoundColumnReferenceInfo>,
    pub allow_aggregates: bool,
    pub allow_window: bool,
    pub allow_default: bool,
}

impl<'a> ExpressionBinder<'a> {
    pub fn new(binder: &'a mut crate::binder::Binder) -> Self {
        Self {
            binder,
            target_type: LogicalType::Unknown,
            stack_depth: INITIAL_DEPTH_INCREMENT,
            max_expression_depth: DEFAULT_MAX_EXPRESSION_DEPTH,
            bound_columns: Vec::new(),
            allow_aggregates: true,
            allow_window: true,
            allow_default: true,
        }
    }

    pub fn with_target_type(
        binder: &'a mut crate::binder::Binder,
        target_type: LogicalType,
    ) -> Self {
        Self {
            binder,
            target_type,
            stack_depth: INITIAL_DEPTH_INCREMENT,
            max_expression_depth: DEFAULT_MAX_EXPRESSION_DEPTH,
            bound_columns: Vec::new(),
            allow_aggregates: true,
            allow_window: true,
            allow_default: true,
        }
    }

    pub fn create_child(binder: &'a mut crate::binder::Binder, parent_depth: usize) -> Self {
        Self {
            binder,
            target_type: LogicalType::Unknown,
            stack_depth: parent_depth + INITIAL_DEPTH_INCREMENT,
            max_expression_depth: DEFAULT_MAX_EXPRESSION_DEPTH,
            bound_columns: Vec::new(),
            allow_aggregates: true,
            allow_window: true,
            allow_default: true,
        }
    }

    pub fn stack_depth(&self) -> usize {
        self.stack_depth
    }

    pub fn has_bound_columns(&self) -> bool {
        !self.bound_columns.is_empty()
    }

    pub fn get_bound_columns(&self) -> &[BoundColumnReferenceInfo] {
        &self.bound_columns
    }

    pub fn add_bound_column(&mut self, name: String, query_location: Option<usize>) {
        self.bound_columns.push(BoundColumnReferenceInfo {
            name,
            query_location,
        });
    }

    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        self.stack_check()?;
        let mut bound_expr = self.bind_expression(expr)?;

        if self.target_type != LogicalType::Unknown {
            bound_expr = CastExpression::add_cast_if_needed(
                bound_expr,
                self.target_type.clone(),
                &self.binder.cast_functions,
            )?;
        }

        Ok(bound_expr)
    }

    pub fn bind_at_depth(&mut self, expr: Expr, depth: usize) -> Result<Expression> {
        let old_depth = self.stack_depth;
        self.stack_depth += depth;
        let result = self.bind(expr);
        self.stack_depth = old_depth;
        result
    }

    pub fn bind_child(&mut self, expr: Expr) -> Result<Expression> {
        self.stack_depth += 1;
        let result = self.bind_expression(expr);
        self.stack_depth -= 1;
        result
    }

    fn stack_check(&self) -> Result<()> {
        if self.stack_depth >= self.max_expression_depth {
            return Err(paro_error::internal(format!(
                "Max expression depth limit of {} exceeded. Expression tree is too deep.",
                self.max_expression_depth
            )));
        }
        Ok(())
    }

    pub fn bind_expression(&mut self, expr: Expr) -> Result<Expression> {
        match expr {
            Expr::Literal { value, .. } => bind::bind_literal(value),

            Expr::Placeholder { span } => self.binder.bind_protocol_parameter(span),

            Expr::ColumnRef { column, .. } => {
                let error = &mut ParoError::default();
                let qualified = self.qualify_column_name(&column, error)?;
                if let Some(qualified_expr) = qualified {
                    if let Expr::ColumnRef {
                        column: qualified_col,
                        ..
                    } = qualified_expr
                    {
                        return self.bind_column_ref(qualified_col);
                    }
                    return self.bind_expression(qualified_expr);
                }

                if Self::is_potential_alias(&column) {
                    if let Some(result) = self.try_resolve_alias_reference(&column, 0, true) {
                        return result;
                    }
                }

                self.bind_column_ref(column)
            }

            Expr::UnaryOp { op, expr, .. } => {
                if matches!(op, paro_parser::ast::UnaryOperator::Not) {
                    bind::bind_not(self, *expr)
                } else {
                    let func_name = op.to_string();
                    bind::bind_function(
                        self.binder,
                        None,
                        &func_name,
                        vec![*expr],
                        false,
                        None,
                        vec![],
                    )
                }
            }

            Expr::BinaryOp {
                left, op, right, ..
            } => self.bind_binary_op(*left, op, *right),

            Expr::JsonOp {
                left, op, right, ..
            } => self.bind_json_op(*left, op, *right),

            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => bind::bind_case(self, operand, conditions, results, else_result),

            Expr::IsNull { expr, not, .. } => bind::bind_is_null(self, *expr, not),

            Expr::IsDistinctFrom {
                left, right, not, ..
            } => bind::bind_comparison(
                self,
                *left,
                *right,
                if not {
                    ComparisonType::NotDistinctFrom
                } else {
                    ComparisonType::DistinctFrom
                },
            ),

            Expr::InList {
                expr, list, not, ..
            } => bind::bind_in_list(self, *expr, list, not),

            Expr::FunctionCall { func, .. } => self.bind_function_call(func),

            Expr::CountAll { window, .. } => {
                if let Some(over) = window {
                    bind::bind_window_expression(
                        self.binder,
                        "count_star",
                        vec![],
                        false,
                        None,
                        vec![],
                        over,
                        false,
                    )
                } else {
                    bind::bind_function(
                        self.binder,
                        None,
                        "count_star",
                        vec![],
                        false,
                        None,
                        vec![],
                    )
                }
            }

            Expr::Subquery {
                subquery, modifier, ..
            } => bind::bind_subquery_expression(
                self.binder,
                *subquery,
                match modifier {
                    None => SubqueryType::Scalar,
                    Some(SubqueryModifier::Any | SubqueryModifier::Some) => SubqueryType::Any,
                    Some(SubqueryModifier::All) => SubqueryType::All,
                },
                None,
                None,
            ),

            Expr::Exists { subquery, not, .. } => {
                let subquery_type = if not {
                    SubqueryType::NotExists
                } else {
                    SubqueryType::Exists
                };
                bind::bind_subquery_expression(self.binder, *subquery, subquery_type, None, None)
            }

            Expr::InSubquery {
                expr,
                subquery,
                not,
                ..
            } => self.bind_in_subquery(*expr, *subquery, not),

            Expr::Between {
                expr,
                not,
                low,
                high,
                ..
            } => bind::bind_between(self, *expr, *low, *high, not),

            Expr::Cast {
                expr, target_type, ..
            } => bind::bind_cast(self, *expr, target_type),

            Expr::TryCast {
                expr, target_type, ..
            } => bind::bind_try_cast(self, *expr, target_type),

            Expr::Tuple { exprs, .. } => {
                if exprs.len() == 1 {
                    self.bind_child(exprs.into_iter().next().unwrap())
                } else {
                    bind::bind_tuple(self, exprs)
                }
            }

            Expr::Array { exprs, .. } => bind::bind_array(self, exprs),

            Expr::MapAccess { expr, accessor, .. } => bind::bind_map_access(self, *expr, accessor),

            _ => Err(paro_error::not_implemented(format!(
                "Expression not supported: {:?}",
                expr
            ))),
        }
    }

    fn bind_column_ref(&mut self, column: ColumnRef) -> Result<Expression> {
        let column_name = column.column.name().to_string();
        self.add_bound_column(column_name, None);
        bind::bind_column_ref_from_column_ref(self.binder, column)
    }

    fn bind_binary_op(
        &mut self,
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<Expression> {
        if let Some(bound) = self.try_bind_subquery_comparison(&left, &op, &right)? {
            return Ok(bound);
        }

        match op {
            BinaryOperator::And => bind::bind_conjunction(self, left, right, ConjunctionType::And),
            BinaryOperator::Or => bind::bind_conjunction(self, left, right, ConjunctionType::Or),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::Lte
            | BinaryOperator::Gt
            | BinaryOperator::Gte => {
                let comparison_type = match op {
                    BinaryOperator::Eq => ComparisonType::Equal,
                    BinaryOperator::NotEq => ComparisonType::NotEqual,
                    BinaryOperator::Lt => ComparisonType::LessThan,
                    BinaryOperator::Lte => ComparisonType::LessThanOrEqual,
                    BinaryOperator::Gt => ComparisonType::GreaterThan,
                    BinaryOperator::Gte => ComparisonType::GreaterThanOrEqual,
                    _ => unreachable!(),
                };
                bind::bind_comparison(self, left, right, comparison_type)
            }
            BinaryOperator::Like(None) | BinaryOperator::NotLike(None) => {
                let not = matches!(op, BinaryOperator::NotLike(None));
                bind::bind_like(self, left, right, not)
            }
            _ => {
                let func_name = op.to_func_name();
                bind::bind_function(
                    self.binder,
                    None,
                    &func_name,
                    vec![left, right],
                    false,
                    None,
                    vec![],
                )
            }
        }
    }

    fn try_bind_subquery_comparison(
        &mut self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<Option<Expression>> {
        let Expr::Subquery {
            subquery,
            modifier: Some(modifier),
            ..
        } = right
        else {
            return Ok(None);
        };

        let comparison_type = match op {
            BinaryOperator::Eq => ComparisonType::Equal,
            BinaryOperator::NotEq => ComparisonType::NotEqual,
            BinaryOperator::Lt => ComparisonType::LessThan,
            BinaryOperator::Lte => ComparisonType::LessThanOrEqual,
            BinaryOperator::Gt => ComparisonType::GreaterThan,
            BinaryOperator::Gte => ComparisonType::GreaterThanOrEqual,
            _ => return Ok(None),
        };
        let subquery_type = match modifier {
            SubqueryModifier::Any | SubqueryModifier::Some => SubqueryType::Any,
            SubqueryModifier::All => SubqueryType::All,
        };
        let left_child = self.bind_child(left.clone())?;

        Ok(Some(bind::bind_subquery_expression(
            self.binder,
            (**subquery).clone(),
            subquery_type,
            Some(left_child),
            Some(comparison_type),
        )?))
    }

    fn bind_json_op(&mut self, left: Expr, op: JsonOperator, right: Expr) -> Result<Expression> {
        match op {
            JsonOperator::AtAt => self.bind_json_atat(left, right),
            _ => {
                let func_name = op.to_func_name();
                bind::bind_function(
                    self.binder,
                    None,
                    &func_name,
                    vec![left, right],
                    false,
                    None,
                    vec![],
                )
            }
        }
    }

    fn bind_json_atat(&mut self, left: Expr, right: Expr) -> Result<Expression> {
        let left_type = self.bind_child(left.clone())?.return_type();
        let right_type = self.bind_child(right.clone())?.return_type();

        match resolve_atat_bind_target(&left_type, &right_type)? {
            AtAtBindTarget::FullText { swap_args } => {
                if swap_args {
                    bind::bind_function(
                        self.binder,
                        None,
                        "fulltext_match_internal",
                        vec![right, left],
                        false,
                        None,
                        vec![],
                    )
                } else {
                    bind::bind_function(
                        self.binder,
                        None,
                        "fulltext_match_internal",
                        vec![left, right],
                        false,
                        None,
                        vec![],
                    )
                }
            }
            AtAtBindTarget::JsonFallback => {
                let func_name = JsonOperator::AtAt.to_func_name();
                bind::bind_function(
                    self.binder,
                    None,
                    &func_name,
                    vec![left, right],
                    false,
                    None,
                    vec![],
                )
            }
        }
    }

    fn bind_function_call(&mut self, func: paro_parser::ast::FunctionCall) -> Result<Expression> {
        let paro_parser::ast::FunctionCall {
            distinct,
            schema,
            name,
            args,
            order_by,
            filter,
            window,
            ..
        } = func;
        let name = name.to_string();
        let schema_name = schema.map(|ident| ident.name);
        let filter = filter.map(|filter| *filter);
        let order_bys = order_by;

        if name.eq_ignore_ascii_case("grouping") {
            if window.is_some() {
                return Err(paro_error::syntax(
                    "GROUPING() cannot be used as a window function".to_string(),
                ));
            }
            return self.bind_grouping_call(distinct, args, filter, order_bys);
        }

        if let Some(over) = window {
            if !self.allow_window {
                return Err(paro_error::syntax(format!(
                    "Window functions are not allowed in this context (function: {})",
                    name
                )));
            }
            let ignore_nulls = over.ignore_nulls.unwrap_or(false);
            return bind::bind_window_expression(
                self.binder,
                &name,
                args,
                distinct,
                filter,
                order_bys,
                over.window,
                ignore_nulls,
            );
        }

        if name.eq_ignore_ascii_case("coalesce") {
            return bind::bind_coalesce(self, args);
        }

        let result = bind::bind_function(
            self.binder,
            schema_name.as_deref(),
            &name,
            args,
            distinct,
            filter,
            order_bys,
        )?;

        if !self.allow_aggregates && matches!(result, Expression::Aggregate(_)) {
            return Err(paro_error::syntax(format!(
                "Aggregate functions are not allowed in this context (function: {})",
                name
            )));
        }

        Ok(result)
    }

    fn bind_grouping_call(
        &mut self,
        distinct: bool,
        args: Vec<Expr>,
        filter: Option<Expr>,
        order_bys: Vec<paro_parser::ast::OrderByExpr>,
    ) -> Result<Expression> {
        if distinct {
            return Err(paro_error::syntax(
                "GROUPING() does not support DISTINCT".to_string(),
            ));
        }
        if filter.is_some() {
            return Err(paro_error::syntax(
                "GROUPING() does not support FILTER".to_string(),
            ));
        }
        if !order_bys.is_empty() {
            return Err(paro_error::syntax(
                "GROUPING() does not support WITHIN GROUP ORDER BY".to_string(),
            ));
        }
        if args.is_empty() {
            return Err(paro_error::syntax(
                "GROUPING requires at least one argument".to_string(),
            ));
        }
        if args.len() >= 64 {
            return Err(paro_error::syntax(
                "GROUPING statement cannot have more than 64 groups".to_string(),
            ));
        }

        let grouping_context = self
            .binder
            .active_grouping_context
            .as_ref()
            .ok_or_else(|| {
                paro_error::syntax("GROUPING statement cannot be used without groups".to_string())
            })?;
        let group_info = grouping_context.group_info.clone();
        let groupings_index = grouping_context.groupings_index;

        let mut group_indexes = Vec::with_capacity(args.len());
        for mut arg in args {
            Self::qualify_column_names(self.binder, &mut arg);

            let group_idx = if let Expr::ColumnRef { column, .. } = &arg {
                if column.schema.is_none() && column.table.is_none() {
                    group_info.find_alias(column.column.name())
                } else {
                    None
                }
            } else {
                None
            }
            .or_else(|| group_info.find_group(&arg.to_string()))
            .ok_or_else(|| {
                paro_error::syntax(format!(
                    "GROUPING child \"{}\" must be a grouping column",
                    arg
                ))
            })?;
            group_indexes.push(group_idx);
        }

        let grouping_context = self
            .binder
            .active_grouping_context
            .as_mut()
            .ok_or_else(|| {
                paro_error::syntax("GROUPING statement cannot be used without groups".to_string())
            })?;
        let col_idx = grouping_context.grouping_functions.len();
        grouping_context.grouping_functions.push(group_indexes);
        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(groupings_index, col_idx),
            LogicalType::BigInt,
        )))
    }

    fn bind_in_subquery(
        &mut self,
        expr: Expr,
        subquery: paro_parser::ast::Query,
        not: bool,
    ) -> Result<Expression> {
        let left = self.bind_child(expr)?;
        let bound_subquery = bind::bind_subquery_expression(
            self.binder,
            subquery,
            SubqueryType::Any,
            Some(left),
            Some(ComparisonType::Equal),
        )?;

        if not {
            Ok(Expression::Operator(OperatorExpression::new_unary(
                OperatorType::Not,
                bound_subquery,
                LogicalType::Boolean,
            )))
        } else {
            Ok(bound_subquery)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_atat_bind_target, AtAtBindTarget};
    use paro_common::types::LogicalType;

    #[test]
    fn resolve_atat_bind_target_dispatches_fulltext_and_json() {
        assert_eq!(
            resolve_atat_bind_target(&LogicalType::TsVector, &LogicalType::TsQuery).unwrap(),
            AtAtBindTarget::FullText { swap_args: false }
        );
        assert_eq!(
            resolve_atat_bind_target(&LogicalType::TsQuery, &LogicalType::TsVector).unwrap(),
            AtAtBindTarget::FullText { swap_args: true }
        );
        assert_eq!(
            resolve_atat_bind_target(&LogicalType::Varchar, &LogicalType::Varchar).unwrap(),
            AtAtBindTarget::JsonFallback
        );
    }

    #[test]
    fn resolve_atat_bind_target_rejects_mixed_fulltext_types() {
        let err = resolve_atat_bind_target(&LogicalType::TsVector, &LogicalType::Varchar)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("operator @@ expects TSVECTOR and TSQUERY"),
            "expected clear type mismatch error, got: {err}"
        );
    }
}
