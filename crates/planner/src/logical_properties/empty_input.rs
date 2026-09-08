// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Output semantics of a plan when its relational input is empty.
//!
//! Scalar-subquery decorrelation needs more than a cardinality estimate.  A
//! global aggregate can still produce one row over an empty input, and unary
//! operators above it may project, filter, reorder, or remove that row.  This
//! property carries that exact behavior through those operators without
//! evaluating user expressions during binding.

use std::collections::HashMap;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::AggregateEmptyInput;

use crate::expression::{
    CaseExpression, ConjunctionExpression, ConjunctionType, ConstantExpression, Expression,
    ExpressionIterator, ExpressionVisitDecision,
};
use crate::operator::{LogicalOperator, ProjectionMap};

#[derive(Debug)]
pub(crate) enum EmptyInputBehavior {
    /// The tree has no empty-input-sensitive relational boundary that needs a
    /// scalar decorrelation repair.
    NotApplicable,
    /// An applicable boundary exists, but one of its contracts is not exact.
    /// Such a tree must not use the aggregate decorrelation repair path.
    Indeterminate,
    /// The tree deterministically emits no rows.
    ZeroRows(Box<[LogicalType]>),
    /// The tree emits at most one row. `predicate`, when present, decides
    /// whether that row survives filters above the aggregate boundary.
    AtMostOneRow {
        outputs: Box<[Expression]>,
        predicate: Option<Expression>,
    },
}

impl EmptyInputBehavior {
    pub(crate) fn derive(plan: &LogicalOperator) -> Self {
        match plan {
            LogicalOperator::Aggregate(aggregate) => {
                let plain_global = aggregate.groups.is_empty()
                    && aggregate.grouping_functions.is_empty()
                    && (aggregate.grouping_sets.is_empty()
                        || (aggregate.grouping_sets.len() == 1
                            && aggregate.grouping_sets[0].expressions.is_empty()));
                if !plain_global {
                    return if aggregate
                        .grouping_sets
                        .iter()
                        .any(|set| set.expressions.is_empty())
                    {
                        // Multiple empty-input grouping domains can emit more
                        // than one row and therefore are not a scalar law.
                        Self::Indeterminate
                    } else {
                        Self::ZeroRows(plan.types().into_boxed_slice())
                    };
                }

                let outputs = aggregate
                    .aggregates
                    .iter()
                    .map(|expression| {
                        let Expression::Aggregate(aggregate) = expression else {
                            return None;
                        };
                        let return_type = aggregate.return_type.clone();
                        let value = match &aggregate.function.empty_input {
                            AggregateEmptyInput::Null => Value::Null(return_type.clone()),
                            exact @ AggregateEmptyInput::Exact(_) => {
                                exact.exact_value(&return_type)?.clone()
                            }
                            AggregateEmptyInput::Unknown => return None,
                        };
                        Some(Expression::Constant(
                            ConstantExpression::new(value, return_type).into(),
                        ))
                    })
                    .collect::<Option<Vec<_>>>();
                outputs.map_or(Self::Indeterminate, |outputs| Self::AtMostOneRow {
                    outputs: outputs.into_boxed_slice(),
                    predicate: None,
                })
            }
            LogicalOperator::Projection(projection) => Self::derive(&projection.child.operator)
                .project_expressions(
                    projection.child.get_column_bindings(),
                    &projection.expressions,
                ),
            LogicalOperator::Order(order) => Self::derive(&order.child.operator)
                .project_indices(&order.projection_map, &order.child.types()),
            LogicalOperator::Filter(filter) => Self::derive(&filter.child.operator)
                .filter(&filter.expressions, filter.child.get_column_bindings())
                .project_indices(&filter.projection_map, &filter.child.types()),
            LogicalOperator::Limit(limit) => {
                let child = Self::derive(&limit.child.operator);
                let Some(limit_value) = constant_nonnegative(limit.limit.as_ref()) else {
                    return if limit.limit.is_some() {
                        Self::Indeterminate
                    } else {
                        child.apply_offset(limit.offset.as_ref(), plan.types())
                    };
                };
                if limit_value == 0 {
                    Self::ZeroRows(plan.types().into_boxed_slice())
                } else {
                    child.apply_offset(limit.offset.as_ref(), plan.types())
                }
            }
            LogicalOperator::TopN(topn) => {
                if topn.limit == 0 || topn.offset > 0 {
                    Self::ZeroRows(plan.types().into_boxed_slice())
                } else {
                    Self::derive(&topn.child.operator)
                        .project_indices(&topn.projection_map, &topn.child.types())
                }
            }
            LogicalOperator::Distinct(distinct) => Self::derive(&distinct.child.operator),
            _ => Self::NotApplicable,
        }
    }

    /// Return the value a scalar result column must expose when decorrelation
    /// turns the empty branch into a missing join row.
    pub(crate) fn scalar_fallback(
        self,
        ordinal: usize,
        return_type: &LogicalType,
    ) -> Result<Option<Expression>> {
        let typed_null = || {
            Expression::Constant(
                ConstantExpression::new(Value::Null(return_type.clone()), return_type.clone())
                    .into(),
            )
        };
        match self {
            Self::NotApplicable => Ok(None),
            Self::Indeterminate => Err(paro_error::not_implemented(
                "correlated scalar aggregate has an indeterminate empty-input contract",
            )),
            Self::ZeroRows(types) => {
                let ty = types.get(ordinal).ok_or_else(|| {
                    paro_error::internal("scalar empty-input ordinal is outside the output layout")
                })?;
                if ty != return_type {
                    return Err(paro_error::internal(
                        "scalar empty-input zero-row contract changed result type",
                    ));
                }
                Ok(Some(typed_null()))
            }
            Self::AtMostOneRow { outputs, predicate } => {
                let output = outputs.get(ordinal).cloned().ok_or_else(|| {
                    paro_error::internal("scalar empty-input ordinal is outside the output layout")
                })?;
                if output.return_type() != *return_type {
                    return Err(paro_error::internal(
                        "scalar empty-input contract changed result type",
                    ));
                }
                Ok(Some(match predicate {
                    Some(predicate) => Expression::Case(
                        CaseExpression::new(predicate, output, typed_null(), return_type.clone())
                            .into(),
                    ),
                    None => output,
                }))
            }
        }
    }

    fn project_expressions(
        self,
        child_bindings: Vec<crate::operator::ColumnBinding>,
        expressions: &[Expression],
    ) -> Self {
        match self {
            Self::AtMostOneRow { outputs, predicate } => {
                if outputs.len() != child_bindings.len() {
                    return Self::Indeterminate;
                }
                let replacements = child_bindings
                    .into_iter()
                    .zip(outputs)
                    .collect::<HashMap<_, _>>();
                let rewrite = |expression: &Expression| {
                    let rewritten = expression.clone().replace_column_ref(&|column| {
                        (column.depth == 0)
                            .then(|| replacements.get(&column.binding).cloned())
                            .flatten()
                    });
                    contains_column_ref(&rewritten)
                        .then_some(())
                        .map_or(Some(rewritten), |_| None)
                };
                let Some(outputs) = expressions.iter().map(rewrite).collect::<Option<Vec<_>>>()
                else {
                    return Self::Indeterminate;
                };
                let predicate = match predicate {
                    Some(predicate) => match rewrite(&predicate) {
                        Some(predicate) => Some(predicate),
                        None => return Self::Indeterminate,
                    },
                    None => None,
                };
                Self::AtMostOneRow {
                    outputs: outputs.into_boxed_slice(),
                    predicate,
                }
            }
            Self::ZeroRows(_) => Self::ZeroRows(
                expressions
                    .iter()
                    .map(Expression::return_type)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            other => other,
        }
    }

    fn project_indices(self, projection: &ProjectionMap, child_types: &[LogicalType]) -> Self {
        let indices = projection.to_indices(child_types.len());
        match self {
            Self::AtMostOneRow { outputs, predicate } => {
                let Some(projected) = indices
                    .iter()
                    .map(|index| outputs.get(*index).cloned())
                    .collect::<Option<Vec<_>>>()
                else {
                    return Self::Indeterminate;
                };
                Self::AtMostOneRow {
                    outputs: projected.into_boxed_slice(),
                    predicate,
                }
            }
            Self::ZeroRows(types) => {
                let Some(projected) = indices
                    .iter()
                    .map(|index| types.get(*index).cloned())
                    .collect::<Option<Vec<_>>>()
                else {
                    return Self::Indeterminate;
                };
                Self::ZeroRows(projected.into_boxed_slice())
            }
            other => other,
        }
    }

    fn filter(
        self,
        filters: &[Expression],
        child_bindings: Vec<crate::operator::ColumnBinding>,
    ) -> Self {
        let Self::AtMostOneRow { outputs, predicate } = self else {
            return self;
        };
        if filters.is_empty() {
            return Self::AtMostOneRow { outputs, predicate };
        }
        if child_bindings.len() != outputs.len() {
            return Self::Indeterminate;
        }
        let replacements = child_bindings
            .into_iter()
            .zip(outputs.iter().cloned())
            .collect::<HashMap<_, _>>();
        let mut rewritten_filters =
            Vec::with_capacity(filters.len() + usize::from(predicate.is_some()));
        if let Some(predicate) = predicate {
            rewritten_filters.push(predicate);
        }
        for filter in filters {
            let rewritten = filter.clone().replace_column_ref(&|column| {
                if column.depth != 0 {
                    return None;
                }
                replacements.get(&column.binding).cloned()
            });
            if contains_column_ref(&rewritten) {
                return Self::Indeterminate;
            }
            rewritten_filters.push(rewritten);
        }
        let predicate = if rewritten_filters.len() == 1 {
            rewritten_filters.pop()
        } else {
            Some(Expression::Conjunction(
                ConjunctionExpression::new(ConjunctionType::And, rewritten_filters).into(),
            ))
        };
        Self::AtMostOneRow { outputs, predicate }
    }

    fn apply_offset(self, offset: Option<&Expression>, output_types: Vec<LogicalType>) -> Self {
        match offset {
            None => self,
            Some(offset) => match constant_nonnegative(Some(offset)) {
                Some(0) => self,
                Some(_) => Self::ZeroRows(output_types.into_boxed_slice()),
                None => Self::Indeterminate,
            },
        }
    }
}

fn constant_nonnegative(expression: Option<&Expression>) -> Option<usize> {
    let expression = expression?;
    let Expression::Constant(constant) = expression else {
        return None;
    };
    let value = constant.value.as_i64()?;
    usize::try_from(value).ok()
}

fn contains_column_ref(expression: &Expression) -> bool {
    let mut found = false;
    ExpressionIterator::visit(expression, &mut |candidate| {
        if matches!(candidate, Expression::ColumnRef(_)) {
            found = true;
            ExpressionVisitDecision::SkipChildren
        } else {
            ExpressionVisitDecision::Descend
        }
    });
    found
}

/// Remove sorting and limiting work that is provably inert above an at-most
/// one-row scalar branch. Besides reducing execution work, this keeps
/// decorrelation from expanding `LIMIT 1` into a partitioned window after a
/// global aggregate has already proved the stronger cardinality contract.
pub(crate) fn normalize_scalar_singleton_wrappers(plan: LogicalOperator) -> LogicalOperator {
    match plan {
        LogicalOperator::Limit(mut limit) => {
            limit.child =
                Box::new((*limit.child).map_operator(normalize_scalar_singleton_wrappers));
            let limit_keeps_singleton = match limit.limit.as_ref() {
                None => true,
                Some(limit) => constant_nonnegative(Some(limit)).is_some_and(|limit| limit >= 1),
            };
            let offset_is_zero = match limit.offset.as_ref() {
                None => true,
                Some(offset) => constant_nonnegative(Some(offset)) == Some(0),
            };
            if limit_keeps_singleton
                && offset_is_zero
                && maximum_cardinality(&limit.child.operator).is_some_and(|maximum| maximum <= 1)
            {
                (*limit.child).into_operator()
            } else {
                LogicalOperator::Limit(limit)
            }
        }
        LogicalOperator::Order(mut order) => {
            order.child =
                Box::new((*order.child).map_operator(normalize_scalar_singleton_wrappers));
            let removable = maximum_cardinality(&order.child.operator)
                .is_some_and(|maximum| maximum <= 1)
                && order.orders.iter().all(|order| {
                    let properties = order.expression.evaluation_properties();
                    properties.can_share_evaluation() && properties.is_infallible()
                });
            if removable {
                (*order.child).into_operator()
            } else {
                LogicalOperator::Order(order)
            }
        }
        LogicalOperator::TopN(mut topn) => {
            topn.child = Box::new((*topn.child).map_operator(normalize_scalar_singleton_wrappers));
            let removable = topn.limit >= 1
                && topn.offset == 0
                && maximum_cardinality(&topn.child.operator).is_some_and(|maximum| maximum <= 1)
                && topn.orders.iter().all(|order| {
                    let properties = order.expression.evaluation_properties();
                    properties.can_share_evaluation() && properties.is_infallible()
                });
            if removable && topn.projection_map.is_identity(topn.child.types().len()) {
                (*topn.child).into_operator()
            } else {
                LogicalOperator::TopN(topn)
            }
        }
        LogicalOperator::Projection(mut projection) => {
            projection.child =
                Box::new((*projection.child).map_operator(normalize_scalar_singleton_wrappers));
            LogicalOperator::Projection(projection)
        }
        LogicalOperator::Filter(mut filter) => {
            filter.child =
                Box::new((*filter.child).map_operator(normalize_scalar_singleton_wrappers));
            LogicalOperator::Filter(filter)
        }
        LogicalOperator::Distinct(mut distinct) => {
            distinct.child =
                Box::new((*distinct.child).map_operator(normalize_scalar_singleton_wrappers));
            LogicalOperator::Distinct(distinct)
        }
        other => other,
    }
}

/// Schema-invariant cardinality facts only. Snapshot row counts never enter
/// this property because prepared plans may outlive the data used to compile
/// them.
fn maximum_cardinality(plan: &LogicalOperator) -> Option<u64> {
    match plan {
        LogicalOperator::DummyScan => Some(1),
        LogicalOperator::EmptyResult(_) => Some(0),
        LogicalOperator::ExpressionGet(values) => u64::try_from(values.expressions.len()).ok(),
        LogicalOperator::Aggregate(aggregate)
            if aggregate.groups.is_empty()
                && aggregate.grouping_functions.is_empty()
                && (aggregate.grouping_sets.is_empty()
                    || (aggregate.grouping_sets.len() == 1
                        && aggregate.grouping_sets[0].expressions.is_empty())) =>
        {
            Some(1)
        }
        LogicalOperator::Projection(projection) => maximum_cardinality(&projection.child.operator),
        LogicalOperator::Filter(filter) => maximum_cardinality(&filter.child.operator),
        LogicalOperator::Order(order) => maximum_cardinality(&order.child.operator),
        LogicalOperator::Distinct(distinct) => maximum_cardinality(&distinct.child.operator),
        LogicalOperator::Limit(limit) => match limit.limit.as_ref() {
            Some(limit_expression) => constant_nonnegative(Some(limit_expression))
                .and_then(|limit_value| u64::try_from(limit_value).ok())
                .map(|limit_value| {
                    maximum_cardinality(&limit.child.operator)
                        .unwrap_or(u64::MAX)
                        .min(limit_value)
                }),
            None => maximum_cardinality(&limit.child.operator),
        },
        LogicalOperator::TopN(topn) => Some(
            maximum_cardinality(&topn.child.operator)
                .unwrap_or(u64::MAX)
                .min(u64::try_from(topn.limit).unwrap_or(u64::MAX)),
        ),
        _ => None,
    }
}
