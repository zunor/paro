// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded SQL expression and predicate presentation.

#[cfg(test)]
use super::super::PhysicalPlan;
use crate::expression::{
    AggregateExpression, AggregateType, Expression, OperatorType, WindowFrameBound, WindowFrameType,
};
use crate::physical::specs::AggregateSpec;
use paro_catalog::entry::TableCatalogEntry;
use paro_storage::index::{Predicate, PredicateTree};
use std::cell::Cell;
pub(super) fn format_payload_expr(
    expression: &Expression,
    spec: &AggregateSpec,
    formatter: &ExplainExpressionFormatter<'_>,
) -> String {
    if let Expression::Reference(reference) = expression {
        if let Some(payload) = spec.projection_exprs.get(reference.index) {
            return formatter.format(payload);
        }
    }
    formatter.format(expression)
}

pub(super) fn format_aggregate_expr(
    expression: &Expression,
    spec: &AggregateSpec,
    formatter: &ExplainExpressionFormatter<'_>,
) -> String {
    let Expression::Aggregate(aggregate) = expression else {
        return formatter.format(expression);
    };
    format_bound_aggregate(aggregate, &|child| {
        format_payload_expr(child, spec, formatter)
    })
}

fn format_bound_aggregate(
    aggregate: &AggregateExpression,
    format_child: &impl Fn(&Expression) -> String,
) -> String {
    let distinct = if aggregate.aggr_type == AggregateType::Distinct {
        "DISTINCT "
    } else {
        ""
    };
    let args = if aggregate.children.is_empty() {
        "*".to_string()
    } else {
        aggregate
            .children
            .iter()
            .map(format_child)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut rendered = format!("{}({distinct}{args})", aggregate.function.name);
    if let Some(filter) = aggregate.filter.as_ref() {
        rendered.push_str(" FILTER (WHERE ");
        rendered.push_str(&format_child(filter));
        rendered.push(')');
    }
    if !aggregate.order_bys.is_empty() {
        rendered.push_str(" WITHIN GROUP (ORDER BY ");
        rendered.push_str(
            &aggregate
                .order_bys
                .iter()
                .map(|order| {
                    format!(
                        "{} {} NULLS {}",
                        format_child(&order.expression),
                        if order.ascending { "ASC" } else { "DESC" },
                        if order.nulls_first { "FIRST" } else { "LAST" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
        rendered.push(')');
    }
    rendered
}

pub(super) struct ExplainExpressionFormatter<'a> {
    columns: &'a [String],
}

#[cfg(test)]
mod expression_format_tests {
    use super::*;
    use crate::expression::{
        ComparisonExpression, ComparisonType, ConstantExpression, ReferenceExpression,
    };
    use paro_common::{runtime_value::Value, types::LogicalType};

    #[test]
    fn comparison_presentation_does_not_depend_on_native_hash_orientation() {
        let names = ["sum(amount)".to_string()];
        let formatter = ExplainExpressionFormatter::new(&names);
        let column =
            Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer).into());
        for literal in [Value::Integer(100), Value::Null(LogicalType::Integer)] {
            let constant =
                Expression::Constant(ConstantExpression::new(literal, LogicalType::Integer).into());
            for op in [
                ComparisonType::Equal,
                ComparisonType::NotEqual,
                ComparisonType::LessThan,
                ComparisonType::LessThanOrEqual,
                ComparisonType::GreaterThan,
                ComparisonType::GreaterThanOrEqual,
                ComparisonType::DistinctFrom,
                ComparisonType::NotDistinctFrom,
            ] {
                assert_eq!(op.flipped().flipped(), op);
                let normal = Expression::Comparison(
                    ComparisonExpression::new(op, column.clone(), constant.clone()).into(),
                );
                let reversed = Expression::Comparison(
                    ComparisonExpression::new(op.flipped(), constant.clone(), column.clone())
                        .into(),
                );
                let before = reversed.allocation_identity();
                assert_eq!(formatter.format(&normal), formatter.format(&reversed));
                assert_eq!(reversed.allocation_identity(), before);
                assert!(formatter.format(&normal).starts_with("sum(amount) "));
            }
        }
    }

    #[test]
    fn explain_text_uses_matrixone_operator_and_property_columns() {
        assert_eq!(PhysicalPlan::explain_operator_indent(0), 0);
        assert_eq!(PhysicalPlan::explain_operator_indent(1), 2);
        assert_eq!(PhysicalPlan::explain_operator_indent(2), 8);
        assert_eq!(PhysicalPlan::explain_operator_indent(3), 14);

        assert_eq!(PhysicalPlan::explain_property_indent(0), 2);
        assert_eq!(PhysicalPlan::explain_property_indent(1), 8);
        assert_eq!(PhysicalPlan::explain_property_indent(2), 14);
    }
}

pub(super) const EXPLAIN_EXPRESSION_MAX_NODES: usize = 1_024;
const EXPLAIN_EXPRESSION_MAX_DEPTH: usize = 64;
const EXPLAIN_EXPRESSION_MAX_BYTES: usize = 16 * 1_024;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ExplainPrecedence {
    Lowest,
    Or,
    And,
    Not,
    Comparison,
    Primary,
}

struct ExplainFormatBudget {
    remaining_nodes: Cell<usize>,
}

impl ExplainFormatBudget {
    fn new() -> Self {
        Self {
            remaining_nodes: Cell::new(EXPLAIN_EXPRESSION_MAX_NODES),
        }
    }

    fn enter(&self, depth: usize) -> bool {
        let remaining = self.remaining_nodes.get();
        if depth >= EXPLAIN_EXPRESSION_MAX_DEPTH || remaining == 0 {
            return false;
        }
        self.remaining_nodes.set(remaining - 1);
        true
    }
}

impl<'a> ExplainExpressionFormatter<'a> {
    pub(super) fn new(columns: &'a [String]) -> Self {
        Self { columns }
    }

    fn column(&self, index: usize) -> String {
        self.columns
            .get(index)
            .cloned()
            .unwrap_or_else(|| format!("<column {index}>"))
    }

    pub(super) fn format_order_by(&self, orders: &[crate::binder::ir::OrderByNode]) -> String {
        bound_explain_text(
            orders
                .iter()
                .map(|order| {
                    format!(
                        "{} {} NULLS {}",
                        self.format(&order.expression),
                        if order.ascending { "ASC" } else { "DESC" },
                        if order.nulls_first { "FIRST" } else { "LAST" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        )
    }

    pub(super) fn format(&self, expression: &Expression) -> String {
        let budget = ExplainFormatBudget::new();
        bound_explain_text(self.format_expression(
            expression,
            ExplainPrecedence::Lowest,
            0,
            &budget,
        ))
    }

    fn format_expression(
        &self,
        expression: &Expression,
        parent_precedence: ExplainPrecedence,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        if !budget.enter(depth) {
            return "…".to_string();
        }
        let next_depth = depth + 1;
        let (rendered, precedence) = match expression {
            Expression::Reference(reference) => {
                (self.column(reference.index), ExplainPrecedence::Primary)
            }
            Expression::ColumnRef(column) => (
                self.column(column.binding.column_index),
                ExplainPrecedence::Primary,
            ),
            Expression::Constant(constant) => {
                (constant.value.to_string(), ExplainPrecedence::Primary)
            }
            Expression::Parameter(parameter) => (
                format!("${}", parameter.slot.index.index() + 1),
                ExplainPrecedence::Primary,
            ),
            Expression::Comparison(comparison) => {
                // Native scalar identity may choose either orientation by
                // fingerprint. Presentation has its own stable convention:
                // keep a lone literal on the right. This changes no IR or
                // evaluation order and prevents hash order from changing a
                // named predicate boundary in EXPLAIN consumers.
                let (left, op, right) =
                    if matches!(comparison.left.as_ref(), Expression::Constant(_))
                        && !matches!(comparison.right.as_ref(), Expression::Constant(_))
                    {
                        (
                            &comparison.right,
                            comparison.comparison_type.flipped(),
                            &comparison.left,
                        )
                    } else {
                        (
                            &comparison.left,
                            comparison.comparison_type,
                            &comparison.right,
                        )
                    };
                (
                    format!(
                        "{} {} {}",
                        self.format_expression(
                            left,
                            ExplainPrecedence::Comparison,
                            next_depth,
                            budget,
                        ),
                        op,
                        self.format_expression(
                            right,
                            ExplainPrecedence::Comparison,
                            next_depth,
                            budget,
                        )
                    ),
                    ExplainPrecedence::Comparison,
                )
            }
            Expression::Conjunction(conjunction) => {
                let (separator, precedence) = match conjunction.conjunction_type {
                    crate::expression::ConjunctionType::And => (" AND ", ExplainPrecedence::And),
                    crate::expression::ConjunctionType::Or => (" OR ", ExplainPrecedence::Or),
                };
                (
                    conjunction
                        .children
                        .iter()
                        .map(|child| self.format_expression(child, precedence, next_depth, budget))
                        .collect::<Vec<_>>()
                        .join(separator),
                    precedence,
                )
            }
            Expression::Cast(cast) => {
                let cast_name = if cast.try_cast { "TRY_CAST" } else { "CAST" };
                (
                    format!(
                        "{cast_name}({} AS {})",
                        self.format_expression(
                            &cast.child,
                            ExplainPrecedence::Lowest,
                            next_depth,
                            budget,
                        ),
                        cast.target_type
                    ),
                    ExplainPrecedence::Primary,
                )
            }
            Expression::Function(function) => (
                format!(
                    "{}({})",
                    function.function.name.as_str(),
                    function
                        .children
                        .iter()
                        .map(|child| self.format_expression(
                            child,
                            ExplainPrecedence::Lowest,
                            next_depth,
                            budget,
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                ExplainPrecedence::Primary,
            ),
            Expression::Aggregate(aggregate) => (
                format_bound_aggregate(aggregate, &|child| {
                    self.format_expression(child, ExplainPrecedence::Lowest, next_depth, budget)
                }),
                ExplainPrecedence::Primary,
            ),
            Expression::Case(case_expression) => (
                format!(
                    "CASE WHEN {} THEN {} ELSE {} END",
                    self.format_expression(
                        &case_expression.check,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    ),
                    self.format_expression(
                        &case_expression.result_if_true,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    ),
                    self.format_expression(
                        &case_expression.result_if_false,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    )
                ),
                ExplainPrecedence::Primary,
            ),
            Expression::Operator(operator) => self.format_operator(operator, next_depth, budget),
            Expression::Subquery(subquery) => {
                let kind = match subquery.subquery_type {
                    crate::expression::SubqueryType::Scalar => "SUBQUERY",
                    crate::expression::SubqueryType::Exists => "EXISTS SUBQUERY",
                    crate::expression::SubqueryType::NotExists => "NOT EXISTS SUBQUERY",
                    crate::expression::SubqueryType::Any => "ANY SUBQUERY",
                    crate::expression::SubqueryType::All => "ALL SUBQUERY",
                };
                if subquery.children.is_empty() {
                    (format!("<{kind}>"), ExplainPrecedence::Primary)
                } else {
                    (
                        format!(
                            "{} {} <{kind}>",
                            subquery
                                .children
                                .iter()
                                .map(|child| self.format_expression(
                                    child,
                                    ExplainPrecedence::Lowest,
                                    next_depth,
                                    budget,
                                ))
                                .collect::<Vec<_>>()
                                .join(", "),
                            subquery.comparison_type
                        ),
                        ExplainPrecedence::Comparison,
                    )
                }
            }
            Expression::Window(window) => (
                self.format_window(window, next_depth, budget),
                ExplainPrecedence::Primary,
            ),
        };
        let rendered = bound_explain_text(rendered);
        if precedence < parent_precedence {
            bound_explain_text(format!("({rendered})"))
        } else {
            rendered
        }
    }

    fn format_operator(
        &self,
        operator: &crate::expression::OperatorExpression,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> (String, ExplainPrecedence) {
        let child = |index: usize, precedence: ExplainPrecedence| {
            operator
                .children
                .get(index)
                .map(|child| self.format_expression(child, precedence, depth, budget))
        };
        let children = || {
            operator
                .children
                .iter()
                .map(|child| {
                    self.format_expression(child, ExplainPrecedence::Lowest, depth, budget)
                })
                .collect::<Vec<_>>()
        };
        match operator.operator_type {
            OperatorType::In | OperatorType::NotIn => (
                child(0, ExplainPrecedence::Comparison).map_or_else(
                    || "<invalid IN>".to_string(),
                    |left| {
                        format!(
                            "{} {}IN ({})",
                            left,
                            if operator.operator_type == OperatorType::NotIn {
                                "NOT "
                            } else {
                                ""
                            },
                            operator.children[1..]
                                .iter()
                                .map(|child| self.format_expression(
                                    child,
                                    ExplainPrecedence::Lowest,
                                    depth,
                                    budget,
                                ))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                ),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::Not => (
                child(0, ExplainPrecedence::Not)
                    .map(|child| format!("NOT {child}"))
                    .unwrap_or_else(|| "<invalid NOT>".to_string()),
                ExplainPrecedence::Not,
            ),
            OperatorType::IsNull => (
                child(0, ExplainPrecedence::Comparison)
                    .map(|child| format!("{child} IS NULL"))
                    .unwrap_or_else(|| "<invalid IS NULL>".to_string()),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::IsNotNull => (
                child(0, ExplainPrecedence::Comparison)
                    .map(|child| format!("{child} IS NOT NULL"))
                    .unwrap_or_else(|| "<invalid IS NOT NULL>".to_string()),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::Coalesce => (
                format!("COALESCE({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::Like | OperatorType::ILike => (
                match (
                    child(0, ExplainPrecedence::Comparison),
                    child(1, ExplainPrecedence::Comparison),
                ) {
                    (Some(left), Some(right)) => format!(
                        "{} {} {}",
                        left,
                        if operator.operator_type == OperatorType::Like {
                            "LIKE"
                        } else {
                            "ILIKE"
                        },
                        right
                    ),
                    _ => "<invalid LIKE>".to_string(),
                },
                ExplainPrecedence::Comparison,
            ),
            OperatorType::ArrayConstructor => (
                format!("[{}]", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::StructConstructor => (
                format!("({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::ArrayExtract => (
                match (
                    child(0, ExplainPrecedence::Primary),
                    child(1, ExplainPrecedence::Lowest),
                ) {
                    (Some(array), Some(index)) => format!("{array}[{index}]"),
                    _ => "<invalid array extract>".to_string(),
                },
                ExplainPrecedence::Primary,
            ),
            OperatorType::ErrorIfMultipleRows => (
                format!("error_if_multiple_rows({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
        }
    }

    fn format_window(
        &self,
        window: &crate::expression::WindowExpression,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        let mut rendered = format!(
            "{}({}) OVER (",
            window.function_name(),
            window
                .arguments()
                .iter()
                .map(|argument| self.format_expression(
                    argument,
                    ExplainPrecedence::Lowest,
                    depth,
                    budget,
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !window.partitions.is_empty() {
            rendered.push_str("PARTITION BY ");
            rendered.push_str(
                &window
                    .partitions
                    .iter()
                    .map(|partition| {
                        self.format_expression(partition, ExplainPrecedence::Lowest, depth, budget)
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !window.orders.is_empty() {
            if !window.partitions.is_empty() {
                rendered.push(' ');
            }
            rendered.push_str("ORDER BY ");
            rendered.push_str(
                &window
                    .orders
                    .iter()
                    .map(|order| {
                        format!(
                            "{} {} NULLS {}",
                            self.format_expression(
                                &order.expression,
                                ExplainPrecedence::Lowest,
                                depth,
                                budget,
                            ),
                            if order.ascending { "ASC" } else { "DESC" },
                            if order.nulls_first { "FIRST" } else { "LAST" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !window.partitions.is_empty() || !window.orders.is_empty() {
            rendered.push(' ');
        }
        rendered.push_str(match window.frame.frame_type {
            WindowFrameType::Rows => "ROWS BETWEEN ",
            WindowFrameType::Range => "RANGE BETWEEN ",
        });
        rendered.push_str(&self.format_window_bound(
            &window.frame.start_bound,
            window.frame.start_is_preceding,
            depth,
            budget,
        ));
        rendered.push_str(" AND ");
        rendered.push_str(&self.format_window_bound(
            &window.frame.end_bound,
            window.frame.end_is_preceding,
            depth,
            budget,
        ));
        rendered.push(')');
        bound_explain_text(rendered)
    }

    fn format_window_bound(
        &self,
        bound: &WindowFrameBound,
        preceding: bool,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        match bound {
            WindowFrameBound::Unbounded => format!(
                "UNBOUNDED {}",
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Offset(offset) => format!(
                "{} {}",
                self.format_expression(offset, ExplainPrecedence::Lowest, depth, budget),
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
        }
    }
}

pub(super) fn bound_explain_text(mut text: String) -> String {
    if text.len() <= EXPLAIN_EXPRESSION_MAX_BYTES {
        return text;
    }
    let mut boundary = EXPLAIN_EXPRESSION_MAX_BYTES.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text.truncate(boundary);
    text.push('…');
    text
}

pub(super) fn table_column_name(table: &TableCatalogEntry, column_id: usize) -> String {
    table
        .columns
        .get(column_id)
        .map(|column| column.name.clone())
        .unwrap_or_else(|| format!("<column {column_id}>"))
}

pub(super) fn format_predicate_tree<V: std::fmt::Display>(
    predicate: &PredicateTree<V>,
    table: &TableCatalogEntry,
) -> String {
    let budget = ExplainFormatBudget::new();
    format_predicate_tree_with_precedence(predicate, table, ExplainPrecedence::Lowest, 0, &budget)
}

fn format_predicate_tree_with_precedence<V: std::fmt::Display>(
    predicate: &PredicateTree<V>,
    table: &TableCatalogEntry,
    parent_precedence: ExplainPrecedence,
    depth: usize,
    budget: &ExplainFormatBudget,
) -> String {
    if !budget.enter(depth) {
        return "…".to_string();
    }
    let next_depth = depth + 1;
    let (rendered, precedence) = match predicate {
        PredicateTree::Leaf(predicate) => (
            format_predicate(predicate, table),
            ExplainPrecedence::Primary,
        ),
        PredicateTree::And(children) => (
            children
                .iter()
                .map(|child| {
                    format_predicate_tree_with_precedence(
                        child,
                        table,
                        ExplainPrecedence::And,
                        next_depth,
                        budget,
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            ExplainPrecedence::And,
        ),
        PredicateTree::Or(children) => (
            children
                .iter()
                .map(|child| {
                    format_predicate_tree_with_precedence(
                        child,
                        table,
                        ExplainPrecedence::Or,
                        next_depth,
                        budget,
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR "),
            ExplainPrecedence::Or,
        ),
    };
    if precedence < parent_precedence {
        bound_explain_text(format!("({rendered})"))
    } else {
        bound_explain_text(rendered)
    }
}

fn format_predicate<V: std::fmt::Display>(
    predicate: &Predicate<V>,
    table: &TableCatalogEntry,
) -> String {
    let name = |column_id: u32| table_column_name(table, column_id as usize);
    match predicate {
        Predicate::Eq { column_id, value } => format!("{} = {value}", name(*column_id)),
        Predicate::NotEq { column_id, value } => format!("{} != {value}", name(*column_id)),
        Predicate::Lt { column_id, value } => format!("{} < {value}", name(*column_id)),
        Predicate::Le { column_id, value } => format!("{} <= {value}", name(*column_id)),
        Predicate::Gt { column_id, value } => format!("{} > {value}", name(*column_id)),
        Predicate::Ge { column_id, value } => format!("{} >= {value}", name(*column_id)),
        Predicate::In { column_id, values } => format!(
            "{} IN ({})",
            name(*column_id),
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Predicate::FixedIn { column_id, values } => {
            format!("{} IN ({} fixed values)", name(*column_id), values.len())
        }
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => format!("{} BETWEEN {lower} AND {upper}", name(*column_id)),
        Predicate::IsNull { column_id } => format!("{} IS NULL", name(*column_id)),
        Predicate::IsNotNull { column_id } => format!("{} IS NOT NULL", name(*column_id)),
        Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        } => format!(
            "{} {} PREFIX {prefix:?}",
            name(*column_id),
            if *negated { "NOT" } else { "HAS" }
        ),
        Predicate::StringPrefixIn {
            column_id,
            prefixes,
        } => format!(
            "{} HAS PREFIX IN ({})",
            name(*column_id),
            prefixes
                .iter()
                .map(|prefix| format!("{prefix:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Predicate::StringLike {
            column_id,
            pattern,
            negated,
        } => format!(
            "{} {} {pattern:?}",
            name(*column_id),
            if *negated { "NOT LIKE" } else { "LIKE" }
        ),
        Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        } => format!(
            "{} {comparison} {}",
            name(*left_column_id),
            name(*right_column_id)
        ),
    }
}

pub(super) fn format_search_predicate(
    predicate: &crate::physical::specs::SearchPredicateTemplate,
    table: &TableCatalogEntry,
) -> String {
    format_predicate_tree(predicate.tree(), table)
}
