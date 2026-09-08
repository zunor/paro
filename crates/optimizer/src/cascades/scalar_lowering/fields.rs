// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One operand-field contract for native scalar import and export. Relational
//! child layouts and operator-private scalar scopes are deliberately distinct.

use paro_common::error::Result;
use paro_planner::expression::{Expression, WindowExpression};
use paro_planner::operator::{Join, JoinCondition, LogicalOperator};

#[derive(Debug, Clone, Copy)]
pub(super) enum ReferenceScope {
    Input,
    Output,
    Left,
    None,
    Reducers,
}

macro_rules! define_operand_fields {
    ($operand:ident, $visit:ident, [$($borrow:tt)*], $fallback:ident) => {
        pub(super) enum $operand<'a> {
            Expression(&'a $($borrow)* Expression, ReferenceScope),
            Comparison(&'a $($borrow)* JoinCondition),
            Window(&'a $($borrow)* WindowExpression),
        }

        pub(super) fn $visit<Child>(
            operator: &$($borrow)* LogicalOperator<Child>,
            mut callback: impl FnMut($operand<'_>) -> Result<()>,
        ) -> Result<()> {
            match operator {
                LogicalOperator::Get(get) => {
                    for expression in &$($borrow)* get.runtime_filter_expressions {
                        callback($operand::Expression(expression, ReferenceScope::Output))?;
                    }
                }
                LogicalOperator::Aggregate(aggregate) => {
                    for expression in &$($borrow)* aggregate.groups {
                        callback($operand::Expression(expression, ReferenceScope::Input))?;
                    }
                    for expression in &$($borrow)* aggregate.aggregates {
                        callback($operand::Expression(expression, ReferenceScope::Input))?;
                    }
                    if let Some(reduction) = &$($borrow)* aggregate.post_reduction {
                        for reducer in &$($borrow)* reduction.reducers {
                            callback($operand::Expression(reducer, ReferenceScope::None))?;
                        }
                        for expression in &$($borrow)* reduction.scalar_expressions {
                            callback($operand::Expression(expression, ReferenceScope::Reducers))?;
                        }
                        callback($operand::Expression(&$($borrow)* reduction.predicate, ReferenceScope::None))?;
                    }
                }
                LogicalOperator::Join(Join::Comparison(join)) => {
                    for condition in &$($borrow)* join.conditions {
                        callback($operand::Comparison(condition))?;
                    }
                    for expression in &$($borrow)* join.duplicate_eliminated_columns {
                        callback($operand::Expression(expression, ReferenceScope::Left))?;
                    }
                }
                LogicalOperator::Window(window) => {
                    for expression in &$($borrow)* window.expressions {
                        callback($operand::Window(expression))?;
                    }
                }
                LogicalOperator::GraphScan(scan) => {
                    if let Some(expression) = &$($borrow)* scan.filter {
                        callback($operand::Expression(expression, ReferenceScope::Output))?;
                    }
                }
                LogicalOperator::GraphExpand(expand) => {
                    if let Some(expression) = &$($borrow)* expand.edge_filter {
                        callback($operand::Expression(expression, ReferenceScope::Input))?;
                    }
                    if let Some(expression) = &$($borrow)* expand.target_filter {
                        callback($operand::Expression(expression, ReferenceScope::Input))?;
                    }
                }
                LogicalOperator::GraphMatch(graph) => {
                    for element in &$($borrow)* graph.bound_pattern.elements {
                        let filter = match element {
                            paro_planner::binder::bind::graph::BoundPatternElement::Vertex(vertex) => &$($borrow)* vertex.filter,
                            paro_planner::binder::bind::graph::BoundPatternElement::Edge(edge) => &$($borrow)* edge.filter,
                        };
                        if let Some(expression) = filter {
                            callback($operand::Expression(expression, ReferenceScope::Input))?;
                        }
                    }
                    for column in &$($borrow)* graph.columns {
                        callback($operand::Expression(&$($borrow)* column.expr, ReferenceScope::Input))?;
                    }
                }
                _ => {
                    let mut error = None;
                    paro_planner::visitor::$fallback(operator, |expression| {
                        if error.is_none() {
                            error = callback($operand::Expression(expression, ReferenceScope::Input)).err();
                        }
                    });
                    if let Some(error) = error {
                        return Err(error);
                    }
                }
            }
            Ok(())
        }
    };
}

define_operand_fields!(OperandRef, visit_fields, [], enumerate_expression_refs);
define_operand_fields!(OperandMut, visit_fields_mut, [mut], enumerate_expressions);
