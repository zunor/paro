// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Native staging for the closed scan-prefix subset of late payload lowering.
//!
//! Row-id payload fetching remains on the authoritative owned rule.  This
//! adapter only handles the local Projection -> Filter -> Get rewrite whose
//! semantic witness is an exact ASCII membership predicate.  It therefore
//! avoids importing an owned tree without claiming that the full rule has
//! been migrated.

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::ScalarPredicateProjection;
use paro_planner::expression::{ConjunctionType, Expression, OperatorType};
use paro_planner::operator::{ColumnBinding, Filter, Get, LogicalOperator, Projection};

use super::staging::{NativeChild, NativeShell};
use super::{Memo, PatternOperand, PlannerTransformState, boundary};

/// Try the exact scan-prefix part of LatePayloadFetch on the native shell.
///
/// A native miss deliberately returns None so the owned implementation can
/// still handle row-id paths, joins, and other shapes outside this contract.
pub(super) fn try_native_late_payload_prefix(
    binding: &PatternOperand,
    memo: &Memo,
    state: &PlannerTransformState,
    facts: &boundary::BoundarySnapshot,
) -> Result<Option<NativeShell>> {
    let Some((shell, layouts)) =
        NativeShell::from_pattern_with_layouts(memo, state, binding, facts)?
    else {
        return Ok(None);
    };
    if super::native_shell_contains_control_boundary(&shell) {
        return Ok(None);
    }

    let root = shell.root;
    let original_layout = layouts
        .get(root)
        .cloned()
        .ok_or_else(|| paro_error::internal("native late-payload shell has no root layout"))?;
    let LogicalOperator::Projection(projection) = shell.root_operator().clone() else {
        return Ok(None);
    };
    let NativeChild::Node(filter_index) = projection.child.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Filter(filter) = shell
        .nodes
        .get(filter_index)
        .ok_or_else(|| paro_error::internal("native late-payload shell lost its filter"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };
    let NativeChild::Node(get_index) = filter.child.clone() else {
        return Ok(None);
    };
    let LogicalOperator::Get(mut get) = shell
        .nodes
        .get(get_index)
        .ok_or_else(|| paro_error::internal("native late-payload shell lost its Get"))?
        .operator
        .clone()
    else {
        return Ok(None);
    };

    let Some(candidate) = prove_prefix_candidate(&projection, &filter, &get) else {
        return Ok(None);
    };
    let source_type = get
        .column_types
        .get(candidate.source_binding.column_index)
        .cloned()
        .ok_or_else(|| paro_error::internal("native prefix source type is missing"))?;
    let source_column = get
        .stored_column(candidate.source_binding.column_index)
        .ok_or_else(|| paro_error::internal("native prefix source is not a stored column"))?;
    let derived_binding =
        get.append_matched_utf8_prefix(source_column, candidate.byte_width, source_type);

    let mut projection = projection;
    for output_index in candidate.output_indices {
        let expression = projection
            .expressions
            .get_mut(output_index)
            .ok_or_else(|| paro_error::internal("native prefix output ordinal is stale"))?;
        *expression = Expression::ColumnRef(
            paro_planner::expression::ColumnRefExpression::new(
                derived_binding,
                LogicalType::Varchar,
            )
            .into(),
        );
    }
    projection.returned_types = projection
        .expressions
        .iter()
        .map(Expression::return_type)
        .collect();

    let get_width_before = layouts
        .get(get_index)
        .ok_or_else(|| paro_error::internal("native prefix shell lost Get layout"))?
        .len();
    if derived_binding.column_index != get_width_before {
        return Err(paro_error::internal(
            "native prefix Get appended a non-suffix output",
        ));
    }
    let mut filter = filter;
    filter.projection_map.include(derived_binding.column_index);

    let mut nodes = shell.nodes.into_vec();
    nodes[get_index].operator = LogicalOperator::Get(get);
    nodes[get_index].source_proofs = Box::new([]);
    nodes[filter_index].operator = LogicalOperator::Filter(filter);
    nodes[filter_index].source_proofs = Box::new([]);
    nodes[root].operator = LogicalOperator::Projection(projection);
    nodes[root].source_proofs = Box::new([]);

    let (shell, result_layout) = super::compact_native_shell_with_layout(NativeShell {
        nodes: nodes.into_boxed_slice(),
        root,
    })?;
    if result_layout != original_layout {
        return Ok(None);
    }
    Ok(Some(shell))
}

struct PrefixCandidate {
    source_binding: ColumnBinding,
    byte_width: usize,
    output_indices: Vec<usize>,
}

fn prove_prefix_candidate(
    projection: &Projection<NativeChild>,
    filter: &Filter<NativeChild>,
    get: &Get,
) -> Option<PrefixCandidate> {
    let table = get
        .table
        .as_ref()
        .filter(|table| table.get_storage().is_some())?;
    let mut candidate: Option<PrefixCandidate> = None;
    for (output_index, expression) in projection.expressions.iter().enumerate() {
        let Expression::Function(function) = expression else {
            continue;
        };
        let Some(ScalarPredicateProjection::Utf8Substring {
            source_argument,
            start: 1,
            length: Some(length),
        }) = function.function.predicate_projection.as_ref()
        else {
            continue;
        };
        let byte_width = usize::try_from(*length).ok().filter(|width| *width > 0)?;
        let Expression::ColumnRef(source) = function.children.get(*source_argument)? else {
            return None;
        };
        if source.depth != 0
            || source.binding.table_index != get.table_index
            || source.return_type != LogicalType::Varchar
        {
            return None;
        }
        let catalog_column = get.stored_column(source.binding.column_index)?;
        if table
            .columns
            .get(catalog_column)
            .is_none_or(|definition| definition.logical_type != source.return_type)
        {
            return None;
        }
        if !filter.expressions.iter().any(|predicate| {
            prove_prefix_filter_expression(
                predicate,
                source.binding,
                &function.function,
                byte_width,
            )
        }) {
            return None;
        }
        match &mut candidate {
            Some(candidate)
                if candidate.source_binding == source.binding
                    && candidate.byte_width == byte_width =>
            {
                candidate.output_indices.push(output_index);
            }
            None => {
                candidate = Some(PrefixCandidate {
                    source_binding: source.binding,
                    byte_width,
                    output_indices: vec![output_index],
                });
            }
            Some(_) => return None,
        }
    }
    candidate
}

fn prove_prefix_filter_expression(
    expression: &Expression,
    source_binding: ColumnBinding,
    kernel: &paro_function::scalar::BoundScalarFunction,
    byte_width: usize,
) -> bool {
    if let Expression::Conjunction(conjunction) = expression {
        if conjunction.conjunction_type != ConjunctionType::And {
            return false;
        }
        return conjunction.children.iter().any(|child| {
            prove_prefix_filter_expression(child, source_binding, kernel, byte_width)
        });
    }
    let Expression::Operator(operator) = expression else {
        return false;
    };
    if operator.operator_type != OperatorType::In || operator.children.len() < 2 {
        return false;
    }
    let Expression::Function(projected) = &operator.children[0] else {
        return false;
    };
    let Some(ScalarPredicateProjection::Utf8Substring {
        source_argument,
        start: 1,
        length: Some(length),
    }) = projected.function.predicate_projection.as_ref()
    else {
        return false;
    };
    if usize::try_from(*length).ok() != Some(byte_width)
        || !crate::aggregate::semantic_kernels::scalar_kernels_equal(&projected.function, kernel)
        || !matches!(
            projected.children.get(*source_argument),
            Some(Expression::ColumnRef(column))
                if column.depth == 0 && column.binding == source_binding
        )
        || !operator.children[1..].iter().all(|child| {
            matches!(child, Expression::Constant(constant)
                if matches!(&constant.value, Value::Varchar(value)
                    if value.is_ascii() && value.len() == byte_width))
        })
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_function::scalar::ScalarBindInput;
    use paro_function::scalar::string::get_substring_functions;
    use paro_planner::expression::{
        ColumnRefExpression, ConstantExpression, FunctionExpression, OperatorExpression,
    };

    fn source(binding: ColumnBinding) -> Expression {
        Expression::ColumnRef(ColumnRefExpression::new(binding, LogicalType::Varchar).into())
    }

    fn substring(input: Expression) -> Expression {
        let functions = get_substring_functions();
        let (function, types) = functions
            .bind(&[
                LogicalType::Varchar,
                LogicalType::BigInt,
                LogicalType::BigInt,
            ])
            .unwrap();
        let bound = function
            .bind(&ScalarBindInput::new(
                types,
                vec![None, Some(Value::BigInt(1)), Some(Value::BigInt(2))],
            ))
            .unwrap();
        Expression::Function(
            FunctionExpression::new(
                bound,
                vec![
                    input,
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(1), LogicalType::BigInt).into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::BigInt(2), LogicalType::BigInt).into(),
                    ),
                ],
                LogicalType::Varchar,
            )
            .into(),
        )
    }

    #[test]
    fn prefix_witness_requires_ascii_constants_and_same_kernel() {
        let binding = ColumnBinding::new(7, 3);
        let projected = substring(source(binding));
        let Expression::Function(projected_function) = &projected else {
            unreachable!()
        };
        let predicate = Expression::Operator(
            OperatorExpression::new(
                OperatorType::In,
                vec![
                    substring(source(binding)),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("ab".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );
        assert!(prove_prefix_filter_expression(
            &predicate,
            binding,
            &projected_function.function,
            2,
        ));

        let non_ascii = Expression::Operator(
            OperatorExpression::new(
                OperatorType::In,
                vec![
                    substring(source(binding)),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("é".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );
        assert!(!prove_prefix_filter_expression(
            &non_ascii,
            binding,
            &projected_function.function,
            2,
        ));
    }
}
