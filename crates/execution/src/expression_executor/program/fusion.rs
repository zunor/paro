// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Multi-output schedules compiled from safe expression DAG chains.

use std::collections::BTreeMap;

use paro_common::runtime_value::Value;
use paro_common::types::{LogicalType, StringView};
use paro_planner::expression::ComparisonType;

use paro_function::scalar::operators::arithmetic::{DecimalFactorChainPlan, DecimalOperandSide};

use super::{ExpressionConstant, PhysicalExpression};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDecimalFactorChain {
    pub producer_output: usize,
    pub consumer_output: usize,
    pub shared_slot: usize,
    pub consumer_shared_side: DecimalOperandSide,
    pub plan: DecimalFactorChainPlan,
}

/// One-pass evaluation of mutually exclusive `VARCHAR = constant` outputs.
///
/// The constants are distinct and inline, so a matching input row can stop at
/// the first equality. This preserves ordinary three-valued comparison
/// results while avoiding one full vector traversal per output.
#[derive(Debug, Clone)]
pub struct PhysicalVarcharEqualityDispatch {
    pub input_index: usize,
    pub outputs: Box<[usize]>,
    pub constants: Box<[StringView]>,
}

pub(super) fn compile_decimal_factor_chains(
    roots: &[PhysicalExpression],
    shared_nodes: &[PhysicalExpression],
    root_to_unique: &[usize],
    root_first_output: &[usize],
) -> Vec<PhysicalDecimalFactorChain> {
    let mut chains = Vec::new();
    for producer_output in 0..root_to_unique.len() {
        if root_first_output[producer_output] != producer_output {
            continue;
        }
        let PhysicalExpression::Shared(producer) = &roots[root_to_unique[producer_output]] else {
            continue;
        };
        let Some(PhysicalExpression::Function(producer_function)) = shared_nodes.get(producer.slot)
        else {
            continue;
        };

        for consumer_output in 0..root_to_unique.len() {
            if consumer_output == producer_output
                || root_first_output[consumer_output] != consumer_output
            {
                continue;
            }
            let PhysicalExpression::Function(consumer) = &roots[root_to_unique[consumer_output]]
            else {
                continue;
            };
            for consumer_shared_side in [DecimalOperandSide::Left, DecimalOperandSide::Right] {
                let shared_idx = usize::from(consumer_shared_side == DecimalOperandSide::Right);
                if !matches!(
                    consumer.children.get(shared_idx),
                    Some(PhysicalExpression::Shared(shared)) if shared.slot == producer.slot
                ) {
                    continue;
                }
                let Some(plan) = DecimalFactorChainPlan::try_new(
                    &producer_function.function,
                    &consumer.function,
                    consumer_shared_side,
                ) else {
                    continue;
                };
                chains.push(PhysicalDecimalFactorChain {
                    producer_output,
                    consumer_output,
                    shared_slot: producer.slot,
                    consumer_shared_side,
                    plan,
                });
                break;
            }
        }
    }
    chains
}

pub(super) fn compile_varchar_equality_dispatches(
    roots: &[PhysicalExpression],
    root_to_unique: &[usize],
    root_first_output: &[usize],
) -> Vec<PhysicalVarcharEqualityDispatch> {
    let mut candidates = BTreeMap::<usize, Vec<(usize, StringView)>>::new();
    for output in 0..root_to_unique.len() {
        if root_first_output[output] != output {
            continue;
        }
        let PhysicalExpression::Comparison(comparison) = &roots[root_to_unique[output]] else {
            continue;
        };
        if comparison.comparison_type != ComparisonType::Equal
            || comparison.left_type != LogicalType::Varchar
        {
            continue;
        }
        let Some((input_index, constant)) =
            equality_input_and_constant(comparison.left.as_ref(), comparison.right.as_ref())
        else {
            continue;
        };
        candidates
            .entry(input_index)
            .or_default()
            .push((output, constant));
    }

    candidates
        .into_iter()
        .filter_map(|(input_index, entries)| {
            if entries.len() < 3
                || entries.iter().enumerate().any(|(index, (_, value))| {
                    entries[..index]
                        .iter()
                        .any(|(_, candidate)| candidate == value)
                })
            {
                return None;
            }
            let (outputs, constants): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
            Some(PhysicalVarcharEqualityDispatch {
                input_index,
                outputs: outputs.into_boxed_slice(),
                constants: constants.into_boxed_slice(),
            })
        })
        .collect()
}

fn equality_input_and_constant(
    left: &PhysicalExpression,
    right: &PhysicalExpression,
) -> Option<(usize, StringView)> {
    direct_varchar_input(left)
        .zip(inline_varchar_constant(right))
        .or_else(|| direct_varchar_input(right).zip(inline_varchar_constant(left)))
}

fn direct_varchar_input(expression: &PhysicalExpression) -> Option<usize> {
    match expression {
        PhysicalExpression::ColumnRef(column) if column.return_type == LogicalType::Varchar => {
            Some(column.column_index)
        }
        PhysicalExpression::Reference(reference)
            if reference.return_type == LogicalType::Varchar =>
        {
            Some(reference.index)
        }
        _ => None,
    }
}

fn inline_varchar_constant(expression: &PhysicalExpression) -> Option<StringView> {
    let PhysicalExpression::Constant(ExpressionConstant {
        value: Value::Varchar(value),
        return_type: LogicalType::Varchar,
    }) = expression
    else {
        return None;
    };
    StringView::try_inline(value.as_bytes())
}
