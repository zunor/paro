// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Multi-output schedules compiled from safe expression DAG chains.

use paro_function::scalar::operators::arithmetic::is_decimal_factor_fusion;

use super::PhysicalExpression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDecimalFactorChain {
    pub producer_output: usize,
    pub consumer_output: usize,
    pub shared_slot: usize,
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
        if producer_function.children.len() != 2
            || !is_decimal_factor_fusion(&producer_function.function)
            || count_shared_uses(roots, producer.slot) != 2
        {
            continue;
        }

        let mut consumer_output = None;
        for output in 0..root_to_unique.len() {
            if output == producer_output || root_first_output[output] != output {
                continue;
            }
            let PhysicalExpression::Function(consumer) = &roots[root_to_unique[output]] else {
                continue;
            };
            if consumer.children.len() != 2
                || !is_decimal_factor_fusion(&consumer.function)
                || !matches!(
                    &consumer.children[0],
                    PhysicalExpression::Shared(shared) if shared.slot == producer.slot
                )
            {
                continue;
            }
            if consumer_output.replace(output).is_some() {
                consumer_output = None;
                break;
            }
        }
        if let Some(consumer_output) = consumer_output {
            chains.push(PhysicalDecimalFactorChain {
                producer_output,
                consumer_output,
                shared_slot: producer.slot,
            });
        }
    }
    chains
}

fn count_shared_uses(expressions: &[PhysicalExpression], slot: usize) -> usize {
    expressions
        .iter()
        .map(|expression| count_shared_uses_in(expression, slot))
        .sum()
}

fn count_shared_uses_in(expression: &PhysicalExpression, slot: usize) -> usize {
    match expression {
        PhysicalExpression::Shared(shared) => usize::from(shared.slot == slot),
        PhysicalExpression::Function(expression) => expression
            .children
            .iter()
            .map(|child| count_shared_uses_in(child, slot))
            .sum(),
        PhysicalExpression::Cast(expression) => count_shared_uses_in(&expression.child, slot),
        PhysicalExpression::Comparison(expression) => {
            count_shared_uses_in(&expression.left, slot)
                + count_shared_uses_in(&expression.right, slot)
        }
        PhysicalExpression::Conjunction(expression) => expression
            .children
            .iter()
            .map(|child| count_shared_uses_in(child, slot))
            .sum(),
        PhysicalExpression::Case(expression) => {
            count_shared_uses_in(&expression.check, slot)
                + count_shared_uses_in(&expression.result_if_true, slot)
                + count_shared_uses_in(&expression.result_if_false, slot)
        }
        PhysicalExpression::Operator(expression) => expression
            .children
            .iter()
            .map(|child| count_shared_uses_in(child, slot))
            .sum(),
        PhysicalExpression::Constant(_)
        | PhysicalExpression::Parameter(_)
        | PhysicalExpression::ColumnRef(_)
        | PhysicalExpression::Reference(_) => 0,
    }
}
