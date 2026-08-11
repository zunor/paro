// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Multi-output schedules compiled from safe expression DAG chains.

use paro_function::scalar::operators::arithmetic::{DecimalFactorChainPlan, DecimalOperandSide};

use super::PhysicalExpression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalDecimalFactorChain {
    pub producer_output: usize,
    pub consumer_output: usize,
    pub shared_slot: usize,
    pub consumer_shared_side: DecimalOperandSide,
    pub plan: DecimalFactorChainPlan,
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
