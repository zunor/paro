// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Execution of compiled multi-output expression schedules.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;
use paro_function::scalar::operators::arithmetic::{
    try_execute_decimal_factor_chain, DecimalOperandSide,
};

use super::*;
use crate::expression_executor::program::PhysicalDecimalFactorChain;

impl ExpressionExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_execute_decimal_factor_chain(
        chain: &PhysicalDecimalFactorChain,
        physical: &PhysicalExpressionProgram,
        states: &mut [CompiledExpressionState],
        input: VectorKernelInput<'_>,
        runtime: &dyn FunctionExecContext,
        result: &mut Chunk,
        shared: &mut SharedEvaluation<'_>,
    ) -> Result<bool> {
        let Some(PhysicalExpression::Function(producer)) = shared.nodes.get(chain.shared_slot)
        else {
            return Ok(false);
        };
        let PhysicalExpression::Function(consumer) = physical.root(chain.consumer_output) else {
            return Ok(false);
        };
        let consumer_state_idx = physical.root_state_index(chain.consumer_output);
        let Some(CompiledExpressionState::Function(consumer_state)) =
            states.get_mut(consumer_state_idx)
        else {
            return Ok(false);
        };
        let Some(producer_state_slot) = shared.states.get(chain.shared_slot).cloned() else {
            return Ok(false);
        };
        let Some(mut producer_state) = SharedStateLease::take(&producer_state_slot) else {
            return Ok(false);
        };

        let CompiledExpressionState::Function(producer_state) = producer_state.state_mut() else {
            return Ok(false);
        };
        let [producer_outer_state, producer_inner_state] =
            producer_state.child_states.as_mut_slice()
        else {
            return Ok(false);
        };
        let [consumer_left_state, consumer_right_state] =
            consumer_state.child_states.as_mut_slice()
        else {
            return Ok(false);
        };
        let consumer_other_idx =
            usize::from(chain.consumer_shared_side == DecimalOperandSide::Left);
        let consumer_other_state = if consumer_other_idx == 0 {
            consumer_left_state
        } else {
            consumer_right_state
        };

        let producer_outer = Self::execute_value(
            &producer.children[0],
            producer_outer_state,
            input.columns,
            input.selection,
            input.count,
            runtime,
            input.params,
            shared,
        )?;
        let producer_inner = Self::execute_value(
            &producer.children[1],
            producer_inner_state,
            input.columns,
            input.selection,
            input.count,
            runtime,
            input.params,
            shared,
        )?;
        let consumer_other = Self::execute_value(
            &consumer.children[consumer_other_idx],
            consumer_other_state,
            input.columns,
            input.selection,
            input.count,
            runtime,
            input.params,
            shared,
        )?;

        let (producer_result, consumer_result) = two_output_vectors(
            &mut result.data,
            chain.producer_output,
            chain.consumer_output,
        )?;
        let executed = try_execute_decimal_factor_chain(
            chain.plan,
            producer_outer.as_vector(),
            producer_inner.as_vector(),
            consumer_other.as_vector(),
            producer_result,
            consumer_result,
            input.count,
        )?;
        if executed {
            producer_result.set_len(input.count);
            consumer_result.set_len(input.count);
            let signature = shared.signature(input.selection, input.count);
            let slot = shared.slots.get_mut(chain.shared_slot).ok_or_else(|| {
                paro_error::internal("shared expression scratch slot out of bounds")
            })?;
            slot.value.set_value(producer_result.reference());
            slot.signature = Some(signature);
        }
        Ok(executed)
    }
}

fn two_output_vectors(
    outputs: &mut [Arc<Vector>],
    first: usize,
    second: usize,
) -> Result<(&mut Vector, &mut Vector)> {
    if first == second || first >= outputs.len() || second >= outputs.len() {
        return Err(paro_error::internal(format!(
            "invalid multi-output expression columns: first={first}, second={second}, outputs={}",
            outputs.len()
        )));
    }
    if first < second {
        let (before, after) = outputs.split_at_mut(second);
        Ok((
            Arc::make_mut(&mut before[first]),
            Arc::make_mut(&mut after[0]),
        ))
    } else {
        let (before, after) = outputs.split_at_mut(first);
        Ok((
            Arc::make_mut(&mut after[0]),
            Arc::make_mut(&mut before[second]),
        ))
    }
}
