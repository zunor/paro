// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Execution of compiled multi-output expression schedules.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::vector::Vector;
use paro_function::scalar::operators::arithmetic::try_execute_decimal_factor_chain;

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
        let Some(mut producer_state) = shared
            .states
            .get_mut(chain.shared_slot)
            .and_then(Option::take)
        else {
            return Ok(false);
        };

        let execution = catch_unwind(AssertUnwindSafe(|| {
            let CompiledExpressionState::Function(producer_state) = &mut producer_state else {
                return Ok(false);
            };
            let [producer_outer_state, producer_inner_state] =
                producer_state.child_states.as_mut_slice()
            else {
                return Ok(false);
            };
            let [_, consumer_inner_state] = consumer_state.child_states.as_mut_slice() else {
                return Ok(false);
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
            let consumer_inner = Self::execute_value(
                &consumer.children[1],
                consumer_inner_state,
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
                &producer.function,
                &consumer.function,
                producer_outer.as_vector(),
                producer_inner.as_vector(),
                consumer_inner.as_vector(),
                producer_result,
                consumer_result,
                input.count,
            )?;
            if executed {
                producer_result.set_len(input.count);
                consumer_result.set_len(input.count);
            }
            Ok(executed)
        }));
        shared.states[chain.shared_slot] = Some(producer_state);
        match execution {
            Ok(result) => result,
            Err(payload) => resume_unwind(payload),
        }
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
