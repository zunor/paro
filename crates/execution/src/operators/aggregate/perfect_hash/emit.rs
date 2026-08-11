// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::vector::VECTOR_SIZE;
use paro_function::scalar::FunctionExecContext;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::operators::aggregate::perfect_aggregate_hashtable::PerfectAggregateScanScratch;
use crate::operators::aggregate::perfect_aggregate_hashtable::PerfectAggregateStateFilter;
use crate::operators::output::ensure_source_output;
use crate::physical::specs::AggregateSpec;
use crate::runtime::breaker::{
    AggregateBuildCompactionReclaimer, AggregateFinalizedStateReclaimer, AggregateHandle,
    AggregateRuntimeState, HandleRef,
};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    BreakerHandleGlobal, PerfectHashAggregateEmitSourceLocal, SourceGlobal, SourceLocal,
};
use crate::runtime::ExpressionEvalInput;

#[derive(Debug, Clone)]
pub struct PerfectHashAggregateEmitSourceExec {
    pub handle: HandleRef<AggregateHandle>,
    pub spec: AggregateSpec,
}

impl PerfectHashAggregateEmitSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        Ok(SourceGlobal::PerfectHashAggregateEmit(Arc::new(
            BreakerHandleGlobal {
                handle: ctx.handles.get(self.handle)?,
            },
        )))
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        let mut local = PerfectHashAggregateEmitSourceLocal {
            state_filter: compile_state_filter(&self.spec)?,
            ..Default::default()
        };
        if !self.spec.having_filter.is_empty() {
            if self.spec.having_filter.len() != 1 {
                return Err(paro_error::internal(
                    "aggregate HAVING lowering requires one normalized predicate",
                ));
            }
            if local.state_filter.is_none() {
                local.having_executor = Some(ExpressionExecutor::with_expressions_for_session(
                    &self.spec.having_filter,
                    ctx.query.session.as_ref(),
                ));
            }
            local.having_selection = Some(paro_common::vector::SelectionVector::try_with_capacity(
                VECTOR_SIZE,
                ctx.query
                    .allocator(paro_common::allocator::MemoryTag::BaseTable),
            )?);
        }
        local.scan_scratch = Some(PerfectAggregateScanScratch::try_new(
            &self.spec.output_types[self.spec.grouping_key_count
                ..self.spec.grouping_key_count + self.spec.aggregates.len()],
            VECTOR_SIZE,
            ctx.query
                .allocator(paro_common::allocator::MemoryTag::BaseTable),
        )?);
        Ok(SourceLocal::PerfectHashAggregateEmit(local))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        ctx.cancel.check()?;
        let SourceGlobal::PerfectHashAggregateEmit(global) = global else {
            return Err(paro_error::internal(
                "perfect aggregate emit source global state mismatch",
            ));
        };
        if !global.handle.is_finalized() {
            return Err(paro_error::internal(
                "perfect aggregate emit source polled before handle was finalized",
            ));
        }
        let SourceLocal::PerfectHashAggregateEmit(local) = local else {
            return Err(paro_error::internal(
                "perfect aggregate emit source local state mismatch",
            ));
        };
        if local.table.is_none() {
            ctx.query.memory.unregister_reclaimer_by_name(
                &AggregateBuildCompactionReclaimer::name_for(&global.handle),
            );
            ctx.query.memory.unregister_reclaimer_by_name(
                &AggregateFinalizedStateReclaimer::name_for(&global.handle),
            );
            let Some(state) = global.handle.take_state()? else {
                return Ok(SourcePoll::Finished);
            };
            let AggregateRuntimeState::Perfect(state) = state else {
                return Err(paro_error::internal(
                    "aggregate handle does not contain perfect aggregate state",
                ));
            };
            if state.build_table.is_some() || !state.pending_tables.is_empty() {
                return Err(paro_error::internal(
                    "finalized perfect aggregate state retained mutable build tables",
                ));
            }
            local.table = Some(state.finalized_table.ok_or_else(|| {
                paro_error::internal("finalized perfect aggregate state has no table")
            })?);
        }
        ensure_source_output(output, &self.spec.output_types, VECTOR_SIZE)?;
        let table = local.table.as_mut().ok_or_else(|| {
            paro_error::internal("perfect aggregate emit source did not load table")
        })?;
        let scratch = local.scan_scratch.as_mut().ok_or_else(|| {
            paro_error::internal("perfect aggregate emit source has no scan scratch")
        })?;
        if let (Some(filter), Some(selection)) =
            (local.state_filter.as_ref(), local.having_selection.as_mut())
        {
            if table.scan_with_state_filter(
                &mut local.position,
                output,
                scratch,
                selection,
                filter,
            )? {
                return Ok(SourcePoll::Output);
            }
        } else if let (Some(executor), Some(selection)) = (
            local.having_executor.as_mut(),
            local.having_selection.as_mut(),
        ) {
            if table.scan_with_aggregate_filter(
                &mut local.position,
                output,
                scratch,
                selection,
                |aggregates, count, selection| {
                    executor.select_kernel(
                        0,
                        VectorKernelInput::from_eval_input(ExpressionEvalInput {
                            params: ctx.query.params.as_ref(),
                            columns: aggregates,
                        })
                        .with_count(count),
                        ctx.query,
                        selection,
                    )
                },
            )? {
                return Ok(SourcePoll::Output);
            }
        } else if table.scan_with_scratch(&mut local.position, output, scratch)? {
            return Ok(SourcePoll::Output);
        }
        output.try_set_cardinality(0)?;
        Ok(SourcePoll::Finished)
    }
}

pub(crate) fn compile_state_filter(
    spec: &AggregateSpec,
) -> Result<Option<PerfectAggregateStateFilter>> {
    let [paro_planner::expression::Expression::Comparison(comparison)] =
        spec.having_filter.as_ref()
    else {
        return Ok(None);
    };
    let (reference, constant_expr, comparison_type) = match &*comparison.left {
        paro_planner::expression::Expression::Reference(reference) => {
            (reference, &*comparison.right, comparison.comparison_type)
        }
        _ => match &*comparison.right {
            paro_planner::expression::Expression::Reference(reference) => (
                reference,
                &*comparison.left,
                match invert_comparison(comparison.comparison_type) {
                    Some(comparison) => comparison,
                    None => return Ok(None),
                },
            ),
            _ => return Ok(None),
        },
    };
    let Some(constant) =
        crate::physical::generator::predicate_builder::evaluate_bound_constant(constant_expr)?
    else {
        return Ok(None);
    };
    // A comparison with NULL evaluates to NULL, but aggregate finalization
    // still has to validate every state. Keep that case on the generic path.
    if constant.is_null() {
        return Ok(None);
    }
    let Some(aggregate) = spec.aggregates.get(reference.index) else {
        return Ok(None);
    };
    let paro_planner::expression::Expression::Aggregate(aggregate) = aggregate else {
        return Ok(None);
    };
    if aggregate.function.state_filter.is_none() {
        return Ok(None);
    }
    // State filters compare the function's finalized value directly. Requiring
    // an exact type match makes casts (including DECIMAL scale changes) an
    // explicit generic-HAVING boundary instead of relying on binder coercion
    // details or rounding inside a function-specific fast path.
    if !state_filter_types_match(&reference.return_type, &aggregate.return_type, &constant) {
        return Ok(None);
    }
    let Some(comparison) = map_comparison(comparison_type) else {
        return Ok(None);
    };
    Ok(Some(PerfectAggregateStateFilter {
        aggregate_index: reference.index,
        comparison,
        constant,
    }))
}

fn state_filter_types_match(
    reference_type: &paro_common::types::LogicalType,
    aggregate_type: &paro_common::types::LogicalType,
    constant: &paro_common::runtime_value::Value,
) -> bool {
    reference_type == aggregate_type && constant.logical_type() == *aggregate_type
}

fn map_comparison(
    comparison: paro_planner::expression::ComparisonType,
) -> Option<paro_function::aggregate::AggregateComparison> {
    use paro_function::aggregate::AggregateComparison as Target;
    use paro_planner::expression::ComparisonType as Source;
    Some(match comparison {
        Source::Equal => Target::Equal,
        Source::NotEqual => Target::NotEqual,
        Source::LessThan => Target::LessThan,
        Source::GreaterThan => Target::GreaterThan,
        Source::LessThanOrEqual => Target::LessThanOrEqual,
        Source::GreaterThanOrEqual => Target::GreaterThanOrEqual,
        Source::DistinctFrom | Source::NotDistinctFrom => return None,
    })
}

fn invert_comparison(
    comparison: paro_planner::expression::ComparisonType,
) -> Option<paro_planner::expression::ComparisonType> {
    use paro_planner::expression::ComparisonType;
    Some(match comparison {
        ComparisonType::Equal => ComparisonType::Equal,
        ComparisonType::NotEqual => ComparisonType::NotEqual,
        ComparisonType::LessThan => ComparisonType::GreaterThan,
        ComparisonType::GreaterThan => ComparisonType::LessThan,
        ComparisonType::LessThanOrEqual => ComparisonType::GreaterThanOrEqual,
        ComparisonType::GreaterThanOrEqual => ComparisonType::LessThanOrEqual,
        ComparisonType::DistinctFrom | ComparisonType::NotDistinctFrom => return None,
    })
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;

    use super::state_filter_types_match;

    #[test]
    fn state_filter_requires_the_exact_finalized_value_type() {
        let decimal_38_2 = LogicalType::Decimal {
            precision: 38,
            scale: 2,
        };
        assert!(state_filter_types_match(
            &decimal_38_2,
            &decimal_38_2,
            &Value::Decimal(30_000, 38, 2),
        ));
        assert!(!state_filter_types_match(
            &decimal_38_2,
            &decimal_38_2,
            &Value::Decimal(300_001, 38, 3),
        ));
        assert!(!state_filter_types_match(
            &LogicalType::Decimal {
                precision: 38,
                scale: 3,
            },
            &decimal_38_2,
            &Value::Decimal(30_000, 38, 2),
        ));
    }
}
