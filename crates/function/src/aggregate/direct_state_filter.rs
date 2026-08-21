// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Prepared predicates over finalized fixed-width aggregate states.

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;

use super::distributive::decimal::{wide_sum_output_value, DecimalNarrowState, DecimalSumState};
use super::{AggregateComparison, AggregateFinalizeProjection, AggregateFunction};

/// Comparison compiled once for direct-address aggregate state traversal.
///
/// This capability is intentionally narrower than [`super::AggregateStateFilterFn`]:
/// it admits only fixed-width states whose validation and comparison can be
/// performed from one state address without vector materialization.
#[derive(Debug, Clone)]
pub struct PreparedDirectAggregateStatePredicate {
    comparison: AggregateComparison,
    constant: i128,
    projection: PreparedAggregateFinalizeProjection,
    state: PreparedDirectAggregateState,
}

#[derive(Debug, Clone)]
enum PreparedAggregateFinalizeProjection {
    Identity,
    DecimalCast {
        cast: super::super::decimal::PreparedI128DecimalCast,
        try_cast: bool,
    },
}

#[derive(Debug, Clone)]
enum PreparedDirectAggregateState {
    DecimalNarrowSum {
        output_limit: i128,
        output_precision: u8,
    },
    DecimalWideSum {
        input_scale: u8,
        output_scale: u8,
        output_limit: i128,
        output_precision: u8,
    },
}

impl PreparedDirectAggregateStatePredicate {
    pub(super) fn decimal_narrow_sum(
        comparison: AggregateComparison,
        constant: i128,
        projection: AggregateFinalizeProjection,
        source_scale: u8,
        output_limit: i128,
        output_precision: u8,
    ) -> Result<Self> {
        Ok(Self {
            comparison,
            constant,
            projection: prepare_decimal_projection(projection, source_scale)?,
            state: PreparedDirectAggregateState::DecimalNarrowSum {
                output_limit,
                output_precision,
            },
        })
    }

    pub(super) fn decimal_wide_sum(
        comparison: AggregateComparison,
        constant: i128,
        projection: AggregateFinalizeProjection,
        source_scale: u8,
        input_scale: u8,
        output_scale: u8,
        output_limit: i128,
        output_precision: u8,
    ) -> Result<Self> {
        Ok(Self {
            comparison,
            constant,
            projection: prepare_decimal_projection(projection, source_scale)?,
            state: PreparedDirectAggregateState::DecimalWideSum {
                input_scale,
                output_scale,
                output_limit,
                output_precision,
            },
        })
    }

    /// Evaluate one initialized aggregate state.
    ///
    /// # Safety
    ///
    /// `state` must point to the bound fixed-width state used to prepare this
    /// predicate and remain live for the duration of the call.
    #[inline(always)]
    pub unsafe fn matches(&self, state: *const u8) -> Result<bool> {
        let value = match self.state {
            PreparedDirectAggregateState::DecimalNarrowSum {
                output_limit,
                output_precision,
            } => {
                let state = unsafe { &*state.cast::<DecimalNarrowState>() };
                if !state.is_set() {
                    return Ok(false);
                }
                if state.overflowed() {
                    return Err(paro_error::out_of_range("Decimal SUM aggregate overflow"));
                }
                let value = state.value();
                if value.unsigned_abs() >= output_limit as u128 {
                    return Err(paro_error::out_of_range(format!(
                        "Decimal SUM result exceeds precision {output_precision}"
                    )));
                }
                value
            }
            PreparedDirectAggregateState::DecimalWideSum {
                input_scale,
                output_scale,
                output_limit,
                output_precision,
            } => {
                let state = unsafe { &*state.cast::<DecimalSumState>() };
                let Some(value) = wide_sum_output_value(
                    state,
                    input_scale,
                    output_scale,
                    output_limit,
                    output_precision,
                )?
                else {
                    return Ok(false);
                };
                value
            }
        };
        let Some(value) = project_decimal_value(value, &self.projection)? else {
            // TRY_CAST failure is NULL; an ordinary comparison with NULL is
            // UNKNOWN and therefore does not select the row.
            return Ok(false);
        };
        Ok(match self.comparison {
            AggregateComparison::Equal => value == self.constant,
            AggregateComparison::NotEqual => value != self.constant,
            AggregateComparison::LessThan => value < self.constant,
            AggregateComparison::GreaterThan => value > self.constant,
            AggregateComparison::LessThanOrEqual => value <= self.constant,
            AggregateComparison::GreaterThanOrEqual => value >= self.constant,
        })
    }
}

pub fn prepare_direct_state_predicate(
    function: &AggregateFunction,
    projection: &AggregateFinalizeProjection,
    comparison: AggregateComparison,
    constant: &Value,
) -> Result<Option<PreparedDirectAggregateStatePredicate>> {
    let Some(prepare) = function.direct_state_filter else {
        return Ok(None);
    };
    prepare(function, projection, comparison, constant)
}

fn project_decimal_value(
    value: i128,
    projection: &PreparedAggregateFinalizeProjection,
) -> Result<Option<i128>> {
    match projection {
        PreparedAggregateFinalizeProjection::Identity => Ok(Some(value)),
        PreparedAggregateFinalizeProjection::DecimalCast { cast, try_cast } => {
            let result = cast.cast(value);
            if *try_cast && result.is_err() {
                return Ok(None);
            }
            result.map(Some)
        }
    }
}

fn prepare_decimal_projection(
    projection: AggregateFinalizeProjection,
    source_scale: u8,
) -> Result<PreparedAggregateFinalizeProjection> {
    Ok(match projection {
        AggregateFinalizeProjection::Identity => PreparedAggregateFinalizeProjection::Identity,
        AggregateFinalizeProjection::DecimalCast {
            target_precision,
            target_scale,
            try_cast,
        } => PreparedAggregateFinalizeProjection::DecimalCast {
            cast: super::super::decimal::PreparedI128DecimalCast::new(
                source_scale,
                target_precision,
                target_scale,
            )?,
            try_cast,
        },
    })
}
