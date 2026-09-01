// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Pure planning-time aggregate layout and admission calculations.
//!
//! This module intentionally contains no executor state, allocator, hash-table,
//! or vector dependency.  The optimizer uses it to decide whether a physical
//! aggregate implementation fits a resource grant; execution independently
//! validates and materializes the selected contract.

use std::mem::size_of;

use crate::physical::AggregateSpec;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_function::aggregate::{
    AggregateDirectUpdate, DecimalDirectUpdate, DirectGroupedAggregateProgram,
};
use paro_planner::expression::Expression;
use paro_planner::operator::Aggregate as LogicalAggregate;

use super::specs::aggregate::{PerfectHashAggregatePlan, PerfectHashResourceContract};

const MIN_ALIGNMENT: usize = 8;
const VARLEN_REF_WIDTH: usize = 16;

#[derive(Debug, Clone)]
pub(crate) struct AggregateStateLayout {
    offsets: Box<[usize]>,
}

impl AggregateStateLayout {
    pub(crate) fn from_spec(spec: &AggregateSpec) -> Result<Self> {
        validate_descriptor_lengths(spec)?;
        let mut offsets = Vec::with_capacity(spec.aggregates.len());
        let mut current = 0usize;
        for (index, expression) in spec.aggregates.iter().enumerate() {
            let Expression::Aggregate(aggregate) = expression else {
                return Err(paro_error::internal(format!(
                    "physical aggregate descriptor {index} is not an aggregate expression"
                )));
            };
            current = align_to(current, MIN_ALIGNMENT)?;
            offsets.push(current);
            let state_size = align_to(aggregate.function.state_size, MIN_ALIGNMENT)?;
            current = current
                .checked_add(state_size)
                .ok_or_else(|| paro_error::internal("aggregate state layout size overflow"))?;
        }
        Ok(Self {
            offsets: offsets.into_boxed_slice(),
        })
    }

    fn offset(&self, index: usize) -> Option<usize> {
        self.offsets.get(index).copied()
    }
}

pub(crate) fn compile_direct_update_program(
    spec: &AggregateSpec,
    layout: &AggregateStateLayout,
) -> Result<DirectGroupedAggregateProgram> {
    validate_descriptor_lengths(spec)?;
    let mut program = DirectGroupedAggregateProgram::new(spec.aggregates.len());
    for (index, expression) in spec.aggregates.iter().enumerate() {
        let Expression::Aggregate(aggregate) = expression else {
            return Err(paro_error::internal(format!(
                "physical aggregate descriptor {index} is not an aggregate expression"
            )));
        };
        if aggregate.is_distinct() || aggregate.filter.is_some() || !aggregate.order_bys.is_empty()
        {
            continue;
        }
        let input = match aggregate.function.direct_update {
            Some(AggregateDirectUpdate::CountStar) => None,
            _ => spec.aggregate_inputs[index].first().copied(),
        };
        let offset = layout.offset(index).ok_or_else(|| {
            paro_error::internal("aggregate state layout lost a descriptor offset")
        })?;
        program.try_add(
            index,
            aggregate.function.direct_update,
            offset,
            input,
            aggregate.function.state_is_trivially_copyable(),
        );
    }
    Ok(program)
}

pub(crate) fn perfect_hash_occupancy_bytes(slots: usize) -> Option<usize> {
    slots
        .div_ceil(u64::BITS as usize)
        .checked_mul(size_of::<u64>())
}

pub(crate) fn group_storage_width(group_types: &[LogicalType]) -> Result<usize> {
    let mut current = if group_types.is_empty() {
        0
    } else {
        group_types.len().div_ceil(8)
    };
    for logical_type in group_types {
        let varlen = is_varlen_group_type(logical_type);
        let width = if varlen {
            VARLEN_REF_WIDTH
        } else {
            let width = logical_type.type_size();
            if width == 0 {
                return Err(paro_error::internal(format!(
                    "unsupported aggregate group key type: {logical_type:?}"
                )));
            }
            width
        };
        current = align_to(
            current,
            if varlen {
                MIN_ALIGNMENT
            } else {
                width.min(MIN_ALIGNMENT)
            },
        )?;
        current = current
            .checked_add(width)
            .ok_or_else(|| paro_error::internal("aggregate group layout size overflow"))?;
    }
    align_to(current, MIN_ALIGNMENT)
}

fn validate_descriptor_lengths(spec: &AggregateSpec) -> Result<()> {
    let count = spec.aggregates.len();
    if spec.aggregate_inputs.len() != count
        || spec.aggregate_filters.len() != count
        || spec.aggregate_orders.len() != count
    {
        return Err(paro_error::internal(format!(
            "aggregate descriptor length mismatch: aggregates={count} inputs={} filters={} orders={}",
            spec.aggregate_inputs.len(),
            spec.aggregate_filters.len(),
            spec.aggregate_orders.len()
        )));
    }
    Ok(())
}

fn is_varlen_group_type(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::TsVector
            | LogicalType::TsQuery
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::Blob
    )
}

fn align_to(value: usize, alignment: usize) -> Result<usize> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(paro_error::internal(format!(
            "invalid aggregate layout alignment: {alignment}"
        )));
    }
    value
        .checked_add(alignment - 1)
        .map(|aligned| aligned & !(alignment - 1))
        .ok_or_else(|| paro_error::internal("aggregate layout alignment overflow"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PerfectHashPlanningDomain {
    logical_type: LogicalType,
    varchar: bool,
}

impl PerfectHashPlanningDomain {
    pub(crate) fn try_new(logical_type: LogicalType) -> Option<Self> {
        let varchar = logical_type == LogicalType::Varchar;
        (logical_type.is_integer() || varchar).then_some(Self {
            logical_type,
            varchar,
        })
    }

    /// Return only a domain that is invariant for every value of the SQL
    /// type. Table statistics are estimates for a particular snapshot and
    /// must never define the addressable range of a cacheable physical plan.
    pub(crate) fn invariant_bounds(&self) -> Option<(i128, i128)> {
        if self.varchar {
            return None;
        }
        integer_type_bounds(&self.logical_type)
    }
}

fn integer_type_bounds(logical_type: &LogicalType) -> Option<(i128, i128)> {
    match logical_type {
        LogicalType::TinyInt => Some((i128::from(i8::MIN), i128::from(i8::MAX))),
        LogicalType::SmallInt => Some((i128::from(i16::MIN), i128::from(i16::MAX))),
        LogicalType::Integer => Some((i128::from(i32::MIN), i128::from(i32::MAX))),
        LogicalType::BigInt => Some((i128::from(i64::MIN), i128::from(i64::MAX))),
        LogicalType::UTinyInt => Some((0, i128::from(u8::MAX))),
        LogicalType::USmallInt => Some((0, i128::from(u16::MAX))),
        LogicalType::UInteger => Some((0, i128::from(u32::MAX))),
        LogicalType::UBigInt => Some((0, i128::from(u64::MAX))),
        _ => None,
    }
}

const PERFECT_HASH_RANGE_LIMIT: u128 = 1u128 << 32;

/// Build the complete immutable perfect-hash contract used by enumeration,
/// costing, extraction, and execution. The key domain comes only from schema
/// invariants. `table_bytes_upper` deliberately over-accounts direct
/// update scratch so the executor can validate its exact allocation against
/// one contract without rediscovering feasibility.
pub(crate) fn plan_perfect_hash_aggregate(
    aggregate: &LogicalAggregate,
    groups: &[Expression],
    aggregate_exprs: &[Expression],
) -> Option<PerfectHashAggregatePlan> {
    if groups.is_empty()
        || aggregate.grouping_sets.len() > 1
        || !aggregate.grouping_functions.is_empty()
        || aggregate.groups.len() != groups.len()
    {
        return None;
    }

    let mut state_row_bytes = 0usize;
    let mut direct_scratch_per_slot = 0usize;
    let mut all_direct = true;
    for expression in aggregate_exprs {
        let Expression::Aggregate(function) = expression else {
            return None;
        };
        if function.is_distinct() || !function.order_bys.is_empty() {
            return None;
        }
        state_row_bytes = align_to(state_row_bytes, MIN_ALIGNMENT).ok()?;
        state_row_bytes = state_row_bytes
            .checked_add(align_to(function.function.state_size, MIN_ALIGNMENT).ok()?)?;
        match function.function.direct_update {
            Some(AggregateDirectUpdate::CountStar) => {}
            Some(AggregateDirectUpdate::Decimal(kind)) => {
                // Treat every aggregate as a distinct input source. The
                // executor may share sources, so this is an upper bound.
                let source_bytes = match kind {
                    DecimalDirectUpdate::NarrowSumI64 | DecimalDirectUpdate::AverageI64 => {
                        std::mem::size_of::<i64>() + std::mem::size_of::<i128>()
                    }
                    DecimalDirectUpdate::WideSumI128 | DecimalDirectUpdate::AverageI128 => {
                        // i256 is represented by four 64-bit limbs.
                        std::mem::size_of::<i128>() + 4 * std::mem::size_of::<u64>()
                    }
                };
                direct_scratch_per_slot = direct_scratch_per_slot.checked_add(source_bytes)?;
            }
            None => all_direct = false,
        }
    }
    state_row_bytes = state_row_bytes.max(1);

    let mut group_minima = Vec::with_capacity(aggregate.groups.len());
    let mut group_cardinalities = Vec::with_capacity(aggregate.groups.len());
    let mut slots = 1usize;
    for group in &aggregate.groups {
        let domain = PerfectHashPlanningDomain::try_new(group.return_type())?;
        let (minimum, maximum) = domain.invariant_bounds()?;
        let range = u128::try_from(maximum.checked_sub(minimum)?).ok()?;
        if range >= PERFECT_HASH_RANGE_LIMIT {
            return None;
        }
        let cardinality = usize::try_from(range.checked_add(2)?).ok()?;
        slots = slots.checked_mul(cardinality)?;
        group_minima.push(minimum);
        group_cardinalities.push(cardinality);
    }

    let state_bytes = state_row_bytes.checked_mul(slots)?;
    let state_storage_bytes = state_bytes
        .div_ceil(std::mem::size_of::<u64>())
        .checked_mul(std::mem::size_of::<u64>())?;
    let occupancy_bytes = perfect_hash_occupancy_bytes(slots)?;
    let scratch_slots = slots.min(VECTOR_SIZE);
    let scratch_bytes = if direct_scratch_per_slot == 0 {
        0
    } else {
        direct_scratch_per_slot
            .checked_add(std::mem::size_of::<usize>())?
            .checked_mul(scratch_slots)?
            .checked_add(scratch_slots.checked_mul(std::mem::size_of::<usize>())?)?
    };
    let materialized_slots = all_direct
        .then(|| VECTOR_SIZE.checked_mul(std::mem::size_of::<usize>()))
        .flatten()
        .unwrap_or(0);
    let table_bytes_upper = state_storage_bytes
        .checked_add(occupancy_bytes)?
        .checked_add(scratch_bytes)?
        .checked_add(materialized_slots)?;
    let per_task_scratch_bytes = groups
        .iter()
        .chain(aggregate_exprs.iter().filter_map(|expression| {
            let Expression::Aggregate(aggregate) = expression else {
                return None;
            };
            aggregate.children.first()
        }))
        .try_fold(0usize, |bytes, expression| {
            let width = expression.return_type().type_size().max(VARLEN_REF_WIDTH);
            bytes.checked_add(width.checked_mul(VECTOR_SIZE)?)
        })?
        .checked_add(VECTOR_SIZE.checked_mul(size_of::<u64>() + size_of::<u32>())?)?;
    let memory = crate::physical::ExecutionMemoryContract {
        fixed_non_revocable_bytes: u64::try_from(table_bytes_upper).ok()?,
        fixed_scratch_bytes: crate::physical::resources::BLOCKING_FIXED_SCRATCH_BYTES,
        per_task_scratch_bytes: u64::try_from(per_task_scratch_bytes).ok()?,
        max_concurrent_tasks: 1,
        revocable_minimum_bytes: 0,
        revocable_target_bytes: 0,
        spill_buffer_minimum_bytes: 0,
    };
    memory.validate().ok()?;

    Some(PerfectHashAggregatePlan {
        group_minima: group_minima.into_boxed_slice(),
        group_cardinalities: group_cardinalities.into_boxed_slice(),
        resource: PerfectHashResourceContract {
            slots,
            table_bytes_upper,
            memory,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_storage_matches_executor_abi() {
        assert_eq!(group_storage_width(&[LogicalType::Integer]).unwrap(), 8);
        assert_eq!(group_storage_width(&[LogicalType::Varchar]).unwrap(), 24);
    }

    #[test]
    fn occupancy_is_word_aligned() {
        assert_eq!(perfect_hash_occupancy_bytes(0), Some(0));
        assert_eq!(perfect_hash_occupancy_bytes(1), Some(8));
        assert_eq!(perfect_hash_occupancy_bytes(65), Some(16));
    }
}
