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
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::aggregate::{AggregateDirectUpdate, DirectGroupedAggregateProgram};
use paro_planner::expression::Expression;
use paro_storage::statistics::{BaseStatistics, StringStats};

const MIN_ALIGNMENT: usize = 8;
const VARLEN_REF_WIDTH: usize = 16;

#[derive(Debug, Clone)]
pub(crate) struct AggregateStateLayout {
    offsets: Box<[usize]>,
    total_size: usize,
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
            total_size: current,
        })
    }

    pub(crate) fn total_size(&self) -> usize {
        self.total_size
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

    pub(crate) fn min_max_from_stats(
        &self,
        stats: Option<&BaseStatistics>,
    ) -> Option<(i128, i128)> {
        if self.varchar {
            let stats = StringStats::get_data(stats?)?;
            if stats.max_string_length()? > 1 {
                return None;
            }
            let min = encode_single_byte_varchar(stats.min_bytes())?;
            let max = encode_single_byte_varchar(stats.max_bytes())?;
            return (min <= max).then_some((min, max));
        }
        stats
            .and_then(|stats| {
                Some((
                    integer_value(&stats.min_value()?)?,
                    integer_value(&stats.max_value()?)?,
                ))
            })
            .or_else(|| integer_type_bounds(&self.logical_type))
    }
}

fn integer_value(value: &Value) -> Option<i128> {
    match value {
        Value::TinyInt(value) => Some(i128::from(*value)),
        Value::SmallInt(value) => Some(i128::from(*value)),
        Value::Integer(value) => Some(i128::from(*value)),
        Value::BigInt(value) => Some(i128::from(*value)),
        Value::HugeInt(value) => Some(*value),
        Value::UTinyInt(value) => Some(i128::from(*value)),
        Value::USmallInt(value) => Some(i128::from(*value)),
        Value::UInteger(value) => Some(i128::from(*value)),
        Value::UBigInt(value) => Some(i128::from(*value)),
        Value::UHugeInt(value) => i128::try_from(*value).ok(),
        _ => None,
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

fn encode_single_byte_varchar(value: &[u8]) -> Option<i128> {
    match value {
        [] => Some(0),
        [byte] => Some(i128::from(*byte) + 1),
        _ => None,
    }
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
