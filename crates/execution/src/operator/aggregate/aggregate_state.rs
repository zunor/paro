// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Aggregate state layout metadata.

use paro_common::error::{self as paro_error, Result};

use super::aggregate_object::AggregateObject;

const MIN_STATE_ALIGNMENT: usize = 8;

/// In-memory layout of aggregate states in a row/state buffer.
#[derive(Debug, Clone)]
pub struct AggregateStateLayout {
    offsets: Vec<usize>,
    total_size: usize,
}

impl AggregateStateLayout {
    /// Build layout from aggregate objects.
    ///
    /// Every aggregate state is aligned to at least 8 bytes.
    pub fn new(aggregate_objects: &[AggregateObject]) -> Result<Self> {
        let mut offsets = Vec::with_capacity(aggregate_objects.len());
        let mut current_offset = 0usize;
        for (idx, aggregate) in aggregate_objects.iter().enumerate() {
            if aggregate.payload_size % MIN_STATE_ALIGNMENT != 0 {
                return Err(paro_error::internal(format!(
                    "Aggregate payload_size must be {MIN_STATE_ALIGNMENT}-byte aligned at index {idx}: {}",
                    aggregate.payload_size
                )));
            }
            current_offset = align_to(current_offset, MIN_STATE_ALIGNMENT)?;
            offsets.push(current_offset);
            current_offset = current_offset
                .checked_add(aggregate.payload_size)
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Aggregate state layout overflow at index {idx}: offset={} payload={}",
                        current_offset, aggregate.payload_size
                    ))
                })?;
        }
        Ok(Self {
            offsets,
            total_size: current_offset,
        })
    }

    pub fn aggregate_count(&self) -> usize {
        self.offsets.len()
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }

    pub fn state_offset(&self, agg_idx: usize) -> usize {
        self.offsets[agg_idx]
    }
}

fn align_to(value: usize, alignment: usize) -> Result<usize> {
    debug_assert!(alignment.is_power_of_two());
    let addend = alignment - 1;
    value
        .checked_add(addend)
        .map(|aligned| aligned & !addend)
        .ok_or_else(|| {
            paro_error::internal(format!(
                "Failed to align aggregate state offset {value} to {alignment}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::types::LogicalType;
    use paro_function::aggregate::AggregateFunction;
    use paro_function::aggregate::AggregateInputData;
    use paro_planner::expression::AggregateExpression;

    unsafe fn initialize(_state: *mut u8) {}

    unsafe fn update(
        _inputs: &[&paro_common::vector::Vector],
        _input_data: &AggregateInputData,
        _states: &paro_common::vector::Vector,
        _count: usize,
    ) {
    }

    unsafe fn combine(
        _source: &paro_common::vector::Vector,
        _target: &paro_common::vector::Vector,
        _input_data: &AggregateInputData,
        _count: usize,
    ) {
    }

    unsafe fn finalize(
        _states: &paro_common::vector::Vector,
        _input_data: &AggregateInputData,
        _result: &mut paro_common::vector::Vector,
        _count: usize,
    ) {
    }

    fn make_test_object(payload_size: usize) -> AggregateObject {
        let function = AggregateFunction::new(
            "test".to_string(),
            vec![LogicalType::Integer],
            LogicalType::BigInt,
            payload_size,
            initialize,
            update,
            combine,
            finalize,
            None,
            None,
        );
        let bound = AggregateExpression::new(function, vec![], LogicalType::BigInt);
        AggregateObject::from_bound(&bound).expect("aggregate object")
    }

    #[test]
    fn state_layout_uses_aligned_offsets() {
        let objects = vec![make_test_object(1), make_test_object(9)];
        let layout = AggregateStateLayout::new(&objects).expect("layout");
        assert_eq!(layout.aggregate_count(), 2);
        assert_eq!(layout.state_offset(0), 0);
        assert_eq!(layout.state_offset(1), 8);
        assert_eq!(layout.total_size(), 24);
    }

    #[test]
    fn state_layout_rejects_unaligned_payload() {
        let mut objects = vec![make_test_object(8)];
        objects[0].payload_size = 3;
        let layout = AggregateStateLayout::new(&objects);
        assert!(layout.is_err());
    }
}
