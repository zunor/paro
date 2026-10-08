// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cost and feasibility of a read-before-write materialized input.

use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_ENFORCER_STREAM_ROW,
};
use paro_common::error::Result;
use paro_planner::physical::{cost::CompactRange, PhysicalCost};

pub(crate) fn mutation_input(
    rows: CompactRange,
    row_width: u64,
    available_memory: u64,
    tasks: u16,
    calibration: &MachineCalibrationBundle,
) -> Result<Option<PhysicalCost>> {
    rows.checked_add(CompactRange::ZERO)?;
    let width = row_width.max(1);
    let bytes = if !rows.upper.is_finite() || rows.upper >= u64::MAX as f64 / width as f64 {
        u64::MAX
    } else {
        rows.upper.ceil().max(0.0) as u64 * width
    };
    // This ABI retains immutable chunks in memory. Spill permission must not
    // be misrepresented as a spill implementation for the mutation barrier.
    if bytes > available_memory {
        return Ok(None);
    }
    let mut work = LocalOperatorWork::default();
    work.add(OP_ENFORCER_STREAM_ROW, rows)?;
    let mut cost = calibration.fold_for_tasks(&work, ParallelWorkProfile::Pipeline, tasks)?;
    cost.peak_memory_upper = bytes;
    cost.validate()?;
    Ok(Some(cost))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_buffer_checks_upper_bound_before_admission() {
        let calibration = MachineCalibrationBundle::builtin_production();
        let rows = CompactRange::new(1.0, 3.0, 10.0).unwrap();
        assert!(mutation_input(rows, 8, 79, 1, &calibration)
            .unwrap()
            .is_none());
        let cost = mutation_input(rows, 8, 80, 1, &calibration)
            .unwrap()
            .unwrap();
        assert_eq!(cost.peak_memory_upper, 80);
        assert_eq!(cost.spill_bytes_expected, 0);
    }
}
