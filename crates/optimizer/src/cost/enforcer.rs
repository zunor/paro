// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Resource-aware cost of physical property conversions.

use super::calibration::{
    LocalOperatorWork, MachineCalibrationBundle, ParallelWorkProfile, OP_ENFORCER_RANDOM_FETCH,
    OP_ENFORCER_SORT_COMPARE, OP_ENFORCER_SPILL_PAGE, OP_ENFORCER_STREAM_ROW,
};
use crate::physical::enforcer::EnforcerStep;
use crate::physical::{SearchCost, SpillPolicy};
use paro_common::error::Result;

/// Fixed-size evidence used to replay property-enforcement cost. The row
/// interval is the candidate output estimate; the grant fields make blocking
/// enforcers part of feasibility rather than an extraction-time surprise.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EnforcerCostInput {
    pub rows: crate::physical::cost::CompactRange,
    pub row_width_bytes: u64,
    pub hard_memory_bytes: u64,
    pub spill_policy: SpillPolicy,
    pub max_parallel_tasks: u16,
}

impl EnforcerCostInput {
    pub fn unbounded(rows: crate::physical::cost::CompactRange, row_width_bytes: u64) -> Self {
        Self {
            rows,
            row_width_bytes: row_width_bytes.max(1),
            hard_memory_bytes: u64::MAX,
            spill_policy: SpillPolicy::Allowed,
            max_parallel_tasks: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct EnforcerPhaseCost {
    /// Keep the discriminant outside `SearchCost` without boxing it. This
    /// value is ignored when `present` is false; the explicit bit preserves
    /// the semantic difference between no phase and a zero-work phase while
    /// keeping candidate costing allocation-free.
    cost: SearchCost,
    present: bool,
}

impl EnforcerPhaseCost {
    pub(crate) fn compose_after(self, input: SearchCost) -> Result<SearchCost> {
        if self.present {
            input.sequential(self.cost)
        } else {
            Ok(input)
        }
    }

    #[cfg(test)]
    pub(crate) fn phase(self) -> Option<SearchCost> {
        self.present.then_some(self.cost)
    }
}

pub(crate) fn enforcer_cost(
    steps: &[EnforcerStep],
    input: EnforcerCostInput,
    calibration: &MachineCalibrationBundle,
) -> Result<Option<EnforcerPhaseCost>> {
    if steps.is_empty() {
        return Ok(Some(EnforcerPhaseCost {
            cost: SearchCost::ZERO,
            present: false,
        }));
    }
    input
        .rows
        .checked_add(crate::physical::cost::CompactRange::ZERO)?;
    let mut work = LocalOperatorWork::default();
    let mut profile = ParallelWorkProfile::Pipeline;
    let mut peak_memory_upper = 0_u64;
    let mut spill_bytes_expected = 0_u64;
    let row_bytes_upper = bytes_for_rows(input.rows.upper, input.row_width_bytes);
    for step in steps {
        match step {
            EnforcerStep::Sort(_) | EnforcerStep::LocalSort(_) => {
                profile = ParallelWorkProfile::BlockingMerge;
                work.add(OP_ENFORCER_SORT_COMPARE, sort_work(input.rows)?)?;
                if row_bytes_upper > input.hard_memory_bytes {
                    if input.spill_policy == SpillPolicy::Forbidden {
                        return Ok(None);
                    }
                    peak_memory_upper = peak_memory_upper.max(input.hard_memory_bytes);
                    let spill_bytes = row_bytes_upper.saturating_mul(2);
                    spill_bytes_expected = spill_bytes_expected.saturating_add(spill_bytes);
                    work.add(
                        OP_ENFORCER_SPILL_PAGE,
                        crate::physical::cost::CompactRange::point(pages(spill_bytes) as f64)?,
                    )?;
                } else {
                    peak_memory_upper = peak_memory_upper.max(row_bytes_upper);
                }
            }
            EnforcerStep::MutationInputSpool { .. } | EnforcerStep::Spool => {
                // The current immutable materialized-handle ABI owns chunks in
                // memory. Advertising spill here would violate the runtime
                // contract, so a class that cannot contain the upper bound is
                // infeasible rather than silently overcommitted.
                if row_bytes_upper > input.hard_memory_bytes {
                    return Ok(None);
                }
                peak_memory_upper = peak_memory_upper.max(row_bytes_upper);
                work.add(OP_ENFORCER_STREAM_ROW, input.rows)?;
            }
            EnforcerStep::Fetch { values } | EnforcerStep::FetchPreservingOrder { values, .. } => {
                work.add(
                    OP_ENFORCER_RANDOM_FETCH,
                    scale_range(input.rows, values.len().max(1) as f64)?,
                )?;
            }
            EnforcerStep::Gather
            | EnforcerStep::RepartitionHash { .. }
            | EnforcerStep::RepartitionRange { .. }
            | EnforcerStep::MergeGather(_)
            | EnforcerStep::PrepareOrderedFetch(_)
            | EnforcerStep::Flatten
            | EnforcerStep::Factorize(_) => {
                work.add(OP_ENFORCER_STREAM_ROW, input.rows)?;
            }
        }
    }
    let mut result = calibration.fold_for_tasks(&work, profile, input.max_parallel_tasks)?;
    result.peak_memory_upper = peak_memory_upper;
    result.spill_bytes_expected = spill_bytes_expected;
    result.validate()?;
    Ok(Some(EnforcerPhaseCost {
        cost: result,
        present: true,
    }))
}

fn scale_range(
    range: crate::physical::cost::CompactRange,
    factor: f64,
) -> Result<crate::physical::cost::CompactRange> {
    crate::physical::cost::CompactRange::new(
        range.lower * factor,
        range.expected * factor,
        range.upper * factor,
    )
}

fn sort_work(
    rows: crate::physical::cost::CompactRange,
) -> Result<crate::physical::cost::CompactRange> {
    let comparisons = |rows: f64| {
        if rows <= 1.0 {
            rows
        } else {
            rows * rows.log2()
        }
    };
    crate::physical::cost::CompactRange::new(
        comparisons(rows.lower),
        comparisons(rows.expected),
        comparisons(rows.upper),
    )
}

fn bytes_for_rows(rows: f64, width: u64) -> u64 {
    if !rows.is_finite() || rows >= u64::MAX as f64 / width.max(1) as f64 {
        u64::MAX
    } else {
        rows.ceil().max(0.0) as u64 * width.max(1)
    }
}

fn pages(bytes: u64) -> u64 {
    bytes.saturating_add(4095) / 4096
}
