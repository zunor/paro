// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared task-supply policy for planning and execution.
//!
//! A resource grant describes how many workers a query may use. It does not
//! prove that a physical phase contains enough independent work to keep those
//! workers useful. Keeping this policy in `paro-common` lets the optimizer's
//! duration model and the runtime scheduler use the same amortization
//! boundary without either layer depending on the other.

/// Minimum physical byte-work required to justify one useful pipeline worker.
///
/// Work may still be split into smaller morsels for load balancing. This
/// threshold only bounds useful concurrent consumers and avoids treating
/// storage fragments or a large worker grant as latent parallel work.
pub const MIN_USEFUL_PIPELINE_WORK_BYTES: u64 = 4 * 1024 * 1024;

/// Bound admitted worker capacity by the amount of useful physical work.
///
/// Non-empty work always has one executable task. Empty work has no task in
/// the runtime, while callers representing a plan operating point may clamp
/// the result to one when their cost structure requires a non-zero capacity.
pub fn useful_pipeline_tasks(total_work_bytes: u64, admitted_tasks: usize) -> usize {
    if total_work_bytes == 0 || admitted_tasks == 0 {
        return 0;
    }
    let demanded = total_work_bytes.div_ceil(MIN_USEFUL_PIPELINE_WORK_BYTES);
    usize::try_from(demanded)
        .unwrap_or(usize::MAX)
        .min(admitted_tasks)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn useful_tasks_separate_work_supply_from_worker_grant() {
        assert_eq!(useful_pipeline_tasks(0, 8), 0);
        assert_eq!(useful_pipeline_tasks(1, 8), 1);
        assert_eq!(useful_pipeline_tasks(MIN_USEFUL_PIPELINE_WORK_BYTES, 8), 1);
        assert_eq!(
            useful_pipeline_tasks(MIN_USEFUL_PIPELINE_WORK_BYTES + 1, 8),
            2
        );
        assert_eq!(
            useful_pipeline_tasks(100 * MIN_USEFUL_PIPELINE_WORK_BYTES, 8),
            8
        );
    }
}
