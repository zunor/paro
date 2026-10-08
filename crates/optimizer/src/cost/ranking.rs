// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Ranking local implementations at a fixed resource operating point.
use paro_planner::physical::cost::PhysicalCost;
use std::cmp::Ordering;

/// Preserve the production latency policy: executable memory completion,
/// work/span makespan, then deterministic work, risk and memory tie breaks.
pub(crate) fn compare_latency(left: &PhysicalCost, right: &PhysicalCost) -> Ordering {
    fn makespan(cost: &PhysicalCost) -> f64 {
        (cost.work_latency.expected / f64::from(cost.max_parallel_tasks.max(1)))
            .max(cost.critical_path.expected)
    }
    left.memory_completion
        .preference_cmp(right.memory_completion)
        .then_with(|| makespan(left).total_cmp(&makespan(right)))
        .then_with(|| {
            left.work_latency
                .expected
                .total_cmp(&right.work_latency.expected)
        })
        .then_with(|| {
            left.score
                .range
                .expected
                .total_cmp(&right.score.range.expected)
        })
        .then_with(|| {
            left.score
                .risk_adjusted
                .total_cmp(&right.score.risk_adjusted)
        })
        .then_with(|| left.peak_memory_upper.cmp(&right.peak_memory_upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_planner::physical::cost::CompactRange;
    #[test]
    fn work_span_and_capacity_determine_makespan() {
        let cost = |work, span| PhysicalCost {
            work_latency: CompactRange::point(work).unwrap(),
            critical_path: CompactRange::point(span).unwrap(),
            max_parallel_tasks: 4,
            ..PhysicalCost::ZERO
        };
        assert_eq!(
            compare_latency(&cost(104.0, 26.0), &cost(100.0, 100.0)),
            Ordering::Less
        );
    }
}
