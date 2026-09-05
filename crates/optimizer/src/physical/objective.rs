// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! End-to-end physical objective profiles.
//!
//! The same value is carried by Memo goals and physical portfolios so search,
//! frontier pruning and runtime admission cannot silently use different
//! rankings for the same feasible candidates.

use std::cmp::Ordering;

use super::cost::SearchCost;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectiveProfile {
    Latency,
    Throughput,
    Memory,
    Robustness,
}

impl ObjectiveProfile {
    pub const fn stable_tag(self) -> u64 {
        match self {
            Self::Latency => 0,
            Self::Throughput => 1,
            Self::Memory => 2,
            Self::Robustness => 3,
        }
    }

    pub fn compare(self, left: &SearchCost, right: &SearchCost) -> Ordering {
        left.memory_completion
            .preference_cmp(right.memory_completion)
            .then_with(|| match self {
                Self::Latency => expected_makespan(left)
                    .total_cmp(&expected_makespan(right))
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
                    .then_with(|| left.peak_memory_upper.cmp(&right.peak_memory_upper)),
                Self::Throughput => left.resources_expected[0]
                    .total_cmp(&right.resources_expected[0])
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
                    .then_with(|| left.peak_memory_upper.cmp(&right.peak_memory_upper)),
                Self::Memory => left
                    .peak_memory_upper
                    .cmp(&right.peak_memory_upper)
                    .then_with(|| {
                        left.score
                            .risk_adjusted
                            .total_cmp(&right.score.risk_adjusted)
                    })
                    .then_with(|| left.spill_bytes_expected.cmp(&right.spill_bytes_expected)),
                Self::Robustness => left
                    .score
                    .range
                    .upper
                    .total_cmp(&right.score.range.upper)
                    .then_with(|| {
                        left.score
                            .risk_adjusted
                            .total_cmp(&right.score.risk_adjusted)
                    })
                    .then_with(|| left.peak_memory_upper.cmp(&right.peak_memory_upper)),
            })
    }
}

/// Capacity/dependency lower bound for the physical operating point. The
/// calibrated critical path already includes serial fractions, coordination
/// and worker efficiency; W/P prevents a plan with more aggregate work from
/// masquerading as arbitrarily parallel.
fn expected_makespan(cost: &SearchCost) -> f64 {
    let capacity = f64::from(cost.max_parallel_tasks.max(1));
    (cost.work_latency.expected / capacity).max(cost.critical_path.expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::cost::CompactRange;

    fn cost(work: f64, span: f64, tasks: u16) -> SearchCost {
        SearchCost {
            work_latency: CompactRange::point(work).unwrap(),
            critical_path: CompactRange::point(span).unwrap(),
            max_parallel_tasks: tasks,
            ..SearchCost::ZERO
        }
    }

    #[test]
    fn latency_combines_work_span_and_capacity() {
        let serial = cost(100.0, 100.0, 4);
        let parallel = cost(104.0, 26.0, 4);

        assert_eq!(
            ObjectiveProfile::Latency.compare(&parallel, &serial),
            Ordering::Less
        );
    }
}
