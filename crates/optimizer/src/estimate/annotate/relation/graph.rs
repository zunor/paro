// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph traversal cardinality, path bounds and catalog graph identity.

use super::*;

pub(super) fn quantifier_bounds(quantifier: Option<&PathQuantifier>) -> (u64, u64) {
    match quantifier {
        None => (1, 1),
        Some(PathQuantifier::Plus) => (1, 4),
        Some(PathQuantifier::Star) => (0, 4),
        Some(PathQuantifier::Bounded { lower, upper }) => (*lower, upper.unwrap_or(4).min(4)),
    }
}

pub(super) fn hop_multiplier(min_hops: u64, max_hops: u64) -> f64 {
    if min_hops == 1 && max_hops == 1 {
        1.0
    } else {
        max_hops.max(min_hops.max(1)) as f64
    }
}

pub(super) fn estimate_expand_factor(
    stats: &dyn GraphStatsProvider,
    source_label: &str,
    edge_label: &str,
    target_label: &str,
    direction: paro_planner::logical::operator::ExpandDirection,
) -> f64 {
    use paro_planner::logical::operator::ExpandDirection;

    let source_count = stats.vertex_count(source_label).unwrap_or(1).max(1) as f64;
    let forward = stats.pattern_step_count(source_label, edge_label, target_label);
    let backward = stats.pattern_step_count(target_label, edge_label, source_label);
    let count = match direction {
        ExpandDirection::Forward => forward,
        ExpandDirection::Backward => backward,
        ExpandDirection::Both => forward.zip(backward).map(|(a, b)| a.saturating_add(b)),
    };
    count
        .map(|count| count as f64 / source_count)
        .or_else(|| stats.avg_degree(source_label))
        .unwrap_or(1.0)
}

pub(super) fn graph_name_for_plan(mut plan: &OwnedLogicalPlan) -> Option<&str> {
    loop {
        plan = match &plan.operator {
            LogicalOperator::GraphScan(scan) => return Some(scan.graph_name.as_str()),
            LogicalOperator::GraphExpand(expand) => expand.child.as_ref(),
            LogicalOperator::Filter(filter) => filter.child.as_ref(),
            LogicalOperator::EmptyResult(empty) => empty.child.as_ref(),
            _ => return None,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_planner::logical::operator::ExpandDirection;
    use paro_storage::index::graph::GraphStatistics;

    #[test]
    fn backward_degree_uses_the_traversal_source_population() {
        let stats = GraphStatistics::default()
            .with_vertex_count("person", 100)
            .with_vertex_count("city", 10)
            .with_pattern_step_count("person", "lives", "city", 100);
        assert_eq!(
            estimate_expand_factor(&stats, "person", "lives", "city", ExpandDirection::Forward),
            1.0
        );
        assert_eq!(
            estimate_expand_factor(&stats, "city", "lives", "person", ExpandDirection::Backward),
            10.0
        );
    }
}
