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

    match direction {
        ExpandDirection::Forward => {
            estimate_pattern_factor(stats, source_label, edge_label, target_label)
        }
        ExpandDirection::Backward => {
            estimate_pattern_factor(stats, target_label, edge_label, source_label)
        }
        ExpandDirection::Both => {
            estimate_pattern_factor(stats, source_label, edge_label, target_label)
                + estimate_pattern_factor(stats, target_label, edge_label, source_label)
        }
    }
}

pub(super) fn estimate_pattern_factor(
    stats: &dyn GraphStatsProvider,
    source_label: &str,
    edge_label: &str,
    target_label: &str,
) -> f64 {
    let source_count = stats.vertex_count(source_label).unwrap_or(1).max(1) as f64;
    stats
        .pattern_step_count(source_label, edge_label, target_label)
        .map(|count| (count as f64 / source_count).max(1.0 / source_count))
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
