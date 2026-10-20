// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded ordering of fixed-length graph expansions. Keep binding identities
//! stable; choose a start and frontier order from graph cardinality evidence.

use std::collections::{BTreeMap, BTreeSet};

use super::limits::RegionLimits;
use crate::cost::graph::{expansion_work, scan_work};

#[derive(Default)]
pub(crate) struct Work {
    pub transitions: u64,
    pub budget_fallbacks: u64,
}
use crate::context::OptimizationContext;
use paro_catalog::entry::VertexTableInfo;
use paro_common::error::Result;
use paro_planner::expression::{ConjunctionExpression, ConjunctionType, Expression};
use paro_planner::logical::operator::{ExpandDirection, GraphExpand, GraphScan, LogicalOperator};
use paro_planner::logical::plan::OwnedLogicalPlan;
use paro_storage::index::graph::GraphStatsProvider;

struct Candidate {
    work: f64,
    start: usize,
    order: Vec<(usize, bool)>,
}

struct Step<'a> {
    expand: &'a GraphExpand,
    source: usize,
    target: usize,
}

/// At most 32 vertices and 64 edges, with one greedy frontier per start.
/// No plan clones are created during search and unavailable evidence retains
/// the canonical order. Quantified/path-valued patterns keep their path order.
pub(crate) fn optimize(
    plan: OwnedLogicalPlan,
    ctx: &mut OptimizationContext,
    limits: &RegionLimits,
    work: &mut Work,
) -> Result<OwnedLogicalPlan> {
    plan.try_map_post_order(|mut plan| {
        ctx.session.cancellation.check()?;
        let LogicalOperator::Projection(project) = &mut plan.operator else {
            return Ok(plan);
        };
        let mut node = project.child.as_ref();
        let mut expands = Vec::new();
        let scan = loop {
            match &node.operator {
                LogicalOperator::GraphExpand(expand) => {
                    if expand.quantifier.is_some()
                        || expand.path_mode.is_some()
                        || expand.has_path_functions
                        || expands.len() == 64
                    {
                        return Ok(plan);
                    }
                    expands.push(expand.as_ref());
                    node = expand.child.as_ref();
                }
                LogicalOperator::GraphScan(scan) => break scan.as_ref(),
                _ => return Ok(plan),
            }
        };
        if expands.len() < 2 {
            return Ok(plan);
        }
        expands.reverse();
        let Some(stats) = ctx.graph_stats.get(&scan.graph_name) else {
            return Ok(plan);
        };
        let catalog = ctx.session.catalog();
        let txn = ctx.session.catalog_txn_view();
        let schema = catalog.get_schema(&txn, &scan.schema_name)?;
        let graph = schema.get_property_graph(&txn, &scan.graph_name)?;
        let mut vertices = BTreeMap::from([(scan.table_index, scan.vertex_info.clone())]);
        let mut predicates = scan.filter.iter().cloned().collect::<Vec<_>>();
        for expand in &expands {
            let Some(info) = graph
                .info
                .vertex_tables
                .iter()
                .find(|v| v.label == expand.target_label)
            else {
                return Ok(plan);
            };
            vertices.insert(expand.target_table_index, info.clone());
            predicates.extend(expand.edge_filter.iter().cloned());
            predicates.extend(expand.target_filter.iter().cloned());
        }
        if vertices.len() > 32
            || predicates.iter().any(|e| {
                e.evaluation_properties().is_reorder_fence()
                    || !e.evaluation_properties().is_infallible()
            })
        {
            return Ok(plan);
        }
        let steps = expands
            .iter()
            .map(|expand| Step {
                expand,
                source: expand.source_table_index,
                target: expand.target_table_index,
            })
            .collect::<Vec<_>>();
        let Some((start, order)) = choose_order(&vertices, &steps, stats.as_ref(), limits, work)
        else {
            return Ok(plan);
        };
        let mut bound = BTreeSet::from([start]);
        let mut current =
            OwnedLogicalPlan::synthetic(LogicalOperator::GraphScan(Box::new(GraphScan::new(
                vertices[&start].clone(),
                None,
                start,
                scan.output_table_index,
                vertices[&start].label.clone(),
                scan.graph_name.clone(),
                scan.schema_name.clone(),
            ))));
        for (edge_index, reverse) in order {
            let step = &steps[edge_index];
            let (source, target) = if reverse {
                (step.target, step.source)
            } else {
                (step.source, step.target)
            };
            let source_info = &vertices[&source];
            let target_info = &vertices[&target];
            let mut expand = GraphExpand::new(
                step.expand.edge_info.clone(),
                direction(step.expand.direction, reverse),
                source_info.label.clone(),
                source,
                step.expand.edge_table_index,
                target,
                scan.output_table_index,
                target_info.label.clone(),
                source_info.table_oid,
                target_info.table_oid,
                target_info.table_name.clone(),
                current,
            );
            expand.target_is_bound = bound.contains(&target);
            bound.insert(target);
            current = OwnedLogicalPlan::synthetic(LogicalOperator::GraphExpand(Box::new(expand)));
        }
        if let LogicalOperator::GraphExpand(expand) = &mut current.operator {
            expand.target_filter = match predicates.len() {
                0 => None,
                1 => predicates.pop(),
                _ => Some(Expression::Conjunction(
                    ConjunctionExpression::new(ConjunctionType::And, predicates).into(),
                )),
            };
        }
        project.child = Box::new(current);
        Ok(plan)
    })
}

fn direction(direction: ExpandDirection, reverse: bool) -> ExpandDirection {
    match (direction, reverse) {
        (ExpandDirection::Forward, true) => ExpandDirection::Backward,
        (ExpandDirection::Backward, true) => ExpandDirection::Forward,
        _ => direction,
    }
}

fn choose_order(
    vertices: &BTreeMap<usize, VertexTableInfo>,
    steps: &[Step<'_>],
    stats: &dyn GraphStatsProvider,
    limits: &RegionLimits,
    observed: &mut Work,
) -> Option<(usize, Vec<(usize, bool)>)> {
    let counts = vertices
        .iter()
        .map(|(index, vertex)| Some((*index, stats.vertex_count(&vertex.label)? as f64)))
        .collect::<Option<BTreeMap<_, _>>>()?;
    let edge_counts = steps
        .iter()
        .map(|step| {
            stats
                .edge_count(&step.expand.edge_info.label)
                .map(|n| n as f64)
        })
        .collect::<Option<Vec<_>>>()?;
    let mut best: Option<Candidate> = None;
    for (&start, &count) in &counts {
        let mut bound = BTreeSet::from([start]);
        let mut remaining = (0..steps.len()).collect::<BTreeSet<_>>();
        let mut order = Vec::new();
        let mut rows = count;
        let mut work = scan_work(count);
        while !remaining.is_empty() {
            let mut selected: Option<(f64, usize, bool, usize, f64)> = None;
            for &index in &remaining {
                let step = &steps[index];
                for reverse in [false, true] {
                    let (source, target) = if reverse {
                        (step.target, step.source)
                    } else {
                        (step.source, step.target)
                    };
                    if !bound.contains(&source) {
                        continue;
                    }
                    observed.transitions += 1;
                    let multiplicity = if step.expand.direction == ExpandDirection::Both {
                        2.0
                    } else {
                        1.0
                    };
                    let degree = edge_counts[index] * multiplicity / counts[&source].max(1.0);
                    let factor = if bound.contains(&target) {
                        degree / counts[&target].max(1.0)
                    } else {
                        degree
                    };
                    // Closing edges inspect adjacency but emit only matching
                    // pairs. Charge both traversal and full carrier copying.
                    let next_rows = rows * factor;
                    let next_work = expansion_work(rows, degree, next_rows, order.len() + 1);
                    if selected
                        .as_ref()
                        .is_none_or(|(score, ..)| next_work < *score)
                    {
                        selected = Some((next_work, index, reverse, target, next_rows));
                    }
                }
            }
            let (next_work, index, reverse, target, next_rows) = selected?;
            work += next_work;
            rows = next_rows;
            remaining.remove(&index);
            bound.insert(target);
            order.push((index, reverse));
        }
        if best.as_ref().is_none_or(|candidate| work < candidate.work) {
            best = Some(Candidate { work, start, order });
        }
    }
    // Connected edge subsets share the same estimated cardinality regardless
    // of traversal order. Keep one cheapest prefix per subset, with compact
    // predecessor receipts. A strict statement budget retains the legal greedy
    // candidate when exact search is too large or exhausts its allowance.
    if steps.len() <= usize::from(limits.exact_relations).min(16) {
        #[derive(Clone, Copy)]
        struct State {
            work: f64,
            rows: f64,
            bound: u64,
            previous: usize,
            edge: usize,
            reverse: bool,
        }
        let bits = vertices
            .keys()
            .enumerate()
            .map(|(bit, &key)| (key, 1u64 << bit))
            .collect::<BTreeMap<_, _>>();
        let full = (1usize << steps.len()) - 1;
        let mut remaining_budget = limits.connected_pairs as usize;
        'starts: for (&start, &count) in &counts {
            let mut states: Vec<Option<State>> = vec![None; full + 1];
            states[0] = Some(State {
                work: scan_work(count),
                rows: count,
                bound: bits[&start],
                previous: 0,
                edge: 0,
                reverse: false,
            });
            for mask in 0..full {
                let Some(state) = states[mask] else {
                    continue;
                };
                for (index, step) in steps.iter().enumerate() {
                    if mask & (1 << index) != 0 {
                        continue;
                    }
                    for reverse in [false, true] {
                        let (source, target) = if reverse {
                            (step.target, step.source)
                        } else {
                            (step.source, step.target)
                        };
                        if state.bound & bits[&source] == 0 {
                            continue;
                        }
                        if remaining_budget == 0 {
                            observed.budget_fallbacks += 1;
                            break 'starts;
                        }
                        remaining_budget -= 1;
                        observed.transitions += 1;
                        let degree = edge_counts[index]
                            * if step.expand.direction == ExpandDirection::Both {
                                2.0
                            } else {
                                1.0
                            }
                            / counts[&source].max(1.0);
                        let factor = if state.bound & bits[&target] != 0 {
                            degree / counts[&target].max(1.0)
                        } else {
                            degree
                        };
                        let rows = state.rows * factor;
                        let next = mask | (1 << index);
                        let work = state.work
                            + expansion_work(state.rows, degree, rows, next.count_ones() as usize);
                        if states[next].is_none_or(|old| work < old.work) {
                            states[next] = Some(State {
                                work,
                                rows,
                                bound: state.bound | bits[&target],
                                previous: mask,
                                edge: index,
                                reverse,
                            });
                        }
                    }
                }
            }
            if let Some(final_state) = states[full] {
                if best
                    .as_ref()
                    .is_none_or(|candidate| final_state.work < candidate.work)
                {
                    let mut order = Vec::with_capacity(steps.len());
                    let mut mask = full;
                    while mask != 0 {
                        let state = states[mask].unwrap();
                        order.push((state.edge, state.reverse));
                        mask = state.previous;
                    }
                    order.reverse();
                    best = Some(Candidate {
                        work: final_state.work,
                        start,
                        order,
                    });
                }
            }
        }
    }
    best.map(|candidate| (candidate.start, candidate.order))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::EdgeTableInfo;
    use paro_storage::index::graph::GraphStatistics;

    fn vertex(index: usize) -> VertexTableInfo {
        VertexTableInfo {
            table_name: format!("v{index}"),
            table_oid: index as u64,
            key_column_ids: vec![0],
            label: format!("v{index}"),
            property_column_ids: vec![0],
        }
    }

    fn edge(source: usize, target: usize, index: usize) -> GraphExpand {
        let edge = EdgeTableInfo {
            table_name: format!("e{index}"),
            table_oid: (10 + index) as u64,
            key_column_ids: vec![],
            source_key_column_ids: vec![0],
            source_vertex_table: format!("v{source}"),
            source_ref_column_ids: vec![0],
            destination_key_column_ids: vec![1],
            destination_vertex_table: format!("v{target}"),
            destination_ref_column_ids: vec![0],
            label: format!("e{index}"),
            property_column_ids: vec![],
        };
        GraphExpand::new(
            edge,
            ExpandDirection::Forward,
            format!("v{source}"),
            source,
            10 + index,
            target,
            99,
            format!("v{target}"),
            source as u64,
            target as u64,
            format!("v{target}"),
            OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
        )
    }

    #[test]
    fn sparse_frontier_precedes_hub_fanout_and_budget_retains_legal_order() {
        // Two large populations joined through a tiny hub, and a sparse direct
        // edge. Starting with the hub fanout creates a large intermediate bag.
        let vertices = (0..3).map(|i| (i, vertex(i))).collect();
        let expands = [edge(0, 1, 0), edge(1, 2, 1), edge(0, 2, 2)];
        let steps = expands
            .iter()
            .map(|e| Step {
                expand: e,
                source: e.source_table_index,
                target: e.target_table_index,
            })
            .collect::<Vec<_>>();
        let stats = GraphStatistics::default()
            .with_vertex_count("v0", 10000)
            .with_vertex_count("v1", 10)
            .with_vertex_count("v2", 10000)
            .with_edge_count("e0", 10000)
            .with_edge_count("e1", 10000)
            .with_edge_count("e2", 100);
        for budget in [0, 65536] {
            let mut work = Work::default();
            let (start, order) = choose_order(
                &vertices,
                &steps,
                &stats,
                &RegionLimits {
                    connected_pairs: budget,
                    ..Default::default()
                },
                &mut work,
            )
            .unwrap();
            assert_eq!(order[0].0, 2, "the sparse edge must precede hub fanout");
            let mut bound = BTreeSet::from([start]);
            let mut used = BTreeSet::new();
            for (index, reverse) in order {
                assert!(used.insert(index));
                let step = &steps[index];
                let (source, target) = if reverse {
                    (step.target, step.source)
                } else {
                    (step.source, step.target)
                };
                assert!(bound.contains(&source));
                bound.insert(target);
            }
            assert_eq!(used.len(), 3);
            assert_eq!(bound.len(), 3);
            assert!(work.transitions > 0);
            assert_eq!(work.budget_fallbacks, u64::from(budget == 0));
        }
        assert!(choose_order(
            &vertices,
            &steps,
            &GraphStatistics::default(),
            &RegionLimits::default(),
            &mut Work::default()
        )
        .is_none());
    }
}
