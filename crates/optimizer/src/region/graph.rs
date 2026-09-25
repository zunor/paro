// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Deterministic bounded enumeration for fixed-length graph patterns.
//!
//! This module never chooses a start vertex. It preserves the binder order as
//! the mandatory baseline, ranks legal optional frontiers only to decide which
//! candidates fit the compile budget, and leaves final selection to Memo using
//! graph-snapshot cardinalities gathered after relationalization.

use std::cmp::Reverse;
use std::collections::BTreeSet;

use paro_parser::ast::EdgeDirection;
use paro_planner::binder::bind::graph::{
    BoundEdgeVariable, BoundPatternElement, BoundVertexVariable,
};
use paro_planner::operator::GraphMatch;

pub struct GraphFrontierEnumerator;

#[derive(Debug, Clone)]
struct PatternChain {
    vertices: Vec<BoundVertexVariable>,
    edges: Vec<BoundEdgeVariable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchOrder {
    LeftFirst,
    RightFirst,
}

impl GraphFrontierEnumerator {
    pub fn new() -> Self {
        Self
    }

    /// Enumerate a bounded, stable set of legal pattern orders.
    ///
    /// Candidate zero is always the binder order. Optional candidates are
    /// ranked by a cheap semantic-shape heuristic before truncation; that rank
    /// is never used as the final physical cost.
    pub fn enumerate_pattern_orders(
        &self,
        graph_match: &GraphMatch,
        max_candidates: usize,
    ) -> Vec<Vec<BoundPatternElement>> {
        let baseline = graph_match.bound_pattern.elements.clone();
        if max_candidates == 0 {
            return Vec::new();
        }
        if graph_match.has_path_functions || max_candidates == 1 {
            return vec![baseline];
        }

        let pattern = Self::parse_pattern(&baseline);
        if pattern.vertices.len() <= 1 {
            return vec![baseline];
        }

        let baseline_fingerprint = Self::pattern_order_fingerprint(&baseline);
        let mut fingerprints = BTreeSet::from([baseline_fingerprint]);
        let mut optional = Vec::new();
        for start_idx in 0..pattern.vertices.len() {
            for branch_order in [BranchOrder::LeftFirst, BranchOrder::RightFirst] {
                let reordered = Self::reorder_pattern(&pattern, start_idx, branch_order);
                let fingerprint = Self::pattern_order_fingerprint(&reordered);
                if fingerprints.insert(fingerprint.clone()) {
                    optional.push((
                        Reverse(Self::frontier_rank(&pattern.vertices[start_idx])),
                        fingerprint,
                        reordered,
                    ));
                }
            }
        }
        optional.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));

        let mut candidates = Vec::with_capacity(max_candidates.min(optional.len() + 1));
        candidates.push(baseline);
        candidates.extend(
            optional
                .into_iter()
                .take(max_candidates - 1)
                .map(|(_, _, order)| order),
        );
        candidates
    }

    fn frontier_rank(vertex: &BoundVertexVariable) -> u32 {
        let filter_rank = if vertex.filter.is_some() { 100 } else { 0 };
        let key_rank =
            if vertex.filter.is_some() && !vertex.vertex_table_info.key_column_ids.is_empty() {
                50
            } else {
                0
            };
        let width_rank = if vertex.vertex_table_info.property_column_ids.len() <= 3 {
            25
        } else {
            0
        };
        filter_rank + key_rank + width_rank
    }

    fn parse_pattern(elements: &[BoundPatternElement]) -> PatternChain {
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        for element in elements {
            match element {
                BoundPatternElement::Vertex(vertex) => vertices.push(vertex.clone()),
                BoundPatternElement::Edge(edge) => edges.push(edge.clone()),
            }
        }
        PatternChain { vertices, edges }
    }

    fn reorder_pattern(
        pattern: &PatternChain,
        start_idx: usize,
        branch_order: BranchOrder,
    ) -> Vec<BoundPatternElement> {
        let mut reordered = vec![BoundPatternElement::Vertex(
            pattern.vertices[start_idx].clone(),
        )];
        let left_steps = Self::left_branch_steps(pattern, start_idx);
        let right_steps = Self::right_branch_steps(pattern, start_idx);
        let ordered_steps = match branch_order {
            BranchOrder::LeftFirst => left_steps
                .into_iter()
                .chain(right_steps)
                .collect::<Vec<_>>(),
            BranchOrder::RightFirst => right_steps
                .into_iter()
                .chain(left_steps)
                .collect::<Vec<_>>(),
        };
        for (edge, target) in ordered_steps {
            reordered.push(BoundPatternElement::Edge(edge));
            reordered.push(BoundPatternElement::Vertex(target));
        }
        reordered
    }

    fn left_branch_steps(
        pattern: &PatternChain,
        start_idx: usize,
    ) -> Vec<(BoundEdgeVariable, BoundVertexVariable)> {
        let mut steps = Vec::new();
        for edge_idx in (0..start_idx).rev() {
            let mut edge = pattern.edges[edge_idx].clone();
            Self::flip_edge(&mut edge);
            steps.push((edge, pattern.vertices[edge_idx].clone()));
        }
        steps
    }

    fn right_branch_steps(
        pattern: &PatternChain,
        start_idx: usize,
    ) -> Vec<(BoundEdgeVariable, BoundVertexVariable)> {
        let mut steps = Vec::new();
        for edge_idx in start_idx..pattern.edges.len() {
            steps.push((
                pattern.edges[edge_idx].clone(),
                pattern.vertices[edge_idx + 1].clone(),
            ));
        }
        steps
    }

    fn flip_edge(edge: &mut BoundEdgeVariable) {
        edge.direction = match edge.direction {
            EdgeDirection::Right => EdgeDirection::Left,
            EdgeDirection::Left => EdgeDirection::Right,
            EdgeDirection::Undirected => EdgeDirection::Undirected,
            EdgeDirection::LeftRight => EdgeDirection::LeftRight,
        };
        std::mem::swap(&mut edge.source_variable, &mut edge.destination_variable);
    }

    fn pattern_order_fingerprint(elements: &[BoundPatternElement]) -> String {
        let mut fingerprint = String::new();
        for element in elements {
            match element {
                BoundPatternElement::Vertex(vertex) => {
                    fingerprint.push_str("v:");
                    fingerprint.push_str(&vertex.variable_name);
                    fingerprint.push(';');
                }
                BoundPatternElement::Edge(edge) => {
                    fingerprint.push_str("e:");
                    fingerprint.push_str(&edge.variable_name);
                    fingerprint.push(':');
                    fingerprint.push_str(&edge.source_variable);
                    fingerprint.push('>');
                    fingerprint.push_str(&edge.destination_variable);
                    fingerprint.push(':');
                    fingerprint.push_str(match edge.direction {
                        EdgeDirection::Right => "r",
                        EdgeDirection::Left => "l",
                        EdgeDirection::Undirected => "u",
                        EdgeDirection::LeftRight => "b",
                    });
                    fingerprint.push(';');
                }
            }
        }
        fingerprint
    }
}

impl Default for GraphFrontierEnumerator {
    fn default() -> Self {
        Self::new()
    }
}
