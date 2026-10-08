// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One connected-subgraph traversal. Candidate representation, grain and
//! admission are owned by the receiving planning domain, never by traversal.

use std::collections::HashSet;
use std::sync::Arc;

use super::enumerator::{get_all_neighbor_sets, EnumerationOutcome};
use super::query_graph::{NeighborInfo, QueryGraphEdges};
use super::relation::{JoinRelationSet, JoinRelationSetManager};

pub(crate) trait ConnectedRegion {
    fn relations(&self) -> usize;
    fn graph(&self) -> &QueryGraphEdges;
    fn sets(&mut self) -> &mut JoinRelationSetManager;
    fn contains(&self, set: &Arc<JoinRelationSet>) -> bool;
    fn emit(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        connections: &[NeighborInfo],
    ) -> EnumerationOutcome;
}

pub(crate) fn enumerate(region: &mut impl ConnectedRegion) -> EnumerationOutcome {
    for i in (1..=region.relations()).rev() {
        let start = region.sets().get_relation(i - 1);
        let outcome = emit_csg(region, &start);
        if outcome != EnumerationOutcome::Complete {
            return outcome;
        }
        let mut excluded = (0..i).collect();
        let outcome = enumerate_csg_recursive(region, &start, &mut excluded);
        if outcome != EnumerationOutcome::Complete {
            return outcome;
        }
    }
    EnumerationOutcome::Complete
}

/// Emit a connected subgraph (CSG).
fn emit_csg<R: ConnectedRegion>(region: &mut R, node: &Arc<JoinRelationSet>) -> EnumerationOutcome {
    if node.count() == region.relations() {
        return EnumerationOutcome::Complete;
    }

    // Create exclusion set as everything inside the subgraph and anything below it
    let mut exclusion_set = HashSet::new();
    for i in 0..node.relations()[0] {
        exclusion_set.insert(i);
    }
    for &rel in node.relations() {
        exclusion_set.insert(rel);
    }

    // Find neighbors given this exclusion set
    let neighbors = region.graph().get_neighbors(node, &exclusion_set);
    if neighbors.is_empty() {
        return EnumerationOutcome::Complete;
    }

    // Neighbors should be in reverse order
    let mut neighbors = neighbors;
    neighbors.sort_by(|a, b| b.cmp(a));

    // Add neighbors to exclusion set for recursive calls
    let mut new_exclusion_set = exclusion_set.clone();
    for &neighbor in &neighbors {
        new_exclusion_set.insert(neighbor);
    }

    for neighbor_idx in neighbors {
        let neighbor_relation = region.sets().get_relation(neighbor_idx);

        // Check if connected
        let connections = region.graph().get_connections(node, &neighbor_relation);
        if !connections.is_empty() {
            let outcome = region.emit(node, &neighbor_relation, &connections);
            if outcome != EnumerationOutcome::Complete {
                return outcome;
            }
        }

        let outcome =
            enumerate_cmp_recursive(region, node, &neighbor_relation, &mut new_exclusion_set);
        if outcome != EnumerationOutcome::Complete {
            return outcome;
        }

        new_exclusion_set.remove(&neighbor_idx);
    }

    EnumerationOutcome::Complete
}

/// Enumerate connected subgraphs recursively.
fn enumerate_csg_recursive<R: ConnectedRegion>(
    region: &mut R,
    node: &Arc<JoinRelationSet>,
    exclusion_set: &mut HashSet<usize>,
) -> EnumerationOutcome {
    // Find neighbors of S under the exclusion set
    let neighbors = region.graph().get_neighbors(node, exclusion_set);
    if neighbors.is_empty() {
        return EnumerationOutcome::Complete;
    }

    let all_subsets = get_all_neighbor_sets(neighbors.clone());
    let mut union_sets = Vec::new();

    for rel_set in &all_subsets {
        let neighbor = region.sets().get_relation_from_vec(rel_set.clone());
        let new_set = region.sets().union(node, &neighbor);

        if new_set.count() > node.count() && region.contains(&new_set) {
            let outcome = emit_csg(region, &new_set);
            if outcome != EnumerationOutcome::Complete {
                return outcome;
            }
        }
        union_sets.push(new_set);
    }

    let mut new_exclusion_set = exclusion_set.clone();
    for &neighbor in &neighbors {
        new_exclusion_set.insert(neighbor);
    }

    for union_set in union_sets {
        let outcome = enumerate_csg_recursive(region, &union_set, &mut new_exclusion_set);
        if outcome != EnumerationOutcome::Complete {
            return outcome;
        }
    }

    EnumerationOutcome::Complete
}

/// Enumerate complement pairs recursively.
fn enumerate_cmp_recursive<R: ConnectedRegion>(
    region: &mut R,
    left: &Arc<JoinRelationSet>,
    right: &Arc<JoinRelationSet>,
    exclusion_set: &mut HashSet<usize>,
) -> EnumerationOutcome {
    // Get neighbors of the second relation under the exclusion set
    let neighbors = region.graph().get_neighbors(right, exclusion_set);
    if neighbors.is_empty() {
        return EnumerationOutcome::Complete;
    }

    let all_subsets = get_all_neighbor_sets(neighbors.clone());
    let mut union_sets = Vec::new();

    for rel_set in &all_subsets {
        let neighbor = region.sets().get_relation_from_vec(rel_set.clone());
        let combined_set = region.sets().union(right, &neighbor);

        debug_assert!(combined_set.count() > right.count());

        if region.contains(&combined_set) {
            let connections = region.graph().get_connections(left, &combined_set);
            if !connections.is_empty() {
                let outcome = region.emit(left, &combined_set, &connections);
                if outcome != EnumerationOutcome::Complete {
                    return outcome;
                }
            }
        }
        union_sets.push(combined_set);
    }

    let mut new_exclusion_set = exclusion_set.clone();
    for &neighbor in &neighbors {
        new_exclusion_set.insert(neighbor);
    }

    for union_set in union_sets {
        let outcome = enumerate_cmp_recursive(region, left, &union_set, &mut new_exclusion_set);
        if outcome != EnumerationOutcome::Complete {
            return outcome;
        }
    }

    EnumerationOutcome::Complete
}
