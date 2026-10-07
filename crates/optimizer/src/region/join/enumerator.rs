// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Enumerate candidate join orders with DPccp and a greedy fallback.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_common::logging::targets;
use tracing::debug;

use crate::cost::region::RegionCostModel;
use crate::region::join::candidate::DPJoinNode;
use crate::region::join::query_graph::{
    CutPredicateResolution, JoinPredicateSet, NeighborInfo, QueryGraphEdges,
};
use crate::region::join::relation::{JoinRelationSet, JoinRelationSetManager};

/// Terminal state of one enumeration strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnumerationOutcome {
    Complete,
    /// A plan was produced by a bounded/greedy strategy, but the declared
    /// search domain was not exhausted.  This is usable as an anytime seed,
    /// never as an exact join-order proof.
    Approximate,
    PairBudgetExhausted,
    Ineligible,
    MissingSubplan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PairEmission {
    Emitted(Arc<JoinRelationSet>),
    MissingInput,
    Ineligible,
}

/// The PlanEnumerator performs join order optimization using dynamic programming.
///
pub(crate) struct PlanEnumerator<'a> {
    /// The query graph containing edges between relations.
    query_graph: &'a QueryGraphEdges,
    /// The set manager for creating/looking up relation sets.
    set_manager: &'a mut JoinRelationSetManager,
    /// The cost model for evaluating join costs.
    cost_model: &'a mut RegionCostModel,
    /// Number of relations in the query.
    num_relations: usize,
    /// Bounded non-dominated work/memory frontier for each relation set.
    plans: HashMap<Arc<JoinRelationSet>, Vec<DPJoinNode>>,
    /// The total number of join pairs considered.
    pairs: usize,
    exact_relation_limit: usize,
    max_pairs: usize,
    max_frontier_size: usize,
    frontier_truncated: bool,
}

impl super::connected::ConnectedRegion for PlanEnumerator<'_> {
    fn relations(&self) -> usize {
        self.num_relations
    }
    fn graph(&self) -> &QueryGraphEdges {
        self.query_graph
    }
    fn sets(&mut self) -> &mut JoinRelationSetManager {
        self.set_manager
    }
    fn contains(&self, set: &Arc<JoinRelationSet>) -> bool {
        self.plans.contains_key(set)
    }
    fn emit(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        connections: &[NeighborInfo],
    ) -> EnumerationOutcome {
        self.try_emit_pair(left, right, connections)
    }
}

impl<'a> PlanEnumerator<'a> {
    /// Create a new PlanEnumerator.
    #[cfg(test)]
    pub fn new(
        query_graph: &'a QueryGraphEdges,
        set_manager: &'a mut JoinRelationSetManager,
        cost_model: &'a mut RegionCostModel,
        num_relations: usize,
    ) -> Self {
        Self::with_budget(
            query_graph,
            set_manager,
            cost_model,
            num_relations,
            12,
            10_000,
            4,
        )
    }

    pub fn with_budget(
        query_graph: &'a QueryGraphEdges,
        set_manager: &'a mut JoinRelationSetManager,
        cost_model: &'a mut RegionCostModel,
        num_relations: usize,
        exact_relation_limit: usize,
        max_pairs: usize,
        max_frontier_size: usize,
    ) -> Self {
        Self {
            query_graph,
            set_manager,
            cost_model,
            num_relations,
            plans: HashMap::new(),
            pairs: 0,
            exact_relation_limit,
            max_pairs,
            max_frontier_size: max_frontier_size.max(1),
            frontier_truncated: false,
        }
    }

    /// Initialize leaf plans (single relations).
    pub fn init_leaf_plans(&mut self) {
        for i in 0..self.num_relations {
            let set = self.set_manager.get_relation(i);
            let cardinality = self.cost_model.get_cardinality(&set);
            let risk_cardinality = self.cost_model.get_risk_cardinality(&set);
            let materialization_cardinality = self.cost_model.get_materialization_cardinality(&set);
            let mut node = DPJoinNode::leaf(
                set.clone(),
                self.cost_model.payload_width(set.as_ref()),
                cardinality,
                risk_cardinality,
                materialization_cardinality,
            );

            node.cardinality_provenance = self.cost_model.relation_provenance(i);
            self.plans.insert(set, vec![node]);
        }
    }

    /// Solve the join order using dynamic programming.
    ///
    pub fn solve_join_order(&mut self) -> EnumerationOutcome {
        // For small graphs, try exact algorithm first
        if self.num_relations <= self.exact_relation_limit {
            match self.solve_join_order_exactly() {
                EnumerationOutcome::Complete => {
                    // Exact DP must retain the whole non-dominated frontier.
                    // A bounded resident frontier is an anytime policy, not
                    // an admissible proof that no discarded plan can improve
                    // a parent under another resource grant.
                    if self.frontier_truncated {
                        return EnumerationOutcome::Approximate;
                    }
                    // Check if we got a final plan
                    let mut all_relations = HashSet::new();
                    for i in 0..self.num_relations {
                        all_relations.insert(i);
                    }
                    let total_set = self.set_manager.get_relation_from_set(&all_relations);

                    if let Some(final_plan) =
                        self.plans.get(&total_set).and_then(|plans| plans.first())
                    {
                        debug!(
                            target: targets::OPTIMIZER,
                            relations = self.num_relations,
                            pairs = self.pairs,
                            left = %final_plan.left_set,
                            right = %final_plan.right_set,
                            cardinality = final_plan.cardinality,
                            cost = final_plan.cost,
                            "Completed exact join-order enumeration"
                        );
                        return EnumerationOutcome::Complete;
                    }
                }
                EnumerationOutcome::Ineligible => return EnumerationOutcome::Ineligible,
                EnumerationOutcome::MissingSubplan => {
                    return EnumerationOutcome::MissingSubplan;
                }
                EnumerationOutcome::Approximate => return EnumerationOutcome::Approximate,
                EnumerationOutcome::PairBudgetExhausted => {}
            }
        }

        // Exact DP state is all-or-nothing. Greedy enumeration starts from the
        // authoritative leaves instead of accidentally depending on whichever
        // composite sets happened to fit inside the pair budget.
        self.plans.retain(|set, _| set.count() == 1);
        debug!(
            target: targets::OPTIMIZER,
            relations = self.num_relations,
            exact_pairs = self.pairs,
            "Falling back to greedy-seeded linearized join-order enumeration"
        );
        self.pairs = 0;
        match self.solve_join_order_approximately() {
            EnumerationOutcome::Approximate => {}
            outcome => return outcome,
        }
        self.pairs = 0;
        self.refine_along_seed_order()
    }

    /// Get the optimal plans.
    pub fn get_plans(&self) -> &HashMap<Arc<JoinRelationSet>, Vec<DPJoinNode>> {
        &self.plans
    }

    /// Get the final plan for all relations.
    #[cfg(test)]
    pub fn get_final_plan(&mut self) -> Option<&DPJoinNode> {
        let mut all_relations = HashSet::new();
        for i in 0..self.num_relations {
            all_relations.insert(i);
        }
        let total_set = self.set_manager.get_relation_from_set(&all_relations);
        self.plans.get(&total_set).and_then(|plans| plans.first())
    }

    pub fn get_final_plans(&mut self) -> &[DPJoinNode] {
        let mut all_relations = HashSet::new();
        for i in 0..self.num_relations {
            all_relations.insert(i);
        }
        let total_set = self.set_manager.get_relation_from_set(&all_relations);
        self.plans.get(&total_set).map(Vec::as_slice).unwrap_or(&[])
    }

    // Private methods

    /// Solve join order exactly using dynamic programming.
    fn solve_join_order_exactly(&mut self) -> EnumerationOutcome {
        let outcome = super::connected::enumerate(self);
        if outcome != EnumerationOutcome::Complete {
            return outcome;
        }

        // DPccp intentionally enumerates connected cuts.  A two-relation
        // Cartesian region has no graph neighbor to emit, but its only legal
        // binary tree is still an exact result. Keep this small completion
        // case in the exact path instead of silently delegating it to the
        // greedy seed path.
        if self.num_relations == 2 {
            let left = self.set_manager.get_relation(0);
            let right = self.set_manager.get_relation(1);
            let total = self.set_manager.union(&left, &right);
            if !self.plans.contains_key(&total) {
                let outcome = self.try_emit_pair(&left, &right, &[]);
                if outcome != EnumerationOutcome::Complete {
                    return outcome;
                }
            }
        }
        EnumerationOutcome::Complete
    }

    /// Try to emit a pair of relations.
    ///
    /// Returns an explicit terminal state rather than overloading a timeout
    /// boolean with query-graph ineligibility.
    fn try_emit_pair(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        connections: &[NeighborInfo],
    ) -> EnumerationOutcome {
        self.pairs += 1;
        if self.pairs > self.max_pairs {
            return EnumerationOutcome::PairBudgetExhausted;
        }

        match self.emit_pair(left, right, connections) {
            PairEmission::Emitted(_) => EnumerationOutcome::Complete,
            // Exact DP can discover a connected cut before both component
            // plans have been emitted. That pair is deferred, not rejected;
            // later CSG/CMP traversal may revisit it once its inputs exist.
            PairEmission::MissingInput => EnumerationOutcome::Complete,
            PairEmission::Ineligible => EnumerationOutcome::Ineligible,
        }
    }

    /// Emit a pair of relations and create a join node.
    fn emit_pair(
        &mut self,
        left: &Arc<JoinRelationSet>,
        right: &Arc<JoinRelationSet>,
        connections: &[NeighborInfo],
    ) -> PairEmission {
        // Get the left and right plans
        let left_plans = match self.plans.get(left) {
            Some(plans) => plans.clone(),
            None => return PairEmission::MissingInput,
        };

        let right_plans = match self.plans.get(right) {
            Some(plans) => plans.clone(),
            None => return PairEmission::MissingInput,
        };

        let mut new_set = None;
        for left_plan in &left_plans {
            for right_plan in &right_plans {
                // Costing owns the canonical union and cardinality estimate
                // for this pair; reuse its set instead of hashing the same
                // bitset twice.
                let Some(new_node) = self.create_join_tree(left_plan, right_plan, connections)
                else {
                    return PairEmission::Ineligible;
                };
                let set = Arc::clone(&new_node.set);
                self.insert_frontier(set.clone(), new_node);
                new_set = Some(set);
            }
        }
        new_set
            .map(PairEmission::Emitted)
            .unwrap_or(PairEmission::MissingInput)
    }

    fn insert_frontier(&mut self, set: Arc<JoinRelationSet>, candidate: DPJoinNode) {
        let frontier = self.plans.entry(set).or_default();
        let candidate_shape = candidate.compact_shape();
        if frontier.iter().any(|existing| {
            existing.cost <= candidate.cost
                && existing.peak_build_bytes <= candidate.peak_build_bytes
                && (existing.cost < candidate.cost
                    || existing.peak_build_bytes < candidate.peak_build_bytes
                    || existing.compact_shape() <= candidate_shape)
        }) {
            return;
        }
        frontier.retain(|existing| {
            !(candidate.cost <= existing.cost
                && candidate.peak_build_bytes <= existing.peak_build_bytes)
                || (candidate.cost == existing.cost
                    && candidate.peak_build_bytes == existing.peak_build_bytes
                    && candidate_shape > existing.compact_shape())
        });
        frontier.push(candidate);
        frontier.sort_by(|left, right| {
            left.cost
                .total_cmp(&right.cost)
                .then_with(|| left.peak_build_bytes.cmp(&right.peak_build_bytes))
                .then_with(|| left.compact_shape().cmp(right.compact_shape()))
        });
        // The resident cap is an explicit anytime policy. It can produce a
        // useful seed, but it is never evidence that the discarded frontier
        // members cannot improve a parent under another resource grant.
        if frontier.len() > self.max_frontier_size {
            self.frontier_truncated = true;
            let lowest_memory = frontier
                .iter()
                .enumerate()
                .min_by_key(|(_, plan)| plan.peak_build_bytes)
                .map(|(index, _)| index)
                .unwrap_or(0);
            if lowest_memory >= self.max_frontier_size {
                let low_memory_plan = frontier.remove(lowest_memory);
                frontier.truncate(self.max_frontier_size - 1);
                frontier.push(low_memory_plan);
            } else {
                frontier.truncate(self.max_frontier_size);
            }
            frontier.sort_by(|left, right| {
                left.cost
                    .total_cmp(&right.cost)
                    .then_with(|| left.peak_build_bytes.cmp(&right.peak_build_bytes))
                    .then_with(|| left.compact_shape().cmp(right.compact_shape()))
            });
        }
    }

    fn create_join_tree(
        &mut self,
        left: &DPJoinNode,
        right: &DPJoinNode,
        connections: &[NeighborInfo],
    ) -> Option<DPJoinNode> {
        let predicates = match Self::collect_cut_predicates(connections, &left.set, &right.set) {
            CutPredicateResolution::Resolved(predicates) => predicates,
            CutPredicateResolution::Ineligible => return None,
        };

        Some(self.cost_model.compute_cost_and_create_node(
            left,
            right,
            self.set_manager,
            predicates,
        ))
    }

    fn collect_cut_predicates(
        connections: &[NeighborInfo],
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> CutPredicateResolution {
        JoinPredicateSet::from_filters(
            connections
                .iter()
                .flat_map(|connection| &connection.filters),
            left,
            right,
        )
    }

    /// Solve join order approximately using a greedy algorithm.
    fn solve_join_order_approximately(&mut self) -> EnumerationOutcome {
        // Start with all base relations
        let mut join_relations: Vec<Arc<JoinRelationSet>> = (0..self.num_relations)
            .map(|i| self.set_manager.get_relation(i))
            .collect();

        while join_relations.len() > 1 {
            let mut best_left = 0;
            let mut best_right = 0;
            let mut best_cost = f64::MAX;
            let mut best_set = None;
            let mut found_connection = false;

            // Find the best pair to join
            for i in 0..join_relations.len() {
                for j in (i + 1)..join_relations.len() {
                    self.pairs = self.pairs.saturating_add(1);
                    if self.pairs > self.max_pairs {
                        return EnumerationOutcome::PairBudgetExhausted;
                    }
                    let connections = self
                        .query_graph
                        .get_connections(&join_relations[i], &join_relations[j]);

                    if !connections.is_empty() {
                        let combined = match self.emit_pair(
                            &join_relations[i],
                            &join_relations[j],
                            &connections,
                        ) {
                            PairEmission::Emitted(combined) => combined,
                            PairEmission::MissingInput => {
                                return EnumerationOutcome::MissingSubplan;
                            }
                            PairEmission::Ineligible => return EnumerationOutcome::Ineligible,
                        };
                        if let Some(node) =
                            self.plans.get(&combined).and_then(|plans| plans.first())
                        {
                            if node.cost < best_cost {
                                best_cost = node.cost;
                                best_left = i;
                                best_right = j;
                                best_set = Some(Arc::clone(&combined));
                                found_connection = true;
                            }
                        }
                    }
                }
            }

            if !found_connection {
                // Fallback: just pick first two
                best_left = 0;
                best_right = 1;
                let combined = match self.emit_pair(
                    &join_relations[best_left],
                    &join_relations[best_right],
                    &[],
                ) {
                    PairEmission::Emitted(combined) => combined,
                    PairEmission::MissingInput => return EnumerationOutcome::MissingSubplan,
                    PairEmission::Ineligible => return EnumerationOutcome::Ineligible,
                };
                best_set = Some(combined);
            }

            // Ensure best_right > best_left for removal
            if best_left > best_right {
                std::mem::swap(&mut best_left, &mut best_right);
            }

            // Update join_relations
            let Some(new_set) = best_set else {
                return EnumerationOutcome::MissingSubplan;
            };
            join_relations.remove(best_right);
            join_relations.remove(best_left);
            join_relations.push(new_set);
        }
        // Greedy enumeration intentionally returns a seed only.  Callers may
        // execute it as an anytime candidate, but must not advertise it as a
        // complete proof of the declared join search space.
        EnumerationOutcome::Approximate
    }
}

impl PlanEnumerator<'_> {
    /// Improve the greedy seed by dynamic programming over the contiguous
    /// intervals of its leaf order (linearized DP). Every subtree of the seed
    /// is such an interval, so the seed stays on the frontier and the result
    /// is never worse; every connected bushy tree whose leaves keep this
    /// order is considered, with O(n^3) pair emissions. Running out of pair
    /// budget keeps whatever the frontier already holds.
    fn refine_along_seed_order(&mut self) -> EnumerationOutcome {
        let Some(order) = self.seed_leaf_order() else {
            return EnumerationOutcome::Approximate;
        };
        let leaves = order.len();
        let mut intervals = Vec::with_capacity(leaves);
        for start in 0..leaves {
            let mut row = Vec::with_capacity(leaves - start);
            let mut set = self.set_manager.get_relation(order[start]);
            row.push(Arc::clone(&set));
            for &relation in &order[start + 1..] {
                let leaf = self.set_manager.get_relation(relation);
                set = self.set_manager.union(&set, &leaf);
                row.push(Arc::clone(&set));
            }
            intervals.push(row);
        }
        // intervals[start][len - 1] covers order[start..start + len].
        for len in 2..=leaves {
            for start in 0..=leaves - len {
                for left_len in 1..len {
                    let left = Arc::clone(&intervals[start][left_len - 1]);
                    let right = Arc::clone(&intervals[start + left_len][len - left_len - 1]);
                    if !self.plans.contains_key(&left) || !self.plans.contains_key(&right) {
                        continue;
                    }
                    let connections = self.query_graph.get_connections(&left, &right);
                    if connections.is_empty() {
                        continue;
                    }
                    if self.try_emit_pair(&left, &right, &connections)
                        == EnumerationOutcome::PairBudgetExhausted
                    {
                        return EnumerationOutcome::Approximate;
                    }
                }
            }
        }
        EnumerationOutcome::Approximate
    }

    /// Leaves of the cheapest complete plan, left to right.
    fn seed_leaf_order(&mut self) -> Option<Vec<usize>> {
        let root = self.get_final_plans().first()?.clone();
        let mut order = Vec::with_capacity(self.num_relations);
        let mut pending = vec![Arc::new(root)];
        while let Some(node) = pending.pop() {
            if node.is_leaf {
                order.extend_from_slice(node.set.relations());
                continue;
            }
            pending.push(node.right_plan.clone()?);
            pending.push(node.left_plan.clone()?);
        }
        (order.len() == self.num_relations).then_some(order)
    }
}

/// Get all non-empty subsets of a set of neighbors.
///
/// This generates all 2^n - 1 subsets of the input set.
pub(crate) fn get_all_neighbor_sets(mut neighbors: Vec<usize>) -> Vec<Vec<usize>> {
    neighbors.sort();

    // Keep the historical cardinality/lexicographic order, but represent a
    // subset as a compact sorted Vec. The old HashSet implementation cloned
    // every partial set at every level and then sorted it again in the set
    // manager. Join-region enumeration invokes this helper for thousands of
    // cuts, so the temporary hash tables became a measurable allocation
    // stream without adding any search coverage.
    let mut result = Vec::with_capacity(if neighbors.len() < usize::BITS as usize {
        (1usize << neighbors.len()).saturating_sub(1)
    } else {
        0
    });
    for size in 1..=neighbors.len() {
        append_neighbor_subsets(
            &neighbors,
            0,
            size,
            &mut Vec::with_capacity(size),
            &mut result,
        );
    }
    result
}

fn append_neighbor_subsets(
    neighbors: &[usize],
    start: usize,
    remaining: usize,
    current: &mut Vec<usize>,
    output: &mut Vec<Vec<usize>>,
) {
    if remaining == 0 {
        output.push(current.clone());
        return;
    }
    let last_start = neighbors.len().saturating_sub(remaining);
    for index in start..=last_start {
        current.push(neighbors[index]);
        append_neighbor_subsets(neighbors, index + 1, remaining - 1, current, output);
        current.pop();
    }
}

/// Add supersets by adding one more neighbor to each existing set.
#[cfg(test)]
fn add_super_sets(current: &[HashSet<usize>], all_neighbors: &[usize]) -> Vec<HashSet<usize>> {
    let mut result = Vec::new();

    for neighbor_set in current {
        let max_val = neighbor_set.iter().max().copied().unwrap_or(0);

        for &neighbor in all_neighbors {
            if neighbor <= max_val {
                continue;
            }
            if !neighbor_set.contains(&neighbor) {
                let mut new_set = neighbor_set.clone();
                new_set.insert(neighbor);
                result.push(new_set);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests;
