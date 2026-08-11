// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cardinality estimation helpers for join-order planning.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use paro_common::logging::targets;
use paro_planner::expression::{ConjunctionType, Expression};
use paro_planner::operator::{ColumnBinding, JoinType};
use tracing::trace;

use crate::join_order::equality_graph::{find_component, union_components, EqualityClassGraph};
use crate::join_order::query_graph::FilterInfo;
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::join_order::relation_manager::RelationStats;

/// Default fraction of preserved-side rows that match a SEMI/ANTI join.
const DEFAULT_SEMI_ANTI_MATCH_FRACTION: f64 = 0.2;

/// Information about the denominator calculation.
#[derive(Debug)]
pub struct DenomInfo {
    /// Relations that contribute to the numerator.
    pub numerator_relations: Arc<JoinRelationSet>,
    /// Filter strength multiplier.
    pub filter_strength: f64,
    /// The denominator value.
    pub denominator: f64,
}

impl DenomInfo {
    pub fn new(
        numerator_relations: Arc<JoinRelationSet>,
        filter_strength: f64,
        denominator: f64,
    ) -> Self {
        Self {
            numerator_relations,
            filter_strength,
            denominator,
        }
    }
}

/// Statistics for a set of equivalent relations (columns joined by equality).
#[derive(Debug, Clone)]
pub struct RelationsSetToStats {
    /// Column bindings that are equivalent in a join plan.
    /// If you have A.x = B.y and B.y = C.z, then one set is {A.x, B.y, C.z}.
    pub equivalent_relations: HashSet<ColumnBinding>,
    /// The estimated distinct count using HLL.
    pub distinct_count_hll: usize,
    /// The estimated distinct count without HLL.
    pub distinct_count_no_hll: usize,
    /// Whether we have a distinct count from HLL.
    pub has_distinct_count_hll: bool,
    /// Filters that reference these columns.
    pub filters: Vec<Arc<FilterInfo>>,
}

impl RelationsSetToStats {
    /// Create a new RelationsSetToStats from a set of column bindings.
    pub fn new(equivalent_relations: HashSet<ColumnBinding>) -> Self {
        Self {
            equivalent_relations,
            distinct_count_hll: 0,
            distinct_count_no_hll: usize::MAX,
            has_distinct_count_hll: false,
            filters: Vec::new(),
        }
    }

    /// Get the best distinct count estimate.
    pub fn get_distinct_count(&self) -> usize {
        if self.has_distinct_count_hll {
            self.distinct_count_hll
        } else {
            self.distinct_count_no_hll
        }
    }
}

/// Filter info with total domain information.
#[derive(Debug)]
pub struct FilterInfoWithTotalDomains {
    /// The filter info.
    pub filter_info: Arc<FilterInfo>,
    /// Distinct count from HLL.
    pub distinct_count_hll: usize,
    /// Distinct count without HLL.
    pub distinct_count_no_hll: usize,
    /// Whether we have HLL distinct count.
    pub has_distinct_count_hll: bool,
}

impl FilterInfoWithTotalDomains {
    pub fn new(filter_info: Arc<FilterInfo>, stats: &RelationsSetToStats) -> Self {
        Self {
            filter_info,
            distinct_count_hll: stats.distinct_count_hll,
            distinct_count_no_hll: stats.distinct_count_no_hll,
            has_distinct_count_hll: stats.has_distinct_count_hll,
        }
    }

    /// Get the best distinct count estimate.
    pub fn get_distinct_count(&self) -> usize {
        if self.has_distinct_count_hll {
            self.distinct_count_hll
        } else {
            self.distinct_count_no_hll
        }
    }
}

/// Subgraph with denominator information.
#[derive(Debug, Clone)]
struct Subgraph2Denominator {
    /// The relations in this subgraph.
    relations: Option<Arc<JoinRelationSet>>,
    /// Relations that contribute to the numerator.
    numerator_relations: Option<Arc<JoinRelationSet>>,
    /// The denominator value.
    denom: f64,
}

impl Default for Subgraph2Denominator {
    fn default() -> Self {
        Self {
            relations: None,
            numerator_relations: None,
            denom: 1.0,
        }
    }
}

/// Helper for cardinality calculation.
#[derive(Debug, Clone, Default)]
pub struct CardinalityHelper {
    /// Cardinality before filters are applied.
    pub cardinality_before_filters: f64,
}

#[derive(Debug, Clone, Copy)]
struct BindingCardinalityStats {
    distinct_count: usize,
    relation_cardinality: usize,
    from_hll: bool,
}

#[derive(Debug, Clone, Copy)]
struct DistinctDomainEstimate {
    hll_max: usize,
    upper_bound_min: usize,
    has_hll: bool,
}

/// Deterministic Prim frontier ordered by parent relation, child relation,
/// filter, edge, then child vertex.
type EqualityTreeFrontierEntry = Reverse<(usize, usize, usize, usize, usize)>;

#[derive(Debug, Default)]
struct EqualityDenominatorScratch {
    parents: Vec<usize>,
    sizes: Vec<usize>,
    active: Vec<bool>,
    components: Vec<Vec<usize>>,
    vertex_domains: Vec<Option<DistinctDomainEstimate>>,
    tree_visited: Vec<bool>,
    owned_edges: Vec<Option<usize>>,
    tree_frontier: BinaryHeap<EqualityTreeFrontierEntry>,
}

impl EqualityDenominatorScratch {
    fn prepare(&mut self, vertices: usize) {
        self.parents.clear();
        self.parents.extend(0..vertices);
        self.sizes.clear();
        self.sizes.resize(vertices, 1);
        self.active.clear();
        self.active.resize(vertices, false);
        self.components.resize_with(vertices, Vec::new);
        for component in &mut self.components {
            component.clear();
        }
        self.vertex_domains.clear();
        self.vertex_domains.resize(vertices, None);
        self.tree_visited.clear();
        self.tree_visited.resize(vertices, false);
        self.owned_edges.clear();
        self.owned_edges.resize(vertices, None);
        self.tree_frontier.clear();
    }
}

#[derive(Debug, Clone)]
struct CorrelatedEqualityDomains {
    strongest: f64,
    redundant_product: f64,
    relation_bindings: HashMap<usize, HashSet<ColumnBinding>>,
}

impl CorrelatedEqualityDomains {
    fn new(domain: f64, relation_bindings: HashMap<usize, HashSet<ColumnBinding>>) -> Self {
        Self {
            strongest: domain,
            redundant_product: 1.0,
            relation_bindings,
        }
    }

    fn add(&mut self, domain: f64, relation_bindings: HashMap<usize, HashSet<ColumnBinding>>) {
        if domain > self.strongest {
            self.redundant_product *= self.strongest;
            self.strongest = domain;
        } else {
            self.redundant_product *= domain;
        }
        for (relation, bindings) in relation_bindings {
            self.relation_bindings
                .entry(relation)
                .or_default()
                .extend(bindings);
        }
    }
}

#[derive(Debug)]
struct EqualityPairDomain {
    domain: f64,
    relation_bindings: HashMap<usize, HashSet<ColumnBinding>>,
}

impl Default for EqualityPairDomain {
    fn default() -> Self {
        Self {
            domain: 1.0,
            relation_bindings: HashMap::new(),
        }
    }
}

fn push_tree_frontier(
    graph: &EqualityClassGraph,
    active: &[bool],
    tree_visited: &[bool],
    tree_frontier: &mut BinaryHeap<EqualityTreeFrontierEntry>,
    parent: usize,
) {
    for edge_index in &graph.adjacency[parent] {
        let edge = &graph.edges[*edge_index];
        let child = if edge.left == parent {
            edge.right
        } else {
            debug_assert_eq!(edge.right, parent);
            edge.left
        };
        if !active[child] || tree_visited[child] {
            continue;
        }
        tree_frontier.push(Reverse((
            graph.vertices[parent].relation,
            graph.vertices[child].relation,
            edge.filter_index,
            *edge_index,
            child,
        )));
    }
}

impl DistinctDomainEstimate {
    fn observed(value: usize) -> Self {
        Self {
            hll_max: value.max(1),
            upper_bound_min: usize::MAX,
            has_hll: true,
        }
    }

    fn upper_bound(value: usize) -> Self {
        Self {
            hll_max: 0,
            upper_bound_min: value.max(1),
            has_hll: false,
        }
    }

    fn value(self) -> usize {
        if self.has_hll {
            self.hll_max.max(1)
        } else {
            self.upper_bound_min.max(1)
        }
    }
}

impl CardinalityHelper {
    pub fn new(cardinality_before_filters: f64) -> Self {
        Self {
            cardinality_before_filters,
        }
    }
}

/// The CardinalityEstimator estimates join cardinalities.
///
/// counts of joined columns. If you have two tables A and B joined using A.x = B.y,
/// we assume that each tuple in A will match ~ B/distinct(y) tuples in B.
/// The cardinality estimation then becomes (|A|*|B|) / max(distinct(x), distinct(y)).
#[derive(Debug, Default)]
pub struct CardinalityEstimator {
    /// Statistics for equivalent relation sets.
    relation_set_stats: Vec<RelationsSetToStats>,
    /// Equality-class topology compiled once before DP subset enumeration.
    equality_graphs: Vec<EqualityClassGraph>,
    /// Statistics initialization can reorder equality classes. Recompile the
    /// compact topology lazily after the last mutation.
    equality_graphs_dirty: bool,
    /// Reusable union-find and component storage for DP subset estimates.
    equality_scratch: EqualityDenominatorScratch,
    /// Distinct-count estimates before equivalence classes merge their domains.
    ///
    /// A semi join needs the two sides independently: the fraction of preserved
    /// keys that can match is bounded by `right_ndv / left_ndv`. The merged
    /// equivalence-class domain deliberately loses that directionality.
    binding_stats: HashMap<ColumnBinding, BindingCardinalityStats>,
    /// Unique keys are correctness guarantees, unlike marginal NDV estimates.
    /// They allow parallel equality classes to use a joint domain without
    /// assuming the key columns are statistically independent.
    relation_unique_keys: HashMap<usize, Vec<Vec<ColumnBinding>>>,
    relation_cardinalities: HashMap<usize, usize>,
    /// Cached cardinalities for relation sets.
    relation_set_2_cardinality: HashMap<String, CardinalityHelper>,
    /// Set manager for creating/looking up relation sets.
    set_manager: JoinRelationSetManager,
}

impl CardinalityEstimator {
    /// Create a new CardinalityEstimator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize equivalent relations from filter information.
    ///
    /// This builds equivalence classes for columns that are joined by equality.
    /// For example, if we have A.x = B.y and B.y = C.z, then {A.x, B.y, C.z}
    /// form an equivalence class.
    pub fn init_equivalent_relations(&mut self, filter_infos: &[Arc<FilterInfo>]) {
        for filter in filter_infos {
            if self.single_column_filter(filter) {
                // Filter on one relation (e.g., string or range filter on a column)
                self.add_relation_stats(filter);
                continue;
            } else if self.empty_filter(filter) {
                continue;
            }

            // Multi-column filter (join condition)
            let matching_sets = self.determine_matching_equivalent_sets(filter);
            self.add_to_equivalence_sets(filter.clone(), matching_sets);
        }

        self.remove_empty_total_domains();
    }

    /// Initialize cardinality estimator properties for a relation.
    pub fn init_cardinality_estimator_props(
        &mut self,
        set: &Arc<JoinRelationSet>,
        stats: &RelationStats,
    ) {
        debug_assert!(stats.stats_initialized);

        let relation_cardinality = stats.cardinality as f64;
        let card_helper = CardinalityHelper::new(relation_cardinality);
        self.relation_set_2_cardinality
            .insert(set.to_string(), card_helper);

        self.binding_stats
            .extend(stats.column_distinct_count.iter().map(|(binding, count)| {
                (
                    *binding,
                    BindingCardinalityStats {
                        distinct_count: count.distinct_count,
                        relation_cardinality: stats.cardinality,
                        from_hll: count.from_hll,
                    },
                )
            }));
        let relation = set.relations()[0];
        self.relation_unique_keys
            .insert(relation, stats.unique_keys.clone());
        self.relation_cardinalities
            .insert(relation, stats.cardinality);

        self.update_total_domains(set, stats);

        // Sort relations from greatest distinct count to lowest
        self.relation_set_stats.sort_by(|a, b| {
            let a_count = if a.has_distinct_count_hll {
                a.distinct_count_hll
            } else {
                a.distinct_count_no_hll
            };
            let b_count = if b.has_distinct_count_hll {
                b.distinct_count_hll
            } else {
                b.distinct_count_no_hll
            };
            b_count.cmp(&a_count)
        });
        // Equality graphs refer to their owning statistics class by index.
        // Progressive NDV initialization can reorder those classes; defer one
        // rebuild until the first estimate after initialization, then reuse it
        // throughout DP subset enumeration.
        self.equality_graphs_dirty = true;
    }

    /// Estimate cardinality for a join relation set.
    pub fn estimate_cardinality(&mut self, new_set: &JoinRelationSet) -> f64 {
        self.ensure_equality_graphs();
        let key = new_set.to_string();
        if let Some(helper) = self.relation_set_2_cardinality.get(&key) {
            return helper.cardinality_before_filters;
        }

        let denom_info = self.get_denominator(new_set);
        let numerator = self.get_numerator(&denom_info.numerator_relations);

        let result = numerator / denom_info.denominator;
        let new_entry = CardinalityHelper::new(result);
        self.relation_set_2_cardinality.insert(key, new_entry);
        result
    }

    /// Remove empty total domains from the stats.
    pub fn remove_empty_total_domains(&mut self) {
        self.relation_set_stats
            .retain(|r| !r.equivalent_relations.is_empty());
        self.equality_graphs_dirty = true;
    }

    fn ensure_equality_graphs(&mut self) {
        if self.equality_graphs_dirty {
            self.equality_graphs = EqualityClassGraph::build_all(&self.relation_set_stats);
            self.equality_graphs_dirty = false;
        }
    }

    /// Update total domains with statistics from a relation.
    pub fn update_total_domains(&mut self, set: &Arc<JoinRelationSet>, stats: &RelationStats) {
        debug_assert_eq!(set.count(), 1);

        // Initialize the distinct count for all columns used in joins
        for (key, distinct_count) in &stats.column_distinct_count {
            for relation_to_tdom in &mut self.relation_set_stats {
                if !relation_to_tdom.equivalent_relations.contains(key) {
                    continue;
                }

                if distinct_count.from_hll && relation_to_tdom.has_distinct_count_hll {
                    relation_to_tdom.distinct_count_hll = relation_to_tdom
                        .distinct_count_hll
                        .max(distinct_count.distinct_count);
                } else if distinct_count.from_hll && !relation_to_tdom.has_distinct_count_hll {
                    relation_to_tdom.has_distinct_count_hll = true;
                    relation_to_tdom.distinct_count_hll = distinct_count.distinct_count;
                } else {
                    relation_to_tdom.distinct_count_no_hll = relation_to_tdom
                        .distinct_count_no_hll
                        .min(distinct_count.distinct_count);
                }
                break;
            }
        }
    }

    // Private helper methods

    /// Check if a filter is empty (no specific columns referenced).
    fn empty_filter(&self, filter_info: &FilterInfo) -> bool {
        filter_info.left_set.is_none() && filter_info.right_set.is_none()
    }

    /// Add relation stats for a single-column filter.
    fn add_relation_stats(&mut self, filter_info: &FilterInfo) {
        debug_assert!(filter_info.set.count() >= 1);

        // Check if we already have this binding in an equivalence set
        if let Some(left_binding) = filter_info.left_binding {
            let left_binding = left_binding.column;
            for r2tdom in &self.relation_set_stats {
                if r2tdom.equivalent_relations.contains(&left_binding) {
                    // Found an equivalent filter
                    return;
                }
            }

            // Create a new equivalence set with just this binding
            let mut bindings = HashSet::new();
            bindings.insert(left_binding);
            self.relation_set_stats
                .push(RelationsSetToStats::new(bindings));
        }
    }

    /// Check if this is a single-column filter.
    fn single_column_filter(&self, filter_info: &FilterInfo) -> bool {
        if filter_info.left_set.is_some()
            && filter_info.right_set.is_some()
            && filter_info.set.count() > 1
        {
            // Both sets are from different relations
            return false;
        }
        if self.empty_filter(filter_info) {
            return false;
        }
        if matches!(filter_info.join_type, JoinType::Semi | JoinType::Anti) {
            return false;
        }
        true
    }

    /// Determine which equivalence sets match a filter.
    fn determine_matching_equivalent_sets(&self, filter_info: &FilterInfo) -> Vec<usize> {
        let mut matching_sets = Vec::new();

        for (i, r2tdom) in self.relation_set_stats.iter().enumerate() {
            if let Some(left_binding) = filter_info.left_binding {
                if r2tdom.equivalent_relations.contains(&left_binding.column) {
                    matching_sets.push(i);
                    continue;
                }
            }
            if let Some(right_binding) = filter_info.right_binding {
                if r2tdom.equivalent_relations.contains(&right_binding.column) {
                    // Don't add both left and right to matching_sets
                    // since both get added to that index anyway
                    matching_sets.push(i);
                }
            }
        }

        matching_sets
    }

    /// Add a filter to equivalence sets.
    fn add_to_equivalence_sets(&mut self, filter_info: Arc<FilterInfo>, matching_sets: Vec<usize>) {
        debug_assert!(matching_sets.len() <= 2);

        if matching_sets.len() > 1 {
            // An equivalence relation is connecting two sets
            // Merge the second set into the first
            let idx0 = matching_sets[0];
            let idx1 = matching_sets[1];

            // Collect data from the second set
            let bindings_to_add: Vec<_> = self.relation_set_stats[idx1]
                .equivalent_relations
                .iter()
                .copied()
                .collect();
            // Add to the first set
            for binding in bindings_to_add {
                self.relation_set_stats[idx0]
                    .equivalent_relations
                    .insert(binding);
            }

            let filters_to_add = std::mem::take(&mut self.relation_set_stats[idx1].filters);
            self.relation_set_stats[idx0].filters.extend(filters_to_add);
            self.relation_set_stats[idx0].filters.push(filter_info);

            // Clear the second set (will be removed later)
            self.relation_set_stats[idx1].equivalent_relations.clear();
        } else if matching_sets.len() == 1 {
            let idx = matching_sets[0];
            if let Some(left_binding) = filter_info.left_binding {
                self.relation_set_stats[idx]
                    .equivalent_relations
                    .insert(left_binding.column);
            }
            if let Some(right_binding) = filter_info.right_binding {
                self.relation_set_stats[idx]
                    .equivalent_relations
                    .insert(right_binding.column);
            }
            self.relation_set_stats[idx].filters.push(filter_info);
        } else {
            // No matching sets, create a new one
            let mut bindings = HashSet::new();
            if let Some(left_binding) = filter_info.left_binding {
                bindings.insert(left_binding.column);
            }
            if let Some(right_binding) = filter_info.right_binding {
                bindings.insert(right_binding.column);
            }
            let mut new_stats = RelationsSetToStats::new(bindings);
            new_stats.filters.push(filter_info);
            self.relation_set_stats.push(new_stats);
        }
    }

    /// Get the numerator for cardinality calculation.
    fn get_numerator(&self, set: &JoinRelationSet) -> f64 {
        let mut numerator = 1.0;
        for i in 0..set.count() {
            let single_node_set = self.set_manager_get_relation(set.relations()[i]);
            if let Some(card_helper) = self.relation_set_2_cardinality.get(&single_node_set) {
                let card = card_helper.cardinality_before_filters;
                numerator *= if card == 0.0 { 1.0 } else { card };
            }
        }
        numerator
    }

    /// Helper to get a relation set string.
    fn set_manager_get_relation(&self, index: usize) -> String {
        format!("[{}]", index)
    }

    /// Get edges (filters) that are subsets of the requested set.
    fn get_edges(&self, requested_set: &JoinRelationSet) -> Vec<FilterInfoWithTotalDomains> {
        let mut result = Vec::new();
        for relation_2_tdom in &self.relation_set_stats {
            for filter in &relation_2_tdom.filters {
                if JoinRelationSet::is_subset(requested_set, &filter.set) {
                    result.push(FilterInfoWithTotalDomains::new(
                        filter.clone(),
                        relation_2_tdom,
                    ));
                }
            }
        }
        result
    }

    /// Compute equality selectivity once per independent edge of each
    /// equivalence class.
    ///
    /// Walking filters in arbitrary order under-counts cyclic join graphs: as
    /// soon as one spanning path connects the relation set, later equality
    /// classes can be mistaken for redundant edges. Each column-equivalence
    /// class instead contributes `NDV^(vertices-components)`, the rank of its
    /// induced equality graph. Extra transitive predicates add cycles but no
    /// additional selectivity.
    ///
    /// Different equality classes that connect the same relation pair are not
    /// independent unless a joint-domain statistic says so. Multiplying their
    /// single-column NDVs can underestimate a composite-key join by orders of
    /// magnitude. Until such a joint statistic exists, keep the strongest
    /// domain for parallel equality edges. This preserves independent
    /// equality topology across the rest of each graph without manufacturing
    /// independence between columns from marginal statistics alone.
    fn equality_denominator(&mut self, requested_set: &JoinRelationSet) -> (f64, HashSet<usize>) {
        debug_assert!(
            !self.equality_graphs_dirty,
            "equality topology must be compiled before cardinality lookup"
        );
        let mut denominator = 1.0;
        let mut correlated_pairs = HashMap::<(usize, usize), CorrelatedEqualityDomains>::new();
        let mut consumed_filters = HashSet::new();
        let Self {
            relation_set_stats,
            equality_graphs,
            binding_stats,
            relation_unique_keys,
            relation_cardinalities,
            equality_scratch,
            ..
        } = self;

        for graph in equality_graphs {
            let stats = &relation_set_stats[graph.stats_index];
            equality_scratch.prepare(graph.vertices.len());
            for edge in &graph.edges {
                let left_relation = graph.vertices[edge.left].relation;
                let right_relation = graph.vertices[edge.right].relation;
                if !requested_set.contains(left_relation) || !requested_set.contains(right_relation)
                {
                    continue;
                }
                equality_scratch.active[edge.left] = true;
                equality_scratch.active[edge.right] = true;
                union_components(
                    &mut equality_scratch.parents,
                    &mut equality_scratch.sizes,
                    edge.left,
                    edge.right,
                );
                consumed_filters.insert(edge.filter_index);
            }
            if equality_scratch
                .active
                .iter()
                .filter(|active| **active)
                .count()
                < 2
            {
                continue;
            }

            let fallback_distinct = if stats.has_distinct_count_hll {
                DistinctDomainEstimate::observed(stats.distinct_count_hll)
            } else {
                DistinctDomainEstimate::upper_bound(stats.distinct_count_no_hll)
            };
            for (index, vertex) in graph.vertices.iter().enumerate() {
                if !equality_scratch.active[index] {
                    continue;
                }
                let distinct = binding_stats.get(&vertex.binding).map(|binding| {
                    if binding.from_hll {
                        DistinctDomainEstimate::observed(binding.distinct_count)
                    } else {
                        DistinctDomainEstimate::upper_bound(binding.distinct_count)
                    }
                });
                let distinct = distinct.unwrap_or(fallback_distinct);
                equality_scratch.vertex_domains[index] = Some(distinct);
                let root = find_component(&mut equality_scratch.parents, index);
                equality_scratch.components[root].push(index);
            }
            for component_vertices in equality_scratch
                .components
                .iter_mut()
                .filter(|component| !component.is_empty())
            {
                let vertices = component_vertices.len();
                let independent_edges = vertices.saturating_sub(1);
                // Under uniformity and containment, equating N domains
                // contributes the product of the N-1 largest NDVs:
                // product(cardinality) * min(NDV) / product(NDV). Numeric
                // min/max ranges provide useful per-column upper bounds when
                // HLL is unavailable; a filtered relation's row count then
                // caps that bound without pretending every join key has the
                // same domain. This is invariant to predicate order and keeps
                // a one-row filtered dimension from erasing the wider domain
                // of the relation it filters.
                let root = *component_vertices
                    .iter()
                    .min_by_key(|vertex| {
                        (
                            equality_scratch.vertex_domains[**vertex]
                                .expect("active equality vertex must have a domain")
                                .value(),
                            graph.vertices[**vertex].relation,
                            graph.vertices[**vertex].binding.table_index,
                            graph.vertices[**vertex].binding.column_index,
                        )
                    })
                    .expect("non-empty equality component must have a root");
                equality_scratch.tree_visited[root] = true;

                // Give every denominator factor a concrete owner edge. The
                // root is the smallest domain, so multiplying every child
                // domain still yields the N-1 largest domains. Ownership is
                // later used to correlate parallel equality classes without
                // ever dividing out a factor that was not multiplied here.
                equality_scratch.tree_frontier.clear();
                push_tree_frontier(
                    graph,
                    &equality_scratch.active,
                    &equality_scratch.tree_visited,
                    &mut equality_scratch.tree_frontier,
                    root,
                );
                let mut remaining = vertices.saturating_sub(1);
                while remaining > 0 {
                    let Some(Reverse((_, _, _, edge_index, child))) =
                        equality_scratch.tree_frontier.pop()
                    else {
                        debug_assert!(false, "equality component must have a spanning tree");
                        break;
                    };
                    if equality_scratch.tree_visited[child] {
                        continue;
                    }
                    equality_scratch.tree_visited[child] = true;
                    equality_scratch.owned_edges[child] = Some(edge_index);
                    push_tree_frontier(
                        graph,
                        &equality_scratch.active,
                        &equality_scratch.tree_visited,
                        &mut equality_scratch.tree_frontier,
                        child,
                    );
                    remaining -= 1;
                }

                let component_denominator = component_vertices
                    .iter()
                    .filter(|vertex| **vertex != root)
                    .fold(1.0, |product, vertex| {
                        product
                            * equality_scratch.vertex_domains[*vertex]
                                .expect("active equality vertex must have a domain")
                                .value() as f64
                    });
                denominator *= component_denominator;
                trace!(
                    target: targets::OPTIMIZER,
                    requested_set = %requested_set,
                    class = ?stats.equivalent_relations,
                    vertices,
                    independent_edges,
                    component_denominator,
                    "Applied equality-class rank selectivity"
                );
            }

            // Correlation is accounted against the exact spanning-tree edge
            // that owns each factor, never reconstructed from endpoint maxima.
            let mut graph_pairs = HashMap::<(usize, usize), EqualityPairDomain>::new();
            for (child, edge_index) in equality_scratch.owned_edges.iter().enumerate() {
                let Some(edge_index) = edge_index else {
                    continue;
                };
                let edge = &graph.edges[*edge_index];
                let left_relation = graph.vertices[edge.left].relation;
                let right_relation = graph.vertices[edge.right].relation;
                let pair = if left_relation < right_relation {
                    (left_relation, right_relation)
                } else {
                    (right_relation, left_relation)
                };
                let domain = equality_scratch.vertex_domains[child]
                    .expect("owned equality vertex must have a domain")
                    .value() as f64;
                let pair_domain = graph_pairs.entry(pair).or_default();
                pair_domain.domain *= domain;
                pair_domain
                    .relation_bindings
                    .entry(left_relation)
                    .or_default()
                    .insert(graph.vertices[edge.left].binding);
                pair_domain
                    .relation_bindings
                    .entry(right_relation)
                    .or_default()
                    .insert(graph.vertices[edge.right].binding);
            }
            for (pair, pair_domain) in graph_pairs {
                use std::collections::hash_map::Entry;
                match correlated_pairs.entry(pair) {
                    Entry::Occupied(mut entry) => entry
                        .get_mut()
                        .add(pair_domain.domain, pair_domain.relation_bindings),
                    Entry::Vacant(entry) => {
                        entry.insert(CorrelatedEqualityDomains::new(
                            pair_domain.domain,
                            pair_domain.relation_bindings,
                        ));
                    }
                }
            }
        }

        for domains in correlated_pairs.values() {
            denominator /= domains.redundant_product;

            let unique_joint_domain = domains
                .relation_bindings
                .iter()
                .filter_map(|(relation, observed_bindings)| {
                    let covers_unique_key =
                        relation_unique_keys.get(relation).is_some_and(|keys| {
                            keys.iter().any(|key| {
                                !key.is_empty()
                                    && key
                                        .iter()
                                        .all(|binding| observed_bindings.contains(binding))
                            })
                        });
                    covers_unique_key
                        .then(|| relation_cardinalities.get(relation).copied())
                        .flatten()
                })
                .max()
                .unwrap_or(0) as f64;
            if unique_joint_domain > domains.strongest {
                denominator *= unique_joint_domain / domains.strongest;
            }
        }
        (denominator.max(1.0), consumed_filters)
    }

    /// Check if an edge connects to a subgraph.
    fn edge_connects(edge: &FilterInfoWithTotalDomains, subgraph: &Subgraph2Denominator) -> bool {
        if let Some(ref relations) = subgraph.relations {
            if let Some(ref left_set) = edge.filter_info.left_set {
                if JoinRelationSet::is_subset(relations, left_set) {
                    return true;
                }
            }
            if let Some(ref right_set) = edge.filter_info.right_set {
                if JoinRelationSet::is_subset(relations, right_set) {
                    return true;
                }
            }
        }
        false
    }

    /// Find subgraphs connected by an edge.
    fn subgraphs_connected_by_edge(
        edge: &FilterInfoWithTotalDomains,
        subgraphs: &[Subgraph2Denominator],
    ) -> Vec<usize> {
        if subgraphs.is_empty() {
            return Vec::new();
        }

        // Check combinations of subgraphs
        for outer in 0..subgraphs.len() {
            for inner in (outer + 1)..subgraphs.len() {
                if Self::edge_connects(edge, &subgraphs[outer])
                    && Self::edge_connects(edge, &subgraphs[inner])
                {
                    // Order is important: we delete the inner subgraph later
                    return vec![outer, inner];
                }
            }
            // Check if edge connects with just outer
            if Self::edge_connects(edge, &subgraphs[outer]) {
                return vec![outer];
            }
        }

        Vec::new()
    }
    fn update_numerator_relations(
        &mut self,
        left: &Subgraph2Denominator,
        right: &Subgraph2Denominator,
        filter: &FilterInfoWithTotalDomains,
    ) -> Arc<JoinRelationSet> {
        match filter.filter_info.join_type {
            JoinType::Semi | JoinType::Anti => {
                // For SEMI/ANTI joins, only include the left side in numerator
                if let (Some(ref left_rels), Some(ref filter_left)) =
                    (&left.relations, &filter.filter_info.left_set)
                {
                    if let Some(ref right_rels) = right.relations {
                        if let Some(ref filter_right) = filter.filter_info.right_set {
                            if JoinRelationSet::is_subset(left_rels, filter_left)
                                && JoinRelationSet::is_subset(right_rels, filter_right)
                            {
                                return left.numerator_relations.clone().unwrap();
                            }
                        }
                    }
                }
                right.numerator_relations.clone().unwrap()
            }
            _ => {
                // Cross product or inner join
                let left_num = left.numerator_relations.as_ref().unwrap();
                let right_num = right.numerator_relations.as_ref().unwrap();
                self.set_manager.union(left_num, right_num)
            }
        }
    }

    /// Calculate updated denominator for a join.
    fn calculate_updated_denom(
        &self,
        left: &Subgraph2Denominator,
        right: &Subgraph2Denominator,
        filter: &FilterInfoWithTotalDomains,
    ) -> f64 {
        let mut new_denom = left.denom * right.denom;

        match filter.filter_info.join_type {
            JoinType::Inner => {
                // Get comparison type from the filter expression
                let comparison_type = Self::get_comparison_type(&filter.filter_info.filter);

                if comparison_type.is_none() {
                    // No comparison, denominator is just the product
                    new_denom *= filter.get_distinct_count() as f64;
                    return new_denom;
                }
                let extra_ratio = match comparison_type {
                    Some(ComparisonKind::Equal) => filter.get_distinct_count() as f64,
                    Some(ComparisonKind::NotEqual) | Some(ComparisonKind::Range) => {
                        // Assume this blows up, but use tdom to bound it
                        let tdom = filter.get_distinct_count() as f64;
                        tdom.powf(2.0 / 3.0)
                    }
                    None => 1.0,
                };

                new_denom *= extra_ratio;
                new_denom
            }
            JoinType::Semi | JoinType::Anti => {
                let output_fraction = self.semi_anti_output_fraction(filter);
                if let (Some(ref left_rels), Some(ref filter_left)) =
                    (&left.relations, &filter.filter_info.left_set)
                {
                    if let Some(ref right_rels) = right.relations {
                        if let Some(ref filter_right) = filter.filter_info.right_set {
                            if JoinRelationSet::is_subset(left_rels, filter_left)
                                && JoinRelationSet::is_subset(right_rels, filter_right)
                            {
                                return left.denom / output_fraction;
                            }
                        }
                    }
                }
                right.denom / output_fraction
            }
            _ => {
                // Cross product
                new_denom
            }
        }
    }

    /// Estimate the output fraction of the preserved side of a SEMI/ANTI join.
    ///
    /// For an equality predicate, the right-hand key domain bounds how many
    /// distinct left-hand keys can match. This is especially important for a
    /// semi join against a selective grouped subquery: its output cardinality
    /// already bounds its NDV, whereas a fixed selectivity discards that signal.
    fn semi_anti_output_fraction(&self, filter: &FilterInfoWithTotalDomains) -> f64 {
        let estimate = if Self::get_comparison_type(&filter.filter_info.filter)
            == Some(ComparisonKind::Equal)
        {
            filter
                .filter_info
                .reduction_join_bindings()
                .and_then(|bindings| {
                    let preserved = *self.binding_stats.get(&bindings.preserved)?;
                    let filtering = *self.binding_stats.get(&bindings.filtering)?;
                    let estimate = (preserved.distinct_count > 0).then_some((
                        (filtering.distinct_count as f64
                            / preserved.distinct_count.max(filtering.distinct_count) as f64)
                            .clamp(0.0, 1.0),
                        1.0 / preserved.relation_cardinality.max(1) as f64,
                    ));
                    trace!(
                        target: targets::OPTIMIZER,
                        filter_index = filter.filter_info.filter_index,
                        join_type = ?filter.filter_info.join_type,
                        preserved_binding = ?bindings.preserved,
                        filtering_binding = ?bindings.filtering,
                        preserved_ndv = preserved.distinct_count,
                        filtering_ndv = filtering.distinct_count,
                        preserved_rows = preserved.relation_cardinality,
                        filtering_rows = filtering.relation_cardinality,
                        "Estimated reduction-join key coverage"
                    );
                    estimate
                })
        } else {
            None
        };
        let (matched_fraction, minimum_output_fraction) =
            estimate.unwrap_or((DEFAULT_SEMI_ANTI_MATCH_FRACTION, 0.0));

        let output_fraction = match filter.filter_info.join_type {
            JoinType::Semi => matched_fraction,
            JoinType::Anti => 1.0 - matched_fraction,
            _ => return DEFAULT_SEMI_ANTI_MATCH_FRACTION,
        };

        // Cardinality estimates participate in divisions and plan costs. Keep
        // an empty estimate representable as a single expected row instead of
        // introducing infinity into the denominator graph.
        output_fraction.max(minimum_output_fraction)
    }

    /// Get the comparison type from an expression.
    fn get_comparison_type(expr: &Expression) -> Option<ComparisonKind> {
        match expr {
            Expression::Comparison(comp) => {
                use paro_planner::expression::ComparisonType;
                match comp.comparison_type {
                    ComparisonType::Equal | ComparisonType::NotDistinctFrom => {
                        Some(ComparisonKind::Equal)
                    }
                    ComparisonType::NotEqual | ComparisonType::DistinctFrom => {
                        Some(ComparisonKind::NotEqual)
                    }
                    ComparisonType::LessThan
                    | ComparisonType::LessThanOrEqual
                    | ComparisonType::GreaterThan
                    | ComparisonType::GreaterThanOrEqual => Some(ComparisonKind::Range),
                }
            }
            Expression::Conjunction(conj) => {
                if conj.conjunction_type == ConjunctionType::Or {
                    return None;
                }
                // Equality determines the hash domain of a mixed conjunction
                // regardless of SQL predicate order. Residual inequalities
                // refine matches inside that domain; they must not hide the
                // equality simply because they appear first.
                conj.children
                    .iter()
                    .filter_map(Self::get_comparison_type)
                    .min_by_key(|kind| match kind {
                        ComparisonKind::Equal => 0,
                        ComparisonKind::NotEqual => 1,
                        ComparisonKind::Range => 2,
                    })
            }
            _ => None,
        }
    }

    /// Get the denominator for cardinality calculation.
    fn get_denominator(&mut self, set: &JoinRelationSet) -> DenomInfo {
        let mut subgraphs: Vec<Subgraph2Denominator> = Vec::new();
        let mut unused_edge_tdoms: HashSet<usize> = HashSet::new();
        let (equality_denominator, consumed_equalities) = self.equality_denominator(set);

        // Get edges sorted by largest tdom to smallest
        let edges = self
            .get_edges(set)
            .into_iter()
            .filter(|edge| !consumed_equalities.contains(&edge.filter_info.filter_index))
            .collect::<Vec<_>>();

        for edge in &edges {
            // Check if we've already connected all relations
            if subgraphs.len() == 1 {
                if let Some(ref rels) = subgraphs[0].relations {
                    if rels.to_string() == set.to_string() {
                        // All relations connected, skip remaining edges
                        if edge.has_distinct_count_hll {
                            unused_edge_tdoms.insert(edge.distinct_count_hll);
                        }
                        continue;
                    }
                }
            }

            let subgraph_connections = Self::subgraphs_connected_by_edge(edge, &subgraphs);

            if subgraph_connections.is_empty() {
                // Create a new subgraph from this edge
                let mut left_subgraph = Subgraph2Denominator::default();
                let mut right_subgraph = Subgraph2Denominator::default();

                left_subgraph.relations = edge.filter_info.left_set.clone();
                left_subgraph.numerator_relations = edge.filter_info.left_set.clone();
                right_subgraph.relations = edge.filter_info.right_set.clone();
                right_subgraph.numerator_relations = edge.filter_info.right_set.clone();

                left_subgraph.numerator_relations =
                    Some(self.update_numerator_relations(&left_subgraph, &right_subgraph, edge));
                left_subgraph.relations = Some(edge.filter_info.set.clone());
                left_subgraph.denom =
                    self.calculate_updated_denom(&left_subgraph, &right_subgraph, edge);

                subgraphs.push(left_subgraph);
            } else if subgraph_connections.len() == 1 {
                // Extend existing subgraph
                let idx = subgraph_connections[0];
                let mut right_subgraph = Subgraph2Denominator::default();
                right_subgraph.relations = edge.filter_info.right_set.clone();
                right_subgraph.numerator_relations = edge.filter_info.right_set.clone();

                // Check if right is already in left
                if let Some(ref left_rels) = subgraphs[idx].relations {
                    if let Some(ref right_rels) = right_subgraph.relations {
                        if JoinRelationSet::is_subset(left_rels, right_rels) {
                            right_subgraph.relations = edge.filter_info.left_set.clone();
                            right_subgraph.numerator_relations = edge.filter_info.left_set.clone();
                        }
                    }
                }

                // Check if edge connects same subgraph to itself
                if let (Some(ref left_rels), Some(ref filter_left), Some(ref filter_right)) = (
                    &subgraphs[idx].relations,
                    &edge.filter_info.left_set,
                    &edge.filter_info.right_set,
                ) {
                    if JoinRelationSet::is_subset(left_rels, filter_left)
                        && JoinRelationSet::is_subset(left_rels, filter_right)
                    {
                        // Edge connects same subgraph, skip
                        continue;
                    }
                }

                let new_numerator =
                    self.update_numerator_relations(&subgraphs[idx], &right_subgraph, edge);
                let new_relations = if let Some(ref left_rels) = subgraphs[idx].relations {
                    if let Some(ref right_rels) = right_subgraph.relations {
                        Some(self.set_manager.union(left_rels, right_rels))
                    } else {
                        Some(left_rels.clone())
                    }
                } else {
                    right_subgraph.relations.clone()
                };
                let new_denom =
                    self.calculate_updated_denom(&subgraphs[idx], &right_subgraph, edge);

                subgraphs[idx].numerator_relations = Some(new_numerator);
                subgraphs[idx].relations = new_relations;
                subgraphs[idx].denom = new_denom;
            } else if subgraph_connections.len() == 2 {
                // Merge two subgraphs
                let idx0 = subgraph_connections[0];
                let idx1 = subgraph_connections[1];
                debug_assert!(idx0 < idx1);

                let subgraph_to_delete = subgraphs[idx1].clone();

                let new_relations = if let (Some(ref left_rels), Some(ref right_rels)) =
                    (&subgraphs[idx0].relations, &subgraph_to_delete.relations)
                {
                    Some(self.set_manager.union(left_rels, right_rels))
                } else {
                    subgraphs[idx0].relations.clone()
                };

                let new_numerator =
                    self.update_numerator_relations(&subgraphs[idx0], &subgraph_to_delete, edge);
                let new_denom =
                    self.calculate_updated_denom(&subgraphs[idx0], &subgraph_to_delete, edge);

                subgraphs[idx0].relations = new_relations;
                subgraphs[idx0].numerator_relations = Some(new_numerator);
                subgraphs[idx0].denom = new_denom;

                // Remove the merged subgraph
                subgraphs.remove(idx1);
            }
        }

        // Slight penalty for unused edges
        let denom_multiplier = 1.0 + unused_edge_tdoms.len() as f64;

        // Merge remaining subgraphs as cross products
        if subgraphs.len() > 1 {
            let mut final_subgraph = subgraphs[0].clone();
            for merge_with in subgraphs.iter().skip(1) {
                if let (Some(ref left_rels), Some(ref right_rels)) =
                    (&final_subgraph.relations, &merge_with.relations)
                {
                    final_subgraph.relations = Some(self.set_manager.union(left_rels, right_rels));
                }
                if let (Some(ref left_num), Some(ref right_num)) = (
                    &final_subgraph.numerator_relations,
                    &merge_with.numerator_relations,
                ) {
                    final_subgraph.numerator_relations =
                        Some(self.set_manager.union(left_num, right_num));
                }
                final_subgraph.denom *= merge_with.denom;
            }
            subgraphs = vec![final_subgraph];
        }

        // Handle relations connected by cross products
        if !subgraphs.is_empty() {
            // Collect relations to add first to avoid borrow issues
            let relations_to_add: Vec<Arc<JoinRelationSet>> = {
                let rels = match &subgraphs[0].relations {
                    Some(r) => r.clone(),
                    None => return DenomInfo::new(Arc::new(set.clone()), 1.0, 1.0),
                };

                if rels.count() == set.count() {
                    Vec::new()
                } else {
                    (0..set.count())
                        .filter_map(|rel_index| {
                            let relation_id = set.relations()[rel_index];
                            let rel = self.set_manager.get_relation(relation_id);
                            if !JoinRelationSet::is_subset(&rels, &rel) {
                                Some(rel)
                            } else {
                                None
                            }
                        })
                        .collect()
                }
            };

            // Now apply the updates
            for rel in relations_to_add {
                if let Some(ref num_rels) = subgraphs[0].numerator_relations {
                    subgraphs[0].numerator_relations = Some(self.set_manager.union(num_rels, &rel));
                }
                if let Some(ref cur_rels) = subgraphs[0].relations {
                    subgraphs[0].relations = Some(self.set_manager.union(cur_rels, &rel));
                }
            }
        }

        // Handle empty subgraphs or zero denominator
        if subgraphs.is_empty() || subgraphs[0].denom == 0.0 {
            return DenomInfo::new(Arc::new(set.clone()), 1.0, equality_denominator);
        }

        DenomInfo::new(
            subgraphs[0].numerator_relations.clone().unwrap(),
            1.0,
            subgraphs[0].denom * denom_multiplier * equality_denominator,
        )
    }
}

/// Kind of comparison for cardinality estimation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComparisonKind {
    Equal,
    NotEqual,
    Range,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join_order::relation::JoinRelationSetManager;
    use crate::join_order::relation_manager::DistinctCount;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
        ConjunctionType, ConstantExpression,
    };

    fn create_column_ref(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(ColumnRefExpression {
            binding: paro_planner::operator::ColumnBinding {
                table_index,
                column_index,
            },
            depth: 0,
            return_type: LogicalType::Integer,
        })
    }

    fn create_constant(value: i64) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::BigInt(value),
            return_type: LogicalType::BigInt,
        })
    }

    fn column_distinct_counts(
        table_index: usize,
        counts: impl IntoIterator<Item = DistinctCount>,
    ) -> HashMap<ColumnBinding, DistinctCount> {
        counts
            .into_iter()
            .enumerate()
            .map(|(column_index, count)| (ColumnBinding::new(table_index, column_index), count))
            .collect()
    }

    fn create_equality_filter(
        set_manager: &mut JoinRelationSetManager,
        left_table: usize,
        left_col: usize,
        right_table: usize,
        right_col: usize,
        filter_index: usize,
    ) -> Arc<FilterInfo> {
        let expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(left_table, left_col)),
            right: Box::new(create_column_ref(right_table, right_col)),
            comparison_type: ComparisonType::Equal,
        });

        let set = set_manager.get_relation_from_vec(vec![left_table, right_table]);
        let left_set = set_manager.get_relation(left_table);
        let right_set = set_manager.get_relation(right_table);

        let mut filter = FilterInfo::new_inner(expr, set, filter_index);
        filter.set_left_set(left_set);
        filter.set_right_set(right_set);
        filter.set_left_binding(ColumnBinding::new(left_table, left_col), left_table);
        filter.set_right_binding(ColumnBinding::new(right_table, right_col), right_table);

        Arc::new(filter)
    }

    fn create_semi_anti_filter(
        set_manager: &mut JoinRelationSetManager,
        join_type: JoinType,
    ) -> Arc<FilterInfo> {
        let expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::Equal,
        });
        let set = set_manager.get_relation_from_vec(vec![0, 1]);
        let left_set = set_manager.get_relation(0);
        let right_set = set_manager.get_relation(1);
        let mut filter = FilterInfo::new(
            expr,
            set,
            0,
            join_type,
            paro_planner::operator::AntiJoinMode::Regular,
        );
        filter.set_left_set(left_set);
        filter.set_right_set(right_set);
        filter.set_left_binding(ColumnBinding::new(0, 0), 0);
        filter.set_right_binding(ColumnBinding::new(1, 0), 1);
        Arc::new(filter)
    }

    fn create_single_column_filter(
        set_manager: &mut JoinRelationSetManager,
        table: usize,
        col: usize,
        filter_index: usize,
    ) -> Arc<FilterInfo> {
        let expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(table, col)),
            right: Box::new(create_constant(10)),
            comparison_type: ComparisonType::GreaterThan,
        });

        let set = set_manager.get_relation(table);
        let left_set = set_manager.get_relation(table);

        let mut filter = FilterInfo::new_inner(expr, set, filter_index);
        filter.set_left_set(left_set);
        filter.set_left_binding(ColumnBinding::new(table, col), table);

        Arc::new(filter)
    }

    #[test]
    fn test_cardinality_estimator_new() {
        let estimator = CardinalityEstimator::new();
        assert!(estimator.relation_set_stats.is_empty());
        assert!(estimator.relation_set_2_cardinality.is_empty());
    }

    #[test]
    fn test_init_equivalent_relations_single_filter() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        let filter = create_single_column_filter(&mut set_manager, 0, 0, 0);
        estimator.init_equivalent_relations(&[filter]);

        assert_eq!(estimator.relation_set_stats.len(), 1);
        assert!(estimator.relation_set_stats[0]
            .equivalent_relations
            .contains(&ColumnBinding::new(0, 0)));
    }

    #[test]
    fn test_init_equivalent_relations_join_filter() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        estimator.init_equivalent_relations(&[filter]);

        assert_eq!(estimator.relation_set_stats.len(), 1);
        assert!(estimator.relation_set_stats[0]
            .equivalent_relations
            .contains(&ColumnBinding::new(0, 0)));
        assert!(estimator.relation_set_stats[0]
            .equivalent_relations
            .contains(&ColumnBinding::new(1, 0)));
    }

    #[test]
    fn test_init_equivalent_relations_transitive() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        // A.x = B.y and B.y = C.z should create one equivalence class
        let filter1 = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        let filter2 = create_equality_filter(&mut set_manager, 1, 0, 2, 0, 1);

        estimator.init_equivalent_relations(&[filter1, filter2]);

        // Should have one equivalence class with all three bindings
        assert_eq!(estimator.relation_set_stats.len(), 1);
        assert!(estimator.relation_set_stats[0]
            .equivalent_relations
            .contains(&ColumnBinding::new(0, 0)));
        assert!(estimator.relation_set_stats[0]
            .equivalent_relations
            .contains(&ColumnBinding::new(1, 0)));
        assert!(estimator.relation_set_stats[0]
            .equivalent_relations
            .contains(&ColumnBinding::new(2, 0)));
    }

    #[test]
    fn merging_equivalence_classes_retains_all_join_edges() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        let filter_ab = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        let filter_cd = create_equality_filter(&mut set_manager, 2, 0, 3, 0, 1);
        let filter_bc = create_equality_filter(&mut set_manager, 1, 0, 2, 0, 2);

        estimator.init_equivalent_relations(&[filter_ab, filter_cd, filter_bc]);

        assert_eq!(estimator.relation_set_stats.len(), 1);
        assert_eq!(estimator.relation_set_stats[0].filters.len(), 3);
    }

    #[test]
    fn test_init_cardinality_estimator_props() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        // Initialize with a filter first
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        estimator.init_equivalent_relations(&[filter]);

        // Initialize props for relation 0
        let set0 = set_manager.get_relation(0);
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);
        estimator.init_cardinality_estimator_props(&set0, &stats0);

        // Check that cardinality was stored
        assert!(estimator.relation_set_2_cardinality.contains_key("[0]"));
        let helper = &estimator.relation_set_2_cardinality["[0]"];
        assert_eq!(helper.cardinality_before_filters, 1000.0);
    }

    #[test]
    fn test_estimate_cardinality_single_relation() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        // Initialize relation 0
        let set0 = set_manager.get_relation(0);
        let stats0 = RelationStats::with_cardinality(1000);
        estimator.init_cardinality_estimator_props(&set0, &stats0);

        // Estimate cardinality for single relation
        let cardinality = estimator.estimate_cardinality(&set0);
        assert_eq!(cardinality, 1000.0);
    }

    #[test]
    fn test_estimate_cardinality_join() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        // Create join filter
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        estimator.init_equivalent_relations(&[filter]);

        // Initialize relation 0 with cardinality 1000, distinct count 100
        let set0 = set_manager.get_relation(0);
        let mut stats0 = RelationStats::with_cardinality(1000);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(100, true)]);
        estimator.init_cardinality_estimator_props(&set0, &stats0);

        // Initialize relation 1 with cardinality 500, distinct count 50
        let set1 = set_manager.get_relation(1);
        let mut stats1 = RelationStats::with_cardinality(500);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(50, true)]);
        estimator.init_cardinality_estimator_props(&set1, &stats1);

        // Estimate cardinality for join
        let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
        let cardinality = estimator.estimate_cardinality(&join_set);

        // Expected: (1000 * 500) / max(100, 50) = 500000 / 100 = 5000
        assert!(cardinality > 0.0);
    }

    #[test]
    fn parallel_equality_classes_do_not_assume_composite_key_independence() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filters = vec![
            create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
            create_equality_filter(&mut set_manager, 0, 1, 1, 1, 1),
        ];
        estimator.init_equivalent_relations(&filters);

        let left = set_manager.get_relation(0);
        let mut left_stats = RelationStats::with_cardinality(6_001_215);
        left_stats.column_distinct_count = column_distinct_counts(
            0,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&left, &left_stats);

        let right = set_manager.get_relation(1);
        let mut right_stats = RelationStats::with_cardinality(800_000);
        right_stats.column_distinct_count = column_distinct_counts(
            1,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&right, &right_stats);

        let join = set_manager.get_relation_from_vec(vec![0, 1]);
        // Marginal NDVs cannot establish that the two key columns are
        // independent. Use the strongest known single-column selectivity
        // instead of turning a 24M estimate into 2.4K.
        assert_eq!(estimator.estimate_cardinality(&join), 24_004_860.0);
    }

    #[test]
    fn declared_composite_key_provides_a_joint_equality_domain() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filters = vec![
            create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
            create_equality_filter(&mut set_manager, 0, 1, 1, 1, 1),
        ];
        estimator.init_equivalent_relations(&filters);

        let probe = set_manager.get_relation(0);
        let mut probe_stats = RelationStats::with_cardinality(6_001_215);
        probe_stats.column_distinct_count = column_distinct_counts(
            0,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&probe, &probe_stats);

        let build = set_manager.get_relation(1);
        let mut build_stats = RelationStats::with_cardinality(800_000);
        build_stats.column_distinct_count = column_distinct_counts(
            1,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        build_stats.unique_keys = vec![vec![ColumnBinding::new(1, 0), ColumnBinding::new(1, 1)]];
        estimator.init_cardinality_estimator_props(&build, &build_stats);

        let join = set_manager.get_relation_from_vec(vec![0, 1]);
        assert_eq!(estimator.estimate_cardinality(&join), 6_001_215.0);
    }

    #[test]
    fn parallel_key_correlation_survives_a_larger_equivalence_scope() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filters = vec![
            // part.partkey = lineitem.partkey = partsupp.partkey
            create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
            create_equality_filter(&mut set_manager, 1, 0, 2, 0, 1),
            // lineitem.suppkey = partsupp.suppkey
            create_equality_filter(&mut set_manager, 1, 1, 2, 1, 2),
        ];
        estimator.init_equivalent_relations(&filters);

        let part = set_manager.get_relation(0);
        let mut part_stats = RelationStats::with_cardinality(150_000);
        part_stats.column_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(150_000, true)]);
        estimator.init_cardinality_estimator_props(&part, &part_stats);

        let lineitem = set_manager.get_relation(1);
        let mut lineitem_stats = RelationStats::with_cardinality(6_001_215);
        lineitem_stats.column_distinct_count = column_distinct_counts(
            1,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&lineitem, &lineitem_stats);

        let partsupp = set_manager.get_relation(2);
        let mut partsupp_stats = RelationStats::with_cardinality(800_000);
        partsupp_stats.column_distinct_count = column_distinct_counts(
            2,
            [
                DistinctCount::new(200_000, true),
                DistinctCount::new(10_000, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&partsupp, &partsupp_stats);

        let join = set_manager.get_relation_from_vec(vec![0, 1, 2]);
        assert_eq!(estimator.estimate_cardinality(&join), 18_003_645.0);
    }

    #[test]
    fn star_correlation_only_removes_owned_denominator_factors() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filters = vec![
            create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
            create_equality_filter(&mut set_manager, 0, 0, 2, 0, 1),
            create_equality_filter(&mut set_manager, 0, 1, 1, 1, 2),
            create_equality_filter(&mut set_manager, 0, 1, 2, 1, 3),
        ];
        estimator.init_equivalent_relations(&filters);

        for (relation, cardinality, distinct) in [(0, 100, 100), (1, 10, 10), (2, 10, 10)] {
            let set = set_manager.get_relation(relation);
            let mut stats = RelationStats::with_cardinality(cardinality);
            stats.column_distinct_count = column_distinct_counts(
                relation,
                [
                    DistinctCount::new(distinct, true),
                    DistinctCount::new(distinct, true),
                ],
            );
            estimator.init_cardinality_estimator_props(&set, &stats);
        }

        let join = set_manager.get_relation_from_vec(vec![0, 1, 2]);
        estimator.ensure_equality_graphs();
        let (denominator, _) = estimator.equality_denominator(&join);

        // Each star contributes X(100) * one leaf(10). The second,
        // correlated star removes exactly those two owned factors.
        assert_eq!(denominator, 1_000.0);
    }

    #[test]
    fn parallel_self_join_edges_retain_every_owned_factor() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filters = vec![
            // One equality class has three binding vertices but only two
            // relation aliases. Both tree edges therefore belong to pair
            // (0, 1), and both factors must survive pair-level correlation.
            create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0),
            create_equality_filter(&mut set_manager, 0, 1, 1, 0, 1),
            // A second equality class connects the same relation pair.
            create_equality_filter(&mut set_manager, 0, 2, 1, 1, 2),
        ];
        estimator.init_equivalent_relations(&filters);

        let left = set_manager.get_relation(0);
        let mut left_stats = RelationStats::with_cardinality(1_000);
        left_stats.column_distinct_count = column_distinct_counts(
            0,
            [
                DistinctCount::new(5, true),
                DistinctCount::new(5, true),
                DistinctCount::new(20, true),
            ],
        );
        estimator.init_cardinality_estimator_props(&left, &left_stats);

        let right = set_manager.get_relation(1);
        let mut right_stats = RelationStats::with_cardinality(1_000);
        right_stats.column_distinct_count = column_distinct_counts(
            1,
            [DistinctCount::new(1, true), DistinctCount::new(20, true)],
        );
        estimator.init_cardinality_estimator_props(&right, &right_stats);

        let join = set_manager.get_relation_from_vec(vec![0, 1]);
        estimator.ensure_equality_graphs();
        assert_eq!(
            estimator
                .equality_graphs
                .iter()
                .map(|graph| graph.vertices.len())
                .max(),
            Some(3)
        );
        let (denominator, _) = estimator.equality_denominator(&join);

        // The first class owns 5 * 5 = 25 for pair (0, 1); the second owns
        // 20. Correlation keeps the stronger whole-class domain, 25.
        assert_eq!(denominator, 25.0);
    }

    #[test]
    fn filtered_relation_domains_produce_a_nonzero_join_estimate() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);
        estimator.init_equivalent_relations(&[filter]);

        let set0 = set_manager.get_relation(0);
        let mut stats0 = RelationStats::with_cardinality(9);
        stats0.column_distinct_count = column_distinct_counts(0, [DistinctCount::new(9, true)]);
        estimator.init_cardinality_estimator_props(&set0, &stats0);

        let set1 = set_manager.get_relation(1);
        let mut stats1 = RelationStats::with_cardinality(19);
        stats1.column_distinct_count = column_distinct_counts(1, [DistinctCount::new(19, true)]);
        estimator.init_cardinality_estimator_props(&set1, &stats1);

        let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
        assert_eq!(estimator.estimate_cardinality(&join_set), 9.0);
    }

    #[test]
    fn semi_join_uses_the_build_side_distinct_domain() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filter = create_semi_anti_filter(&mut set_manager, JoinType::Semi);
        estimator.init_equivalent_relations(&[filter]);

        let left = set_manager.get_relation(0);
        let mut left_stats = RelationStats::with_cardinality(1_500_000);
        left_stats.column_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(1_500_000, false)]);
        estimator.init_cardinality_estimator_props(&left, &left_stats);

        let right = set_manager.get_relation(1);
        let mut right_stats = RelationStats::with_cardinality(735);
        right_stats.column_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(735, false)]);
        estimator.init_cardinality_estimator_props(&right, &right_stats);

        let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
        assert_eq!(estimator.estimate_cardinality(&join_set), 735.0);
    }

    #[test]
    fn anti_join_estimates_the_unmatched_preserved_rows() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();
        let filter = create_semi_anti_filter(&mut set_manager, JoinType::Anti);
        estimator.init_equivalent_relations(&[filter]);

        let left = set_manager.get_relation(0);
        let mut left_stats = RelationStats::with_cardinality(1_000);
        left_stats.column_distinct_count =
            column_distinct_counts(0, [DistinctCount::new(1_000, true)]);
        estimator.init_cardinality_estimator_props(&left, &left_stats);

        let right = set_manager.get_relation(1);
        let mut right_stats = RelationStats::with_cardinality(100);
        right_stats.column_distinct_count =
            column_distinct_counts(1, [DistinctCount::new(100, true)]);
        estimator.init_cardinality_estimator_props(&right, &right_stats);

        let join_set = set_manager.get_relation_from_vec(vec![0, 1]);
        assert_eq!(estimator.estimate_cardinality(&join_set), 900.0);
    }

    #[test]
    fn test_estimate_cardinality_cached() {
        let mut set_manager = JoinRelationSetManager::new();
        let mut estimator = CardinalityEstimator::new();

        let set0 = set_manager.get_relation(0);
        let stats0 = RelationStats::with_cardinality(1000);
        estimator.init_cardinality_estimator_props(&set0, &stats0);

        // First call
        let card1 = estimator.estimate_cardinality(&set0);
        // Second call should return cached value
        let card2 = estimator.estimate_cardinality(&set0);

        assert_eq!(card1, card2);
    }

    #[test]
    fn test_relations_set_to_stats() {
        let mut bindings = HashSet::new();
        bindings.insert(ColumnBinding::new(0, 0));
        bindings.insert(ColumnBinding::new(1, 0));

        let stats = RelationsSetToStats::new(bindings);
        assert_eq!(stats.equivalent_relations.len(), 2);
        assert!(!stats.has_distinct_count_hll);
        assert_eq!(stats.distinct_count_no_hll, usize::MAX);
    }

    #[test]
    fn test_relations_set_to_stats_get_distinct_count() {
        let mut stats = RelationsSetToStats::new(HashSet::new());

        // Without HLL
        stats.distinct_count_no_hll = 100;
        assert_eq!(stats.get_distinct_count(), 100);

        // With HLL
        stats.has_distinct_count_hll = true;
        stats.distinct_count_hll = 150;
        assert_eq!(stats.get_distinct_count(), 150);
    }

    #[test]
    fn test_filter_info_with_total_domains() {
        let mut set_manager = JoinRelationSetManager::new();
        let filter = create_equality_filter(&mut set_manager, 0, 0, 1, 0, 0);

        let mut stats = RelationsSetToStats::new(HashSet::new());
        stats.distinct_count_hll = 100;
        stats.has_distinct_count_hll = true;

        let filter_with_domains = FilterInfoWithTotalDomains::new(filter, &stats);
        assert_eq!(filter_with_domains.distinct_count_hll, 100);
        assert!(filter_with_domains.has_distinct_count_hll);
        assert_eq!(filter_with_domains.get_distinct_count(), 100);
    }

    #[test]
    fn test_cardinality_helper() {
        let helper = CardinalityHelper::new(1000.0);
        assert_eq!(helper.cardinality_before_filters, 1000.0);
    }

    #[test]
    fn test_denom_info() {
        let set = Arc::new(JoinRelationSet::new(vec![0, 1]));
        let info = DenomInfo::new(set.clone(), 1.0, 100.0);

        assert_eq!(info.filter_strength, 1.0);
        assert_eq!(info.denominator, 100.0);
        assert_eq!(info.numerator_relations.count(), 2);
    }

    #[test]
    fn test_remove_empty_total_domains() {
        let mut estimator = CardinalityEstimator::new();

        // Add some stats
        let mut bindings1 = HashSet::new();
        bindings1.insert(ColumnBinding::new(0, 0));
        estimator
            .relation_set_stats
            .push(RelationsSetToStats::new(bindings1));

        // Add empty stats
        estimator
            .relation_set_stats
            .push(RelationsSetToStats::new(HashSet::new()));

        // Add more stats
        let mut bindings2 = HashSet::new();
        bindings2.insert(ColumnBinding::new(1, 0));
        estimator
            .relation_set_stats
            .push(RelationsSetToStats::new(bindings2));

        estimator.remove_empty_total_domains();

        assert_eq!(estimator.relation_set_stats.len(), 2);
    }

    #[test]
    fn test_get_comparison_type() {
        // Equal comparison
        let eq_expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::Equal,
        });
        assert_eq!(
            CardinalityEstimator::get_comparison_type(&eq_expr),
            Some(ComparisonKind::Equal)
        );

        // Not equal comparison
        let ne_expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::NotEqual,
        });
        assert_eq!(
            CardinalityEstimator::get_comparison_type(&ne_expr),
            Some(ComparisonKind::NotEqual)
        );

        // Range comparison
        let lt_expr = Expression::Comparison(ComparisonExpression {
            left: Box::new(create_column_ref(0, 0)),
            right: Box::new(create_column_ref(1, 0)),
            comparison_type: ComparisonType::LessThan,
        });
        assert_eq!(
            CardinalityEstimator::get_comparison_type(&lt_expr),
            Some(ComparisonKind::Range)
        );

        // Constant (no comparison)
        let const_expr = create_constant(42);
        assert_eq!(CardinalityEstimator::get_comparison_type(&const_expr), None);

        let disjunction = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::Or,
            vec![eq_expr, create_constant(7)],
        ));
        assert_eq!(
            CardinalityEstimator::get_comparison_type(&disjunction),
            None
        );
    }
}
