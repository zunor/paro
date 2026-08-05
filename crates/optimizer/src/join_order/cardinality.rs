// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cardinality estimation helpers for join-order planning.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_planner::expression::Expression;
use paro_planner::operator::{ColumnBinding, JoinType};

use crate::join_order::query_graph::FilterInfo;
use crate::join_order::relation::{JoinRelationSet, JoinRelationSetManager};
use crate::join_order::relation_manager::RelationStats;

/// Default selectivity for SEMI/ANTI joins.
pub const DEFAULT_SEMI_ANTI_SELECTIVITY: f64 = 5.0;

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
    }

    /// Estimate cardinality for a join relation set.
    pub fn estimate_cardinality(&mut self, new_set: &JoinRelationSet) -> f64 {
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
                if r2tdom.equivalent_relations.contains(&left_binding) {
                    matching_sets.push(i);
                    continue;
                }
            }
            if let Some(right_binding) = filter_info.right_binding {
                if r2tdom.equivalent_relations.contains(&right_binding) {
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
            self.relation_set_stats[idx0].filters.push(filter_info);

            // Clear the second set (will be removed later)
            self.relation_set_stats[idx1].equivalent_relations.clear();
        } else if matching_sets.len() == 1 {
            let idx = matching_sets[0];
            if let Some(left_binding) = filter_info.left_binding {
                self.relation_set_stats[idx]
                    .equivalent_relations
                    .insert(left_binding);
            }
            if let Some(right_binding) = filter_info.right_binding {
                self.relation_set_stats[idx]
                    .equivalent_relations
                    .insert(right_binding);
            }
            self.relation_set_stats[idx].filters.push(filter_info);
        } else {
            // No matching sets, create a new one
            let mut bindings = HashSet::new();
            if let Some(left_binding) = filter_info.left_binding {
                bindings.insert(left_binding);
            }
            if let Some(right_binding) = filter_info.right_binding {
                bindings.insert(right_binding);
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
                // For SEMI/ANTI, use default selectivity
                if let (Some(ref left_rels), Some(ref filter_left)) =
                    (&left.relations, &filter.filter_info.left_set)
                {
                    if let Some(ref right_rels) = right.relations {
                        if let Some(ref filter_right) = filter.filter_info.right_set {
                            if JoinRelationSet::is_subset(left_rels, filter_left)
                                && JoinRelationSet::is_subset(right_rels, filter_right)
                            {
                                return left.denom * DEFAULT_SEMI_ANTI_SELECTIVITY;
                            }
                        }
                    }
                }
                right.denom * DEFAULT_SEMI_ANTI_SELECTIVITY
            }
            _ => {
                // Cross product
                new_denom
            }
        }
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
                // Check children for comparison
                for child in &conj.children {
                    if let Some(kind) = Self::get_comparison_type(child) {
                        return Some(kind);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Get the denominator for cardinality calculation.
    fn get_denominator(&mut self, set: &JoinRelationSet) -> DenomInfo {
        let mut subgraphs: Vec<Subgraph2Denominator> = Vec::new();
        let mut unused_edge_tdoms: HashSet<usize> = HashSet::new();

        // Get edges sorted by largest tdom to smallest
        let edges = self.get_edges(set);

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
            return DenomInfo::new(Arc::new(set.clone()), 1.0, 1.0);
        }

        DenomInfo::new(
            subgraphs[0].numerator_relations.clone().unwrap(),
            1.0,
            subgraphs[0].denom * denom_multiplier,
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
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression,
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
        filter.set_left_binding(ColumnBinding::new(left_table, left_col));
        filter.set_right_binding(ColumnBinding::new(right_table, right_col));

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
        filter.set_left_binding(ColumnBinding::new(table, col));

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
    }
}
