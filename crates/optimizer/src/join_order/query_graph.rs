// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Query graph data structures used by the join-order optimizer.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use paro_planner::expression::Expression;
use paro_planner::operator::{AntiJoinMode, ColumnBinding, JoinType};

use crate::join_order::relation::JoinRelationSet;

/// Information about a join filter.
///
/// This struct is used by the cardinality estimator to set the initial cardinality
/// and is also eventually transformed into a query edge.
///
#[derive(Debug, Clone)]
pub struct FilterInfo {
    /// The filter expression.
    pub filter: Expression,
    /// The set of relations this filter references.
    pub set: Arc<JoinRelationSet>,
    /// Index of this filter in the filter list.
    pub filter_index: usize,
    /// The type of join this filter is part of.
    join_type: JoinType,
    /// NULL semantics for an anti-join edge.
    pub anti_join_mode: AntiJoinMode,
    /// The left side of the join (if applicable).
    left_set: Option<Arc<JoinRelationSet>>,
    /// The right side of the join (if applicable).
    right_set: Option<Arc<JoinRelationSet>>,
    /// Direct equality key from the left expression, including its optimizer
    /// relation ID. Table indexes and relation IDs are separate namespaces.
    pub left_binding: Option<JoinKeyBinding>,
    /// Direct equality key from the right expression, including its optimizer
    /// relation ID. Table indexes and relation IDs are separate namespaces.
    pub right_binding: Option<JoinKeyBinding>,
    /// Logical preserved/filtering roles resolved once after edge extraction.
    reduction_roles: Option<ReductionJoinRoles>,
}

/// Orientation of a logical join edge across a candidate tree cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinEdgeOrientation {
    Forward,
    Inverted,
}

/// Logical roles carried by a reduction-join edge.
///
/// These are resolved once when the cut predicate set is built. Costing and
/// plan reconstruction must consume this same witness rather than independently
/// rediscovering which child is preserved by SEMI/ANTI semantics.
#[derive(Debug, Clone)]
pub(crate) struct ReductionJoinRoles {
    preserved: Arc<JoinRelationSet>,
    filtering: Arc<JoinRelationSet>,
}

impl ReductionJoinRoles {
    fn new(filter: &FilterInfo) -> Option<Self> {
        Some(Self {
            preserved: Arc::clone(filter.left_set.as_ref()?),
            filtering: Arc::clone(filter.right_set.as_ref()?),
        })
    }

    pub(crate) fn orientation_across(
        &self,
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> Option<JoinEdgeOrientation> {
        edge_orientation(left, right, &self.preserved, &self.filtering)
    }
}

/// Predicates and authoritative logical semantics for one join-tree cut.
#[derive(Debug, Clone)]
pub(crate) struct JoinPredicateSet {
    predicates: Box<[OrientedJoinPredicate]>,
    join_type: JoinType,
    anti_join_mode: AntiJoinMode,
    reduction_orientation: Option<JoinEdgeOrientation>,
    has_join_conditions: bool,
}

/// One selected filter with its expression orientation resolved for this cut.
///
/// `None` means the expression cannot be represented as a binary join
/// condition across the cut and must remain a residual filter.
#[derive(Debug, Clone)]
pub(crate) struct OrientedJoinPredicate {
    filter: Arc<FilterInfo>,
    orientation: Option<JoinEdgeOrientation>,
}

impl OrientedJoinPredicate {
    pub(crate) fn filter(&self) -> &FilterInfo {
        self.filter.as_ref()
    }

    pub(crate) fn orientation(&self) -> Option<JoinEdgeOrientation> {
        self.orientation
    }
}

/// Outcome of resolving the predicates attached to one candidate DP cut.
///
/// Invalid reduction metadata makes the whole join region ineligible for
/// reordering. It is deliberately not an execution error: the caller still
/// owns the original logical tree and can retain it unchanged.
#[derive(Debug, Clone)]
pub(crate) enum CutPredicateResolution {
    Resolved(Option<JoinPredicateSet>),
    Ineligible,
}

impl JoinPredicateSet {
    /// Build a cut predicate set, resolving reduction semantics once.
    ///
    /// A SEMI/ANTI boundary owns the cut: predicates with other join types are
    /// not folded into that reduction operator. Duplicate graph edges are
    /// collapsed by their stable filter index.
    pub(crate) fn from_filters<'a>(
        filters: impl IntoIterator<Item = &'a Arc<FilterInfo>>,
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> CutPredicateResolution {
        // Cut predicate sets are tiny in practice. A single linear-deduplicated
        // vector avoids both the temporary candidate allocation and a hash
        // table on every DP pair while retaining stable predicate order.
        let mut boundary_join_type = None;
        let mut selected = Vec::<Arc<FilterInfo>>::new();
        for filter in filters {
            if boundary_join_type.is_none()
                && matches!(filter.join_type, JoinType::Semi | JoinType::Anti)
            {
                boundary_join_type = Some(filter.join_type);
                selected.retain(|candidate| candidate.join_type == filter.join_type);
            }
            if boundary_join_type.is_some_and(|join_type| filter.join_type != join_type)
                || selected
                    .iter()
                    .any(|candidate| candidate.filter_index == filter.filter_index)
            {
                continue;
            }
            selected.push(Arc::clone(filter));
        }
        let filters = selected;
        if filters.is_empty() {
            return CutPredicateResolution::Resolved(None);
        }

        let semantic_filter = filters
            .iter()
            .find(|filter| matches!(filter.join_type, JoinType::Semi | JoinType::Anti))
            .or_else(|| {
                filters
                    .iter()
                    .find(|filter| filter.join_type != JoinType::Invalid)
            });
        let (join_type, anti_join_mode) = semantic_filter
            .map_or((JoinType::Inner, AntiJoinMode::Regular), |filter| {
                (filter.join_type, filter.anti_join_mode)
            });
        let reduction_orientation = if matches!(join_type, JoinType::Semi | JoinType::Anti) {
            if filters
                .iter()
                .filter(|filter| filter.join_type == join_type)
                .any(|filter| filter.reduction_roles.is_none())
            {
                return CutPredicateResolution::Ineligible;
            }
            let mut role_orientations = filters
                .iter()
                .filter(|filter| filter.join_type == join_type)
                .map(|filter| {
                    filter
                        .reduction_roles
                        .as_ref()
                        .and_then(|roles| roles.orientation_across(left, right))
                });
            let Some(Some(orientation)) = role_orientations.next() else {
                return CutPredicateResolution::Ineligible;
            };
            if role_orientations.any(|candidate| candidate != Some(orientation)) {
                return CutPredicateResolution::Ineligible;
            }
            Some(orientation)
        } else {
            None
        };

        let predicates: Box<[_]> = filters
            .into_iter()
            .map(|filter| OrientedJoinPredicate {
                orientation: filter.orientation_across(left, right),
                filter,
            })
            .collect();
        let has_join_conditions = predicates
            .iter()
            .any(|predicate| predicate.orientation.is_some());
        CutPredicateResolution::Resolved(Some(Self {
            predicates,
            join_type,
            anti_join_mode,
            reduction_orientation,
            has_join_conditions,
        }))
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub(crate) fn predicates(&self) -> &[OrientedJoinPredicate] {
        &self.predicates
    }

    pub(crate) fn anti_join_mode(&self) -> AntiJoinMode {
        self.anti_join_mode
    }

    pub(crate) fn reduction_orientation(&self) -> Option<JoinEdgeOrientation> {
        self.reduction_orientation
    }

    /// Whether reconstruction can materialize at least one binary comparison
    /// condition on this cut.
    ///
    /// Multi-relation residual expressions still create query-graph
    /// connectivity, but they lower as a cross product followed by a filter.
    /// Costing and reconstruction consume this same resolved witness.
    pub(crate) fn has_join_conditions(&self) -> bool {
        self.has_join_conditions
    }
}

/// A direct equality key and the join-order relation that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinKeyBinding {
    pub column: ColumnBinding,
    pub relation: usize,
}

/// Directional equality bindings for a reduction join.
///
/// SEMI and ANTI joins preserve their logical left side and use the logical
/// right side only to decide whether a preserved row survives. Naming the two
/// roles prevents cardinality code from silently reversing directional
/// estimates such as `filtering_ndv / preserved_ndv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReductionJoinBindings {
    pub preserved: ColumnBinding,
    pub filtering: ColumnBinding,
}

impl FilterInfo {
    /// Create a new FilterInfo.
    pub fn new(
        filter: Expression,
        set: Arc<JoinRelationSet>,
        filter_index: usize,
        join_type: JoinType,
        anti_join_mode: AntiJoinMode,
    ) -> Self {
        Self {
            filter,
            set,
            filter_index,
            join_type,
            anti_join_mode,
            left_set: None,
            right_set: None,
            left_binding: None,
            right_binding: None,
            reduction_roles: None,
        }
    }

    /// Create a new FilterInfo with default INNER join type.
    pub fn new_inner(filter: Expression, set: Arc<JoinRelationSet>, filter_index: usize) -> Self {
        Self::new(
            filter,
            set,
            filter_index,
            JoinType::Inner,
            AntiJoinMode::Regular,
        )
    }

    /// Set the left relation set.
    pub fn set_left_set(&mut self, left_set: Arc<JoinRelationSet>) {
        self.left_set = Some(left_set);
        self.refresh_reduction_roles();
    }

    /// Set the right relation set.
    pub fn set_right_set(&mut self, right_set: Arc<JoinRelationSet>) {
        self.right_set = Some(right_set);
        self.refresh_reduction_roles();
    }

    fn refresh_reduction_roles(&mut self) {
        self.reduction_roles = matches!(self.join_type, JoinType::Semi | JoinType::Anti)
            .then(|| ReductionJoinRoles::new(self))
            .flatten();
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub(crate) fn left_set(&self) -> Option<&Arc<JoinRelationSet>> {
        self.left_set.as_ref()
    }

    pub(crate) fn right_set(&self) -> Option<&Arc<JoinRelationSet>> {
        self.right_set.as_ref()
    }

    /// Set the left column binding.
    pub fn set_left_binding(&mut self, binding: ColumnBinding, relation: usize) {
        self.left_binding = Some(JoinKeyBinding {
            column: binding,
            relation,
        });
    }

    /// Set the right column binding.
    pub fn set_right_binding(&mut self, binding: ColumnBinding, relation: usize) {
        self.right_binding = Some(JoinKeyBinding {
            column: binding,
            relation,
        });
    }

    /// Return equality-key bindings with reduction-join semantics attached.
    ///
    /// Filter extraction preserves the logical expression orientation for
    /// SEMI/ANTI joins: the left expression belongs to the preserved input and
    /// the right expression belongs to the filtering input.
    pub(crate) fn reduction_join_bindings(&self) -> Option<ReductionJoinBindings> {
        if !matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
            return None;
        }
        let preserved = self.left_binding?;
        let filtering = self.right_binding?;
        let bindings = ReductionJoinBindings {
            preserved: preserved.column,
            filtering: filtering.column,
        };
        debug_assert!(
            self.left_set
                .as_ref()
                .is_none_or(|set| set.contains(preserved.relation)),
            "reduction-join preserved binding must belong to its left relation set"
        );
        debug_assert!(
            self.right_set
                .as_ref()
                .is_none_or(|set| set.contains(filtering.relation)),
            "reduction-join filtering binding must belong to its right relation set"
        );
        Some(bindings)
    }

    /// Resolve this predicate's expression orientation across a tree cut.
    pub(crate) fn orientation_across(
        &self,
        left: &JoinRelationSet,
        right: &JoinRelationSet,
    ) -> Option<JoinEdgeOrientation> {
        edge_orientation(
            left,
            right,
            self.left_set.as_ref()?,
            self.right_set.as_ref()?,
        )
    }
}

fn edge_orientation(
    left: &JoinRelationSet,
    right: &JoinRelationSet,
    edge_left: &JoinRelationSet,
    edge_right: &JoinRelationSet,
) -> Option<JoinEdgeOrientation> {
    if left.contains_all(edge_left) && right.contains_all(edge_right) {
        Some(JoinEdgeOrientation::Forward)
    } else if left.contains_all(edge_right) && right.contains_all(edge_left) {
        Some(JoinEdgeOrientation::Inverted)
    } else {
        None
    }
}

/// Information about a neighboring relation in the query graph.
///
#[derive(Debug, Clone)]
pub struct NeighborInfo {
    /// The neighboring relation set.
    pub neighbor: Arc<JoinRelationSet>,
    /// Filters connecting to this neighbor.
    /// Empty filters indicate a cross product edge.
    pub filters: Vec<Arc<FilterInfo>>,
}

impl NeighborInfo {
    /// Create a new NeighborInfo.
    pub fn new(neighbor: Arc<JoinRelationSet>) -> Self {
        Self {
            neighbor,
            filters: Vec::new(),
        }
    }

    /// Create a new NeighborInfo with a filter.
    pub fn with_filter(neighbor: Arc<JoinRelationSet>, filter: Arc<FilterInfo>) -> Self {
        Self {
            neighbor,
            filters: vec![filter],
        }
    }

    /// Add a filter to this neighbor.
    pub fn add_filter(&mut self, filter: Arc<FilterInfo>) {
        self.filters.push(filter);
    }

    /// Check if this is a cross product edge (no filters).
    pub fn is_cross_product(&self) -> bool {
        self.filters.is_empty()
    }
}

/// A node in the query edge tree.
///
/// The tree structure allows efficient lookup of edges for relation sets.
#[derive(Debug, Default)]
struct QueryEdge {
    /// Neighbors at this node.
    neighbors: Vec<NeighborInfo>,
    /// Child edges indexed by relation index.
    children: HashMap<usize, Box<QueryEdge>>,
}

impl QueryEdge {
    fn new() -> Self {
        Self {
            neighbors: Vec::new(),
            children: HashMap::new(),
        }
    }
}

/// The QueryGraphEdges contains edges between relations and allows edges to be created/queried.
///
#[derive(Debug, Default)]
pub struct QueryGraphEdges {
    /// Root of the edge tree.
    root: QueryEdge,
}

impl QueryGraphEdges {
    /// Create a new empty QueryGraphEdges.
    pub fn new() -> Self {
        Self {
            root: QueryEdge::new(),
        }
    }

    /// Create an edge between two relation sets.
    ///
    /// If `filter_info` is None, this creates a cross product edge.
    pub fn create_edge(
        &mut self,
        left: &JoinRelationSet,
        right: Arc<JoinRelationSet>,
        filter_info: Option<Arc<FilterInfo>>,
    ) {
        debug_assert!(left.count() > 0 && right.count() > 0);

        // Find or create the QueryEdge for the left set
        let edge = self.get_or_create_query_edge(left);

        // Check if neighbor already exists
        for neighbor in &mut edge.neighbors {
            if Arc::ptr_eq(&neighbor.neighbor, &right) {
                // Neighbor exists, add filter if we have one
                if let Some(filter) = filter_info {
                    neighbor.add_filter(filter);
                }
                return;
            }
        }

        // Neighbor doesn't exist, create it
        let neighbor = if let Some(filter) = filter_info {
            NeighborInfo::with_filter(right, filter)
        } else {
            NeighborInfo::new(right)
        };
        edge.neighbors.push(neighbor);
    }

    /// Get connections between two relation sets.
    ///
    /// Returns all NeighborInfo entries where the neighbor is a subset of `other`.
    pub fn get_connections(
        &self,
        node: &JoinRelationSet,
        other: &JoinRelationSet,
    ) -> Vec<NeighborInfo> {
        let mut connections = Vec::new();
        self.enumerate_neighbors(node, |info| {
            if other.contains_all(&info.neighbor) {
                connections.push(info.clone());
            }
            false // Continue enumeration
        });
        connections
    }

    /// Get neighbors of a node that are not in the exclusion set.
    ///
    /// Returns the smallest relation index from each valid neighbor.
    pub fn get_neighbors(
        &self,
        node: &JoinRelationSet,
        exclusion_set: &HashSet<usize>,
    ) -> Vec<usize> {
        let mut result = HashSet::new();
        self.enumerate_neighbors(node, |info| {
            if !Self::is_excluded(&info.neighbor, exclusion_set) {
                // Add the smallest relation index from the neighbor
                if let Some(&first) = info.neighbor.relations().first() {
                    result.insert(first);
                }
            }
            false // Continue enumeration
        });
        result.into_iter().collect()
    }

    /// Enumerate all neighbors of a given relation set.
    ///
    /// The callback returns true to stop enumeration early.
    pub fn enumerate_neighbors<F>(&self, node: &JoinRelationSet, mut callback: F)
    where
        F: FnMut(&NeighborInfo) -> bool,
    {
        for j in 0..node.count() {
            if let Some(child) = self.root.children.get(&node.relations()[j]) {
                if self.enumerate_neighbors_dfs(node, child, j + 1, &mut callback) {
                    return;
                }
            }
        }
    }
    // Private helper methods

    /// Get or create the QueryEdge for a relation set.
    fn get_or_create_query_edge(&mut self, left: &JoinRelationSet) -> &mut QueryEdge {
        debug_assert!(left.count() > 0);

        let mut edge = &mut self.root;
        for &rel in left.relations() {
            edge = edge
                .children
                .entry(rel)
                .or_insert_with(|| Box::new(QueryEdge::new()));
        }
        edge
    }

    /// DFS enumeration of neighbors.
    fn enumerate_neighbors_dfs<F>(
        &self,
        node: &JoinRelationSet,
        edge: &QueryEdge,
        index: usize,
        callback: &mut F,
    ) -> bool
    where
        F: FnMut(&NeighborInfo) -> bool,
    {
        // Process neighbors at this node
        for neighbor in &edge.neighbors {
            if callback(neighbor) {
                return true;
            }
        }

        // Continue DFS to children
        for node_index in index..node.count() {
            if let Some(child) = edge.children.get(&node.relations()[node_index]) {
                if self.enumerate_neighbors_dfs(node, child, node_index + 1, callback) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a relation set is excluded.
    fn is_excluded(node: &JoinRelationSet, exclusion_set: &HashSet<usize>) -> bool {
        if let Some(&first) = node.relations().first() {
            exclusion_set.contains(&first)
        } else {
            false
        }
    }

    /// Convert a QueryEdge to string representation.
    fn query_edge_to_string(edge: &QueryEdge, prefix: &[usize]) -> String {
        let mut result = String::new();

        // Format source
        let source = format!(
            "[{}]",
            prefix
                .iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Add neighbors
        for neighbor in &edge.neighbors {
            result.push_str(&format!("{} -> {}\n", source, neighbor.neighbor));
        }

        // Recurse to children
        for (&rel, child) in &edge.children {
            let mut new_prefix = prefix.to_vec();
            new_prefix.push(rel);
            result.push_str(&Self::query_edge_to_string(child, &new_prefix));
        }

        result
    }
}

impl fmt::Display for QueryGraphEdges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Self::query_edge_to_string(&self.root, &[]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join_order::relation::JoinRelationSetManager;
    use paro_common::runtime_value::Value;
    use paro_common::types::LogicalType;
    use paro_planner::expression::ConstantExpression;

    fn create_dummy_filter(set: Arc<JoinRelationSet>, index: usize) -> Arc<FilterInfo> {
        let expr = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });
        Arc::new(FilterInfo::new_inner(expr, set, index))
    }

    #[test]
    fn test_filter_info_creation() {
        let mut manager = JoinRelationSetManager::new();
        let set = manager.get_relation_from_vec(vec![0, 1]);

        let expr = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });
        let filter = FilterInfo::new_inner(expr, set.clone(), 0);

        assert_eq!(filter.filter_index, 0);
        assert_eq!(filter.join_type, JoinType::Inner);
        assert!(filter.left_set.is_none());
        assert!(filter.right_set.is_none());
    }

    #[test]
    fn test_filter_info_with_sets() {
        let mut manager = JoinRelationSetManager::new();
        let set = manager.get_relation_from_vec(vec![0, 1]);
        let left = manager.get_relation(0);
        let right = manager.get_relation(1);

        let expr = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });
        let mut filter = FilterInfo::new(expr, set, 0, JoinType::Left, AntiJoinMode::Regular);
        filter.set_left_set(left.clone());
        filter.set_right_set(right.clone());
        filter.set_left_binding(ColumnBinding::new(0, 0), 0);
        filter.set_right_binding(ColumnBinding::new(1, 0), 1);

        assert_eq!(filter.join_type, JoinType::Left);
        assert!(filter.left_set.is_some());
        assert!(filter.right_set.is_some());
        assert!(filter.left_binding.is_some());
        assert!(filter.right_binding.is_some());
        assert!(filter.reduction_join_bindings().is_none());
    }

    #[test]
    fn reduction_join_bindings_name_preserved_and_filtering_sides() {
        let mut manager = JoinRelationSetManager::new();
        let set = manager.get_relation_from_vec(vec![0, 1]);
        let expr = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });
        let mut filter = FilterInfo::new(expr, set, 0, JoinType::Semi, AntiJoinMode::Regular);
        filter.set_left_set(manager.get_relation(0));
        filter.set_right_set(manager.get_relation(1));
        // Logical table indexes deliberately differ from optimizer relation IDs.
        filter.set_left_binding(ColumnBinding::new(6, 2), 0);
        filter.set_right_binding(ColumnBinding::new(8, 3), 1);

        assert_eq!(
            filter.reduction_join_bindings(),
            Some(ReductionJoinBindings {
                preserved: ColumnBinding::new(6, 2),
                filtering: ColumnBinding::new(8, 3),
            })
        );
    }

    #[test]
    fn predicate_set_rejects_any_reduction_edge_without_roles() {
        let mut manager = JoinRelationSetManager::new();
        let preserved = manager.get_relation(0);
        let filtering = manager.get_relation(1);
        let joined = manager.union(&preserved, &filtering);
        let expression = Expression::Constant(ConstantExpression {
            value: Value::Boolean(true),
            return_type: LogicalType::Boolean,
        });
        let missing_roles = Arc::new(FilterInfo::new(
            expression.clone(),
            joined.clone(),
            0,
            JoinType::Anti,
            AntiJoinMode::Regular,
        ));
        let mut valid_roles =
            FilterInfo::new(expression, joined, 1, JoinType::Anti, AntiJoinMode::Regular);
        valid_roles.set_left_set(preserved.clone());
        valid_roles.set_right_set(filtering.clone());
        let valid_roles = Arc::new(valid_roles);

        assert!(matches!(
            JoinPredicateSet::from_filters([&missing_roles, &valid_roles], &preserved, &filtering),
            CutPredicateResolution::Ineligible
        ));
    }

    #[test]
    fn predicate_set_rejects_reduction_without_a_role_witness() {
        let mut manager = JoinRelationSetManager::new();
        let joined = manager.get_relation_from_vec(vec![0, 1]);
        let missing_roles = Arc::new(FilterInfo::new(
            Expression::Constant(ConstantExpression {
                value: Value::Boolean(true),
                return_type: LogicalType::Boolean,
            }),
            joined,
            0,
            JoinType::Anti,
            AntiJoinMode::Regular,
        ));

        let left = manager.get_relation(0);
        let right = manager.get_relation(1);
        assert!(matches!(
            JoinPredicateSet::from_filters([&missing_roles], &left, &right),
            CutPredicateResolution::Ineligible
        ));
    }

    #[test]
    fn test_neighbor_info_creation() {
        let mut manager = JoinRelationSetManager::new();
        let neighbor = manager.get_relation(1);

        let info = NeighborInfo::new(neighbor);
        assert!(info.is_cross_product());
        assert!(info.filters.is_empty());
    }

    #[test]
    fn test_neighbor_info_with_filter() {
        let mut manager = JoinRelationSetManager::new();
        let set = manager.get_relation_from_vec(vec![0, 1]);
        let neighbor = manager.get_relation(1);
        let filter = create_dummy_filter(set, 0);

        let info = NeighborInfo::with_filter(neighbor, filter);
        assert!(!info.is_cross_product());
        assert_eq!(info.filters.len(), 1);
    }

    #[test]
    fn test_query_graph_create_edge() {
        let mut manager = JoinRelationSetManager::new();
        let left = manager.get_relation(0);
        let right = manager.get_relation(1);
        let set = manager.get_relation_from_vec(vec![0, 1]);
        let filter = create_dummy_filter(set, 0);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&left, right.clone(), Some(filter));

        let s = graph.to_string();
        assert!(s.contains("[0] -> [1]"));
    }

    #[test]
    fn test_query_graph_create_cross_product_edge() {
        let mut manager = JoinRelationSetManager::new();
        let left = manager.get_relation(0);
        let right = manager.get_relation(1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&left, right.clone(), None);

        let s = graph.to_string();
        assert!(s.contains("[0] -> [1]"));
    }

    #[test]
    fn test_query_graph_multiple_edges() {
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let r2 = manager.get_relation(2);
        let set01 = manager.get_relation_from_vec(vec![0, 1]);
        let set12 = manager.get_relation_from_vec(vec![1, 2]);

        let filter1 = create_dummy_filter(set01, 0);
        let filter2 = create_dummy_filter(set12, 1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(filter1));
        graph.create_edge(&r1, r2.clone(), Some(filter2));

        let s = graph.to_string();
        assert!(s.contains("[0] -> [1]"));
        assert!(s.contains("[1] -> [2]"));
    }

    #[test]
    fn test_query_graph_add_filter_to_existing_edge() {
        let mut manager = JoinRelationSetManager::new();
        let left = manager.get_relation(0);
        let right = manager.get_relation(1);
        let set = manager.get_relation_from_vec(vec![0, 1]);

        let filter1 = create_dummy_filter(set.clone(), 0);
        let filter2 = create_dummy_filter(set, 1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&left, right.clone(), Some(filter1));
        graph.create_edge(&left, right.clone(), Some(filter2));

        // Should only have one edge with two filters
        let mut count = 0;
        graph.enumerate_neighbors(&left, |info| {
            count += 1;
            assert_eq!(info.filters.len(), 2);
            false
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn test_query_graph_get_neighbors() {
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let r2 = manager.get_relation(2);
        let set01 = manager.get_relation_from_vec(vec![0, 1]);
        let set02 = manager.get_relation_from_vec(vec![0, 2]);

        let filter1 = create_dummy_filter(set01, 0);
        let filter2 = create_dummy_filter(set02, 1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(filter1));
        graph.create_edge(&r0, r2.clone(), Some(filter2));

        // Get neighbors of r0 with no exclusions
        let neighbors = graph.get_neighbors(&r0, &HashSet::new());
        assert_eq!(neighbors.len(), 2);
        assert!(neighbors.contains(&1));
        assert!(neighbors.contains(&2));

        // Get neighbors of r0 excluding r1
        let mut exclusion = HashSet::new();
        exclusion.insert(1);
        let neighbors = graph.get_neighbors(&r0, &exclusion);
        assert_eq!(neighbors.len(), 1);
        assert!(neighbors.contains(&2));
    }

    #[test]
    fn test_query_graph_get_connections() {
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let r2 = manager.get_relation(2);
        let r12 = manager.get_relation_from_vec(vec![1, 2]);
        let set01 = manager.get_relation_from_vec(vec![0, 1]);
        let set02 = manager.get_relation_from_vec(vec![0, 2]);

        let filter1 = create_dummy_filter(set01, 0);
        let filter2 = create_dummy_filter(set02, 1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(filter1));
        graph.create_edge(&r0, r2.clone(), Some(filter2));

        // Get connections from r0 to r12 (should find both r1 and r2)
        let connections = graph.get_connections(&r0, &r12);
        assert_eq!(connections.len(), 2);
    }

    #[test]
    fn test_query_graph_hyperedge() {
        let mut manager = JoinRelationSetManager::new();
        let r01 = manager.get_relation_from_vec(vec![0, 1]);
        let r2 = manager.get_relation(2);
        let set012 = manager.get_relation_from_vec(vec![0, 1, 2]);

        let filter = create_dummy_filter(set012, 0);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r01, r2.clone(), Some(filter));

        let s = graph.to_string();
        assert!(s.contains("[0, 1] -> [2]"));
    }

    #[test]
    fn test_query_graph_enumerate_neighbors() {
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let r2 = manager.get_relation(2);
        let set01 = manager.get_relation_from_vec(vec![0, 1]);
        let set02 = manager.get_relation_from_vec(vec![0, 2]);

        let filter1 = create_dummy_filter(set01, 0);
        let filter2 = create_dummy_filter(set02, 1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(filter1));
        graph.create_edge(&r0, r2.clone(), Some(filter2));

        let mut neighbors = Vec::new();
        graph.enumerate_neighbors(&r0, |info| {
            neighbors.push(info.neighbor.clone());
            false
        });

        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn test_query_graph_enumerate_early_stop() {
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let r2 = manager.get_relation(2);
        let set01 = manager.get_relation_from_vec(vec![0, 1]);
        let set02 = manager.get_relation_from_vec(vec![0, 2]);

        let filter1 = create_dummy_filter(set01, 0);
        let filter2 = create_dummy_filter(set02, 1);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(filter1));
        graph.create_edge(&r0, r2.clone(), Some(filter2));

        let mut count = 0;
        graph.enumerate_neighbors(&r0, |_| {
            count += 1;
            true // Stop after first
        });

        assert_eq!(count, 1);
    }

    #[test]
    fn test_query_graph_display() {
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let set01 = manager.get_relation_from_vec(vec![0, 1]);

        let filter = create_dummy_filter(set01, 0);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(filter));

        let display = format!("{}", graph);
        assert!(display.contains("[0] -> [1]"));
    }

    #[test]
    fn test_query_graph_empty() {
        let graph = QueryGraphEdges::new();
        assert_eq!(graph.to_string(), "");
    }

    #[test]
    fn test_query_graph_complex_structure() {
        // Create a more complex graph:
        // 0 -- 1
        // 2 -- 3
        let mut manager = JoinRelationSetManager::new();
        let r0 = manager.get_relation(0);
        let r1 = manager.get_relation(1);
        let r2 = manager.get_relation(2);
        let r3 = manager.get_relation(3);

        let set01 = manager.get_relation_from_vec(vec![0, 1]);
        let set02 = manager.get_relation_from_vec(vec![0, 2]);
        let set13 = manager.get_relation_from_vec(vec![1, 3]);
        let set23 = manager.get_relation_from_vec(vec![2, 3]);

        let mut graph = QueryGraphEdges::new();
        graph.create_edge(&r0, r1.clone(), Some(create_dummy_filter(set01, 0)));
        graph.create_edge(&r0, r2.clone(), Some(create_dummy_filter(set02, 1)));
        graph.create_edge(&r1, r3.clone(), Some(create_dummy_filter(set13, 2)));
        graph.create_edge(&r2, r3.clone(), Some(create_dummy_filter(set23, 3)));

        // Check neighbors of each node
        let n0 = graph.get_neighbors(&r0, &HashSet::new());
        assert_eq!(n0.len(), 2);

        let n1 = graph.get_neighbors(&r1, &HashSet::new());
        assert_eq!(n1.len(), 1);
        assert!(n1.contains(&3));

        let n2 = graph.get_neighbors(&r2, &HashSet::new());
        assert_eq!(n2.len(), 1);
        assert!(n2.contains(&3));
    }
}
