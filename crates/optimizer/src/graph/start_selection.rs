// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph Start Selection Optimizer
//!
//! Selects the optimal starting vertex for graph pattern traversal using
//! graph statistics when available and a heuristic fallback otherwise.
//!
//! ## Cost Model
//!
//! For each candidate start vertex:
//! - estimate scan rows from `vertex_count(label)` and filter selectivity
//! - estimate each expand step from `pattern_step_count / vertex_count(source_label)`
//! - choose the lowest total estimated output rows
//!
//! When statistics are unavailable, we fall back to the pre-existing heuristic:
//! - `+100` if the vertex has a filter
//! - `+50` if the filter likely hits a key column
//! - `+25` if the vertex table is narrow
//!
//! Unlike the old implementation, all vertices in the pattern are candidates,
//! so a selective middle vertex can become the start of the traversal.

use paro_common::error::Result;
use paro_parser::ast::EdgeDirection;
use paro_planner::binder::bind::graph::{
    BoundEdgeVariable, BoundPatternElement, BoundVertexVariable,
};
use paro_planner::operator::{GraphMatch, LogicalOperator};
use paro_planner::plan::LogicalPlan;
use paro_storage::index::graph::GraphStatsProvider;

use crate::context::OptimizationContext;

/// Selects the optimal starting vertex for graph pattern traversal.
pub struct GraphStartSelection;

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

impl GraphStartSelection {
    pub fn new() -> Self {
        Self
    }

    /// Optimize the plan by selecting the best start vertex for each graph match.
    pub fn optimize(
        &mut self,
        plan: LogicalPlan,
        ctx: &mut OptimizationContext,
    ) -> Result<LogicalPlan> {
        let plan = plan.try_map_children(|child| self.optimize(child, ctx))?;
        Ok(plan.map_operator(|operator| match operator {
            LogicalOperator::GraphMatch(mut graph_match) => {
                self.select_start(&mut graph_match, ctx);
                LogicalOperator::GraphMatch(graph_match)
            }
            other => other,
        }))
    }

    /// Select the best starting vertex for the pattern and reorder if needed.
    fn select_start(&self, gm: &mut GraphMatch, ctx: &mut OptimizationContext) {
        if gm.has_path_functions {
            return;
        }

        let pattern = Self::parse_pattern(&gm.bound_pattern.elements);
        if pattern.vertices.len() <= 1 {
            return;
        }

        let graph_name = &gm.graph_entry.info.graph_name;
        let stats = ctx.graph_stats.get(graph_name);
        let (start_idx, branch_order) = stats
            .as_deref()
            .map(|stats| self.best_start_with_statistics(&pattern, stats))
            .unwrap_or_else(|| self.best_start_with_heuristic(&pattern));

        if start_idx == 0 && branch_order == BranchOrder::RightFirst {
            return;
        }

        gm.bound_pattern.elements = Self::reorder_pattern(&pattern, start_idx, branch_order);
    }

    /// Score a vertex for start selection.
    ///
    /// Higher score = better candidate for starting the traversal.
    fn score_vertex(v: &BoundVertexVariable) -> i64 {
        let mut score: i64 = 0;

        // +100 if the vertex has a WHERE predicate
        if v.filter.is_some() {
            score += 100;

            // +50 if the predicate references a key (indexed) column
            if Self::filter_references_key_column(v) {
                score += 50;
            }
        }

        // +25 if the vertex table is "smaller" (fewer property columns as proxy)
        // Fewer property columns suggests a simpler/smaller table.
        if v.vertex_table_info.property_column_ids.len() <= 3 {
            score += 25;
        }

        score
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

    fn best_start_with_statistics(
        &self,
        pattern: &PatternChain,
        stats: &dyn GraphStatsProvider,
    ) -> (usize, BranchOrder) {
        let mut best_idx = 0usize;
        let mut best_order = BranchOrder::RightFirst;
        let mut best_cost = f64::INFINITY;

        for start_idx in 0..pattern.vertices.len() {
            let scan_rows = self.estimate_scan_rows(&pattern.vertices[start_idx], stats);
            let left_factor = self.estimate_branch_factor(pattern, start_idx, true, stats);
            let right_factor = self.estimate_branch_factor(pattern, start_idx, false, stats);
            let total_cost = scan_rows * left_factor * right_factor;
            let branch_order = if left_factor <= right_factor {
                BranchOrder::LeftFirst
            } else {
                BranchOrder::RightFirst
            };

            if total_cost < best_cost
                || (f64::abs(total_cost - best_cost) < f64::EPSILON && start_idx < best_idx)
            {
                best_idx = start_idx;
                best_order = branch_order;
                best_cost = total_cost;
            }
        }

        (best_idx, best_order)
    }

    fn best_start_with_heuristic(&self, pattern: &PatternChain) -> (usize, BranchOrder) {
        let mut best_idx = 0usize;
        let mut best_score = i64::MIN;

        for (idx, vertex) in pattern.vertices.iter().enumerate() {
            let score = Self::score_vertex(vertex);
            if score > best_score {
                best_idx = idx;
                best_score = score;
            }
        }

        (best_idx, BranchOrder::LeftFirst)
    }

    fn estimate_scan_rows(
        &self,
        vertex: &BoundVertexVariable,
        stats: &dyn GraphStatsProvider,
    ) -> f64 {
        let base = stats
            .vertex_count(&vertex.vertex_table_info.label)
            .unwrap_or(1000)
            .max(1) as f64;
        if vertex.filter.is_none() {
            return base;
        }
        if Self::filter_references_key_column(vertex) {
            return 1.0;
        }
        (base * 0.10).max(1.0)
    }

    fn estimate_branch_factor(
        &self,
        pattern: &PatternChain,
        start_idx: usize,
        left_branch: bool,
        stats: &dyn GraphStatsProvider,
    ) -> f64 {
        let mut factor = 1.0f64;
        let steps = if left_branch {
            Self::left_branch_steps(pattern, start_idx)
        } else {
            Self::right_branch_steps(pattern, start_idx)
        };
        for (edge, source, target) in steps {
            factor *= Self::estimate_expand_factor(
                stats,
                &source.vertex_table_info.label,
                &edge.edge_table_info.label,
                &target.vertex_table_info.label,
            );
        }
        factor
    }

    fn estimate_expand_factor(
        stats: &dyn GraphStatsProvider,
        source_label: &str,
        edge_label: &str,
        target_label: &str,
    ) -> f64 {
        let source_count = stats.vertex_count(source_label).unwrap_or(1).max(1) as f64;

        if let Some(step_count) = stats.pattern_step_count(source_label, edge_label, target_label) {
            return (step_count as f64 / source_count).max(1.0 / source_count);
        }
        if let Some(reverse_count) =
            stats.pattern_step_count(target_label, edge_label, source_label)
        {
            return (reverse_count as f64 / source_count).max(1.0 / source_count);
        }
        stats
            .avg_degree(source_label)
            .unwrap_or(1.0)
            .max(1.0 / source_count)
    }

    fn reorder_pattern(
        pattern: &PatternChain,
        start_idx: usize,
        branch_order: BranchOrder,
    ) -> Vec<BoundPatternElement> {
        let mut reordered = vec![BoundPatternElement::Vertex(
            pattern.vertices[start_idx].clone(),
        )];

        let left_steps = Self::left_branch_steps(pattern, start_idx)
            .into_iter()
            .map(|(edge, _source, target)| (edge, target))
            .collect::<Vec<_>>();
        let right_steps = Self::right_branch_steps(pattern, start_idx)
            .into_iter()
            .map(|(edge, _source, target)| (edge, target))
            .collect::<Vec<_>>();

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
    ) -> Vec<(BoundEdgeVariable, BoundVertexVariable, BoundVertexVariable)> {
        let mut steps = Vec::new();
        for edge_idx in (0..start_idx).rev() {
            let mut edge = pattern.edges[edge_idx].clone();
            Self::flip_edge(&mut edge);
            steps.push((
                edge,
                pattern.vertices[edge_idx + 1].clone(),
                pattern.vertices[edge_idx].clone(),
            ));
        }
        steps
    }

    fn right_branch_steps(
        pattern: &PatternChain,
        start_idx: usize,
    ) -> Vec<(BoundEdgeVariable, BoundVertexVariable, BoundVertexVariable)> {
        let mut steps = Vec::new();
        for edge_idx in start_idx..pattern.edges.len() {
            steps.push((
                pattern.edges[edge_idx].clone(),
                pattern.vertices[edge_idx].clone(),
                pattern.vertices[edge_idx + 1].clone(),
            ));
        }
        steps
    }

    /// Check if the vertex's filter references one of its key columns.
    fn filter_references_key_column(v: &BoundVertexVariable) -> bool {
        if let Some(ref _filter) = v.filter {
            // The key columns are the primary key of the vertex table.
            // If the filter exists, it likely references indexed columns.
            // defined, assume the filter may reference them.
            // A more precise check would walk the expression tree to find ColumnRef
            // expressions matching key_column_ids, but that requires expression
            // introspection utilities not yet available.
            !v.vertex_table_info.key_column_ids.is_empty()
        } else {
            false
        }
    }

    /// Flip an edge's direction and swap its source/destination variables.
    fn flip_edge(edge: &mut BoundEdgeVariable) {
        edge.direction = match edge.direction {
            EdgeDirection::Right => EdgeDirection::Left,
            EdgeDirection::Left => EdgeDirection::Right,
            // Undirected and LeftRight are symmetric — no change needed.
            EdgeDirection::Undirected => EdgeDirection::Undirected,
            EdgeDirection::LeftRight => EdgeDirection::LeftRight,
        };
        std::mem::swap(&mut edge.source_variable, &mut edge.destination_variable);
    }
}

impl Default for GraphStartSelection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::{
        CreatePropertyGraphInfo, EdgeTableInfo, PropertyGraphCatalogEntry, VertexTableInfo,
    };
    use paro_common::types::LogicalType;
    use paro_context::{test_support::TestStatementContextBuilder, StatementContext};
    use paro_parser::ast::EdgeDirection;
    use paro_planner::binder::bind::graph::{
        BoundEdgeVariable, BoundPatternElement, BoundVertexVariable,
    };
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::{BoundGraphColumn, BoundGraphPattern};
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    };
    use paro_planner::operator::ColumnBinding;
    use paro_planner::operator::{GraphMatch, LogicalOperator};
    use paro_storage::index::graph::GraphStatistics;
    use std::sync::Arc;

    use crate::context::{GraphStatsCache, GraphStatsLoader, OptimizationContext};

    fn make_test_session() -> Arc<StatementContext> {
        TestStatementContextBuilder::minimal().build()
    }

    struct MockGraphStatsLoader {
        stats: Option<Arc<GraphStatistics>>,
    }

    impl GraphStatsLoader for MockGraphStatsLoader {
        fn load(&self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
            (graph_name == "test_graph")
                .then(|| self.stats.clone())
                .flatten()
        }
    }

    fn make_vertex(
        name: &str,
        label: &str,
        table_index: usize,
        filter: Option<Expression>,
    ) -> BoundVertexVariable {
        BoundVertexVariable {
            variable_name: name.to_string(),
            vertex_table_info: VertexTableInfo {
                table_name: label.to_string(),
                table_oid: 0,
                key_column_ids: vec![0],
                label: label.to_string(),
                property_column_ids: vec![0, 1],
            },
            table_index,
            column_bindings: vec![
                ColumnBinding::new(table_index, 0),
                ColumnBinding::new(table_index, 1),
            ],
            column_names: vec!["id".to_string(), "name".to_string()],
            filter,
        }
    }

    fn make_edge(
        name: &str,
        label: &str,
        table_index: usize,
        direction: EdgeDirection,
        src_var: &str,
        dst_var: &str,
    ) -> BoundEdgeVariable {
        BoundEdgeVariable {
            variable_name: name.to_string(),
            edge_table_info: EdgeTableInfo {
                table_name: label.to_string(),
                table_oid: 0,
                key_column_ids: vec![],
                source_key_column_ids: vec![0],
                source_vertex_table: "person".to_string(),
                source_ref_column_ids: vec![0],
                destination_key_column_ids: vec![1],
                destination_vertex_table: "person".to_string(),
                destination_ref_column_ids: vec![0],
                label: label.to_string(),
                property_column_ids: vec![0],
            },
            table_index,
            column_bindings: vec![ColumnBinding::new(table_index, 0)],
            column_names: vec!["since".to_string()],
            direction,
            quantifier: None,
            filter: None,
            source_variable: src_var.to_string(),
            destination_variable: dst_var.to_string(),
        }
    }

    fn make_column(table_index: usize, col_index: usize) -> BoundGraphColumn {
        BoundGraphColumn {
            expr: Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, col_index),
                LogicalType::Varchar,
            )),
            alias: format!("col_{}_{}", table_index, col_index),
            logical_type: LogicalType::Varchar,
        }
    }

    fn make_graph_match(
        elements: Vec<BoundPatternElement>,
        columns: Vec<BoundGraphColumn>,
        table_index: usize,
    ) -> LogicalOperator {
        let output_types = columns.iter().map(|c| c.logical_type.clone()).collect();
        LogicalOperator::GraphMatch(GraphMatch::new(
            Arc::new(PropertyGraphCatalogEntry::new(
                CreatePropertyGraphInfo::new(
                    "test".to_string(),
                    "main".to_string(),
                    "test_graph".to_string(),
                ),
                0,
                "test".to_string(),
                paro_catalog::entry::CatalogObjectId::from_raw(10_001),
            )),
            BoundGraphPattern { elements },
            columns,
            table_index,
            output_types,
            None,
            false,
        ))
    }

    fn optimize(plan: LogicalOperator) -> LogicalOperator {
        optimize_with_stats(plan, None)
    }

    fn optimize_with_stats(
        plan: LogicalOperator,
        stats: Option<GraphStatistics>,
    ) -> LogicalOperator {
        let session = make_test_session();
        let bind_context = BindContext::new();
        let mut ctx = OptimizationContext::new(session, bind_context.clone());
        if let Some(stats) = stats {
            ctx.graph_stats = GraphStatsCache::with_loader(Arc::new(MockGraphStatsLoader {
                stats: Some(Arc::new(stats)),
            }));
        }
        GraphStartSelection::new()
            .optimize(LogicalPlan::new(&bind_context, plan), &mut ctx)
            .expect("optimize graph start selection")
            .operator
    }

    fn make_simple_filter() -> Expression {
        // name = 'Alice'
        Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(0, 1),
                LogicalType::Varchar,
            )),
            Expression::Constant(ConstantExpression::new(
                paro_common::runtime_value::Value::Varchar("Alice".to_string()),
                LogicalType::Varchar,
            )),
        ))
    }

    // --- Tests ---

    #[test]
    fn test_no_reorder_when_first_vertex_has_filter() {
        // (a:Person WHERE name='Alice') -[k:Knows]-> (b:Person)
        // a has filter, b doesn't → a stays as start
        let v_a = make_vertex("a", "Person", 10, Some(make_simple_filter()));
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12, None);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1), make_column(12, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        // Should NOT reverse — a is already the best start
        if let LogicalOperator::GraphMatch(gm) = &result {
            if let BoundPatternElement::Vertex(v) = &gm.bound_pattern.elements[0] {
                assert_eq!(v.variable_name, "a");
            } else {
                panic!("expected vertex");
            }
            if let BoundPatternElement::Edge(e) = &gm.bound_pattern.elements[1] {
                assert_eq!(e.direction, EdgeDirection::Right);
            } else {
                panic!("expected edge");
            }
        } else {
            panic!("expected GraphMatch");
        }
    }

    #[test]
    fn test_reverse_when_last_vertex_has_filter() {
        // (a:Person) -[k:Knows]-> (b:Person WHERE name='Alice')
        // b has filter, a doesn't → reverse to start from b
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12, Some(make_simple_filter()));

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1), make_column(12, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        // Should reverse: [V(b), E(k_flipped), V(a)]
        if let LogicalOperator::GraphMatch(gm) = &result {
            let elems = &gm.bound_pattern.elements;
            assert_eq!(elems.len(), 3);

            if let BoundPatternElement::Vertex(v) = &elems[0] {
                assert_eq!(v.variable_name, "b", "b should now be first");
            } else {
                panic!("expected vertex");
            }
            if let BoundPatternElement::Edge(e) = &elems[1] {
                assert_eq!(
                    e.direction,
                    EdgeDirection::Left,
                    "Right should flip to Left"
                );
                assert_eq!(e.source_variable, "b", "source/dest should be swapped");
                assert_eq!(e.destination_variable, "a");
            } else {
                panic!("expected edge");
            }
            if let BoundPatternElement::Vertex(v) = &elems[2] {
                assert_eq!(v.variable_name, "a", "a should now be last");
            } else {
                panic!("expected vertex");
            }
        } else {
            panic!("expected GraphMatch");
        }
    }

    #[test]
    fn test_reverse_two_hop_chain() {
        // (a:Person) -[k1]-> (b:Person) -[k2]-> (c:Person WHERE name='Alice')
        // c has filter → reverse entire chain
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k1 = make_edge("k1", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12, None);
        let e_k2 = make_edge("k2", "Knows", 13, EdgeDirection::Right, "b", "c");
        let v_c = make_vertex("c", "Person", 14, Some(make_simple_filter()));

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k1),
            BoundPatternElement::Vertex(v_b),
            BoundPatternElement::Edge(e_k2),
            BoundPatternElement::Vertex(v_c),
        ];
        let columns = vec![make_column(10, 1), make_column(14, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        // Reversed: [V(c), E(k2_flipped), V(b), E(k1_flipped), V(a)]
        if let LogicalOperator::GraphMatch(gm) = &result {
            let elems = &gm.bound_pattern.elements;
            assert_eq!(elems.len(), 5);

            if let BoundPatternElement::Vertex(v) = &elems[0] {
                assert_eq!(v.variable_name, "c");
            }
            if let BoundPatternElement::Edge(e) = &elems[1] {
                assert_eq!(e.variable_name, "k2");
                assert_eq!(e.direction, EdgeDirection::Left);
            }
            if let BoundPatternElement::Vertex(v) = &elems[2] {
                assert_eq!(v.variable_name, "b");
            }
            if let BoundPatternElement::Edge(e) = &elems[3] {
                assert_eq!(e.variable_name, "k1");
                assert_eq!(e.direction, EdgeDirection::Left);
            }
            if let BoundPatternElement::Vertex(v) = &elems[4] {
                assert_eq!(v.variable_name, "a");
            }
        } else {
            panic!("expected GraphMatch");
        }
    }

    #[test]
    fn test_no_reorder_when_scores_tied() {
        // Both vertices have no filter → tied at 0, keep original order
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12, None);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        if let LogicalOperator::GraphMatch(gm) = &result {
            if let BoundPatternElement::Vertex(v) = &gm.bound_pattern.elements[0] {
                assert_eq!(v.variable_name, "a", "should keep original order on tie");
            }
        }
    }

    #[test]
    fn test_path_functions_keep_original_pattern_order() {
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k1 = make_edge("k1", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12, Some(make_simple_filter()));
        let e_k2 = make_edge("k2", "Knows", 13, EdgeDirection::Right, "b", "c");
        let v_c = make_vertex("c", "Person", 14, None);

        let columns = vec![make_column(10, 1)];
        let output_types = columns.iter().map(|c| c.logical_type.clone()).collect();
        let plan = LogicalOperator::GraphMatch(GraphMatch::new(
            Arc::new(PropertyGraphCatalogEntry::new(
                CreatePropertyGraphInfo::new(
                    "test".to_string(),
                    "main".to_string(),
                    "test_graph".to_string(),
                ),
                0,
                "test".to_string(),
                paro_catalog::entry::CatalogObjectId::from_raw(10_002),
            )),
            BoundGraphPattern {
                elements: vec![
                    BoundPatternElement::Vertex(v_a),
                    BoundPatternElement::Edge(e_k1),
                    BoundPatternElement::Vertex(v_b),
                    BoundPatternElement::Edge(e_k2),
                    BoundPatternElement::Vertex(v_c),
                ],
            },
            columns,
            100,
            output_types,
            None,
            true,
        ));

        let result = optimize(plan);

        if let LogicalOperator::GraphMatch(gm) = &result {
            if let BoundPatternElement::Vertex(v) = &gm.bound_pattern.elements[0] {
                assert_eq!(v.variable_name, "a");
            } else {
                panic!("expected vertex");
            }
            if let BoundPatternElement::Vertex(v) = &gm.bound_pattern.elements[2] {
                assert_eq!(v.variable_name, "b");
            } else {
                panic!("expected vertex");
            }
        } else {
            panic!("expected GraphMatch");
        }
    }

    #[test]
    fn test_undirected_edge_stays_undirected_after_flip() {
        // (a:Person) -[k:Knows]- (b:Person WHERE name='Alice')
        // Undirected edge should stay undirected after reversal
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Undirected, "a", "b");
        let v_b = make_vertex("b", "Person", 12, Some(make_simple_filter()));

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        if let LogicalOperator::GraphMatch(gm) = &result {
            if let BoundPatternElement::Edge(e) = &gm.bound_pattern.elements[1] {
                assert_eq!(
                    e.direction,
                    EdgeDirection::Undirected,
                    "undirected should stay undirected"
                );
            }
        }
    }

    #[test]
    fn test_single_vertex_no_change() {
        // Single vertex pattern — nothing to reorder
        let v_a = make_vertex("a", "Person", 10, None);
        let elements = vec![BoundPatternElement::Vertex(v_a)];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        if let LogicalOperator::GraphMatch(gm) = &result {
            assert_eq!(gm.bound_pattern.elements.len(), 1);
        }
    }

    #[test]
    fn test_nested_in_filter() {
        // GraphMatch nested under a Filter should still be optimized
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12, Some(make_simple_filter()));

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1)];
        let graph_match = make_graph_match(elements, columns, 100);

        let plan = LogicalOperator::Filter(paro_planner::operator::Filter::new(
            LogicalPlan::synthetic(graph_match),
            vec![],
        ));

        let result = optimize(plan);

        // Should still reverse inside the Filter
        if let LogicalOperator::Filter(f) = &result {
            if let LogicalOperator::GraphMatch(gm) = &f.child.operator {
                if let BoundPatternElement::Vertex(v) = &gm.bound_pattern.elements[0] {
                    assert_eq!(v.variable_name, "b", "b should be first after reversal");
                }
            } else {
                panic!("expected GraphMatch inside Filter");
            }
        } else {
            panic!("expected Filter");
        }
    }

    #[test]
    fn test_backward_edge_flips_to_right() {
        // (a:Person) <-[k:Knows]- (b:Person WHERE name='Alice')
        // After reversal: (b) -[k_flipped]-> (a), Left flips to Right
        let v_a = make_vertex("a", "Person", 10, None);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Left, "a", "b");
        let v_b = make_vertex("b", "Person", 12, Some(make_simple_filter()));

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let result = optimize(plan);

        if let LogicalOperator::GraphMatch(gm) = &result {
            if let BoundPatternElement::Edge(e) = &gm.bound_pattern.elements[1] {
                assert_eq!(
                    e.direction,
                    EdgeDirection::Right,
                    "Left should flip to Right"
                );
            }
        }
    }

    #[test]
    fn test_statistics_choose_middle_vertex() {
        let v_a = make_vertex("a", "Person", 10, None);
        let e_ab = make_edge("ab", "WorksAt", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Company", 12, Some(make_simple_filter()));
        let e_bc = make_edge("bc", "LocatedIn", 13, EdgeDirection::Right, "b", "c");
        let v_c = make_vertex("c", "City", 14, None);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_ab),
            BoundPatternElement::Vertex(v_b),
            BoundPatternElement::Edge(e_bc),
            BoundPatternElement::Vertex(v_c),
        ];
        let columns = vec![make_column(10, 1), make_column(12, 1), make_column(14, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let stats = GraphStatistics::default()
            .with_vertex_count("Person", 1_000)
            .with_vertex_count("Company", 10)
            .with_vertex_count("City", 100)
            .with_pattern_step_count("Person", "WorksAt", "Company", 1_000)
            .with_pattern_step_count("Company", "LocatedIn", "City", 10);

        let result = optimize_with_stats(plan, Some(stats));

        if let LogicalOperator::GraphMatch(gm) = &result {
            let elems = &gm.bound_pattern.elements;
            if let BoundPatternElement::Vertex(v) = &elems[0] {
                assert_eq!(v.variable_name, "b");
            } else {
                panic!("expected vertex");
            }
            if let BoundPatternElement::Edge(e) = &elems[1] {
                assert_eq!(e.variable_name, "bc");
                assert_eq!(e.direction, EdgeDirection::Right);
                assert_eq!(e.source_variable, "b");
                assert_eq!(e.destination_variable, "c");
            } else {
                panic!("expected edge");
            }
            if let BoundPatternElement::Edge(e) = &elems[3] {
                assert_eq!(e.variable_name, "ab");
                assert_eq!(e.direction, EdgeDirection::Left);
                assert_eq!(e.source_variable, "b");
                assert_eq!(e.destination_variable, "a");
            } else {
                panic!("expected edge");
            }
        } else {
            panic!("expected GraphMatch");
        }
    }
}
