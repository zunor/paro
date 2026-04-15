//! Graph Match Decompose Optimizer
//!
//! Decomposes `GraphMatch` into a chain of `GraphScan` +
//! `GraphExpand` operators, topped by a `Projection` for COLUMNS.
//!
//! ## Algorithm
//!
//! 1. Extract the first vertex from the pattern chain → `GraphScan` (start node)
//! 2. For each subsequent (edge, vertex) pair → `GraphExpand`
//! 3. Wrap with `Projection` for the COLUMNS expressions

use paro_parser::ast::EdgeDirection;
use paro_planner::binder::bind::graph::BoundPatternElement;
use paro_planner::operator::{
    ExpandDirection, GraphExpand, GraphMatch, GraphScan, LogicalOperator, Projection,
};
use paro_planner::plan::LogicalPlan;
use std::collections::HashMap;

/// Decomposes `GraphMatch` into Scan + Expand chain + Projection.
pub struct GraphMatchDecompose;

impl GraphMatchDecompose {
    pub fn new() -> Self {
        Self
    }

    /// Optimize the plan by recursively decomposing any `GraphMatch` nodes.
    #[cfg(test)]
    pub(crate) fn optimize(&mut self, plan: LogicalOperator) -> LogicalOperator {
        self.optimize_plan(LogicalPlan::synthetic(plan)).operator
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.rewrite_plan(plan)
    }

    fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.rewrite_plan(child));
        let operator = match plan.operator {
            LogicalOperator::GraphMatch(gm) => self.decompose(gm),
            other => other,
        };
        LogicalPlan {
            id: plan.id,
            stats: plan.stats,
            operator,
        }
    }

    /// Decompose a `GraphMatch` into Scan + Expand chain + Projection.
    ///
    /// Pattern chain: [V(a), E(k1), V(b), E(k2), V(c),...]
    ///
    /// Result:
    /// ```text
    /// Projection (COLUMNS)
    ///   └─ GraphExpand (k2, target=c)
    ///       └─ GraphExpand (k1, target=b)
    ///           └─ GraphScan (a)
    /// ```
    fn decompose(&self, gm: GraphMatch) -> LogicalOperator {
        let elements = gm.bound_pattern.elements;
        let columns = gm.columns;
        let table_index = gm.table_index;
        let graph_name = gm.graph_entry.info.graph_name.clone();
        let schema_name = gm.graph_entry.info.schema.clone();
        let path_mode = gm.path_mode.clone();

        // The pattern chain alternates: Vertex, Edge, Vertex, Edge, Vertex,...
        // First vertex becomes the root `GraphScan`.
        let mut iter = elements.into_iter().peekable();
        let first = iter.next().expect("pattern chain must not be empty");
        let first_vertex = match first {
            BoundPatternElement::Vertex(v) => v,
            BoundPatternElement::Edge(_) => {
                panic!("pattern chain must start with a vertex")
            }
        };

        let mut current = LogicalPlan::synthetic(LogicalOperator::GraphScan(GraphScan::new(
            first_vertex.vertex_table_info.clone(),
            first_vertex.filter.clone(),
            first_vertex.table_index,
            first_vertex.vertex_table_info.label.clone(),
            graph_name,
            schema_name,
        )));
        let mut bound_vertices = HashMap::new();
        bound_vertices.insert(first_vertex.variable_name.clone(), first_vertex.clone());

        // Each remaining (edge, vertex) extends the chain with `GraphExpand`.
        while let Some(edge_elem) = iter.next() {
            let edge = match edge_elem {
                BoundPatternElement::Edge(e) => e,
                BoundPatternElement::Vertex(_) => {
                    panic!("expected edge after vertex in pattern chain")
                }
            };

            let target_elem = iter
                .next()
                .expect("pattern chain must have a vertex after each edge");
            let target_vertex = match target_elem {
                BoundPatternElement::Vertex(v) => v,
                BoundPatternElement::Edge(_) => {
                    panic!("expected vertex after edge in pattern chain")
                }
            };

            let direction = match edge.direction {
                EdgeDirection::Right => ExpandDirection::Forward,
                EdgeDirection::Left => ExpandDirection::Backward,
                EdgeDirection::Undirected | EdgeDirection::LeftRight => ExpandDirection::Both,
            };

            let source_vertex = bound_vertices
                .get(&edge.source_variable)
                .or_else(|| bound_vertices.get(&edge.destination_variable))
                .unwrap_or_else(|| {
                    panic!(
                        "one endpoint of edge '{}' must already be bound before expand (source='{}', destination='{}')",
                        edge.variable_name, edge.source_variable, edge.destination_variable
                    )
                });
            let source_table_index = source_vertex.table_index;

            let is_terminal_expand = iter.peek().is_none();
            let mut expand = GraphExpand::new(
                edge.edge_table_info.clone(),
                direction,
                source_vertex.vertex_table_info.label.clone(),
                source_table_index,
                edge.table_index,
                target_vertex.table_index,
                target_vertex.vertex_table_info.label.clone(),
                source_vertex.vertex_table_info.table_oid,
                target_vertex.vertex_table_info.table_oid,
                target_vertex.vertex_table_info.table_name.clone(),
                current,
            );
            expand.edge_filter = edge.filter.clone();
            expand.target_filter = target_vertex.filter.clone();
            expand.quantifier = edge.quantifier.clone();
            expand.path_mode = path_mode.clone();
            expand.has_path_functions = gm.has_path_functions && is_terminal_expand;

            current = LogicalPlan::synthetic(LogicalOperator::GraphExpand(expand));
            bound_vertices.insert(target_vertex.variable_name.clone(), target_vertex);
        }

        // Output columns are applied in a final `Projection`.
        let expressions: Vec<_> = columns.iter().map(|c| c.expr.clone()).collect();
        let projection = Projection::new(table_index, current, expressions);
        LogicalOperator::Projection(projection)
    }
}

impl Default for GraphMatchDecompose {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_catalog::entry::{EdgeTableInfo, VertexTableInfo};
    use paro_common::types::LogicalType;
    use paro_parser::ast::EdgeDirection;
    use paro_planner::binder::bind::graph::{
        BoundEdgeVariable, BoundPatternElement, BoundVertexVariable,
    };
    use paro_planner::binder::ir::{BoundGraphColumn, BoundGraphPattern};
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::operator::ColumnBinding;
    use paro_planner::operator::{GraphMatch, LogicalOperator, LogicalOperatorType};
    use std::sync::Arc;

    fn make_vertex(name: &str, label: &str, table_index: usize) -> BoundVertexVariable {
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
            filter: None,
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
            Arc::new(paro_catalog::entry::PropertyGraphCatalogEntry::new(
                paro_catalog::entry::CreatePropertyGraphInfo::new(
                    "test".to_string(),
                    "main".to_string(),
                    "test_graph".to_string(),
                ),
                0,
                "test".to_string(),
            )),
            BoundGraphPattern { elements },
            columns,
            table_index,
            output_types,
            None,
            false,
        ))
    }

    #[test]
    fn test_one_hop_decompose() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1), make_column(12, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        // Result should be: Projection → GraphExpand → GraphScan
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            assert_eq!(proj.table_index, 100);
            assert_eq!(proj.expressions.len(), 2);
            let child = proj.child.as_ref();
            assert_eq!(child.operator.op_type(), LogicalOperatorType::GraphExpand);
            if let LogicalOperator::GraphExpand(ge) = &child.operator {
                assert_eq!(ge.edge_info.label, "Knows");
                assert_eq!(ge.direction, ExpandDirection::Forward);
                assert_eq!(ge.source_label, "Person");
                assert_eq!(ge.source_table_index, 10);
                assert_eq!(ge.edge_table_index, 11);
                assert_eq!(ge.target_table_index, 12);
                assert_eq!(ge.target_label, "Person");
                assert_eq!(ge.child.operator.op_type(), LogicalOperatorType::GraphScan);
                if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                    assert_eq!(gs.table_index, 10);
                    assert_eq!(gs.label, "Person");
                }
            }
        }
    }

    #[test]
    fn test_two_hop_decompose() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k1 = make_edge("k1", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);
        let e_k2 = make_edge("k2", "Knows", 13, EdgeDirection::Right, "b", "c");
        let v_c = make_vertex("c", "Person", 14);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k1),
            BoundPatternElement::Vertex(v_b),
            BoundPatternElement::Edge(e_k2),
            BoundPatternElement::Vertex(v_c),
        ];
        let columns = vec![make_column(10, 1), make_column(12, 1), make_column(14, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        // Result: Projection → GraphExpand(k2) → GraphExpand(k1) → GraphScan(a)
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            assert_eq!(proj.expressions.len(), 3);
            if let LogicalOperator::GraphExpand(ge2) = &proj.child.operator {
                assert_eq!(ge2.edge_info.label, "Knows");
                assert_eq!(ge2.target_table_index, 14);
                assert_eq!(ge2.source_table_index, 12);
                if let LogicalOperator::GraphExpand(ge1) = &ge2.child.operator {
                    assert_eq!(ge1.edge_info.label, "Knows");
                    assert_eq!(ge1.target_table_index, 12);
                    assert_eq!(ge1.source_table_index, 10);
                    assert_eq!(ge2.source_table_index, 12);
                    assert_eq!(ge1.child.operator.op_type(), LogicalOperatorType::GraphScan);
                } else {
                    panic!("expected GraphExpand(k1)");
                }
            } else {
                panic!("expected GraphExpand(k2)");
            }
        }
    }

    #[test]
    fn test_backward_edge_decompose() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Left, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                assert_eq!(ge.direction, ExpandDirection::Backward);
                assert_eq!(ge.source_table_index, 10);
                assert_eq!(ge.target_table_index, 12);
            } else {
                panic!("expected GraphExpand");
            }
        }
    }

    #[test]
    fn test_undirected_edge_decompose() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Undirected, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                assert_eq!(ge.direction, ExpandDirection::Both);
            } else {
                panic!("expected GraphExpand");
            }
        }
    }

    #[test]
    fn test_nested_in_filter() {
        // GraphMatch nested under a Filter should still be decomposed
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

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

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        // Result: Filter → Projection → GraphExpand → GraphScan
        assert_eq!(result.op_type(), LogicalOperatorType::Filter);
        if let LogicalOperator::Filter(f) = &result {
            assert_eq!(f.child.operator.op_type(), LogicalOperatorType::Projection);
        }
    }

    #[test]
    fn test_middle_start_expand_uses_bound_source_vertex() {
        let v_b = make_vertex("b", "Person", 12);
        let e_k1 = make_edge("k1", "Knows", 11, EdgeDirection::Left, "b", "a");
        let v_a = make_vertex("a", "Person", 10);
        let e_k2 = make_edge("k2", "Knows", 13, EdgeDirection::Right, "b", "c");
        let v_c = make_vertex("c", "Person", 14);

        let elements = vec![
            BoundPatternElement::Vertex(v_b),
            BoundPatternElement::Edge(e_k1),
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k2),
            BoundPatternElement::Vertex(v_c),
        ];
        let columns = vec![make_column(10, 1), make_column(12, 1), make_column(14, 1)];
        let plan = make_graph_match(elements, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(right_expand) = &proj.child.operator {
                assert_eq!(right_expand.source_table_index, 12);
                assert_eq!(right_expand.target_table_index, 14);
                if let LogicalOperator::GraphExpand(left_expand) = &right_expand.child.operator {
                    assert_eq!(left_expand.source_table_index, 12);
                    assert_eq!(left_expand.target_table_index, 10);
                    assert_eq!(left_expand.direction, ExpandDirection::Backward);
                } else {
                    panic!("expected left branch expand");
                }
            } else {
                panic!("expected graph expand chain");
            }
        } else {
            panic!("expected projection");
        }
    }
}
