// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Graph Match Decompose Optimizer
//!
//! Decomposes `GraphMatch` into a chain of `GraphScan` +
//! `GraphExpand` operators, topped by a `Projection` for COLUMNS.
//!
//! ## Algorithm
//!
//! 1. Scan the first canonical vertex binding.
//! 2. Expand a connected edge frontier; close already-bound endpoints with
//!    identity constraints while retaining edge multiplicity.
//! 3. Wrap with `Projection` for the COLUMNS expressions

use paro_parser::ast::EdgeDirection;
use paro_planner::logical::operator::{
    ExpandDirection, GraphExpand, GraphMatch, GraphScan, LogicalOperator, Projection,
};
use paro_planner::logical::plan::OwnedLogicalPlan;
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
        self.optimize_plan(OwnedLogicalPlan::synthetic(plan))
            .into_operator()
    }

    pub fn optimize_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        self.rewrite_plan(plan)
    }

    fn rewrite_plan(&mut self, plan: OwnedLogicalPlan) -> OwnedLogicalPlan {
        plan.try_map_post_order(|plan| {
            let (id, stats, operator) = plan.into_parts();
            let operator = match operator {
                LogicalOperator::GraphMatch(gm) => self.decompose(*gm),
                other => other,
            };
            Ok(OwnedLogicalPlan {
                id,
                stats,
                operator,
            })
        })
        .expect("graph decomposition traversal cannot fail")
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
        let columns = gm.columns;
        let table_index = gm.table_index;
        let relation_alias = gm.relation_alias;
        let graph_name = gm.graph_entry.info.graph_name.clone();
        let schema_name = gm.graph_entry.info.schema.clone();
        let path_mode = gm.path_mode.clone();

        // Bindings identify vertices, while edges identify topology. Path syntax
        // order does not imply that the next expansion starts at the previous
        // destination (branches and cycles share already-bound vertices).
        let first_name = gm.bound_pattern.vertices[0].variable_name.clone();
        let vertices = gm
            .bound_pattern
            .vertices
            .into_iter()
            .map(|vertex| (vertex.variable_name.clone(), vertex))
            .collect::<HashMap<_, _>>();
        let mut edges = gm.bound_pattern.edges;
        let preserve_path_order = gm.has_path_functions
            || path_mode.is_some()
            || edges.iter().any(|edge| edge.quantifier.is_some());
        let mut path_vertex = first_name.clone();
        let first_vertex = &vertices[&first_name];
        let mut current =
            OwnedLogicalPlan::synthetic(LogicalOperator::GraphScan(Box::new(GraphScan::new(
                first_vertex.vertex_table_info.clone(),
                first_vertex.filter.clone(),
                first_vertex.table_index,
                table_index,
                first_vertex.vertex_table_info.label.clone(),
                graph_name,
                schema_name,
            ))));
        let mut bound_vertices = std::collections::HashSet::new();
        bound_vertices.insert(first_vertex.variable_name.clone());

        while !edges.is_empty() {
            // A closing edge constrains both bound endpoints and must preserve
            // each matching edge's multiplicity. Otherwise extend the frontier.
            let index = if preserve_path_order {
                0
            } else {
                edges
                    .iter()
                    .position(|edge| {
                        bound_vertices.contains(&edge.source_variable)
                            && bound_vertices.contains(&edge.destination_variable)
                    })
                    .or_else(|| {
                        edges.iter().position(|edge| {
                            bound_vertices.contains(&edge.source_variable)
                                || bound_vertices.contains(&edge.destination_variable)
                        })
                    })
                    .expect("binder verified connected graph pattern")
            };
            let edge = edges.remove(index);
            let from_source = if preserve_path_order {
                path_vertex == edge.source_variable
            } else {
                bound_vertices.contains(&edge.source_variable)
            };
            let (source_name, target_name) = if from_source {
                (&edge.source_variable, &edge.destination_variable)
            } else {
                (&edge.destination_variable, &edge.source_variable)
            };
            let source_vertex = &vertices[source_name];
            let target_vertex = &vertices[target_name];
            let direction = match edge.direction {
                EdgeDirection::Undirected | EdgeDirection::LeftRight
                    if source_vertex.vertex_table_info.label
                        == target_vertex.vertex_table_info.label =>
                {
                    ExpandDirection::Both
                }
                _ if from_source => ExpandDirection::Forward,
                _ => ExpandDirection::Backward,
            };
            let mut expand = GraphExpand::new(
                edge.edge_table_info.clone(),
                direction,
                source_vertex.vertex_table_info.label.clone(),
                source_vertex.table_index,
                edge.table_index,
                target_vertex.table_index,
                table_index,
                target_vertex.vertex_table_info.label.clone(),
                source_vertex.vertex_table_info.table_oid,
                target_vertex.vertex_table_info.table_oid,
                target_vertex.vertex_table_info.table_name.clone(),
                current,
            );
            expand.target_is_bound = bound_vertices.contains(target_name);
            expand.edge_filter = edge.filter.clone();
            expand.target_filter = if expand.target_is_bound {
                None
            } else {
                target_vertex.filter.clone()
            };
            expand.quantifier = edge.quantifier.clone();
            expand.path_mode = path_mode.clone();
            expand.has_path_functions = gm.has_path_functions && edges.is_empty();
            current = OwnedLogicalPlan::synthetic(LogicalOperator::GraphExpand(Box::new(expand)));
            bound_vertices.insert(target_name.clone());
            path_vertex = target_name.clone();
        }
        if let Some(predicate) = gm.bound_pattern.filter {
            let slot = match &mut current.operator {
                LogicalOperator::GraphExpand(expand) => &mut expand.target_filter,
                LogicalOperator::GraphScan(scan) => &mut scan.filter,
                _ => unreachable!(),
            };
            *slot = Some(match slot.take() {
                Some(previous) => paro_planner::expression::Expression::Conjunction(
                    paro_planner::expression::ConjunctionExpression::new(
                        paro_planner::expression::ConjunctionType::And,
                        vec![previous, predicate],
                    )
                    .into(),
                ),
                None => predicate,
            });
        }

        // Output columns are applied in a final `Projection`.
        let expressions: Vec<_> = columns.iter().map(|c| c.expr.clone()).collect();
        let visible_names = columns.iter().map(|column| column.alias.clone()).collect();
        let projection = Projection::new(table_index, current, expressions)
            .with_visible_names(visible_names)
            .with_visible_qualifier(relation_alias);
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
    use paro_planner::binder::bind::graph::{BoundEdgeVariable, BoundVertexVariable};
    use paro_planner::binder::context::BindContext;
    use paro_planner::binder::ir::{BoundGraphColumn, BoundGraphPattern};
    use paro_planner::expression::{ColumnRefExpression, Expression};
    use paro_planner::logical::operator::ColumnBinding;
    use paro_planner::logical::operator::{GraphMatch, LogicalOperator, LogicalOperatorType};
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
            expr: Expression::ColumnRef(
                ColumnRefExpression::new(
                    ColumnBinding::new(table_index, col_index),
                    LogicalType::Varchar,
                )
                .into(),
            ),
            alias: format!("col_{}_{}", table_index, col_index),
            logical_type: LogicalType::Varchar,
        }
    }

    fn make_graph_match(
        vertices: Vec<BoundVertexVariable>,
        edges: Vec<BoundEdgeVariable>,
        columns: Vec<BoundGraphColumn>,
        table_index: usize,
    ) -> LogicalOperator {
        let output_types = columns.iter().map(|c| c.logical_type.clone()).collect();
        LogicalOperator::GraphMatch(Box::new(GraphMatch::new(
            Arc::new(paro_catalog::entry::PropertyGraphCatalogEntry::new(
                paro_catalog::entry::CreatePropertyGraphInfo::new(
                    "test".to_string(),
                    "main".to_string(),
                    "test_graph".to_string(),
                ),
                0,
                "test".to_string(),
                paro_catalog::entry::CatalogObjectId::from_raw(10_001),
            )),
            BoundGraphPattern {
                vertices,
                edges,
                filter: None,
            },
            columns,
            table_index,
            "graph_table".to_string(),
            output_types,
            None,
            false,
        )))
    }

    #[test]
    fn path_payload_keeps_syntax_order_and_reverse_closing_direction() {
        // A later self-loop is already bound at the root. It must not jump in
        // front of the path, and a reverse closing edge must start at b.
        let mut plan = make_graph_match(
            vec![
                make_vertex("a", "Person", 10),
                make_vertex("b", "Person", 12),
            ],
            vec![
                make_edge("first", "Knows", 11, EdgeDirection::Right, "a", "b"),
                make_edge("back", "Knows", 13, EdgeDirection::Left, "a", "b"),
                make_edge("loop", "Knows", 14, EdgeDirection::Right, "a", "a"),
            ],
            vec![make_column(10, 0)],
            100,
        );
        if let LogicalOperator::GraphMatch(gm) = &mut plan {
            gm.has_path_functions = true;
        }
        let result = GraphMatchDecompose::new().optimize(plan);
        let LogicalOperator::Projection(projection) = result else {
            panic!("projection");
        };
        let LogicalOperator::GraphExpand(last) = &projection.child.operator else {
            panic!("last expand");
        };
        assert_eq!(last.edge_table_index, 14);
        assert!(last.has_path_functions);
        let LogicalOperator::GraphExpand(back) = &last.child.operator else {
            panic!("closing expand");
        };
        assert_eq!(back.edge_table_index, 13);
        assert_eq!(back.source_table_index, 12);
        assert_eq!(back.direction, ExpandDirection::Backward);
        assert!(back.target_is_bound);
        let LogicalOperator::GraphExpand(first) = &back.child.operator else {
            panic!("first expand");
        };
        assert_eq!(first.edge_table_index, 11);
    }

    #[test]
    fn test_one_hop_decompose() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let vertices = vec![v_a, v_b];
        let edges = vec![e_k];
        let columns = vec![make_column(10, 1), make_column(12, 1)];
        let plan = make_graph_match(vertices, edges, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        crate::verify::verify_logical_plan(
            &BindContext::new(),
            &OwnedLogicalPlan::synthetic(result),
        )
        .expect("decomposed graph projection should satisfy logical invariants");
    }

    #[test]
    fn test_graph_projection_verifier_rejects_unknown_relation() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);
        let vertices = vec![v_a, v_b];
        let edges = vec![e_k];
        let plan = make_graph_match(vertices, edges, vec![make_column(99, 1)], 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = OwnedLogicalPlan::synthetic(decompose.optimize(plan));
        let error = crate::verify::verify_logical_plan(&BindContext::new(), &result)
            .expect_err("unknown graph relation must not pass verification");

        assert!(error.to_string().contains("unavailable binding"));
    }

    #[test]
    fn test_one_hop_decompose_shape() {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let vertices = vec![v_a, v_b];
        let edges = vec![e_k];
        let columns = vec![make_column(10, 1), make_column(12, 1)];
        let plan = make_graph_match(vertices, edges, columns, 100);

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

        let vertices = vec![v_a, v_b, v_c];
        let edges = vec![e_k1, e_k2];
        let columns = vec![make_column(10, 1), make_column(12, 1), make_column(14, 1)];
        let plan = make_graph_match(vertices, edges, columns, 100);

        let mut decompose = GraphMatchDecompose::new();
        let result = decompose.optimize(plan);

        // Result: Projection → GraphExpand(k2) → GraphExpand(k1) → GraphScan(a)
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            assert_eq!(proj.expressions.len(), 3);
            assert_eq!(proj.child.types().len(), 8);
            let carrier_bindings = proj.child.get_column_bindings();
            assert_eq!(carrier_bindings.len(), 8);
            assert!(carrier_bindings
                .iter()
                .all(|binding| binding.table_index == 100));
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
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Left, "b", "a");
        let v_b = make_vertex("b", "Person", 12);

        let vertices = vec![v_a, v_b];
        let edges = vec![e_k];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(vertices, edges, columns, 100);

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

        let vertices = vec![v_a, v_b];
        let edges = vec![e_k];
        let columns = vec![make_column(10, 1)];
        let plan = make_graph_match(vertices, edges, columns, 100);

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

        let vertices = vec![v_a, v_b];
        let edges = vec![e_k];
        let columns = vec![make_column(10, 1)];
        let graph_match = make_graph_match(vertices, edges, columns, 100);

        let plan = LogicalOperator::Filter(paro_planner::logical::operator::Filter::new(
            OwnedLogicalPlan::synthetic(graph_match),
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
        let e_k1 = make_edge("k1", "Knows", 11, EdgeDirection::Left, "a", "b");
        let v_a = make_vertex("a", "Person", 10);
        let e_k2 = make_edge("k2", "Knows", 13, EdgeDirection::Right, "b", "c");
        let v_c = make_vertex("c", "Person", 14);

        let vertices = vec![v_b, v_a, v_c];
        let edges = vec![e_k1, e_k2];
        let columns = vec![make_column(10, 1), make_column(12, 1), make_column(14, 1)];
        let plan = make_graph_match(vertices, edges, columns, 100);

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
