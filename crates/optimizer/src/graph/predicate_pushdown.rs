//! Graph Predicate Pushdown Optimizer
//!
//! Pushes filter predicates from outer `Filter` nodes down into
//! `GraphScan` and `GraphExpand` operators.
//!
//! ## Motivation
//!
//! The general `FilterPushdown` pass explicitly skips graph projection chains
//! (because late materialization means the Projection's column bindings don't
//! map to physical columns in the graph chain). This pass understands the
//! graph operator semantics and can safely push predicates into the right
//! graph operator.
//!
//! ## Algorithm
//!
//! 1. Find `Filter → Projection(graph chain)` patterns
//! 2. Split the filter's predicates by AND
//! 3. Remap each predicate through the Projection (replace GRAPH_TABLE output
//!    column refs with the underlying COLUMNS expressions)
//! 4. Extract table_index references from the remapped predicate
//! 5. Walk the graph chain to find the deepest operator that owns all
//!    referenced table indices:
//!    - GraphScan owns `table_index` (the scan vertex)
//!    - GraphExpand owns `edge_table_index` and `target_table_index`
//! 6. Push the predicate into the operator's filter slot, or keep it above
//!    if it spans multiple operators

use std::collections::HashSet;

use paro_planner::expression::{
    ColumnRefExpression, ConjunctionExpression, ConjunctionType, Expression,
};
use paro_planner::operator::{Filter, LogicalOperator, Projection};
use paro_planner::plan::LogicalPlan;

/// Pushes predicates from outer WHERE into graph scan/expand operators.
pub struct GraphPredicatePushdown;

impl GraphPredicatePushdown {
    pub fn new() -> Self {
        Self
    }

    #[cfg(test)]
    fn optimize(&mut self, plan: LogicalOperator) -> LogicalOperator {
        self.optimize_plan(LogicalPlan::synthetic(plan)).operator
    }

    pub fn optimize_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        self.rewrite_plan(plan)
    }

    fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.rewrite_plan(child));
        plan.map_operator(|operator| self.rewrite_operator(operator))
    }

    fn rewrite_operator(&mut self, plan: LogicalOperator) -> LogicalOperator {
        match plan {
            LogicalOperator::Filter(f) => self.try_pushdown_filter(f),
            other => other,
        }
    }

    /// Try to push predicates from a Filter into graph operators.
    fn try_pushdown_filter(&mut self, filter: Filter) -> LogicalOperator {
        // Case 1: Filter → Projection(graph chain)
        if let LogicalOperator::Projection(ref proj) = filter.child.operator {
            if proj.child.is_graph_chain() {
                return self.pushdown_through_graph_projection(filter);
            }
        }

        // Case 2: Filter → graph chain directly (no Projection)
        if filter.child.is_graph_chain() {
            return self.pushdown_into_graph_chain(filter);
        }

        // Not a graph filter — recurse into child
        LogicalOperator::Filter(filter)
    }

    /// Push predicates from `Filter → Projection(graph chain)`.
    ///
    /// Remaps predicates through the Projection, then pushes into graph operators.
    fn pushdown_through_graph_projection(&mut self, filter: Filter) -> LogicalOperator {
        let proj = match filter.child.operator {
            LogicalOperator::Projection(p) => p,
            _ => unreachable!(),
        };

        // Split all filter predicates by AND
        let mut all_preds = Vec::new();
        for expr in filter.expressions {
            Self::split_and_predicates(expr, &mut all_preds);
        }

        // Remap each predicate through the Projection
        let mut remapped = Vec::new();
        let mut remaining = Vec::new();
        for pred in all_preds {
            match Self::remap_through_projection(&proj, pred.clone()) {
                Some(remapped_pred) => remapped.push(remapped_pred),
                None => remaining.push(pred),
            }
        }

        // Push remapped predicates into the graph chain
        let mut graph_chain = *proj.child;
        let mut unpushed = Vec::new();
        for pred in remapped {
            if !Self::push_into_chain(&mut graph_chain, pred.clone()) {
                unpushed.push(pred);
            }
        }

        // Reconstruct: Projection wraps the (possibly modified) graph chain
        let new_proj = Projection::new(proj.table_index, graph_chain, proj.expressions);
        let mut result = LogicalOperator::Projection(new_proj);

        // Any predicates that couldn't be remapped or pushed stay as a Filter above.
        // Unpushed remapped predicates need to be un-remapped back to projection space,
        // but that's complex. Instead, we wrap them as a Filter between Projection and
        // graph chain. Actually, since they reference graph variable table indices (not
        // the projection's table_index), we insert them as a Filter below the Projection.
        if !unpushed.is_empty() {
            // Insert Filter between Projection and graph chain
            let LogicalOperator::Projection(mut p) = result else {
                unreachable!();
            };
            p.child = Box::new(LogicalPlan::synthetic(LogicalOperator::Filter(
                Filter::new(*p.child, unpushed),
            )));
            result = LogicalOperator::Projection(p);
        }

        if !remaining.is_empty() {
            result =
                LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(result), remaining));
        }

        result
    }

    /// Push predicates from a Filter directly above a graph chain.
    fn pushdown_into_graph_chain(&mut self, filter: Filter) -> LogicalOperator {
        let mut all_preds = Vec::new();
        for expr in filter.expressions {
            Self::split_and_predicates(expr, &mut all_preds);
        }

        let mut graph_chain = *filter.child;
        let mut remaining = Vec::new();
        for pred in all_preds {
            if !Self::push_into_chain(&mut graph_chain, pred.clone()) {
                remaining.push(pred);
            }
        }

        if remaining.is_empty() {
            graph_chain.operator
        } else {
            LogicalOperator::Filter(Filter::new(graph_chain, remaining))
        }
    }

    /// Try to push a single predicate into the graph chain.
    ///
    /// Returns `true` if the predicate was successfully pushed, `false` if it
    /// must remain as a filter above.
    fn push_into_chain(chain: &mut LogicalPlan, pred: Expression) -> bool {
        let bindings = Self::extract_table_indices(&pred);
        if bindings.is_empty() {
            // Constant predicate — can't push into graph operators
            return false;
        }

        Self::push_into_chain_recursive(chain, pred, &bindings)
    }

    /// Recursively walk the graph chain to find the right operator for the predicate.
    fn push_into_chain_recursive(
        plan: &mut LogicalPlan,
        pred: Expression,
        bindings: &HashSet<usize>,
    ) -> bool {
        match &mut plan.operator {
            LogicalOperator::GraphScan(gs) => {
                // GraphScan owns its table_index
                if bindings.len() == 1 && bindings.contains(&gs.table_index) {
                    Self::merge_filter(&mut gs.filter, pred);
                    return true;
                }
                false
            }
            LogicalOperator::GraphExpand(ge) => {
                // GraphExpand owns edge_table_index and target_table_index.
                // Check if predicate only references the edge
                if bindings.len() == 1 && bindings.contains(&ge.edge_table_index) {
                    Self::merge_filter(&mut ge.edge_filter, pred);
                    return true;
                }
                // Check if predicate only references the target vertex
                if bindings.len() == 1 && bindings.contains(&ge.target_table_index) {
                    Self::merge_filter(&mut ge.target_filter, pred);
                    return true;
                }

                // Try pushing deeper into the child chain
                if Self::push_into_chain_recursive(ge.child.as_mut(), pred.clone(), bindings) {
                    return true;
                }

                // If the predicate references only this expand's indices (edge + target),
                // we can't push it further but it's already handled by the expand.
                // For cross-operator predicates (e.g., a.age > b.age), we can't push.
                false
            }
            LogicalOperator::Filter(f) => {
                // There might be an existing filter in the chain — try pushing past it
                Self::push_into_chain_recursive(f.child.as_mut(), pred, bindings)
            }
            _ => false,
        }
    }

    /// Merge a predicate into an existing optional filter using AND.
    fn merge_filter(slot: &mut Option<Expression>, pred: Expression) {
        match slot.take() {
            Some(existing) => {
                *slot = Some(Expression::Conjunction(ConjunctionExpression::new(
                    ConjunctionType::And,
                    vec![existing, pred],
                )));
            }
            None => {
                *slot = Some(pred);
            }
        }
    }

    /// Split an expression by AND into individual predicates.
    fn split_and_predicates(expr: Expression, out: &mut Vec<Expression>) {
        if let Expression::Conjunction(ref conj) = expr {
            if conj.conjunction_type == ConjunctionType::And {
                for child in conj.children.clone() {
                    Self::split_and_predicates(child, out);
                }
                return;
            }
        }
        out.push(expr);
    }

    /// Extract all table_index values referenced by an expression.
    fn extract_table_indices(expr: &Expression) -> HashSet<usize> {
        let mut indices = HashSet::new();
        Self::collect_table_indices(expr, &mut indices);
        indices
    }

    fn collect_table_indices(expr: &Expression, indices: &mut HashSet<usize>) {
        match expr {
            Expression::ColumnRef(col) => {
                indices.insert(col.binding.table_index);
            }
            Expression::Comparison(comp) => {
                Self::collect_table_indices(&comp.left, indices);
                Self::collect_table_indices(&comp.right, indices);
            }
            Expression::Conjunction(conj) => {
                for child in &conj.children {
                    Self::collect_table_indices(child, indices);
                }
            }
            Expression::Function(func) => {
                for child in &func.children {
                    Self::collect_table_indices(child, indices);
                }
            }
            Expression::Cast(cast) => {
                Self::collect_table_indices(&cast.child, indices);
            }
            Expression::Operator(op) => {
                for child in &op.children {
                    Self::collect_table_indices(child, indices);
                }
            }
            Expression::Constant(_) | Expression::Reference(_) => {}
            Expression::Aggregate(agg) => {
                for child in &agg.children {
                    Self::collect_table_indices(child, indices);
                }
                if let Some(filter) = &agg.filter {
                    Self::collect_table_indices(filter, indices);
                }
                for order in &agg.order_bys {
                    Self::collect_table_indices(&order.expression, indices);
                }
            }
            Expression::Case(case) => {
                Self::collect_table_indices(&case.check, indices);
                Self::collect_table_indices(&case.result_if_true, indices);
                Self::collect_table_indices(&case.result_if_false, indices);
            }
            Expression::Subquery(_) | Expression::Window(_) => {}
        }
    }

    /// Remap a predicate through a Projection.
    ///
    /// Replaces column references to the Projection's table_index with the
    /// underlying expression from the Projection's expression list.
    /// Returns `None` if the predicate references columns outside the Projection.
    fn remap_through_projection(proj: &Projection, expr: Expression) -> Option<Expression> {
        use std::cell::Cell;
        let success = Cell::new(true);
        let result = expr.replace_column_ref(&|col: &ColumnRefExpression| {
            if col.binding.table_index == proj.table_index
                && col.binding.column_index < proj.expressions.len()
            {
                Some(proj.expressions[col.binding.column_index].clone())
            } else {
                success.set(false);
                None
            }
        });
        if success.get() {
            Some(result)
        } else {
            None
        }
    }
}

impl Default for GraphPredicatePushdown {
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
    use paro_parser::ast::EdgeDirection;
    use paro_planner::binder::bind::graph::{
        BoundEdgeVariable, BoundPatternElement, BoundVertexVariable,
    };
    use paro_planner::binder::ir::{BoundGraphColumn, BoundGraphPattern};
    use paro_planner::expression::{
        ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression, Expression,
    };
    use paro_planner::operator::ColumnBinding;
    use paro_planner::operator::{GraphMatch, LogicalOperator, LogicalOperatorType};
    use std::sync::Arc;

    use crate::graph::match_decompose::GraphMatchDecompose;

    fn make_vertex(name: &str, label: &str, table_index: usize) -> BoundVertexVariable {
        BoundVertexVariable {
            variable_name: name.to_string(),
            vertex_table_info: VertexTableInfo {
                table_name: label.to_string(),
                table_oid: 0,
                key_column_ids: vec![0],
                label: label.to_string(),
                property_column_ids: vec![0, 1, 2],
            },
            table_index,
            column_bindings: vec![
                ColumnBinding::new(table_index, 0),
                ColumnBinding::new(table_index, 1),
                ColumnBinding::new(table_index, 2),
            ],
            column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
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
            column_names: vec!["weight".to_string()],
            direction,
            quantifier: None,
            filter: None,
            source_variable: src_var.to_string(),
            destination_variable: dst_var.to_string(),
        }
    }

    fn make_column(table_index: usize, col_index: usize, ty: LogicalType) -> BoundGraphColumn {
        BoundGraphColumn {
            expr: Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, col_index),
                ty.clone(),
            )),
            alias: format!("col_{}_{}", table_index, col_index),
            logical_type: ty,
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
            )),
            BoundGraphPattern { elements },
            columns,
            table_index,
            output_types,
            None,
            false,
        ))
    }

    /// Build a decomposed plan: Projection → GraphExpand → GraphScan
    fn build_one_hop_plan() -> LogicalOperator {
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        // COLUMNS: a.name (col 1), a.age (col 2), b.name (col 1), k.weight (col 0)
        let columns = vec![
            make_column(10, 1, LogicalType::Varchar), // a.name → output col 0
            make_column(10, 2, LogicalType::Integer), // a.age  → output col 1
            make_column(12, 1, LogicalType::Varchar), // b.name → output col 2
            make_column(11, 0, LogicalType::Float),   // k.weight → output col 3
        ];
        let plan = make_graph_match(elements, columns, 100);

        // Decompose
        let mut decompose = GraphMatchDecompose::new();
        decompose.optimize(plan)
    }

    /// Make a comparison predicate: column_ref op constant
    fn make_pred(
        table_index: usize,
        col_index: usize,
        col_type: LogicalType,
        cmp: ComparisonType,
        val: paro_common::runtime_value::Value,
    ) -> Expression {
        Expression::Comparison(ComparisonExpression::new(
            cmp,
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, col_index),
                col_type.clone(),
            )),
            Expression::Constant(ConstantExpression::new(val, col_type)),
        ))
    }

    // --- Tests ---

    #[test]
    fn test_push_scan_vertex_predicate() {
        // Filter(a.age > 30) → Projection → GraphExpand → GraphScan
        // The predicate references output col 1 (a.age, table_index=100, col_index=1)
        // After remapping through Projection: references table_index=10 (scan vertex)
        // Should be pushed into GraphScan.filter
        let plan = build_one_hop_plan();
        let pred = make_pred(
            100,
            1,
            LogicalType::Integer,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Integer(30),
        );
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(plan), vec![pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        // Result should be: Projection → GraphExpand → GraphScan(filter: age > 30)
        // No outer Filter
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                assert!(ge.edge_filter.is_none(), "edge should have no filter");
                assert!(ge.target_filter.is_none(), "target should have no filter");
                if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                    assert!(gs.filter.is_some(), "scan should have filter");
                } else {
                    panic!("expected GraphScan");
                }
            } else {
                panic!("expected GraphExpand");
            }
        } else {
            panic!("expected Projection");
        }
    }

    #[test]
    fn test_push_target_vertex_predicate() {
        // Filter(b.name = 'Alice') → Projection → GraphExpand → GraphScan
        // Output col 2 = b.name (table_index=12 = target vertex)
        // Should be pushed into GraphExpand.target_filter
        let plan = build_one_hop_plan();
        let pred = make_pred(
            100,
            2,
            LogicalType::Varchar,
            ComparisonType::Equal,
            paro_common::runtime_value::Value::Varchar("Alice".to_string()),
        );
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(plan), vec![pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                assert!(ge.target_filter.is_some(), "target should have filter");
                assert!(ge.edge_filter.is_none());
            } else {
                panic!("expected GraphExpand");
            }
        }
    }

    #[test]
    fn test_push_edge_predicate() {
        // Filter(k.weight > 0.5) → Projection → GraphExpand → GraphScan
        // Output col 3 = k.weight (table_index=11 = edge)
        // Should be pushed into GraphExpand.edge_filter
        let plan = build_one_hop_plan();
        let pred = make_pred(
            100,
            3,
            LogicalType::Float,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Float(0.5),
        );
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(plan), vec![pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                assert!(ge.edge_filter.is_some(), "edge should have filter");
                assert!(ge.target_filter.is_none());
                if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                    assert!(gs.filter.is_none());
                }
            }
        }
    }

    #[test]
    fn test_push_multiple_predicates_to_different_operators() {
        // Filter(a.age > 30 AND k.weight > 0.5) → Projection → GraphExpand → GraphScan
        // a.age > 30 → GraphScan.filter
        // k.weight > 0.5 → GraphExpand.edge_filter
        let plan = build_one_hop_plan();
        let pred_a = make_pred(
            100,
            1,
            LogicalType::Integer,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Integer(30),
        );
        let pred_k = make_pred(
            100,
            3,
            LogicalType::Float,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Float(0.5),
        );
        let and_pred = Expression::Conjunction(ConjunctionExpression::new(
            ConjunctionType::And,
            vec![pred_a, pred_k],
        ));
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(plan), vec![and_pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        // No outer Filter — both predicates pushed
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                assert!(ge.edge_filter.is_some(), "edge should have filter");
                if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                    assert!(gs.filter.is_some(), "scan should have filter");
                }
            }
        }
    }

    #[test]
    fn test_cross_variable_predicate_stays_above() {
        // Filter(a.age > b.age) — references both table_index=10 and table_index=12
        // This can't be pushed into a single operator, so it stays as a Filter
        let plan = build_one_hop_plan();

        // Build a.age > b.age: but we need to reference through the projection.
        // Output col 1 = a.age (table_index=10, col 2)
        // Output col 2 = b.name (table_index=12, col 1)
        // Let's make a predicate that after remapping references both table 10 and 12
        let cross_pred = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::GreaterThan,
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(100, 1), // a.age
                LogicalType::Integer,
            )),
            Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(100, 2), // b.name
                LogicalType::Varchar,
            )),
        ));
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(plan), vec![cross_pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        // The cross-variable predicate can't be pushed into a single graph operator.
        // It should remain as a Filter (between Projection and graph chain).
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            // There should be a Filter between Projection and GraphExpand
            assert!(
                matches!(&proj.child.operator, LogicalOperator::Filter(_)),
                "unpushed predicate should be a Filter below Projection"
            );
        }
    }

    #[test]
    fn test_no_filter_no_change() {
        // No Filter above the graph chain — nothing to push
        let plan = build_one_hop_plan();

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(plan);

        // Should be unchanged: Projection → GraphExpand → GraphScan
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
    }

    #[test]
    fn test_merge_with_existing_filter() {
        // GraphScan already has a filter from the MATCH WHERE clause.
        // Pushing another predicate should AND them together.
        let mut v_a = make_vertex("a", "Person", 10);
        v_a.filter = Some(make_pred(
            10,
            1,
            LogicalType::Varchar,
            ComparisonType::Equal,
            paro_common::runtime_value::Value::Varchar("Bob".to_string()),
        ));
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![
            make_column(10, 1, LogicalType::Varchar),
            make_column(10, 2, LogicalType::Integer),
            make_column(12, 1, LogicalType::Varchar),
            make_column(11, 0, LogicalType::Float),
        ];
        let plan = make_graph_match(elements, columns, 100);
        let mut decompose = GraphMatchDecompose::new();
        let decomposed = decompose.optimize(plan);

        // Add outer filter: a.age > 30 (output col 1)
        let pred = make_pred(
            100,
            1,
            LogicalType::Integer,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Integer(30),
        );
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(decomposed), vec![pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        // GraphScan should now have a Conjunction(AND) filter
        assert_eq!(result.op_type(), LogicalOperatorType::Projection);
        if let LogicalOperator::Projection(proj) = &result {
            if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                    assert!(gs.filter.is_some());
                    // Should be AND conjunction of existing + new
                    if let Some(Expression::Conjunction(conj)) = &gs.filter {
                        assert_eq!(conj.conjunction_type, ConjunctionType::And);
                        assert_eq!(conj.children.len(), 2);
                    } else {
                        panic!("expected AND conjunction filter");
                    }
                }
            }
        }
    }

    #[test]
    fn test_filter_directly_above_graph_chain() {
        // Filter directly above GraphExpand (no Projection)
        // This can happen if the Projection was removed or in intermediate states
        let v_a = make_vertex("a", "Person", 10);
        let e_k = make_edge("k", "Knows", 11, EdgeDirection::Right, "a", "b");
        let v_b = make_vertex("b", "Person", 12);

        let elements = vec![
            BoundPatternElement::Vertex(v_a),
            BoundPatternElement::Edge(e_k),
            BoundPatternElement::Vertex(v_b),
        ];
        let columns = vec![make_column(10, 1, LogicalType::Varchar)];
        let plan = make_graph_match(elements, columns, 100);
        let mut decompose = GraphMatchDecompose::new();
        let decomposed = decompose.optimize(plan);

        // Extract the graph chain from under the Projection
        let graph_chain = if let LogicalOperator::Projection(proj) = decomposed {
            *proj.child
        } else {
            panic!("expected Projection");
        };

        // Put a Filter directly above the graph chain with a predicate on table_index=10
        let pred = make_pred(
            10,
            2,
            LogicalType::Integer,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Integer(25),
        );
        let filtered = LogicalOperator::Filter(Filter::new(graph_chain, vec![pred]));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(filtered);

        // Should push into GraphScan, no remaining Filter
        assert_eq!(result.op_type(), LogicalOperatorType::GraphExpand);
        if let LogicalOperator::GraphExpand(ge) = &result {
            if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                assert!(gs.filter.is_some(), "scan should have filter");
            }
        }
    }

    #[test]
    fn test_nested_under_order() {
        // Order → Filter → Projection → GraphExpand → GraphScan
        // Should still find and push the filter
        let plan = build_one_hop_plan();
        let pred = make_pred(
            100,
            1,
            LogicalType::Integer,
            ComparisonType::GreaterThan,
            paro_common::runtime_value::Value::Integer(30),
        );
        let filtered =
            LogicalOperator::Filter(Filter::new(LogicalPlan::synthetic(plan), vec![pred]));
        let ordered = LogicalOperator::Order(paro_planner::operator::Order::new(
            LogicalPlan::synthetic(filtered),
            vec![],
        ));

        let mut opt = GraphPredicatePushdown::new();
        let result = opt.optimize(ordered);

        // Order → Projection → GraphExpand → GraphScan(filter)
        assert_eq!(result.op_type(), LogicalOperatorType::Order);
        if let LogicalOperator::Order(o) = &result {
            assert_eq!(o.child.operator.op_type(), LogicalOperatorType::Projection);
            if let LogicalOperator::Projection(proj) = &o.child.operator {
                if let LogicalOperator::GraphExpand(ge) = &proj.child.operator {
                    if let LogicalOperator::GraphScan(gs) = &ge.child.operator {
                        assert!(gs.filter.is_some());
                    }
                }
            }
        }
    }
}
