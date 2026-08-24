// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Arena-backed immutable physical plan.

use std::cell::Cell;
use std::fmt::Write;

use super::children::{PlanChildren, PlanChildrenArena};
use super::ids::PhysicalPlanNodeId;
use super::node::PhysicalPlanNode;
use super::properties::PlanPropertyMap;
use super::specs::{AggregateSpec, NestedLoopJoinSpec, PhysicalNodeKind, SearchSourceSpec};
use crate::explain::types::{
    ExplainDoc, ExplainNode, ExplainProperty, ExplainValue, EXPLAIN_FORMAT_VERSION,
};
use paro_catalog::entry::{StandardEntry, TableCatalogEntry};
use paro_planner::expression::{
    AggregateExpression, AggregateType, Expression, OperatorType, WindowFrameBound, WindowFrameType,
};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition};
use paro_planner::operator::ExplainSpec;
use paro_planner::plan::CardinalityEstimate;
use paro_storage::index::{Predicate, PredicateTree};
use paro_storage::search::CapabilityToken;
use paro_storage::table::segment_reorderer::{
    OrderByStatistics, SegmentOrderOptions, SegmentOrderType,
};

#[derive(Debug, Clone, Default)]
pub struct PhysicalPlanNodeArena {
    nodes: Vec<PhysicalPlanNode>,
}

impl PhysicalPlanNodeArena {
    pub fn push(&mut self, mut node: PhysicalPlanNode) -> PhysicalPlanNodeId {
        let id = PhysicalPlanNodeId::new(self.nodes.len());
        node.id = id;
        self.nodes.push(node);
        id
    }

    pub fn get(&self, id: PhysicalPlanNodeId) -> Option<&PhysicalPlanNode> {
        self.nodes.get(id.index())
    }

    pub(crate) fn get_mut(&mut self, id: PhysicalPlanNodeId) -> Option<&mut PhysicalPlanNode> {
        self.nodes.get_mut(id.index())
    }

    pub fn iter(&self) -> impl Iterator<Item = &PhysicalPlanNode> {
        self.nodes.iter()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub root: PhysicalPlanNodeId,
    pub nodes: PhysicalPlanNodeArena,
    pub children: PlanChildrenArena,
    pub properties: PlanPropertyMap,
}

impl PhysicalPlan {
    pub fn new(
        root: PhysicalPlanNodeId,
        nodes: PhysicalPlanNodeArena,
        children: PlanChildrenArena,
        properties: PlanPropertyMap,
    ) -> Self {
        Self {
            root,
            nodes,
            children,
            properties,
        }
    }

    pub fn node(&self, id: PhysicalPlanNodeId) -> &PhysicalPlanNode {
        self.nodes
            .get(id)
            .expect("physical plan node id must refer to arena entry")
    }

    pub fn child_ids<'a>(&'a self, children: &'a PlanChildren) -> &'a [PhysicalPlanNodeId] {
        children.as_slice(&self.children)
    }

    /// Remove nodes made unreachable by physical rewrites and reassign dense
    /// ids. A physical plan arena is part of the observable plan contract; it
    /// must not retain folded operators that can be mistaken for consumers.
    pub(crate) fn compact_reachable(&mut self) {
        let mut reachable = vec![false; self.nodes.len()];
        let mut stack = vec![self.root];
        while let Some(id) = stack.pop() {
            if std::mem::replace(&mut reachable[id.index()], true) {
                continue;
            }
            stack.extend_from_slice(self.child_ids(&self.node(id).children));
        }

        if reachable.iter().all(|reachable| *reachable) {
            return;
        }

        let mut remap = vec![PhysicalPlanNodeId::INVALID; reachable.len()];
        let mut next_index = 0;
        for (old_index, is_reachable) in reachable.iter().copied().enumerate() {
            if is_reachable {
                remap[old_index] = PhysicalPlanNodeId::new(next_index);
                next_index += 1;
            }
        }

        let old_children = std::mem::take(&mut self.children);
        let old_nodes = std::mem::take(&mut self.nodes.nodes);
        let mut nodes = PhysicalPlanNodeArena::default();
        let mut children = PlanChildrenArena::default();
        for mut node in old_nodes
            .into_iter()
            .filter(|node| reachable[node.id.index()])
        {
            let remapped_children = node
                .children
                .as_slice(&old_children)
                .iter()
                .map(|child| remap[child.index()])
                .collect();
            node.children = children.pack(remapped_children);
            node.id = PhysicalPlanNodeId::INVALID;
            nodes.push(node);
        }

        self.root = remap[self.root.index()];
        self.nodes = nodes;
        self.children = children;
    }

    /// Return a structural one-row guarantee, independent of optimizer
    /// cardinality estimates. Consumers may use this as a semantic proof.
    pub fn guarantees_exactly_one_row(&self, id: PhysicalPlanNodeId) -> bool {
        let node = self.node(id);
        match &node.kind {
            PhysicalNodeKind::Aggregate(spec) => {
                spec.grouping_key_count == 0
                    && spec.grouping_sets.len() <= 1
                    && spec.having_filter.is_empty()
            }
            PhysicalNodeKind::Project(_) | PhysicalNodeKind::Sort(_) => {
                let [child] = self.child_ids(&node.children) else {
                    return false;
                };
                self.guarantees_exactly_one_row(*child)
            }
            _ => false,
        }
    }

    pub fn format_tree(&self) -> String {
        let mut out = String::new();
        self.format_node(self.root, 0, &mut out);
        out
    }

    pub fn format_explain_text_with_spec(&self, spec: &ExplainSpec) -> String {
        let doc = self.to_explain_doc(*spec);
        let mut out = String::new();
        self.format_explain_node(&doc.root, spec, 0, false, &mut out);
        out
    }

    pub fn format_explain_json(&self, spec: ExplainSpec) -> String {
        self.to_explain_doc(spec).to_json().to_string()
    }

    fn to_explain_doc(&self, spec: ExplainSpec) -> ExplainDoc {
        ExplainDoc {
            format_version: EXPLAIN_FORMAT_VERSION,
            spec,
            root: self.explain_node(self.root),
            summary: Vec::new(),
        }
    }

    fn format_node(&self, id: PhysicalPlanNodeId, depth: usize, out: &mut String) {
        let node = self.node(id);
        for _ in 0..depth {
            out.push_str("  ");
        }
        let _ = writeln!(
            out,
            "#{:03} {} logical={:?} cols={}",
            id.index(),
            node.label.display_name,
            node.label.logical_plan_node,
            node.output.column_count()
        );
        for child in self.child_ids(&node.children) {
            self.format_node(*child, depth + 1, out);
        }
    }

    fn format_explain_node(
        &self,
        node: &ExplainNode,
        spec: &ExplainSpec,
        depth: usize,
        child_prefix: bool,
        out: &mut String,
    ) {
        for _ in 0..depth {
            out.push_str("  ");
        }
        if child_prefix {
            out.push_str("->  ");
        }
        out.push_str(&node.header_text(spec));
        out.push('\n');
        self.format_explain_properties(node, spec, depth, out);
        for child in &node.children {
            self.format_explain_node(child, spec, depth + 1, true, out);
        }
    }

    fn format_explain_properties(
        &self,
        node: &ExplainNode,
        spec: &ExplainSpec,
        depth: usize,
        out: &mut String,
    ) {
        let property_depth = depth + 1;
        let mut write_property = |line: String| {
            for _ in 0..property_depth {
                out.push_str("  ");
            }
            let _ = writeln!(out, "{line}");
        };

        let mut output_schema = None;
        for property in &node.properties {
            if property.label == "Output Schema" {
                output_schema = Some(property.text_line());
                continue;
            }
            write_property(property.text_line());
        }
        if spec.detail.verbose {
            if let Some(cardinality) = node.estimated_cardinality {
                write_property(format!(
                    "Cardinality: [{}, {}, {}]",
                    cardinality.min, cardinality.expected, cardinality.max
                ));
            }
            if let Some(output_schema) = output_schema {
                write_property(output_schema);
            }
        }
    }

    fn explain_node(&self, id: PhysicalPlanNodeId) -> ExplainNode {
        let node = self.node(id);
        ExplainNode {
            node_id: Some((id.index() + 1) as u64),
            operator_name: explain_operator_name(&node.kind).to_string(),
            relation_name: explain_relation_name(&node.kind),
            relation_alias: explain_relation_alias(&node.kind),
            output_names: node.output.explain_names(matches!(
                &node.kind,
                PhysicalNodeKind::HashJoin(_)
                    | PhysicalNodeKind::NestedLoopJoin(_)
                    | PhysicalNodeKind::SortRangeJoin(_)
                    | PhysicalNodeKind::ClassicIeJoin(_)
                    | PhysicalNodeKind::CrossProduct(_)
                    | PhysicalNodeKind::DelimJoin(_)
            )),
            estimated_cardinality: explain_cardinality(&node.kind, node.cardinality),
            actual: None,
            properties: collect_explain_properties(self, id, node),
            children: self
                .child_ids(&node.children)
                .iter()
                .map(|child| self.explain_node(*child))
                .collect(),
        }
    }

    fn input_names(&self, id: PhysicalPlanNodeId, child_index: usize) -> Vec<String> {
        self.child_ids(&self.node(id).children)
            .get(child_index)
            .map(|child| self.expression_scope_names(*child))
            .unwrap_or_default()
    }

    /// Names used only while rendering expressions consumed by `id`'s
    /// parent. Internal definitions are expanded at most one producer hop;
    /// they never become the producer's schema or feed another definition.
    fn expression_scope_names(&self, id: PhysicalPlanNodeId) -> Vec<String> {
        let node = self.node(id);
        let mut names = node.output.explain_names(true);
        match &node.kind {
            PhysicalNodeKind::Project(spec) => {
                let stable_input_names = self
                    .child_ids(&node.children)
                    .first()
                    .map(|child| self.node(*child).output.explain_names(true))
                    .unwrap_or_default();
                let formatter = ExplainExpressionFormatter::new(&stable_input_names);
                for (index, expression) in spec.expressions.iter().enumerate() {
                    if node
                        .output
                        .identities
                        .get(index)
                        .is_some_and(super::row_type::ColumnIdentity::is_internal)
                    {
                        names[index] = formatter.format(expression);
                    }
                }
            }
            PhysicalNodeKind::Window(spec) => {
                let stable_input_names = self
                    .child_ids(&node.children)
                    .first()
                    .map(|child| self.node(*child).output.explain_names(true))
                    .unwrap_or_default();
                let formatter = ExplainExpressionFormatter::new(&stable_input_names);
                for (offset, expression) in spec.expressions.iter().enumerate() {
                    let index = spec.input_width + offset;
                    if let Some(name) = names.get_mut(index) {
                        *name = formatter.format(&Expression::Window(expression.clone()));
                    }
                }
            }
            PhysicalNodeKind::Aggregate(spec) => {
                let stable_input_names = self
                    .child_ids(&node.children)
                    .first()
                    .map(|child| self.node(*child).output.explain_names(true))
                    .unwrap_or_default();
                if let Some(scope_names) = aggregate_scope_names(spec, &stable_input_names) {
                    for (index, scope_name) in scope_names.into_iter().enumerate() {
                        if node
                            .output
                            .identities
                            .get(index)
                            .is_some_and(super::row_type::ColumnIdentity::is_internal)
                        {
                            names[index] = scope_name;
                        }
                    }
                }
            }
            PhysicalNodeKind::Filter(spec) => {
                if let Some(child) = self.child_ids(&node.children).first() {
                    let child_names = self.expression_scope_names(*child);
                    names = spec
                        .projection_map
                        .iter()
                        .filter_map(|index| child_names.get(*index).cloned())
                        .collect();
                }
            }
            PhysicalNodeKind::Sort(spec) => {
                if let Some(child) = self.child_ids(&node.children).first() {
                    let child_names = self.expression_scope_names(*child);
                    names = spec
                        .projection_map
                        .iter()
                        .filter_map(|index| child_names.get(*index).cloned())
                        .collect();
                }
            }
            PhysicalNodeKind::Limit(_)
            | PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::EmptyResult(_) => {
                if let Some(child) = self.child_ids(&node.children).first() {
                    let child_names = self.expression_scope_names(*child);
                    if child_names.len() == names.len() {
                        names = child_names;
                    }
                }
            }
            PhysicalNodeKind::HashJoin(spec) => {
                let children = self.child_ids(&node.children);
                if let [left, right] = children {
                    let left_names = self.expression_scope_names(*left);
                    let right_names = self.expression_scope_names(*right);
                    names = spec
                        .left_projection
                        .iter()
                        .filter_map(|index| left_names.get(*index).cloned())
                        .chain(
                            spec.build_input_projection
                                .iter()
                                .take(spec.build_output_count)
                                .filter_map(|index| right_names.get(*index).cloned()),
                        )
                        .collect();
                }
            }
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                names = self.join_expression_scope_names(
                    node,
                    &spec.left_projection,
                    &spec.right_projection,
                );
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                names = self.join_expression_scope_names(
                    node,
                    &spec.left_projection,
                    &spec.right_projection,
                );
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                names = self.join_expression_scope_names(
                    node,
                    &spec.left_projection,
                    &spec.right_projection,
                );
            }
            PhysicalNodeKind::CrossProduct(_) => {
                let children = self.child_ids(&node.children);
                if let [left, right] = children {
                    names = self
                        .expression_scope_names(*left)
                        .into_iter()
                        .chain(self.expression_scope_names(*right))
                        .collect();
                }
            }
            PhysicalNodeKind::DelimJoin(_) => {
                if let Some(consumer) = self.child_ids(&node.children).get(1) {
                    let consumer_names = self.expression_scope_names(*consumer);
                    if consumer_names.len() == names.len() {
                        names = consumer_names;
                    }
                }
            }
            _ => {}
        }
        names
    }

    fn join_expression_scope_names(
        &self,
        node: &PhysicalPlanNode,
        left_projection: &[usize],
        right_projection: &[usize],
    ) -> Vec<String> {
        let children = self.child_ids(&node.children);
        let [left, right] = children else {
            return node.output.explain_names(true);
        };
        let left_names = self.expression_scope_names(*left);
        let right_names = self.expression_scope_names(*right);
        left_projection
            .iter()
            .filter_map(|index| left_names.get(*index).cloned())
            .chain(
                right_projection
                    .iter()
                    .filter_map(|index| right_names.get(*index).cloned()),
            )
            .collect()
    }

    fn join_input_names(&self, id: PhysicalPlanNodeId, child_index: usize) -> Vec<String> {
        let Some(child_id) = self
            .child_ids(&self.node(id).children)
            .get(child_index)
            .copied()
        else {
            return Vec::new();
        };
        self.node(child_id).output.explain_names(true)
    }
}

fn collect_explain_properties(
    plan: &PhysicalPlan,
    id: PhysicalPlanNodeId,
    node: &PhysicalPlanNode,
) -> Vec<ExplainProperty> {
    let mut properties = Vec::new();
    let input_names = plan.input_names(id, 0);
    let output_names = node.output.explain_names(false);
    let input_formatter = ExplainExpressionFormatter::new(&input_names);

    match &node.kind {
        PhysicalNodeKind::Project(spec) => {
            if !spec.output_names.is_empty() {
                let scope_names = plan.expression_scope_names(id);
                let outputs = output_names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        if node
                            .output
                            .identities
                            .get(index)
                            .is_some_and(super::row_type::ColumnIdentity::is_internal)
                            && scope_names.get(index).is_some_and(|scope| scope != name)
                        {
                            scope_names[index].clone()
                        } else {
                            name.clone()
                        }
                    })
                    .collect::<Vec<_>>();
                push_list_property(&mut properties, "Output", &outputs);
            }
        }
        PhysicalNodeKind::RowsetScan(spec) => {
            let scan_formatter = ExplainExpressionFormatter::new(&output_names);
            if !output_names.is_empty() {
                push_list_property(&mut properties, "Columns", &output_names);
            }
            if let Some(predicate) = &spec.predicate {
                push_string_property(
                    &mut properties,
                    "Pushed Predicate",
                    format_predicate_tree(predicate, spec.table.as_ref()),
                );
            }
            if spec.planned_materialization().is_late() {
                push_string_property(&mut properties, "Late Materialize", "auto".to_string());
            }
            if !spec.residual_predicates.is_empty() {
                push_string_property(
                    &mut properties,
                    "Residual Predicate",
                    spec.residual_predicates
                        .iter()
                        .map(|expression| scan_formatter.format(expression))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            if !spec.runtime_filter_expressions.is_empty() {
                push_string_property(
                    &mut properties,
                    "Runtime Filter",
                    spec.runtime_filter_expressions
                        .iter()
                        .map(|expression| scan_formatter.format(expression))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            if let Some(order) = &spec.scan_order {
                push_string_property(
                    &mut properties,
                    "Scan Order",
                    format_segment_order(order, spec.table.as_ref()),
                );
            }
        }
        PhysicalNodeKind::Filter(spec) => {
            let filter_formatter = ExplainExpressionFormatter::new(&input_names);
            if !spec.expressions.is_empty() {
                push_string_property(
                    &mut properties,
                    "Filter",
                    spec.expressions
                        .iter()
                        .map(|expression| filter_formatter.format(expression))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
        }
        PhysicalNodeKind::Sort(spec) => {
            if !spec.orders.is_empty() {
                push_string_property(
                    &mut properties,
                    "Sort Key",
                    input_formatter.format_order_by(&spec.orders),
                );
            }
        }
        PhysicalNodeKind::TopN(spec) => {
            if !spec.orders.is_empty() {
                push_string_property(
                    &mut properties,
                    "Sort Key",
                    input_formatter.format_order_by(&spec.orders),
                );
            }
            push_string_property(&mut properties, "Limit", spec.limit.to_string());
        }
        PhysicalNodeKind::VectorSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_vector_search_properties(&mut properties, spec);
        }
        PhysicalNodeKind::SparseVectorSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_sparse_search_properties(&mut properties, spec);
        }
        PhysicalNodeKind::FullTextSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_fulltext_search_properties(&mut properties, spec);
        }
        PhysicalNodeKind::AdaptiveSearch(spec) => {
            push_string_property(&mut properties, "Strategy", "adaptive".to_string());
            push_search_token_properties(
                &mut properties,
                search_source_token(spec.selected.as_ref()),
            );
            push_search_source_properties(&mut properties, spec.selected.as_ref());
        }
        PhysicalNodeKind::Limit(spec) => {
            if let Some(limit) = &spec.limit {
                push_string_property(&mut properties, "Limit", input_formatter.format(limit));
            }
            if let Some(offset) = &spec.offset {
                push_string_property(&mut properties, "Offset", input_formatter.format(offset));
            }
        }
        PhysicalNodeKind::HashJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            if spec.build_time_integer_index.is_some() {
                push_string_property(
                    &mut properties,
                    "Build Index",
                    "integer_build_time".to_string(),
                );
            }
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(
                &mut properties,
                &spec.key_conditions,
                &left_names,
                &right_names,
            );
            push_join_conditions(
                &mut properties,
                &spec.build_residual_conditions,
                &left_names,
                &right_names,
            );
        }
        PhysicalNodeKind::NestedLoopJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            if let Some(strategy) = nested_loop_strategy(spec) {
                push_string_property(&mut properties, "Strategy", strategy.to_string());
            }
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(&mut properties, &spec.conditions, &left_names, &right_names);
            if let Some(condition) = &spec.arbitrary_condition {
                let combined_names = left_names
                    .iter()
                    .chain(right_names.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                push_string_property(
                    &mut properties,
                    "Join Filter",
                    ExplainExpressionFormatter::new(&combined_names).format(condition),
                );
            }
        }
        PhysicalNodeKind::SortRangeJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_string_property(&mut properties, "Strategy", "sort_range".to_string());
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(&mut properties, &spec.conditions, &left_names, &right_names);
        }
        PhysicalNodeKind::ClassicIeJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_string_property(&mut properties, "Strategy", "classic_ie_join".to_string());
            let left_names = plan.join_input_names(id, 0);
            let right_names = plan.join_input_names(id, 1);
            push_join_conditions(&mut properties, &spec.conditions, &left_names, &right_names);
        }
        PhysicalNodeKind::GraphScan(spec) => {
            push_string_property(&mut properties, "Graph", spec.graph_name.clone());
            push_string_property(&mut properties, "Vertex Label", spec.label.clone());
            push_string_property(
                &mut properties,
                "Table",
                spec.vertex_info.table_name.clone(),
            );
            if spec.filter.is_some() {
                push_string_property(&mut properties, "Filter", "<pushed down>".to_string());
            }
        }
        PhysicalNodeKind::GraphExpand(spec) => {
            push_string_property(&mut properties, "Graph", spec.graph_name.clone());
            push_string_property(&mut properties, "Edge Label", spec.edge_info.label.clone());
            push_string_property(
                &mut properties,
                "Direction",
                format!("{:?}", spec.direction),
            );
            if spec.min_hops != 1 || spec.max_hops != 1 {
                push_string_property(
                    &mut properties,
                    "Hops",
                    format_hops(spec.min_hops, spec.max_hops),
                );
            }
        }
        PhysicalNodeKind::GraphShortestPath(spec) => {
            push_string_property(&mut properties, "Graph", spec.graph_name.clone());
            push_string_property(&mut properties, "Edge Label", spec.edge_info.label.clone());
            push_string_property(
                &mut properties,
                "Direction",
                format!("{:?}", spec.direction),
            );
            push_string_property(
                &mut properties,
                "Hops",
                format_hops(spec.min_hops, spec.max_hops),
            );
        }
        PhysicalNodeKind::GraphProject(spec) => {
            if !spec.output_names.is_empty() {
                push_list_property(&mut properties, "Output", &output_names);
            }
            if !spec.filters.is_empty() {
                push_string_property(&mut properties, "Filter", "<pushed down>".to_string());
            }
        }
        PhysicalNodeKind::RowFetch(spec) => {
            if !output_names.is_empty() {
                push_list_property(&mut properties, "Output", &output_names);
            }
            push_string_property(&mut properties, "Sources", spec.mappings.len().to_string());
        }
        PhysicalNodeKind::Aggregate(spec) => {
            push_aggregate_properties(&mut properties, spec, &input_names);
        }
        PhysicalNodeKind::PartitionAggregateWindow(spec) => {
            push_aggregate_properties(&mut properties, &spec.aggregate, &input_names);
            push_string_property(
                &mut properties,
                "Retained Detail Columns",
                spec.detail_columns.len().to_string(),
            );
            if !output_names.is_empty() {
                push_list_property(&mut properties, "Output", &output_names);
            }
        }
        PhysicalNodeKind::MaterializedCte(spec) => {
            push_string_property(&mut properties, "CTE Name", spec.cte_name.clone());
            push_string_property(
                &mut properties,
                "Materialization",
                format_cte_materialization(spec.materialized).to_string(),
            );
            push_string_property(
                &mut properties,
                "Reference Count",
                spec.ref_count.to_string(),
            );
        }
        PhysicalNodeKind::RecursiveCte(spec) => {
            push_string_property(&mut properties, "CTE Name", spec.cte_name.clone());
            push_string_property(&mut properties, "Union All", spec.union_all.to_string());
        }
        PhysicalNodeKind::CteScan(spec) => {
            push_string_property(&mut properties, "CTE Index", spec.cte_index.to_string());
            push_string_property(&mut properties, "Table Index", spec.table_index.to_string());
        }
        PhysicalNodeKind::Insert(spec) => {
            push_string_property(&mut properties, "Table", spec.table.name().to_string());
            if !spec.column_index_map.is_empty() {
                let mapping = spec
                    .column_index_map
                    .iter()
                    .enumerate()
                    .filter_map(|(input_idx, column_idx)| {
                        spec.table
                            .columns
                            .get(*column_idx)
                            .map(|column| format!("input#{input_idx}->{}", column.name))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if !mapping.is_empty() {
                    push_string_property(&mut properties, "Column Mapping", mapping);
                }
            }
        }
        PhysicalNodeKind::CopyToFile(spec) => {
            push_string_property(&mut properties, "File", spec.file_path.clone());
            push_string_property(
                &mut properties,
                "PerThreadOutput",
                spec.per_thread_output.to_string(),
            );
        }
        _ => {}
    }

    push_string_property(&mut properties, "Output Schema", format_output_schema(node));

    properties
}

fn push_aggregate_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &AggregateSpec,
    input_names: &[String],
) {
    let formatter = ExplainExpressionFormatter::new(input_names);
    if spec.grouping_key_count > 0 {
        push_string_property(
            properties,
            "Group Key",
            spec.groups
                .iter()
                .map(|expression| format_payload_expr(expression, spec, &formatter))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if !spec.aggregates.is_empty() {
        push_string_property(
            properties,
            "Aggregates",
            spec.aggregates
                .iter()
                .map(|expression| format_aggregate_expr(expression, spec, &formatter))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    if let Some(reduction) = &spec.post_reduction {
        let aggregate_names = spec
            .aggregates
            .iter()
            .map(|expression| format_aggregate_expr(expression, spec, &formatter))
            .collect::<Vec<_>>();
        let reduction_formatter = ExplainExpressionFormatter::new(&aggregate_names);
        push_string_property(
            properties,
            "Post Reduction",
            reduction
                .reducers
                .iter()
                .map(|expression| reduction_formatter.format(expression))
                .collect::<Vec<_>>()
                .join(", "),
        );
        push_string_property(
            properties,
            "Post Predicate",
            reduction_formatter.format(&reduction.predicate),
        );
    }
    if !spec.grouping_sets.is_empty() {
        push_string_property(
            properties,
            "Grouping Sets",
            spec.grouping_sets
                .iter()
                .map(|set| {
                    format!(
                        "({})",
                        set.iter()
                            .map(|index| {
                                spec.groups.get(*index).map_or_else(
                                    || format!("<group {index}>"),
                                    |expression| format_payload_expr(expression, spec, &formatter),
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
}

fn push_search_token_properties(properties: &mut Vec<ExplainProperty>, token: &CapabilityToken) {
    push_string_property(
        properties,
        "Search Definition",
        token.definition_id.to_string(),
    );
    push_string_property(
        properties,
        "Search Generation",
        token.generation_id.to_string(),
    );
    push_string_property(properties, "Search Root", token.root_version.to_string());
    push_string_property(
        properties,
        "Search Capability",
        format!("{:?}", token.capability_state),
    );
}

fn push_search_filter_properties(
    properties: &mut Vec<ExplainProperty>,
    predicate: Option<&crate::physical::specs::SearchPredicateTemplate>,
    contract: crate::physical::specs::SearchFilterContract,
    materialization: Option<paro_storage::search::ExactBitmapMaterialization>,
    table: &TableCatalogEntry,
) {
    let Some(predicate) = predicate else {
        return;
    };
    push_string_property(
        properties,
        "Pushed Predicate",
        format_search_predicate(predicate, table),
    );
    let contract = match (contract, materialization) {
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentBitmapNoResidual,
            Some(paro_storage::search::ExactBitmapMaterialization::ScalarIndex),
        ) => "exact segment bitmap from scalar postings; no residual filter",
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentBitmapNoResidual,
            Some(paro_storage::search::ExactBitmapMaterialization::ColumnScan),
        ) => "exact segment bitmap materialized by column scan; no residual filter",
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentBitmapNoResidual,
            Some(paro_storage::search::ExactBitmapMaterialization::Mixed {
                indexed_rows,
                scanned_rows,
            }),
        ) => {
            push_string_property(
                properties,
                "Filter Bitmap Coverage",
                format!("indexed rows {indexed_rows}; column-scan rows {scanned_rows}"),
            );
            "exact segment bitmap from mixed scalar postings and column scans; no residual filter"
        }
        (crate::physical::specs::SearchFilterContract::ExactSegmentBitmapNoResidual, None) => {
            "exact segment bitmap; materialization unknown; no residual filter"
        }
        (crate::physical::specs::SearchFilterContract::None, _) => "unproven",
    };
    push_string_property(properties, "Filter Pushdown", contract.to_string());
}

fn push_dense_search_filter_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::VectorSearchSpec,
) {
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.bitmap_materialization,
        spec.table.as_ref(),
    );
    if spec.predicate.is_none() {
        return;
    }

    let has_runtime_parameters = spec
        .predicate
        .as_ref()
        .is_some_and(|predicate| predicate.has_runtime_parameters());
    if !has_runtime_parameters {
        if let Some(rows) = spec.estimated_filter_rows {
            push_string_property(properties, "Filter Rows (estimated)", rows.to_string());
        }
    }

    let strategy = match (
        has_runtime_parameters,
        spec.estimated_filter_rows,
        spec.estimated_total_rows,
    ) {
        (true, _, _) => {
            "runtime exact bitmap; exact scan below cardinality threshold, otherwise adaptive connected HNSW with observed admission, two-hop predicate refinement, then exact fallback"
                .to_string()
        }
        (false, Some(rows), Some(total_rows)) => {
            use paro_storage::index::hnsw::{
                estimate_filtered_search_strategy, HnswFilteredSearchStrategy,
            };
            let effective_ef = spec.search_policy.effective_ef(spec.k, spec.params.ef);
            let decision = estimate_filtered_search_strategy(
                rows,
                total_rows,
                spec.k,
                effective_ef,
                spec.avg_level0_degree,
                spec.search_policy,
            );
            let name = match decision.strategy {
                HnswFilteredSearchStrategy::ExactScan => "exact bitmap distance scan",
                HnswFilteredSearchStrategy::MaskedTopK => {
                    "adaptive connected HNSW; masked admission expected"
                }
                HnswFilteredSearchStrategy::RefinedTopK => {
                    "adaptive connected HNSW; two-hop predicate refinement expected"
                }
            };
            match decision.strategy {
                HnswFilteredSearchStrategy::ExactScan => format!(
                    "{name} (estimated {rows}/{total_rows} rows; exact threshold {})",
                    spec.search_policy.filtered_plain_scan_threshold
                ),
                HnswFilteredSearchStrategy::MaskedTopK => format!(
                    "{name} (estimated {rows}/{total_rows} rows; scored {}; admitted {} >= required {}; runtime may upgrade to refinement, then exact fallback on underfill)",
                    decision.expected_scored_points,
                    decision.expected_admitted_points,
                    decision.required_admitted_points
                ),
                HnswFilteredSearchStrategy::RefinedTopK => format!(
                    "{name} (estimated {rows}/{total_rows} rows; scored {}; admitted {} < required {}; exact scan is final fallback)",
                    decision.expected_scored_points,
                    decision.expected_admitted_points,
                    decision.required_admitted_points
                ),
            }
        }
        _ => "runtime exact bitmap; cardinality unavailable at plan time".to_string(),
    };
    push_string_property(properties, "Filtered Strategy", strategy);
}

fn push_vector_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::VectorSearchSpec,
) {
    push_string_property(properties, "Search Candidate", "dense vector".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Limit", spec.k.to_string());
    push_string_property(
        properties,
        "Search Ef",
        spec.params
            .ef
            .map_or_else(|| "default".to_string(), |ef| ef.to_string()),
    );
    push_dense_search_filter_properties(properties, spec);
}

fn push_sparse_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::SparseVectorSearchSpec,
) {
    push_string_property(properties, "Search Candidate", "sparse vector".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Limit", spec.k.to_string());
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.bitmap_materialization,
        spec.table.as_ref(),
    );
}

fn push_fulltext_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::FullTextSearchSpec,
) {
    push_string_property(properties, "Search Candidate", "full text".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Mode", format!("{:?}", spec.mode));
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.bitmap_materialization,
        spec.table.as_ref(),
    );
}

fn push_search_source_properties(properties: &mut Vec<ExplainProperty>, source: &SearchSourceSpec) {
    match source {
        SearchSourceSpec::Vector(spec) => push_vector_search_properties(properties, spec),
        SearchSourceSpec::Sparse(spec) => push_sparse_search_properties(properties, spec),
        SearchSourceSpec::FullText(spec) => push_fulltext_search_properties(properties, spec),
    }
}

fn search_source_token(source: &SearchSourceSpec) -> &CapabilityToken {
    match source {
        SearchSourceSpec::Vector(spec) => &spec.capability_token,
        SearchSourceSpec::Sparse(spec) => &spec.capability_token,
        SearchSourceSpec::FullText(spec) => &spec.capability_token,
    }
}

fn push_string_property(properties: &mut Vec<ExplainProperty>, label: &'static str, value: String) {
    properties.push(ExplainProperty::new(
        label,
        ExplainValue::String(bound_explain_text(value)),
    ));
}

fn push_list_property(
    properties: &mut Vec<ExplainProperty>,
    label: &'static str,
    values: &[String],
) {
    let mut bounded = values
        .iter()
        .take(EXPLAIN_EXPRESSION_MAX_NODES)
        .cloned()
        .map(bound_explain_text)
        .map(ExplainValue::String)
        .collect::<Vec<_>>();
    if values.len() > EXPLAIN_EXPRESSION_MAX_NODES {
        bounded.push(ExplainValue::String("…".to_string()));
    }
    properties.push(ExplainProperty::new(label, ExplainValue::List(bounded)));
}

fn push_join_conditions(
    properties: &mut Vec<ExplainProperty>,
    conditions: &[JoinCondition],
    left_names: &[String],
    right_names: &[String],
) {
    if conditions.is_empty() {
        return;
    }
    properties.push(ExplainProperty::new(
        "Join Condition",
        ExplainValue::List(
            conditions
                .iter()
                .map(|condition| {
                    ExplainValue::String(format_join_condition(condition, left_names, right_names))
                })
                .collect(),
        ),
    ));
}

fn explain_operator_name(kind: &PhysicalNodeKind) -> &'static str {
    match kind {
        PhysicalNodeKind::Project(_) => "PROJECTION",
        PhysicalNodeKind::Sort(_) => "ORDER_BY",
        PhysicalNodeKind::Aggregate(_) => "AGGREGATE",
        _ => kind.name(),
    }
}

fn explain_relation_name(kind: &PhysicalNodeKind) -> Option<String> {
    match kind {
        PhysicalNodeKind::RowsetScan(spec) => spec.relation_name.clone(),
        PhysicalNodeKind::VectorSearch(spec) => Some(qualified_table_name(spec.table.as_ref())),
        PhysicalNodeKind::SparseVectorSearch(spec) => {
            Some(qualified_table_name(spec.table.as_ref()))
        }
        PhysicalNodeKind::FullTextSearch(spec) => Some(qualified_table_name(spec.table.as_ref())),
        PhysicalNodeKind::AdaptiveSearch(spec) => Some(qualified_table_name(spec.table.as_ref())),
        _ => None,
    }
}

fn explain_relation_alias(kind: &PhysicalNodeKind) -> Option<String> {
    match kind {
        PhysicalNodeKind::RowsetScan(spec) => spec.relation_alias.clone(),
        PhysicalNodeKind::Values(spec) => spec.relation_alias.clone(),
        _ => None,
    }
}

fn explain_cardinality(
    kind: &PhysicalNodeKind,
    estimated: Option<CardinalityEstimate>,
) -> Option<CardinalityEstimate> {
    let fallback = fallback_cardinality(kind);
    match (estimated, fallback) {
        (Some(current), Some(fallback))
            if is_search_scan(kind) && current.expected == 0 && fallback.expected > 0 =>
        {
            Some(fallback)
        }
        (Some(current), _) => Some(current),
        (None, fallback) => fallback,
    }
}

fn is_search_scan(kind: &PhysicalNodeKind) -> bool {
    matches!(
        kind,
        PhysicalNodeKind::VectorSearch(_)
            | PhysicalNodeKind::SparseVectorSearch(_)
            | PhysicalNodeKind::FullTextSearch(_)
            | PhysicalNodeKind::AdaptiveSearch(_)
    )
}

fn fallback_cardinality(kind: &PhysicalNodeKind) -> Option<CardinalityEstimate> {
    let rows = match kind {
        PhysicalNodeKind::VectorSearch(spec) => spec
            .table
            .storage
            .as_ref()
            .and_then(|table| table.hnsw_index_statistics(spec.column_id as u32))
            .map(|stats| stats.num_indexed_vectors as u64)
            .filter(|rows| *rows > 0)
            .or_else(|| table_row_count(spec.table.as_ref())),
        PhysicalNodeKind::SparseVectorSearch(spec) => spec
            .table
            .storage
            .as_ref()
            .and_then(|table| table.sparse_index_statistics(spec.column_id as u32))
            .map(|stats| stats.num_indexed_vectors as u64)
            .filter(|rows| *rows > 0)
            .or_else(|| table_row_count(spec.table.as_ref())),
        PhysicalNodeKind::FullTextSearch(spec) => spec
            .table
            .storage
            .as_ref()
            .and_then(|table| table.fulltext_index_statistics(spec.column_id as u32))
            .map(|stats| stats.total_docs as u64)
            .filter(|rows| *rows > 0)
            .or_else(|| table_row_count(spec.table.as_ref())),
        PhysicalNodeKind::AdaptiveSearch(spec) => table_row_count(spec.table.as_ref()),
        _ => None,
    }?;
    Some(CardinalityEstimate::exact(rows))
}

fn table_row_count(table: &paro_catalog::entry::TableCatalogEntry) -> Option<u64> {
    table
        .storage
        .as_ref()
        .and_then(|storage| storage.tablet().statistics().ok())
        .map(|stats| stats.num_rows)
        .filter(|rows| *rows > 0)
        .or_else(|| {
            table
                .statistics()
                .and_then(|stats| (stats.row_count > 0).then_some(stats.row_count))
        })
}

fn qualified_table_name(table: &paro_catalog::entry::TableCatalogEntry) -> String {
    format!("{}.{}", table.schema_name(), table.name())
}

fn format_segment_order(order: &SegmentOrderOptions, table: &TableCatalogEntry) -> String {
    let stat = match order.order_by {
        OrderByStatistics::Min => "min",
        OrderByStatistics::Max => "max",
    };
    let direction = match order.order_type {
        SegmentOrderType::Asc => "ASC",
        SegmentOrderType::Desc => "DESC",
    };
    let limit = order
        .row_limit
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();
    let offset = if order.row_offset == 0 {
        String::new()
    } else {
        format!(" OFFSET {}", order.row_offset)
    };
    let column = table_column_name(table, order.column_idx);
    format!("{column} {direction} by {stat}{limit}{offset}")
}

fn format_hops(min_hops: u64, max_hops: u64) -> String {
    if max_hops == u64::MAX {
        format!("{{{},}}", min_hops)
    } else {
        format!("{{{min_hops},{max_hops}}}")
    }
}

fn format_output_schema(node: &PhysicalPlanNode) -> String {
    if node.output.column_count() == 0 {
        return "(none)".to_string();
    }
    node.output
        .identities
        .iter()
        .enumerate()
        .zip(node.output.types.iter())
        .map(|((ordinal, identity), ty)| {
            let name = match identity {
                super::row_type::ColumnIdentity::Visible {
                    name,
                    qualifier: Some(qualifier),
                } => qualifier
                    .iter()
                    .map(|part| format_schema_identifier(part))
                    .chain(std::iter::once(format_schema_identifier(name)))
                    .collect::<Vec<_>>()
                    .join("."),
                super::row_type::ColumnIdentity::Visible {
                    name,
                    qualifier: None,
                } => format_schema_identifier(name),
                super::row_type::ColumnIdentity::Internal => {
                    format_schema_identifier(&format!("__internal_{}", ordinal + 1))
                }
                super::row_type::ColumnIdentity::InternalNamed(name) => {
                    format_schema_identifier(name)
                }
            };
            format!("{name} {ty}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_schema_identifier(name: &str) -> String {
    if !name.is_empty()
        && name.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        })
    {
        return name.to_string();
    }
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn format_join_condition(
    condition: &JoinCondition,
    left_names: &[String],
    right_names: &[String],
) -> String {
    let left = ExplainExpressionFormatter::new(left_names).format(&condition.left);
    let right = ExplainExpressionFormatter::new(right_names).format(&condition.right);
    format!(
        "{} {} {}",
        left,
        join_comparison_symbol(condition.comparison),
        right
    )
}

fn join_comparison_symbol(comparison: JoinComparisonType) -> &'static str {
    match comparison {
        JoinComparisonType::Equal => "=",
        JoinComparisonType::NotEqual => "<>",
        JoinComparisonType::LessThan => "<",
        JoinComparisonType::GreaterThan => ">",
        JoinComparisonType::LessThanOrEqual => "<=",
        JoinComparisonType::GreaterThanOrEqual => ">=",
        JoinComparisonType::NotDistinctFrom => "IS NOT DISTINCT FROM",
        JoinComparisonType::DistinctFrom => "IS DISTINCT FROM",
    }
}

fn nested_loop_strategy(spec: &NestedLoopJoinSpec) -> Option<&'static str> {
    if is_range_only_join(&spec.conditions) {
        Some("nl_fallback")
    } else {
        None
    }
}

fn is_range_only_join(conditions: &[JoinCondition]) -> bool {
    !conditions.is_empty()
        && conditions.iter().all(|condition| {
            matches!(
                condition.comparison,
                JoinComparisonType::LessThan
                    | JoinComparisonType::GreaterThan
                    | JoinComparisonType::LessThanOrEqual
                    | JoinComparisonType::GreaterThanOrEqual
            )
        })
}

fn format_cte_materialization(
    materialized: paro_planner::binder::ir::CTEMaterialize,
) -> &'static str {
    match materialized {
        paro_planner::binder::ir::CTEMaterialize::Default
        | paro_planner::binder::ir::CTEMaterialize::Materialized => "MATERIALIZED",
        paro_planner::binder::ir::CTEMaterialize::NotMaterialized => "NOT MATERIALIZED",
    }
}

fn aggregate_scope_names(spec: &AggregateSpec, input_names: &[String]) -> Option<Vec<String>> {
    let formatter = ExplainExpressionFormatter::new(input_names);
    let mut state_names = spec
        .groups
        .iter()
        .map(|expression| format_payload_expr(expression, spec, &formatter))
        .chain(
            spec.aggregates
                .iter()
                .map(|expression| format_aggregate_expr(expression, spec, &formatter)),
        )
        .collect::<Vec<_>>();
    state_names.extend(spec.grouping_functions.iter().map(|grouping| {
        let arguments = grouping
            .iter()
            .filter_map(|index| spec.groups.get(*index))
            .map(|expression| format_payload_expr(expression, spec, &formatter))
            .collect::<Vec<_>>()
            .join(", ");
        bound_explain_text(format!("grouping({arguments})"))
    }));

    if spec.state_output_projection.is_empty() {
        return Some(state_names);
    }
    spec.state_output_projection
        .iter()
        .map(|index| state_names.get(*index).cloned())
        .collect()
}

fn format_payload_expr(
    expression: &Expression,
    spec: &AggregateSpec,
    formatter: &ExplainExpressionFormatter<'_>,
) -> String {
    if let Expression::Reference(reference) = expression {
        if let Some(payload) = spec.projection_exprs.get(reference.index) {
            return formatter.format(payload);
        }
    }
    formatter.format(expression)
}

fn format_aggregate_expr(
    expression: &Expression,
    spec: &AggregateSpec,
    formatter: &ExplainExpressionFormatter<'_>,
) -> String {
    let Expression::Aggregate(aggregate) = expression else {
        return formatter.format(expression);
    };
    format_bound_aggregate(aggregate, &|child| {
        format_payload_expr(child, spec, formatter)
    })
}

fn format_bound_aggregate(
    aggregate: &AggregateExpression,
    format_child: &impl Fn(&Expression) -> String,
) -> String {
    let distinct = if aggregate.aggr_type == AggregateType::Distinct {
        "DISTINCT "
    } else {
        ""
    };
    let args = if aggregate.children.is_empty() {
        "*".to_string()
    } else {
        aggregate
            .children
            .iter()
            .map(format_child)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut rendered = format!("{}({distinct}{args})", aggregate.function.name);
    if let Some(filter) = aggregate.filter.as_ref() {
        rendered.push_str(" FILTER (WHERE ");
        rendered.push_str(&format_child(filter));
        rendered.push(')');
    }
    if !aggregate.order_bys.is_empty() {
        rendered.push_str(" WITHIN GROUP (ORDER BY ");
        rendered.push_str(
            &aggregate
                .order_bys
                .iter()
                .map(|order| {
                    format!(
                        "{} {} NULLS {}",
                        format_child(&order.expression),
                        if order.ascending { "ASC" } else { "DESC" },
                        if order.nulls_first { "FIRST" } else { "LAST" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
        rendered.push(')');
    }
    rendered
}

struct ExplainExpressionFormatter<'a> {
    columns: &'a [String],
}

const EXPLAIN_EXPRESSION_MAX_NODES: usize = 1_024;
const EXPLAIN_EXPRESSION_MAX_DEPTH: usize = 64;
const EXPLAIN_EXPRESSION_MAX_BYTES: usize = 16 * 1_024;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ExplainPrecedence {
    Lowest,
    Or,
    And,
    Not,
    Comparison,
    Primary,
}

struct ExplainFormatBudget {
    remaining_nodes: Cell<usize>,
}

impl ExplainFormatBudget {
    fn new() -> Self {
        Self {
            remaining_nodes: Cell::new(EXPLAIN_EXPRESSION_MAX_NODES),
        }
    }

    fn enter(&self, depth: usize) -> bool {
        let remaining = self.remaining_nodes.get();
        if depth >= EXPLAIN_EXPRESSION_MAX_DEPTH || remaining == 0 {
            return false;
        }
        self.remaining_nodes.set(remaining - 1);
        true
    }
}

impl<'a> ExplainExpressionFormatter<'a> {
    fn new(columns: &'a [String]) -> Self {
        Self { columns }
    }

    fn column(&self, index: usize) -> String {
        self.columns
            .get(index)
            .cloned()
            .unwrap_or_else(|| format!("<column {index}>"))
    }

    fn format_order_by(&self, orders: &[paro_planner::binder::ir::OrderByNode]) -> String {
        bound_explain_text(
            orders
                .iter()
                .map(|order| {
                    format!(
                        "{} {} NULLS {}",
                        self.format(&order.expression),
                        if order.ascending { "ASC" } else { "DESC" },
                        if order.nulls_first { "FIRST" } else { "LAST" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        )
    }

    fn format(&self, expression: &Expression) -> String {
        let budget = ExplainFormatBudget::new();
        bound_explain_text(self.format_expression(
            expression,
            ExplainPrecedence::Lowest,
            0,
            &budget,
        ))
    }

    fn format_expression(
        &self,
        expression: &Expression,
        parent_precedence: ExplainPrecedence,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        if !budget.enter(depth) {
            return "…".to_string();
        }
        let next_depth = depth + 1;
        let (rendered, precedence) = match expression {
            Expression::Reference(reference) => {
                (self.column(reference.index), ExplainPrecedence::Primary)
            }
            Expression::ColumnRef(column) => (
                self.column(column.binding.column_index),
                ExplainPrecedence::Primary,
            ),
            Expression::Constant(constant) => {
                (constant.value.to_string(), ExplainPrecedence::Primary)
            }
            Expression::Parameter(parameter) => (
                format!("${}", parameter.slot.index.index() + 1),
                ExplainPrecedence::Primary,
            ),
            Expression::Comparison(comparison) => (
                format!(
                    "{} {} {}",
                    self.format_expression(
                        &comparison.left,
                        ExplainPrecedence::Comparison,
                        next_depth,
                        budget,
                    ),
                    comparison.comparison_type,
                    self.format_expression(
                        &comparison.right,
                        ExplainPrecedence::Comparison,
                        next_depth,
                        budget,
                    )
                ),
                ExplainPrecedence::Comparison,
            ),
            Expression::Conjunction(conjunction) => {
                let (separator, precedence) = match conjunction.conjunction_type {
                    paro_planner::expression::ConjunctionType::And => {
                        (" AND ", ExplainPrecedence::And)
                    }
                    paro_planner::expression::ConjunctionType::Or => {
                        (" OR ", ExplainPrecedence::Or)
                    }
                };
                (
                    conjunction
                        .children
                        .iter()
                        .map(|child| self.format_expression(child, precedence, next_depth, budget))
                        .collect::<Vec<_>>()
                        .join(separator),
                    precedence,
                )
            }
            Expression::Cast(cast) => {
                let cast_name = if cast.try_cast { "TRY_CAST" } else { "CAST" };
                (
                    format!(
                        "{cast_name}({} AS {})",
                        self.format_expression(
                            &cast.child,
                            ExplainPrecedence::Lowest,
                            next_depth,
                            budget,
                        ),
                        cast.target_type
                    ),
                    ExplainPrecedence::Primary,
                )
            }
            Expression::Function(function) => (
                format!(
                    "{}({})",
                    function.function.name.as_str(),
                    function
                        .children
                        .iter()
                        .map(|child| self.format_expression(
                            child,
                            ExplainPrecedence::Lowest,
                            next_depth,
                            budget,
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                ExplainPrecedence::Primary,
            ),
            Expression::Aggregate(aggregate) => (
                format_bound_aggregate(aggregate, &|child| {
                    self.format_expression(child, ExplainPrecedence::Lowest, next_depth, budget)
                }),
                ExplainPrecedence::Primary,
            ),
            Expression::Case(case_expression) => (
                format!(
                    "CASE WHEN {} THEN {} ELSE {} END",
                    self.format_expression(
                        &case_expression.check,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    ),
                    self.format_expression(
                        &case_expression.result_if_true,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    ),
                    self.format_expression(
                        &case_expression.result_if_false,
                        ExplainPrecedence::Lowest,
                        next_depth,
                        budget,
                    )
                ),
                ExplainPrecedence::Primary,
            ),
            Expression::Operator(operator) => self.format_operator(operator, next_depth, budget),
            Expression::Subquery(subquery) => {
                let kind = match subquery.subquery_type {
                    paro_planner::expression::SubqueryType::Scalar => "SUBQUERY",
                    paro_planner::expression::SubqueryType::Exists => "EXISTS SUBQUERY",
                    paro_planner::expression::SubqueryType::NotExists => "NOT EXISTS SUBQUERY",
                    paro_planner::expression::SubqueryType::Any => "ANY SUBQUERY",
                    paro_planner::expression::SubqueryType::All => "ALL SUBQUERY",
                };
                if subquery.children.is_empty() {
                    (format!("<{kind}>"), ExplainPrecedence::Primary)
                } else {
                    (
                        format!(
                            "{} {} <{kind}>",
                            subquery
                                .children
                                .iter()
                                .map(|child| self.format_expression(
                                    child,
                                    ExplainPrecedence::Lowest,
                                    next_depth,
                                    budget,
                                ))
                                .collect::<Vec<_>>()
                                .join(", "),
                            subquery.comparison_type
                        ),
                        ExplainPrecedence::Comparison,
                    )
                }
            }
            Expression::Window(window) => (
                self.format_window(window, next_depth, budget),
                ExplainPrecedence::Primary,
            ),
        };
        let rendered = bound_explain_text(rendered);
        if precedence < parent_precedence {
            bound_explain_text(format!("({rendered})"))
        } else {
            rendered
        }
    }

    fn format_operator(
        &self,
        operator: &paro_planner::expression::OperatorExpression,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> (String, ExplainPrecedence) {
        let child = |index: usize, precedence: ExplainPrecedence| {
            operator
                .children
                .get(index)
                .map(|child| self.format_expression(child, precedence, depth, budget))
        };
        let children = || {
            operator
                .children
                .iter()
                .map(|child| {
                    self.format_expression(child, ExplainPrecedence::Lowest, depth, budget)
                })
                .collect::<Vec<_>>()
        };
        match operator.operator_type {
            OperatorType::In | OperatorType::NotIn => (
                child(0, ExplainPrecedence::Comparison).map_or_else(
                    || "<invalid IN>".to_string(),
                    |left| {
                        format!(
                            "{} {}IN ({})",
                            left,
                            if operator.operator_type == OperatorType::NotIn {
                                "NOT "
                            } else {
                                ""
                            },
                            operator.children[1..]
                                .iter()
                                .map(|child| self.format_expression(
                                    child,
                                    ExplainPrecedence::Lowest,
                                    depth,
                                    budget,
                                ))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                ),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::Not => (
                child(0, ExplainPrecedence::Not)
                    .map(|child| format!("NOT {child}"))
                    .unwrap_or_else(|| "<invalid NOT>".to_string()),
                ExplainPrecedence::Not,
            ),
            OperatorType::IsNull => (
                child(0, ExplainPrecedence::Comparison)
                    .map(|child| format!("{child} IS NULL"))
                    .unwrap_or_else(|| "<invalid IS NULL>".to_string()),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::IsNotNull => (
                child(0, ExplainPrecedence::Comparison)
                    .map(|child| format!("{child} IS NOT NULL"))
                    .unwrap_or_else(|| "<invalid IS NOT NULL>".to_string()),
                ExplainPrecedence::Comparison,
            ),
            OperatorType::Coalesce => (
                format!("COALESCE({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::Like | OperatorType::ILike => (
                match (
                    child(0, ExplainPrecedence::Comparison),
                    child(1, ExplainPrecedence::Comparison),
                ) {
                    (Some(left), Some(right)) => format!(
                        "{} {} {}",
                        left,
                        if operator.operator_type == OperatorType::Like {
                            "LIKE"
                        } else {
                            "ILIKE"
                        },
                        right
                    ),
                    _ => "<invalid LIKE>".to_string(),
                },
                ExplainPrecedence::Comparison,
            ),
            OperatorType::ArrayConstructor => (
                format!("[{}]", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::StructConstructor => (
                format!("({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
            OperatorType::ArrayExtract => (
                match (
                    child(0, ExplainPrecedence::Primary),
                    child(1, ExplainPrecedence::Lowest),
                ) {
                    (Some(array), Some(index)) => format!("{array}[{index}]"),
                    _ => "<invalid array extract>".to_string(),
                },
                ExplainPrecedence::Primary,
            ),
            OperatorType::ErrorIfMultipleRows => (
                format!("error_if_multiple_rows({})", children().join(", ")),
                ExplainPrecedence::Primary,
            ),
        }
    }

    fn format_window(
        &self,
        window: &paro_planner::expression::WindowExpression,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        let mut rendered = format!(
            "{}({}) OVER (",
            window.function_name(),
            window
                .arguments()
                .iter()
                .map(|argument| self.format_expression(
                    argument,
                    ExplainPrecedence::Lowest,
                    depth,
                    budget,
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !window.partitions.is_empty() {
            rendered.push_str("PARTITION BY ");
            rendered.push_str(
                &window
                    .partitions
                    .iter()
                    .map(|partition| {
                        self.format_expression(partition, ExplainPrecedence::Lowest, depth, budget)
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !window.orders.is_empty() {
            if !window.partitions.is_empty() {
                rendered.push(' ');
            }
            rendered.push_str("ORDER BY ");
            rendered.push_str(
                &window
                    .orders
                    .iter()
                    .map(|order| {
                        format!(
                            "{} {} NULLS {}",
                            self.format_expression(
                                &order.expression,
                                ExplainPrecedence::Lowest,
                                depth,
                                budget,
                            ),
                            if order.ascending { "ASC" } else { "DESC" },
                            if order.nulls_first { "FIRST" } else { "LAST" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !window.partitions.is_empty() || !window.orders.is_empty() {
            rendered.push(' ');
        }
        rendered.push_str(match window.frame.frame_type {
            WindowFrameType::Rows => "ROWS BETWEEN ",
            WindowFrameType::Range => "RANGE BETWEEN ",
        });
        rendered.push_str(&self.format_window_bound(
            &window.frame.start_bound,
            window.frame.start_is_preceding,
            depth,
            budget,
        ));
        rendered.push_str(" AND ");
        rendered.push_str(&self.format_window_bound(
            &window.frame.end_bound,
            window.frame.end_is_preceding,
            depth,
            budget,
        ));
        rendered.push(')');
        bound_explain_text(rendered)
    }

    fn format_window_bound(
        &self,
        bound: &WindowFrameBound,
        preceding: bool,
        depth: usize,
        budget: &ExplainFormatBudget,
    ) -> String {
        match bound {
            WindowFrameBound::Unbounded => format!(
                "UNBOUNDED {}",
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Offset(offset) => format!(
                "{} {}",
                self.format_expression(offset, ExplainPrecedence::Lowest, depth, budget),
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
        }
    }
}

fn bound_explain_text(mut text: String) -> String {
    if text.len() <= EXPLAIN_EXPRESSION_MAX_BYTES {
        return text;
    }
    let mut boundary = EXPLAIN_EXPRESSION_MAX_BYTES.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    text.truncate(boundary);
    text.push('…');
    text
}

fn table_column_name(table: &TableCatalogEntry, column_id: usize) -> String {
    table
        .columns
        .get(column_id)
        .map(|column| column.name.clone())
        .unwrap_or_else(|| format!("<column {column_id}>"))
}

fn format_predicate_tree<V: std::fmt::Display>(
    predicate: &PredicateTree<V>,
    table: &TableCatalogEntry,
) -> String {
    let budget = ExplainFormatBudget::new();
    format_predicate_tree_with_precedence(predicate, table, ExplainPrecedence::Lowest, 0, &budget)
}

fn format_predicate_tree_with_precedence<V: std::fmt::Display>(
    predicate: &PredicateTree<V>,
    table: &TableCatalogEntry,
    parent_precedence: ExplainPrecedence,
    depth: usize,
    budget: &ExplainFormatBudget,
) -> String {
    if !budget.enter(depth) {
        return "…".to_string();
    }
    let next_depth = depth + 1;
    let (rendered, precedence) = match predicate {
        PredicateTree::Leaf(predicate) => (
            format_predicate(predicate, table),
            ExplainPrecedence::Primary,
        ),
        PredicateTree::And(children) => (
            children
                .iter()
                .map(|child| {
                    format_predicate_tree_with_precedence(
                        child,
                        table,
                        ExplainPrecedence::And,
                        next_depth,
                        budget,
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            ExplainPrecedence::And,
        ),
        PredicateTree::Or(children) => (
            children
                .iter()
                .map(|child| {
                    format_predicate_tree_with_precedence(
                        child,
                        table,
                        ExplainPrecedence::Or,
                        next_depth,
                        budget,
                    )
                })
                .collect::<Vec<_>>()
                .join(" OR "),
            ExplainPrecedence::Or,
        ),
    };
    if precedence < parent_precedence {
        bound_explain_text(format!("({rendered})"))
    } else {
        bound_explain_text(rendered)
    }
}

fn format_predicate<V: std::fmt::Display>(
    predicate: &Predicate<V>,
    table: &TableCatalogEntry,
) -> String {
    let name = |column_id: u32| table_column_name(table, column_id as usize);
    match predicate {
        Predicate::Eq { column_id, value } => format!("{} = {value}", name(*column_id)),
        Predicate::NotEq { column_id, value } => format!("{} != {value}", name(*column_id)),
        Predicate::Lt { column_id, value } => format!("{} < {value}", name(*column_id)),
        Predicate::Le { column_id, value } => format!("{} <= {value}", name(*column_id)),
        Predicate::Gt { column_id, value } => format!("{} > {value}", name(*column_id)),
        Predicate::Ge { column_id, value } => format!("{} >= {value}", name(*column_id)),
        Predicate::In { column_id, values } => format!(
            "{} IN ({})",
            name(*column_id),
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Predicate::FixedIn { column_id, values } => {
            format!("{} IN ({} fixed values)", name(*column_id), values.len())
        }
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => format!("{} BETWEEN {lower} AND {upper}", name(*column_id)),
        Predicate::IsNull { column_id } => format!("{} IS NULL", name(*column_id)),
        Predicate::IsNotNull { column_id } => format!("{} IS NOT NULL", name(*column_id)),
        Predicate::StringPrefix {
            column_id,
            prefix,
            negated,
        } => format!(
            "{} {} PREFIX {prefix:?}",
            name(*column_id),
            if *negated { "NOT" } else { "HAS" }
        ),
        Predicate::StringPrefixIn {
            column_id,
            prefixes,
        } => format!(
            "{} HAS PREFIX IN ({})",
            name(*column_id),
            prefixes
                .iter()
                .map(|prefix| format!("{prefix:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Predicate::StringLike {
            column_id,
            pattern,
            negated,
        } => format!(
            "{} {} {pattern:?}",
            name(*column_id),
            if *negated { "NOT LIKE" } else { "LIKE" }
        ),
        Predicate::ColumnComparison {
            left_column_id,
            right_column_id,
            comparison,
        } => format!(
            "{} {comparison} {}",
            name(*left_column_id),
            name(*right_column_id)
        ),
    }
}

fn format_search_predicate(
    predicate: &crate::physical::specs::SearchPredicateTemplate,
    table: &TableCatalogEntry,
) -> String {
    format_predicate_tree(predicate.tree(), table)
}
