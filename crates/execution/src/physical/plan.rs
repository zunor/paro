// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Arena-backed immutable physical plan.

use std::fmt::Write;

use super::children::{PlanChildren, PlanChildrenArena};
use super::ids::PhysicalPlanNodeId;
use super::node::PhysicalPlanNode;
use super::properties::PlanPropertyMap;
use super::specs::{AggregateSpec, NestedLoopJoinSpec, PhysicalNodeKind, SearchSourceSpec};
use crate::explain::types::{ExplainDoc, ExplainNode, ExplainProperty, ExplainValue};
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
            format_version: 1,
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
            output_names: self.resolved_explain_output_names(id),
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

    /// Names visible to expressions consuming this node.
    ///
    /// Physical expressions address columns positionally. EXPLAIN resolves
    /// those positions against the producer schema instead of exposing the
    /// execution ABI (`#0`, `#1`, ...). Synthetic physical outputs are
    /// expanded here so parents inherit useful names as well.
    fn explain_output_names(&self, id: PhysicalPlanNodeId) -> Vec<String> {
        let node = self.node(id);
        let child_ids = self.child_ids(&node.children);
        match &node.kind {
            PhysicalNodeKind::Project(spec) => {
                let input_names = child_ids
                    .first()
                    .map(|child| self.explain_output_names(*child))
                    .unwrap_or_default();
                let formatter = ExplainExpressionFormatter::new(&input_names);
                spec.output_names
                    .iter()
                    .zip(spec.expressions.iter())
                    .map(|(name, expression)| {
                        if is_internal_column_name(name) {
                            formatter.format(expression)
                        } else {
                            name.clone()
                        }
                    })
                    .collect()
            }
            PhysicalNodeKind::Aggregate(spec) => {
                let input_names = child_ids
                    .first()
                    .map(|child| self.explain_output_names(*child))
                    .unwrap_or_default();
                aggregate_output_names(spec, &input_names)
                    .filter(|names| names.len() == node.output.column_count())
                    .unwrap_or_else(|| node.output.names.to_vec())
            }
            PhysicalNodeKind::Filter(spec) => child_ids
                .first()
                .map(|child| self.explain_output_names(*child))
                .map(|names| project_explain_names(&names, &spec.projection_map))
                .filter(|names| names.len() == node.output.column_count())
                .unwrap_or_else(|| node.output.names.to_vec()),
            PhysicalNodeKind::Sort(spec) => child_ids
                .first()
                .map(|child| self.explain_output_names(*child))
                .map(|names| project_explain_names(&names, &spec.projection_map))
                .filter(|names| names.len() == node.output.column_count())
                .unwrap_or_else(|| node.output.names.to_vec()),
            PhysicalNodeKind::Window(spec) => {
                let child_names = child_ids
                    .first()
                    .map(|child| self.explain_output_names(*child))
                    .unwrap_or_default();
                let formatter = ExplainExpressionFormatter::new(&child_names);
                let mut names = child_names
                    .iter()
                    .take(spec.input_width)
                    .cloned()
                    .collect::<Vec<_>>();
                names.extend(
                    spec.expressions
                        .iter()
                        .map(|expression| formatter.format_window(expression)),
                );
                if names.len() == node.output.column_count() {
                    names
                } else {
                    node.output.names.to_vec()
                }
            }
            PhysicalNodeKind::HashJoin(spec) => {
                let left_names = self.join_input_names(id, 0);
                let right_names = self.join_input_names(id, 1);
                let mut names = project_explain_names(&left_names, &spec.left_projection);
                names.extend(
                    spec.build_input_projection
                        .iter()
                        .take(spec.build_output_count)
                        .filter_map(|index| right_names.get(*index).cloned()),
                );
                complete_explain_names(names, node)
            }
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                let left_names = self.join_input_names(id, 0);
                let right_names = self.join_input_names(id, 1);
                let mut names = project_explain_names(&left_names, &spec.left_projection);
                names.extend(project_explain_names(&right_names, &spec.right_projection));
                complete_explain_names(names, node)
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                let left_names = self.join_input_names(id, 0);
                let right_names = self.join_input_names(id, 1);
                let mut names = project_explain_names(&left_names, &spec.left_projection);
                names.extend(project_explain_names(&right_names, &spec.right_projection));
                complete_explain_names(names, node)
            }
            PhysicalNodeKind::ClassicIeJoin(spec) => {
                let left_names = self.join_input_names(id, 0);
                let right_names = self.join_input_names(id, 1);
                let mut names = project_explain_names(&left_names, &spec.left_projection);
                names.extend(project_explain_names(&right_names, &spec.right_projection));
                complete_explain_names(names, node)
            }
            PhysicalNodeKind::CrossProduct(_) => {
                let mut names = self.join_input_names(id, 0);
                names.extend(self.join_input_names(id, 1));
                complete_explain_names(names, node)
            }
            PhysicalNodeKind::ExternalTable(spec) => {
                let mut names = node.output.names.to_vec();
                if let Some(child_id) = child_ids.first() {
                    let child_names = self.explain_output_names(*child_id);
                    let worker_outputs = spec.worker_output_types.len();
                    for (output_index, name) in names.iter_mut().enumerate().skip(worker_outputs) {
                        let child_index = spec.argument_count + output_index - worker_outputs;
                        if is_internal_column_name(name) {
                            if let Some(source_name) = child_names.get(child_index) {
                                *name = source_name.clone();
                            }
                        }
                    }
                }
                names
            }
            PhysicalNodeKind::Limit(_)
            | PhysicalNodeKind::TopN(_)
            | PhysicalNodeKind::EmptyResult(_) => child_ids
                .first()
                .map(|child| self.explain_output_names(*child))
                .filter(|names| names.len() == node.output.column_count())
                .unwrap_or_else(|| node.output.names.to_vec()),
            _ => node.output.names.to_vec(),
        }
    }

    fn input_names(&self, id: PhysicalPlanNodeId, child_index: usize) -> Vec<String> {
        self.child_ids(&self.node(id).children)
            .get(child_index)
            .map(|child| self.resolved_explain_output_names(*child))
            .unwrap_or_default()
    }

    fn resolved_explain_output_names(&self, id: PhysicalPlanNodeId) -> Vec<String> {
        let mut names = self.explain_output_names(id);
        for (name, alias) in names.iter_mut().zip(self.consumer_aliases(id)) {
            if let Some(alias) = alias {
                *name = alias;
            }
        }
        names
    }

    /// A simple projection above an operator owns the SQL-visible alias for a
    /// pass-through column. Feed that alias back into the child EXPLAIN scope;
    /// this recovers names such as `t(id)` after VALUES has been lowered to its
    /// positional `col0` execution schema.
    fn consumer_aliases(&self, id: PhysicalPlanNodeId) -> Vec<Option<String>> {
        let mut aliases = vec![None; self.node(id).output.column_count()];
        for parent in self.nodes.iter() {
            let Some(child_index) = self
                .child_ids(&parent.children)
                .iter()
                .position(|child| *child == id)
            else {
                continue;
            };

            match &parent.kind {
                PhysicalNodeKind::Project(project) => {
                    for (expression, output_name) in
                        project.expressions.iter().zip(project.output_names.iter())
                    {
                        let Expression::Reference(reference) = expression else {
                            continue;
                        };
                        if !is_internal_column_name(output_name) {
                            if let Some(alias) = aliases.get_mut(reference.index) {
                                *alias = Some(output_name.clone());
                            }
                        }
                    }
                }
                PhysicalNodeKind::MaterializedCte(cte) if child_index == 0 => {
                    for (alias, column_name) in aliases.iter_mut().zip(cte.column_names.iter()) {
                        *alias = Some(column_name.clone());
                    }
                }
                _ => {}
            }
        }
        aliases
    }

    fn join_input_names(&self, id: PhysicalPlanNodeId, child_index: usize) -> Vec<String> {
        let Some(child_id) = self
            .child_ids(&self.node(id).children)
            .get(child_index)
            .copied()
        else {
            return Vec::new();
        };
        let names = self.resolved_explain_output_names(child_id);
        let child = self.node(child_id);
        let qualifier = match &child.kind {
            PhysicalNodeKind::RowsetScan(spec) => spec
                .relation_alias
                .as_deref()
                .or(spec.relation_name.as_deref()),
            _ => None,
        };
        let Some(qualifier) = qualifier else {
            return names;
        };
        names
            .into_iter()
            .map(|name| qualify_column_name(qualifier, name))
            .collect()
    }
}

fn collect_explain_properties(
    plan: &PhysicalPlan,
    id: PhysicalPlanNodeId,
    node: &PhysicalPlanNode,
) -> Vec<ExplainProperty> {
    let mut properties = Vec::new();
    let input_names = plan.input_names(id, 0);
    let output_names = plan.resolved_explain_output_names(id);
    let input_formatter = ExplainExpressionFormatter::new(&input_names);

    match &node.kind {
        PhysicalNodeKind::Project(spec) => {
            if !spec.output_names.is_empty() {
                push_list_property(&mut properties, "Output", &output_names);
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
            let filter_names =
                input_names_with_output_aliases(&input_names, &spec.projection_map, &output_names);
            let filter_formatter = ExplainExpressionFormatter::new(&filter_names);
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
            if let Some(output_names) = aggregate_output_names(spec, &input_names) {
                if !output_names.is_empty() {
                    push_list_property(&mut properties, "Output", &output_names);
                }
            }
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

    push_string_property(
        &mut properties,
        "Output Schema",
        format_output_schema(node, &output_names),
    );

    properties
}

fn project_explain_names(names: &[String], projection: &[usize]) -> Vec<String> {
    projection
        .iter()
        .filter_map(|index| names.get(*index).cloned())
        .collect()
}

fn complete_explain_names(mut names: Vec<String>, node: &PhysicalPlanNode) -> Vec<String> {
    let output_width = node.output.column_count();
    if names.len() > output_width {
        return node.output.names.to_vec();
    }
    names.extend(node.output.names.iter().skip(names.len()).cloned());
    names
}

fn input_names_with_output_aliases(
    input_names: &[String],
    projection: &[usize],
    output_names: &[String],
) -> Vec<String> {
    let mut names = input_names.to_vec();
    for (output_index, input_index) in projection.iter().copied().enumerate() {
        let Some((input_name, output_name)) = names
            .get_mut(input_index)
            .zip(output_names.get(output_index))
        else {
            continue;
        };
        if !is_internal_column_name(output_name) {
            *input_name = output_name.clone();
        }
    }
    names
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
    push_string_property(
        properties,
        "Filter Pushdown",
        "storage-level exact per-segment bitmap (no residual FILTER)".to_string(),
    );
}

fn push_dense_search_filter_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::VectorSearchSpec,
) {
    push_search_filter_properties(properties, spec.predicate.as_ref(), spec.table.as_ref());
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

    let policy = spec
        .table
        .storage
        .as_ref()
        .and_then(|storage| storage.vector_search_policy(spec.column_id as u32, spec.distance));
    let strategy = match (has_runtime_parameters, policy, spec.estimated_filter_rows) {
        (true, Some(policy), _) => format!(
            "runtime exact bitmap: exact scan at <= {}; otherwise unfiltered HNSW navigation + exact-bitmap filtered Top-K + predicate-local refinement (exact fallback if underfilled)",
            policy.filtered_plain_scan_threshold
        ),
        (true, None, _) => {
            "runtime exact bitmap chooses exact scan or unfiltered-navigation filtered Top-K"
                .to_string()
        }
        (false, Some(policy), Some(rows))
            if rows <= policy.filtered_plain_scan_threshold as u64 =>
        {
            format!(
                "exact filtered distance scan ({} <= threshold {})",
                rows, policy.filtered_plain_scan_threshold
            )
        }
        (false, Some(policy), Some(rows)) => format!(
            "unfiltered HNSW navigation + exact-bitmap filtered Top-K + predicate-local refinement ({} > exact threshold {}; exact fallback if underfilled)",
            rows, policy.filtered_plain_scan_threshold
        ),
        _ => "runtime exact bitmap chooses exact scan or unfiltered-navigation filtered Top-K"
            .to_string(),
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
    push_search_filter_properties(properties, spec.predicate.as_ref(), spec.table.as_ref());
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
    push_search_filter_properties(properties, spec.predicate.as_ref(), spec.table.as_ref());
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
    properties.push(ExplainProperty::new(label, ExplainValue::String(value)));
}

fn push_list_property(
    properties: &mut Vec<ExplainProperty>,
    label: &'static str,
    values: &[String],
) {
    properties.push(ExplainProperty::new(
        label,
        ExplainValue::List(values.iter().cloned().map(ExplainValue::String).collect()),
    ));
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

fn format_output_schema(node: &PhysicalPlanNode, names: &[String]) -> String {
    if node.output.column_count() == 0 {
        return "(none)".to_string();
    }
    names
        .iter()
        .zip(node.output.types.iter())
        .map(|(name, ty)| format!("{name} {ty}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_join_condition(
    condition: &JoinCondition,
    left_names: &[String],
    right_names: &[String],
) -> String {
    let mut left = ExplainExpressionFormatter::new(left_names).format(&condition.left);
    let mut right = ExplainExpressionFormatter::new(right_names).format(&condition.right);
    if left.starts_with("delim_") && !is_internal_column_name(&right) {
        left = right.clone();
    } else if right.starts_with("delim_") && !is_internal_column_name(&left) {
        right = left.clone();
    }
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

fn aggregate_output_names(spec: &AggregateSpec, input_names: &[String]) -> Option<Vec<String>> {
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
        format!("grouping({arguments})")
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
            .join(", ")
    }

    fn format(&self, expression: &Expression) -> String {
        match expression {
            Expression::Reference(reference) => self.column(reference.index),
            Expression::ColumnRef(column) => self.column(column.binding.column_index),
            Expression::Constant(constant) => constant.value.to_string(),
            Expression::Parameter(parameter) => format!("${}", parameter.slot.index.index() + 1),
            Expression::Comparison(comparison) => format!(
                "{} {} {}",
                self.format(&comparison.left),
                comparison.comparison_type,
                self.format(&comparison.right)
            ),
            Expression::Conjunction(conjunction) => {
                let separator = match conjunction.conjunction_type {
                    paro_planner::expression::ConjunctionType::And => " AND ",
                    paro_planner::expression::ConjunctionType::Or => " OR ",
                };
                conjunction
                    .children
                    .iter()
                    .map(|child| self.format(child))
                    .collect::<Vec<_>>()
                    .join(separator)
            }
            Expression::Cast(cast) => {
                let cast_name = if cast.try_cast { "TRY_CAST" } else { "CAST" };
                format!(
                    "{cast_name}({} AS {})",
                    self.format(&cast.child),
                    cast.target_type
                )
            }
            Expression::Function(function) => format!(
                "{}({})",
                function.function.name.as_str(),
                function
                    .children
                    .iter()
                    .map(|child| self.format(child))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Expression::Aggregate(aggregate) => {
                format_bound_aggregate(aggregate, &|child| self.format(child))
            }
            Expression::Case(case_expression) => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.format(&case_expression.check),
                self.format(&case_expression.result_if_true),
                self.format(&case_expression.result_if_false)
            ),
            Expression::Operator(operator) => self.format_operator(operator),
            Expression::Subquery(subquery) => {
                let kind = match subquery.subquery_type {
                    paro_planner::expression::SubqueryType::Scalar => "SUBQUERY",
                    paro_planner::expression::SubqueryType::Exists => "EXISTS SUBQUERY",
                    paro_planner::expression::SubqueryType::NotExists => "NOT EXISTS SUBQUERY",
                    paro_planner::expression::SubqueryType::Any => "ANY SUBQUERY",
                    paro_planner::expression::SubqueryType::All => "ALL SUBQUERY",
                };
                if subquery.children.is_empty() {
                    format!("<{kind}>")
                } else {
                    format!(
                        "{} {} <{kind}>",
                        subquery
                            .children
                            .iter()
                            .map(|child| self.format(child))
                            .collect::<Vec<_>>()
                            .join(", "),
                        subquery.comparison_type
                    )
                }
            }
            Expression::Window(window) => self.format_window(window),
        }
    }

    fn format_operator(&self, operator: &paro_planner::expression::OperatorExpression) -> String {
        let children = || {
            operator
                .children
                .iter()
                .map(|child| self.format(child))
                .collect::<Vec<_>>()
        };
        match operator.operator_type {
            OperatorType::In | OperatorType::NotIn if operator.children.len() >= 2 => format!(
                "{} {}IN ({})",
                self.format(&operator.children[0]),
                if operator.operator_type == OperatorType::NotIn {
                    "NOT "
                } else {
                    ""
                },
                operator.children[1..]
                    .iter()
                    .map(|child| self.format(child))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            OperatorType::Not => operator
                .children
                .first()
                .map(|child| format!("NOT {}", self.format(child)))
                .unwrap_or_else(|| "NOT".to_string()),
            OperatorType::IsNull => operator
                .children
                .first()
                .map(|child| format!("{} IS NULL", self.format(child)))
                .unwrap_or_else(|| "IS NULL".to_string()),
            OperatorType::IsNotNull => operator
                .children
                .first()
                .map(|child| format!("{} IS NOT NULL", self.format(child)))
                .unwrap_or_else(|| "IS NOT NULL".to_string()),
            OperatorType::Coalesce => format!("COALESCE({})", children().join(", ")),
            OperatorType::Like | OperatorType::ILike if operator.children.len() == 2 => format!(
                "{} {} {}",
                self.format(&operator.children[0]),
                if operator.operator_type == OperatorType::Like {
                    "LIKE"
                } else {
                    "ILIKE"
                },
                self.format(&operator.children[1])
            ),
            OperatorType::ArrayConstructor => format!("[{}]", children().join(", ")),
            OperatorType::StructConstructor => format!("({})", children().join(", ")),
            OperatorType::ArrayExtract if operator.children.len() == 2 => format!(
                "{}[{}]",
                self.format(&operator.children[0]),
                self.format(&operator.children[1])
            ),
            OperatorType::ErrorIfMultipleRows => {
                format!("error_if_multiple_rows({})", children().join(", "))
            }
            _ => format!("{:?}({})", operator.operator_type, children().join(", ")),
        }
    }

    fn format_window(&self, window: &paro_planner::expression::WindowExpression) -> String {
        let mut rendered = format!(
            "{}({}) OVER (",
            window.function_name(),
            window
                .arguments()
                .iter()
                .map(|argument| self.format(argument))
                .collect::<Vec<_>>()
                .join(", ")
        );
        if !window.partitions.is_empty() {
            rendered.push_str("PARTITION BY ");
            rendered.push_str(
                &window
                    .partitions
                    .iter()
                    .map(|partition| self.format(partition))
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
                            self.format(&order.expression),
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
        rendered.push_str(
            &self.format_window_bound(&window.frame.start_bound, window.frame.start_is_preceding),
        );
        rendered.push_str(" AND ");
        rendered.push_str(
            &self.format_window_bound(&window.frame.end_bound, window.frame.end_is_preceding),
        );
        rendered.push(')');
        rendered
    }

    fn format_window_bound(&self, bound: &WindowFrameBound, preceding: bool) -> String {
        match bound {
            WindowFrameBound::Unbounded => format!(
                "UNBOUNDED {}",
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Offset(offset) => format!(
                "{} {}",
                self.format(offset),
                if preceding { "PRECEDING" } else { "FOLLOWING" }
            ),
        }
    }
}

fn table_column_name(table: &TableCatalogEntry, column_id: usize) -> String {
    table
        .columns
        .get(column_id)
        .map(|column| column.name.clone())
        .unwrap_or_else(|| format!("<column {column_id}>"))
}

fn format_predicate_tree(predicate: &PredicateTree, table: &TableCatalogEntry) -> String {
    match predicate {
        PredicateTree::Leaf(predicate) => format_predicate(predicate, table),
        PredicateTree::And(children) => children
            .iter()
            .map(|child| format_predicate_tree(child, table))
            .collect::<Vec<_>>()
            .join(" AND "),
        PredicateTree::Or(children) => children
            .iter()
            .map(|child| format_predicate_tree(child, table))
            .collect::<Vec<_>>()
            .join(" OR "),
    }
}

fn format_predicate(predicate: &Predicate, table: &TableCatalogEntry) -> String {
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
    use crate::physical::specs::SearchPredicateTemplate;

    match predicate {
        SearchPredicateTemplate::Bound(predicate) => format_predicate_tree(predicate, table),
        SearchPredicateTemplate::ParameterComparison {
            column_id,
            comparison,
            slot,
            ..
        } => format!(
            "{} {comparison} ${}",
            table_column_name(table, *column_id as usize),
            slot.index.index() + 1
        ),
        SearchPredicateTemplate::And(children) => format!(
            "({})",
            children
                .iter()
                .map(|child| format_search_predicate(child, table))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        SearchPredicateTemplate::Or(children) => format!(
            "({})",
            children
                .iter()
                .map(|child| format_search_predicate(child, table))
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
    }
}

fn is_internal_column_name(name: &str) -> bool {
    if name.starts_with("__") {
        return true;
    }
    [
        "col", "expr_", "ref_", "aggr_", "group_", "window_", "delim_",
    ]
    .iter()
    .any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

fn qualify_column_name(qualifier: &str, name: String) -> String {
    if name.contains('.')
        || name
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || character == '_'))
    {
        name
    } else {
        format!("{qualifier}.{name}")
    }
}
