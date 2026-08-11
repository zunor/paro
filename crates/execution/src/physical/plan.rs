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
use paro_catalog::entry::StandardEntry;
use paro_planner::expression::{AggregateType, Expression};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition};
use paro_planner::operator::ExplainSpec;
use paro_planner::plan::CardinalityEstimate;
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
            output_names: node.output.names.to_vec(),
            estimated_cardinality: explain_cardinality(&node.kind, node.cardinality),
            actual: None,
            properties: collect_explain_properties(node),
            children: self
                .child_ids(&node.children)
                .iter()
                .map(|child| self.explain_node(*child))
                .collect(),
        }
    }
}

fn collect_explain_properties(node: &PhysicalPlanNode) -> Vec<ExplainProperty> {
    let mut properties = Vec::new();

    match &node.kind {
        PhysicalNodeKind::Project(spec) => {
            push_list_property(&mut properties, "Output", &spec.output_names);
        }
        PhysicalNodeKind::RowsetScan(spec) => {
            if !spec.output_names.is_empty() {
                push_list_property(&mut properties, "Columns", &spec.output_names);
            }
            if let Some(predicate) = &spec.predicate {
                push_string_property(&mut properties, "Pushed Predicate", predicate.to_string());
            }
            if spec.late_materialize {
                push_string_property(&mut properties, "Late Materialize", "auto".to_string());
            }
            if !spec.residual_predicates.is_empty() {
                push_string_property(
                    &mut properties,
                    "Residual Predicate",
                    spec.residual_predicates
                        .iter()
                        .map(format_expr)
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
                        .map(format_expr)
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
            if let Some(order) = &spec.scan_order {
                push_string_property(&mut properties, "Scan Order", format_segment_order(order));
            }
        }
        PhysicalNodeKind::Filter(spec) => {
            if !spec.expressions.is_empty() {
                push_string_property(
                    &mut properties,
                    "Filter",
                    spec.expressions
                        .iter()
                        .map(format_expr)
                        .collect::<Vec<_>>()
                        .join(" AND "),
                );
            }
        }
        PhysicalNodeKind::Sort(spec) => {
            if !spec.orders.is_empty() {
                push_string_property(&mut properties, "Sort Key", format_order_by(&spec.orders));
            }
        }
        PhysicalNodeKind::TopN(spec) => {
            if !spec.orders.is_empty() {
                push_string_property(&mut properties, "Sort Key", format_order_by(&spec.orders));
            }
            push_string_property(&mut properties, "Limit", spec.limit.to_string());
        }
        PhysicalNodeKind::VectorSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_string_property(&mut properties, "Column", spec.column_id.to_string());
            push_string_property(&mut properties, "Limit", spec.k.to_string());
        }
        PhysicalNodeKind::SparseVectorSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_string_property(&mut properties, "Column", spec.column_id.to_string());
            push_string_property(&mut properties, "Limit", spec.k.to_string());
        }
        PhysicalNodeKind::FullTextSearch(spec) => {
            push_search_token_properties(&mut properties, &spec.capability_token);
            push_string_property(&mut properties, "Column", spec.column_id.to_string());
            push_string_property(&mut properties, "Mode", format!("{:?}", spec.mode));
        }
        PhysicalNodeKind::AdaptiveSearch(spec) => {
            push_string_property(&mut properties, "Strategy", "adaptive".to_string());
            push_search_token_properties(
                &mut properties,
                search_source_token(spec.selected.as_ref()),
            );
        }
        PhysicalNodeKind::Limit(spec) => {
            if let Some(limit) = &spec.limit {
                push_string_property(&mut properties, "Limit", format_expr(limit));
            }
            if let Some(offset) = &spec.offset {
                push_string_property(&mut properties, "Offset", format_expr(offset));
            }
        }
        PhysicalNodeKind::HashJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_join_conditions(&mut properties, &spec.key_conditions);
            push_join_conditions(&mut properties, &spec.residual_conditions);
        }
        PhysicalNodeKind::NestedLoopJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            if let Some(strategy) = nested_loop_strategy(spec) {
                push_string_property(&mut properties, "Strategy", strategy.to_string());
            }
            push_join_conditions(&mut properties, &spec.conditions);
            if let Some(condition) = &spec.arbitrary_condition {
                push_string_property(&mut properties, "Join Filter", format_expr(condition));
            }
        }
        PhysicalNodeKind::SortRangeJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_string_property(&mut properties, "Strategy", "sort_range".to_string());
            push_join_conditions(&mut properties, &spec.conditions);
        }
        PhysicalNodeKind::ClassicIeJoin(spec) => {
            push_string_property(&mut properties, "Join Type", spec.join_type.to_string());
            push_string_property(&mut properties, "Strategy", "classic_ie_join".to_string());
            push_join_conditions(&mut properties, &spec.conditions);
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
            push_list_property(&mut properties, "Output", &spec.output_names);
            if !spec.filters.is_empty() {
                push_string_property(&mut properties, "Filter", "<pushed down>".to_string());
            }
        }
        PhysicalNodeKind::Aggregate(spec) => {
            push_aggregate_properties(&mut properties, spec);
            if !spec.output_names.is_empty() {
                push_list_property(&mut properties, "Output", &spec.output_names);
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

fn push_aggregate_properties(properties: &mut Vec<ExplainProperty>, spec: &AggregateSpec) {
    if spec.grouping_key_count > 0 {
        push_string_property(
            properties,
            "Group Key",
            spec.groups
                .iter()
                .map(|expr| format_payload_expr(expr, spec))
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
                .map(|expr| format_aggregate_expr(expr, spec))
                .collect::<Vec<_>>()
                .join(", "),
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
                            .map(|idx| idx.to_string())
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

fn push_join_conditions(properties: &mut Vec<ExplainProperty>, conditions: &[JoinCondition]) {
    if conditions.is_empty() {
        return;
    }
    properties.push(ExplainProperty::new(
        "Join Condition",
        ExplainValue::List(
            conditions
                .iter()
                .map(|condition| ExplainValue::String(format_join_condition(condition)))
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

fn format_order_by(orders: &[paro_planner::binder::ir::OrderByNode]) -> String {
    orders
        .iter()
        .map(|order| {
            format!(
                "{} {} NULLS {}",
                format_expr(&order.expression),
                if order.ascending { "ASC" } else { "DESC" },
                if order.nulls_first { "FIRST" } else { "LAST" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_segment_order(order: &SegmentOrderOptions) -> String {
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
    format!(
        "col#{} {direction} by {stat}{limit}{offset}",
        order.column_idx
    )
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
        .names
        .iter()
        .zip(node.output.types.iter())
        .map(|(name, ty)| format!("{name} {ty}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_join_condition(condition: &JoinCondition) -> String {
    format!(
        "{} {} {}",
        format_expr(&condition.left),
        join_comparison_symbol(condition.comparison),
        format_expr(&condition.right)
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

fn format_payload_expr(expr: &Expression, spec: &AggregateSpec) -> String {
    if let Expression::Reference(reference) = expr {
        if let Some(payload) = spec.projection_exprs.get(reference.index) {
            return format_expr(payload);
        }
    }
    format_expr(expr)
}

fn format_aggregate_expr(expr: &Expression, spec: &AggregateSpec) -> String {
    let Expression::Aggregate(aggregate) = expr else {
        return format_expr(expr);
    };
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
            .map(|child| format_payload_expr(child, spec))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut rendered = format!("{}({distinct}{args})", aggregate.function.name);
    if let Some(filter) = aggregate.filter.as_ref() {
        rendered.push_str(" FILTER (WHERE ");
        rendered.push_str(&format_payload_expr(filter, spec));
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
                        format_payload_expr(&order.expression, spec),
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

fn format_expr(expr: &Expression) -> String {
    match expr {
        Expression::Reference(reference) => format!("#{}", reference.index),
        Expression::ColumnRef(column_ref) => format!("#{}", column_ref.binding.column_index),
        Expression::Constant(constant) => constant.value.to_string(),
        Expression::Comparison(comparison) => format!(
            "{} {} {}",
            format_expr(&comparison.left),
            comparison.comparison_type,
            format_expr(&comparison.right)
        ),
        Expression::Conjunction(conjunction) => {
            let separator = match conjunction.conjunction_type {
                paro_planner::expression::ConjunctionType::And => " AND ",
                paro_planner::expression::ConjunctionType::Or => " OR ",
            };
            conjunction
                .children
                .iter()
                .map(format_expr)
                .collect::<Vec<_>>()
                .join(separator)
        }
        Expression::Cast(cast) => {
            let cast_name = if cast.try_cast { "TRY_CAST" } else { "CAST" };
            format!(
                "{cast_name}({} AS {})",
                format_expr(&cast.child),
                cast.target_type
            )
        }
        Expression::Function(function) => {
            format!(
                "{}({})",
                function.function.name.as_str(),
                function
                    .children
                    .iter()
                    .map(format_expr)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        Expression::Aggregate(aggregate) => {
            format!(
                "{}({})",
                aggregate.function.name,
                aggregate
                    .children
                    .iter()
                    .map(format_expr)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        Expression::Operator(operator) => match operator.operator_type {
            paro_planner::expression::OperatorType::IsNull => operator
                .children
                .first()
                .map(|child| format!("{} IS NULL", format_expr(child)))
                .unwrap_or_else(|| "IS NULL".to_string()),
            paro_planner::expression::OperatorType::IsNotNull => operator
                .children
                .first()
                .map(|child| format!("{} IS NOT NULL", format_expr(child)))
                .unwrap_or_else(|| "IS NOT NULL".to_string()),
            paro_planner::expression::OperatorType::In if operator.children.len() >= 2 => {
                format!(
                    "{} IN ({})",
                    format_expr(&operator.children[0]),
                    operator.children[1..]
                        .iter()
                        .map(format_expr)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            _ => format!("{expr:?}"),
        },
        _ => format!("{expr:?}"),
    }
}
