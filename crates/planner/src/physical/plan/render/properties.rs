// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Operator-specific display properties.

use super::super::PhysicalPlan;
use super::expression::*;
use crate::logical::operator::join::{JoinComparisonType, JoinCondition};
use crate::logical::plan::CardinalityEstimate;
use crate::physical::explain::types::{ExplainProperty, ExplainValue};
use crate::physical::ids::PhysicalPlanNodeId;
use crate::physical::node::PhysicalPlanNode;
use crate::physical::specs::{
    AggregateSpec, NestedLoopJoinSpec, PhysicalNodeKind, SearchSourceSpec,
};
use paro_catalog::entry::{StandardEntry, TableCatalogEntry};
use paro_storage::search::CapabilityToken;
use paro_storage::table::segment_reorderer::{
    OrderByStatistics, SegmentOrderOptions, SegmentOrderType,
};
pub(in crate::physical::plan) fn collect_explain_properties(
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
                            .is_some_and(crate::physical::row_type::ColumnIdentity::is_internal)
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
            let column_ids = spec
                .column_projection
                .columns()
                .iter()
                .map(|column| column.to_string())
                .collect::<Vec<_>>();
            if !column_ids.is_empty() {
                push_list_property(&mut properties, "Column IDs", &column_ids);
            }
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
            if spec.offset != 0 {
                push_string_property(&mut properties, "Offset", spec.offset.to_string());
            }
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
            if let Some(runtime_filter) = &spec.runtime_filter {
                push_string_property(
                    &mut properties,
                    "Runtime Filter",
                    format!(
                        "wait_complete capability={:?} peak={}B builders={} artifact={:?}",
                        runtime_filter.resource.capability,
                        runtime_filter.resource.peak_memory_bytes,
                        runtime_filter.resource.max_local_builders,
                        runtime_filter.artifact
                    ),
                );
            }
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
                expand_direction_name(spec.direction).to_string(),
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
                expand_direction_name(spec.direction).to_string(),
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
            let visible_output_count = spec
                .projection
                .as_ref()
                .map_or(output_names.len(), |projection| projection.visible_count);
            let visible_outputs = &output_names[..visible_output_count.min(output_names.len())];
            if !visible_outputs.is_empty() {
                push_list_property(&mut properties, "Output", visible_outputs);
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
        if spec.initial_lookup_hash_key_count < spec.grouping_key_count {
            push_string_property(
                properties,
                "Initial Lookup Hash Key",
                spec.groups[..spec.initial_lookup_hash_key_count]
                    .iter()
                    .map(|expression| format_payload_expr(expression, spec, &formatter))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
    }
    let aggregate_names = spec
        .aggregates
        .iter()
        .map(|expression| format_aggregate_expr(expression, spec, &formatter))
        .collect::<Vec<_>>();
    if !aggregate_names.is_empty() {
        push_string_property(properties, "Aggregates", aggregate_names.join(", "));
    }
    // Emit-time HAVING references aggregate results, not the output layout
    // (which also contains grouping keys and may be independently projected).
    let result_formatter = ExplainExpressionFormatter::new(&aggregate_names);
    if !spec.having_filter.is_empty() {
        push_string_property(
            properties,
            "Having",
            spec.having_filter
                .iter()
                .map(|expression| result_formatter.format(expression))
                .collect::<Vec<_>>()
                .join(" AND "),
        );
    }
    if let Some(reduction) = &spec.post_reduction {
        push_string_property(
            properties,
            "Post Reduction",
            reduction
                .reducers
                .iter()
                .map(|expression| result_formatter.format(expression))
                .collect::<Vec<_>>()
                .join(", "),
        );
        push_string_property(
            properties,
            "Post Predicate",
            result_formatter.format(&reduction.predicate),
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

fn expand_direction_name(direction: crate::logical::operator::ExpandDirection) -> &'static str {
    match direction {
        crate::logical::operator::ExpandDirection::Forward => "forward",
        crate::logical::operator::ExpandDirection::Backward => "backward",
        crate::logical::operator::ExpandDirection::Both => "both",
    }
}

fn search_capability_state_name(
    state: &paro_storage::search::SearchCapabilityState,
) -> &'static str {
    match state {
        paro_storage::search::SearchCapabilityState::Queryable => "queryable",
        paro_storage::search::SearchCapabilityState::NotQueryable { reason } => match reason {
            paro_storage::search::SearchNotQueryableReason::CoverageIncomplete => {
                "not_queryable:coverage_incomplete"
            }
            paro_storage::search::SearchNotQueryableReason::TailOverBudget => {
                "not_queryable:tail_over_budget"
            }
            paro_storage::search::SearchNotQueryableReason::FreshnessRequired => {
                "not_queryable:freshness_required"
            }
            paro_storage::search::SearchNotQueryableReason::ProviderDisabled => {
                "not_queryable:provider_disabled"
            }
        },
    }
}

fn search_request_mode_name(mode: &paro_storage::search::SearchRequestMode) -> String {
    match mode {
        paro_storage::search::SearchRequestMode::Filter => "filter".to_string(),
        paro_storage::search::SearchRequestMode::TopK { limit } => format!("top_k:{limit}"),
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
        search_capability_state_name(&token.capability_state).to_string(),
    );
}

fn push_search_filter_properties(
    properties: &mut Vec<ExplainProperty>,
    predicate: Option<&crate::physical::specs::SearchPredicateTemplate>,
    contract: crate::physical::specs::SearchFilterContract,
    materialization: Option<paro_storage::search::ExactFilterMaterialization>,
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
            crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual,
            Some(paro_storage::search::ExactFilterMaterialization::ScalarIndex),
        ) => "exact segment row set from scalar index; ordinal admission and posting scan; no residual filter",
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual,
            Some(paro_storage::search::ExactFilterMaterialization::ColumnScan),
        ) => "exact segment row set materialized by column scan; no residual filter",
        (
            crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual,
            Some(paro_storage::search::ExactFilterMaterialization::Mixed {
                indexed_rows,
                scanned_rows,
            }),
        ) => {
            push_string_property(
                properties,
                "Filter Row-Set Coverage",
                format!("indexed rows {indexed_rows}; column-scan rows {scanned_rows}"),
            );
            "exact segment row set from mixed scalar indexes and column scans; no residual filter"
        }
        (crate::physical::specs::SearchFilterContract::ExactSegmentRowSetNoResidual, None) => {
            "exact segment row set; materialization unknown; no residual filter"
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
        spec.filter_materialization,
        spec.table.as_ref(),
    );
    if spec.params.objective == paro_storage::index::hnsw::HnswSearchObjective::Exact {
        if spec.predicate.is_some() {
            push_string_property(
                properties,
                "Filtered Strategy",
                "query objective forces exact row-set distance scan; HNSW navigation, predicate topology, and adaptive refinement are disabled"
                    .to_string(),
            );
        }
        return;
    }
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

    let predicate_columns = paro_storage::index::collect_predicate_columns(
        spec.predicate
            .as_ref()
            .expect("predicate checked above")
            .tree(),
    );
    let accelerated_columns = spec
        .filter_topology
        .columns()
        .iter()
        .copied()
        .filter(|column| predicate_columns.contains(column))
        .map(|column| table_column_name(spec.table.as_ref(), column as usize))
        .collect::<Vec<_>>();
    // Runtime prepares exact segment row sets and lowers every immutable
    // artifact from its actual physical scan workload. EXPLAIN describes that
    // contract rather than pretending a plan-time selectivity estimate fixes
    // the executed path.
    let exact_scan = if accelerated_columns.is_empty() {
        "exact row-set distance scan"
    } else {
        "exact row-set distance scan using scalar-block covering vector ranges for compatible ordinal predicates"
    };
    let strategy = format!(
        "runtime cost choice per immutable artifact: {exact_scan} is compared with graph work using definition-pinned physical cost coefficients, effective ef, and exact covering/base row counts; graph execution uses predicate-agnostic HNSW navigation with deferred global-beam admission for broad predicates, exact eager admission and hierarchical scalar-block topology for selective predicates, observed two-hop repair, and exact fallback"
    );
    push_string_property(properties, "Filtered Strategy", strategy);
    if !accelerated_columns.is_empty() {
        push_string_property(
            properties,
            "Exact Filter Scan",
            format!(
                "covering scalar-block vector ranges available for compatible single-column ordinal predicates on {}; base vector pages are not gathered",
                accelerated_columns.join(", ")
            ),
        );
        push_string_property(
            properties,
            "Predicate Topology",
            format!(
                "available on {}: tagged hierarchical scalar-block HNSW plus bounded vector-aware cross-block routing; every admitted block has a durable entry point and exact row-set admission",
                accelerated_columns.join(", ")
            ),
        );
    }
}

fn push_vector_search_properties(
    properties: &mut Vec<ExplainProperty>,
    spec: &crate::physical::specs::VectorSearchSpec,
) {
    let widths =
        spec.search_policy
            .effective_widths(spec.k, spec.params.ef, spec.params.rerank_window);
    push_string_property(properties, "Search Candidate", "dense vector".to_string());
    push_string_property(
        properties,
        "Column",
        table_column_name(spec.table.as_ref(), spec.column_id),
    );
    push_string_property(properties, "Limit", spec.k.to_string());
    push_string_property(
        properties,
        "Search Objective",
        match spec.params.objective {
            paro_storage::index::hnsw::HnswSearchObjective::CostOptimized => {
                "cost_optimized: definition-pinned cost selects exact scoring or graph traversal"
                    .to_string()
            }
            paro_storage::index::hnsw::HnswSearchObjective::Exact => {
                "exact: score every admitted vector; graph traversal disabled".to_string()
            }
        },
    );
    push_string_property(
        properties,
        "Search Ef",
        format!(
            "{} (effective {} per graph shard)",
            spec.params
                .ef
                .map_or_else(|| "default".to_string(), |ef| ef.to_string()),
            widths.ef
        ),
    );
    push_string_property(
        properties,
        "Graph Shards",
        format!(
            "{} (estimated aggregate beam width {})",
            spec.graph_shard_count,
            widths.ef.saturating_mul(spec.graph_shard_count),
        ),
    );
    push_string_property(
        properties,
        "Exact Rerank Window",
        format!(
            "{} (effective {}, definition policy {})",
            spec.params
                .rerank_window
                .map_or_else(|| "default".to_string(), |window| window.to_string()),
            widths.rerank_window,
            spec.search_policy.rerank_policy,
        ),
    );
    push_string_property(
        properties,
        "Exact/Graph Cost Profile",
        format!(
            "random-access={} units, exact-f32={} units/dimension, sequential={} units/dimension, symmetric-i16={} units/dimension, graph={} unique scores/ef, routing={} (source={}, definition-pinned); observed generation average level-0 degree={:.2} is descriptive, not a cost cap",
            spec.search_policy.distance_cost.random_access_cost_units,
            spec.search_policy.distance_cost.exact_f32_dimension_cost_units,
            spec.search_policy.distance_cost.sequential_dimension_cost_units,
            spec.search_policy.distance_cost.symmetric_i16_dimension_cost_units,
            spec.search_policy.distance_cost.graph_scored_points_per_ef,
            spec.search_policy.vector_encoding,
            spec.search_policy.distance_cost.source,
            spec.avg_level0_degree,
        ),
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
        spec.filter_materialization,
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
    push_string_property(properties, "Mode", search_request_mode_name(&spec.mode));
    push_search_filter_properties(
        properties,
        spec.predicate.as_ref(),
        spec.filter_contract,
        spec.filter_materialization,
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

pub(super) fn explain_operator_name(kind: &PhysicalNodeKind) -> &'static str {
    match kind {
        PhysicalNodeKind::Project(_) => "PROJECTION",
        PhysicalNodeKind::Sort(_) => "ORDER_BY",
        PhysicalNodeKind::Aggregate(_) => "AGGREGATE",
        _ => kind.name(),
    }
}

pub(super) fn explain_relation_name(kind: &PhysicalNodeKind) -> Option<String> {
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

pub(super) fn explain_relation_alias(kind: &PhysicalNodeKind) -> Option<String> {
    match kind {
        PhysicalNodeKind::RowsetScan(spec) => spec.relation_alias.clone(),
        PhysicalNodeKind::Values(spec) => spec.relation_alias.clone(),
        _ => None,
    }
}

pub(super) fn explain_cardinality(
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
            .estimated_total_rows
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
    let visible_width = match &node.kind {
        PhysicalNodeKind::RowFetch(spec) => spec
            .projection
            .as_ref()
            .map_or(node.output.column_count(), |projection| {
                projection.visible_count
            }),
        _ => node.output.column_count(),
    };
    node.output
        .identities
        .iter()
        .enumerate()
        .zip(node.output.types.iter())
        .take(visible_width)
        .map(|((ordinal, identity), ty)| {
            let name = match identity {
                crate::physical::row_type::ColumnIdentity::Visible {
                    name,
                    qualifier: Some(qualifier),
                } => qualifier
                    .iter()
                    .map(|part| format_schema_identifier(part))
                    .chain(std::iter::once(format_schema_identifier(name)))
                    .collect::<Vec<_>>()
                    .join("."),
                crate::physical::row_type::ColumnIdentity::Visible {
                    name,
                    qualifier: None,
                } => format_schema_identifier(name),
                crate::physical::row_type::ColumnIdentity::Internal => {
                    format_schema_identifier(&format!("__internal_{}", ordinal + 1))
                }
                crate::physical::row_type::ColumnIdentity::InternalNamed(name) => {
                    format_schema_identifier(name)
                }
                crate::physical::row_type::ColumnIdentity::Locator { .. } => {
                    format_schema_identifier("rowid")
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

fn format_cte_materialization(materialized: crate::binder::ir::CTEMaterialize) -> &'static str {
    match materialized {
        crate::binder::ir::CTEMaterialize::Default
        | crate::binder::ir::CTEMaterialize::Materialized => "MATERIALIZED",
        crate::binder::ir::CTEMaterialize::NotMaterialized => "NOT MATERIALIZED",
    }
}

pub(super) fn aggregate_scope_names(
    spec: &AggregateSpec,
    input_names: &[String],
) -> Option<Vec<String>> {
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
