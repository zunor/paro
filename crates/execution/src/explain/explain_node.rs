use std::collections::HashMap;

use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{
    AggregateType, ComparisonType, ConjunctionType, Expression, OperatorExpression, OperatorType,
    OrderByExpression, WindowExpression,
};
use paro_planner::operator::join::{JoinComparisonType, JoinCondition};
use paro_planner::operator::{ExplainMode, ExplainSpec};
use paro_storage::index::{Predicate, PredicateTree};
use serde_json::to_string_pretty;

use crate::explain::annotated_operator::ExplainAnnotatedOperator;
use crate::explain::types::{
    ExplainActualStats, ExplainDoc, ExplainLogicalInfo, ExplainNode, ExplainProperty,
    ExplainRuntimeStats, ExplainSchema, ExplainValue,
};
use crate::operator::aggregate::hash_aggregate::HashAggregate;
use crate::operator::aggregate::perfect_hash_aggregate::PerfectHashAggregate;
use crate::operator::aggregate::ungrouped_aggregate::UngroupedAggregate;
use crate::operator::filter::Filter;
use crate::operator::helper::order::Order;
use crate::operator::helper::streaming_limit::StreamingLimit;
use crate::operator::helper::topn::TopN;
use crate::operator::join::hash_join::operator::HashJoin;
use crate::operator::join::iejoin::IEJoin;
use crate::operator::join::nested_loop_join::NestedLoopJoin;
use crate::operator::join::piecewise_merge_join::PiecewiseMergeJoin;
use crate::operator::projection::Projection;
use crate::operator::scan::rowset_scan::PhysicalRowsetScan;
use crate::operator::set::cte::{CteScan, CTE as PhysicalCTE};
use crate::operator::set::recursive_cte::RecursiveCTE as PhysicalRecursiveCTE;
use crate::operator::window::window_operator::Window;
use crate::operator::PhysicalOperator;

#[derive(Debug, Clone, Default)]
pub struct ExplainContext {
    pub input_names: Vec<String>,
    pub left_names: Vec<String>,
    pub right_names: Vec<String>,
    pub scan_columns: HashMap<u64, String>,
}

impl ExplainContext {
    pub fn input(input_names: Vec<String>) -> Self {
        Self {
            input_names,
            ..Self::default()
        }
    }

    pub fn join(left_names: Vec<String>, right_names: Vec<String>) -> Self {
        Self {
            left_names,
            right_names,
            ..Self::default()
        }
    }

    pub fn scan(column_names: &[String]) -> Self {
        let scan_columns = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (idx as u64, name.clone()))
            .collect();
        Self {
            input_names: column_names.to_vec(),
            scan_columns,
            ..Self::default()
        }
    }
}

/// Render a physical plan tree into PostgreSQL-style text lines.
pub fn explain_physical_plan(plan: &dyn PhysicalOperator, analyze: bool) -> Vec<String> {
    let spec = if analyze {
        ExplainSpec::text_analyze()
    } else {
        ExplainSpec::text_plan()
    };
    let doc = build_explain_doc(plan, spec);
    render_explain_text_lines(&doc)
}

pub(crate) fn build_explain_doc(plan: &dyn PhysicalOperator, spec: ExplainSpec) -> ExplainDoc {
    ExplainDoc {
        format_version: 1,
        spec,
        root: build_explain_node(plan, &spec),
        summary: Vec::new(),
    }
}

pub(crate) fn render_explain_json_string(doc: &ExplainDoc) -> String {
    to_string_pretty(&doc.to_json()).expect("ExplainDoc JSON serialization should not fail")
}

pub(crate) fn render_explain_text_lines(doc: &ExplainDoc) -> Vec<String> {
    // Do not prepend "QUERY PLAN" here: result columns are already named QUERY PLAN,
    // and clients print that as the header (PostgreSQL/psql-style).
    let mut lines = vec![doc.root.header_text(&doc.spec)];
    write_properties_and_children(&doc.root, 0, &doc.spec, &mut lines);
    if doc.spec.detail.summary {
        for property in &doc.summary {
            lines.push(property.text_line());
        }
    }
    lines
}

fn write_properties_and_children(
    node: &ExplainNode,
    base_indent: usize,
    spec: &ExplainSpec,
    lines: &mut Vec<String>,
) {
    let property_indent = base_indent + 2;
    for property in &node.properties {
        lines.push(format!(
            "{}{}",
            " ".repeat(property_indent),
            property.text_line()
        ));
    }
    for child in &node.children {
        let header_indent = " ".repeat(base_indent + 2);
        lines.push(format!("{header_indent}->  {}", child.header_text(spec)));
        write_properties_and_children(child, base_indent + 6, spec, lines);
    }
}

fn build_explain_node(op: &dyn PhysicalOperator, spec: &ExplainSpec) -> ExplainNode {
    let base = unwrap_explain_operator(op);
    let logical = logical_info_for_operator(op);
    let schema = schema_for_operator(op);
    let children = (0..op.children_count())
        .filter_map(|idx| op.child(idx))
        .map(|child| build_explain_node(child, spec))
        .collect::<Vec<_>>();

    let mut node = ExplainNode {
        node_id: op.explain_node_id(),
        operator_name: op.explain_name(),
        relation_name: schema.relation_name.clone(),
        relation_alias: schema.relation_alias.clone(),
        output_names: Vec::new(),
        estimated_cardinality: estimated_cardinality(op, &logical),
        actual: actual_stats(op, base, spec),
        properties: build_properties(base, &schema, &children, spec, &logical),
        children,
    };
    node.output_names = resolve_output_names(base, &schema, &node.properties);

    if matches!(base.as_any().downcast_ref::<PhysicalRowsetScan>(), Some(_)) {
        node.operator_name = "ROWSET_SCAN".to_string();
    }
    node
}

fn unwrap_explain_operator(mut op: &dyn PhysicalOperator) -> &dyn PhysicalOperator {
    while let Some(wrapper) = op.as_any().downcast_ref::<ExplainAnnotatedOperator>() {
        op = wrapper.inner();
    }
    op
}

fn logical_info_for_operator(op: &dyn PhysicalOperator) -> ExplainLogicalInfo {
    op.as_any()
        .downcast_ref::<ExplainAnnotatedOperator>()
        .map(|wrapper| wrapper.logical().clone())
        .unwrap_or_default()
}

fn estimated_cardinality(
    op: &dyn PhysicalOperator,
    logical: &ExplainLogicalInfo,
) -> Option<paro_planner::plan::CardinalityEstimate> {
    logical.estimated_cardinality.or_else(|| {
        let rows = unwrap_explain_operator(op).estimated_cardinality();
        (rows > 0).then_some(paro_planner::plan::CardinalityEstimate::exact(rows as u64))
    })
}

fn actual_stats(
    op: &dyn PhysicalOperator,
    base: &dyn PhysicalOperator,
    spec: &ExplainSpec,
) -> Option<ExplainActualStats> {
    if !matches!(spec.mode, ExplainMode::Analyze) {
        return None;
    }
    let mut actual = op
        .explain_node_id()
        .and_then(|node_id| {
            op.explain_profiler()
                .and_then(|profiler| profiler.node_stats(node_id))
        })
        .unwrap_or_default();
    if spec.detail.memory {
        actual.runtime = base.runtime_memory_stats();
    }
    if actual.output_rows > 0
        || actual.loops > 0
        || actual.startup_time_ms.is_some()
        || actual.total_time_ms.is_some()
        || actual.runtime.spilled.is_some()
        || actual.runtime.peak_memory_bytes.is_some()
        || actual.runtime.temp_storage_bytes.is_some()
    {
        Some(actual)
    } else {
        None
    }
}

fn schema_for_operator(op: &dyn PhysicalOperator) -> ExplainSchema {
    op.explain_schema()
        .cloned()
        .unwrap_or_else(|| ExplainSchema {
            output_names: default_output_names(op.types().len()),
            relation_name: None,
            relation_alias: None,
        })
}

fn default_output_names(column_count: usize) -> Vec<String> {
    (0..column_count)
        .map(|idx| format!("col_{}", idx + 1))
        .collect()
}

fn child_schema(children: &[ExplainNode], child_idx: usize) -> ExplainSchema {
    children
        .get(child_idx)
        .map_or_else(ExplainSchema::default, |child| ExplainSchema {
            output_names: child.output_names.clone(),
            relation_name: child.relation_name.clone(),
            relation_alias: child.relation_alias.clone(),
        })
}

fn build_properties(
    base: &dyn PhysicalOperator,
    schema: &ExplainSchema,
    children: &[ExplainNode],
    spec: &ExplainSpec,
    logical: &ExplainLogicalInfo,
) -> Vec<ExplainProperty> {
    if let Some(scan) = base.as_any().downcast_ref::<PhysicalRowsetScan>() {
        let column_names = if schema.output_names.is_empty() {
            scan.projected_column_names()
        } else {
            schema.output_names.clone()
        };
        let mut properties = Vec::new();
        if !column_names.is_empty() {
            properties.push(ExplainProperty::new(
                "Columns",
                ExplainValue::List(
                    column_names
                        .iter()
                        .cloned()
                        .map(ExplainValue::String)
                        .collect(),
                ),
            ));
        }
        if let Some(predicate_tree) = scan.predicate_tree() {
            properties.push(ExplainProperty::new(
                "Filter",
                ExplainValue::String(format_predicate_tree_with_context(
                    predicate_tree,
                    &ExplainContext::scan(&column_names),
                )),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(projection) = base.as_any().downcast_ref::<Projection>() {
        let input_names = child_schema(children, 0).output_names;
        let ctx = ExplainContext::input(input_names);
        return finalize_properties(
            vec![ExplainProperty::new(
                "Output",
                ExplainValue::List(
                    projection
                        .expressions()
                        .iter()
                        .map(|expr| {
                            ExplainValue::String(format_bound_expression_with_context(expr, &ctx))
                        })
                        .collect(),
                ),
            )],
            base,
            schema,
            spec,
            logical,
        );
    }

    if let Some(filter) = base.as_any().downcast_ref::<Filter>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        return finalize_properties(
            vec![ExplainProperty::new(
                "Filter",
                ExplainValue::String(format_bound_expression_with_context(
                    filter.predicate(),
                    &ctx,
                )),
            )],
            base,
            schema,
            spec,
            logical,
        );
    }

    if let Some(order) = base.as_any().downcast_ref::<Order>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        let mut properties = vec![ExplainProperty::new(
            "Sort Key",
            ExplainValue::String(format_bound_order_by_nodes_with_context(
                order.orders(),
                &ctx,
            )),
        )];
        if matches!(spec.mode, ExplainMode::Analyze) && spec.detail.memory {
            if let Some(spilled) = base.runtime_memory_stats().spilled {
                properties.push(ExplainProperty::new(
                    "Sort Method",
                    ExplainValue::String(
                        if spilled {
                            "external merge"
                        } else {
                            "quicksort"
                        }
                        .to_string(),
                    ),
                ));
            }
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(topn) = base.as_any().downcast_ref::<TopN>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        let mut properties = vec![ExplainProperty::new(
            "Sort Key",
            ExplainValue::String(format_bound_order_by_nodes_with_context(
                topn.orders(),
                &ctx,
            )),
        )];
        properties.push(ExplainProperty::new(
            "Limit",
            ExplainValue::Unsigned(topn.limit() as u64),
        ));
        if topn.offset() > 0 {
            properties.push(ExplainProperty::new(
                "Offset",
                ExplainValue::Unsigned(topn.offset() as u64),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(limit) = base.as_any().downcast_ref::<StreamingLimit>() {
        let mut properties = fallback_properties(limit.explain_params(), spec);
        properties.retain(|property| property.label != "External");
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(hash_agg) = base.as_any().downcast_ref::<HashAggregate>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        let mut properties = Vec::new();
        if !hash_agg.aggregate_data.groups.is_empty() {
            properties.push(ExplainProperty::new(
                "Group Key",
                ExplainValue::List(
                    hash_agg
                        .aggregate_data
                        .groups
                        .iter()
                        .map(|expr| {
                            ExplainValue::String(format_bound_expression_with_context(expr, &ctx))
                        })
                        .collect(),
                ),
            ));
        }
        if !hash_agg.aggregate_data.aggregates.is_empty() {
            properties.push(ExplainProperty::new(
                "Aggregates",
                ExplainValue::List(
                    hash_agg
                        .aggregate_data
                        .aggregates
                        .iter()
                        .map(|expr| {
                            ExplainValue::String(format_bound_expression_with_context(expr, &ctx))
                        })
                        .collect(),
                ),
            ));
        }
        if let Some(width) = hash_agg.inline_key_width() {
            properties.push(ExplainProperty::new(
                "Key Mode",
                ExplainValue::String(format!("INLINE_KEY_{width}B")),
            ));
        }
        if let Some(partition_count) = hash_agg.radix_partition_count() {
            properties.push(ExplainProperty::new(
                "Partitioning",
                ExplainValue::String(format!("RADIX_PARTITIONS={partition_count}")),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(perfect_agg) = base.as_any().downcast_ref::<PerfectHashAggregate>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        let mut properties = Vec::new();
        if !perfect_agg.aggregate_data.groups.is_empty() {
            properties.push(ExplainProperty::new(
                "Group Key",
                ExplainValue::List(
                    perfect_agg
                        .aggregate_data
                        .groups
                        .iter()
                        .map(|expr| {
                            ExplainValue::String(format_bound_expression_with_context(expr, &ctx))
                        })
                        .collect(),
                ),
            ));
        }
        if !perfect_agg.aggregate_data.aggregates.is_empty() {
            properties.push(ExplainProperty::new(
                "Aggregates",
                ExplainValue::List(
                    perfect_agg
                        .aggregate_data
                        .aggregates
                        .iter()
                        .map(|expr| {
                            ExplainValue::String(format_bound_expression_with_context(expr, &ctx))
                        })
                        .collect(),
                ),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(ungrouped_agg) = base.as_any().downcast_ref::<UngroupedAggregate>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        if !ungrouped_agg.aggregate_data.aggregates.is_empty() {
            return finalize_properties(
                vec![ExplainProperty::new(
                    "Aggregates",
                    ExplainValue::List(
                        ungrouped_agg
                            .aggregate_data
                            .aggregates
                            .iter()
                            .map(|expr| {
                                ExplainValue::String(format_bound_expression_with_context(
                                    expr, &ctx,
                                ))
                            })
                            .collect(),
                    ),
                )],
                base,
                schema,
                spec,
                logical,
            );
        }
        return finalize_properties(Vec::new(), base, schema, spec, logical);
    }

    if let Some(hash_join) = base.as_any().downcast_ref::<HashJoin>() {
        let join_ctx = ExplainContext::join(
            child_schema(children, 0).output_names,
            child_schema(children, 1).output_names,
        );
        let join_filter_stats = hash_join.runtime_join_filter_stats();
        let mut properties = vec![ExplainProperty::new(
            "Join Type",
            ExplainValue::String(hash_join.join_type().to_string()),
        )];
        if !hash_join.conditions().is_empty() {
            properties.push(ExplainProperty::new(
                "Join Condition",
                ExplainValue::List(
                    hash_join
                        .conditions()
                        .iter()
                        .map(|condition| {
                            ExplainValue::String(format_join_condition_with_context(
                                condition, &join_ctx,
                            ))
                        })
                        .collect(),
                ),
            ));
        }
        if spec.detail.verbose || join_filter_stats.is_some() {
            if let Some(kind) = hash_join.join_filter_kind() {
                properties.push(ExplainProperty::new(
                    "Join Filter Kind",
                    ExplainValue::String(kind.to_string()),
                ));
            }
            if let Some(target_indices) = hash_join.join_filter_target_condition_indices() {
                let target_values = target_indices
                    .into_iter()
                    .filter_map(|condition_idx| hash_join.equality_conditions().get(condition_idx))
                    .map(|condition| {
                        ExplainValue::String(format_bound_expression_with_context(
                            &condition.left,
                            &ExplainContext::input(join_ctx.left_names.clone()),
                        ))
                    })
                    .collect::<Vec<_>>();
                if !target_values.is_empty() {
                    properties.push(ExplainProperty::new(
                        "Join Filter Target",
                        ExplainValue::List(target_values),
                    ));
                }
            }
            if let Some(prune_ratio) = join_filter_stats.and_then(|stats| stats.prune_ratio()) {
                properties.push(ExplainProperty::new(
                    "Join Filter Prune Ratio",
                    ExplainValue::Float(prune_ratio),
                ));
            }
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(nested_join) = base.as_any().downcast_ref::<NestedLoopJoin>() {
        let join_ctx = ExplainContext::join(
            child_schema(children, 0).output_names,
            child_schema(children, 1).output_names,
        );
        let mut properties = vec![ExplainProperty::new(
            "Join Type",
            ExplainValue::String(nested_join.join_type.to_string()),
        )];
        if !nested_join.comparison_conditions.is_empty() {
            properties.push(ExplainProperty::new(
                "Join Condition",
                ExplainValue::List(
                    nested_join
                        .comparison_conditions
                        .iter()
                        .map(|condition| {
                            ExplainValue::String(format_join_condition_with_context(
                                condition, &join_ctx,
                            ))
                        })
                        .collect(),
                ),
            ));
        }
        if let Some(condition) = &nested_join.arbitrary_condition {
            properties.push(ExplainProperty::new(
                "Filter",
                ExplainValue::String(format_bound_expression_with_context(condition, &join_ctx)),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(piecewise_join) = base.as_any().downcast_ref::<PiecewiseMergeJoin>() {
        let join_ctx = ExplainContext::join(
            child_schema(children, 0).output_names,
            child_schema(children, 1).output_names,
        );
        return finalize_properties(
            vec![
                ExplainProperty::new(
                    "Join Type",
                    ExplainValue::String(piecewise_join.join_type.to_string()),
                ),
                ExplainProperty::new(
                    "Join Condition",
                    ExplainValue::List(vec![ExplainValue::String(
                        format_join_condition_with_context(&piecewise_join.condition, &join_ctx),
                    )]),
                ),
            ],
            base,
            schema,
            spec,
            logical,
        );
    }

    if let Some(ie_join) = base.as_any().downcast_ref::<IEJoin>() {
        let join_ctx = ExplainContext::join(
            child_schema(children, 0).output_names,
            child_schema(children, 1).output_names,
        );
        return finalize_properties(
            vec![
                ExplainProperty::new(
                    "Join Type",
                    ExplainValue::String(ie_join.join_type.to_string()),
                ),
                ExplainProperty::new(
                    "Join Condition",
                    ExplainValue::List(
                        ie_join
                            .conditions
                            .iter()
                            .map(|condition| {
                                ExplainValue::String(format_join_condition_with_context(
                                    condition, &join_ctx,
                                ))
                            })
                            .collect(),
                    ),
                ),
            ],
            base,
            schema,
            spec,
            logical,
        );
    }

    if let Some(window) = base.as_any().downcast_ref::<Window>() {
        let ctx = ExplainContext::input(child_schema(children, 0).output_names);
        let properties = vec![ExplainProperty::new(
            "Window Functions",
            ExplainValue::List(
                window
                    .expressions()
                    .iter()
                    .map(|expr| {
                        ExplainValue::String(format_window_expression_with_context(expr, &ctx))
                    })
                    .collect(),
            ),
        )];
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(cte) = base.as_any().downcast_ref::<PhysicalCTE>() {
        let mut properties = vec![
            ExplainProperty::new("CTE Name", ExplainValue::String(cte.cte_name.clone())),
            ExplainProperty::new(
                "Materialization",
                ExplainValue::String(format_materialization_hint(cte.materialization).to_string()),
            ),
            ExplainProperty::new(
                "Reference Count",
                ExplainValue::Unsigned(cte.ref_count as u64),
            ),
        ];
        if matches!(spec.mode, ExplainMode::Analyze) {
            properties.push(ExplainProperty::new(
                "Materialized Rows",
                ExplainValue::Unsigned(cte.working_table.row_count() as u64),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(recursive_cte) = base.as_any().downcast_ref::<PhysicalRecursiveCTE>() {
        let mut properties = vec![
            ExplainProperty::new(
                "CTE Name",
                ExplainValue::String(recursive_cte.cte_name.clone()),
            ),
            ExplainProperty::new(
                "Set Operation",
                ExplainValue::String(
                    if recursive_cte.union_all {
                        "UNION ALL"
                    } else {
                        "UNION"
                    }
                    .to_string(),
                ),
            ),
        ];
        if matches!(spec.mode, ExplainMode::Analyze) {
            properties.push(ExplainProperty::new(
                "Iterations",
                ExplainValue::Unsigned(recursive_cte.productive_iterations() as u64),
            ));
        }
        return finalize_properties(properties, base, schema, spec, logical);
    }

    if let Some(cte_scan) = base.as_any().downcast_ref::<CteScan>() {
        return finalize_properties(
            vec![ExplainProperty::new(
                "CTE Name",
                ExplainValue::String(cte_scan.cte_name.clone()),
            )],
            base,
            schema,
            spec,
            logical,
        );
    }

    finalize_properties(
        fallback_properties(base.explain_params(), spec),
        base,
        schema,
        spec,
        logical,
    )
}

fn resolve_output_names(
    base: &dyn PhysicalOperator,
    schema: &ExplainSchema,
    properties: &[ExplainProperty],
) -> Vec<String> {
    if let Some(property) = properties
        .iter()
        .find(|property| property.label == "Output")
    {
        if let ExplainValue::List(values) = &property.value {
            let output_names = values.iter().map(ExplainValue::to_text).collect::<Vec<_>>();
            if !output_names.is_empty() && output_names.len() >= schema.output_names.len() {
                return output_names;
            }
        }
    }
    if !schema.output_names.is_empty() {
        return schema.output_names.clone();
    }
    if let Some(scan) = base.as_any().downcast_ref::<PhysicalRowsetScan>() {
        return scan.projected_column_names();
    }
    Vec::new()
}

fn fallback_properties(params: Vec<String>, spec: &ExplainSpec) -> Vec<ExplainProperty> {
    let mut properties = Vec::new();
    for param in params {
        let Some((label, value)) = param.split_once(':') else {
            continue;
        };
        if !matches!(spec.mode, ExplainMode::Analyze)
            && matches!(label.trim(), "External" | "Spilled")
        {
            continue;
        }
        properties.push(ExplainProperty::new(
            label.trim(),
            ExplainValue::String(value.trim().to_string()),
        ));
    }
    properties
}

fn finalize_properties(
    mut properties: Vec<ExplainProperty>,
    base: &dyn PhysicalOperator,
    schema: &ExplainSchema,
    spec: &ExplainSpec,
    logical: &ExplainLogicalInfo,
) -> Vec<ExplainProperty> {
    if matches!(spec.mode, ExplainMode::Analyze) && spec.detail.memory {
        append_runtime_memory_properties(&mut properties, base.runtime_memory_stats());
    }
    if let Some(search) = &logical.search {
        properties.push(ExplainProperty::new(
            "Search Decision",
            ExplainValue::String(search.summary.clone()),
        ));
        if let Some(confidence) = &search.confidence {
            properties.push(ExplainProperty::new(
                "Search Confidence",
                ExplainValue::String(confidence.clone()),
            ));
        }
        if spec.detail.verbose && !search.candidates.is_empty() {
            properties.push(ExplainProperty::new(
                "Search Candidates",
                ExplainValue::List(
                    search
                        .candidates
                        .iter()
                        .cloned()
                        .map(ExplainValue::String)
                        .collect(),
                ),
            ));
        }
    }
    if spec.detail.verbose {
        if let Some(estimate) = logical.estimated_cardinality {
            properties.push(ExplainProperty::new(
                "Cardinality",
                ExplainValue::String(format!(
                    "[{}, {}, {}]",
                    estimate.min, estimate.expected, estimate.max
                )),
            ));
        }
        if let Some(property) = verbose_output_schema_property(base, schema) {
            properties.push(property);
        }
    }
    properties
}

fn append_runtime_memory_properties(
    properties: &mut Vec<ExplainProperty>,
    runtime: ExplainRuntimeStats,
) {
    if let Some(spilled) = runtime.spilled {
        properties.push(ExplainProperty::new("Spilled", ExplainValue::Bool(spilled)));
    }
    if let Some(peak_memory_bytes) = runtime.peak_memory_bytes {
        properties.push(ExplainProperty::new(
            "Peak Memory",
            ExplainValue::Bytes(peak_memory_bytes),
        ));
    }
    if let Some(temp_storage_bytes) = runtime.temp_storage_bytes {
        properties.push(ExplainProperty::new(
            "Temp Storage",
            ExplainValue::Bytes(temp_storage_bytes),
        ));
    }
}

fn verbose_output_schema_property(
    base: &dyn PhysicalOperator,
    schema: &ExplainSchema,
) -> Option<ExplainProperty> {
    let types = base.types();
    if types.is_empty() {
        return None;
    }
    let mut names = if !schema.output_names.is_empty() {
        schema.output_names.clone()
    } else if let Some(scan) = base.as_any().downcast_ref::<PhysicalRowsetScan>() {
        scan.projected_column_names()
    } else {
        default_output_names(types.len())
    };
    if names.len() < types.len() {
        names.extend(
            default_output_names(types.len())
                .into_iter()
                .skip(names.len()),
        );
    }
    names.truncate(types.len());

    Some(ExplainProperty::new(
        "Output Schema",
        ExplainValue::List(
            names
                .into_iter()
                .zip(types.iter())
                .map(|(name, ty)| ExplainValue::String(format!("{name} {ty}")))
                .collect(),
        ),
    ))
}

fn format_materialization_hint(
    materialization: paro_planner::binder::ir::CTEMaterialize,
) -> &'static str {
    match materialization {
        paro_planner::binder::ir::CTEMaterialize::Default => "DEFAULT",
        paro_planner::binder::ir::CTEMaterialize::Materialized => "MATERIALIZED",
        paro_planner::binder::ir::CTEMaterialize::NotMaterialized => "NOT MATERIALIZED",
    }
}

pub(crate) fn format_bound_order_by_nodes(orders: &[OrderByNode]) -> String {
    format_bound_order_by_nodes_with_context(orders, &ExplainContext::default())
}

fn format_bound_order_by_nodes_with_context(
    orders: &[OrderByNode],
    ctx: &ExplainContext,
) -> String {
    orders
        .iter()
        .map(|order| format_bound_order_by_node(order, ctx))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_bound_order_by_node(order: &OrderByNode, ctx: &ExplainContext) -> String {
    let direction = if order.ascending { "ASC" } else { "DESC" };
    let nulls = if order.nulls_first {
        "NULLS FIRST"
    } else {
        "NULLS LAST"
    };
    format!(
        "{} {} {}",
        format_bound_expression_with_context(&order.expression, ctx),
        direction,
        nulls
    )
}

fn format_window_order_by_expression(order: &OrderByExpression, ctx: &ExplainContext) -> String {
    let direction = if order.ascending { "ASC" } else { "DESC" };
    let nulls = if order.nulls_first {
        "NULLS FIRST"
    } else {
        "NULLS LAST"
    };
    format!(
        "{} {} {}",
        format_bound_expression_with_context(&order.expression, ctx),
        direction,
        nulls
    )
}

pub(crate) fn format_join_condition(condition: &JoinCondition) -> String {
    format_join_condition_with_context(condition, &ExplainContext::default())
}

fn format_join_condition_with_context(condition: &JoinCondition, ctx: &ExplainContext) -> String {
    let left_ctx = ExplainContext::input(ctx.left_names.clone());
    let right_ctx = ExplainContext::input(ctx.right_names.clone());
    format!(
        "{} {} {}",
        format_bound_expression_with_context(&condition.left, &left_ctx),
        format_join_comparison(condition.comparison),
        format_bound_expression_with_context(&condition.right, &right_ctx)
    )
}

fn format_join_comparison(comparison: JoinComparisonType) -> &'static str {
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

fn format_aggregate_expression(
    name: &str,
    aggr_type: AggregateType,
    arguments: &[Expression],
    filter: Option<&Expression>,
    order_bys: &[OrderByExpression],
    ctx: &ExplainContext,
) -> String {
    let args = if arguments.is_empty() {
        "*".to_string()
    } else {
        let rendered = arguments
            .iter()
            .map(|expr| format_bound_expression_with_context(expr, ctx))
            .collect::<Vec<_>>()
            .join(", ");
        match aggr_type {
            AggregateType::NonDistinct => rendered,
            AggregateType::Distinct => format!("DISTINCT {rendered}"),
        }
    };

    let mut result = format!("{}({args})", name.to_uppercase());
    if !order_bys.is_empty() {
        let orders = order_bys
            .iter()
            .map(|order| format_window_order_by_expression(order, ctx))
            .collect::<Vec<_>>()
            .join(", ");
        result.push_str(&format!(" WITHIN GROUP (ORDER BY {orders})"));
    }
    if let Some(filter_expr) = filter {
        result.push_str(&format!(
            " FILTER (WHERE {})",
            format_bound_expression_with_context(filter_expr, ctx)
        ));
    }
    result
}

pub(crate) fn format_window_expression(expr: &WindowExpression) -> String {
    format_window_expression_with_context(expr, &ExplainContext::default())
}

fn format_window_expression_with_context(expr: &WindowExpression, ctx: &ExplainContext) -> String {
    let args = expr
        .children
        .iter()
        .map(|expr| format_bound_expression_with_context(expr, ctx))
        .collect::<Vec<_>>()
        .join(", ");
    let function_call = format!("{}({})", expr.function.name.to_uppercase(), args);

    let mut clauses = Vec::new();
    if !expr.partitions.is_empty() {
        clauses.push(format!(
            "PARTITION BY {}",
            expr.partitions
                .iter()
                .map(|expr| format_bound_expression_with_context(expr, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !expr.orders.is_empty() {
        clauses.push(format!(
            "ORDER BY {}",
            expr.orders
                .iter()
                .map(|order| format_window_order_by_expression(order, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let frame_type = match expr.frame.frame_type {
        paro_planner::expression::WindowFrameType::Rows => "ROWS",
        paro_planner::expression::WindowFrameType::Range => "RANGE",
    };
    let start =
        format_window_frame_bound(&expr.frame.start_bound, expr.frame.start_is_preceding, ctx);
    let end = format_window_frame_bound(&expr.frame.end_bound, expr.frame.end_is_preceding, ctx);
    clauses.push(format!("{frame_type} BETWEEN {start} AND {end}"));

    format!("{function_call} OVER ({})", clauses.join(" "))
}

fn format_window_frame_bound(
    bound: &paro_planner::expression::WindowFrameBound,
    is_preceding: bool,
    ctx: &ExplainContext,
) -> String {
    match bound {
        paro_planner::expression::WindowFrameBound::Unbounded => {
            if is_preceding {
                "UNBOUNDED PRECEDING".to_string()
            } else {
                "UNBOUNDED FOLLOWING".to_string()
            }
        }
        paro_planner::expression::WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        paro_planner::expression::WindowFrameBound::Offset(expr) => {
            let suffix = if is_preceding {
                "PRECEDING"
            } else {
                "FOLLOWING"
            };
            format!(
                "{} {suffix}",
                format_bound_expression_with_context(expr, ctx)
            )
        }
    }
}

pub(crate) fn format_predicate_tree(tree: &PredicateTree) -> String {
    format_predicate_tree_with_context(tree, &ExplainContext::default())
}

fn format_predicate_tree_with_context(tree: &PredicateTree, ctx: &ExplainContext) -> String {
    match tree {
        PredicateTree::Leaf(predicate) => format_predicate(predicate, ctx),
        PredicateTree::And(children) => format!(
            "({})",
            children
                .iter()
                .map(|child| format_predicate_tree_with_context(child, ctx))
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        PredicateTree::Or(children) => format!(
            "({})",
            children
                .iter()
                .map(|child| format_predicate_tree_with_context(child, ctx))
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
    }
}

fn format_predicate(predicate: &Predicate, ctx: &ExplainContext) -> String {
    let column_name = |column_id: &u32| {
        ctx.scan_columns
            .get(&(u64::from(*column_id)))
            .cloned()
            .unwrap_or_else(|| format!("col_{}", u64::from(*column_id) + 1))
    };
    match predicate {
        Predicate::Eq { column_id, value } => format!("{} = {value}", column_name(column_id)),
        Predicate::NotEq { column_id, value } => format!("{} <> {value}", column_name(column_id)),
        Predicate::Lt { column_id, value } => format!("{} < {value}", column_name(column_id)),
        Predicate::Le { column_id, value } => format!("{} <= {value}", column_name(column_id)),
        Predicate::Gt { column_id, value } => format!("{} > {value}", column_name(column_id)),
        Predicate::Ge { column_id, value } => format!("{} >= {value}", column_name(column_id)),
        Predicate::In { column_id, values } => {
            let values = values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({values})", column_name(column_id))
        }
        Predicate::Range {
            column_id,
            lower,
            upper,
        } => {
            format!("{} BETWEEN {lower} AND {upper}", column_name(column_id))
        }
        Predicate::IsNull { column_id } => format!("{} IS NULL", column_name(column_id)),
        Predicate::IsNotNull { column_id } => format!("{} IS NOT NULL", column_name(column_id)),
    }
}

pub(crate) fn format_bound_expression(expr: &Expression) -> String {
    format_bound_expression_with_context(expr, &ExplainContext::default())
}

fn format_bound_expression_with_context(expr: &Expression, ctx: &ExplainContext) -> String {
    match expr {
        Expression::Constant(constant) => constant.value.to_string(),
        Expression::ColumnRef(column_ref) => ctx
            .input_names
            .get(column_ref.binding.column_index)
            .cloned()
            .or_else(|| {
                ctx.scan_columns
                    .get(&(column_ref.binding.column_index as u64))
                    .cloned()
            })
            .unwrap_or_else(|| format!("col_{}", column_ref.binding.column_index + 1)),
        Expression::Function(function) => {
            let args = function
                .children
                .iter()
                .map(|expr| format_bound_expression_with_context(expr, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({args})", function.function.name.to_uppercase())
        }
        Expression::Cast(cast_expr) => {
            let cast_name = if cast_expr.try_cast {
                "TRY_CAST"
            } else {
                "CAST"
            };
            format!(
                "{cast_name}({} AS {})",
                format_bound_expression_with_context(&cast_expr.child, ctx),
                cast_expr.target_type
            )
        }
        Expression::Conjunction(conjunction) => {
            let connector = match conjunction.conjunction_type {
                ConjunctionType::And => " AND ",
                ConjunctionType::Or => " OR ",
            };
            format!(
                "({})",
                conjunction
                    .children
                    .iter()
                    .map(|expr| format_bound_expression_with_context(expr, ctx))
                    .collect::<Vec<_>>()
                    .join(connector)
            )
        }
        Expression::Case(case_expr) => format!(
            "CASE WHEN {} THEN {} ELSE {} END",
            format_bound_expression_with_context(&case_expr.check, ctx),
            format_bound_expression_with_context(&case_expr.result_if_true, ctx),
            format_bound_expression_with_context(&case_expr.result_if_false, ctx)
        ),
        Expression::Comparison(comparison) => format!(
            "({} {} {})",
            format_bound_expression_with_context(&comparison.left, ctx),
            format_comparison(comparison.comparison_type),
            format_bound_expression_with_context(&comparison.right, ctx)
        ),
        Expression::Operator(operator) => format_operator_expression(operator, ctx),
        Expression::Reference(reference) => ctx
            .input_names
            .get(reference.index)
            .cloned()
            .unwrap_or_else(|| format!("ref_{}", reference.index + 1)),
        Expression::Aggregate(aggregate) => format_aggregate_expression(
            &aggregate.function.name,
            aggregate.aggr_type,
            &aggregate.children,
            aggregate.filter.as_deref(),
            &aggregate.order_bys,
            ctx,
        ),
        Expression::Subquery(subquery) => format!("<subquery:{:?}>", subquery.subquery_type),
        Expression::Window(window) => format_window_expression_with_context(window, ctx),
    }
}

fn format_operator_expression(expr: &OperatorExpression, ctx: &ExplainContext) -> String {
    match expr.operator_type {
        OperatorType::In | OperatorType::NotIn => {
            let in_keyword = if expr.operator_type == OperatorType::In {
                "IN"
            } else {
                "NOT IN"
            };
            if expr.children.is_empty() {
                return format!("{in_keyword} ()");
            }
            let left = format_bound_expression_with_context(&expr.children[0], ctx);
            let right = expr.children[1..]
                .iter()
                .map(|expr| format_bound_expression_with_context(expr, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({left} {in_keyword} ({right}))")
        }
        OperatorType::Not => expr.children.first().map_or_else(
            || "NOT".to_string(),
            |child| format!("(NOT {})", format_bound_expression_with_context(child, ctx)),
        ),
        OperatorType::IsNull => expr.children.first().map_or_else(
            || "IS NULL".to_string(),
            |child| {
                format!(
                    "({} IS NULL)",
                    format_bound_expression_with_context(child, ctx)
                )
            },
        ),
        OperatorType::IsNotNull => expr.children.first().map_or_else(
            || "IS NOT NULL".to_string(),
            |child| {
                format!(
                    "({} IS NOT NULL)",
                    format_bound_expression_with_context(child, ctx)
                )
            },
        ),
        OperatorType::Coalesce => format!(
            "COALESCE({})",
            expr.children
                .iter()
                .map(|child| format_bound_expression_with_context(child, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        OperatorType::Like | OperatorType::ILike => {
            let operator = if expr.operator_type == OperatorType::Like {
                "LIKE"
            } else {
                "ILIKE"
            };
            if expr.children.len() < 2 {
                return operator.to_string();
            }
            format!(
                "({} {} {})",
                format_bound_expression_with_context(&expr.children[0], ctx),
                operator,
                format_bound_expression_with_context(&expr.children[1], ctx)
            )
        }
        OperatorType::ArrayConstructor => format!(
            "[{}]",
            expr.children
                .iter()
                .map(|child| format_bound_expression_with_context(child, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        OperatorType::StructConstructor => format!(
            "STRUCT({})",
            expr.children
                .iter()
                .map(|child| format_bound_expression_with_context(child, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        OperatorType::ArrayExtract => {
            if expr.children.len() < 2 {
                return "ARRAY_EXTRACT".to_string();
            }
            format!(
                "{}[{}]",
                format_bound_expression_with_context(&expr.children[0], ctx),
                format_bound_expression_with_context(&expr.children[1], ctx)
            )
        }
        OperatorType::ErrorIfMultipleRows => {
            if expr.children.len() < 2 {
                return "SCALAR_SUBQUERY_CHECK".to_string();
            }
            format!(
                "SCALAR_SUBQUERY_CHECK({}, {})",
                format_bound_expression_with_context(&expr.children[0], ctx),
                format_bound_expression_with_context(&expr.children[1], ctx)
            )
        }
    }
}

fn format_comparison(comparison_type: ComparisonType) -> &'static str {
    match comparison_type {
        ComparisonType::Equal => "=",
        ComparisonType::NotEqual => "<>",
        ComparisonType::LessThan => "<",
        ComparisonType::LessThanOrEqual => "<=",
        ComparisonType::GreaterThan => ">",
        ComparisonType::GreaterThanOrEqual => ">=",
        ComparisonType::DistinctFrom => "IS DISTINCT FROM",
        ComparisonType::NotDistinctFrom => "IS NOT DISTINCT FROM",
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use paro_common::types::LogicalType;

    use crate::explain::annotated_operator::ExplainAnnotatedOperator;
    use crate::explain::types::{
        ExplainLogicalInfo, ExplainNodeId, ExplainSchema, ExplainSearchInfo,
    };

    use super::{build_explain_doc, explain_physical_plan, render_explain_text_lines};
    use crate::operator::PhysicalOperator;
    use crate::operator_type::PhysicalOperatorType;
    use paro_planner::operator::{ExplainDetail, ExplainFormat, ExplainMode, ExplainSpec};

    #[derive(Debug)]
    struct TestOperator {
        name: &'static str,
        rows: usize,
        params: Vec<String>,
        children: Vec<Arc<dyn PhysicalOperator>>,
        types: Vec<LogicalType>,
        node_id: Option<ExplainNodeId>,
        schema: Option<ExplainSchema>,
    }

    impl TestOperator {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                rows: 0,
                params: vec![],
                children: vec![],
                types: vec![],
                node_id: None,
                schema: None,
            }
        }
    }

    impl PhysicalOperator for TestOperator {
        fn operator_type(&self) -> PhysicalOperatorType {
            PhysicalOperatorType::DummyScan
        }

        fn name(&self) -> &str {
            self.name
        }

        fn explain_params(&self) -> Vec<String> {
            self.params.clone()
        }

        fn explain_node_id(&self) -> Option<ExplainNodeId> {
            self.node_id
        }

        fn explain_schema(&self) -> Option<&ExplainSchema> {
            self.schema.as_ref()
        }

        fn types(&self) -> &[LogicalType] {
            &self.types
        }

        fn estimated_cardinality(&self) -> usize {
            self.rows
        }

        fn children_count(&self) -> usize {
            self.children.len()
        }

        fn child(&self, index: usize) -> Option<&dyn PhysicalOperator> {
            self.children.get(index).map(|child| child.as_ref())
        }

        fn child_arc(&self, index: usize) -> Option<Arc<dyn PhysicalOperator>> {
            self.children.get(index).cloned()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn explain_text_uses_pg_style_indent() {
        let grandchild = Arc::new(TestOperator {
            name: "FILTER",
            params: vec!["Filter: (id > 0)".to_string()],
            ..TestOperator::new("FILTER")
        });
        let child = Arc::new(TestOperator {
            name: "SCAN",
            params: vec!["Columns: id".to_string()],
            schema: Some(ExplainSchema {
                output_names: vec!["id".to_string()],
                relation_name: Some("public.t".to_string()),
                relation_alias: None,
            }),
            children: vec![grandchild],
            ..TestOperator::new("SCAN")
        });
        let root = TestOperator {
            name: "PROJECTION",
            params: vec!["Output: id".to_string()],
            children: vec![child],
            ..TestOperator::new("PROJECTION")
        };

        let lines = explain_physical_plan(&root, false);
        assert_eq!(lines[0], "PROJECTION");
        assert_eq!(lines[1], "  Output: id");
        assert_eq!(lines[2], "  ->  SCAN on public.t");
        assert_eq!(lines[3], "        Columns: id");
        assert_eq!(lines[4], "        ->  FILTER");
        assert_eq!(lines[5], "              Filter: (id > 0)");
    }

    #[test]
    fn explain_doc_keeps_spec() {
        let root = TestOperator::new("SCAN");
        let spec = ExplainSpec {
            mode: ExplainMode::Analyze,
            format: ExplainFormat::Json,
            detail: ExplainDetail::default(),
        };
        let doc = build_explain_doc(&root, spec);
        assert!(matches!(doc.spec.mode, ExplainMode::Analyze));
        assert!(matches!(doc.spec.format, ExplainFormat::Json));
    }

    #[test]
    fn explain_verbose_renders_search_metadata_from_logical_info() {
        let root = Arc::new(ExplainAnnotatedOperator::new(
            1,
            ExplainSchema {
                output_names: vec!["id".to_string()],
                relation_name: Some("public.docs".to_string()),
                relation_alias: None,
            },
            ExplainLogicalInfo {
                estimated_cardinality: Some(paro_planner::plan::CardinalityEstimate {
                    min: 1,
                    expected: 3,
                    max: 8,
                }),
                search: Some(ExplainSearchInfo {
                    summary: "INDEX_SCAN FULLTEXT_TOPK(column_id=1) cost=1.250".to_string(),
                    confidence: Some("HIGH".to_string()),
                    candidates: vec!["fulltext_topk cost=1.250 threshold=2".to_string()],
                }),
            },
            None,
            Arc::new(TestOperator::new("PROJECTION")),
        ));

        let spec = ExplainSpec {
            mode: ExplainMode::Plan,
            format: ExplainFormat::Text,
            detail: ExplainDetail {
                verbose: true,
                ..ExplainDetail::default()
            },
        };
        let doc = build_explain_doc(root.as_ref(), spec);
        let lines = render_explain_text_lines(&doc);

        assert!(lines.iter().any(|line| {
            line.contains("Search Decision: INDEX_SCAN FULLTEXT_TOPK(column_id=1) cost=1.250")
        }));
        assert!(lines
            .iter()
            .any(|line| line.contains("Search Confidence: HIGH")));
        assert!(lines
            .iter()
            .any(|line| line.contains("Search Candidates: fulltext_topk cost=1.250 threshold=2")));
        assert!(lines
            .iter()
            .any(|line| line.contains("Cardinality: [1, 3, 8]")));
    }
}
