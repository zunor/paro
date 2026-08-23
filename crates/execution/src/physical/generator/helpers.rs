// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::operators::aggregate::perfect_hash_key::PerfectHashKeyDomain;
use paro_catalog::entry::StandardEntry;
use paro_planner::operator::graph_expand::graph_path_element_list_type;

pub(crate) fn extract_payload_expression(
    expr: Expression,
    projection_exprs: &mut Vec<Expression>,
    payload_types: &mut Vec<paro_common::types::LogicalType>,
) -> Expression {
    let return_type = expr.return_type();
    if expr.evaluation_properties().can_share_evaluation() {
        if let Some(reference_index) = projection_exprs
            .iter()
            .position(|existing| existing.equals(&expr))
        {
            return Expression::Reference(ReferenceExpression::new(reference_index, return_type));
        }
    }
    let reference_index = projection_exprs.len();
    payload_types.push(return_type.clone());
    projection_exprs.push(expr);
    Expression::Reference(ReferenceExpression::new(reference_index, return_type))
}

#[derive(Debug, Clone)]
pub(crate) struct PerfectHashPlanInfo {
    pub(crate) group_minima: Vec<i128>,
    pub(crate) group_cardinalities: Vec<usize>,
}

const PERFECT_HASH_RANGE_LIMIT: u128 = 1u128 << 32;
pub(crate) fn can_use_perfect_hash_aggregate(
    aggregate: &LogicalAggregate,
    groups: &[Expression],
    aggregate_exprs: &[Expression],
) -> Option<PerfectHashPlanInfo> {
    if groups.is_empty()
        || aggregate.grouping_sets.len() > 1
        || !aggregate.grouping_functions.is_empty()
        || aggregate.groups.len() != groups.len()
    {
        return None;
    }

    for aggregate_expr in aggregate_exprs {
        let Expression::Aggregate(aggregate) = aggregate_expr else {
            return None;
        };
        if aggregate.is_distinct() || !aggregate.order_bys.is_empty() {
            return None;
        }
    }

    let mut group_minima = Vec::with_capacity(aggregate.groups.len());
    let mut group_cardinalities = Vec::with_capacity(aggregate.groups.len());

    for group_idx in 0..aggregate.groups.len() {
        let group_type = aggregate.groups[group_idx].return_type();
        let group_stats = aggregate
            .group_stats
            .get(group_idx)
            .and_then(|stats| stats.as_ref());
        let domain = PerfectHashKeyDomain::try_new(group_type)?;
        let (min_value, max_value) = domain.min_max_from_stats(group_stats)?;
        let range = max_value.checked_sub(min_value)?;
        let range_u128 = u128::try_from(range).ok()?;
        if range_u128 >= PERFECT_HASH_RANGE_LIMIT {
            return None;
        }
        // One code for NULL and one-based codes for every value in the
        // inclusive range. Mixed-radix indexing consumes exactly this domain;
        // rounding each key to a power of two wastes a material fraction of a
        // large direct-addressing table.
        let cardinality = usize::try_from(range_u128.checked_add(2)?).ok()?;
        group_minima.push(min_value);
        group_cardinalities.push(cardinality);
    }

    Some(PerfectHashPlanInfo {
        group_minima,
        group_cardinalities,
    })
}

pub(crate) fn logical_name(op: &LogicalOperator) -> &'static str {
    match op {
        LogicalOperator::Get(_) => "GET",
        LogicalOperator::Filter(_) => "FILTER",
        LogicalOperator::Projection(_) => "PROJECTION",
        LogicalOperator::RowFetch(_) => "ROW_FETCH",
        LogicalOperator::ExternalProject(_) => "EXTERNAL_PROJECT",
        LogicalOperator::ExternalTable(_) => "EXTERNAL_TABLE",
        LogicalOperator::Limit(_) => "LIMIT",
        LogicalOperator::Order(_) => "ORDER",
        LogicalOperator::TopN(_) => "TOP_N",
        LogicalOperator::CreateTable(_) => "CREATE_TABLE",
        LogicalOperator::CreateRoutine(_) => "CREATE_ROUTINE",
        LogicalOperator::Alter(_) => "ALTER",
        LogicalOperator::CreateSequence(_) => "CREATE_SEQUENCE",
        LogicalOperator::CreateSchema(_) => "CREATE_SCHEMA",
        LogicalOperator::CreateIndex(_) => "CREATE_INDEX",
        LogicalOperator::CreateView(_) => "CREATE_VIEW",
        LogicalOperator::Drop(_) => "DROP",
        LogicalOperator::CreatePropertyGraph(_) => "CREATE_PROPERTY_GRAPH",
        LogicalOperator::DropPropertyGraph(_) => "DROP_PROPERTY_GRAPH",
        LogicalOperator::RefreshPropertyGraph(_) => "REFRESH_PROPERTY_GRAPH",
        LogicalOperator::Aggregate(_) => "AGGREGATE",
        LogicalOperator::Insert(_) => "INSERT",
        LogicalOperator::Delete(_) => "DELETE",
        LogicalOperator::Update(_) => "UPDATE",
        LogicalOperator::ExpressionGet(_) => "EXPRESSION_GET",
        LogicalOperator::Join(_) => "JOIN",
        LogicalOperator::DelimGet(_) => "DELIM_GET",
        LogicalOperator::DependentJoin(_) => "DEPENDENT_JOIN",
        LogicalOperator::SetOperation(_) => "SET_OPERATION",
        LogicalOperator::Distinct(_) => "DISTINCT",
        LogicalOperator::Window(_) => "WINDOW",
        LogicalOperator::Explain(_) => "EXPLAIN",
        LogicalOperator::EmptyResult(_) => "EMPTY_RESULT",
        LogicalOperator::MaterializedCTE(_) => "MATERIALIZED_CTE",
        LogicalOperator::RecursiveCTE(_) => "RECURSIVE_CTE",
        LogicalOperator::CTERef(_) => "CTE_REF",
        LogicalOperator::TableFunctionGet(_) => "TABLE_FUNCTION_GET",
        LogicalOperator::SearchScan(_) => "SEARCH_SCAN",
        LogicalOperator::FullTextFilterScan(_) => "FULL_TEXT_FILTER_SCAN",
        LogicalOperator::CopyTo(_) => "COPY_TO",
        LogicalOperator::GraphMatch(_) => "GRAPH_MATCH",
        LogicalOperator::GraphScan(_) => "GRAPH_SCAN",
        LogicalOperator::GraphExpand(_) => "GRAPH_EXPAND",
        LogicalOperator::DummyScan => "DUMMY_SCAN",
    }
}

pub(crate) fn is_read_csv_table_function(plan: &LogicalPlan) -> bool {
    matches!(
        &plan.operator,
        LogicalOperator::TableFunctionGet(get) if get.function.name.eq_ignore_ascii_case("read_csv")
    )
}

pub(crate) fn physical_output_row_type(logical: &LogicalPlan) -> Result<RowType> {
    let types = logical.types();
    let visible_names = logical.output_names();
    let names = align_output_names(visible_names.clone(), types.len(), "logical output")?;
    let identities = identities_from_visible_names(&visible_names, types.len());
    Ok(RowType::with_identities(names, types, identities))
}

pub(crate) fn physical_output_row_type_for_kind(
    logical: &LogicalPlan,
    kind: &PhysicalNodeKind,
    child_outputs: &[&RowType],
) -> Result<RowType> {
    let mut output = match kind {
        PhysicalNodeKind::GraphScan(spec) => Ok(RowType::new(
            vec!["local_vertex_id".to_string(), "rowid".to_string()],
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::GraphExpand(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::GraphShortestPath(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::RowFetch(spec) => Ok(spec.projection.as_ref().map_or_else(
            || {
                RowType::new(
                    spec.raw_output_names.to_vec(),
                    spec.raw_output_types.to_vec(),
                )
            },
            |projection| {
                RowType::new(
                    projection.output_names.to_vec(),
                    projection.output_types.to_vec(),
                )
            },
        )),
        PhysicalNodeKind::GraphProject(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::Values(spec) => Ok(RowType::with_identities(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
            spec.output_names
                .iter()
                .map(|name| {
                    spec.relation_alias.as_ref().map_or_else(
                        || ColumnIdentity::visible(name.clone()),
                        |alias| ColumnIdentity::qualified(name.clone(), alias.clone()),
                    )
                })
                .collect(),
        )),
        PhysicalNodeKind::HashJoin(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::CrossProduct(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::DelimJoin(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::DelimScan(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::Window(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::RecursiveCte(spec) => Ok(RowType::new(
            spec.column_names.to_vec(),
            spec.column_types.to_vec(),
        )),
        PhysicalNodeKind::CteScan(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        PhysicalNodeKind::Project(spec) => {
            Ok(RowType::new(spec.output_names.to_vec(), logical.types()))
        }
        PhysicalNodeKind::Aggregate(spec) => Ok(RowType::new(
            spec.output_names.to_vec(),
            spec.output_types.to_vec(),
        )),
        _ => physical_output_row_type(logical),
    }?;

    output.identities =
        physical_column_identities(logical, kind, child_outputs, &output).into_boxed_slice();
    debug_assert_eq!(output.identities.len(), output.column_count());
    Ok(output)
}

fn physical_column_identities(
    logical: &LogicalPlan,
    kind: &PhysicalNodeKind,
    child_outputs: &[&RowType],
    output: &RowType,
) -> Vec<ColumnIdentity> {
    let fallback = || {
        let visible_names = logical.output_names();
        identities_from_visible_names(&visible_names, output.column_count())
    };
    let child = |index: usize| child_outputs.get(index).copied();

    match kind {
        PhysicalNodeKind::RowsetScan(spec) => {
            let qualifier = spec.relation_alias.as_ref().map_or_else(
                || {
                    vec![
                        spec.table.schema_name().to_string(),
                        spec.table.name().to_string(),
                    ]
                },
                |alias| vec![alias.clone()],
            );
            spec.output_names
                .iter()
                .zip(spec.output_sources.iter())
                .map(|(name, source)| match source {
                    paro_planner::operator::GetColumnSource::Stored { .. } => {
                        ColumnIdentity::qualified_path(name.clone(), qualifier.clone())
                    }
                    paro_planner::operator::GetColumnSource::MatchedUtf8Prefix {
                        source_column,
                        byte_width,
                    } => ColumnIdentity::internal_named(format!(
                        "__matched_prefix_{source_column}_{byte_width}"
                    )),
                    paro_planner::operator::GetColumnSource::VirtualRowId => {
                        ColumnIdentity::internal_named("rowid")
                    }
                })
                .collect()
        }
        PhysicalNodeKind::Values(spec) => spec
            .output_names
            .iter()
            .map(|name| {
                spec.relation_alias.as_ref().map_or_else(
                    || ColumnIdentity::visible(name.clone()),
                    |alias| ColumnIdentity::qualified(name.clone(), alias.clone()),
                )
            })
            .collect(),
        PhysicalNodeKind::GraphProject(spec) => {
            let (visible_names, visible_qualifier) = match &logical.operator {
                LogicalOperator::Projection(project) => (
                    project.visible_names.as_slice(),
                    project.visible_qualifier.as_deref(),
                ),
                _ => (spec.output_names.as_ref(), None),
            };
            (0..output.column_count())
                .map(|index| {
                    let name = visible_names
                        .get(index)
                        .or_else(|| spec.output_names.get(index))
                        .expect("graph project column must have an output name");
                    visible_qualifier.map_or_else(
                        || ColumnIdentity::visible(name.clone()),
                        |qualifier| ColumnIdentity::qualified(name.clone(), qualifier),
                    )
                })
                .collect()
        }
        PhysicalNodeKind::Project(spec) => {
            let (visible_names, visible_qualifier) = match &logical.operator {
                LogicalOperator::Projection(project) => (
                    project.visible_names.clone(),
                    project.visible_qualifier.as_deref(),
                ),
                _ => (logical.output_names(), None),
            };
            spec.expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    if index < spec.visible_count {
                        let name = visible_names
                            .get(index)
                            .or_else(|| spec.output_names.get(index))
                            .expect("visible project column must have an output name");
                        return visible_qualifier.map_or_else(
                            || ColumnIdentity::visible(name.clone()),
                            |qualifier| ColumnIdentity::qualified(name.clone(), qualifier),
                        );
                    }
                    if let Expression::Reference(reference) = expression {
                        if let Some(identity) =
                            child(0).and_then(|row_type| row_type.identities.get(reference.index))
                        {
                            return identity.clone();
                        }
                    }
                    ColumnIdentity::internal_named(format!("expression_{}", index + 1))
                })
                .collect()
        }
        PhysicalNodeKind::Filter(spec) => child(0)
            .map(|input| project_identities(input, &spec.projection_map))
            .unwrap_or_else(fallback),
        PhysicalNodeKind::Sort(spec) => child(0)
            .map(|input| project_identities(input, &spec.projection_map))
            .unwrap_or_else(fallback),
        PhysicalNodeKind::Limit(_)
        | PhysicalNodeKind::TopN(_)
        | PhysicalNodeKind::EmptyResult(_) => child(0)
            .filter(|input| input.column_count() == output.column_count())
            .map(|input| input.identities.to_vec())
            .unwrap_or_else(fallback),
        PhysicalNodeKind::HashJoin(spec) => {
            let mut identities = child(0)
                .map(|input| project_identities(input, &spec.left_projection))
                .unwrap_or_default();
            if let Some(input) = child(1) {
                identities.extend(
                    spec.build_input_projection
                        .iter()
                        .take(spec.build_output_count)
                        .filter_map(|index| input.identities.get(*index).cloned()),
                );
            }
            complete_identities(identities, output, fallback)
        }
        PhysicalNodeKind::NestedLoopJoin(spec) => join_projection_identities(
            child(0),
            child(1),
            &spec.left_projection,
            &spec.right_projection,
            output,
            fallback,
        ),
        PhysicalNodeKind::SortRangeJoin(spec) => join_projection_identities(
            child(0),
            child(1),
            &spec.left_projection,
            &spec.right_projection,
            output,
            fallback,
        ),
        PhysicalNodeKind::ClassicIeJoin(spec) => join_projection_identities(
            child(0),
            child(1),
            &spec.left_projection,
            &spec.right_projection,
            output,
            fallback,
        ),
        PhysicalNodeKind::CrossProduct(_) => {
            let mut identities = child(0)
                .map(|input| input.identities.to_vec())
                .unwrap_or_default();
            if let Some(input) = child(1) {
                identities.extend(input.identities.iter().cloned());
            }
            complete_identities(identities, output, fallback)
        }
        PhysicalNodeKind::Aggregate(spec) => {
            let mut identities = spec
                .groups
                .iter()
                .enumerate()
                .map(|(group_index, group)| {
                    let payload = if let Expression::Reference(reference) = group {
                        spec.projection_exprs.get(reference.index).unwrap_or(group)
                    } else {
                        group
                    };
                    if let Expression::Reference(reference) = payload {
                        child(0)
                            .and_then(|input| input.identities.get(reference.index))
                            .cloned()
                            .unwrap_or_else(|| {
                                ColumnIdentity::internal_named(format!(
                                    "group_key_{}",
                                    group_index + 1
                                ))
                            })
                    } else {
                        ColumnIdentity::internal_named(format!("group_key_{}", group_index + 1))
                    }
                })
                .collect::<Vec<_>>();
            identities.extend((0..spec.aggregates.len()).map(|index| {
                ColumnIdentity::internal_named(format!("aggregate_state_{}", index + 1))
            }));
            identities
                .extend((0..spec.grouping_functions.len()).map(|index| {
                    ColumnIdentity::internal_named(format!("grouping_{}", index + 1))
                }));
            identities.resize(output.column_count(), ColumnIdentity::Internal);
            identities
        }
        PhysicalNodeKind::Window(spec) => {
            let mut identities = child(0)
                .map(|input| {
                    input
                        .identities
                        .iter()
                        .take(spec.input_width)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            identities.extend(
                (0..spec.expressions.len())
                    .map(|index| ColumnIdentity::internal_named(format!("window_{}", index + 1))),
            );
            identities.resize(output.column_count(), ColumnIdentity::Internal);
            identities
        }
        PhysicalNodeKind::ExternalProject(spec) => {
            let input_width = spec.input_types.len();
            let mut identities = child(0)
                .map(|input| input.identities.to_vec())
                .unwrap_or_default();
            identities.truncate(input_width);
            identities.extend(
                spec.output_names
                    .iter()
                    .skip(input_width)
                    .cloned()
                    .map(ColumnIdentity::visible),
            );
            complete_identities(identities, output, fallback)
        }
        PhysicalNodeKind::ExternalTable(spec) => {
            let worker_width = spec.worker_output_types.len();
            let visible_names = logical.output_names();
            let mut identities = visible_names
                .iter()
                .take(worker_width)
                .cloned()
                .map(ColumnIdentity::visible)
                .collect::<Vec<_>>();
            if let Some(input) = child(0) {
                identities.extend(input.identities.iter().skip(spec.argument_count).cloned());
            }
            complete_identities(identities, output, fallback)
        }
        PhysicalNodeKind::PartitionAggregateWindow(spec) => {
            let mut identities = child(0)
                .map(|input| project_identities(input, &spec.detail_columns))
                .unwrap_or_default();
            identities.resize(output.column_count(), ColumnIdentity::Internal);
            identities
        }
        PhysicalNodeKind::MaterializedCte(_) => child(1)
            .filter(|input| input.column_count() == output.column_count())
            .map(|input| input.identities.to_vec())
            .unwrap_or_else(fallback),
        PhysicalNodeKind::DelimJoin(_) => child(1)
            .filter(|input| input.column_count() == output.column_count())
            .map(|input| input.identities.to_vec())
            .unwrap_or_else(fallback),
        PhysicalNodeKind::CteScan(spec) => spec
            .output_names
            .iter()
            .cloned()
            .map(|name| ColumnIdentity::qualified(name, spec.relation_alias.clone()))
            .collect(),
        PhysicalNodeKind::VectorSearch(_)
        | PhysicalNodeKind::SparseVectorSearch(_)
        | PhysicalNodeKind::FullTextSearch(_)
        | PhysicalNodeKind::AdaptiveSearch(_) => match &logical.operator {
            LogicalOperator::SearchScan(search) => search
                .projections
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    search_projection_identity(search, expression)
                        .or_else(|| {
                            search
                                .output_names
                                .get(index)
                                .cloned()
                                .map(ColumnIdentity::visible)
                        })
                        .unwrap_or(ColumnIdentity::Internal)
                })
                .collect(),
            LogicalOperator::FullTextFilterScan(search) => get_column_identities(&search.get),
            _ => fallback(),
        },
        PhysicalNodeKind::RowFetch(spec) => {
            if let Some(projection) = &spec.projection {
                let visible_names = match &logical.operator {
                    LogicalOperator::Projection(project) => &project.visible_names,
                    _ => return fallback(),
                };
                return projection
                    .expressions
                    .iter()
                    .enumerate()
                    .map(|(index, expression)| {
                        visible_names
                            .get(index)
                            .cloned()
                            .map(ColumnIdentity::visible)
                            .or_else(|| {
                                let Expression::Reference(reference) = expression else {
                                    return None;
                                };
                                child(0)
                                    .and_then(|input| input.identities.get(reference.index))
                                    .cloned()
                            })
                            .unwrap_or(ColumnIdentity::Internal)
                    })
                    .collect();
            }
            let mut identities = child(0)
                .map(|input| input.identities.to_vec())
                .unwrap_or_default();
            for mapping in &spec.mappings {
                identities.extend(mapping.column_ids.iter().map(|column_id| {
                    logical_row_fetch_column_name(logical, mapping.table_index, *column_id)
                        .map(|name| ColumnIdentity::qualified(name, mapping.table_name.clone()))
                        .unwrap_or(ColumnIdentity::Internal)
                }));
            }
            complete_identities(identities, output, fallback)
        }
        PhysicalNodeKind::SetOperation(_) => child(0)
            .filter(|input| input.column_count() == output.column_count())
            .map(|input| {
                input
                    .identities
                    .iter()
                    .map(ColumnIdentity::without_qualifier)
                    .collect()
            })
            .unwrap_or_else(fallback),
        PhysicalNodeKind::GraphScan(_) => (0..output.column_count())
            .map(|index| match index {
                0 => ColumnIdentity::internal_named("local_vertex_id"),
                1 => ColumnIdentity::internal_named("rowid"),
                _ => ColumnIdentity::Internal,
            })
            .collect(),
        _ => fallback(),
    }
}

fn get_column_identities(get: &Get) -> Vec<ColumnIdentity> {
    let qualifier = get.relation_alias.as_ref().map_or_else(
        || {
            get.table.as_ref().map_or_else(Vec::new, |table| {
                vec![table.schema_name().to_string(), table.name().to_string()]
            })
        },
        |alias| vec![alias.clone()],
    );
    get.names
        .iter()
        .zip(get.column_sources.iter())
        .map(|(name, source)| match source {
            paro_planner::operator::GetColumnSource::Stored { .. } if qualifier.is_empty() => {
                ColumnIdentity::visible(name.clone())
            }
            paro_planner::operator::GetColumnSource::Stored { .. } => {
                ColumnIdentity::qualified_path(name.clone(), qualifier.clone())
            }
            paro_planner::operator::GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => ColumnIdentity::internal_named(format!(
                "__matched_prefix_{source_column}_{byte_width}"
            )),
            paro_planner::operator::GetColumnSource::VirtualRowId => {
                ColumnIdentity::internal_named("rowid")
            }
        })
        .collect()
}

fn search_projection_identity(
    search: &LogicalSearchScan,
    expression: &Expression,
) -> Option<ColumnIdentity> {
    let index = match expression {
        Expression::ColumnRef(column) if column.binding.table_index == search.get.table_index => {
            column.binding.column_index
        }
        Expression::Reference(reference) => reference.index,
        _ => return None,
    };
    get_column_identities(&search.get).get(index).cloned()
}

fn logical_row_fetch_column_name(
    logical: &LogicalPlan,
    table_index: usize,
    column_id: u32,
) -> Option<String> {
    let fetch = match &logical.operator {
        LogicalOperator::RowFetch(fetch) => fetch,
        LogicalOperator::Projection(project) => match &project.child.operator {
            LogicalOperator::RowFetch(fetch) => fetch,
            _ => return None,
        },
        _ => return None,
    };
    fetch
        .sources
        .iter()
        .find(|source| source.materialized_table_index == table_index)
        .and_then(|source| source.table.columns.get(column_id as usize))
        .map(|column| column.name.clone())
}

fn identities_from_visible_names(names: &[String], output_width: usize) -> Vec<ColumnIdentity> {
    (0..output_width)
        .map(|index| {
            names
                .get(index)
                .cloned()
                .map(ColumnIdentity::visible)
                .unwrap_or(ColumnIdentity::Internal)
        })
        .collect()
}

fn project_identities(input: &RowType, projection: &[usize]) -> Vec<ColumnIdentity> {
    projection
        .iter()
        .filter_map(|index| input.identities.get(*index).cloned())
        .collect()
}

fn join_projection_identities(
    left: Option<&RowType>,
    right: Option<&RowType>,
    left_projection: &[usize],
    right_projection: &[usize],
    output: &RowType,
    fallback: impl FnOnce() -> Vec<ColumnIdentity>,
) -> Vec<ColumnIdentity> {
    let mut identities = left
        .map(|input| project_identities(input, left_projection))
        .unwrap_or_default();
    if let Some(input) = right {
        identities.extend(project_identities(input, right_projection));
    }
    complete_identities(identities, output, fallback)
}

fn complete_identities(
    mut identities: Vec<ColumnIdentity>,
    output: &RowType,
    fallback: impl FnOnce() -> Vec<ColumnIdentity>,
) -> Vec<ColumnIdentity> {
    if identities.len() > output.column_count() {
        return fallback();
    }
    identities.resize(output.column_count(), ColumnIdentity::Internal);
    identities
}

pub(crate) fn align_output_names(
    mut names: Vec<String>,
    output_width: usize,
    label: &str,
) -> Result<Vec<String>> {
    if names.len() > output_width {
        return Err(paro_error::internal(format!(
            "{label} has {} names for {output_width} columns",
            names.len()
        )));
    }
    if names.len() < output_width {
        let visible_width = names.len();
        names.reserve(output_width - visible_width);
        for idx in visible_width..output_width {
            names.push(format!("__paro_hidden_{}", idx - visible_width + 1));
        }
    }
    Ok(names)
}

pub(crate) fn project_by_index<T: Clone>(
    values: &[T],
    projection_map: &[usize],
    label: &str,
) -> Result<Vec<T>> {
    projection_map
        .iter()
        .map(|&idx| {
            values.get(idx).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "{label} projection index {idx} is out of range for {} columns",
                    values.len()
                ))
            })
        })
        .collect()
}

pub(crate) fn hash_join_left_projection(join: &ComparisonJoin) -> Vec<usize> {
    match join.join_type {
        JoinType::RightSemi | JoinType::RightAnti => Vec::new(),
        _ => join.left_projection_map.to_indices(join.left.types().len()),
    }
}

pub(crate) fn hash_join_right_projection(join: &ComparisonJoin) -> Vec<usize> {
    match join.join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => Vec::new(),
        _ => join
            .right_projection_map
            .to_indices(join.right.types().len()),
    }
}

pub(crate) fn comparison_join_output_names(join: &ComparisonJoin) -> Result<Vec<String>> {
    let left_projection = hash_join_left_projection(join);
    let right_projection = hash_join_right_projection(join);
    let left_names = project_by_index(
        &join.left.output_names(),
        &left_projection,
        "comparison join left output",
    )?;
    let right_names = project_by_index(
        &join.right.output_names(),
        &right_projection,
        "comparison join right output",
    )?;
    Ok(join_output_names(join.join_type, left_names, right_names))
}

pub(crate) fn supports_typed_hash_join_type(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::Right
            | JoinType::Inner
            | JoinType::Outer
            | JoinType::Semi
            | JoinType::Anti
            | JoinType::Mark
            | JoinType::Single
            | JoinType::RightSemi
            | JoinType::RightAnti
    )
}

pub(crate) fn supports_external_hash_join_type(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
    )
}

pub(crate) fn is_hash_join_comparison(comparison: JoinComparisonType) -> bool {
    matches!(
        comparison,
        JoinComparisonType::Equal | JoinComparisonType::NotDistinctFrom
    )
}

pub(crate) fn nlj_left_projection(join: &ComparisonJoin) -> Vec<usize> {
    match join.join_type {
        JoinType::RightSemi | JoinType::RightAnti => Vec::new(),
        _ => join.left_projection_map.to_indices(join.left.types().len()),
    }
}

pub(crate) fn nlj_right_projection(join: &ComparisonJoin) -> Vec<usize> {
    match join.join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark => Vec::new(),
        _ => join
            .right_projection_map
            .to_indices(join.right.types().len()),
    }
}

pub(crate) fn join_output_names(
    join_type: JoinType,
    left_names: Vec<String>,
    right_names: Vec<String>,
) -> Vec<String> {
    match join_type {
        JoinType::Semi | JoinType::Anti => left_names,
        JoinType::RightSemi | JoinType::RightAnti => right_names,
        JoinType::Mark => {
            let mut names = left_names;
            names.push("mark".to_string());
            names
        }
        _ => {
            let mut names = left_names;
            names.extend(right_names);
            names
        }
    }
}

pub(crate) fn explain_line_expression(line: impl Into<String>) -> Box<[Expression]> {
    Box::new([Expression::Constant(ConstantExpression::new(
        Value::Varchar(line.into()),
        paro_common::types::LogicalType::Varchar,
    ))])
}

pub(crate) fn is_graph_chain(plan: &LogicalPlan) -> bool {
    matches!(
        &plan.operator,
        LogicalOperator::GraphScan(_) | LogicalOperator::GraphExpand(_)
    )
}

pub(crate) fn extract_graph_name_from_logical(plan: &LogicalPlan) -> Option<String> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.graph_name.clone()),
        LogicalOperator::GraphExpand(expand) => {
            extract_graph_name_from_logical(expand.child.as_ref())
        }
        _ => None,
    }
}

pub(crate) fn extract_schema_name_from_logical(plan: &LogicalPlan) -> Option<String> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => Some(scan.schema_name.clone()),
        LogicalOperator::GraphExpand(expand) => {
            extract_schema_name_from_logical(expand.child.as_ref())
        }
        _ => None,
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct GraphChainLayout {
    pub(crate) width: usize,
    pub(crate) output_table_index: usize,
    pub(crate) local_id_cols: HashMap<usize, usize>,
    pub(crate) rowid_cols: HashMap<usize, usize>,
}

pub(crate) fn build_graph_chain_layout(plan: &LogicalPlan) -> Result<GraphChainLayout> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let mut layout = GraphChainLayout {
                width: scan.output_types.len(),
                output_table_index: scan.output_table_index,
                ..GraphChainLayout::default()
            };
            layout.local_id_cols.insert(scan.table_index, 0);
            layout.rowid_cols.insert(scan.table_index, 1);
            Ok(layout)
        }
        LogicalOperator::GraphExpand(expand) => {
            let mut layout = build_graph_chain_layout(expand.child.as_ref())?;
            if layout.output_table_index != expand.output_table_index {
                return Err(paro_error::internal(format!(
                    "GraphExpand carrier namespace changed within a graph chain: child={}, expand={}",
                    layout.output_table_index, expand.output_table_index
                )));
            }
            let base = layout.width;
            layout.rowid_cols.insert(expand.edge_table_index, base);
            layout
                .local_id_cols
                .insert(expand.target_table_index, base + 1);
            layout
                .rowid_cols
                .insert(expand.target_table_index, base + 2);
            layout.width = expand.output_types().len();
            if layout.width != base + 3 + usize::from(expand.has_path_functions) * 3 {
                return Err(paro_error::internal(
                    "GraphExpand logical carrier width is inconsistent with its child",
                ));
            }
            Ok(layout)
        }
        _ => Err(paro_error::internal(format!(
            "Unexpected operator in graph chain layout: {:?}",
            plan.operator.op_type()
        ))),
    }
}

pub(crate) fn build_rowid_mappings_from_logical(
    plan: &LogicalPlan,
    schema_name: &str,
) -> Result<Vec<GraphRowFetchMapping>> {
    let layout = build_graph_chain_layout(plan)?;
    let mut mappings = Vec::new();
    collect_rowid_mappings_from_logical(plan, schema_name, &layout, &mut mappings)?;
    Ok(mappings)
}

pub(crate) fn collect_rowid_mappings_from_logical(
    plan: &LogicalPlan,
    schema_name: &str,
    layout: &GraphChainLayout,
    mappings: &mut Vec<GraphRowFetchMapping>,
) -> Result<()> {
    match &plan.operator {
        LogicalOperator::GraphScan(scan) => {
            let rowid_col_idx = layout
                .rowid_cols
                .get(&scan.table_index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing rowid column layout for graph scan table_index {}",
                        scan.table_index
                    ))
                })?;
            mappings.push(GraphRowFetchMapping {
                table_index: scan.table_index,
                rowid_col_idx,
                table_name: scan.vertex_info.table_name.clone(),
                schema_name: schema_name.to_string(),
            });
            Ok(())
        }
        LogicalOperator::GraphExpand(expand) => {
            collect_rowid_mappings_from_logical(
                expand.child.as_ref(),
                schema_name,
                layout,
                mappings,
            )?;

            let edge_rowid_col_idx = layout
                .rowid_cols
                .get(&expand.edge_table_index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Missing rowid column layout for graph edge table_index {}",
                        expand.edge_table_index
                    ))
                })?;
            mappings.push(GraphRowFetchMapping {
                table_index: expand.edge_table_index,
                rowid_col_idx: edge_rowid_col_idx,
                table_name: expand.edge_info.table_name.clone(),
                schema_name: schema_name.to_string(),
            });

            let target_rowid_col_idx = layout
                .rowid_cols
                .get(&expand.target_table_index)
                .copied()
                .ok_or_else(|| {
                paro_error::internal(format!(
                    "Missing rowid column layout for graph target table_index {}",
                    expand.target_table_index
                ))
            })?;
            mappings.push(GraphRowFetchMapping {
                table_index: expand.target_table_index,
                rowid_col_idx: target_rowid_col_idx,
                table_name: expand.target_table_name.clone(),
                schema_name: schema_name.to_string(),
            });
            Ok(())
        }
        _ => Err(paro_error::internal(format!(
            "Unexpected operator in rowid mapping collection: {:?}",
            plan.operator.op_type()
        ))),
    }
}

pub(crate) fn collect_graph_filters_from_logical(plan: &LogicalPlan) -> Vec<Expression> {
    let mut filters = Vec::new();
    collect_graph_filters_recursive(plan, &mut filters);
    filters
}

pub(crate) fn collect_graph_filters_recursive(plan: &LogicalPlan, filters: &mut Vec<Expression>) {
    match &plan.operator {
        LogicalOperator::GraphScan(_) => {}
        LogicalOperator::GraphExpand(expand) => {
            collect_graph_filters_recursive(expand.child.as_ref(), filters);
            if let Some(filter) = &expand.edge_filter {
                filters.push(filter.clone());
            }
            if let Some(filter) = &expand.target_filter {
                filters.push(filter.clone());
            }
        }
        _ => {}
    }
}

pub(crate) fn graph_expand_output_row_type(
    child_output: RowType,
    has_path_functions: bool,
) -> (Vec<String>, Vec<LogicalType>) {
    let mut names = child_output.names.to_vec();
    names.extend([
        "edge_rowid".to_string(),
        "target_local_id".to_string(),
        "target_rowid".to_string(),
    ]);
    let mut types = child_output.types.to_vec();
    types.extend([
        LogicalType::UBigInt,
        LogicalType::UBigInt,
        LogicalType::UBigInt,
    ]);
    if has_path_functions {
        names.extend([
            "path_length".to_string(),
            "path_vertices".to_string(),
            "path_edges".to_string(),
        ]);
        types.extend([
            LogicalType::BigInt,
            graph_path_element_list_type(),
            graph_path_element_list_type(),
        ]);
    }
    (names, types)
}

pub(crate) fn graph_hop_range(expand: &LogicalGraphExpand) -> Result<(u64, u64)> {
    match &expand.quantifier {
        Some(paro_parser::ast::PathQuantifier::Bounded { lower, upper }) => {
            Ok((*lower, upper.unwrap_or(*lower)))
        }
        Some(paro_parser::ast::PathQuantifier::Plus) => Ok((1, u64::MAX)),
        Some(paro_parser::ast::PathQuantifier::Star) => Ok((0, u64::MAX)),
        None => Ok((1, 1)),
    }
}

pub(crate) fn collect_union_all_row_literals(
    setop: &LogicalSetOperation,
) -> Result<Option<Vec<Box<[Expression]>>>> {
    let mut rows = Vec::new();
    if collect_row_literal_plan(setop.left.as_ref(), setop.types.len(), &mut rows)?
        && collect_row_literal_plan(setop.right.as_ref(), setop.types.len(), &mut rows)?
    {
        return Ok(Some(rows));
    }
    Ok(None)
}

pub(crate) fn collect_row_literal_plan(
    plan: &LogicalPlan,
    output_width: usize,
    rows: &mut Vec<Box<[Expression]>>,
) -> Result<bool> {
    match &plan.operator {
        LogicalOperator::SetOperation(setop)
            if setop.setop_type == SetOpType::Union && setop.setop_all =>
        {
            if setop.types.len() != output_width {
                return Err(paro_error::internal(format!(
                    "UNION ALL child has {} columns, expected {output_width}",
                    setop.types.len()
                )));
            }
            Ok(
                collect_row_literal_plan(setop.left.as_ref(), output_width, rows)?
                    && collect_row_literal_plan(setop.right.as_ref(), output_width, rows)?,
            )
        }
        LogicalOperator::Projection(project)
            if matches!(project.child.operator, LogicalOperator::DummyScan) =>
        {
            if project.expressions.len() != output_width {
                return Err(paro_error::internal(format!(
                    "row-literal projection has {} expressions, expected {output_width}",
                    project.expressions.len()
                )));
            }
            rows.push(project.expressions.clone().into_boxed_slice());
            Ok(true)
        }
        LogicalOperator::ExpressionGet(values) => {
            for row in &values.expressions {
                if row.len() != output_width {
                    return Err(paro_error::internal(format!(
                        "row-literal values row has {} expressions, expected {output_width}",
                        row.len()
                    )));
                }
                rows.push(row.clone().into_boxed_slice());
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}
