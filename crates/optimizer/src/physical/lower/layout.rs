// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_catalog::entry::{CatalogEntry, StandardEntry};

pub(crate) fn physical_output_row_type(logical: &PreparedNode) -> Result<RowType> {
    let types = logical.types();
    let visible_names = logical.output_names();
    let names = align_output_names(visible_names.clone(), types.len(), "logical output")?;
    let identities = identities_from_visible_names(&visible_names, types.len());
    Ok(RowType::with_identities(names, types, identities))
}

pub(crate) fn physical_output_row_type_for_kind(
    logical: &PreparedNode,
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
    logical: &PreparedNode,
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
                    paro_planner::logical::operator::GetColumnSource::Stored { .. } => {
                        ColumnIdentity::qualified_path(name.clone(), qualifier.clone())
                    }
                    paro_planner::logical::operator::GetColumnSource::MatchedUtf8Prefix {
                        source_column,
                        byte_width,
                    } => ColumnIdentity::internal_named(format!(
                        "__matched_prefix_{source_column}_{byte_width}"
                    )),
                    paro_planner::logical::operator::GetColumnSource::VirtualRowId => {
                        ColumnIdentity::Locator {
                            object_id: spec.table.object_id().raw(),
                        }
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
            let mut natural_identities = child(0)
                .map(|input| project_identities(input, &spec.left_projection))
                .unwrap_or_default();
            if let Some(input) = child(1) {
                natural_identities.extend(
                    spec.build_input_projection
                        .iter()
                        .take(spec.build_output_count)
                        .filter_map(|index| input.identities.get(*index).cloned()),
                );
            }
            let mut identities = fallback();
            for (natural_index, identity) in natural_identities.into_iter().enumerate() {
                let Some(output_index) = spec.output_permutation.destination_of(natural_index)
                else {
                    continue;
                };
                if let Some(output_identity) = identities.get_mut(output_index) {
                    *output_identity = identity;
                }
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
            LogicalOperator::FullTextFilterScan(search) => {
                let identities = get_column_identities(&search.get);
                search
                    .projection_map
                    .to_indices(search.get.returned_types.len())
                    .into_iter()
                    .filter_map(|index| identities.get(index).cloned())
                    .collect()
            }
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
                    .map(|(index, _expression)| {
                        if index < projection.visible_count {
                            return visible_names
                                .get(index)
                                .cloned()
                                .map(ColumnIdentity::visible)
                                .unwrap_or(ColumnIdentity::Internal);
                        }
                        ColumnIdentity::Internal
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
            paro_planner::logical::operator::GetColumnSource::Stored { .. }
                if qualifier.is_empty() =>
            {
                ColumnIdentity::visible(name.clone())
            }
            paro_planner::logical::operator::GetColumnSource::Stored { .. } => {
                ColumnIdentity::qualified_path(name.clone(), qualifier.clone())
            }
            paro_planner::logical::operator::GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => ColumnIdentity::internal_named(format!(
                "__matched_prefix_{source_column}_{byte_width}"
            )),
            paro_planner::logical::operator::GetColumnSource::VirtualRowId => {
                get.table.as_ref().map_or_else(
                    || ColumnIdentity::internal_named("rowid"),
                    |table| ColumnIdentity::Locator {
                        object_id: table.object_id().raw(),
                    },
                )
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
    logical: &PreparedNode,
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

pub(crate) fn project_output_names(
    input: &PreparedNode,
    projection_map: &[usize],
    label: &str,
) -> Result<Vec<String>> {
    let names = align_output_names(input.output_names(), input.types().len(), label)?;
    project_by_index(&names, projection_map, label)
}

impl PhysicalPlanBuilder {
    pub(crate) fn plan_node_output(&self, id: PhysicalPlanNodeId) -> RowType {
        self.arena
            .get(id)
            .expect("synthetic physical node must exist")
            .output
            .clone()
    }
}

#[cfg(test)]
mod output_name_tests {
    use super::*;

    #[test]
    fn projected_internal_column_receives_a_physical_name() {
        let bind_context = paro_planner::binder::context::BindContext::new();
        let projection = LogicalProjection::new(
            bind_context.generate_table_index(),
            OwnedLogicalPlan::new(&bind_context, LogicalOperator::DummyScan),
            vec![
                Expression::Constant(
                    ConstantExpression::new(
                        paro_common::runtime_value::Value::Integer(1),
                        LogicalType::Integer,
                    )
                    .into(),
                ),
                Expression::Constant(
                    ConstantExpression::new(
                        paro_common::runtime_value::Value::Boolean(true),
                        LogicalType::Boolean,
                    )
                    .into(),
                ),
            ],
        )
        .with_visible_names(vec!["visible".to_string()]);
        let plan = OwnedLogicalPlan::new(&bind_context, LogicalOperator::Projection(projection));
        let plan = PreparedNode::from_owned(plan).unwrap();

        assert_eq!(
            project_output_names(&plan, &[1], "hidden projection").unwrap(),
            vec!["__paro_hidden_1"]
        );
    }
}
