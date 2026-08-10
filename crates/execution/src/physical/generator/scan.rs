// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::collections::HashSet;
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_planner::expression::ExpressionIterator;
use paro_storage::index::{collect_predicate_columns, PredicateTree};

impl PhysicalPlanGenerator {
    pub(crate) fn lower_get(
        &mut self,
        get: &Get,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if get.table.is_none() {
            return Ok((
                self.unsupported("GET", "base table metadata is not available"),
                Vec::new(),
            ));
        }
        let (runtime_predicate, runtime_residual) = if self.ctx.rowset_scan_pushdown {
            predicate_builder::build_predicate_tree(&get.runtime_filter_expressions, get)?
        } else {
            (None, Vec::new())
        };
        let spec = self.rowset_scan_spec(get, runtime_predicate, runtime_residual, None)?;
        Ok((PhysicalNodeKind::RowsetScan(spec), Vec::new()))
    }

    fn rowset_scan_spec(
        &self,
        get: &Get,
        predicate: Option<PredicateTree>,
        residual_predicates: Vec<Expression>,
        estimated_selectivity: Option<f64>,
    ) -> Result<RowsetScanSpec> {
        let Some(table) = get.table.clone() else {
            return Err(paro_error::internal(
                "base table metadata is not available for rowset scan",
            ));
        };
        let table_column_count = table.columns.len();
        let mut column_ids = Vec::with_capacity(get.column_ids.len());
        let mut emit_row_id = false;
        for (idx, column_id) in get.column_ids.iter().copied().enumerate() {
            if column_id < table_column_count {
                column_ids.push(column_id);
            } else if column_id == table_column_count
                && get
                    .names
                    .get(idx)
                    .is_some_and(|name| name.eq_ignore_ascii_case("rowid"))
            {
                emit_row_id = true;
            } else {
                return Err(paro_error::internal(format!(
                    "column id {column_id} is out of range for table with {table_column_count} columns"
                )));
            }
        }

        let late_materialize = should_late_materialize(
            &predicate,
            &column_ids,
            emit_row_id,
            &table,
            estimated_selectivity,
        );

        Ok(RowsetScanSpec {
            table_index: get.table_index,
            output_names: get.names.clone().into_boxed_slice(),
            returned_types: get.returned_types.clone().into_boxed_slice(),
            relation_name: get.relation_name.clone(),
            relation_alias: get.relation_alias.clone(),
            column_ids: column_ids.into_boxed_slice(),
            emit_row_id,
            column_types: get.column_types.clone().into_boxed_slice(),
            table,
            late_materialize,
            predicate,
            residual_predicates: residual_predicates.into_boxed_slice(),
            scan_order: self
                .ctx
                .rowset_scan_pushdown
                .then(|| get.scan_order.clone())
                .flatten(),
            runtime_filter_expressions: if self.ctx.rowset_scan_pushdown {
                get.runtime_filter_expressions.clone().into_boxed_slice()
            } else {
                Vec::new().into_boxed_slice()
            },
        })
    }

    pub(crate) fn lower_values(
        &mut self,
        values: &ExpressionGet,
    ) -> (PhysicalNodeKind, Vec<PhysicalPlanNodeId>) {
        let output_names = if values.names.len() == values.types.len() {
            values.names.clone()
        } else {
            (0..values.types.len())
                .map(|idx| format!("col{idx}"))
                .collect()
        };
        let expressions = values
            .expressions
            .iter()
            .cloned()
            .map(Vec::into_boxed_slice)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let spec = ValuesSpec {
            table_index: values.table_index,
            expressions,
            output_names: output_names.into_boxed_slice(),
            output_types: values.types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::Values(spec), Vec::new())
    }

    pub(crate) fn lower_empty_result(
        &mut self,
        empty: &LogicalEmptyResult,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(empty.child.as_ref())?;
        Ok((PhysicalNodeKind::EmptyResult(EmptyResultSpec), vec![child]))
    }

    pub(crate) fn lower_filter(
        &mut self,
        filter: &LogicalFilter,
        filter_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if filter
            .projection_map
            .is_identity(filter.child.types().len())
        {
            if let LogicalOperator::Aggregate(aggregate) = &filter.child.operator {
                if let Some(having_filter) = rebase_aggregate_only_filter(
                    filter.expressions.clone(),
                    aggregate.groups.len(),
                    aggregate.aggregates.len(),
                ) {
                    return self.lower_aggregate_with_having(aggregate, having_filter);
                }
            }
        }
        if self.ctx.rowset_scan_pushdown {
            if let LogicalOperator::Get(get) = &filter.child.operator {
                if get.table.is_some() {
                    return self.lower_filter_over_get(filter, get, filter_cardinality);
                }
            }
        }

        let child = self.generate_node(filter.child.as_ref())?;
        let expressions = if filter.expressions.len() <= 1 {
            filter.expressions.clone()
        } else {
            vec![Expression::Conjunction(ConjunctionExpression {
                conjunction_type: ConjunctionType::And,
                children: filter.expressions.clone(),
            })]
        };
        let spec = FilterSpec {
            expressions: expressions.into_boxed_slice(),
            projection_map: filter
                .projection_map
                .to_indices(filter.child.types().len())
                .into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Filter(spec), vec![child]))
    }

    fn lower_filter_over_get(
        &mut self,
        filter: &LogicalFilter,
        get: &Get,
        filter_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let (filter_predicate, mut residual) =
            predicate_builder::build_predicate_tree(&filter.expressions, get)?;
        let (runtime_predicate, mut runtime_residual) =
            predicate_builder::build_predicate_tree(&get.runtime_filter_expressions, get)?;
        residual.append(&mut runtime_residual);

        let predicate =
            predicate_builder::combine_predicate_trees(filter_predicate, runtime_predicate);
        // A residual predicate is evaluated above the scan, so the filter's
        // output cardinality is not a valid estimate of storage-predicate
        // selectivity in that case.
        let estimated_selectivity = residual
            .is_empty()
            .then(|| estimated_filter_selectivity(filter, filter_cardinality))
            .flatten();
        let mut scan_spec =
            self.rowset_scan_spec(get, predicate, residual.clone(), estimated_selectivity)?;

        if residual.is_empty() {
            let projection = filter.projection_map.to_indices(filter.child.types().len());
            project_rowset_scan_spec(&mut scan_spec, get, &projection, estimated_selectivity)?;
            return Ok((PhysicalNodeKind::RowsetScan(scan_spec), Vec::new()));
        }

        let child_kind = PhysicalNodeKind::RowsetScan(scan_spec);
        let child_output = physical_output_row_type_for_kind(filter.child.as_ref(), &child_kind)?;
        let child_label = OperatorLabel::new(filter.child.id, child_kind.name());
        let child_id = self.push_node(
            child_kind,
            child_output,
            Vec::new(),
            child_label,
            filter.child.stats.estimated_cardinality,
        );
        let expressions = normalize_filter_expressions(residual).into_boxed_slice();
        let spec = FilterSpec {
            expressions,
            projection_map: filter
                .projection_map
                .to_indices(filter.child.types().len())
                .into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Filter(spec), vec![child_id]))
    }

    pub(crate) fn lower_project(
        &mut self,
        project: &LogicalProjection,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(project.child.as_ref())?;
        let spec = ProjectSpec {
            table_index: project.table_index,
            expressions: project.expressions.clone().into_boxed_slice(),
            output_names: align_output_names(
                project.output_names.clone(),
                project.expressions.len(),
                "project output",
            )?
            .into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Project(spec), vec![child]))
    }

    pub(crate) fn lower_limit(
        &mut self,
        limit: &LogicalLimit,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(limit.child.as_ref())?;
        let spec = LimitSpec {
            limit: limit.limit.clone(),
            offset: limit.offset.clone(),
            hnsw_ef_hint: limit.hnsw_ef_hint,
        };
        Ok((PhysicalNodeKind::Limit(spec), vec![child]))
    }

    pub(crate) fn lower_topn(
        &mut self,
        topn: &LogicalTopN,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(topn.child.as_ref())?;
        let output_types = topn.child.types();
        let output_names =
            align_output_names(topn.child.output_names(), output_types.len(), "topn output")?;
        let spec = TopNSpec {
            orders: topn.orders.clone().into_boxed_slice(),
            limit: topn.limit,
            offset: topn.offset,
            hnsw_ef_hint: topn.hnsw_ef_hint,
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::TopN(spec), vec![child]))
    }

    pub(crate) fn lower_search_scan(
        &mut self,
        scan: &LogicalSearchScan,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let table =
            scan.get.get_table().cloned().ok_or_else(|| {
                paro_error::internal("Get missing table reference for search scan")
            })?;
        let candidate = selected_search_candidate(&scan.decision)?;
        let (predicate, residual) = search_scan_predicate(scan)?;
        if !residual.is_empty() {
            return Ok((
                self.unsupported(
                    "SEARCH_SCAN",
                    "residual search predicates require a typed filter node above the search source",
                ),
                Vec::new(),
            ));
        }
        let (projected_columns, emit_score) = direct_search_projection(scan)?;
        let output_names = align_output_names(
            scan.output_names.clone(),
            scan.get_types().len(),
            "search scan output",
        )?
        .into_boxed_slice();
        let output_types = scan.get_types().into_boxed_slice();
        let source = search_source_spec_for_candidate(
            table.clone(),
            candidate,
            scan,
            predicate,
            projected_columns,
            emit_score,
            output_names.clone(),
            output_types.clone(),
        )?;

        if matches!(scan.decision, SearchDecision::Adaptive { .. }) {
            return Ok((
                PhysicalNodeKind::AdaptiveSearch(AdaptiveSearchSpec {
                    table,
                    request: scan.request.clone(),
                    decision: scan.decision.clone(),
                    selected: Box::new(source),
                    output_names,
                    output_types,
                }),
                Vec::new(),
            ));
        }

        Ok((physical_search_source_kind(source), Vec::new()))
    }

    pub(crate) fn lower_order(
        &mut self,
        order: &LogicalOrder,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(order.child.as_ref())?;
        let child_types = order.child.types();
        let child_names = align_output_names(
            order.child.output_names(),
            child_types.len(),
            "order child output",
        )?;
        let projection = order.projection_map.to_indices(child_types.len());
        let output_names = project_by_index(&child_names, &projection, "order output")?;
        let output_types = project_by_index(&child_types, &projection, "order output")?;
        let spec = SortSpec {
            orders: order.orders.clone().into_boxed_slice(),
            projection_map: projection.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::Sort(spec), vec![child]))
    }
}

fn rebase_aggregate_only_filter(
    expressions: Vec<Expression>,
    group_count: usize,
    aggregate_count: usize,
) -> Option<Box<[Expression]>> {
    let mut expressions = normalize_filter_expressions(expressions);
    if expressions.is_empty()
        || !expressions
            .iter_mut()
            .all(|expression| rebase_aggregate_references(expression, group_count, aggregate_count))
    {
        return None;
    }
    Some(expressions.into_boxed_slice())
}

fn rebase_aggregate_references(
    expression: &mut Expression,
    group_count: usize,
    aggregate_count: usize,
) -> bool {
    match expression {
        Expression::Reference(reference) => {
            let Some(rebased) = reference.index.checked_sub(group_count) else {
                return false;
            };
            if rebased >= aggregate_count {
                return false;
            }
            reference.index = rebased;
            true
        }
        Expression::ColumnRef(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => false,
        _ => {
            let mut valid = true;
            ExpressionIterator::enumerate_children_mut(expression, |child| {
                valid &= rebase_aggregate_references(child, group_count, aggregate_count);
            });
            valid
        }
    }
}

fn selected_search_candidate(decision: &SearchDecision) -> Result<&SearchCandidate> {
    match decision {
        SearchDecision::IndexScan { candidate, .. } => Ok(candidate),
        SearchDecision::Adaptive { candidates, .. } => candidates.first().ok_or_else(|| {
            paro_error::internal("adaptive search decision has no index candidates")
        }),
    }
}

fn search_scan_predicate(
    scan: &LogicalSearchScan,
) -> Result<(Option<PredicateTree>, Vec<Expression>)> {
    let (predicate_tree, mut residual) =
        predicate_builder::build_predicate_tree(&scan.absorbed_predicates, &scan.get)?;
    residual.extend(scan.residual_predicates.clone());
    Ok((predicate_tree, residual))
}

fn direct_search_projection(scan: &LogicalSearchScan) -> Result<(Box<[usize]>, bool)> {
    if scan.score_projection_index + 1 != scan.projections.len() {
        return Err(paro_error::internal(
            "search source requires the score projection to be the final output column",
        ));
    }

    let mut projected_columns = Vec::with_capacity(scan.projections.len().saturating_sub(1));
    for (idx, expr) in scan.projections.iter().enumerate() {
        if idx == scan.score_projection_index {
            continue;
        }
        let column_id = direct_projection_column(expr, &scan.get).ok_or_else(|| {
            paro_error::internal(
                "search source can only lower direct base-column projections before score",
            )
        })?;
        projected_columns.push(column_id);
    }
    Ok((projected_columns.into_boxed_slice(), true))
}

fn direct_projection_column(expr: &Expression, get: &Get) -> Option<usize> {
    let source_index = match expr {
        Expression::Reference(reference) => reference.index,
        Expression::ColumnRef(column) => column.binding.column_index,
        _ => return None,
    };
    get.column_ids.get(source_index).copied()
}

#[allow(clippy::too_many_arguments)]
fn search_source_spec_for_candidate(
    table: Arc<TableCatalogEntry>,
    candidate: &SearchCandidate,
    scan: &LogicalSearchScan,
    predicate: Option<PredicateTree>,
    projected_columns: Box<[usize]>,
    emit_score: bool,
    output_names: Box<[String]>,
    output_types: Box<[LogicalType]>,
) -> Result<SearchSourceSpec> {
    match &candidate.intent {
        SearchIntent::Hnsw(intent) => Ok(SearchSourceSpec::Vector(VectorSearchSpec {
            table,
            capability_token: candidate.token.clone(),
            column_id: intent.column_id as usize,
            query_vector: intent.query_vector.clone(),
            k: scan.limit,
            params: paro_storage::index::hnsw::types::SearchParams::default(),
            predicate,
            projected_columns,
            emit_score,
            output_names,
            output_types,
        })),
        SearchIntent::Sparse(intent) => Ok(SearchSourceSpec::Sparse(SparseVectorSearchSpec {
            table,
            capability_token: candidate.token.clone(),
            column_id: intent.column_id as usize,
            query_vector: intent.query_vector.clone(),
            k: scan.limit,
            predicate,
            projected_columns,
            emit_score,
            output_names,
            output_types,
        })),
        SearchIntent::FullText(intent) => Ok(SearchSourceSpec::FullText(FullTextSearchSpec {
            table,
            capability_token: candidate.token.clone(),
            column_id: intent.column_id as usize,
            query: intent.query.clone(),
            query_kind: intent.query_kind,
            query_stats: intent.query_stats,
            config: intent.config.clone(),
            score_mode: intent.score_mode,
            mode: SearchRequestMode::TopK { limit: scan.limit },
            predicate,
            projected_columns,
            emit_score,
            output_names,
            output_types,
        })),
    }
}

fn physical_search_source_kind(source: SearchSourceSpec) -> PhysicalNodeKind {
    match source {
        SearchSourceSpec::Vector(spec) => PhysicalNodeKind::VectorSearch(spec),
        SearchSourceSpec::Sparse(spec) => PhysicalNodeKind::SparseVectorSearch(spec),
        SearchSourceSpec::FullText(spec) => PhysicalNodeKind::FullTextSearch(spec),
    }
}

fn normalize_filter_expressions(expressions: Vec<Expression>) -> Vec<Expression> {
    if expressions.len() <= 1 {
        expressions
    } else {
        vec![Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: expressions,
        })]
    }
}

fn project_rowset_scan_spec(
    spec: &mut RowsetScanSpec,
    get: &Get,
    projection_map: &[usize],
    estimated_selectivity: Option<f64>,
) -> Result<()> {
    let table_column_count = spec.table.columns.len();
    let mut output_names = Vec::with_capacity(projection_map.len());
    let mut returned_types = Vec::with_capacity(projection_map.len());
    let mut column_ids = Vec::with_capacity(projection_map.len());
    let mut column_types = Vec::with_capacity(projection_map.len());
    let mut emit_row_id = false;

    for &idx in projection_map {
        let name = get.names.get(idx).cloned().ok_or_else(|| {
            paro_error::internal(format!(
                "filter projection index {idx} is out of range for rowset output with {} columns",
                get.names.len()
            ))
        })?;
        let returned_type = get.returned_types.get(idx).cloned().ok_or_else(|| {
            paro_error::internal(format!(
                "filter projection type index {idx} is out of range for rowset output with {} columns",
                get.returned_types.len()
            ))
        })?;
        let column_id = *get.column_ids.get(idx).ok_or_else(|| {
            paro_error::internal(format!(
                "filter projection column index {idx} is out of range for rowset output with {} columns",
                get.column_ids.len()
            ))
        })?;

        output_names.push(name);
        returned_types.push(returned_type);
        if column_id < table_column_count {
            column_ids.push(column_id);
            let column_type = get.column_types.get(idx).cloned().ok_or_else(|| {
                paro_error::internal(format!(
                    "filter projection column type index {idx} is out of range for rowset output with {} columns",
                    get.column_types.len()
                ))
            })?;
            column_types.push(column_type);
        } else if column_id == table_column_count {
            emit_row_id = true;
        } else {
            return Err(paro_error::internal(format!(
                "filter projection column id {column_id} is out of range for table with {table_column_count} columns"
            )));
        }
    }

    spec.output_names = output_names.into_boxed_slice();
    spec.returned_types = returned_types.into_boxed_slice();
    spec.column_ids = column_ids.into_boxed_slice();
    spec.column_types = column_types.into_boxed_slice();
    spec.emit_row_id = emit_row_id;
    spec.late_materialize = should_late_materialize(
        &spec.predicate,
        &spec.column_ids,
        emit_row_id,
        &spec.table,
        estimated_selectivity,
    );
    Ok(())
}

fn estimated_filter_selectivity(
    filter: &LogicalFilter,
    filter_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
) -> Option<f64> {
    let input = filter.child.stats.estimated_cardinality?.expected;
    let output = filter_cardinality?.expected;
    if input == 0 {
        return Some(0.0);
    }
    Some((output as f64 / input as f64).clamp(0.0, 1.0))
}

/// Choose between eager sequential decoding and late row-id gathering.
///
/// The model compares bytes touched rather than query shapes. Sequential input
/// costs one unit per byte, while sparse gathers carry an additional access
/// penalty because every selected value performs row lookup and scatter work.
/// Unknown selectivity deliberately favors late materialization, which bounds
/// work for selective runtime filters without specializing for any workload.
fn should_late_materialize(
    predicate: &Option<PredicateTree>,
    column_ids: &[usize],
    emit_row_id: bool,
    table: &TableCatalogEntry,
    estimated_selectivity: Option<f64>,
) -> bool {
    const UNKNOWN_SELECTIVITY: f64 = 0.25;
    const GATHER_ACCESS_PENALTY: f64 = 2.0;

    let Some(predicate) = predicate else {
        return false;
    };
    let predicate_columns = collect_predicate_columns(predicate);
    if predicate_columns.is_empty() {
        return false;
    }

    let output_columns = if column_ids.is_empty() && !emit_row_id {
        (0..table.columns.len()).collect::<Vec<_>>()
    } else {
        column_ids.to_vec()
    };
    let output_columns = output_columns.into_iter().collect::<HashSet<_>>();
    let deferred_width = output_columns
        .iter()
        .filter(|column_id| !predicate_columns.contains(&(**column_id as u32)))
        .filter_map(|column_id| table.columns.get(*column_id))
        .map(|column| column.logical_type.physical_size().max(1))
        .sum::<usize>();
    if deferred_width == 0 {
        return false;
    }

    let predicate_width = predicate_columns
        .iter()
        .filter_map(|column_id| table.columns.get(*column_id as usize))
        .map(|column| column.logical_type.physical_size().max(1))
        .sum::<usize>();
    let predicate_column_ids = predicate_columns
        .iter()
        .map(|column_id| *column_id as usize)
        .collect::<HashSet<_>>();
    let eager_width = output_columns
        .union(&predicate_column_ids)
        .filter_map(|column_id| table.columns.get(*column_id))
        .map(|column| column.logical_type.physical_size().max(1))
        .sum::<usize>();
    let selectivity = estimated_selectivity
        .unwrap_or(UNKNOWN_SELECTIVITY)
        .clamp(0.0, 1.0);
    let late_cost =
        predicate_width as f64 + selectivity * deferred_width as f64 * GATHER_ACCESS_PENALTY;
    late_cost < eager_width as f64
}
