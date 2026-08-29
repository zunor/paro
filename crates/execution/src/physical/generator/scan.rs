// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_planner::expression::ExpressionIterator;
use paro_storage::index::PredicateTree;

use crate::physical::specs::{SearchFilterContract, SearchPredicateTemplate};

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
        let mut column_ids = Vec::with_capacity(get.column_sources.len());
        let mut value_projections = Vec::with_capacity(get.column_sources.len());
        let mut emit_row_id = false;
        for source in &get.column_sources {
            match source {
                paro_planner::operator::GetColumnSource::Stored { column_id } => {
                    column_ids.push(*column_id);
                    value_projections.push(RowsetColumnValueProjection::Stored);
                }
                paro_planner::operator::GetColumnSource::MatchedUtf8Prefix {
                    source_column,
                    byte_width,
                } => {
                    column_ids.push(*source_column);
                    value_projections.push(rowset_value_projection(
                        *source_column,
                        *byte_width,
                        predicate.as_ref(),
                    )?);
                }
                paro_planner::operator::GetColumnSource::VirtualRowId => emit_row_id = true,
            }
        }

        let column_projection =
            RowsetColumnProjection::try_with_value_projections(column_ids, value_projections)?;

        Ok(RowsetScanSpec {
            table_index: get.table_index,
            output_names: get.names.clone().into_boxed_slice(),
            returned_types: get.returned_types.clone().into_boxed_slice(),
            output_sources: get.column_sources.clone().into_boxed_slice(),
            relation_name: get.relation_name.clone(),
            relation_alias: get.relation_alias.clone(),
            column_projection,
            emit_row_id,
            column_types: get.column_types.clone().into_boxed_slice(),
            table,
            access_policy: RowsetScanAccessPolicy::new(
                self.ctx.rowset_scan_pushdown,
                estimated_selectivity,
                self.ctx.scan_access_cost,
            ),
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
            relation_alias: values.relation_alias.clone(),
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
        if let LogicalOperator::Aggregate(aggregate) = &filter.child.operator {
            if let Some(having_filter) = rebase_aggregate_only_filter(
                filter.expressions.clone(),
                aggregate.groups.len(),
                aggregate.aggregates.len(),
            ) {
                return self.lower_aggregate_filter(filter, aggregate, having_filter);
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

    /// Attach aggregate-only HAVING predicates to the aggregate emit path
    /// while preserving the filter's independently derived output projection.
    ///
    /// HAVING dependencies and output dependencies are deliberately separate:
    /// column lifetime analysis may remove an aggregate value from the parent
    /// layout even though the value is still required to decide whether a
    /// group survives. The aggregate owns that predicate; a projection above
    /// it owns the final carrier shape.
    fn lower_aggregate_filter(
        &mut self,
        filter: &LogicalFilter,
        aggregate: &LogicalAggregate,
        having_filter: Box<[Expression]>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let aggregate_width = aggregate.returned_types.len();
        let projection = filter.projection_map.to_indices(aggregate_width);
        let (aggregate_kind, aggregate_children) =
            self.lower_aggregate_with_having(aggregate, having_filter)?;
        if filter.projection_map.is_identity(aggregate_width) {
            return Ok((aggregate_kind, aggregate_children));
        }

        let aggregate_child_outputs = aggregate_children
            .iter()
            .map(|child| {
                &self
                    .arena
                    .get(*child)
                    .expect("aggregate child must remain in the physical arena")
                    .output
            })
            .collect::<Vec<_>>();
        let aggregate_output = physical_output_row_type_for_kind(
            filter.child.as_ref(),
            &aggregate_kind,
            &aggregate_child_outputs,
        )?;
        let aggregate_label = OperatorLabel::new(filter.child.id, aggregate_kind.name());
        let aggregate_id = self.push_node(
            aggregate_kind,
            aggregate_output,
            aggregate_children,
            aggregate_label,
            filter.child.stats.estimated_cardinality,
        );

        let expressions = projection
            .iter()
            .map(|&index| {
                aggregate
                    .returned_types
                    .get(index)
                    .cloned()
                    .map(|ty| Expression::Reference(ReferenceExpression::new(index, ty)))
                    .ok_or_else(|| {
                        paro_error::internal(format!(
                            "aggregate HAVING projection index {index} is out of bounds for {aggregate_width} columns"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let output_names = project_by_index(
            &filter.child.output_names(),
            &projection,
            "aggregate HAVING output",
        )?;
        Ok((
            PhysicalNodeKind::Project(ProjectSpec {
                expressions: expressions.into_boxed_slice(),
                output_names: output_names.into_boxed_slice(),
                visible_count: 0,
            }),
            vec![aggregate_id],
        ))
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
            project_rowset_scan_spec(&mut scan_spec, get, &projection)?;
            return Ok((PhysicalNodeKind::RowsetScan(scan_spec), Vec::new()));
        }

        let child_kind = PhysicalNodeKind::RowsetScan(scan_spec);
        let child_output =
            physical_output_row_type_for_kind(filter.child.as_ref(), &child_kind, &[])?;
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
            expressions: project.expressions.clone().into_boxed_slice(),
            output_names: align_output_names(
                project.visible_names.clone(),
                project.expressions.len(),
                "project output",
            )?
            .into_boxed_slice(),
            visible_count: project.visible_count,
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
            hnsw_options: limit.hnsw_options,
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
            hnsw_options: topn.hnsw_options,
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::TopN(spec), vec![child]))
    }

    pub(crate) fn lower_search_scan(
        &mut self,
        scan: &LogicalSearchScan,
        logical: &LogicalPlan,
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
        let projection = lower_search_projection(scan)?;
        let final_output_names = align_output_names(
            scan.output_names.clone(),
            scan.get_types().len(),
            "search scan output",
        )?
        .into_boxed_slice();
        let final_output_types = scan.get_types().into_boxed_slice();
        let (source_output_names, source_output_types) = if projection.is_identity {
            (final_output_names.clone(), final_output_types.clone())
        } else {
            (
                projection.source_output_names.clone(),
                projection.source_output_types.clone(),
            )
        };
        let source = search_source_spec_for_candidate(
            table.clone(),
            candidate,
            scan,
            predicate,
            projection.projected_columns,
            true,
            source_output_names.clone(),
            source_output_types.clone(),
        )?;

        let source_kind = if matches!(scan.decision, SearchDecision::Adaptive { .. }) {
            PhysicalNodeKind::AdaptiveSearch(AdaptiveSearchSpec {
                table,
                request: scan.request.clone(),
                decision: scan.decision.clone(),
                selected: Box::new(source),
                output_names: source_output_names.clone(),
                output_types: source_output_types.clone(),
            })
        } else {
            physical_search_source_kind(source)
        };

        if projection.is_identity {
            return Ok((source_kind, Vec::new()));
        }

        let source_output = RowType::with_identities(
            source_output_names.to_vec(),
            source_output_types.to_vec(),
            source_output_names
                .iter()
                .cloned()
                .map(ColumnIdentity::internal_named)
                .collect(),
        );
        let source_id = self.push_node(
            source_kind,
            source_output,
            Vec::new(),
            OperatorLabel::new(logical.id, "SEARCH_SOURCE"),
            logical.stats.estimated_cardinality,
        );
        Ok((
            PhysicalNodeKind::Project(ProjectSpec {
                expressions: projection.expressions,
                output_names: final_output_names,
                visible_count: scan.output_names.len().min(scan.projections.len()),
            }),
            vec![source_id],
        ))
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
) -> Result<(Option<SearchPredicateTemplate>, Vec<Expression>)> {
    let (predicate_tree, mut residual) =
        predicate_builder::build_search_predicate_template(&scan.absorbed_predicates, &scan.get)?;
    residual.extend(scan.residual_predicates.clone());
    Ok((predicate_tree, residual))
}

struct SearchProjectionLowering {
    projected_columns: Box<[usize]>,
    source_output_names: Box<[String]>,
    source_output_types: Box<[LogicalType]>,
    expressions: Box<[Expression]>,
    is_identity: bool,
}

/// Lower the absorbed SQL projection against a minimal search-source schema.
///
/// A search provider owns row admission, ranking, and materialization of base
/// columns. It must not grow a second expression evaluator merely because the
/// ordered score is also used inside a visible expression. The source emits
/// each referenced stored column once followed by the canonical raw score;
/// ordinary projection evaluates every derived value above it. The common
/// `base columns..., score` shape remains an identity and avoids that operator.
fn lower_search_projection(scan: &LogicalSearchScan) -> Result<SearchProjectionLowering> {
    let mut source_indexes = Vec::new();
    for expression in &scan.projections {
        collect_search_projection_sources(
            expression,
            &scan.score_expression,
            &scan.get,
            &mut source_indexes,
        )?;
    }

    let score_index = source_indexes.len();
    let mut expressions = scan.projections.clone();
    for expression in &mut expressions {
        rebase_search_projection(
            expression,
            &scan.score_expression,
            &scan.get,
            &source_indexes,
            score_index,
        )?;
    }

    let mut projected_columns = Vec::with_capacity(source_indexes.len());
    let mut source_output_names = Vec::with_capacity(source_indexes.len() + 1);
    let mut source_output_types = Vec::with_capacity(source_indexes.len() + 1);
    for source_index in source_indexes {
        let column_id = scan.get.stored_column(source_index).ok_or_else(|| {
            paro_error::internal(
                "search projection references a Get output that is not a stored column",
            )
        })?;
        projected_columns.push(column_id);
        source_output_names.push(
            scan.get
                .names
                .get(source_index)
                .cloned()
                .ok_or_else(|| paro_error::internal("search source name is out of bounds"))?,
        );
        source_output_types.push(
            scan.get
                .returned_types
                .get(source_index)
                .cloned()
                .ok_or_else(|| paro_error::internal("search source type is out of bounds"))?,
        );
    }
    source_output_names.push("__search_score".to_string());
    source_output_types.push(scan.score_expression.return_type());

    let is_identity = expressions.len() == source_output_types.len()
        && expressions.iter().enumerate().all(|(index, expression)| {
            matches!(expression, Expression::Reference(reference) if reference.index == index)
                && expression.return_type() == source_output_types[index]
        });

    Ok(SearchProjectionLowering {
        projected_columns: projected_columns.into_boxed_slice(),
        source_output_names: source_output_names.into_boxed_slice(),
        source_output_types: source_output_types.into_boxed_slice(),
        expressions: expressions.into_boxed_slice(),
        is_identity,
    })
}

fn collect_search_projection_sources(
    expression: &Expression,
    score_expression: &Expression,
    get: &Get,
    source_indexes: &mut Vec<usize>,
) -> Result<()> {
    if expression.equals(score_expression) {
        return Ok(());
    }
    if let Some(source_index) = search_projection_source_index(expression, get)? {
        if get.stored_column(source_index).is_none() {
            return Err(paro_error::internal(
                "search projection references a Get output that is not a stored column",
            ));
        }
        if !source_indexes.contains(&source_index) {
            source_indexes.push(source_index);
        }
        return Ok(());
    }

    let mut result = Ok(());
    ExpressionIterator::enumerate_children(expression, |child| {
        if result.is_ok() {
            result =
                collect_search_projection_sources(child, score_expression, get, source_indexes);
        }
    });
    result
}

fn rebase_search_projection(
    expression: &mut Expression,
    score_expression: &Expression,
    get: &Get,
    source_indexes: &[usize],
    score_index: usize,
) -> Result<()> {
    if expression.equals(score_expression) {
        *expression = Expression::Reference(ReferenceExpression::new(
            score_index,
            score_expression.return_type(),
        ));
        return Ok(());
    }
    if let Some(source_index) = search_projection_source_index(expression, get)? {
        let rebased = source_indexes
            .iter()
            .position(|candidate| *candidate == source_index)
            .ok_or_else(|| {
                paro_error::internal("search projection source was not collected before rebasing")
            })?;
        *expression =
            Expression::Reference(ReferenceExpression::new(rebased, expression.return_type()));
        return Ok(());
    }

    let mut result = Ok(());
    ExpressionIterator::enumerate_children_mut(expression, |child| {
        if result.is_ok() {
            result =
                rebase_search_projection(child, score_expression, get, source_indexes, score_index);
        }
    });
    result
}

fn search_projection_source_index(expression: &Expression, get: &Get) -> Result<Option<usize>> {
    match expression {
        Expression::Reference(reference) => Ok(Some(reference.index)),
        Expression::ColumnRef(column) if column.binding.table_index == get.table_index => {
            Ok(Some(column.binding.column_index))
        }
        Expression::ColumnRef(_) => Err(paro_error::internal(
            "search projection references a column outside its base table",
        )),
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn search_source_spec_for_candidate(
    table: Arc<TableCatalogEntry>,
    candidate: &SearchCandidate,
    scan: &LogicalSearchScan,
    predicate: Option<SearchPredicateTemplate>,
    projected_columns: Box<[usize]>,
    emit_score: bool,
    output_names: Box<[String]>,
    output_types: Box<[LogicalType]>,
) -> Result<SearchSourceSpec> {
    // Every caller reaches this point only after proving that all remaining
    // filters are represented by `predicate`; a residual would have produced
    // an Unsupported physical node above. Make that proof construction
    // explicit and separately describe how its exact row set is obtained.
    let filter_contract = exact_search_filter_contract(predicate.as_ref());
    let filter_materialization = candidate.exact_filter_materialization;
    match &candidate.intent {
        SearchIntent::Hnsw(intent) => {
            let search_policy = table
                .storage
                .as_ref()
                .and_then(|storage| storage.vector_search_policy(intent.column_id, intent.distance))
                .ok_or_else(|| {
                    paro_error::data_corrupted(
                        "queryable HNSW candidate is missing its validated search policy",
                    )
                })?;
            let avg_level0_degree = match table.storage.as_ref() {
                Some(storage) => storage
                    .hnsw_generation_statistics(candidate.token.definition_id)?
                    .map_or(0.0, |stats| stats.avg_level0_degree),
                None => 0.0,
            };
            let graph_shard_count = table
                .storage
                .as_ref()
                .and_then(|storage| {
                    storage.search_generation_artifact_count(candidate.token.definition_id)
                })
                .unwrap_or_default();
            let filter_topology = table
                .storage
                .as_ref()
                .and_then(|storage| {
                    storage.vector_filter_topology(intent.column_id, intent.distance)
                })
                .ok_or_else(|| {
                    paro_error::data_corrupted(
                        "queryable HNSW candidate is missing its validated filter topology",
                    )
                })?;
            Ok(SearchSourceSpec::Vector(VectorSearchSpec {
                table,
                capability_token: candidate.token.clone(),
                column_id: intent.column_id as usize,
                query: intent.query.clone(),
                distance: intent.distance,
                k: scan.limit,
                params: paro_storage::index::hnsw::types::SearchParams {
                    ef: intent.options.ef,
                    rerank_window: intent.options.rerank_window,
                    objective: intent.options.objective,
                    ..Default::default()
                },
                search_policy,
                filter_topology,
                avg_level0_degree,
                graph_shard_count,
                predicate,
                filter_contract,
                filter_materialization,
                estimated_filter_rows: candidate
                    .estimated_cost()
                    .and_then(|cost| cost.estimated_rows),
                estimated_total_rows: candidate
                    .estimated_cost()
                    .and_then(|cost| cost.estimated_total_rows),
                projected_columns,
                emit_score,
                output_names,
                output_types,
            }))
        }
        SearchIntent::Sparse(intent) => Ok(SearchSourceSpec::Sparse(SparseVectorSearchSpec {
            table,
            capability_token: candidate.token.clone(),
            column_id: intent.column_id as usize,
            query_vector: intent.query_vector.clone(),
            k: scan.limit,
            predicate,
            filter_contract,
            filter_materialization,
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
            filter_contract,
            filter_materialization,
            projected_columns,
            emit_score,
            output_names,
            output_types,
        })),
    }
}

pub(super) fn exact_search_filter_contract(
    predicate: Option<&SearchPredicateTemplate>,
) -> SearchFilterContract {
    SearchFilterContract::exact_no_residual(predicate)
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
) -> Result<()> {
    let mut output_names = Vec::with_capacity(projection_map.len());
    let mut returned_types = Vec::with_capacity(projection_map.len());
    let mut output_sources = Vec::with_capacity(projection_map.len());
    let mut column_ids = Vec::with_capacity(projection_map.len());
    let mut value_projections = Vec::with_capacity(projection_map.len());
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
        let source = get.column_source(idx).ok_or_else(|| {
            paro_error::internal(format!(
                "filter projection source index {idx} is out of range for rowset output with {} columns",
                get.column_sources.len()
            ))
        })?;

        output_names.push(name);
        returned_types.push(returned_type);
        output_sources.push(source);
        match source {
            paro_planner::operator::GetColumnSource::Stored { column_id } => {
                column_ids.push(column_id);
                value_projections.push(RowsetColumnValueProjection::Stored);
                column_types.push(get.column_types[idx].clone());
            }
            paro_planner::operator::GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => {
                column_ids.push(source_column);
                value_projections.push(rowset_value_projection(
                    source_column,
                    byte_width,
                    spec.predicate.as_ref(),
                )?);
                column_types.push(get.column_types[idx].clone());
            }
            paro_planner::operator::GetColumnSource::VirtualRowId => emit_row_id = true,
        }
    }

    spec.output_names = output_names.into_boxed_slice();
    spec.returned_types = returned_types.into_boxed_slice();
    spec.output_sources = output_sources.into_boxed_slice();
    spec.column_projection =
        RowsetColumnProjection::try_with_value_projections(column_ids, value_projections)?;
    spec.column_types = column_types.into_boxed_slice();
    spec.emit_row_id = emit_row_id;
    Ok(())
}

fn rowset_value_projection(
    source_column: usize,
    byte_width: usize,
    predicate: Option<&PredicateTree>,
) -> Result<RowsetColumnValueProjection> {
    let column_id = source_column as u32;
    if byte_width == 0
        || !predicate.is_some_and(|tree| tree.proves_ascii_prefix_width(column_id, byte_width))
    {
        return Err(paro_error::internal(
            "matched-prefix scan output lacks its exact pushed predicate witness",
        ));
    }
    Ok(RowsetColumnValueProjection::MatchedUtf8Prefix { byte_width })
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
