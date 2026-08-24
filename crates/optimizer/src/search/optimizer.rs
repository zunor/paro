// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::BuiltinIntrinsicId;
use paro_planner::binder::deep_copy::deep_copy_plan;
use paro_planner::expression::{
    Expression, ExpressionIterator, ExpressionVisitDecision, OperatorType,
};
use paro_planner::operator::{
    build_fulltext_query_stats, normalize_fulltext_config, Confidence, Filter, FullTextFilterScan,
    FullTextQueryKind, FullTextScoreMode, Get, LogicalOperator, Projection, SearchCandidate,
    SearchDecision, SearchScan, TopN,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::search::{
    DenseVectorQuery, ExactFilterMaterialization, FullTextIntent, HnswIntent,
    NormalizedSearchRequest, ProjectionSpec, SearchCostEstimate as PlannedSearchCostEstimate,
    SearchIntent, SearchRequestMode, SequentialCapability, SparseIntent,
};

use crate::context::OptimizationContext;
use crate::statistics::search_cost::{FullTextScanCostModel, VectorScanCostModel};

#[cfg(test)]
use paro_planner::binder::ir::OrderByNode;
#[cfg(test)]
use paro_planner::expression::ReferenceExpression;

const SIMPLE_CONFIG: &str = "simple";

pub struct SearchOptimizer;

impl SearchOptimizer {
    pub fn new() -> Self {
        Self
    }

    pub fn rewrite(&mut self, plan: LogicalPlan, ctx: &OptimizationContext) -> Result<LogicalPlan> {
        let plan = plan.try_map_children(|child| self.rewrite(child, ctx))?;
        self.rewrite_current(plan, ctx)
    }

    fn rewrite_current(
        &mut self,
        plan: LogicalPlan,
        ctx: &OptimizationContext,
    ) -> Result<LogicalPlan> {
        let bind_context = &ctx.bind_context;
        match &plan.operator {
            LogicalOperator::TopN(topn) => {
                if let Some(plan) = self.try_rewrite_topn(
                    deep_copy_plan(&plan, bind_context.shared().as_ref()),
                    topn,
                    ctx,
                )? {
                    return Ok(plan);
                }
                Ok(plan)
            }
            LogicalOperator::Filter(filter) => {
                if let Some(plan) = self.try_rewrite_fulltext_filter(
                    deep_copy_plan(&plan, bind_context.shared().as_ref()),
                    filter,
                    ctx,
                )? {
                    return Ok(plan);
                }
                Ok(plan)
            }
            _ => Ok(plan),
        }
    }

    fn try_rewrite_topn(
        &self,
        plan: LogicalPlan,
        topn: &TopN,
        ctx: &OptimizationContext,
    ) -> Result<Option<LogicalPlan>> {
        let Some(pattern) = extract_topn_pattern(topn) else {
            return Ok(None);
        };
        let Some((table_id, storage)) = get_search_storage(pattern.get) else {
            return Ok(None);
        };

        let candidate_filters = candidate_filters(&pattern.filters, pattern.get);
        let base_rows = base_rows(pattern.get_plan, pattern.get);
        let filtered = ctx.cost_model.estimate_filter_cardinality(
            base_rows,
            &candidate_filters,
            &ctx.column_stats,
        );
        let filter_selectivity = estimate_selectivity(base_rows, filtered.expected);
        let filter_materialization =
            exact_filter_materialization(&candidate_filters, pattern.get, storage.as_ref());
        // A generic filtered Top-K must inspect the base rows before it knows
        // which vectors survive. Output cardinality is not scan work.
        let sequential = build_sequential_capability(table_id, base_rows);

        if let Some(intent) = extract_vector_intent(
            pattern.order_expr,
            pattern.get,
            topn.hnsw_ef_hint,
            pattern.topn.orders[0].ascending,
        )? {
            let search_intent = SearchIntent::Hnsw(intent.clone());
            let Some(capability) = storage.search_capability(&search_intent) else {
                return Ok(None);
            };
            if !capability.is_queryable() {
                return Ok(None);
            }
            let Some(stats) = storage.hnsw_index_statistics(intent.column_id) else {
                return Ok(None);
            };
            let Some(search_policy) =
                storage.vector_search_policy(intent.column_id, intent.distance)
            else {
                return Ok(None);
            };
            let estimated_cost = VectorScanCostModel::estimate_hnsw_cost(
                &stats,
                topn.limit,
                filter_selectivity,
                topn.hnsw_ef_hint,
                search_policy,
                filter_materialization,
            );
            let Some(request) =
                build_topk_request(table_id, pattern.get, topn.limit, search_intent.clone())?
            else {
                return Ok(None);
            };
            let candidate = build_search_candidate(
                search_intent,
                capability,
                estimated_cost,
                filtered.expected,
                base_rows,
                filter_materialization,
            );
            let Some(decision) = select_search_decision(candidate, sequential.clone()) else {
                return Ok(None);
            };
            return Ok(Some(build_search_scan(
                plan,
                pattern,
                request,
                decision,
                candidate_filters,
            )));
        }

        if let Some(intent) = extract_sparse_intent(pattern.order_expr, pattern.get)? {
            let search_intent = SearchIntent::Sparse(intent.clone());
            let Some(capability) = storage.search_capability(&search_intent) else {
                return Ok(None);
            };
            if !capability.is_queryable() {
                return Ok(None);
            }
            let Some(stats) = storage.sparse_index_statistics(intent.column_id) else {
                return Ok(None);
            };
            let estimated_cost = VectorScanCostModel::estimate_sparse_cost(
                &stats,
                intent.query_vector.len(),
                filter_selectivity,
            );
            let Some(request) =
                build_topk_request(table_id, pattern.get, topn.limit, search_intent.clone())?
            else {
                return Ok(None);
            };
            let candidate = build_search_candidate(
                search_intent,
                capability,
                estimated_cost,
                filtered.expected,
                base_rows,
                filter_materialization,
            );
            let Some(decision) = select_search_decision(candidate, sequential.clone()) else {
                return Ok(None);
            };
            return Ok(Some(build_search_scan(
                plan,
                pattern,
                request,
                decision,
                candidate_filters,
            )));
        }

        if let Some(intent) = extract_fulltext_score_intent(pattern.order_expr, pattern.get)? {
            let search_intent = SearchIntent::FullText(intent.clone());
            let Some(capability) = storage.search_capability(&search_intent) else {
                return Ok(None);
            };
            if !capability.is_queryable() {
                return Ok(None);
            }
            let Some(stats) = capability.generation_stats.fulltext_index_statistics() else {
                return Ok(None);
            };
            let estimated_cost = FullTextScanCostModel::estimate_bm25_cost(
                &stats,
                &intent.query_stats,
                intent.score_mode,
                filter_selectivity,
            );
            let Some(request) =
                build_topk_request(table_id, pattern.get, topn.limit, search_intent.clone())?
            else {
                return Ok(None);
            };
            let candidate = build_search_candidate(
                search_intent,
                capability,
                estimated_cost,
                filtered.expected,
                base_rows,
                filter_materialization,
            );
            let Some(decision) = select_search_decision(candidate, sequential) else {
                return Ok(None);
            };
            return Ok(Some(build_search_scan(
                plan,
                pattern,
                request,
                decision,
                candidate_filters,
            )));
        }

        Ok(None)
    }

    fn try_rewrite_fulltext_filter(
        &self,
        plan: LogicalPlan,
        filter: &Filter,
        ctx: &OptimizationContext,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalOperator::Get(get) = &filter.child.operator else {
            return Ok(None);
        };
        let Some((table_id, storage)) = get_search_storage(get) else {
            return Ok(None);
        };

        for (match_idx, expr) in filter.expressions.iter().enumerate() {
            let Some(intent) = extract_fulltext_match_intent(expr, get)? else {
                continue;
            };
            let search_intent = SearchIntent::FullText(intent.clone());
            let Some(capability) = storage.search_capability(&search_intent) else {
                continue;
            };
            if !capability.is_queryable() {
                continue;
            }
            let Some(stats) = capability.generation_stats.fulltext_index_statistics() else {
                continue;
            };

            let base_rows = base_rows(filter.child.as_ref(), get);
            let candidate_filters = candidate_filters(&filter.expressions, get);
            let filter_materialization =
                exact_filter_materialization(&candidate_filters, get, storage.as_ref());
            let filtered = ctx.cost_model.estimate_filter_cardinality(
                base_rows,
                &candidate_filters,
                &ctx.column_stats,
            );
            let estimated_cost = FullTextScanCostModel::estimate_filter_cost(
                &stats,
                &intent.query_stats,
                estimate_selectivity(base_rows, filtered.expected),
            );
            let candidate = build_search_candidate(
                search_intent.clone(),
                capability,
                estimated_cost,
                filtered.expected,
                base_rows,
                filter_materialization,
            );
            let sequential = build_sequential_capability(table_id, base_rows);
            let Some(decision) = select_search_decision(candidate, sequential) else {
                continue;
            };
            let Some(request) = build_filter_request(table_id, get, search_intent)? else {
                continue;
            };

            let mut other_predicates = filter.expressions.clone();
            let match_expression = other_predicates.remove(match_idx);
            return Ok(Some(LogicalPlan {
                id: plan.id,
                stats: plan.stats,
                operator: LogicalOperator::FullTextFilterScan(FullTextFilterScan {
                    get: get.clone(),
                    request,
                    match_expression,
                    other_predicates,
                    residual_predicates: Vec::new(),
                    decision,
                }),
            }));
        }

        Ok(None)
    }
}

fn build_search_scan(
    plan: LogicalPlan,
    pattern: TopNPattern<'_>,
    request: NormalizedSearchRequest,
    decision: SearchDecision,
    candidate_filters: Vec<Expression>,
) -> LogicalPlan {
    LogicalPlan {
        id: plan.id,
        stats: plan.stats,
        operator: LogicalOperator::SearchScan(
            SearchScan::new(
                pattern.get.clone(),
                request,
                decision,
                pattern.projection.expressions.clone(),
                pattern.projection.table_index,
                candidate_filters,
                Vec::new(),
                pattern.order_expr_idx,
                pattern.order_expr.clone(),
                pattern.topn.orders[0].ascending,
                pattern.topn.limit,
            )
            .with_output_names(pattern.projection.visible_names.clone()),
        ),
    }
}

fn build_search_candidate(
    intent: SearchIntent,
    capability: paro_storage::search::SearchCapability,
    estimated_cost: f64,
    estimated_rows: u64,
    estimated_total_rows: u64,
    exact_filter_materialization: Option<ExactFilterMaterialization>,
) -> SearchCandidate {
    let estimated_cost = PlannedSearchCostEstimate::new(estimated_cost)
        .with_rows(estimated_rows)
        .with_total_rows(estimated_total_rows);
    SearchCandidate {
        intent,
        token: capability.capability_token(),
        kind: capability.kind,
        estimated_cost: Some(estimated_cost),
        exact_filter_materialization,
    }
}

fn build_sequential_capability(table_id: u64, estimated_rows: u64) -> SequentialCapability {
    SequentialCapability {
        table_id,
        estimated_cost: None,
    }
    .with_estimated_cost(
        PlannedSearchCostEstimate::new(estimated_rows.max(1) as f64).with_rows(estimated_rows),
    )
}

fn select_search_decision(
    candidate: SearchCandidate,
    sequential: SequentialCapability,
) -> Option<SearchDecision> {
    let candidate_cost = candidate.estimated_cost()?.score;
    let sequential_cost = sequential.estimated_cost?.score;
    if !candidate_cost.is_finite() || !sequential_cost.is_finite() {
        return None;
    }
    if candidate_cost >= sequential_cost {
        return None;
    }

    let ratio = candidate_cost / sequential_cost.max(1.0);
    if ratio <= 0.60 {
        Some(SearchDecision::IndexScan {
            candidate,
            confidence: Confidence::High,
        })
    } else if ratio <= 0.80 {
        Some(SearchDecision::IndexScan {
            candidate,
            confidence: Confidence::Medium,
        })
    } else {
        Some(SearchDecision::Adaptive {
            candidates: vec![candidate],
            sequential,
        })
    }
}

fn build_topk_request(
    table_id: u64,
    get: &Get,
    limit: usize,
    intent: SearchIntent,
) -> Result<Option<NormalizedSearchRequest>> {
    let Some(projections) = projection_spec(get, matches!(intent, SearchIntent::FullText(_)))
    else {
        return Ok(None);
    };
    let request = NormalizedSearchRequest {
        table_id,
        mode: SearchRequestMode::TopK { limit },
        predicate: None,
        projections,
        intents: vec![intent],
        fusion: None,
    };
    request.validate()?;
    Ok(Some(request))
}

fn build_filter_request(
    table_id: u64,
    get: &Get,
    intent: SearchIntent,
) -> Result<Option<NormalizedSearchRequest>> {
    let Some(projections) = projection_spec(get, false) else {
        return Ok(None);
    };
    let request = NormalizedSearchRequest {
        table_id,
        mode: SearchRequestMode::Filter,
        predicate: None,
        projections,
        intents: vec![intent],
        fusion: None,
    };
    request.validate()?;
    Ok(Some(request))
}

fn projection_spec(get: &Get, include_score: bool) -> Option<ProjectionSpec> {
    Some(ProjectionSpec {
        columns: (0..get.returned_types.len())
            .map(|output| get.stored_column(output).map(|column_id| column_id as u32))
            .collect::<Option<Vec<_>>>()?,
        include_score,
    })
}

fn get_search_storage(
    get: &Get,
) -> Option<(
    u64,
    std::sync::Arc<paro_storage::table::table_handle::TableHandle>,
)> {
    let table = get.get_table()?;
    let storage = table.get_storage()?.clone();
    Some((storage.tablet_id(), storage))
}

fn estimate_selectivity(base_rows: u64, filtered_rows: u64) -> f64 {
    if base_rows == 0 {
        0.0
    } else {
        (filtered_rows as f64 / base_rows as f64).clamp(0.0, 1.0)
    }
}

fn base_rows(get_plan: &LogicalPlan, get: &Get) -> u64 {
    get_plan
        .stats
        .estimated_cardinality
        .map(|estimate| estimate.expected)
        .or_else(|| {
            get.get_table()
                .and_then(|table| table.get_storage())
                .and_then(|storage| storage.total_rows().ok())
                .map(|rows| rows as u64)
        })
        .unwrap_or(1000)
}

fn candidate_filters(filters: &[Expression], get: &Get) -> Vec<Expression> {
    let mut all = filters.to_vec();
    all.extend(get.runtime_filter_expressions.iter().cloned());
    all
}

fn exact_filter_materialization(
    filters: &[Expression],
    get: &Get,
    storage: &paro_storage::table::table_handle::TableHandle,
) -> Option<ExactFilterMaterialization> {
    if filters.is_empty() {
        return None;
    }

    let mut stored_columns = Vec::new();
    let mut has_unmapped_column = false;
    for filter in filters {
        ExpressionIterator::visit(filter, &mut |expression| {
            if let Expression::ColumnRef(column) = expression {
                let stored = (column.depth == 0 && column.binding.table_index == get.table_index)
                    .then(|| get.stored_column(column.binding.column_index))
                    .flatten();
                match stored {
                    Some(column_id) => {
                        if !stored_columns.contains(&(column_id as u32)) {
                            stored_columns.push(column_id as u32);
                        }
                    }
                    None => has_unmapped_column = true,
                }
            }
            ExpressionVisitDecision::Descend
        });
    }

    if stored_columns.is_empty() || has_unmapped_column {
        return Some(ExactFilterMaterialization::ColumnScan);
    }
    let Ok((indexed_rows, total_rows)) =
        storage.complete_scalar_index_row_coverage(&stored_columns)
    else {
        return Some(ExactFilterMaterialization::ColumnScan);
    };
    Some(if total_rows > 0 && indexed_rows == total_rows {
        ExactFilterMaterialization::ScalarIndex
    } else if indexed_rows == 0 {
        ExactFilterMaterialization::ColumnScan
    } else {
        ExactFilterMaterialization::Mixed {
            indexed_rows,
            scanned_rows: total_rows.saturating_sub(indexed_rows),
        }
    })
}

struct TopNPattern<'a> {
    topn: &'a TopN,
    projection: &'a Projection,
    get_plan: &'a LogicalPlan,
    get: &'a Get,
    order_expr_idx: usize,
    order_expr: &'a Expression,
    filters: Vec<Expression>,
}

fn extract_topn_pattern(topn: &TopN) -> Option<TopNPattern<'_>> {
    if topn.offset != 0 || topn.orders.len() != 1 {
        return None;
    }
    let projection = match &topn.child.operator {
        LogicalOperator::Projection(projection) => projection,
        _ => return None,
    };
    let order_expr_idx = order_expression_index(&topn.orders[0].expression)?;
    let order_expr = projection.expressions.get(order_expr_idx)?;
    let (filters, get_plan) = find_filters_and_get_plan(projection.child.as_ref())?;
    let LogicalOperator::Get(get) = &get_plan.operator else {
        return None;
    };
    Some(TopNPattern {
        topn,
        projection,
        get_plan,
        get,
        order_expr_idx,
        order_expr,
        filters,
    })
}

fn order_expression_index(expr: &Expression) -> Option<usize> {
    match strip_casts(expr) {
        Expression::Reference(reference) => Some(reference.index),
        Expression::ColumnRef(column) => Some(column.binding.column_index),
        _ => None,
    }
}

fn find_filters_and_get_plan(mut plan: &LogicalPlan) -> Option<(Vec<Expression>, &LogicalPlan)> {
    let mut all_filters = Vec::new();
    loop {
        match &plan.operator {
            LogicalOperator::Filter(filter) => {
                all_filters.extend(filter.expressions.iter().cloned());
                plan = filter.child.as_ref();
            }
            LogicalOperator::Get(_) => {
                return Some((all_filters, plan));
            }
            _ => return None,
        }
    }
}

fn extract_vector_intent(
    expr: &Expression,
    get: &Get,
    ef: Option<usize>,
    ascending: bool,
) -> Result<Option<HnswIntent>> {
    if !ascending {
        return Ok(None);
    }
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    let distance = match func.builtin_intrinsic() {
        Some(BuiltinIntrinsicId::L2Distance) => {
            paro_storage::index::hnsw::DistanceMetric::Euclidean
        }
        Some(BuiltinIntrinsicId::L1Distance) => {
            paro_storage::index::hnsw::DistanceMetric::Manhattan
        }
        Some(BuiltinIntrinsicId::CosineDistance) => {
            paro_storage::index::hnsw::DistanceMetric::Cosine
        }
        Some(BuiltinIntrinsicId::NegativeInnerProduct) => {
            paro_storage::index::hnsw::DistanceMetric::DotProduct
        }
        _ => return Ok(None),
    };
    if func.children.len() != 2 {
        return Ok(None);
    }

    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(column_idx) = extract_scan_col_idx(left) {
        if let Some(query_vector) = extract_query_vector(right)? {
            return Ok(
                resolve_vector_column(get, column_idx).map(|column_id| HnswIntent {
                    column_id,
                    query: query_vector,
                    distance,
                    ef,
                }),
            );
        }
    }
    if let Some(column_idx) = extract_scan_col_idx(right) {
        if let Some(query_vector) = extract_query_vector(left)? {
            return Ok(
                resolve_vector_column(get, column_idx).map(|column_id| HnswIntent {
                    column_id,
                    query: query_vector,
                    distance,
                    ef,
                }),
            );
        }
    }

    Ok(None)
}

fn resolve_vector_column(get: &Get, column_idx: usize) -> Option<u32> {
    if column_idx >= get.column_types.len() {
        return None;
    }
    let column_type = &get.column_types[column_idx];
    if !matches!(column_type, LogicalType::Array(inner, _) if matches!(**inner, LogicalType::Float))
    {
        return None;
    }
    Some(get.stored_column(column_idx)? as u32)
}

fn extract_query_vector(expr: &Expression) -> Result<Option<DenseVectorQuery>> {
    match expr {
        Expression::Constant(constant) => {
            Ok(value_to_vec(&constant.value)?.map(DenseVectorQuery::Literal))
        }
        Expression::Operator(operator)
            if matches!(operator.operator_type, OperatorType::ArrayConstructor) =>
        {
            let mut values = Vec::with_capacity(operator.children.len());
            for child in &operator.children {
                let Expression::Constant(constant) = child else {
                    return Ok(None);
                };
                let Some(value) = value_to_f32(&constant.value) else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(DenseVectorQuery::Literal(values)))
        }
        Expression::Cast(cast) => {
            if let Expression::Parameter(parameter) = cast.child.as_ref() {
                if let LogicalType::Array(child, dimension) = &cast.target_type {
                    if matches!(child.as_ref(), LogicalType::Float) {
                        return Ok(Some(DenseVectorQuery::RuntimeParameter {
                            slot: parameter.slot.clone(),
                            dimension: *dimension,
                        }));
                    }
                }
            }
            if let Expression::Constant(constant) = cast.child.as_ref() {
                if let Value::Varchar(value) = &constant.value {
                    return Ok(Some(DenseVectorQuery::Literal(parse_vector_literal(
                        value,
                    )?)));
                }
            }
            extract_query_vector(cast.child.as_ref())
        }
        Expression::Parameter(parameter) => match &parameter.slot.ty {
            LogicalType::Array(child, dimension)
                if matches!(child.as_ref(), LogicalType::Float) =>
            {
                Ok(Some(DenseVectorQuery::RuntimeParameter {
                    slot: parameter.slot.clone(),
                    dimension: *dimension,
                }))
            }
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn value_to_vec(value: &Value) -> Result<Option<Vec<f32>>> {
    match value {
        Value::Array(values, _, _) | Value::List(values, _) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let Some(value) = value_to_f32(value) else {
                    return Ok(None);
                };
                out.push(value);
            }
            Ok(Some(out))
        }
        Value::Varchar(value) => Ok(Some(parse_vector_literal(value)?)),
        _ => Ok(None),
    }
}

fn value_to_f32(value: &Value) -> Option<f32> {
    let value = match value {
        Value::TinyInt(value) => *value as f32,
        Value::SmallInt(value) => *value as f32,
        Value::Integer(value) => *value as f32,
        Value::BigInt(value) => *value as f32,
        Value::UTinyInt(value) => *value as f32,
        Value::USmallInt(value) => *value as f32,
        Value::UInteger(value) => *value as f32,
        Value::UBigInt(value) => *value as f32,
        Value::Float(value) => *value,
        Value::Double(value) => *value as f32,
        _ => return None,
    };
    value.is_finite().then_some(value)
}

fn parse_vector_literal(input: &str) -> Result<Vec<f32>> {
    let trimmed = input.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(paro_common::error::invalid_value(
            "VECTOR",
            format!("Vector literal must be enclosed in brackets: {input}"),
        ));
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        let value: f32 = part.parse().map_err(|_| {
            paro_common::error::invalid_value(
                "VECTOR",
                format!("Invalid number in vector literal: {part}"),
            )
        })?;
        if !value.is_finite() {
            return Err(paro_common::error::invalid_value(
                "VECTOR",
                format!("Vector elements must be finite numbers, got: {part}"),
            ));
        }
        values.push(value);
    }
    Ok(values)
}

fn extract_sparse_intent(expr: &Expression, get: &Get) -> Result<Option<SparseIntent>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    if !matches!(
        func.builtin_intrinsic(),
        Some(BuiltinIntrinsicId::SparseDistance)
    ) || func.children.len() != 2
    {
        return Ok(None);
    }
    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(column_idx) = extract_scan_col_idx(left) {
        if let Some(query_vector) = extract_query_sparse_vector(right)? {
            return Ok(
                resolve_sparse_column(get, column_idx).map(|column_id| SparseIntent {
                    column_id,
                    query_vector,
                }),
            );
        }
    }
    if let Some(column_idx) = extract_scan_col_idx(right) {
        if let Some(query_vector) = extract_query_sparse_vector(left)? {
            return Ok(
                resolve_sparse_column(get, column_idx).map(|column_id| SparseIntent {
                    column_id,
                    query_vector,
                }),
            );
        }
    }
    Ok(None)
}

fn resolve_sparse_column(get: &Get, column_idx: usize) -> Option<u32> {
    if column_idx >= get.column_types.len() {
        return None;
    }
    matches!(get.column_types[column_idx], LogicalType::Varchar)
        .then(|| {
            get.stored_column(column_idx)
                .map(|column_id| column_id as u32)
        })
        .flatten()
}

fn extract_query_sparse_vector(
    expr: &Expression,
) -> Result<Option<paro_storage::rowset::SparseVector>> {
    match expr {
        Expression::Constant(constant) => {
            if let Value::Varchar(value) = &constant.value {
                return Ok(Some(paro_storage::rowset::SparseVector::parse(value)?));
            }
            Ok(None)
        }
        Expression::Cast(cast) => extract_query_sparse_vector(cast.child.as_ref()),
        _ => Ok(None),
    }
}

fn extract_fulltext_score_intent(expr: &Expression, get: &Get) -> Result<Option<FullTextIntent>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    match func.builtin_intrinsic() {
        Some(BuiltinIntrinsicId::Bm25) => extract_fulltext_query_from_column_and_string(
            func,
            get,
            FullTextQueryKind::Legacy,
            FullTextScoreMode::Bm25,
        ),
        Some(BuiltinIntrinsicId::Bm25ScoreInternal | BuiltinIntrinsicId::TsRank) => {
            extract_internal_fulltext_query(func, get, FullTextScoreMode::Bm25)
        }
        Some(BuiltinIntrinsicId::TsRankCd) => {
            extract_internal_fulltext_query(func, get, FullTextScoreMode::CoverDensity)
        }
        _ => Ok(None),
    }
}

fn extract_fulltext_match_intent(expr: &Expression, get: &Get) -> Result<Option<FullTextIntent>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    match func.builtin_intrinsic() {
        Some(BuiltinIntrinsicId::FullTextMatch) => extract_fulltext_query_from_column_and_string(
            func,
            get,
            FullTextQueryKind::Legacy,
            FullTextScoreMode::Bm25,
        ),
        Some(BuiltinIntrinsicId::FullTextMatchInternal) => {
            extract_internal_fulltext_query(func, get, FullTextScoreMode::Bm25)
        }
        _ => Ok(None),
    }
}

fn extract_fulltext_query_from_column_and_string(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
    query_kind: FullTextQueryKind,
    score_mode: FullTextScoreMode,
) -> Result<Option<FullTextIntent>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(left)) {
        if let Some(query_text) = extract_query_string(right)? {
            let query_stats = build_fulltext_query_stats(&query_text, SIMPLE_CONFIG, query_kind)?;
            return Ok(Some(FullTextIntent {
                column_id,
                query: query_text,
                query_kind,
                query_stats,
                config: SIMPLE_CONFIG.to_string(),
                score_mode,
            }));
        }
    }
    if let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(right)) {
        if let Some(query_text) = extract_query_string(left)? {
            let query_stats = build_fulltext_query_stats(&query_text, SIMPLE_CONFIG, query_kind)?;
            return Ok(Some(FullTextIntent {
                column_id,
                query: query_text,
                query_kind,
                query_stats,
                config: SIMPLE_CONFIG.to_string(),
                score_mode,
            }));
        }
    }
    Ok(None)
}

fn extract_internal_fulltext_query(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
    score_mode: FullTextScoreMode,
) -> Result<Option<FullTextIntent>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let Some((column_id, tsv_config)) = extract_tsvector_source(&func.children[0], get)? else {
        return Ok(None);
    };
    let Some((query_text, source_config, query_kind)) = extract_tsquery_source(&func.children[1])?
    else {
        return Ok(None);
    };
    if source_config
        .as_deref()
        .is_some_and(|config| !tsv_config.eq_ignore_ascii_case(config))
    {
        return Ok(None);
    }
    // TSQUERY values are already normalized and carry no text-search config in
    // their SQL type. A folded TSQUERY constant is therefore interpreted in
    // the indexed TSVECTOR's domain and parsed as canonical tsquery syntax.
    let query_stats = build_fulltext_query_stats(&query_text, &tsv_config, query_kind)?;
    Ok(Some(FullTextIntent {
        column_id: column_id as u32,
        query: query_text,
        query_kind,
        query_stats,
        config: tsv_config,
        score_mode,
    }))
}

fn extract_tsvector_source(expr: &Expression, get: &Get) -> Result<Option<(usize, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    if !matches!(
        func.builtin_intrinsic(),
        Some(BuiltinIntrinsicId::ToTsVector)
    ) {
        return Ok(None);
    }

    let (config_expr, text_expr) = match func.children.as_slice() {
        [text] => (None, text),
        [config, text] => (Some(config), text),
        _ => return Ok(None),
    };

    let config = match config_expr {
        Some(config_expr) => match extract_query_string(config_expr)? {
            Some(config) => match normalize_fulltext_config(&config) {
                Some(config) => config,
                None => return Ok(None),
            },
            None => return Ok(None),
        },
        None => SIMPLE_CONFIG.to_string(),
    };

    let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(text_expr)) else {
        return Ok(None);
    };
    Ok(Some((column_id as usize, config)))
}

fn extract_tsquery_source(
    expr: &Expression,
) -> Result<Option<(String, Option<String>, FullTextQueryKind)>> {
    let expr = strip_casts(expr);
    if let Expression::Constant(constant) = expr {
        return Ok(match (&constant.return_type, &constant.value) {
            (LogicalType::TsQuery, Value::Varchar(query)) => {
                Some((query.clone(), None, FullTextQueryKind::SerializedTsQuery))
            }
            _ => None,
        });
    }
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    let (config_expr, query_expr) = match func.children.as_slice() {
        [query] => (None, query),
        [config, query] => (Some(config), query),
        _ => return Ok(None),
    };
    let config = match config_expr {
        Some(config_expr) => match extract_query_string(config_expr)? {
            Some(config) => match normalize_fulltext_config(&config) {
                Some(config) => config,
                None => return Ok(None),
            },
            None => return Ok(None),
        },
        None => SIMPLE_CONFIG.to_string(),
    };
    let Some(query_text) = extract_query_string(query_expr)? else {
        return Ok(None);
    };
    let query_kind = match func.builtin_intrinsic() {
        Some(BuiltinIntrinsicId::ToTsQuery) => FullTextQueryKind::TsQuery,
        Some(BuiltinIntrinsicId::PlainToTsQuery) => FullTextQueryKind::Plain,
        Some(BuiltinIntrinsicId::PhraseToTsQuery) => FullTextQueryKind::Phrase,
        Some(BuiltinIntrinsicId::WebSearchToTsQuery) => FullTextQueryKind::WebSearch,
        _ => return Ok(None),
    };
    Ok(Some((query_text, Some(config), query_kind)))
}

fn resolve_fulltext_column(get: &Get, column_idx: Option<usize>) -> Option<u32> {
    let column_idx = column_idx?;
    if column_idx >= get.column_types.len() {
        return None;
    }
    matches!(get.column_types[column_idx], LogicalType::Varchar)
        .then(|| {
            get.stored_column(column_idx)
                .map(|column_id| column_id as u32)
        })
        .flatten()
}

fn extract_scan_col_idx(expr: &Expression) -> Option<usize> {
    match strip_casts(expr) {
        Expression::Reference(reference) => Some(reference.index),
        Expression::ColumnRef(column) => Some(column.binding.column_index),
        _ => None,
    }
}

fn extract_query_string(expr: &Expression) -> Result<Option<String>> {
    match strip_casts(expr) {
        Expression::Constant(constant) => match &constant.value {
            Value::Varchar(value) => Ok(Some(value.clone())),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn strip_casts(mut expr: &Expression) -> &Expression {
    while let Expression::Cast(cast) = expr {
        expr = cast.child.as_ref();
    }
    expr
}

#[cfg(test)]
fn rebuild_topn_from_search(search: &SearchScan) -> LogicalOperator {
    let mut child = LogicalPlan::synthetic(LogicalOperator::Get(search.get.clone()));
    if !search.absorbed_predicates.is_empty() {
        child = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            child,
            search.absorbed_predicates.clone(),
        )));
    }
    if !search.residual_predicates.is_empty() {
        child = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
            child,
            search.residual_predicates.clone(),
        )));
    }

    let projection = Projection::new(
        search.projection_table_index,
        child,
        search.projections.clone(),
    )
    .with_visible_names(search.output_names.clone());
    let projection = LogicalPlan::synthetic(LogicalOperator::Projection(projection));
    let order = OrderByNode {
        expression: Expression::Reference(ReferenceExpression::new(
            search.score_projection_index,
            search.score_expression.return_type(),
        )),
        ascending: search.order_ascending,
        nulls_first: false,
    };
    LogicalOperator::TopN(TopN::new(projection, vec![order], search.limit, 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_function::scalar::ScalarFunction;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression, FunctionExpression};
    use paro_planner::operator::ColumnBinding;
    use paro_storage::search::FullTextQueryStats;

    fn noop_scalar(
        _input: &paro_common::chunk::Chunk,
        _state: &dyn paro_function::scalar::ExpressionState,
        _result: &mut paro_common::vector::Vector,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }

    fn scalar_function(
        name: &str,
        arguments: Vec<LogicalType>,
        return_type: LogicalType,
    ) -> ScalarFunction {
        ScalarFunction::new(name.to_string(), arguments, return_type, noop_scalar)
    }

    #[test]
    fn adaptive_decision_is_used_for_close_costs() {
        let decision = select_search_decision(
            SearchCandidate {
                intent: SearchIntent::Hnsw(HnswIntent {
                    column_id: 1,
                    query: DenseVectorQuery::Literal(vec![1.0, 2.0]),
                    distance: paro_storage::index::hnsw::DistanceMetric::Euclidean,
                    ef: None,
                }),
                token: paro_storage::search::CapabilityToken {
                    definition_id: 1,
                    generation_id: 2,
                    root_version: 1,
                    capability_state: paro_storage::search::SearchCapabilityState::Queryable,
                },
                kind: paro_storage::search::SearchIndexKind::Hnsw,
                estimated_cost: Some(PlannedSearchCostEstimate::new(95.0)),
                exact_filter_materialization: None,
            },
            build_sequential_capability(7, 100),
        );
        assert!(matches!(decision, Some(SearchDecision::Adaptive { .. })));
    }

    #[test]
    fn fulltext_match_extracts_query_terms() {
        let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
        let expr = Expression::Function(FunctionExpression::new(
            scalar_function(
                "fulltext_match",
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::Boolean,
            ),
            vec![
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Varchar,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("hello world".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Boolean,
        ));

        let intent = extract_fulltext_match_intent(&expr, &get).unwrap().unwrap();
        assert_eq!(intent.column_id, 0);
        assert_eq!(intent.score_mode, FullTextScoreMode::Bm25);
        assert_eq!(intent.query, "hello world");
        assert_eq!(intent.query_stats.term_count, 2);
        assert_eq!(intent.query_stats.effective_query_terms(), 2);
    }

    #[test]
    fn internal_fulltext_match_accepts_a_folded_tsquery_constant() {
        let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
        let vector = Expression::Function(FunctionExpression::new(
            scalar_function(
                "to_tsvector",
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsVector,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(1, 0),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::TsVector,
        ));
        let expression = Expression::Function(FunctionExpression::new(
            scalar_function(
                "fulltext_match_internal",
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Boolean,
            ),
            vec![
                vector,
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("vector & database".to_string()),
                    LogicalType::TsQuery,
                )),
            ],
            LogicalType::Boolean,
        ));

        let intent = extract_fulltext_match_intent(&expression, &get)
            .unwrap()
            .unwrap();
        assert_eq!(intent.query, "vector & database");
        assert_eq!(intent.query_kind, FullTextQueryKind::SerializedTsQuery);
        assert_eq!(intent.config, "simple");
        assert_eq!(intent.query_stats.effective_query_terms(), 2);
    }

    #[test]
    fn rebuild_topn_from_search_uses_score_projection_index() {
        let search = SearchScan::new(
            Get::new_without_table(1, vec!["v".to_string()], vec![LogicalType::Varchar]),
            NormalizedSearchRequest {
                table_id: 1,
                mode: SearchRequestMode::TopK { limit: 3 },
                predicate: None,
                projections: ProjectionSpec {
                    columns: vec![0],
                    include_score: true,
                },
                intents: vec![SearchIntent::FullText(FullTextIntent {
                    column_id: 0,
                    query: "graph".to_string(),
                    query_kind: FullTextQueryKind::Legacy,
                    query_stats: FullTextQueryStats::new(1),
                    config: "simple".to_string(),
                    score_mode: FullTextScoreMode::Bm25,
                })],
                fusion: None,
            },
            SearchDecision::IndexScan {
                candidate: SearchCandidate {
                    intent: SearchIntent::FullText(FullTextIntent {
                        column_id: 0,
                        query: "graph".to_string(),
                        query_kind: FullTextQueryKind::Legacy,
                        query_stats: FullTextQueryStats::new(1),
                        config: "simple".to_string(),
                        score_mode: FullTextScoreMode::Bm25,
                    }),
                    token: paro_storage::search::CapabilityToken {
                        definition_id: 1,
                        generation_id: 1,
                        root_version: 1,
                        capability_state: paro_storage::search::SearchCapabilityState::Queryable,
                    },
                    kind: paro_storage::search::SearchIndexKind::FullText,
                    estimated_cost: Some(PlannedSearchCostEstimate::new(1.0)),
                    exact_filter_materialization: None,
                },
                confidence: Confidence::High,
            },
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(1),
                    LogicalType::Integer,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Float(0.5),
                    LogicalType::Float,
                )),
            ],
            9,
            vec![],
            vec![],
            1,
            Expression::Constant(ConstantExpression::new(
                Value::Float(0.5),
                LogicalType::Float,
            )),
            false,
            5,
        );

        let LogicalOperator::TopN(topn) = rebuild_topn_from_search(&search) else {
            panic!("expected topn");
        };
        match &topn.orders[0].expression {
            Expression::Reference(reference) => assert_eq!(reference.index, 1),
            other => panic!("expected reference, got {other:?}"),
        }
    }

    #[test]
    fn derived_scan_output_declines_stored_search_projection() {
        let mut get =
            Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
        get.append_matched_utf8_prefix(0, 2, LogicalType::Varchar);
        assert!(projection_spec(&get, false).is_none());
    }
}
