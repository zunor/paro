// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::entry::{
    CatalogEntryEnum, CatalogType, IndexType as CatalogIndexType, TableCatalogEntry,
};
use paro_common::error::Result;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_planner::binder::deep_copy::deep_copy_plan;
use paro_planner::expression::{Expression, OperatorType};
use paro_planner::operator::{
    Confidence, Filter, FullTextFilterScan, Get, LogicalOperator, Projection, SearchCandidate,
    SearchDecision, SearchScan, SearchType, TopN,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::statistics::{
    FullTextIndexStatistics, HnswIndexStatistics, SparseIndexStatistics,
};
use paro_storage::table::table_handle::TableHandle;

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

        if let Some(column_id) = extract_vector_distance_info(pattern.order_expr, pattern.get)? {
            let Some(table) = pattern.get.get_table() else {
                return Ok(None);
            };
            let Some(storage) = table.get_storage() else {
                return Ok(None);
            };
            if !storage.has_vector_index(column_id as u32) {
                return Ok(None);
            }
            let Some(stats) = storage.hnsw_index_statistics(column_id as u32) else {
                return Ok(None);
            };

            let candidate_filters = candidate_filters(&pattern.filters, pattern.get);
            let base_rows = base_rows(pattern.get_plan, pattern.get);
            let filtered = ctx.cost_model.estimate_filter_cardinality(
                base_rows,
                &candidate_filters,
                &ctx.column_stats,
            );
            let threshold = compute_hnsw_threshold(&stats, topn.limit);

            let filter_selectivity = estimate_selectivity(base_rows, filtered.expected);
            let estimated_cost = VectorScanCostModel::estimate_hnsw_cost(
                &stats,
                topn.limit,
                filter_selectivity,
                topn.hnsw_ef_hint,
            );
            let decision = build_decision(
                SearchType::HnswVector {
                    column_id: column_id as u32,
                },
                filtered,
                threshold,
                estimated_cost,
                filtered.expected as f64,
            );
            return Ok(Some(build_search_scan(
                plan,
                pattern,
                decision,
                candidate_filters,
            )));
        }

        if let Some((column_id, query_nnz)) =
            extract_sparse_vector_distance_info(pattern.order_expr, pattern.get)?
        {
            let Some(table) = pattern.get.get_table() else {
                return Ok(None);
            };
            let Some(storage) = table.get_storage() else {
                return Ok(None);
            };
            if !storage.has_sparse_index(column_id as u32) {
                return Ok(None);
            }
            let Some(stats) = storage.sparse_index_statistics(column_id as u32) else {
                return Ok(None);
            };

            let candidate_filters = candidate_filters(&pattern.filters, pattern.get);
            let base_rows = base_rows(pattern.get_plan, pattern.get);
            let filtered = ctx.cost_model.estimate_filter_cardinality(
                base_rows,
                &candidate_filters,
                &ctx.column_stats,
            );
            let threshold = compute_sparse_threshold(&stats, query_nnz);

            let filter_selectivity = estimate_selectivity(base_rows, filtered.expected);
            let estimated_cost =
                VectorScanCostModel::estimate_sparse_cost(&stats, query_nnz, filter_selectivity);
            let decision = build_decision(
                SearchType::SparseVector {
                    column_id: column_id as u32,
                },
                filtered,
                threshold,
                estimated_cost,
                filtered.expected as f64,
            );
            return Ok(Some(build_search_scan(
                plan,
                pattern,
                decision,
                candidate_filters,
            )));
        }

        if let Some(info) = extract_fulltext_score_info(pattern.order_expr, pattern.get)? {
            let Some(table) = pattern.get.get_table() else {
                return Ok(None);
            };
            let Some(storage) = table.get_storage() else {
                return Ok(None);
            };
            if !fulltext_index_pushdown_ready(
                ctx.session.as_ref(),
                table.as_ref(),
                storage.as_ref(),
                info.column_id,
                &info.config,
            ) {
                return Ok(None);
            }
            let Some(stats) = storage.fulltext_index_statistics(info.column_id as u32) else {
                return Ok(None);
            };

            let candidate_filters = candidate_filters(&pattern.filters, pattern.get);
            let base_rows = base_rows(pattern.get_plan, pattern.get);
            let filtered = ctx.cost_model.estimate_filter_cardinality(
                base_rows,
                &candidate_filters,
                &ctx.column_stats,
            );
            let threshold = compute_fulltext_threshold(&stats, info.query_terms);

            let filter_selectivity = estimate_selectivity(base_rows, filtered.expected);
            let estimated_cost = FullTextScanCostModel::estimate_bm25_cost(
                &stats,
                info.query_terms,
                filter_selectivity,
            );
            let decision = build_decision(
                SearchType::FullTextTopK {
                    column_id: info.column_id as u32,
                },
                filtered,
                threshold,
                estimated_cost,
                filtered.expected as f64,
            );
            return Ok(Some(build_search_scan(
                plan,
                pattern,
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
        let Some(table) = get.get_table() else {
            return Ok(None);
        };
        let Some(storage) = table.get_storage() else {
            return Ok(None);
        };

        for (match_idx, expr) in filter.expressions.iter().enumerate() {
            let Some(info) = extract_fulltext_match_info(expr, get)? else {
                continue;
            };
            if !fulltext_index_pushdown_ready(
                ctx.session.as_ref(),
                table.as_ref(),
                storage.as_ref(),
                info.column_id,
                &info.config,
            ) {
                continue;
            }

            let Some(stats) = storage.fulltext_index_statistics(info.column_id as u32) else {
                continue;
            };

            let base_rows = base_rows(filter.child.as_ref(), get);
            let candidate_filters = candidate_filters(&filter.expressions, get);
            let filtered = ctx.cost_model.estimate_filter_cardinality(
                base_rows,
                &candidate_filters,
                &ctx.column_stats,
            );
            let threshold = compute_fulltext_threshold(&stats, info.query_terms);
            let estimated_cost = FullTextScanCostModel::estimate_filter_cost(
                &stats,
                info.query_terms,
                estimate_selectivity(base_rows, filtered.expected),
            );
            let decision = build_decision(
                SearchType::FullTextFilter {
                    column_id: info.column_id as u32,
                },
                filtered,
                threshold,
                estimated_cost,
                filtered.expected as f64,
            );

            let mut other_predicates = filter.expressions.clone();
            let match_expression = other_predicates.remove(match_idx);
            return Ok(Some(LogicalPlan {
                id: plan.id,
                stats: plan.stats,
                operator: LogicalOperator::FullTextFilterScan(FullTextFilterScan {
                    get: get.clone(),
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
    decision: SearchDecision,
    candidate_filters: Vec<Expression>,
) -> LogicalPlan {
    LogicalPlan {
        id: plan.id,
        stats: plan.stats,
        operator: LogicalOperator::SearchScan(
            SearchScan::new(
                pattern.get.clone(),
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
            .with_output_names(pattern.projection.output_names.clone()),
        ),
    }
}

fn build_decision(
    search_type: SearchType,
    filtered: paro_planner::plan::CardinalityEstimate,
    threshold: u64,
    estimated_cost: f64,
    sequential_cost: f64,
) -> SearchDecision {
    if filtered.min > threshold {
        SearchDecision::IndexScan {
            search_type,
            estimated_cost,
            confidence: Confidence::High,
        }
    } else {
        SearchDecision::DeferToRuntime {
            candidates: vec![SearchCandidate {
                search_type,
                estimated_cost,
                threshold,
            }],
            sequential_cost,
        }
    }
}

fn estimate_selectivity(base_rows: u64, filtered_rows: u64) -> f64 {
    if base_rows == 0 {
        0.0
    } else {
        (filtered_rows as f64 / base_rows as f64).clamp(0.0, 1.0)
    }
}

fn compute_hnsw_threshold(stats: &HnswIndexStatistics, limit: usize) -> u64 {
    let indexed = stats.num_indexed_vectors.max(1) as u64;
    indexed
        .min((indexed / 32).max(limit.max(1) as u64 * 8))
        .max(1)
}

fn compute_sparse_threshold(stats: &SparseIndexStatistics, query_nnz: usize) -> u64 {
    let indexed = stats.num_indexed_vectors.max(1) as u64;
    let avg_posting = if stats.num_posting_lists == 0 {
        1
    } else {
        (stats.total_postings / stats.num_posting_lists).max(1) as u64
    };
    indexed
        .min(avg_posting.max(query_nnz.max(1) as u64 * 8))
        .max(1)
}

fn compute_fulltext_threshold(stats: &FullTextIndexStatistics, query_terms: usize) -> u64 {
    let total_docs = stats.total_docs.max(1) as u64;
    let avg_posting = if stats.unique_terms == 0 {
        1
    } else {
        (stats.total_postings / stats.unique_terms as u64).max(1)
    };
    total_docs
        .min(avg_posting.saturating_mul(query_terms.max(1) as u64))
        .max(1)
}

fn base_rows(get_plan: &LogicalPlan, get: &Get) -> u64 {
    get_plan
        .stats
        .estimated_cardinality
        .map(|estimate| estimate.expected)
        .or_else(|| {
            get.get_table()
                .and_then(|table| table.get_storage())
                .map(|storage| storage.total_rows() as u64)
        })
        .unwrap_or(1000)
}

fn candidate_filters(filters: &[Expression], get: &Get) -> Vec<Expression> {
    let mut all = filters.to_vec();
    all.extend(get.runtime_filter_expressions.iter().cloned());
    all
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

fn extract_vector_distance_info(expr: &Expression, get: &Get) -> Result<Option<usize>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    let name = func.function.name.to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "l2_distance" | "l1_distance" | "cosine_distance" | "neg_inner_product"
    ) || func.children.len() != 2
    {
        return Ok(None);
    }

    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(column_idx) = extract_scan_col_idx(left) {
        if extract_query_vector(right)?.is_some() {
            return Ok(resolve_vector_column(get, column_idx));
        }
    }
    if let Some(column_idx) = extract_scan_col_idx(right) {
        if extract_query_vector(left)?.is_some() {
            return Ok(resolve_vector_column(get, column_idx));
        }
    }

    Ok(None)
}

fn resolve_vector_column(get: &Get, column_idx: usize) -> Option<usize> {
    if column_idx >= get.column_ids.len() || column_idx >= get.column_types.len() {
        return None;
    }
    let column_type = &get.column_types[column_idx];
    if !matches!(column_type, LogicalType::Array(inner, _) if matches!(**inner, LogicalType::Float))
    {
        return None;
    }
    Some(get.column_ids[column_idx])
}

fn extract_query_vector(expr: &Expression) -> Result<Option<Vec<f32>>> {
    match expr {
        Expression::Constant(constant) => value_to_vec(&constant.value),
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
            Ok(Some(values))
        }
        Expression::Cast(cast) => {
            if let Expression::Constant(constant) = cast.child.as_ref() {
                if let Value::Varchar(value) = &constant.value {
                    return Ok(Some(parse_vector_literal(value)?));
                }
            }
            extract_query_vector(cast.child.as_ref())
        }
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

fn extract_sparse_vector_distance_info(
    expr: &Expression,
    get: &Get,
) -> Result<Option<(usize, usize)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    if !func.function.name.eq_ignore_ascii_case("sparse_distance") || func.children.len() != 2 {
        return Ok(None);
    }
    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(column_idx) = extract_scan_col_idx(left) {
        if let Some(query_nnz) = extract_query_sparse_vector_nnz(right)? {
            return Ok(
                resolve_sparse_column(get, column_idx).map(|column_id| (column_id, query_nnz))
            );
        }
    }
    if let Some(column_idx) = extract_scan_col_idx(right) {
        if let Some(query_nnz) = extract_query_sparse_vector_nnz(left)? {
            return Ok(
                resolve_sparse_column(get, column_idx).map(|column_id| (column_id, query_nnz))
            );
        }
    }
    Ok(None)
}

fn resolve_sparse_column(get: &Get, column_idx: usize) -> Option<usize> {
    if column_idx >= get.column_ids.len() || column_idx >= get.column_types.len() {
        return None;
    }
    matches!(get.column_types[column_idx], LogicalType::Varchar)
        .then_some(get.column_ids[column_idx])
}

fn extract_query_sparse_vector_nnz(expr: &Expression) -> Result<Option<usize>> {
    match expr {
        Expression::Constant(constant) => {
            if let Value::Varchar(value) = &constant.value {
                return Ok(Some(
                    paro_storage::rowset::SparseVector::parse(value)?.dims.len(),
                ));
            }
            Ok(None)
        }
        Expression::Cast(cast) => extract_query_sparse_vector_nnz(cast.child.as_ref()),
        _ => Ok(None),
    }
}

#[derive(Clone)]
struct FullTextQueryInfo {
    column_id: usize,
    query_terms: usize,
    config: String,
}

fn extract_fulltext_score_info(expr: &Expression, get: &Get) -> Result<Option<FullTextQueryInfo>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    let name = func.function.name.to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "bm25" | "bm25_score_internal" | "ts_rank" | "ts_rank_cd"
    ) {
        return Ok(None);
    }
    match name.as_str() {
        "bm25" => extract_fulltext_query_from_column_and_string(func, get),
        _ => extract_internal_fulltext_query(func, get),
    }
}

fn extract_fulltext_match_info(expr: &Expression, get: &Get) -> Result<Option<FullTextQueryInfo>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    let name = func.function.name.to_ascii_lowercase();
    if !matches!(name.as_str(), "fulltext_match" | "fulltext_match_internal") {
        return Ok(None);
    }
    match name.as_str() {
        "fulltext_match" => extract_fulltext_query_from_column_and_string(func, get),
        _ => extract_internal_fulltext_query(func, get),
    }
}

fn extract_fulltext_query_from_column_and_string(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
) -> Result<Option<FullTextQueryInfo>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(left)) {
        if let Some(query_text) = extract_query_string(right)? {
            return Ok(Some(FullTextQueryInfo {
                column_id,
                query_terms: count_query_terms(&query_text),
                config: SIMPLE_CONFIG.to_string(),
            }));
        }
    }
    if let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(right)) {
        if let Some(query_text) = extract_query_string(left)? {
            return Ok(Some(FullTextQueryInfo {
                column_id,
                query_terms: count_query_terms(&query_text),
                config: SIMPLE_CONFIG.to_string(),
            }));
        }
    }
    Ok(None)
}

fn extract_internal_fulltext_query(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
) -> Result<Option<FullTextQueryInfo>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let Some((column_id, tsv_config)) = extract_tsvector_source(&func.children[0], get)? else {
        return Ok(None);
    };
    let Some((query_text, tsq_config)) = extract_tsquery_source(&func.children[1])? else {
        return Ok(None);
    };
    if !tsv_config.eq_ignore_ascii_case(&tsq_config) {
        return Ok(None);
    }
    Ok(Some(FullTextQueryInfo {
        column_id,
        query_terms: count_query_terms(&query_text),
        config: tsv_config,
    }))
}

fn extract_tsvector_source(expr: &Expression, get: &Get) -> Result<Option<(usize, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    if !func.function.name.eq_ignore_ascii_case("to_tsvector") {
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
    Ok(Some((column_id, config)))
}

fn extract_tsquery_source(expr: &Expression) -> Result<Option<(String, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    let name = func.function.name.to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "plainto_tsquery" | "to_tsquery" | "phraseto_tsquery" | "websearch_to_tsquery"
    ) {
        return Ok(None);
    }

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
    Ok(Some((query_text, config)))
}

fn normalize_fulltext_config(config: &str) -> Option<String> {
    let normalized = config.trim().to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

fn count_query_terms(query_text: &str) -> usize {
    query_text.split_whitespace().count().max(1)
}

fn resolve_fulltext_column(get: &Get, column_idx: Option<usize>) -> Option<usize> {
    let column_idx = column_idx?;
    if column_idx >= get.column_ids.len() || column_idx >= get.column_types.len() {
        return None;
    }
    matches!(get.column_types[column_idx], LogicalType::Varchar)
        .then_some(get.column_ids[column_idx])
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

fn fulltext_index_pushdown_ready(
    session: &StatementContext,
    table_entry: &TableCatalogEntry,
    table_data: &TableHandle,
    column_id: usize,
    config: &str,
) -> bool {
    let runtime_coverage = match table_data.fulltext_index_coverage(column_id as u32) {
        Ok(coverage) => coverage,
        Err(_) => return false,
    };
    if !runtime_coverage.is_complete() {
        return false;
    }

    let txn = session.catalog_txn_view();
    let catalog = session.catalog();
    let schema = match catalog.get_schema(&txn, &table_entry.base.schema_name) {
        Ok(schema) => schema,
        Err(_) => return false,
    };

    for entry in schema
        .collection(CatalogType::Index)
        .expect("index collection")
        .scan(txn.transaction_id, txn.start_time)
    {
        let CatalogEntryEnum::Index(index) = entry.as_ref() else {
            continue;
        };
        if index.table_oid != table_entry.base.base.object_id.raw() {
            continue;
        }
        if index.index_type != CatalogIndexType::FullText || !index.is_ready() {
            continue;
        }
        let Some(binding) = index.fulltext_binding() else {
            continue;
        };
        if binding.column_id.index != column_id as u32 {
            continue;
        }
        if !binding.config.eq_ignore_ascii_case(config) {
            continue;
        }
        if let Some(coverage) = index.coverage() {
            if !coverage.is_complete() {
                continue;
            }
        }
        return true;
    }

    false
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
    .with_output_names(search.output_names.clone());
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
    use paro_planner::operator::{ColumnBinding, SearchType};

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
    fn decision_defer_is_used_for_overlapping_cardinality() {
        let decision = build_decision(
            SearchType::HnswVector { column_id: 1 },
            paro_planner::plan::CardinalityEstimate {
                min: 10,
                expected: 50,
                max: 100,
            },
            40,
            12.0,
            50.0,
        );
        assert!(matches!(decision, SearchDecision::DeferToRuntime { .. }));
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

        let info = extract_fulltext_match_info(&expr, &get).unwrap().unwrap();
        assert_eq!(info.column_id, 0);
        assert_eq!(info.query_terms, 2);
    }

    #[test]
    fn rebuild_topn_from_search_uses_score_projection_index() {
        let search = SearchScan::new(
            Get::new_without_table(1, vec!["v".to_string()], vec![LogicalType::Varchar]),
            SearchDecision::IndexScan {
                search_type: SearchType::FullTextTopK { column_id: 0 },
                estimated_cost: 1.0,
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
}
