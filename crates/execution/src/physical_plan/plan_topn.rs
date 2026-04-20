// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan TopN - Convert TopN to TopN
//!
//!
//! ## Design Notes
//! - Converts TopN to TopN
//! - TopN uses a heap to maintain top N elements

use super::generator::PhysicalPlanGenerator;
use super::predicate_builder;
use crate::operator::filter::Filter;
use crate::operator::helper::order::Order;
use crate::operator::helper::streaming_limit::StreamingLimit;
use crate::operator::helper::topn::TopN;
use crate::operator::projection::Projection;
use crate::operator::search::fulltext_search::{
    FullTextExecMode, FullTextQueryKind, FullTextScanBindData, PhysicalFullTextScan,
};
use crate::operator::search::sparse_search::{PhysicalSparseVectorScan, SparseVectorScanBindData};
use crate::operator::search::vector_search::{PhysicalVectorScan, VectorScanBindData};
use crate::operator::PhysicalOperator;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{
    ConjunctionExpression, ConjunctionType, Expression, OperatorType, ReferenceExpression,
};
use paro_planner::operator::TopN as LogicalTopN;
use paro_planner::operator::{
    build_fulltext_query_stats as build_planner_fulltext_query_stats, FullTextQueryStats,
    FullTextQueryStatsKind, FullTextScoreMode, Get, LogicalOperator,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::index::fulltext::tokenizer::TokenizerKind;
use paro_storage::index::hnsw::types::{AcornParams, SearchParams, ACORN_MAX_SELECTIVITY_DEFAULT};
use paro_storage::rowset::SparseVector;
use paro_storage::search::{HnswIntent, SearchIntent, SparseIntent};
use paro_storage::table::table_handle::TableHandle;
use std::sync::Arc;

const SIMPLE_CONFIG: &str = "simple";
pub const TOPN_EXTERNAL_THRESHOLD: usize = 5_000;
const FORCE_EXTERNAL_SETTING: &str = "force_external";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextScoreInfo {
    pub(crate) text_column_id: usize,
    pub(crate) query_text: String,
    pub(crate) query_kind: FullTextQueryKind,
    pub(crate) score_mode: FullTextScoreMode,
    pub(crate) query_stats: FullTextQueryStats,
    pub(crate) config: String,
}

impl PhysicalPlanGenerator {
    /// Create physical plan for TopN.
    ///
    /// Converts TopN to TopN.
    pub fn create_plan_topn(
        &self,
        topn: &LogicalTopN,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let force_external = self
            .context
            .get_setting(FORCE_EXTERNAL_SETTING)
            .and_then(setting_to_bool)
            .unwrap_or(false);
        if should_use_order_limit_fallback(topn.total_rows(), force_external) {
            return self.create_topn_sort_limit_fallback(topn, child);
        }

        // Get output types from child
        let types = child.types().to_vec();

        // Create physical TopN operator
        let physical_topn = TopN::new(types, topn.orders.clone(), topn.limit, topn.offset, child);

        Ok(Arc::new(physical_topn))
    }

    fn create_topn_sort_limit_fallback(
        &self,
        topn: &LogicalTopN,
        child: Arc<dyn PhysicalOperator>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let types = child.types().to_vec();
        let order: Arc<dyn PhysicalOperator> = Arc::new(Order::new(
            types.clone(),
            topn.orders.clone(),
            Vec::new(),
            child,
            false,
        )?);
        let order = self.annotate_schema(
            order.clone(),
            self.passthrough_schema(&order, topn.child.output_names()),
        );
        let limit: Arc<dyn PhysicalOperator> = Arc::new(StreamingLimit::new(
            types,
            Some(topn.limit),
            Some(topn.offset),
            false,
            order,
        ));
        Ok(self.annotate_schema(
            limit.clone(),
            self.passthrough_schema(&limit, topn.child.output_names()),
        ))
    }

    /// Try to replace TopN (ORDER BY distance LIMIT k) with a vector index scan.
    ///
    /// Returns Some(plan) if we can build a vector scan plan, or None to fall back
    /// to the regular TopN pipeline.
    pub fn try_create_vector_scan(
        &self,
        topn: &LogicalTopN,
    ) -> Result<Option<Arc<dyn PhysicalOperator>>> {
        if topn.offset != 0 || topn.orders.len() != 1 {
            return Ok(None);
        }

        let order = &topn.orders[0];
        if !order.ascending {
            return Ok(None);
        }

        let projection = match &topn.child.operator {
            LogicalOperator::Projection(p) => p,
            _ => return Ok(None),
        };

        let order_expr_idx = match order_expression_index(&order.expression) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let order_expr = match projection.expressions.get(order_expr_idx) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let (filters, get) = match find_filters_and_get(projection.child.as_ref()) {
            Some(result) => result,
            None => return Ok(None),
        };

        let (vector_col_id, query_vec) = match extract_vector_distance(order_expr, get)? {
            Some(result) => result,
            None => return Ok(None),
        };

        let filters_owned: Vec<Expression> = filters.iter().map(|&e| e.clone()).collect();
        let (predicate_tree, residual) =
            predicate_builder::build_predicate_tree(&filters_owned, get)?;

        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for vector scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for vector scan"))?
            .clone();

        if table_data
            .search_capability(&SearchIntent::Hnsw(HnswIntent {
                column_id: vector_col_id as u32,
                query_vector: query_vec.clone(),
            }))
            .is_none()
        {
            return Ok(None);
        }

        let mut bind = VectorScanBindData::new(
            table_data,
            query_vec,
            topn.limit,
            vector_col_id,
            get.column_ids.clone(),
        );
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }
        // Enable ACORN by default for filtered searches (selectivity <= 0.4).
        bind = bind.with_params(SearchParams {
            ef: topn.hnsw_ef_hint,
            acorn: Some(AcornParams {
                enable: true,
                max_selectivity: Some(ACORN_MAX_SELECTIVITY_DEFAULT),
            }),
            random_entry_point: None,
        });

        let mut scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalVectorScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        if !residual.is_empty() {
            let residual_predicate = if residual.len() == 1 {
                residual[0].clone()
            } else {
                Expression::Conjunction(ConjunctionExpression {
                    conjunction_type: ConjunctionType::And,
                    children: residual,
                })
            };
            let filter: Arc<dyn PhysicalOperator> =
                Arc::new(Filter::new(residual_predicate, scan.clone()));
            scan = self.annotate_schema(filter, self.passthrough_schema(&scan, get.names.clone()));
        }

        let projection = Arc::new(Projection::new(projection.expressions.clone(), scan));

        Ok(Some(projection))
    }

    pub fn try_create_sparse_vector_scan(
        &self,
        topn: &LogicalTopN,
    ) -> Result<Option<Arc<dyn PhysicalOperator>>> {
        if topn.offset != 0 || topn.orders.len() != 1 {
            return Ok(None);
        }

        let order = &topn.orders[0];
        // Sparse distance (dot product) should be descending for top matches.
        if order.ascending {
            return Ok(None);
        }

        let projection = match &topn.child.operator {
            LogicalOperator::Projection(p) => p,
            _ => return Ok(None),
        };

        let order_expr_idx = match order_expression_index(&order.expression) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let order_expr = match projection.expressions.get(order_expr_idx) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let (filters, get) = match find_filters_and_get(projection.child.as_ref()) {
            Some(result) => result,
            None => return Ok(None),
        };

        let (sparse_col_id, query_vec) = match extract_sparse_vector_distance(order_expr, get)? {
            Some(result) => result,
            None => return Ok(None),
        };

        let filters_owned: Vec<Expression> = filters.iter().map(|&e| e.clone()).collect();
        let (predicate_tree, residual) =
            predicate_builder::build_predicate_tree(&filters_owned, get)?;
        if !residual.is_empty() {
            return Ok(None);
        }

        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for sparse scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for sparse scan"))?
            .clone();

        if table_data
            .search_capability(&SearchIntent::Sparse(SparseIntent {
                column_id: sparse_col_id as u32,
                query_vector: query_vec.clone(),
            }))
            .is_none()
        {
            return Ok(None);
        }

        let mut bind = SparseVectorScanBindData::new(
            table_data,
            query_vec,
            topn.limit,
            sparse_col_id,
            get.column_ids.clone(),
        );
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }

        let scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalSparseVectorScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        let projection = Arc::new(Projection::new(projection.expressions.clone(), scan));

        Ok(Some(projection))
    }

    pub fn try_create_fulltext_index_scan_from_topn(
        &self,
        topn: &LogicalTopN,
    ) -> Result<Option<Arc<dyn PhysicalOperator>>> {
        if topn.offset != 0 || topn.orders.len() != 1 {
            return Ok(None);
        }

        let order = &topn.orders[0];
        // BM25 score should be descending for top matches.
        if order.ascending {
            return Ok(None);
        }

        let projection = match &topn.child.operator {
            LogicalOperator::Projection(p) => p,
            _ => return Ok(None),
        };

        let order_expr_idx = match order_expression_index(&order.expression) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        let order_expr = match projection.expressions.get(order_expr_idx) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let (filters, get) = match find_filters_and_get(projection.child.as_ref()) {
            Some(result) => result,
            None => return Ok(None),
        };

        let score_info = match extract_fulltext_score(order_expr, get)? {
            Some(result) => result,
            None => return Ok(None),
        };

        let filters_owned: Vec<Expression> = filters.iter().map(|&e| e.clone()).collect();
        let (predicate_tree, residual) =
            predicate_builder::build_predicate_tree(&filters_owned, get)?;

        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for fulltext scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for fulltext scan"))?
            .clone();

        if !fulltext_index_pushdown_ready(table_data.as_ref(), &score_info) {
            return Ok(None);
        }

        let mut bind = FullTextScanBindData::new(
            table_data,
            score_info.query_text,
            topn.limit,
            score_info.text_column_id,
            get.column_ids.clone(),
        )
        .with_query_options(score_info.query_kind, score_info.config)
        .with_exec_mode(FullTextExecMode::ScoreTopK)
        .with_emit_score(true);
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }

        let score_column_idx = get.column_ids.len();
        let mut projection_expressions = projection.expressions.clone();
        if let Some(expr) = projection_expressions.get_mut(order_expr_idx) {
            *expr = replace_fulltext_score_with_reference(expr.clone(), score_column_idx);
        }

        let mut scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalFullTextScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        if !residual.is_empty() {
            let residual_predicate = if residual.len() == 1 {
                residual[0].clone()
            } else {
                Expression::Conjunction(ConjunctionExpression {
                    conjunction_type: ConjunctionType::And,
                    children: residual,
                })
            };
            let filter: Arc<dyn PhysicalOperator> =
                Arc::new(Filter::new(residual_predicate, scan.clone()));
            scan = self.annotate_schema(filter, self.passthrough_schema(&scan, get.names.clone()));
        }

        let projection = Arc::new(Projection::new(projection_expressions, scan));

        Ok(Some(projection))
    }
}

pub(crate) fn replace_fulltext_score_with_reference(
    expr: Expression,
    score_col_idx: usize,
) -> Expression {
    match expr {
        Expression::Function(mut function) => {
            if is_fulltext_score_function(&function.function.name.to_ascii_lowercase()) {
                return Expression::Reference(ReferenceExpression::new(
                    score_col_idx,
                    LogicalType::Float,
                ));
            }
            function.children = function
                .children
                .into_iter()
                .map(|child| replace_fulltext_score_with_reference(child, score_col_idx))
                .collect();
            Expression::Function(function)
        }
        Expression::Cast(mut cast) => {
            cast.child = Box::new(replace_fulltext_score_with_reference(
                *cast.child,
                score_col_idx,
            ));
            Expression::Cast(cast)
        }
        Expression::Conjunction(mut conjunction) => {
            conjunction.children = conjunction
                .children
                .into_iter()
                .map(|child| replace_fulltext_score_with_reference(child, score_col_idx))
                .collect();
            Expression::Conjunction(conjunction)
        }
        Expression::Case(mut case_expr) => {
            case_expr.check = Box::new(replace_fulltext_score_with_reference(
                *case_expr.check,
                score_col_idx,
            ));
            case_expr.result_if_true = Box::new(replace_fulltext_score_with_reference(
                *case_expr.result_if_true,
                score_col_idx,
            ));
            case_expr.result_if_false = Box::new(replace_fulltext_score_with_reference(
                *case_expr.result_if_false,
                score_col_idx,
            ));
            Expression::Case(case_expr)
        }
        Expression::Comparison(mut comparison) => {
            comparison.left = Box::new(replace_fulltext_score_with_reference(
                *comparison.left,
                score_col_idx,
            ));
            comparison.right = Box::new(replace_fulltext_score_with_reference(
                *comparison.right,
                score_col_idx,
            ));
            Expression::Comparison(comparison)
        }
        Expression::Operator(mut operator) => {
            operator.children = operator
                .children
                .into_iter()
                .map(|child| replace_fulltext_score_with_reference(child, score_col_idx))
                .collect();
            Expression::Operator(operator)
        }
        Expression::Aggregate(mut aggregate) => {
            aggregate.children = aggregate
                .children
                .into_iter()
                .map(|child| replace_fulltext_score_with_reference(child, score_col_idx))
                .collect();
            aggregate.filter = aggregate.filter.map(|filter| {
                Box::new(replace_fulltext_score_with_reference(
                    *filter,
                    score_col_idx,
                ))
            });
            aggregate.order_bys = aggregate
                .order_bys
                .into_iter()
                .map(|mut order| {
                    order.expression =
                        replace_fulltext_score_with_reference(order.expression, score_col_idx);
                    order
                })
                .collect();
            Expression::Aggregate(aggregate)
        }
        Expression::Subquery(mut subquery) => {
            subquery.children = subquery
                .children
                .into_iter()
                .map(|child| replace_fulltext_score_with_reference(child, score_col_idx))
                .collect();
            Expression::Subquery(subquery)
        }
        Expression::Window(mut window) => {
            window.children = window
                .children
                .into_iter()
                .map(|child| replace_fulltext_score_with_reference(child, score_col_idx))
                .collect();
            window.partitions = window
                .partitions
                .into_iter()
                .map(|expr| replace_fulltext_score_with_reference(expr, score_col_idx))
                .collect();
            window.orders = window
                .orders
                .into_iter()
                .map(|mut order| {
                    order.expression =
                        replace_fulltext_score_with_reference(order.expression, score_col_idx);
                    order
                })
                .collect();
            Expression::Window(window)
        }
        other => other,
    }
}

pub(crate) fn fulltext_index_pushdown_ready(
    table_data: &TableHandle,
    info: &FullTextScoreInfo,
) -> bool {
    table_data
        .fulltext_capability(info.text_column_id as u32, &info.config)
        .is_some()
}

fn order_expression_index(expr: &Expression) -> Option<usize> {
    match strip_casts(expr) {
        Expression::Reference(r) => Some(r.index),
        Expression::ColumnRef(c) => Some(c.binding.column_index),
        _ => None,
    }
}

fn find_filters_and_get<'a>(mut plan: &'a LogicalPlan) -> Option<(Vec<&'a Expression>, &'a Get)> {
    let mut filters = Vec::new();
    loop {
        match &plan.operator {
            LogicalOperator::Filter(filter) => {
                for expr in &filter.expressions {
                    filters.push(expr);
                }
                plan = filter.child.as_ref();
            }
            LogicalOperator::Get(get) => return Some((filters, get)),
            _ => return None,
        }
    }
}

pub(crate) fn extract_vector_distance(
    expr: &Expression,
    get: &Get,
) -> Result<Option<(usize, Vec<f32>)>> {
    let expr = match expr {
        Expression::Cast(cast) => cast.child.as_ref(),
        _ => expr,
    };

    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };

    let name = func.function.name.to_lowercase();
    if !is_vector_distance_function(&name) {
        return Ok(None);
    }
    if func.children.len() != 2 {
        return Ok(None);
    }

    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(col_idx) = predicate_builder::extract_scan_column_index(left) {
        if let Some(query) = extract_query_vector(right)? {
            return resolve_vector_query(get, col_idx, query);
        }
    }
    if let Some(col_idx) = predicate_builder::extract_scan_column_index(right) {
        if let Some(query) = extract_query_vector(left)? {
            return resolve_vector_query(get, col_idx, query);
        }
    }

    Ok(None)
}

fn resolve_vector_query(
    get: &Get,
    col_idx: usize,
    query: Vec<f32>,
) -> Result<Option<(usize, Vec<f32>)>> {
    if col_idx >= get.column_ids.len() || col_idx >= get.column_types.len() {
        return Ok(None);
    }
    let col_type = &get.column_types[col_idx];
    if !matches!(col_type, LogicalType::Array(inner, _) if matches!(**inner, LogicalType::Float)) {
        return Ok(None);
    }
    let column_id = get.column_ids[col_idx];
    Ok(Some((column_id, query)))
}

fn is_vector_distance_function(name: &str) -> bool {
    matches!(
        name,
        "l2_distance" | "l1_distance" | "cosine_distance" | "neg_inner_product"
    )
}

fn extract_query_vector(expr: &Expression) -> Result<Option<Vec<f32>>> {
    match expr {
        Expression::Constant(c) => value_to_vec(&c.value),
        Expression::Operator(op) if matches!(op.operator_type, OperatorType::ArrayConstructor) => {
            let mut values = Vec::with_capacity(op.children.len());
            for child in &op.children {
                let Expression::Constant(c) = child else {
                    return Ok(None);
                };
                let Some(v) = value_to_f32(&c.value) else {
                    return Ok(None);
                };
                values.push(v);
            }
            Ok(Some(values))
        }
        Expression::Cast(cast) => {
            if let Expression::Constant(c) = cast.child.as_ref() {
                if let Value::Varchar(s) = &c.value {
                    return Ok(Some(parse_vector_literal(s)?));
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
            for v in values {
                let Some(val) = value_to_f32(v) else {
                    return Ok(None);
                };
                out.push(val);
            }
            Ok(Some(out))
        }
        Value::Varchar(s) => Ok(Some(parse_vector_literal(s)?)),
        _ => Ok(None),
    }
}

fn value_to_f32(value: &Value) -> Option<f32> {
    let val = match value {
        Value::TinyInt(v) => *v as f32,
        Value::SmallInt(v) => *v as f32,
        Value::Integer(v) => *v as f32,
        Value::BigInt(v) => *v as f32,
        Value::UTinyInt(v) => *v as f32,
        Value::USmallInt(v) => *v as f32,
        Value::UInteger(v) => *v as f32,
        Value::UBigInt(v) => *v as f32,
        Value::Float(v) => *v,
        Value::Double(v) => *v as f32,
        _ => return None,
    };
    if val.is_finite() {
        Some(val)
    } else {
        None
    }
}

fn parse_vector_literal(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(paro_error::invalid_value(
            "VECTOR",
            format!("Vector literal must be enclosed in brackets: {}", s),
        ));
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(paro_error::invalid_value(
                "VECTOR",
                format!("Empty element in vector literal: {}", s),
            ));
        }
        let val: f32 = part.parse().map_err(|_| {
            paro_error::invalid_value(
                "VECTOR",
                format!("Invalid number in vector literal: {}", part),
            )
        })?;
        if !val.is_finite() {
            return Err(paro_error::invalid_value(
                "VECTOR",
                format!("Vector elements must be finite numbers, got: {}", part),
            ));
        }
        values.push(val);
    }

    Ok(values)
}

pub(crate) fn extract_sparse_vector_distance(
    expr: &Expression,
    get: &Get,
) -> Result<Option<(usize, SparseVector)>> {
    let expr = match expr {
        Expression::Cast(cast) => cast.child.as_ref(),
        _ => expr,
    };

    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };

    let name = func.function.name.to_lowercase();
    if name != "sparse_distance" {
        return Ok(None);
    }
    if func.children.len() != 2 {
        return Ok(None);
    }

    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(col_idx) = predicate_builder::extract_scan_column_index(left) {
        if let Some(query) = extract_query_sparse_vector(right)? {
            return resolve_sparse_query(get, col_idx, query);
        }
    }
    if let Some(col_idx) = predicate_builder::extract_scan_column_index(right) {
        if let Some(query) = extract_query_sparse_vector(left)? {
            return resolve_sparse_query(get, col_idx, query);
        }
    }

    Ok(None)
}

fn extract_query_sparse_vector(expr: &Expression) -> Result<Option<SparseVector>> {
    match expr {
        Expression::Constant(c) => {
            if let Value::Varchar(s) = &c.value {
                return Ok(Some(SparseVector::parse(s)?));
            }
            Ok(None)
        }
        Expression::Cast(cast) => extract_query_sparse_vector(cast.child.as_ref()),
        _ => Ok(None),
    }
}

fn resolve_sparse_query(
    get: &Get,
    col_idx: usize,
    query: SparseVector,
) -> Result<Option<(usize, SparseVector)>> {
    if col_idx >= get.column_ids.len() || col_idx >= get.column_types.len() {
        return Ok(None);
    }
    let col_type = &get.column_types[col_idx];
    if !matches!(col_type, LogicalType::Varchar) {
        return Ok(None);
    }
    let column_id = get.column_ids[col_idx];
    Ok(Some((column_id, query)))
}

pub(crate) fn extract_fulltext_score(
    expr: &Expression,
    get: &Get,
) -> Result<Option<FullTextScoreInfo>> {
    let expr = strip_casts(expr);

    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };

    let name = func.function.name.to_lowercase();
    if !is_fulltext_score_function(&name) {
        return Ok(None);
    }
    match name.as_str() {
        "bm25" => extract_legacy_fulltext_score(func, get, FullTextScoreMode::Bm25),
        "bm25_score_internal" | "ts_rank" => {
            extract_internal_fulltext_score(func, get, FullTextScoreMode::Bm25)
        }
        "ts_rank_cd" => extract_internal_fulltext_score(func, get, FullTextScoreMode::CoverDensity),
        _ => Ok(None),
    }
}

fn is_fulltext_score_function(name: &str) -> bool {
    matches!(
        name,
        "bm25" | "bm25_score_internal" | "ts_rank" | "ts_rank_cd"
    )
}

fn extract_legacy_fulltext_score(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
    score_mode: FullTextScoreMode,
) -> Result<Option<FullTextScoreInfo>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let (left, right) = (&func.children[0], &func.children[1]);
    if let Some(text_column_id) = resolve_fulltext_column(get, extract_scan_col_idx(left)) {
        if let Some(query) = extract_query_string(right)? {
            let query_stats = build_fulltext_query_stats(
                query.as_str(),
                SIMPLE_CONFIG,
                FullTextQueryKind::Legacy,
            )?;
            return Ok(Some(FullTextScoreInfo {
                text_column_id,
                query_text: query,
                query_kind: FullTextQueryKind::Legacy,
                score_mode,
                query_stats,
                config: SIMPLE_CONFIG.to_string(),
            }));
        }
    }
    if let Some(text_column_id) = resolve_fulltext_column(get, extract_scan_col_idx(right)) {
        if let Some(query) = extract_query_string(left)? {
            let query_stats = build_fulltext_query_stats(
                query.as_str(),
                SIMPLE_CONFIG,
                FullTextQueryKind::Legacy,
            )?;
            return Ok(Some(FullTextScoreInfo {
                text_column_id,
                query_text: query,
                query_kind: FullTextQueryKind::Legacy,
                score_mode,
                query_stats,
                config: SIMPLE_CONFIG.to_string(),
            }));
        }
    }
    Ok(None)
}

fn extract_internal_fulltext_score(
    func: &paro_planner::expression::FunctionExpression,
    get: &Get,
    score_mode: FullTextScoreMode,
) -> Result<Option<FullTextScoreInfo>> {
    if func.children.len() != 2 {
        return Ok(None);
    }
    let Some((text_column_id, tsv_config)) = extract_tsvector_source(&func.children[0], get)?
    else {
        return Ok(None);
    };
    let Some((query_text, query_kind, tsq_config)) = extract_tsquery_source(&func.children[1])?
    else {
        return Ok(None);
    };
    if !tsv_config.eq_ignore_ascii_case(&tsq_config) {
        return Ok(None);
    }
    let query_stats = build_fulltext_query_stats(query_text.as_str(), &tsq_config, query_kind)?;
    Ok(Some(FullTextScoreInfo {
        text_column_id,
        query_text,
        query_kind,
        score_mode,
        query_stats,
        config: tsv_config,
    }))
}

fn extract_tsvector_source(expr: &Expression, get: &Get) -> Result<Option<(usize, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(f) => f,
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
            Some(config) => {
                let Some(normalized) = normalize_fulltext_config(&config) else {
                    return Ok(None);
                };
                normalized
            }
            None => return Ok(None),
        },
        None => SIMPLE_CONFIG.to_string(),
    };
    let text_column_id = resolve_fulltext_column(get, extract_scan_col_idx(text_expr));
    Ok(text_column_id.map(|id| (id, config)))
}

fn extract_tsquery_source(
    expr: &Expression,
) -> Result<Option<(String, FullTextQueryKind, String)>> {
    let expr = strip_casts(expr);
    let func = match expr {
        Expression::Function(f) => f,
        _ => return Ok(None),
    };

    let (query_kind, allow_single_arg_default_config) =
        match func.function.name.to_ascii_lowercase().as_str() {
            "to_tsquery" => (FullTextQueryKind::TsQuery, false),
            "plainto_tsquery" => (FullTextQueryKind::Plain, true),
            "phraseto_tsquery" => (FullTextQueryKind::Phrase, false),
            "websearch_to_tsquery" => (FullTextQueryKind::WebSearch, false),
            _ => return Ok(None),
        };

    let (config, query_expr) = match func.children.as_slice() {
        [config, query] => {
            let Some(cfg) = extract_query_string(config)? else {
                return Ok(None);
            };
            let Some(normalized) = normalize_fulltext_config(&cfg) else {
                return Ok(None);
            };
            (normalized, query)
        }
        [query] if allow_single_arg_default_config => (SIMPLE_CONFIG.to_string(), query),
        _ => return Ok(None),
    };
    let Some(query_text) = extract_query_string(query_expr)? else {
        return Ok(None);
    };

    Ok(Some((query_text, query_kind, config)))
}

fn build_fulltext_query_stats(
    query_text: &str,
    config: &str,
    query_kind: FullTextQueryKind,
) -> Result<FullTextQueryStats> {
    build_planner_fulltext_query_stats(query_text, config, map_query_stats_kind(query_kind))
}

fn map_query_stats_kind(query_kind: FullTextQueryKind) -> FullTextQueryStatsKind {
    match query_kind {
        FullTextQueryKind::Legacy => FullTextQueryStatsKind::Legacy,
        FullTextQueryKind::TsQuery => FullTextQueryStatsKind::TsQuery,
        FullTextQueryKind::Plain => FullTextQueryStatsKind::Plain,
        FullTextQueryKind::Phrase => FullTextQueryStatsKind::Phrase,
        FullTextQueryKind::WebSearch => FullTextQueryStatsKind::WebSearch,
    }
}

fn extract_query_string(expr: &Expression) -> Result<Option<String>> {
    match expr {
        Expression::Constant(c) => {
            if let Value::Varchar(s) = &c.value {
                return Ok(Some(s.clone()));
            }
            Ok(None)
        }
        Expression::Cast(cast) => extract_query_string(cast.child.as_ref()),
        _ => Ok(None),
    }
}

fn normalize_fulltext_config(config: &str) -> Option<String> {
    TokenizerKind::from_config(config)
        .ok()
        .map(|kind| kind.config_name().to_string())
}

fn extract_scan_col_idx(expr: &Expression) -> Option<usize> {
    predicate_builder::extract_scan_column_index(strip_casts(expr))
}

fn resolve_fulltext_column(get: &Get, col_idx: Option<usize>) -> Option<usize> {
    let col_idx = col_idx?;
    if col_idx >= get.column_ids.len() || col_idx >= get.column_types.len() {
        return None;
    }
    let col_type = &get.column_types[col_idx];
    if !matches!(col_type, LogicalType::Varchar) {
        return None;
    }
    Some(get.column_ids[col_idx])
}

fn strip_casts(mut expr: &Expression) -> &Expression {
    while let Expression::Cast(cast) = expr {
        expr = cast.child.as_ref();
    }
    expr
}

fn setting_to_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(v) => Some(*v),
        Value::TinyInt(v) => Some(*v != 0),
        Value::SmallInt(v) => Some(*v != 0),
        Value::Integer(v) => Some(*v != 0),
        Value::BigInt(v) => Some(*v != 0),
        Value::UTinyInt(v) => Some(*v != 0),
        Value::USmallInt(v) => Some(*v != 0),
        Value::UInteger(v) => Some(*v != 0),
        Value::UBigInt(v) => Some(*v != 0),
        Value::Varchar(v) => {
            let normalized = v.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "t" | "on" | "1" => Some(true),
                "false" | "f" | "off" | "0" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

fn should_use_order_limit_fallback(total_rows: usize, force_external: bool) -> bool {
    force_external || total_rows > TOPN_EXTERNAL_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;
    use paro_common::vector::Vector;
    use paro_planner::expression::{
        CastExpression, ConstantExpression, FunctionExpression, ReferenceExpression,
    };
    use paro_planner::operator::get::Get;

    fn dummy_fn(
        _: &Chunk,
        _: &dyn paro_function::scalar::ExpressionState,
        _: &mut Vector,
    ) -> Result<()> {
        Ok(())
    }

    #[test]
    fn test_extract_fulltext_score() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);

        let func = FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "bm25".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("query".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Float,
        );

        let res = extract_fulltext_score(&Expression::Function(func), &get).unwrap();
        assert_eq!(
            res,
            Some(FullTextScoreInfo {
                text_column_id: 0,
                query_text: "query".to_string(),
                query_kind: FullTextQueryKind::Legacy,
                score_mode: FullTextScoreMode::Bm25,
                query_stats: FullTextQueryStats::new(1),
                config: "simple".to_string(),
            })
        );
    }

    #[test]
    fn topn_fallback_for_large_limit_offset() {
        assert!(should_use_order_limit_fallback(
            TOPN_EXTERNAL_THRESHOLD.saturating_add(1),
            false
        ));
        assert!(!should_use_order_limit_fallback(
            TOPN_EXTERNAL_THRESHOLD,
            false
        ));
    }

    #[test]
    fn topn_fallback_for_force_external() {
        assert!(should_use_order_limit_fallback(1, true));
        assert!(should_use_order_limit_fallback(0, true));
    }

    #[test]
    fn setting_to_bool_accepts_boolean_and_text_values() {
        assert_eq!(setting_to_bool(&Value::Boolean(true)), Some(true));
        assert_eq!(
            setting_to_bool(&Value::Varchar("ON".to_string())),
            Some(true)
        );
        assert_eq!(
            setting_to_bool(&Value::Varchar("off".to_string())),
            Some(false)
        );
        assert_eq!(setting_to_bool(&Value::Integer(0)), Some(false));
        assert_eq!(setting_to_bool(&Value::Integer(7)), Some(true));
    }

    #[test]
    fn test_extract_fulltext_score_accepts_internal_and_ts_rank_names() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);

        for (name, args) in [
            (
                "bm25_score_internal",
                vec![LogicalType::TsVector, LogicalType::TsQuery],
            ),
            ("ts_rank", vec![LogicalType::TsVector, LogicalType::TsQuery]),
            (
                "ts_rank_cd",
                vec![LogicalType::TsVector, LogicalType::TsQuery],
            ),
        ] {
            let func = FunctionExpression::new(
                paro_function::scalar::ScalarFunction::new(
                    name.to_string(),
                    args,
                    LogicalType::Float,
                    dummy_fn,
                ),
                vec![
                    Expression::Function(FunctionExpression::new(
                        paro_function::scalar::ScalarFunction::new(
                            "to_tsvector".to_string(),
                            vec![LogicalType::Varchar, LogicalType::Varchar],
                            LogicalType::TsVector,
                            dummy_fn,
                        ),
                        vec![
                            Expression::Constant(ConstantExpression::new(
                                Value::Varchar("simple".to_string()),
                                LogicalType::Varchar,
                            )),
                            Expression::Reference(ReferenceExpression::new(
                                0,
                                LogicalType::Varchar,
                            )),
                        ],
                        LogicalType::TsVector,
                    )),
                    Expression::Function(FunctionExpression::new(
                        paro_function::scalar::ScalarFunction::new(
                            "plainto_tsquery".to_string(),
                            vec![LogicalType::Varchar, LogicalType::Varchar],
                            LogicalType::TsQuery,
                            dummy_fn,
                        ),
                        vec![
                            Expression::Constant(ConstantExpression::new(
                                Value::Varchar("simple".to_string()),
                                LogicalType::Varchar,
                            )),
                            Expression::Constant(ConstantExpression::new(
                                Value::Varchar("query".to_string()),
                                LogicalType::Varchar,
                            )),
                        ],
                        LogicalType::TsQuery,
                    )),
                ],
                LogicalType::Float,
            );

            let res = extract_fulltext_score(&Expression::Function(func), &get).unwrap();
            assert!(
                res.is_some(),
                "expected fulltext score extraction for function {}",
                name
            );
            let info = res.unwrap();
            assert_eq!(info.text_column_id, 0);
            assert_eq!(info.query_text, "query");
            assert_eq!(info.query_kind, FullTextQueryKind::Plain);
            match name {
                "ts_rank_cd" => assert_eq!(info.score_mode, FullTextScoreMode::CoverDensity),
                _ => assert_eq!(info.score_mode, FullTextScoreMode::Bm25),
            }
            assert_eq!(info.query_stats.effective_query_terms(), 1);
            assert_eq!(info.config, "simple");
        }
    }

    #[test]
    fn test_extract_fulltext_score_accepts_cast_wrapped_internal_args() {
        use paro_function::scalar::cast::BoundCastInfo;

        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);
        let to_tsvector = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "to_tsvector".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsVector,
                dummy_fn,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
            ],
            LogicalType::TsVector,
        ));
        let tsquery = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "to_tsquery".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::TsQuery,
                dummy_fn,
            ),
            vec![
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("simple".to_string()),
                    LogicalType::Varchar,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("vector & !spam".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::TsQuery,
        ));
        let wrapped_tsv = Expression::Cast(CastExpression::new(
            to_tsvector,
            LogicalType::TsVector,
            BoundCastInfo::identity(&LogicalType::TsVector, &LogicalType::TsVector),
            false,
        ));
        let wrapped_tsq = Expression::Cast(CastExpression::new(
            tsquery,
            LogicalType::TsQuery,
            BoundCastInfo::identity(&LogicalType::TsQuery, &LogicalType::TsQuery),
            false,
        ));
        let score_expr = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "ts_rank".to_string(),
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![wrapped_tsv, wrapped_tsq],
            LogicalType::Float,
        ));

        let res = extract_fulltext_score(&score_expr, &get).unwrap();
        assert_eq!(
            res,
            Some(FullTextScoreInfo {
                text_column_id: 0,
                query_text: "vector & !spam".to_string(),
                query_kind: FullTextQueryKind::TsQuery,
                score_mode: FullTextScoreMode::Bm25,
                query_stats: FullTextQueryStats {
                    term_count: 2,
                    positive_term_count: 1,
                    phrase_count: 0,
                    proximity_count: 0,
                    prefix_count: 0,
                    not_count: 1,
                    or_branch_count: 0,
                },
                config: "simple".to_string(),
            })
        );
    }

    #[test]
    fn test_replace_fulltext_score_with_reference_direct() {
        let score_expr = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "ts_rank".to_string(),
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![],
            LogicalType::Float,
        ));

        let rewritten = replace_fulltext_score_with_reference(score_expr, 3);
        match rewritten {
            Expression::Reference(reference) => {
                assert_eq!(reference.index, 3);
                assert_eq!(reference.return_type, LogicalType::Float);
            }
            other => panic!("expected ReferenceExpression, got {:?}", other),
        }
    }

    #[test]
    fn test_replace_fulltext_score_with_reference_preserves_cast_wrapper() {
        use paro_function::scalar::cast::BoundCastInfo;

        let score_expr = Expression::Function(FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "ts_rank".to_string(),
                vec![LogicalType::TsVector, LogicalType::TsQuery],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![],
            LogicalType::Float,
        ));
        let cast_expr = Expression::Cast(CastExpression::new(
            score_expr,
            LogicalType::Float,
            BoundCastInfo::identity(&LogicalType::Float, &LogicalType::Float),
            false,
        ));

        let rewritten = replace_fulltext_score_with_reference(cast_expr, 5);
        match rewritten {
            Expression::Cast(cast) => match *cast.child {
                Expression::Reference(reference) => {
                    assert_eq!(reference.index, 5);
                    assert_eq!(reference.return_type, LogicalType::Float);
                }
                other => panic!("expected cast child reference, got {:?}", other),
            },
            other => panic!("expected cast wrapper, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_sparse_vector_distance() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);

        let func = FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "sparse_distance".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("{1:0.5, 2:0.1}".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Float,
        );

        let (_col_id, sv) = extract_sparse_vector_distance(&Expression::Function(func), &get)
            .unwrap()
            .unwrap();
        assert_eq!(sv.dims, vec![1, 2]);
        assert_eq!(sv.weights, vec![0.5, 0.1]);
    }

    #[test]
    fn test_extract_sparse_vector_distance_out_of_bounds_safe() {
        let mut get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Varchar]);
        get.column_ids.clear();

        let func = FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "sparse_distance".to_string(),
                vec![LogicalType::Varchar, LogicalType::Varchar],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Varchar)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("{1:0.5,2:0.1}".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Float,
        );

        let res = extract_sparse_vector_distance(&Expression::Function(func), &get).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_extract_fulltext_score_requires_varchar_column() {
        let get = Get::new_without_table(1, Vec::new(), vec![LogicalType::Integer]);

        let func = FunctionExpression::new(
            paro_function::scalar::ScalarFunction::new(
                "bm25".to_string(),
                vec![LogicalType::Integer, LogicalType::Varchar],
                LogicalType::Float,
                dummy_fn,
            ),
            vec![
                Expression::Reference(ReferenceExpression::new(0, LogicalType::Integer)),
                Expression::Constant(ConstantExpression::new(
                    Value::Varchar("query".to_string()),
                    LogicalType::Varchar,
                )),
            ],
            LogicalType::Float,
        );

        let res = extract_fulltext_score(&Expression::Function(func), &get).unwrap();
        assert!(res.is_none());
    }
}
