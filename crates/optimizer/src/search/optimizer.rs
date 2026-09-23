// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_planner::plan::{LogicalInput, LogicalPlanRead};

use paro_catalog::entry::CatalogEntry;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::BuiltinIntrinsicId;
use paro_planner::expression::{
    Expression, ExpressionIterator, ExpressionVisitDecision, OperatorType,
};
use paro_planner::operator::{
    build_fulltext_query_stats, normalize_fulltext_config, Confidence, Filter, FullTextFilterScan,
    FullTextQueryKind, FullTextScoreMode, Get, LogicalOperator, Projection, SearchCandidate,
    SearchDecision, SearchScan, TopN,
};
use paro_planner::plan::{NodeStats, OwnedLogicalPlan, PlanNodeId};
use paro_storage::search::{
    DenseVectorQuery, ExactFilterMaterialization, FullTextIntent, HnswIntent,
    NormalizedSearchRequest, ProjectionSpec, SearchCostEstimate as PlannedSearchCostEstimate,
    SearchIntent, SearchRequestMode, SequentialCapability, SparseIntent,
};

use crate::context::OptimizationContext;
use crate::statistics::cost::{FullTextScanCostModel, VectorScanCostModel};

const SIMPLE_CONFIG: &str = "simple";

pub struct SearchOptimizer;

/// A borrowed operator occurrence. Child links remain in the owner's identity
/// space; `None` from the reader means an opaque group boundary, not a request
/// to choose a representative. No owned subtree is constructed by this API.
pub(crate) struct SearchNodeRef<'a, C> {
    pub id: PlanNodeId,
    pub stats: &'a NodeStats,
    pub operator: &'a LogicalOperator<C>,
}

impl SearchOptimizer {
    pub fn new() -> Self {
        Self
    }

    pub(crate) fn physical_candidate_for_window<'a, C: 'a>(
        &self,
        root: SearchNodeRef<'a, C>,
        node_bound: usize,
        mut read: impl FnMut(&C) -> Result<Option<SearchNodeRef<'a, C>>>,
        ctx: &OptimizationContext,
    ) -> Result<Option<OwnedLogicalPlan>> {
        match root.operator {
            LogicalOperator::Filter(filter) => {
                let Some(child) = read(&filter.child)? else {
                    return Ok(None);
                };
                let LogicalOperator::Get(get) = child.operator else {
                    return Ok(None);
                };
                self.try_rewrite_fulltext_filter(root.id, root.stats, filter, get, child.stats, ctx)
            }
            LogicalOperator::TopN(topn) if topn.offset == 0 && topn.orders.len() == 1 => {
                let Some(child) = read(&topn.child)? else {
                    return Ok(None);
                };
                let LogicalOperator::Projection(projection) = child.operator else {
                    return Ok(None);
                };
                let Some(order_expr_idx) = order_expression_index(&topn.orders[0].expression)
                else {
                    return Ok(None);
                };
                let Some(order_expr) = projection.expressions.get(order_expr_idx) else {
                    return Ok(None);
                };
                let mut link = &projection.child;
                let mut filters = Vec::new();
                // The input is a finite local shell/arena, not the Memo. A
                // malformed cycle is an internal error, never "unsupported".
                for _ in 0..node_bound {
                    let Some(child) = read(link)? else {
                        return Ok(None);
                    };
                    match child.operator {
                        LogicalOperator::Filter(filter) => {
                            if !filter.projection_map.is_all() {
                                return Ok(None);
                            }
                            filters.extend(filter.expressions.iter().cloned());
                            link = &filter.child;
                        }
                        LogicalOperator::Get(get) => {
                            return self.try_rewrite_topn(
                                root.id,
                                root.stats,
                                TopNPattern {
                                    topn,
                                    projection,
                                    get_stats: child.stats,
                                    filters,
                                    get,
                                    order_expr_idx,
                                    order_expr,
                                },
                                ctx,
                            );
                        }
                        _ => return Ok(None),
                    }
                }
                Err(paro_error::internal(
                    "search window exceeded its local node domain",
                ))
            }
            _ => Ok(None),
        }
    }

    /// Derive a physical search payload for exactly this logical root.  It is
    /// intentionally non-recursive: the Memo builder attaches the payload to
    /// the matching Filter/TopN expression and keeps the logical expression
    /// itself provider- and capability-free.
    pub(crate) fn physical_candidate_for_root(
        &self,
        plan: &OwnedLogicalPlan,
        ctx: &OptimizationContext,
    ) -> Result<Option<OwnedLogicalPlan>> {
        let candidate = match &plan.operator {
            LogicalOperator::TopN(topn) => match extract_topn_pattern(topn) {
                Some(pattern) => self.try_rewrite_topn(plan.id, &plan.stats, pattern, ctx),
                None => Ok(None),
            },
            LogicalOperator::Filter(filter) => match &filter.child.operator {
                LogicalOperator::Get(get) => self.try_rewrite_fulltext_filter(
                    plan.id,
                    &plan.stats,
                    filter,
                    get,
                    &filter.child.stats,
                    ctx,
                ),
                _ => Ok(None),
            },
            _ => Ok(None),
        }?;
        if candidate.as_ref().is_some_and(|candidate| {
            !matches!(
                candidate.operator,
                LogicalOperator::SearchScan(_) | LogicalOperator::FullTextFilterScan(_)
            )
        }) {
            return Err(paro_error::internal(
                "search rewrite produced a non-search physical candidate",
            ));
        }
        Ok(candidate)
    }

    /// Native structural binding for provider windows. Unrelated inputs stay
    /// arena references; the only exportable path is TopN/Projection/Filter*
    /// ending at Get, or a Filter directly on Get.
    pub(crate) fn candidate_arena_roots(
        plan: &paro_planner::plan::LogicalPlan,
    ) -> Result<Vec<paro_planner::plan::arena::PlanIndex>> {
        let arena = plan.arena();
        let mut scan_paths = std::collections::BTreeSet::new();
        let mut roots = Vec::new();
        for index in arena.post_order(plan.root())? {
            match &arena.get(index)?.operator {
                LogicalOperator::Get(_) => {
                    scan_paths.insert(index);
                }
                LogicalOperator::Filter(filter) => {
                    if scan_paths.contains(&filter.child) {
                        scan_paths.insert(index);
                    }
                    if matches!(arena.get(filter.child)?.operator, LogicalOperator::Get(_)) {
                        roots.push(index);
                    }
                }
                LogicalOperator::TopN(topn) if topn.offset == 0 && topn.orders.len() == 1 => {
                    if let LogicalOperator::Projection(projection) =
                        &arena.get(topn.child)?.operator
                    {
                        if scan_paths.contains(&projection.child)
                            && order_expression_index(&topn.orders[0].expression)
                                .is_some_and(|ordinal| ordinal < projection.expressions.len())
                        {
                            roots.push(index);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(roots)
    }

    /// Tables whose search capability state is a logical planning input even
    /// when a sequential expression wins. A plain relational scan is not a
    /// negative search observation: adding a provider cannot create a legal
    /// alternative unless the statement carries a matching search intent.
    pub(crate) fn planning_observation_tables<P: LogicalPlanRead>(
        plan: &P,
    ) -> Result<Vec<std::sync::Arc<paro_catalog::entry::TableCatalogEntry>>> {
        let mut tables = std::collections::BTreeMap::new();
        let mut pending = vec![plan];
        while let Some(plan) = pending.pop() {
            let observed_get = match plan.operator() {
                LogicalOperator::TopN(topn) => extract_topn_pattern(topn)
                    .map(|pattern| {
                        let observes = extract_vector_intent(
                            pattern.order_expr,
                            pattern.get,
                            topn.hnsw_options,
                            topn.orders[0].ascending,
                        )?
                        .is_some()
                            || extract_sparse_intent(pattern.order_expr, pattern.get)?.is_some()
                            || extract_fulltext_score_intent(pattern.order_expr, pattern.get)?
                                .is_some();
                        Ok::<_, paro_common::error::ParoError>(observes.then_some(pattern.get))
                    })
                    .transpose()?
                    .flatten(),
                LogicalOperator::Filter(filter) => match filter.child.operator() {
                    LogicalOperator::Get(get) => filter
                        .expressions
                        .iter()
                        .try_fold(false, |observed, expression| {
                            Ok::<_, paro_common::error::ParoError>(
                                observed
                                    || extract_fulltext_match_intent(expression, get)?.is_some(),
                            )
                        })?
                        .then_some(get.as_ref()),
                    _ => None,
                },
                _ => None,
            };
            if let Some(table) = observed_get.and_then(Get::get_table) {
                tables.insert(table.object_id().raw(), table.clone());
            }
            plan.operator()
                .visit_child_links(&mut |child| pending.push(&**child));
        }
        Ok(tables.into_values().collect())
    }

    fn try_rewrite_topn<C>(
        &self,
        id: PlanNodeId,
        root_stats: &NodeStats,
        pattern: TopNPattern<'_, C>,
        ctx: &OptimizationContext,
    ) -> Result<Option<OwnedLogicalPlan>> {
        let topn = pattern.topn;
        let vector_intent = extract_vector_intent(
            pattern.order_expr,
            pattern.get,
            topn.hnsw_options,
            pattern.topn.orders[0].ascending,
        )?;
        if topn.hnsw_options != Default::default() && vector_intent.is_none() {
            return Err(paro_error::invalid_input(
                "HNSW_EF and VECTOR_SEARCH_MODE apply only to ascending dense-vector distance ORDER BY ... LIMIT queries",
            ));
        }
        let Some((table_id, storage)) = get_search_storage(pattern.get) else {
            return Ok(None);
        };

        let candidate_filters = candidate_filters(&pattern.filters, pattern.get);
        let base_rows = base_rows(pattern.get_stats, pattern.get);
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

        if let Some(intent) = vector_intent {
            let search_intent = SearchIntent::Hnsw(intent.clone());
            let Some(capability) = storage.search_capability(&search_intent) else {
                return Ok(None);
            };
            if !capability.is_queryable() {
                return Ok(None);
            }
            let Some(stats) = capability.generation_stats.hnsw_index_statistics()? else {
                return Ok(None);
            };
            let Some(search_policy) =
                storage.vector_search_policy(intent.column_id, intent.distance)
            else {
                return Ok(None);
            };
            let estimated_cost = VectorScanCostModel::estimate_hnsw_cost(
                &stats,
                capability.generation_stats.artifact_count,
                topn.limit,
                filter_selectivity,
                topn.hnsw_options,
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
            return build_search_scan(
                id,
                root_stats,
                pattern,
                request,
                decision,
                candidate_filters,
            );
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
            return build_search_scan(
                id,
                root_stats,
                pattern,
                request,
                decision,
                candidate_filters,
            );
        }

        if let Some(intent) = extract_fulltext_score_intent(pattern.order_expr, pattern.get)? {
            // This provider enumerates matching documents only and ranks high
            // scores first. Without the exact matching predicate, SQL may need
            // zero-score nonmatches (or NULLs); truncating that domain is not an
            // exact replacement, irrespective of the index's Exact capability.
            if topn.orders[0].ascending
                || !candidate_filters
                    .iter()
                    .try_fold(false, |matched, filter| {
                        Ok::<_, paro_common::error::ParoError>(
                            matched || fulltext_filter_matches_score(filter, pattern.get, &intent)?,
                        )
                    })?
            {
                return Ok(None);
            }
            // A matching full-text predicate is the admission semantics of
            // this ranked provider, not a residual scalar predicate. Keeping
            // it in `absorbed_predicates` asks the physical predicate builder
            // to lower the full-text operator a second time and turns an
            // otherwise valid search winner into UNSUPPORTED. Only predicates
            // not represented by the provider remain as exact row-set input.
            let candidate_filters =
                residual_fulltext_filters(candidate_filters, pattern.get, &intent)?;
            let filtered = ctx.cost_model.estimate_filter_cardinality(
                base_rows,
                &candidate_filters,
                &ctx.column_stats,
            );
            let filter_selectivity = estimate_selectivity(base_rows, filtered.expected);
            let filter_materialization =
                exact_filter_materialization(&candidate_filters, pattern.get, storage.as_ref());
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
            return build_search_scan(
                id,
                root_stats,
                pattern,
                request,
                decision,
                candidate_filters,
            );
        }

        Ok(None)
    }

    fn try_rewrite_fulltext_filter<C>(
        &self,
        id: PlanNodeId,
        root_stats: &NodeStats,
        filter: &Filter<C>,
        get: &Get,
        get_stats: &NodeStats,
        ctx: &OptimizationContext,
    ) -> Result<Option<OwnedLogicalPlan>> {
        let Some((table_id, storage)) = get_search_storage(get) else {
            return Ok(None);
        };

        for (match_idx, expr) in filter.expressions.iter().enumerate() {
            let Some(intent) = extract_fulltext_match_intent(expr, get)? else {
                continue;
            };
            let mut other_predicates = filter.expressions.clone();
            let match_expression = other_predicates.remove(match_idx);
            let required_filters = candidate_filters(&other_predicates, get);
            let (_, residual) =
                crate::physical::extraction::predicate_builder::build_search_predicate_template(
                    &required_filters,
                    get,
                )?;
            if !residual.is_empty() {
                // This leaf implements a bitmap-filter source, not a scalar
                // residual Filter. Keep the ordinary relational alternative.
                continue;
            }
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

            let base_rows = base_rows(get_stats, get);
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

            let operator = LogicalOperator::FullTextFilterScan(Box::new(FullTextFilterScan {
                get: get.clone(),
                projection_map: filter.projection_map.clone(),
                request,
                match_expression,
                other_predicates,
                residual_predicates: Vec::new(),
                decision,
            }));
            let stats = root_stats.clone();
            return Ok(Some(OwnedLogicalPlan {
                id,
                stats,
                operator,
            }));
        }

        Ok(None)
    }
}

fn residual_fulltext_filters(
    filters: Vec<Expression>,
    get: &Get,
    score_intent: &FullTextIntent,
) -> Result<Vec<Expression>> {
    let mut residual = Vec::with_capacity(filters.len());
    for filter in filters {
        let represented = fulltext_filter_matches_score(&filter, get, score_intent)?;
        if !represented {
            residual.push(filter);
        }
    }
    Ok(residual)
}

fn fulltext_filter_matches_score(
    filter: &Expression,
    get: &Get,
    score_intent: &FullTextIntent,
) -> Result<bool> {
    Ok(
        extract_fulltext_match_intent(filter, get)?.is_some_and(|predicate_intent| {
            predicate_intent.column_id == score_intent.column_id
                && predicate_intent.query == score_intent.query
                && predicate_intent.query_kind == score_intent.query_kind
                && predicate_intent.config == score_intent.config
        }),
    )
}

fn build_search_scan<C>(
    id: PlanNodeId,
    root_stats: &NodeStats,
    pattern: TopNPattern<'_, C>,
    request: NormalizedSearchRequest,
    decision: SearchDecision,
    candidate_filters: Vec<Expression>,
) -> Result<Option<OwnedLogicalPlan>> {
    // TopK cannot discard a predicate and apply it after truncation. Use the
    // exact extraction contract before publication, not a cost-based guess
    // that a later extractor will support this shape.
    let (_, residual) =
        crate::physical::extraction::predicate_builder::build_search_predicate_template(
            &candidate_filters,
            pattern.get,
        )?;
    if !residual.is_empty() {
        return Ok(None);
    }
    pattern
        .topn
        .projection_map
        .validate(pattern.projection.expressions.len())?;
    let output_indices = pattern
        .topn
        .projection_map
        .to_indices(pattern.projection.expressions.len());
    let projections = output_indices
        .iter()
        .map(|&index| pattern.projection.expressions.get(index).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| paro_error::internal("validated TopN projection became invalid"))?;
    let score_output_index = output_indices
        .iter()
        .position(|&index| index == pattern.order_expr_idx);
    let output_names = output_indices
        .iter()
        .map(|&index| {
            pattern
                .projection
                .visible_names
                .get(index)
                .cloned()
                .unwrap_or_else(|| format!("__paro_hidden_{index}"))
        })
        .collect();
    let operator = LogicalOperator::SearchScan(Box::new(
        SearchScan::new(
            pattern.get.clone(),
            request,
            decision,
            projections,
            pattern.projection.table_index,
            candidate_filters,
            Vec::new(),
            score_output_index,
            pattern.order_expr.clone(),
            pattern.topn.orders[0].ascending,
            pattern.topn.limit,
        )
        .with_output_names(output_names),
    ));
    let stats = root_stats.clone();
    Ok(Some(OwnedLogicalPlan {
        id,
        stats,
        operator,
    }))
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

fn base_rows(stats: &NodeStats, get: &Get) -> u64 {
    stats
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

struct TopNPattern<'a, C = Box<OwnedLogicalPlan>> {
    topn: &'a TopN<C>,
    projection: &'a Projection<C>,
    get_stats: &'a NodeStats,
    filters: Vec<Expression>,
    get: &'a Get,
    order_expr_idx: usize,
    order_expr: &'a Expression,
}

fn extract_topn_pattern<C: paro_planner::plan::LogicalChild>(
    topn: &TopN<C>,
) -> Option<TopNPattern<'_, C>> {
    if topn.offset != 0 || topn.orders.len() != 1 {
        return None;
    }
    let projection = match topn.child.operator() {
        LogicalOperator::Projection(projection) => projection,
        _ => return None,
    };
    let order_expr_idx = order_expression_index(&topn.orders[0].expression)?;
    let order_expr = projection.expressions.get(order_expr_idx)?;
    let get_plan = find_get_plan(&*projection.child)?;
    let LogicalOperator::Get(get) = get_plan.operator() else {
        return None;
    };
    Some(TopNPattern {
        topn,
        projection,
        get_stats: get_plan.node_stats(),
        filters: collect_filters(&*projection.child),
        get,
        order_expr_idx,
        order_expr,
    })
}

fn order_expression_index(expr: &Expression) -> Option<usize> {
    match strip_casts(expr) {
        Expression::Reference(reference) => Some(reference.index),
        Expression::ColumnRef(column) => Some(column.binding.column_index),
        _ => None,
    }
}

fn find_get_plan<P: LogicalPlanRead>(mut plan: &P) -> Option<&P> {
    loop {
        match plan.operator() {
            LogicalOperator::Filter(filter) => {
                if !filter.projection_map.is_all() {
                    return None;
                }
                plan = &*filter.child;
            }
            LogicalOperator::Get(_) => return Some(plan),
            _ => return None,
        }
    }
}

fn collect_filters<P: LogicalPlanRead>(mut plan: &P) -> Vec<Expression> {
    let mut filters = Vec::new();
    while let LogicalOperator::Filter(filter) = plan.operator() {
        filters.extend(filter.expressions.iter().cloned());
        plan = &*filter.child;
    }
    debug_assert!(matches!(plan.operator(), LogicalOperator::Get(_)));
    filters
}

fn extract_vector_intent(
    expr: &Expression,
    get: &Get,
    options: paro_storage::index::hnsw::HnswQueryOptions,
    ascending: bool,
) -> Result<Option<HnswIntent>> {
    if !ascending {
        return Ok(None);
    }
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
    if let Some(column_idx) = extract_scan_col_idx(left, get) {
        if let Some(query_vector) = extract_query_vector(right)? {
            return Ok(
                resolve_vector_column(get, column_idx).map(|column_id| HnswIntent {
                    column_id,
                    query: query_vector,
                    distance,
                    options,
                }),
            );
        }
    }
    if let Some(column_idx) = extract_scan_col_idx(right, get) {
        if let Some(query_vector) = extract_query_vector(left)? {
            return Ok(
                resolve_vector_column(get, column_idx).map(|column_id| HnswIntent {
                    column_id,
                    query: query_vector,
                    distance,
                    options,
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
    if let Some(column_idx) = extract_scan_col_idx(left, get) {
        if let Some(query_vector) = extract_query_sparse_vector(right)? {
            return Ok(
                resolve_sparse_column(get, column_idx).map(|column_id| SparseIntent {
                    column_id,
                    query_vector,
                }),
            );
        }
    }
    if let Some(column_idx) = extract_scan_col_idx(right, get) {
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

pub(crate) fn extract_fulltext_score_intent(
    expr: &Expression,
    get: &Get,
) -> Result<Option<FullTextIntent>> {
    // A cast is part of the projected score contract. In particular narrowing
    // casts can create peers; the provider must not silently replace them with
    // its uncast score. Unsupported score expressions retain ordinary TopN.
    let func = match expr {
        Expression::Function(function) => function,
        _ => return Ok(None),
    };
    match func.builtin_intrinsic() {
        Some(BuiltinIntrinsicId::Bm25) => extract_fulltext_query_from_column_and_string(
            func,
            get,
            FullTextQueryKind::Legacy,
            FullTextScoreMode::DocumentRankV1,
        ),
        Some(BuiltinIntrinsicId::Bm25ScoreInternal | BuiltinIntrinsicId::TsRank) => {
            extract_internal_fulltext_query(func, get, FullTextScoreMode::DocumentRankV1)
        }
        Some(BuiltinIntrinsicId::TsRankCd) => {
            extract_internal_fulltext_query(func, get, FullTextScoreMode::CoverDensityV1)
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
            FullTextScoreMode::CorpusBm25V1,
        ),
        Some(BuiltinIntrinsicId::FullTextMatchInternal) => {
            extract_internal_fulltext_query(func, get, FullTextScoreMode::CorpusBm25V1)
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
    if let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(left, get)) {
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

    let Some(column_id) = resolve_fulltext_column(get, extract_scan_col_idx(text_expr, get)) else {
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

fn extract_scan_col_idx(expr: &Expression, get: &Get) -> Option<usize> {
    match expr {
        Expression::Reference(reference) => Some(reference.index),
        Expression::ColumnRef(column)
            if column.depth == 0 && column.binding.table_index == get.table_index =>
        {
            Some(column.binding.column_index)
        }
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
    fn borrowed_provider_window_preserves_holes_and_reader_errors() {
        let context = OptimizationContext::new(
            paro_context::TestStatementContextBuilder::minimal().build(),
            paro_planner::binder::context::BindContext::new(),
        );
        let stats = NodeStats::default();
        let operator = LogicalOperator::Filter(Filter {
            child: 7usize,
            expressions: Vec::new(),
            projection_map: paro_planner::operator::ProjectionMap::all(),
        });
        let root = || SearchNodeRef {
            id: PlanNodeId(1),
            stats: &stats,
            operator: &operator,
        };
        let mut reads = 0;
        let result = SearchOptimizer::new()
            .physical_candidate_for_window(
                root(),
                1,
                |child| {
                    assert_eq!(*child, 7);
                    reads += 1;
                    Ok(None)
                },
                &context,
            )
            .unwrap();
        assert!(result.is_none());
        assert_eq!(
            reads, 1,
            "a group hole must not trigger representative expansion"
        );
        let error = SearchOptimizer::new()
            .physical_candidate_for_window(
                root(),
                1,
                |_| Err(paro_error::internal("reader failure sentinel")),
                &context,
            )
            .unwrap_err();
        assert!(error.to_string().contains("reader failure sentinel"));
    }

    #[test]
    fn adaptive_decision_is_used_for_close_costs() {
        let decision = select_search_decision(
            SearchCandidate {
                intent: SearchIntent::Hnsw(HnswIntent {
                    column_id: 1,
                    query: DenseVectorQuery::Literal(vec![1.0, 2.0]),
                    distance: paro_storage::index::hnsw::DistanceMetric::Euclidean,
                    options: Default::default(),
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
        let expr = Expression::Function(
            FunctionExpression::new(
                scalar_function(
                    "fulltext_match",
                    vec![LogicalType::Varchar, LogicalType::Varchar],
                    LogicalType::Boolean,
                ),
                vec![
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Varchar)
                            .into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("hello world".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );

        let intent = extract_fulltext_match_intent(&expr, &get).unwrap().unwrap();
        assert_eq!(intent.column_id, 0);
        assert_eq!(intent.score_mode, FullTextScoreMode::CorpusBm25V1);
        assert_eq!(intent.query, "hello world");
        assert_eq!(intent.query_stats.term_count, 2);
        assert_eq!(intent.query_stats.effective_query_terms(), 2);
    }

    #[test]
    fn internal_fulltext_match_accepts_a_folded_tsquery_constant() {
        let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
        let vector = Expression::Function(
            FunctionExpression::new(
                scalar_function(
                    "to_tsvector",
                    vec![LogicalType::Varchar, LogicalType::Varchar],
                    LogicalType::TsVector,
                ),
                vec![
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("simple".to_string()),
                            LogicalType::Varchar,
                        )
                        .into(),
                    ),
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(1, 0), LogicalType::Varchar)
                            .into(),
                    ),
                ],
                LogicalType::TsVector,
            )
            .into(),
        );
        let expression = Expression::Function(
            FunctionExpression::new(
                scalar_function(
                    "fulltext_match_internal",
                    vec![LogicalType::TsVector, LogicalType::TsQuery],
                    LogicalType::Boolean,
                ),
                vec![
                    vector,
                    Expression::Constant(
                        ConstantExpression::new(
                            Value::Varchar("vector & database".to_string()),
                            LogicalType::TsQuery,
                        )
                        .into(),
                    ),
                ],
                LogicalType::Boolean,
            )
            .into(),
        );

        let intent = extract_fulltext_match_intent(&expression, &get)
            .unwrap()
            .unwrap();
        assert_eq!(intent.query, "vector & database");
        assert_eq!(intent.query_kind, FullTextQueryKind::SerializedTsQuery);
        assert_eq!(intent.config, "simple");
        assert_eq!(intent.query_stats.effective_query_terms(), 2);
    }

    #[test]
    fn document_score_requires_the_exact_document_binding_and_argument_order() {
        let get = Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
        let column = |table| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, 0), LogicalType::Varchar).into(),
            )
        };
        let query = || {
            Expression::Constant(
                ConstantExpression::new(Value::Varchar("needle".to_string()), LogicalType::Varchar)
                    .into(),
            )
        };
        let score = |args| {
            Expression::Function(
                FunctionExpression::new(
                    scalar_function(
                        "bm25",
                        vec![LogicalType::Varchar, LogicalType::Varchar],
                        LogicalType::Float,
                    ),
                    args,
                    LogicalType::Float,
                )
                .into(),
            )
        };
        let valid = score(vec![column(1), query()]);
        assert_eq!(
            extract_fulltext_score_intent(&valid, &get)
                .unwrap()
                .unwrap()
                .score_mode,
            FullTextScoreMode::DocumentRankV1
        );
        assert!(
            extract_fulltext_score_intent(&score(vec![column(2), query()]), &get)
                .unwrap()
                .is_none()
        );
        assert!(
            extract_fulltext_score_intent(&score(vec![query(), column(1)]), &get)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn derived_scan_output_declines_stored_search_projection() {
        let mut get =
            Get::new_without_table(1, vec!["body".to_string()], vec![LogicalType::Varchar]);
        get.append_matched_utf8_prefix(0, 2, LogicalType::Varchar);
        assert!(projection_spec(&get, false).is_none());
    }

    #[test]
    fn fulltext_filter_scan_preserves_absorbed_filter_projection() {
        let intent = FullTextIntent {
            column_id: 1,
            query: "graph".to_string(),
            query_kind: FullTextQueryKind::Legacy,
            query_stats: FullTextQueryStats::new(1),
            config: "simple".to_string(),
            score_mode: FullTextScoreMode::CorpusBm25V1,
        };
        let scan = FullTextFilterScan {
            get: Get::new_without_table(
                7,
                vec!["id".to_string(), "body".to_string(), "category".to_string()],
                vec![
                    LogicalType::Integer,
                    LogicalType::Varchar,
                    LogicalType::Varchar,
                ],
            ),
            projection_map: vec![2, 0].into(),
            request: NormalizedSearchRequest {
                table_id: 1,
                mode: SearchRequestMode::Filter,
                predicate: None,
                projections: ProjectionSpec {
                    columns: vec![2, 0],
                    include_score: false,
                },
                intents: vec![SearchIntent::FullText(intent.clone())],
                fusion: None,
            },
            match_expression: Expression::Constant(
                ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
            ),
            other_predicates: Vec::new(),
            residual_predicates: Vec::new(),
            decision: SearchDecision::IndexScan {
                candidate: SearchCandidate {
                    intent: SearchIntent::FullText(intent),
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
        };
        let operator = LogicalOperator::FullTextFilterScan(Box::new(scan));

        assert_eq!(operator.output_names(), ["category", "id"]);
        assert_eq!(
            operator.types(),
            [LogicalType::Varchar, LogicalType::Integer]
        );
        assert_eq!(
            operator.get_column_bindings(),
            [ColumnBinding::new(7, 2), ColumnBinding::new(7, 0)]
        );
    }
}
