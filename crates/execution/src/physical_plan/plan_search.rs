// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{ConjunctionExpression, Expression, ReferenceExpression};
use paro_planner::operator::Projection as LogicalProjection;
use paro_planner::operator::{
    Filter as LogicalFilter, FullTextFilterScan, LogicalOperator, SearchCandidate, SearchDecision,
    SearchScan, TopN as LogicalTopN,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::index::hnsw::types::{AcornParams, SearchParams, ACORN_MAX_SELECTIVITY_DEFAULT};
use paro_storage::search::{FullTextIntent, FullTextQueryKind, SearchIntent};

use super::generator::PhysicalPlanGenerator;
use super::plan_topn::replace_fulltext_score_with_reference;
use super::predicate_builder;
use crate::operator::filter::Filter as PhysicalFilter;
use crate::operator::projection::Projection;
use crate::operator::search::adaptive_search::{
    AdaptiveSearchCandidatePlan, AdaptiveSearchOperator,
};
use crate::operator::search::fulltext_search::{
    FullTextExecMode, FullTextQueryKind as ExecFullTextQueryKind, FullTextScanBindData,
    PhysicalFullTextScan,
};
use crate::operator::search::sparse_search::{PhysicalSparseVectorScan, SparseVectorScanBindData};
use crate::operator::search::vector_search::{PhysicalVectorScan, VectorScanBindData};
use crate::operator::PhysicalOperator;

impl PhysicalPlanGenerator {
    pub fn create_plan_search_scan(
        &self,
        search: &SearchScan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match &search.decision {
            SearchDecision::IndexScan { candidate, .. } => {
                self.create_search_index_plan(search, candidate)
            }
            SearchDecision::Adaptive {
                candidates,
                sequential,
            } => {
                let sequential_plan = self.create_search_sequential_plan(search)?;
                let sequential_cost = sequential
                    .estimated_cost
                    .map(|cost| cost.score)
                    .unwrap_or(f64::INFINITY);
                let candidate_plans = candidates
                    .iter()
                    .map(|candidate| {
                        Ok(AdaptiveSearchCandidatePlan {
                            label: describe_search_candidate(candidate),
                            estimated_cost: candidate
                                .estimated_cost()
                                .map(|cost| cost.score)
                                .unwrap_or(f64::INFINITY),
                            prefer_hint: candidate.capability.prefer_hint,
                            plan: self.create_search_index_plan(search, candidate)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(AdaptiveSearchOperator::new(
                    sequential_plan,
                    sequential_cost,
                    candidate_plans,
                    search.output_names.clone(),
                )))
            }
        }
    }

    pub fn create_plan_fulltext_filter_scan(
        &self,
        scan: &FullTextFilterScan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match &scan.decision {
            SearchDecision::IndexScan { candidate, .. } => {
                self.create_fulltext_filter_index_plan(scan, candidate)
            }
            SearchDecision::Adaptive {
                candidates,
                sequential,
            } => {
                let sequential_plan = self.create_fulltext_filter_sequential_plan(scan)?;
                let sequential_cost = sequential
                    .estimated_cost
                    .map(|cost| cost.score)
                    .unwrap_or(f64::INFINITY);
                let candidate_plans = candidates
                    .iter()
                    .map(|candidate| {
                        Ok(AdaptiveSearchCandidatePlan {
                            label: describe_search_candidate(candidate),
                            estimated_cost: candidate
                                .estimated_cost()
                                .map(|cost| cost.score)
                                .unwrap_or(f64::INFINITY),
                            prefer_hint: candidate.capability.prefer_hint,
                            plan: self.create_fulltext_filter_index_plan(scan, candidate)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(AdaptiveSearchOperator::new(
                    sequential_plan,
                    sequential_cost,
                    candidate_plans,
                    scan.get.names.clone(),
                )))
            }
        }
    }

    fn create_search_index_plan(
        &self,
        search: &SearchScan,
        candidate: &SearchCandidate,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match &candidate.intent {
            SearchIntent::Hnsw(intent) => self.create_hnsw_index_plan(search, intent, candidate),
            SearchIntent::Sparse(intent) => {
                self.create_sparse_index_plan(search, intent, candidate)
            }
            SearchIntent::FullText(intent) => {
                self.create_fulltext_topk_index_plan(search, intent, candidate)
            }
        }
    }

    fn create_fulltext_filter_index_plan(
        &self,
        scan: &FullTextFilterScan,
        candidate: &SearchCandidate,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let SearchIntent::FullText(intent) = &candidate.intent else {
            return Err(paro_error::internal(format!(
                "invalid search intent for FullTextFilterScan: {:?}",
                candidate.intent
            )));
        };

        let table_entry = scan
            .get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for fulltext scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for fulltext scan"))?
            .clone();
        ensure_planned_capability_still_exists(table_data.as_ref(), candidate)?;

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&scan.other_predicates, &scan.get)?;
        let (runtime_tree, mut runtime_residual) = predicate_builder::build_predicate_tree(
            &scan.get.runtime_filter_expressions,
            &scan.get,
        )?;
        let predicate_tree =
            predicate_builder::combine_predicate_trees(predicate_tree, runtime_tree);
        residual.append(&mut runtime_residual);
        residual.extend(scan.residual_predicates.clone());

        let mut bind = FullTextScanBindData::new(
            table_data,
            intent.query.clone(),
            0,
            intent.column_id as usize,
            scan.get.column_ids.clone(),
        )
        .with_query_options(
            map_fulltext_query_kind(intent.query_kind),
            intent.config.clone(),
        )
        .with_query_stats(intent.query_stats)
        .with_score_mode(intent.score_mode)
        .with_exec_mode(FullTextExecMode::Filter);
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }

        let base_scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalFullTextScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: scan.get.names.clone(),
                relation_name: scan.get.relation_name.clone(),
                relation_alias: scan.get.relation_alias.clone(),
            },
        );
        apply_physical_filter(base_scan, residual, scan.get.names.clone(), self)
    }

    fn create_hnsw_index_plan(
        &self,
        search: &SearchScan,
        intent: &paro_storage::search::HnswIntent,
        candidate: &SearchCandidate,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for vector scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for vector scan"))?
            .clone();
        ensure_planned_capability_still_exists(table_data.as_ref(), candidate)?;

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&search.absorbed_predicates, &get)?;
        residual.extend(search.residual_predicates.clone());

        let mut bind = VectorScanBindData::new(
            table_data,
            intent.query_vector.clone(),
            search.limit,
            intent.column_id as usize,
            get.column_ids.clone(),
        );
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }
        bind = bind.with_params(SearchParams {
            ef: None,
            acorn: Some(AcornParams {
                enable: true,
                max_selectivity: Some(ACORN_MAX_SELECTIVITY_DEFAULT),
            }),
            random_entry_point: None,
        });

        let base_scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalVectorScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        let filtered = apply_physical_filter(base_scan, residual, get.names.clone(), self)?;
        Ok(Arc::new(Projection::new(
            search.projections.clone(),
            filtered,
        )))
    }

    fn create_sparse_index_plan(
        &self,
        search: &SearchScan,
        intent: &paro_storage::search::SparseIntent,
        candidate: &SearchCandidate,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for sparse scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for sparse scan"))?
            .clone();
        ensure_planned_capability_still_exists(table_data.as_ref(), candidate)?;

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&search.absorbed_predicates, &get)?;
        residual.extend(search.residual_predicates.clone());

        let mut bind = SparseVectorScanBindData::new(
            table_data,
            intent.query_vector.clone(),
            search.limit,
            intent.column_id as usize,
            get.column_ids.clone(),
        );
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }

        let base_scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalSparseVectorScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        let filtered = apply_physical_filter(base_scan, residual, get.names.clone(), self)?;
        Ok(Arc::new(Projection::new(
            search.projections.clone(),
            filtered,
        )))
    }

    fn create_fulltext_topk_index_plan(
        &self,
        search: &SearchScan,
        intent: &FullTextIntent,
        candidate: &SearchCandidate,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for fulltext scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for fulltext scan"))?
            .clone();
        ensure_planned_capability_still_exists(table_data.as_ref(), candidate)?;

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&search.absorbed_predicates, &get)?;
        residual.extend(search.residual_predicates.clone());

        let mut bind = FullTextScanBindData::new(
            table_data,
            intent.query.clone(),
            search.limit,
            intent.column_id as usize,
            get.column_ids.clone(),
        )
        .with_query_options(
            map_fulltext_query_kind(intent.query_kind),
            intent.config.clone(),
        )
        .with_query_stats(intent.query_stats)
        .with_score_mode(intent.score_mode)
        .with_exec_mode(FullTextExecMode::ScoreTopK)
        .with_emit_score(true);
        if let Some(tree) = predicate_tree {
            bind = bind.with_predicates(tree);
        }

        let score_column_idx = get.column_ids.len();
        let mut projection_expressions = search.projections.clone();
        if let Some(expr) = projection_expressions.get_mut(search.score_projection_index) {
            *expr = replace_fulltext_score_with_reference(expr.clone(), score_column_idx);
        } else {
            return Err(paro_error::internal(format!(
                "score projection index {} out of bounds",
                search.score_projection_index
            )));
        }

        let base_scan: Arc<dyn PhysicalOperator> = self.annotate_schema(
            Arc::new(PhysicalFullTextScan::new(bind)),
            crate::explain::types::ExplainSchema {
                output_names: get.names.clone(),
                relation_name: get.relation_name.clone(),
                relation_alias: get.relation_alias.clone(),
            },
        );
        let filtered = apply_physical_filter(base_scan, residual, get.names.clone(), self)?;
        Ok(Arc::new(Projection::new(projection_expressions, filtered)))
    }

    fn create_search_sequential_plan(
        &self,
        search: &SearchScan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let mut current = self.create_plan_get(&get)?;

        let mut predicates = search.absorbed_predicates.clone();
        predicates.extend(search.residual_predicates.clone());
        if !predicates.is_empty() {
            let filter = LogicalFilter::new(
                LogicalPlan::synthetic(LogicalOperator::Get(get.clone())),
                predicates,
            );
            current = self.create_plan_filter(&filter, current)?;
        }

        let projection = LogicalProjection::new(
            search.projection_table_index,
            LogicalPlan::synthetic(LogicalOperator::Get(get.clone())),
            search.projections.clone(),
        )
        .with_output_names(search.output_names.clone());
        current = self.create_plan_projection(&projection, current)?;

        let topn_projection = LogicalProjection::new(
            search.projection_table_index,
            LogicalPlan::synthetic(LogicalOperator::Get(get)),
            search.projections.clone(),
        )
        .with_output_names(search.output_names.clone());
        let order = OrderByNode {
            expression: Expression::Reference(ReferenceExpression::new(
                search.score_projection_index,
                search.score_expression.return_type(),
            )),
            ascending: search.order_ascending,
            nulls_first: false,
        };
        let topn = LogicalTopN::new(
            LogicalPlan::synthetic(LogicalOperator::Projection(topn_projection)),
            vec![order],
            search.limit,
            0,
        );
        self.create_plan_topn(&topn, current)
    }

    fn create_fulltext_filter_sequential_plan(
        &self,
        scan: &FullTextFilterScan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&scan.get);
        let current = self.create_plan_get(&scan.get)?;

        let mut predicates = vec![scan.match_expression.clone()];
        predicates.extend(scan.other_predicates.clone());
        predicates.extend(scan.residual_predicates.clone());
        let filter = LogicalFilter::new(
            LogicalPlan::synthetic(LogicalOperator::Get(get)),
            predicates,
        );
        self.create_plan_filter(&filter, current)
    }
}

fn clear_runtime_filters(get: &paro_planner::operator::Get) -> paro_planner::operator::Get {
    let mut get = get.clone();
    get.runtime_filter_expressions.clear();
    get
}

fn apply_physical_filter(
    child: Arc<dyn PhysicalOperator>,
    predicates: Vec<Expression>,
    output_names: Vec<String>,
    generator: &PhysicalPlanGenerator,
) -> Result<Arc<dyn PhysicalOperator>> {
    if predicates.is_empty() {
        return Ok(child);
    }

    let predicate = if predicates.len() == 1 {
        predicates[0].clone()
    } else {
        Expression::Conjunction(ConjunctionExpression {
            conjunction_type: paro_planner::expression::ConjunctionType::And,
            children: predicates,
        })
    };
    let filter: Arc<dyn PhysicalOperator> = Arc::new(PhysicalFilter::new(predicate, child.clone()));
    Ok(generator.annotate_schema(filter, generator.passthrough_schema(&child, output_names)))
}

fn ensure_planned_capability_still_exists(
    table_data: &paro_storage::table::table_handle::TableHandle,
    candidate: &SearchCandidate,
) -> Result<()> {
    let Some(capability) = table_data.search_capability(&candidate.intent) else {
        return Err(paro_error::internal(format!(
            "planned search capability is no longer queryable: {:?}",
            candidate.intent
        )));
    };
    if !capability.is_queryable() {
        return Err(paro_error::internal(format!(
            "planned search capability lost queryable coverage: {:?}",
            candidate.intent
        )));
    }
    Ok(())
}

fn map_fulltext_query_kind(query_kind: FullTextQueryKind) -> ExecFullTextQueryKind {
    match query_kind {
        FullTextQueryKind::Legacy => ExecFullTextQueryKind::Legacy,
        FullTextQueryKind::TsQuery => ExecFullTextQueryKind::TsQuery,
        FullTextQueryKind::Plain => ExecFullTextQueryKind::Plain,
        FullTextQueryKind::Phrase => ExecFullTextQueryKind::Phrase,
        FullTextQueryKind::WebSearch => ExecFullTextQueryKind::WebSearch,
    }
}

fn describe_search_candidate(candidate: &SearchCandidate) -> String {
    match &candidate.intent {
        SearchIntent::Hnsw(intent) => format!("hnsw_vector(column_id={})", intent.column_id),
        SearchIntent::Sparse(intent) => format!("sparse_vector(column_id={})", intent.column_id),
        SearchIntent::FullText(intent) => format!(
            "fulltext(column_id={}, score_mode={}, query_terms={})",
            intent.column_id,
            intent.score_mode.as_str(),
            intent.query_stats.effective_query_terms()
        ),
    }
}
