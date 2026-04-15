use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_planner::binder::ir::OrderByNode;
use paro_planner::expression::{ConjunctionExpression, Expression, ReferenceExpression};
use paro_planner::operator::Projection as LogicalProjection;
use paro_planner::operator::{
    Filter as LogicalFilter, FullTextFilterScan, LogicalOperator, SearchDecision, SearchScan,
    SearchType, TopN as LogicalTopN,
};
use paro_planner::plan::LogicalPlan;
use paro_storage::index::hnsw::types::{AcornParams, SearchParams, ACORN_MAX_SELECTIVITY_DEFAULT};

use super::generator::PhysicalPlanGenerator;
use super::plan_filter::{
    extract_fulltext_match, fulltext_index_pushdown_ready as fulltext_filter_pushdown_ready,
};
use super::plan_topn::{
    extract_fulltext_score, extract_sparse_vector_distance, extract_vector_distance,
    fulltext_index_pushdown_ready as fulltext_topk_pushdown_ready,
    replace_fulltext_score_with_reference,
};
use super::predicate_builder;
use crate::operator::filter::Filter as PhysicalFilter;
use crate::operator::projection::Projection;
use crate::operator::scan::adaptive_scan::{AdaptiveCandidatePlan, AdaptiveScanOperator};
use crate::operator::scan::fulltext_scan::{
    FullTextExecMode, FullTextScanBindData, PhysicalFullTextScan,
};
use crate::operator::scan::sparse_vector_scan::{
    PhysicalSparseVectorScan, SparseVectorScanBindData,
};
use crate::operator::scan::vector_scan::{PhysicalVectorScan, VectorScanBindData};
use crate::operator::PhysicalOperator;

impl PhysicalPlanGenerator {
    pub fn create_plan_search_scan(
        &self,
        search: &SearchScan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match &search.decision {
            SearchDecision::IndexScan { search_type, .. } => {
                self.create_search_index_plan(search, search_type)
            }
            SearchDecision::DeferToRuntime {
                candidates,
                sequential_cost,
            } => {
                if let Some(candidate) = best_fulltext_candidate(candidates) {
                    return self.create_search_index_plan(search, &candidate.search_type);
                }
                let sequential_plan = self.create_search_sequential_plan(search)?;
                let mut candidate_plans = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    candidate_plans.push(AdaptiveCandidatePlan {
                        label: describe_search_type(&candidate.search_type),
                        estimated_cost: candidate.estimated_cost,
                        plan: self.create_search_index_plan(search, &candidate.search_type)?,
                    });
                }
                Ok(Arc::new(AdaptiveScanOperator::new(
                    sequential_plan,
                    *sequential_cost,
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
            SearchDecision::IndexScan { search_type, .. } => {
                self.create_fulltext_filter_index_plan(scan, search_type)
            }
            SearchDecision::DeferToRuntime {
                candidates,
                sequential_cost,
            } => {
                if let Some(candidate) = best_fulltext_candidate(candidates) {
                    return self.create_fulltext_filter_index_plan(scan, &candidate.search_type);
                }
                let sequential_plan = self.create_fulltext_filter_sequential_plan(scan)?;
                let mut candidate_plans = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    candidate_plans.push(AdaptiveCandidatePlan {
                        label: describe_search_type(&candidate.search_type),
                        estimated_cost: candidate.estimated_cost,
                        plan: self
                            .create_fulltext_filter_index_plan(scan, &candidate.search_type)?,
                    });
                }
                Ok(Arc::new(AdaptiveScanOperator::new(
                    sequential_plan,
                    *sequential_cost,
                    candidate_plans,
                    scan.get.names.clone(),
                )))
            }
        }
    }

    fn create_search_index_plan(
        &self,
        search: &SearchScan,
        search_type: &SearchType,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match search_type {
            SearchType::HnswVector { .. } => self.create_hnsw_index_plan(search),
            SearchType::SparseVector { .. } => self.create_sparse_index_plan(search),
            SearchType::FullTextTopK { .. } => self.create_fulltext_topk_index_plan(search),
            SearchType::FullTextFilter { .. } => Err(paro_error::internal(
                "FullTextFilter candidate is invalid for SearchScan".to_string(),
            )),
        }
    }

    fn create_fulltext_filter_index_plan(
        &self,
        scan: &FullTextFilterScan,
        search_type: &SearchType,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        if !matches!(search_type, SearchType::FullTextFilter { .. }) {
            return Err(paro_error::internal(format!(
                "invalid search type for FullTextFilterScan: {:?}",
                search_type
            )));
        }

        let match_info = extract_fulltext_match(&scan.match_expression, &scan.get)?
            .ok_or_else(|| paro_error::internal("failed to extract fulltext filter query"))?;
        let table_entry = scan
            .get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for fulltext scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for fulltext scan"))?
            .clone();
        if !fulltext_filter_pushdown_ready(
            self,
            table_entry.as_ref(),
            table_data.as_ref(),
            &match_info,
        ) {
            return Err(paro_error::internal(
                "FullTextFilterScan produced without a ready fulltext index".to_string(),
            ));
        }

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
            match_info.query_text,
            0,
            match_info.text_column_id,
            scan.get.column_ids.clone(),
        )
        .with_query_options(match_info.query_kind, match_info.config)
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

    fn create_hnsw_index_plan(&self, search: &SearchScan) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let (vector_col_id, query_vec) = extract_vector_distance(&search.score_expression, &get)?
            .ok_or_else(|| {
            paro_error::internal("failed to extract vector search parameters")
        })?;
        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for vector scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for vector scan"))?
            .clone();
        if !table_data.has_vector_index(vector_col_id as u32) {
            return Err(paro_error::internal(
                "SearchScan produced without an HNSW index".to_string(),
            ));
        }

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&search.absorbed_predicates, &get)?;
        residual.extend(search.residual_predicates.clone());

        let mut bind = VectorScanBindData::new(
            table_data,
            query_vec,
            search.limit,
            vector_col_id,
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

    fn create_sparse_index_plan(&self, search: &SearchScan) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let (sparse_col_id, query_vec) =
            extract_sparse_vector_distance(&search.score_expression, &get)?.ok_or_else(|| {
                paro_error::internal("failed to extract sparse vector search parameters")
            })?;
        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for sparse scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for sparse scan"))?
            .clone();
        if !table_data.has_sparse_index(sparse_col_id as u32) {
            return Err(paro_error::internal(
                "SearchScan produced without a sparse vector index".to_string(),
            ));
        }

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&search.absorbed_predicates, &get)?;
        residual.extend(search.residual_predicates.clone());

        let mut bind = SparseVectorScanBindData::new(
            table_data,
            query_vec,
            search.limit,
            sparse_col_id,
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
    ) -> Result<Arc<dyn PhysicalOperator>> {
        let get = clear_runtime_filters(&search.get);
        let score_info = extract_fulltext_score(&search.score_expression, &get)?
            .ok_or_else(|| paro_error::internal("failed to extract fulltext search parameters"))?;
        let table_entry = get
            .get_table()
            .ok_or_else(|| paro_error::internal("Get missing table reference for fulltext scan"))?;
        let table_data = table_entry
            .get_storage()
            .ok_or_else(|| paro_error::internal("Table storage unavailable for fulltext scan"))?
            .clone();
        if !fulltext_topk_pushdown_ready(
            self,
            table_entry.as_ref(),
            table_data.as_ref(),
            &score_info,
        ) {
            return Err(paro_error::internal(
                "SearchScan produced without a ready fulltext index".to_string(),
            ));
        }

        let (predicate_tree, mut residual) =
            predicate_builder::build_predicate_tree(&search.absorbed_predicates, &get)?;
        residual.extend(search.residual_predicates.clone());

        let mut bind = FullTextScanBindData::new(
            table_data,
            score_info.query_text,
            search.limit,
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

fn describe_search_type(search_type: &SearchType) -> String {
    match search_type {
        SearchType::HnswVector { .. } => "hnsw_vector".to_string(),
        SearchType::SparseVector { .. } => "sparse_vector".to_string(),
        SearchType::FullTextTopK { .. } => "fulltext_topk".to_string(),
        SearchType::FullTextFilter { .. } => "fulltext_filter".to_string(),
    }
}

fn best_fulltext_candidate<'a>(
    candidates: &'a [paro_planner::operator::SearchCandidate],
) -> Option<&'a paro_planner::operator::SearchCandidate> {
    candidates
        .iter()
        .filter(|candidate| {
            matches!(
                candidate.search_type,
                SearchType::FullTextTopK { .. } | SearchType::FullTextFilter { .. }
            )
        })
        .min_by(|left, right| left.estimated_cost.total_cmp(&right.estimated_cost))
}
