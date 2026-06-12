// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical operators for capability-aware search rewrites.

use paro_common::types::LogicalType;
use paro_storage::search::{
    CapabilityToken, NormalizedSearchRequest, SearchCostEstimate, SearchIndexKind, SearchIntent,
    SequentialCapability,
};

pub use paro_storage::search::{
    analyze_fulltext_query_stats, build_fulltext_query_stats, normalize_fulltext_config,
    FullTextQueryKind, FullTextQueryStats, FullTextScoreMode,
};
pub type FullTextQueryStatsKind = FullTextQueryKind;

use crate::expression::Expression;

use super::Get;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchCandidate {
    pub intent: SearchIntent,
    pub token: CapabilityToken,
    pub kind: SearchIndexKind,
    pub estimated_cost: Option<SearchCostEstimate>,
}

impl SearchCandidate {
    pub const fn estimated_cost(&self) -> Option<SearchCostEstimate> {
        self.estimated_cost
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchDecision {
    IndexScan {
        candidate: SearchCandidate,
        confidence: Confidence,
    },
    Adaptive {
        candidates: Vec<SearchCandidate>,
        sequential: SequentialCapability,
    },
}

#[derive(Debug, Clone)]
pub struct SearchScan {
    pub get: Get,
    pub request: NormalizedSearchRequest,
    pub decision: SearchDecision,
    pub projections: Vec<Expression>,
    /// Preserves the absorbed Projection's aliases for EXPLAIN and execution.
    pub output_names: Vec<String>,
    pub projection_table_index: usize,
    pub absorbed_predicates: Vec<Expression>,
    pub residual_predicates: Vec<Expression>,
    /// Index of the score expression inside `projections`.
    pub score_projection_index: usize,
    pub score_expression: Expression,
    pub order_ascending: bool,
    pub limit: usize,
}

impl SearchScan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        get: Get,
        request: NormalizedSearchRequest,
        decision: SearchDecision,
        projections: Vec<Expression>,
        projection_table_index: usize,
        absorbed_predicates: Vec<Expression>,
        residual_predicates: Vec<Expression>,
        score_projection_index: usize,
        score_expression: Expression,
        order_ascending: bool,
        limit: usize,
    ) -> Self {
        let output_names = (0..projections.len())
            .map(|idx| format!("expr_{}", idx + 1))
            .collect();
        Self {
            get,
            request,
            decision,
            projections,
            output_names,
            projection_table_index,
            absorbed_predicates,
            residual_predicates,
            score_projection_index,
            score_expression,
            order_ascending,
            limit,
        }
    }

    pub fn with_output_names(mut self, output_names: Vec<String>) -> Self {
        self.output_names = output_names;
        self
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.projections
            .iter()
            .map(|expr| expr.return_type())
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct FullTextFilterScan {
    pub get: Get,
    pub request: NormalizedSearchRequest,
    pub match_expression: Expression,
    pub other_predicates: Vec<Expression>,
    pub residual_predicates: Vec<Expression>,
    pub decision: SearchDecision,
}

impl FullTextFilterScan {
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.get.returned_types.clone()
    }
}
