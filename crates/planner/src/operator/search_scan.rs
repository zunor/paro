//! Logical operators for search-path rewrites.

use paro_common::types::LogicalType;

use crate::expression::Expression;

use super::Get;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchType {
    HnswVector { column_id: u32 },
    SparseVector { column_id: u32 },
    FullTextTopK { column_id: u32 },
    FullTextFilter { column_id: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchCandidate {
    pub search_type: SearchType,
    pub estimated_cost: f64,
    pub threshold: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchDecision {
    IndexScan {
        search_type: SearchType,
        estimated_cost: f64,
        confidence: Confidence,
    },
    DeferToRuntime {
        candidates: Vec<SearchCandidate>,
        sequential_cost: f64,
    },
}

#[derive(Debug, Clone)]
pub struct SearchScan {
    pub get: Get,
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
