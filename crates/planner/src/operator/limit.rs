// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Limit Operator
//!
//!

use crate::expression::Expression;
use crate::plan::LogicalPlan;
use paro_storage::index::hnsw::HnswQueryOptions;

/// Limit represents a LIMIT/OFFSET operation.
#[derive(Debug)]
pub struct Limit {
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
    /// Typed dense-vector query options propagated to TopN/VectorScan.
    pub hnsw_options: HnswQueryOptions,
    pub child: Box<LogicalPlan>,
}

impl Limit {
    pub fn new(child: LogicalPlan, limit: Option<Expression>, offset: Option<Expression>) -> Self {
        Self {
            limit,
            offset,
            hnsw_options: HnswQueryOptions::default(),
            child: Box::new(child),
        }
    }

    pub fn with_hnsw_options(mut self, hnsw_options: HnswQueryOptions) -> Self {
        self.hnsw_options = hnsw_options;
        self
    }
}
