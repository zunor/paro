// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! `ORDER BY` + `LIMIT` (+ optional `OFFSET`) fused for top-N evaluation without full sort.

use crate::binder::ir::OrderByNode;
use crate::plan::LogicalPlan;

/// TopN represents an optimized ORDER BY + LIMIT operation.
///
/// This operator is created by the TopN optimizer when it detects a pattern of:
/// - ORDER BY with one or more columns
/// - LIMIT with a constant value
/// - Optionally, OFFSET with a constant value
///
/// The TopN operator is more efficient than separate ORDER + LIMIT because it only
/// maintains a heap of size (limit + offset) instead of sorting the entire input.
#[derive(Debug)]
pub struct TopN {
    /// The ORDER BY clauses
    pub orders: Vec<OrderByNode>,
    /// The LIMIT value (must be constant)
    pub limit: usize,
    /// The OFFSET value (must be constant, 0 if not specified)
    pub offset: usize,
    /// Optional SQL hint `/*+ HNSW_EF(N) */` propagated from Limit.
    pub hnsw_ef_hint: Option<usize>,
    /// The child operator
    pub child: Box<LogicalPlan>,
}

impl TopN {
    /// Create a new TopN operator.
    pub fn new(child: LogicalPlan, orders: Vec<OrderByNode>, limit: usize, offset: usize) -> Self {
        Self {
            orders,
            limit,
            offset,
            hnsw_ef_hint: None,
            child: Box::new(child),
        }
    }

    pub fn with_hnsw_ef_hint(mut self, hnsw_ef_hint: Option<usize>) -> Self {
        self.hnsw_ef_hint = hnsw_ef_hint;
        self
    }

    /// Get the total number of rows to keep (limit + offset).
    pub fn total_rows(&self) -> usize {
        self.limit.saturating_add(self.offset)
    }
}
