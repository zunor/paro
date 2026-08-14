// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Projection Operator
//!
//!

use crate::expression::Expression;
use crate::plan::LogicalPlan;
use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use std::sync::Arc;

/// One base-table row source materialized after a narrow relational carrier.
///
/// `materialized_table_index` owns a private expression namespace whose column
/// ordinals are stable catalog column ids, not positions in the pruned `Get`.
/// `rowid` names the carrier column used to fetch those values. It is a
/// `ColumnRef` during optimization and a positional `Reference` after physical
/// binding resolution.
#[derive(Debug, Clone)]
pub struct LateRowFetchSource {
    pub materialized_table_index: usize,
    pub rowid: Expression,
    pub table: Arc<TableCatalogEntry>,
}

/// Physical late-materialization contract attached to a projection.
///
/// Projection expressions may reference either the ordinary carrier binding
/// or one of the private materialized-table namespaces above. The physical
/// row-fetch project resolves the rowids, reads only referenced catalog
/// columns under the query snapshot, then evaluates the original projection.
#[derive(Debug, Clone)]
pub struct LateRowFetch {
    pub carrier_table_index: usize,
    pub sources: Vec<LateRowFetchSource>,
    /// Buffer a bounded carrier and execute the sparse fetch once during
    /// transform flush. This is used for post-filter/pre-TopN payload where
    /// repeated page-local point reads dominate the small result.
    pub coalesce_input: bool,
}

/// Projection represents a projection operation (SELECT list).
#[derive(Debug)]
pub struct Projection {
    pub table_index: usize,
    pub expressions: Vec<Expression>,
    pub output_names: Vec<String>,
    pub child: Box<LogicalPlan>,
    pub returned_types: Vec<LogicalType>, // Cached types of expressions
    pub late_row_fetch: Option<LateRowFetch>,
}

impl Projection {
    pub fn new(table_index: usize, child: LogicalPlan, expressions: Vec<Expression>) -> Self {
        let returned_types = expressions.iter().map(|e| e.return_type()).collect();
        let output_names = (0..expressions.len())
            .map(|idx| format!("expr_{}", idx + 1))
            .collect();
        Self {
            table_index,
            expressions,
            output_names,
            child: Box::new(child),
            returned_types,
            late_row_fetch: None,
        }
    }

    pub fn with_output_names(mut self, output_names: Vec<String>) -> Self {
        self.output_names = output_names;
        self
    }

    pub fn with_late_row_fetch(mut self, late_row_fetch: LateRowFetch) -> Self {
        self.late_row_fetch = Some(late_row_fetch);
        self
    }
}
