// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical late materialization from stable base-table row identifiers.

use std::sync::Arc;

use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;

use crate::expression::Expression;
use crate::plan::LogicalPlan;

/// One base-table namespace materialized from a rowid carried by the child.
#[derive(Debug, Clone)]
pub struct RowFetchSource {
    pub materialized_table_index: usize,
    pub rowid: Expression,
    pub table: Arc<TableCatalogEntry>,
}

/// Row-preserving materialization boundary.
///
/// The child remains the relational carrier. Each source adds a private
/// catalog-column namespace whose ordinals are physical catalog column ids.
/// A projection above this operator selects the fetched columns it actually
/// needs; physical lowering may fuse that projection with the fetch.
#[derive(Debug)]
pub struct RowFetch {
    pub carrier_table_index: usize,
    pub sources: Vec<RowFetchSource>,
    pub child: Box<LogicalPlan>,
}

impl RowFetch {
    pub fn new(
        carrier_table_index: usize,
        sources: Vec<RowFetchSource>,
        child: LogicalPlan,
    ) -> Self {
        Self {
            carrier_table_index,
            sources,
            child: Box::new(child),
        }
    }

    pub fn output_names(&self) -> Vec<String> {
        let mut names = self.child.output_names();
        for source in &self.sources {
            names.extend(
                source
                    .table
                    .columns
                    .iter()
                    .map(|column| column.name.clone()),
            );
        }
        names
    }

    pub fn output_types(&self) -> Vec<LogicalType> {
        let mut types = self.child.types();
        for source in &self.sources {
            types.extend(
                source
                    .table
                    .columns
                    .iter()
                    .map(|column| column.logical_type.clone()),
            );
        }
        types
    }
}
