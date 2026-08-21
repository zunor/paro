// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Base table scan. Carries `TableCatalogEntry` so execution can open storage without another catalog lookup.

use std::sync::Arc;

use crate::expression::Expression;
use crate::operator::{ColumnBinding, LogicalOperator};
use crate::plan::LogicalPlan;
use paro_catalog::entry::TableCatalogEntry;
use paro_common::types::LogicalType;
use paro_storage::table::segment_reorderer::SegmentOrderOptions;

/// Physical source and value semantics of one Get output.
///
/// `Stored` is the only variant that denotes equality with a catalog column.
/// Derived values carry their source explicitly and therefore cannot be
/// mistaken for the stored value by statistics, runtime-filter, or row-fetch
/// consumers. A virtual row id has no catalog column at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetColumnSource {
    Stored {
        column_id: usize,
    },
    MatchedUtf8Prefix {
        source_column: usize,
        byte_width: usize,
    },
    VirtualRowId,
}

/// Get represents a scan operation on a table.
///
/// This operator is created during planning when a base table is referenced
/// in the FROM clause. It holds all information needed to perform a table scan,
/// including an optional reference to the actual table catalog entry.
#[derive(Debug, Clone)]
pub struct Get {
    /// The index of the table in the bind context.
    pub table_index: usize,
    /// The types of the columns returned by this scan.
    pub returned_types: Vec<LogicalType>,
    /// The names of the columns returned by this scan.
    pub names: Vec<String>,
    /// Stable relation name used for explain output.
    pub relation_name: Option<String>,
    /// Optional user-visible alias.
    pub relation_alias: Option<String>,
    /// Physical source and value semantics for each output.
    pub column_sources: Vec<GetColumnSource>,
    /// The physical logical type read for each output source.
    pub column_types: Vec<LogicalType>,
    /// Reference to the table catalog entry.
    /// This provides access to table metadata and storage (segments) during
    /// physical plan generation.
    ///
    /// the table from the bind_data. Here we store it directly for simplicity.
    pub table: Option<Arc<TableCatalogEntry>>,
    /// Optional scan order for segments.
    pub scan_order: Option<SegmentOrderOptions>,
    /// Semantically redundant runtime filters injected by the optimizer (for
    /// example join-derived min/max bounds). Expressions are bound against
    /// this Get's output layout and may be discarded by rewrites that cannot
    /// preserve a hint without changing its scope.
    pub runtime_filter_expressions: Vec<Expression>,
}

/// Find the base scan below operators that preserve column-binding identity.
///
/// These operators may filter or reorder rows, and `ExternalProject` may add
/// its own output domain, but every child binding keeps its identity and row
/// multiplicity. Proofs about one preserved binding's value (for example exact
/// non-NULL statistics or a declared unique key) can therefore be checked at
/// their use site without mistaking a join's NULL-extended output for the
/// stored column.
pub fn binding_preserving_get(plan: &LogicalPlan) -> Option<&Get> {
    match &plan.operator {
        LogicalOperator::Get(get) => Some(get),
        LogicalOperator::Filter(filter) => binding_preserving_get(filter.child.as_ref()),
        LogicalOperator::Order(order) => binding_preserving_get(order.child.as_ref()),
        LogicalOperator::TopN(topn) => binding_preserving_get(topn.child.as_ref()),
        LogicalOperator::Limit(limit) => binding_preserving_get(limit.child.as_ref()),
        LogicalOperator::ExternalProject(project) => binding_preserving_get(project.child.as_ref()),
        _ => None,
    }
}

impl Get {
    /// Append one scan output atomically across its source and logical metadata.
    pub fn append_output(
        &mut self,
        name: String,
        returned_type: LogicalType,
        column_type: LogicalType,
        source: GetColumnSource,
    ) -> ColumnBinding {
        let output_index = self.returned_types.len();
        debug_assert_eq!(self.names.len(), output_index);
        debug_assert_eq!(self.column_sources.len(), output_index);
        debug_assert_eq!(self.column_types.len(), output_index);
        self.names.push(name);
        self.returned_types.push(returned_type);
        self.column_sources.push(source);
        self.column_types.push(column_type);
        ColumnBinding::new(self.table_index, output_index)
    }

    /// Append the storage row-location output used by DML and late fetch.
    /// A row id is a scan capability, never a catalog column sentinel.
    pub fn append_virtual_rowid(&mut self, name: impl Into<String>) -> ColumnBinding {
        if let Some(output_index) = self
            .column_sources
            .iter()
            .position(|source| matches!(source, GetColumnSource::VirtualRowId))
        {
            return ColumnBinding::new(self.table_index, output_index);
        }
        self.append_output(
            name.into(),
            LogicalType::BigInt,
            LogicalType::BigInt,
            GetColumnSource::VirtualRowId,
        )
    }

    /// Reuse or append one exact prefix derived from a stored textual column.
    /// Optimizer pipelines may revisit scan projection after a structural
    /// rewrite; the physical value identity, not pass count, determines the
    /// output binding.
    pub fn append_matched_utf8_prefix(
        &mut self,
        source_column: usize,
        byte_width: usize,
        column_type: LogicalType,
    ) -> ColumnBinding {
        if let Some(output_index) = self.column_sources.iter().position(|source| {
            matches!(
                source,
                GetColumnSource::MatchedUtf8Prefix {
                    source_column: candidate,
                    byte_width: candidate_width,
                } if *candidate == source_column && *candidate_width == byte_width
            )
        }) {
            return ColumnBinding::new(self.table_index, output_index);
        }
        self.append_output(
            format!("__ascii_prefix_{source_column}_{byte_width}"),
            LogicalType::Varchar,
            column_type,
            GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            },
        )
    }

    /// Return the catalog column only when this output is the stored value
    /// itself. Derived values deliberately return `None` even when they read
    /// bytes from the same physical column.
    #[inline]
    pub fn stored_column(&self, output_index: usize) -> Option<usize> {
        match self.column_sources.get(output_index)? {
            GetColumnSource::Stored { column_id } => Some(*column_id),
            GetColumnSource::MatchedUtf8Prefix { .. } | GetColumnSource::VirtualRowId => None,
        }
    }

    #[inline]
    pub fn column_source(&self, output_index: usize) -> Option<GetColumnSource> {
        self.column_sources.get(output_index).copied()
    }

    /// Create a new Get with a reference to the table catalog entry.
    ///
    /// # Arguments
    /// * `table_index` - The index assigned to this table in the bind context
    /// * `names` - Column names returned by this scan
    /// * `types` - Column types returned by this scan
    /// * `table` - The table catalog entry (provides access to storage)
    pub fn new(
        table_index: usize,
        names: Vec<String>,
        types: Vec<LogicalType>,
        table: Arc<TableCatalogEntry>,
    ) -> Self {
        let column_sources = (0..types.len())
            .map(|column_id| GetColumnSource::Stored { column_id })
            .collect();
        Self {
            table_index,
            returned_types: types.clone(),
            names,
            relation_name: None,
            relation_alias: None,
            column_sources,
            column_types: types,
            table: Some(table),
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        }
    }

    /// Create a Get without a table reference.
    ///
    /// This is used for table functions or other scan sources that don't
    /// have a direct table catalog entry.
    pub fn new_without_table(
        table_index: usize,
        names: Vec<String>,
        types: Vec<LogicalType>,
    ) -> Self {
        let column_sources = (0..types.len())
            .map(|column_id| GetColumnSource::Stored { column_id })
            .collect();
        Self {
            table_index,
            returned_types: types.clone(),
            names,
            relation_name: None,
            relation_alias: None,
            column_sources,
            column_types: types,
            table: None,
            scan_order: None,
            runtime_filter_expressions: Vec::new(),
        }
    }

    /// Get the table catalog entry if available.
    pub fn get_table(&self) -> Option<&Arc<TableCatalogEntry>> {
        self.table.as_ref()
    }

    pub fn with_relation(mut self, relation_name: String, relation_alias: Option<String>) -> Self {
        self.relation_name = Some(relation_name);
        self.relation_alias = relation_alias;
        self
    }
}
