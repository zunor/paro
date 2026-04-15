// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::index_rebuild::CompactionIndexRebuilder;
use crate::compaction::plan::types::CompactionPlan;
use crate::rowset::RowsetSharedPtr;
use crate::table::index_runtime::IndexRuntime;
use crate::tablet::{ColumnId, Tablet};
use paro_common::error::{self as paro_error, Result};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Rebuilder for full-text runtime indexes during compaction.
///
/// Compaction output segments are rewritten without full-text payload pages.
/// This rebuilder reconstructs runtime full-text indexes for columns that were
/// indexed in compaction input rowsets.
pub struct FullTextIndexRebuilder;

impl FullTextIndexRebuilder {
    pub fn new() -> Self {
        Self
    }

    fn collect_indexed_columns(
        input_rowsets: &[RowsetSharedPtr],
    ) -> Result<Vec<(ColumnId, String)>> {
        let mut columns: BTreeMap<ColumnId, String> = BTreeMap::new();
        for rowset in input_rowsets {
            rowset.load()?;
            for segment in rowset.segments() {
                for meta in segment.column_metas() {
                    let column_id = meta.column_id;
                    if let Some(index) = segment.fulltext_index(column_id) {
                        let config = index.tokenizer().kind().config_name().to_string();
                        match columns.entry(column_id) {
                            Entry::Vacant(entry) => {
                                entry.insert(config);
                            }
                            Entry::Occupied(entry) => {
                                if !entry.get().eq_ignore_ascii_case(&config) {
                                    return Err(paro_error::data_corrupted(format!(
                                        "inconsistent full-text tokenizer config for column {}",
                                        column_id
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(columns.into_iter().collect())
    }
}

impl CompactionIndexRebuilder for FullTextIndexRebuilder {
    fn name(&self) -> &'static str {
        "FULLTEXT"
    }

    fn is_applicable(
        &self,
        _tablet: &Tablet,
        _rowset: &RowsetSharedPtr,
        plan: &CompactionPlan,
    ) -> bool {
        let input_rowsets = plan.input_rowset_ptrs();
        Self::collect_indexed_columns(&input_rowsets)
            .map(|columns| !columns.is_empty())
            .unwrap_or(false)
    }

    fn rebuild(
        &self,
        _tablet: &Tablet,
        rowset: &RowsetSharedPtr,
        plan: &CompactionPlan,
    ) -> Result<()> {
        let input_rowsets = plan.input_rowset_ptrs();
        let indexed_columns = Self::collect_indexed_columns(&input_rowsets)?;
        if indexed_columns.is_empty() {
            return Ok(());
        }
        IndexRuntime::build_runtime_fulltext_indexes_for_rowset(rowset, &indexed_columns)
    }
}

pub fn register_fulltext_rebuilder() -> Result<()> {
    crate::compaction::execution::index_rebuild::register_compaction_index_rebuilder(Arc::new(
        FullTextIndexRebuilder::new(),
    ))
}
