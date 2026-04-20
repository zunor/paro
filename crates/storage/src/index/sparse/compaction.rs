// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::index_rebuild::{
    CompactionGenerationContext, CompactionIndexRebuilder,
};
use crate::compaction::plan::types::CompactionPlan;
use crate::rowset::RowsetSharedPtr;
use crate::search::write_path::{
    materialize_rowset_artifacts, SearchWritePlan, SparseWriteBinding,
};
use crate::tablet::{ColumnId, Tablet};
use paro_common::error::Result;
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct SparseIndexRebuilder;

impl SparseIndexRebuilder {
    pub fn new() -> Self {
        Self
    }

    fn collect_indexed_columns(input_rowsets: &[RowsetSharedPtr]) -> Result<Vec<ColumnId>> {
        let mut columns = BTreeSet::new();
        for rowset in input_rowsets {
            rowset.load()?;
            for segment in rowset.segments() {
                for meta in segment.column_metas() {
                    if meta.sparse_index_pointer.is_some() {
                        columns.insert(meta.column_id);
                    }
                }
            }
        }
        Ok(columns.into_iter().collect())
    }
}

impl CompactionIndexRebuilder for SparseIndexRebuilder {
    fn name(&self) -> &'static str {
        "SPARSE"
    }

    fn is_applicable(
        &self,
        _tablet: &Tablet,
        _rowset: &RowsetSharedPtr,
        plan: &CompactionPlan,
    ) -> bool {
        Self::collect_indexed_columns(&plan.input_rowset_ptrs())
            .map(|columns| !columns.is_empty())
            .unwrap_or(false)
    }

    fn rebuild(
        &self,
        _generation_context: &CompactionGenerationContext,
        _tablet: &Tablet,
        rowset: &RowsetSharedPtr,
        plan: &CompactionPlan,
    ) -> Result<()> {
        let columns = Self::collect_indexed_columns(&plan.input_rowset_ptrs())?;
        if columns.is_empty() {
            return Ok(());
        }
        let plan = SearchWritePlan {
            fulltext: Vec::new(),
            sparse: columns
                .into_iter()
                .map(|column_id| SparseWriteBinding { column_id })
                .collect(),
        };
        materialize_rowset_artifacts(rowset, &plan)
    }
}

pub fn register_sparse_rebuilder() -> Result<()> {
    crate::compaction::execution::index_rebuild::register_compaction_index_rebuilder(Arc::new(
        SparseIndexRebuilder::new(),
    ))
}
