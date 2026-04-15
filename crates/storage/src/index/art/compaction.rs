use crate::compaction::execution::index_rebuild::CompactionIndexRebuilder;
use crate::compaction::plan::types::CompactionPlan;
use crate::rowset::RowsetSharedPtr;
use crate::table::index_runtime::IndexRuntime;
use crate::tablet::Tablet;
use paro_common::error::Result;
use std::sync::Arc;

/// Rebuilder for runtime ART predicate indexes during compaction.
///
/// ART is immutable and segment-local, so newly compacted rowsets need a fresh
/// build for every declared ART column on the tablet.
pub struct ArtIndexRebuilder;

impl ArtIndexRebuilder {
    pub fn new() -> Self {
        Self
    }
}

impl CompactionIndexRebuilder for ArtIndexRebuilder {
    fn name(&self) -> &'static str {
        "ART"
    }

    fn is_applicable(
        &self,
        tablet: &Tablet,
        _rowset: &RowsetSharedPtr,
        _plan: &CompactionPlan,
    ) -> bool {
        !tablet.declared_art_columns().is_empty()
    }

    fn rebuild(
        &self,
        tablet: &Tablet,
        rowset: &RowsetSharedPtr,
        _plan: &CompactionPlan,
    ) -> Result<()> {
        let art_columns = tablet.declared_art_columns();
        if art_columns.is_empty() {
            return Ok(());
        }
        IndexRuntime::build_runtime_art_indexes_for_rowset(rowset, &art_columns)
    }
}

pub fn register_art_rebuilder() -> Result<()> {
    crate::compaction::execution::index_rebuild::register_compaction_index_rebuilder(Arc::new(
        ArtIndexRebuilder::new(),
    ))
}
