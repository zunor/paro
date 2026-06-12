// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::rowset_merger::RowsetMerger;
use crate::compaction::execution::workspace::{CompactionBuildOutput, CompactionWorkspace};
use crate::compaction::plan::types::CompactionPlan;
use crate::search::SearchInlineBuilderSet;
use crate::tablet::Tablet;
use paro_common::allocator::default_allocator;
use paro_common::error::Result;
use std::sync::Arc;

pub struct VerticalMerger;

pub type VerticalMerge = VerticalMerger;

impl VerticalMerger {
    pub fn build(
        tablet: &Tablet,
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
    ) -> Result<Option<CompactionBuildOutput>> {
        Self::build_with_search_inline_builders(
            tablet,
            plan,
            workspace,
            SearchInlineBuilderSet::default(),
        )
    }

    pub fn build_with_search_inline_builders(
        tablet: &Tablet,
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
        search_inline_builders: SearchInlineBuilderSet,
    ) -> Result<Option<CompactionBuildOutput>> {
        RowsetMerger::build_with_search_inline_builders(
            tablet,
            plan,
            workspace,
            Arc::new(default_allocator()),
            search_inline_builders,
        )
    }
}
