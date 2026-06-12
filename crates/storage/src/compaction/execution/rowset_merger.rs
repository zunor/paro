// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::execution::primary_key_merger::PrimaryKeyMerger;
use crate::compaction::execution::statistics_merge::merge_rowset_statistics;
use crate::compaction::execution::workspace::{
    CompactionBuildOutput, CompactionWorkspace, StagedArtifact,
};
use crate::compaction::plan::types::{CompactionPlan, MergeSemantics};
use crate::rowset::RowsetWriterBuilder;
use crate::search::SearchInlineBuilderSet;
use crate::tablet::Tablet;
use paro_common::allocator::Allocator;
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;

pub struct RowsetMerger;

const COMPACTION_BATCH_SIZE: usize = 4096;

impl RowsetMerger {
    pub fn build(
        tablet: &Tablet,
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
        allocator: Arc<dyn Allocator>,
    ) -> Result<Option<CompactionBuildOutput>> {
        Self::build_with_search_inline_builders(
            tablet,
            plan,
            workspace,
            allocator,
            SearchInlineBuilderSet::default(),
        )
    }

    pub fn build_with_search_inline_builders(
        tablet: &Tablet,
        plan: Arc<CompactionPlan>,
        workspace: CompactionWorkspace,
        allocator: Arc<dyn Allocator>,
        search_inline_builders: SearchInlineBuilderSet,
    ) -> Result<Option<CompactionBuildOutput>> {
        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("No schema"))?;

        if plan.input_rowsets.is_empty() {
            return Ok(None);
        }

        match plan.merge_semantics {
            MergeSemantics::Deduplicate => PrimaryKeyMerger::build_with_search_inline_builders(
                tablet,
                plan,
                workspace,
                allocator,
                search_inline_builders,
            ),
            MergeSemantics::Append | MergeSemantics::Aggregate => {
                let mut writer = RowsetWriterBuilder::new(
                    schema.clone(),
                    tablet.tablet_id(),
                    plan.output_version,
                    &workspace.rowset_dir,
                )
                .rowset_id(plan.output_rowset_id)
                .build_hnsw_indexes(false)
                .search_inline_builders(search_inline_builders)
                .build()?;

                for input in &plan.input_rowsets {
                    if workspace.is_cancelled() {
                        return Err(paro_error::query_canceled());
                    }

                    let mut iter = input.rowset.new_iterator()?;
                    while let Some((rows, batch)) = iter.next_batch(COMPACTION_BATCH_SIZE)? {
                        if workspace.is_cancelled() {
                            return Err(paro_error::query_canceled());
                        }
                        let columns =
                            crate::codec::batch_encoder::encode_batch(&schema, &batch, rows)?;
                        writer.add_chunk(&columns)?;
                    }
                }

                let output = writer.build_shared()?;
                output.mark_compaction_output(
                    plan.input_rowsets
                        .iter()
                        .map(|input| input.rowset.rowset_id())
                        .collect(),
                );
                if let Ok(stats) = merge_rowset_statistics(&plan.input_rowsets()) {
                    output.set_statistics_cache(stats);
                }
                Ok(Some(CompactionBuildOutput::Rowset(
                    StagedArtifact::from_rowset(plan, workspace, output)?,
                )))
            }
            MergeSemantics::UniqueLatest => Err(paro_error::not_supported(
                "UNIQUE_KEYS compaction build is reserved for future publish-time keyed delta work",
            )),
        }
    }
}

trait PlanInputRows {
    fn input_rowsets(&self) -> Vec<crate::rowset::RowsetSharedPtr>;
}

impl PlanInputRows for CompactionPlan {
    fn input_rowsets(&self) -> Vec<crate::rowset::RowsetSharedPtr> {
        self.input_rowsets
            .iter()
            .map(|input| input.rowset.clone())
            .collect()
    }
}
