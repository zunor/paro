// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tail catch-up work planning.

use std::collections::BTreeMap;

use paro_common::error::Result;

use crate::rowset::{RowsetId, RowsetSharedPtr};

use crate::search::capability::SearchIndexDefinition;
use crate::search::manifest::LoadedManifest;
use crate::search::tail::TailMutationKind;

#[derive(Debug, Clone)]
pub(crate) struct CatchUpWorkItem {
    pub(crate) rowset: RowsetSharedPtr,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CatchUpPlan {
    pub(crate) items: Vec<CatchUpWorkItem>,
    /// Canonical vector rows admitted into this immutable build quantum.
    /// Carry compaction uses the exact planned cardinality to select the
    /// destination size level before provider work starts.
    pub(crate) planned_rows: u64,
}

impl CatchUpPlan {
    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CatchUpPlanner;

impl CatchUpPlanner {
    pub(crate) fn plan(
        &self,
        definition: &SearchIndexDefinition,
        manifest: &LoadedManifest,
        visible_by_id: &BTreeMap<RowsetId, RowsetSharedPtr>,
    ) -> Result<CatchUpPlan> {
        let rowset_limit = manifest
            .root
            .maintenance_state
            .recovery
            .rowset_rate_limit
            .max(1);
        let row_limit = manifest
            .root
            .maintenance_state
            .recovery
            .row_rate_limit
            .max(1);
        let mut items = Vec::new();
        let mut planned_rows = 0u64;

        for entry in &manifest.tail_pending_entries {
            if matches!(entry.mutation, TailMutationKind::Delete) {
                continue;
            }
            if items.len() >= rowset_limit {
                break;
            }
            if !items.is_empty() && planned_rows.saturating_add(entry.row_count) > row_limit {
                break;
            }
            let Some(rowset) = visible_by_id.get(&entry.rowset_id) else {
                continue;
            };
            rowset.load()?;
            if !rowset_can_materialize_definition(definition, rowset) {
                continue;
            }
            planned_rows = planned_rows.saturating_add(entry.row_count);
            items.push(CatchUpWorkItem {
                rowset: rowset.clone(),
            });
        }

        Ok(CatchUpPlan {
            items,
            planned_rows,
        })
    }
}

fn rowset_can_materialize_definition(
    definition: &SearchIndexDefinition,
    rowset: &RowsetSharedPtr,
) -> bool {
    rowset.segments().iter().all(|segment| {
        definition.column_ids.iter().all(|column_id| {
            segment
                .column_metas()
                .iter()
                .any(|meta| meta.column_id == *column_id)
        })
    })
}
