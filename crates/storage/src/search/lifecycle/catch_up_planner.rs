// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tail catch-up work planning.

use std::collections::BTreeMap;

use paro_common::error::Result;

use crate::rowset::{RowsetId, RowsetSharedPtr};

use crate::search::capability::SearchIndexDefinition;
use crate::search::manifest::LoadedManifest;
use crate::search::tail::{TailMutationKind, TailPendingEntry};

#[derive(Debug, Clone)]
pub(crate) struct CatchUpWorkItem {
    /// Exact immutable tail identity admitted into this build quantum.
    ///
    /// Keeping the manifest entry beside its retained rowset prevents the
    /// provider from accidentally observing tail appended after planning or
    /// materializing more work than the scheduler admitted.
    pub(crate) tail_entry: TailPendingEntry,
    pub(crate) rowset: RowsetSharedPtr,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CatchUpPlan {
    pub(crate) items: Vec<CatchUpWorkItem>,
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
                tail_entry: entry.clone(),
                rowset: rowset.clone(),
            });
        }

        Ok(CatchUpPlan { items })
    }

    /// Plan every currently materializable tail rowset for an explicit
    /// foreground materialization request.
    ///
    /// Background catch-up obeys the immutable L0 build quantum encoded in
    /// manifest rate limits. CREATE INDEX / explicit OPTIMIZE instead asks for
    /// complete physical coverage and seals the remaining tail in one request.
    pub(crate) fn plan_all(
        &self,
        definition: &SearchIndexDefinition,
        manifest: &LoadedManifest,
        visible_by_id: &BTreeMap<RowsetId, RowsetSharedPtr>,
    ) -> Result<CatchUpPlan> {
        let mut items = Vec::new();
        for entry in &manifest.tail_pending_entries {
            if matches!(entry.mutation, TailMutationKind::Delete) {
                continue;
            }
            let Some(rowset) = visible_by_id.get(&entry.rowset_id) else {
                continue;
            };
            rowset.load()?;
            if !rowset_can_materialize_definition(definition, rowset) {
                continue;
            }
            items.push(CatchUpWorkItem {
                tail_entry: entry.clone(),
                rowset: rowset.clone(),
            });
        }
        Ok(CatchUpPlan { items })
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
