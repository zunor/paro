// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::rowset::RowsetId;
use crate::tablet::ColumnId;

use super::super::capability::{SearchArtifactRef, SearchIndexDefinition};
use super::super::manifest::LoadedManifest;
use super::super::tail::{TailEntryId, TailMutationKind, TailPendingEntry, TailRowImageRef};

pub(crate) fn tail_entry_already_live(
    entries: &[TailPendingEntry],
    candidate: &TailPendingEntry,
) -> bool {
    entries
        .iter()
        .any(|entry| tail_entry_logical_key(entry) == tail_entry_logical_key(candidate))
}

pub(crate) fn tail_entry_is_covered_by_artifacts(
    definition: &SearchIndexDefinition,
    entry: &TailPendingEntry,
    current_artifact_keys: &BTreeSet<(RowsetId, u32, ColumnId)>,
    snapshot_artifact_keys: &BTreeSet<(RowsetId, u32, ColumnId)>,
) -> bool {
    entry.segment_ids.iter().all(|segment_id| {
        definition.column_ids.iter().all(|column_id| {
            let key = (entry.rowset_id, *segment_id, *column_id);
            current_artifact_keys.contains(&key) || snapshot_artifact_keys.contains(&key)
        })
    })
}

pub(crate) fn artifact_segment_column_keys<'a>(
    artifacts: impl IntoIterator<Item = &'a SearchArtifactRef>,
) -> BTreeSet<(RowsetId, u32, ColumnId)> {
    artifacts
        .into_iter()
        .flat_map(|artifact| {
            artifact.coverage.segments().iter().map(|span| {
                (
                    span.segment.rowset_id,
                    span.segment.segment_id,
                    artifact.column_id,
                )
            })
        })
        .collect()
}

pub(crate) fn assign_tail_entry_ids_for_full_snapshot(
    entries: &mut [TailPendingEntry],
    current_manifest: Option<&LoadedManifest>,
) -> TailEntryId {
    let mut reusable_ids = BTreeMap::new();
    let mut next_id = 1;
    if let Some(manifest) = current_manifest {
        next_id = manifest
            .next_tail_entry_id
            .0
            .max(next_tail_entry_id(&manifest.tail_pending_entries));
        for entry in &manifest.tail_pending_entries {
            if entry.entry_id.is_assigned() {
                reusable_ids.insert(tail_entry_logical_key(entry), entry.entry_id);
            }
        }
    }
    for entry in entries.iter_mut() {
        if let Some(entry_id) = reusable_ids.get(&tail_entry_logical_key(entry)) {
            entry.entry_id = *entry_id;
        }
    }
    assign_tail_entry_ids(entries, &mut next_id);
    TailEntryId(next_id)
}

pub(crate) fn assign_tail_entry_ids(entries: &mut [TailPendingEntry], next_id: &mut u64) {
    for entry in entries {
        if entry.entry_id.is_assigned() {
            continue;
        }
        entry.entry_id = TailEntryId(*next_id);
        *next_id = next_id.saturating_add(1);
    }
}

fn tail_entry_logical_key(
    entry: &TailPendingEntry,
) -> (
    RowsetId,
    Vec<u32>,
    TailMutationKind,
    Option<TailRowImageRef>,
) {
    (
        entry.rowset_id,
        entry.segment_ids.clone(),
        entry.mutation,
        entry.row_image_ref.clone(),
    )
}

fn next_tail_entry_id(entries: &[TailPendingEntry]) -> u64 {
    entries
        .iter()
        .filter(|entry| entry.entry_id.is_assigned())
        .map(|entry| entry.entry_id.0)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}
