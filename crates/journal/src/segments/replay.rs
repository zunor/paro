// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::catalog::{SegmentCatalogStore, SegmentLayout};
use paro_common::error::{self as paro_error, Result};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayCursorEntry {
    pub segment_id: u64,
    pub path: PathBuf,
    pub starting_lsn: u64,
    pub replay_from_lsn: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayCursor {
    entries: Vec<ReplayCursorEntry>,
}

impl ReplayCursor {
    pub fn from_catalog(
        store: &SegmentCatalogStore,
        replay_from_segment_id: u64,
        replay_from_lsn: u64,
    ) -> Result<Self> {
        let Some(catalog) = store.load()? else {
            return Ok(Self::default());
        };

        let mut segments = catalog.segments.clone();
        segments.sort_by_key(|segment| segment.segment_id);

        let replay_segment_id = if replay_from_segment_id == 0 {
            catalog
                .segment_for_replay_lsn(replay_from_lsn)
                .map(|segment| segment.segment_id)
                .unwrap_or(catalog.active_segment_id)
        } else {
            replay_from_segment_id
        };

        let layout: &SegmentLayout = store.layout();
        let mut entries = Vec::new();
        let mut seen_replay_start = false;
        for segment in segments {
            if segment.segment_id < replay_segment_id {
                continue;
            }
            let starting_lsn = segment.start_lsn;
            let entry_replay_from_lsn = if !seen_replay_start {
                seen_replay_start = true;
                replay_from_lsn.max(starting_lsn)
            } else {
                starting_lsn
            };
            let segment_path = layout.segment_path(segment.segment_id);
            entries.push(ReplayCursorEntry {
                segment_id: segment.segment_id,
                path: segment_path,
                starting_lsn,
                replay_from_lsn: entry_replay_from_lsn,
            });
        }

        if replay_segment_id != 0 && !seen_replay_start {
            return Err(paro_error::invalid_input(format!(
                "segment replay cursor start {} not found in catalog {}",
                replay_segment_id,
                layout.catalog_path().display()
            )));
        }

        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[ReplayCursorEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
