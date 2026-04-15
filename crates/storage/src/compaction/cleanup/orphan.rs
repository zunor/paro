// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::cleanup::staging::enqueue_cleanup;
use crate::tablet::Tablet;
use paro_common::error::{self as paro_error, Result};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

pub fn sweep_orphan_rowsets(tablet: &Tablet) -> Result<()> {
    let rowsets_root = tablet.data_dir().join("rowsets");
    if !rowsets_root.exists() {
        return Ok(());
    }

    let active_rowsets = tablet.capture_consistent_rowsets(tablet.max_version())?;
    let active_paths: HashSet<PathBuf> = active_rowsets
        .into_iter()
        .map(|rowset| rowset.rowset_path().to_path_buf())
        .collect();

    for entry in fs::read_dir(&rowsets_root).map_err(|err| {
        paro_error::io_error(format!(
            "scan rowset root {} for orphan cleanup: {}",
            rowsets_root.display(),
            err
        ))
    })? {
        let entry = entry.map_err(|err| {
            paro_error::io_error(format!(
                "read rowset root entry {}: {}",
                rowsets_root.display(),
                err
            ))
        })?;
        let path = entry.path();
        if !path.is_dir() || active_paths.contains(&path) {
            continue;
        }

        if let Err(err) = fs::remove_dir_all(&path) {
            tracing::warn!(
                tablet_id = tablet.tablet_id(),
                path = %path.display(),
                error = %err,
                "failed to remove orphan compaction output during startup; deferring cleanup"
            );
            enqueue_cleanup(path);
        }
    }

    Ok(())
}
