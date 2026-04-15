// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub mod orphan;
pub mod staging;

use crate::tablet::Tablet;

pub fn reconcile_recovery_state(tablet: &Tablet) {
    if let Err(err) = orphan::sweep_orphan_rowsets(tablet) {
        tracing::warn!(
            tablet_id = tablet.tablet_id(),
            error = %err,
            "failed to reconcile orphan compaction outputs during startup"
        );
    }
    staging::sweep_staging_root(tablet.data_dir().join("_compaction"));
}

pub use staging::{cleanup_now, enqueue_cleanup, sweep_staging_root};
