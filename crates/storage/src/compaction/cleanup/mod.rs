// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub mod orphan;
pub mod staging;

use crate::tablet::Tablet;

pub fn reconcile_recovery_state(tablet: &Tablet) {
    // Canonical rowset directories can belong to committed records after the
    // checkpoint cut and before the replay frontier. Removing them during
    // tablet init races journal-tail recovery, so startup cleanup is limited to
    // transient compaction staging workspaces.
    staging::sweep_staging_root(tablet.compaction_staging_dir());
}

pub use staging::{cleanup_now, enqueue_cleanup, sweep_staging_root};
