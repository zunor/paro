// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::entry::CatalogEntryEnum;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Internal version-chain node for a catalog key.
///
/// The payload entry is optional so tombstones do not require cloning arbitrary
/// catalog entry structs just to represent a delete version.
#[derive(Debug)]
pub(crate) struct VersionedEntry {
    pub(crate) entry: Option<Arc<CatalogEntryEnum>>,
    timestamp: AtomicU64,
    deleted: AtomicBool,
    child: Option<Arc<VersionedEntry>>,
}

impl VersionedEntry {
    pub(crate) fn new(
        entry: Option<Arc<CatalogEntryEnum>>,
        timestamp: u64,
        deleted: bool,
        child: Option<Arc<VersionedEntry>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            entry,
            timestamp: AtomicU64::new(timestamp),
            deleted: AtomicBool::new(deleted),
            child,
        })
    }

    pub(crate) fn timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::SeqCst)
    }

    pub(crate) fn set_timestamp(&self, ts: u64) {
        self.timestamp.store(ts, Ordering::SeqCst);
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::SeqCst)
    }

    pub(crate) fn child(&self) -> Option<Arc<VersionedEntry>> {
        self.child.clone()
    }
}
