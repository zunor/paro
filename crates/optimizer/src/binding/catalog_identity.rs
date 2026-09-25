// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Process-local validity of append-only import namespaces, never plan identity.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogVersion(u64);

/// Append preserves existing bindings. Fork or rollback does not: a fork can
/// assign the same next ordinal differently, and rollback may reuse an ordinal.
/// Such boundaries get a fresh token, without rewriting/importing old nodes.
#[derive(Debug)]
pub(super) struct CatalogIdentity(CatalogVersion);

impl Default for CatalogIdentity {
    fn default() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let value = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("catalog identity space exhausted");
        Self(CatalogVersion(value))
    }
}

impl Clone for CatalogIdentity {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl CatalogIdentity {
    pub(super) fn version(&self) -> CatalogVersion {
        self.0
    }
    pub(super) fn invalidate(&mut self) {
        *self = Self::default();
    }
}
