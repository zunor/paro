// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Tablet-local rowset-segment identifier manager.

use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;

/// Stable identifier for a segment within a tablet.
pub type Rssid = u32;

/// Persisted rssid mapping entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RssidMappingEntry {
    pub rssid: Rssid,
    pub rowset_id: u64,
    pub segment_id: u32,
}

/// Maintains the rssid <-> (rowset_id, segment_id) mapping for a tablet.
#[derive(Debug, Default)]
pub struct RssidManager {
    next_rssid: AtomicU32,
    mapping: RwLock<HashMap<Rssid, (u64, u32)>>,
    reverse_mapping: RwLock<HashMap<(u64, u32), Rssid>>,
}

impl RssidManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild a manager from persisted entries.
    pub fn from_entries(entries: &[RssidMappingEntry]) -> Self {
        let mut mapping = HashMap::with_capacity(entries.len());
        let mut reverse_mapping = HashMap::with_capacity(entries.len());
        let mut next_rssid = 0u32;
        for entry in entries {
            mapping.insert(entry.rssid, (entry.rowset_id, entry.segment_id));
            reverse_mapping.insert((entry.rowset_id, entry.segment_id), entry.rssid);
            next_rssid = next_rssid.max(entry.rssid.saturating_add(1));
        }
        Self {
            next_rssid: AtomicU32::new(next_rssid),
            mapping: RwLock::new(mapping),
            reverse_mapping: RwLock::new(reverse_mapping),
        }
    }

    /// Allocate a fresh rssid for `(rowset_id, segment_id)`.
    pub fn allocate(&self, rowset_id: u64, segment_id: u32) -> Rssid {
        if let Some(existing) = self.rssid_for(rowset_id, segment_id) {
            return existing;
        }

        let rssid = self.next_rssid.fetch_add(1, Ordering::SeqCst);
        self.mapping
            .write()
            .unwrap()
            .insert(rssid, (rowset_id, segment_id));
        self.reverse_mapping
            .write()
            .unwrap()
            .insert((rowset_id, segment_id), rssid);
        rssid
    }

    /// Register an existing mapping.
    pub fn register_existing(&self, rssid: Rssid, rowset_id: u64, segment_id: u32) -> Result<()> {
        let mut guard = self.mapping.write().unwrap();
        let mut reverse_guard = self.reverse_mapping.write().unwrap();
        if let Some(existing) = guard.get(&rssid) {
            if *existing != (rowset_id, segment_id) {
                return Err(paro_error::invalid_input(format!(
                    "rssid {} already mapped to ({}, {}), cannot remap to ({}, {})",
                    rssid, existing.0, existing.1, rowset_id, segment_id
                )));
            }
        } else {
            guard.insert(rssid, (rowset_id, segment_id));
        }

        if let Some(existing) = reverse_guard.get(&(rowset_id, segment_id)) {
            if *existing != rssid {
                return Err(paro_error::invalid_input(format!(
                    "rowset/segment ({}, {}) already mapped to rssid {}, cannot remap to {}",
                    rowset_id, segment_id, existing, rssid
                )));
            }
        } else {
            reverse_guard.insert((rowset_id, segment_id), rssid);
        }

        self.next_rssid
            .fetch_max(rssid.saturating_add(1), Ordering::SeqCst);
        Ok(())
    }

    /// Resolve an rssid to `(rowset_id, segment_id)`.
    pub fn resolve(&self, rssid: Rssid) -> Option<(u64, u32)> {
        self.mapping.read().unwrap().get(&rssid).copied()
    }

    /// Resolve a `(rowset_id, segment_id)` pair back to its rssid.
    pub fn rssid_for(&self, rowset_id: u64, segment_id: u32) -> Option<Rssid> {
        self.reverse_mapping
            .read()
            .unwrap()
            .get(&(rowset_id, segment_id))
            .copied()
    }

    /// Snapshot the persisted entries in rssid order.
    pub fn snapshot_entries(&self) -> Vec<RssidMappingEntry> {
        let mut entries: Vec<_> = self
            .mapping
            .read()
            .unwrap()
            .iter()
            .map(|(&rssid, &(rowset_id, segment_id))| RssidMappingEntry {
                rssid,
                rowset_id,
                segment_id,
            })
            .collect();
        entries.sort_by_key(|entry| entry.rssid);
        entries
    }

    /// Next rssid that will be allocated.
    pub fn next_rssid(&self) -> Rssid {
        self.next_rssid.load(Ordering::SeqCst)
    }

    pub fn len(&self) -> usize {
        self.mapping.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{RssidManager, RssidMappingEntry};

    #[test]
    fn allocates_and_resolves_rssids() {
        let manager = RssidManager::new();
        let a = manager.allocate(10, 0);
        let b = manager.allocate(10, 1);

        assert_eq!(a, 0);
        assert_eq!(b, 1);
        assert_eq!(manager.resolve(a), Some((10, 0)));
        assert_eq!(manager.resolve(b), Some((10, 1)));
        assert_eq!(manager.rssid_for(10, 0), Some(a));
        assert_eq!(manager.rssid_for(10, 1), Some(b));
        assert_eq!(manager.next_rssid(), 2);
    }

    #[test]
    fn rebuilds_from_snapshot() {
        let manager = RssidManager::from_entries(&[
            RssidMappingEntry {
                rssid: 3,
                rowset_id: 7,
                segment_id: 0,
            },
            RssidMappingEntry {
                rssid: 5,
                rowset_id: 8,
                segment_id: 2,
            },
        ]);

        assert_eq!(manager.resolve(3), Some((7, 0)));
        assert_eq!(manager.resolve(5), Some((8, 2)));
        assert_eq!(manager.rssid_for(7, 0), Some(3));
        assert_eq!(manager.rssid_for(8, 2), Some(5));
        assert_eq!(manager.next_rssid(), 6);
    }

    #[test]
    fn register_existing_rejects_conflicting_mapping() {
        let manager = RssidManager::new();
        manager.register_existing(9, 100, 1).unwrap();
        let err = manager.register_existing(9, 101, 1).unwrap_err();
        assert!(format!("{}", err).contains("already mapped"));
    }

    #[test]
    fn allocate_is_idempotent_for_existing_pair() {
        let manager = RssidManager::new();
        let first = manager.allocate(55, 3);
        let second = manager.allocate(55, 3);
        assert_eq!(first, second);
        assert_eq!(manager.len(), 1);
        assert_eq!(manager.next_rssid(), 1);
    }
}
