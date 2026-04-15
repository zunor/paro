// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

use paro_common::error::{self as paro_error, Result};

use crate::row::RowStore;

/// Bookkeeping guard for rows pinned through the safe row API.
#[derive(Debug)]
pub struct PinSet<'a> {
    store: Option<&'a RowStore>,
    prefix_state: Option<&'a PrefixReleaseState>,
}

impl<'a> PinSet<'a> {
    pub(crate) fn none() -> Self {
        Self {
            store: None,
            prefix_state: None,
        }
    }

    pub(crate) fn prefix(store: &'a RowStore, prefix_state: &'a PrefixReleaseState) -> Self {
        prefix_state.acquire_pin();
        Self {
            store: Some(store),
            prefix_state: Some(prefix_state),
        }
    }
}

impl Drop for PinSet<'_> {
    fn drop(&mut self) {
        if let (Some(store), Some(state)) = (self.store, self.prefix_state) {
            state.release_pin(store);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PrefixReleaseState {
    logical_release_frontier: AtomicU64,
    logical_scan_chunk_frontier: AtomicU32,
    physical_release_frontier: AtomicU64,
    released_scan_chunk_prefix: AtomicU32,
    outstanding_pins: AtomicUsize,
}

impl PrefixReleaseState {
    pub(crate) fn acquire_pin(&self) {
        self.outstanding_pins.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn release_pin(&self, store: &RowStore) {
        if self.outstanding_pins.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.try_release_prefix(store);
        }
    }

    pub(crate) fn advance_release_frontier(&self, store: &RowStore, frontier: u64) -> Result<()> {
        if frontier > store.count() {
            return Err(paro_error::internal(format!(
                "release frontier {} exceeds row count {}",
                frontier,
                store.count()
            )));
        }

        let current = self.logical_release_frontier.load(Ordering::Acquire);
        if frontier < current {
            return Err(paro_error::internal(format!(
                "release frontier cannot move backwards: current={}, requested={}",
                current, frontier
            )));
        }

        self.logical_release_frontier
            .store(frontier, Ordering::Release);
        self.logical_scan_chunk_frontier.store(
            store.scan_chunk_prefix_for_ordinal_frontier(frontier),
            Ordering::Release,
        );
        self.try_release_prefix(store);
        Ok(())
    }

    pub(crate) fn physical_release_frontier(&self) -> u64 {
        self.physical_release_frontier.load(Ordering::Acquire)
    }

    pub(crate) fn logical_release_frontier(&self) -> u64 {
        self.logical_release_frontier.load(Ordering::Acquire)
    }

    pub(crate) fn outstanding_pins(&self) -> usize {
        self.outstanding_pins.load(Ordering::Acquire)
    }

    fn try_release_prefix(&self, store: &RowStore) {
        if self.outstanding_pins.load(Ordering::Acquire) != 0 {
            return;
        }

        let target = self.logical_scan_chunk_frontier.load(Ordering::Acquire);
        let current = self.released_scan_chunk_prefix.load(Ordering::Acquire);
        if target <= current {
            self.physical_release_frontier.store(
                store.ordinal_frontier_for_scan_chunk_prefix(current),
                Ordering::Release,
            );
            return;
        }

        store.release_scan_chunk_prefix(current, target);
        self.released_scan_chunk_prefix
            .store(target, Ordering::Release);
        self.physical_release_frontier.store(
            store.ordinal_frontier_for_scan_chunk_prefix(target),
            Ordering::Release,
        );
    }
}
