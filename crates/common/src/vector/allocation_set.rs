// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

/// Tracks already-accounted physical allocations so shared buffers are counted once.
#[derive(Debug, Default, Clone)]
pub struct AllocationSet {
    seen: HashSet<usize>,
}

impl AllocationSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, identity: usize, size: usize) -> usize {
        if size == 0 || !self.seen.insert(identity) {
            0
        } else {
            size
        }
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}
