// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Default)]
pub struct ApplyFrontier {
    next_lsn: u64,
    ready: BTreeSet<u64>,
}

impl ApplyFrontier {
    pub fn bootstrap(&mut self, max_lsn: u64) {
        self.next_lsn = max_lsn.saturating_add(1).max(1);
        self.ready.clear();
    }

    pub fn mark_ready(&mut self, lsn: u64) -> Vec<u64> {
        if self.next_lsn == 0 {
            self.next_lsn = 1;
        }
        self.ready.insert(lsn);

        let mut advanced = Vec::new();
        while self.ready.remove(&self.next_lsn) {
            advanced.push(self.next_lsn);
            self.next_lsn += 1;
        }
        advanced
    }
}

#[derive(Debug, Default)]
pub struct PublishFrontier {
    next_lsn: u64,
    ready: BTreeMap<u64, Option<u64>>,
}

impl PublishFrontier {
    pub fn bootstrap(&mut self, max_lsn: u64) {
        self.next_lsn = max_lsn.saturating_add(1).max(1);
        self.ready.clear();
    }

    pub fn mark_ready(&mut self, lsn: u64, commit_id: Option<u64>) -> Vec<(u64, Option<u64>)> {
        if self.next_lsn == 0 {
            self.next_lsn = 1;
        }
        self.ready.insert(lsn, commit_id);

        let mut advanced = Vec::new();
        while let Some(commit_id) = self.ready.remove(&self.next_lsn) {
            advanced.push((self.next_lsn, commit_id));
            self.next_lsn += 1;
        }
        advanced
    }
}
