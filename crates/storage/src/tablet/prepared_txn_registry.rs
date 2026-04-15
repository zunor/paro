// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use std::collections::HashSet;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub(crate) struct PreparedTxnRegistry {
    txns: RwLock<HashSet<u64>>,
}

impl PreparedTxnRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn prepare(&self, tablet_id: u64, txn_id: u64) -> Result<()> {
        let mut prepared = self.txns.write().unwrap();
        if !prepared.insert(txn_id) {
            return Err(paro_error::invalid_input(format!(
                "txn {} already prepared for tablet {}",
                txn_id, tablet_id
            )));
        }
        Ok(())
    }

    pub(crate) fn finish(&self, txn_id: u64) {
        let mut prepared = self.txns.write().unwrap();
        prepared.remove(&txn_id);
    }
}
