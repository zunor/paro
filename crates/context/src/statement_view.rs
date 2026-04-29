// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::{TransactionView, TxnAdmissionState, WriteGuard};
use paro_common::error::{self as paro_error, Result};
use paro_storage::transaction::txn::Transaction;
use paro_transaction::{CommitTs, DerivedLagLease, RetentionRegistry};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct StatementView {
    pub transaction: TransactionView,
    pub active: Option<Arc<Transaction>>,
    pub write_guard: Option<Arc<WriteGuard>>,
    pub admission: Option<Arc<TxnAdmissionState>>,
    pub retention_registry: Option<RetentionRegistry>,
}

impl StatementView {
    pub fn lease_derived_lag_if_needed(
        &self,
        indexed_through_ts: u64,
    ) -> Result<Option<Arc<DerivedLagLease>>> {
        let target_ts = self.transaction.visible_version();
        if target_ts <= indexed_through_ts {
            return Ok(None);
        }
        let Some(registry) = &self.retention_registry else {
            return Ok(None);
        };
        registry
            .lease_derived_lag_range(CommitTs::new(indexed_through_ts), CommitTs::new(target_ts))
            .map(Arc::new)
            .map(Some)
            .map_err(|err| paro_error::internal(format!("failed to lease derived lag: {err}")))
    }
}
