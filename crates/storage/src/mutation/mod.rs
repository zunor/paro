// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod deleter;
pub(crate) mod updater;
pub(crate) mod upsert;
pub(crate) mod writer;

use std::sync::Arc;

use crate::transaction::txn::Transaction;

#[derive(Debug, Clone)]
pub(crate) enum MutationTarget {
    Transaction(Arc<Transaction>),
    Direct,
}
