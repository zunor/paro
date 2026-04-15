use crate::{TxnAdmissionState, WriteGuard};
use paro_storage::transaction::txn::Transaction;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct StatementView {
    pub id: u64,
    pub start_time: u64,
    pub visible_version: u64,
    pub active: Option<Arc<Transaction>>,
    pub write_guard: Option<Arc<WriteGuard>>,
    pub admission: Option<Arc<TxnAdmissionState>>,
}
