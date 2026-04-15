use crate::wal::wal_type::WalType;
use paro_common::effect::{CatalogTxnOp, PostCommitHookDescriptor, PreparedDataOp};
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnRecord {
    Begin {
        txn_id: u64,
        start_time: u64,
    },
    CatalogOp {
        seq: u32,
        op: CatalogTxnOp,
    },
    DataOp {
        seq: u32,
        op: PreparedDataOp,
    },
    PostCommitHook {
        seq: u32,
        hook: PostCommitHookDescriptor,
    },
    Commit {
        txn_id: u64,
        commit_id: u64,
    },
    Abort {
        txn_id: u64,
    },
}

impl TxnRecord {
    pub fn wal_type(&self) -> WalType {
        match self {
            TxnRecord::Begin { .. } => WalType::TxnBegin,
            TxnRecord::CatalogOp { .. } => WalType::TxnCatalogOp,
            TxnRecord::DataOp { .. } => WalType::TxnDataOp,
            TxnRecord::PostCommitHook { .. } => WalType::TxnPostCommitHook,
            TxnRecord::Commit { .. } => WalType::TxnCommit,
            TxnRecord::Abort { .. } => WalType::TxnAbort,
        }
    }

    pub fn serialize_data(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|err| paro_error::serialization_error(format!("txn encode: {err}")))
    }

    pub fn deserialize_data(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|err| paro_error::serialization_error(format!("txn decode: {err}")))
    }
}
