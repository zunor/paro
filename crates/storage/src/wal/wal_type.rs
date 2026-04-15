//! # WAL Entry Types
//!
//! On-disk type bytes for the supported WAL protocol. Historical per-DDL catalog opcodes
//! (bytes 1-24) and row-oriented tuple payloads (26-29) are rejected at parse time;
//! catalog changes use the unified `Txn*` journal records only.

/// WAL entry type enumeration (type byte on disk).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WalType {
    /// Invalid/uninitialized entry
    Invalid = 0,

    /// Set current table context (tablet / database WAL paths that still use USE_TABLE)
    UseTable = 25,

    /// Rowset commit for versioned rowset lifecycle
    RowsetCommit = 30,

    /// Primary key delete (by serialized key bytes)
    PrimaryDelete = 31,

    /// Row-id delete (by explicit row locations)
    RowIdDelete = 32,

    /// Compaction publish intent (replace inputs with one output)
    CompactionPublish = 33,

    /// Unified transaction begin marker
    TxnBegin = 43,
    /// Unified transaction catalog op
    TxnCatalogOp = 44,
    /// Unified transaction data op
    TxnDataOp = 45,
    /// Unified transaction post-commit hook
    TxnPostCommitHook = 46,
    /// Unified transaction commit marker
    TxnCommit = 47,
    /// Unified transaction abort marker
    TxnAbort = 48,

    /// WAL version header
    WalVersion = 98,
    /// Checkpoint marker
    Checkpoint = 99,
    /// Flush marker (sync point)
    WalFlush = 100,
}

fn legacy_catalog_opcode(value: u8) -> bool {
    matches!(
        value,
        1..=6 | 8..=24 // skips 7 (unused on wire)
    )
}

fn unsupported_historical_wal_opcode(value: u8) -> paro_common::error::ParoError {
    paro_common::error::not_supported(format!(
        "unsupported historical WAL opcode {}; requires a clean data directory or a fresh checkpoint written by unified Txn* journal",
        value
    ))
}

impl WalType {
    /// Typed catalog mutations appear only as `TxnCatalogOp` inside a txn envelope.
    #[inline]
    pub fn is_catalog_operation(&self) -> bool {
        matches!(self, WalType::TxnCatalogOp)
    }

    #[inline]
    pub fn is_data_operation(&self) -> bool {
        matches!(
            self,
            WalType::UseTable
                | WalType::RowsetCommit
                | WalType::PrimaryDelete
                | WalType::RowIdDelete
                | WalType::CompactionPublish
                | WalType::TxnDataOp
        )
    }

    #[inline]
    pub fn is_control_operation(&self) -> bool {
        matches!(
            self,
            WalType::WalVersion
                | WalType::Checkpoint
                | WalType::WalFlush
                | WalType::TxnBegin
                | WalType::TxnCatalogOp
                | WalType::TxnDataOp
                | WalType::TxnPostCommitHook
                | WalType::TxnCommit
                | WalType::TxnAbort
        )
    }
}

impl TryFrom<u8> for WalType {
    type Error = paro_common::error::ParoError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use paro_common::error as paro_error;
        if legacy_catalog_opcode(value) {
            return Err(unsupported_historical_wal_opcode(value));
        }
        match value {
            0 => Ok(WalType::Invalid),
            25 => Ok(WalType::UseTable),
            26..=29 => Err(unsupported_historical_wal_opcode(value)),
            30 => Ok(WalType::RowsetCommit),
            31 => Ok(WalType::PrimaryDelete),
            32 => Ok(WalType::RowIdDelete),
            33 => Ok(WalType::CompactionPublish),
            43 => Ok(WalType::TxnBegin),
            44 => Ok(WalType::TxnCatalogOp),
            45 => Ok(WalType::TxnDataOp),
            46 => Ok(WalType::TxnPostCommitHook),
            47 => Ok(WalType::TxnCommit),
            48 => Ok(WalType::TxnAbort),
            98 => Ok(WalType::WalVersion),
            99 => Ok(WalType::Checkpoint),
            100 => Ok(WalType::WalFlush),
            _ => Err(paro_error::serialization_error(format!(
                "Invalid WAL type: {}",
                value
            ))),
        }
    }
}

impl From<WalType> for u8 {
    fn from(val: WalType) -> Self {
        val as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_type_conversion() {
        let types = [
            WalType::Invalid,
            WalType::UseTable,
            WalType::RowsetCommit,
            WalType::CompactionPublish,
            WalType::RowIdDelete,
            WalType::TxnBegin,
            WalType::TxnCommit,
            WalType::Checkpoint,
            WalType::WalFlush,
        ];

        for wal_type in types {
            let byte: u8 = wal_type.into();
            let recovered = WalType::try_from(byte).unwrap();
            assert_eq!(wal_type, recovered);
        }
    }

    #[test]
    fn test_wal_type_categories() {
        assert!(WalType::TxnCatalogOp.is_catalog_operation());
        assert!(!WalType::RowIdDelete.is_catalog_operation());
        assert!(WalType::TxnBegin.is_control_operation());
        assert!(WalType::TxnCommit.is_control_operation());

        assert!(WalType::RowIdDelete.is_data_operation());
        assert!(WalType::CompactionPublish.is_data_operation());
        assert!(WalType::TxnDataOp.is_data_operation());
        assert!(!WalType::TxnCatalogOp.is_data_operation());

        assert!(WalType::Checkpoint.is_control_operation());
        assert!(WalType::WalFlush.is_control_operation());
        assert!(!WalType::UseTable.is_control_operation());
    }

    #[test]
    fn test_invalid_wal_type() {
        let result = WalType::try_from(255u8);
        assert!(result.is_err());
    }

    #[test]
    fn test_legacy_row_group_type_is_rejected() {
        let result = WalType::try_from(29u8);
        let err = result.expect_err("legacy type 29 should be rejected");
        assert!(err.is_feature_not_supported());
        assert!(err
            .to_string()
            .contains("unsupported historical WAL opcode 29"));
    }

    #[test]
    fn test_legacy_per_ddl_catalog_rejected() {
        let err = WalType::try_from(3u8).expect_err("CreateSchema byte");
        assert!(err.is_feature_not_supported());
        assert!(err
            .to_string()
            .contains("unsupported historical WAL opcode 3"));
    }

    #[test]
    fn test_legacy_tuple_dml_rejected() {
        for b in [26u8, 27, 28] {
            let err = WalType::try_from(b).expect_err("tuple DML");
            assert!(err.is_feature_not_supported());
            assert!(err
                .to_string()
                .contains(&format!("unsupported historical WAL opcode {}", b)));
        }
    }
}
