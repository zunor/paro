//! Helpers for tests that need a minimal flushed catalog DDL transaction envelope.
#![doc(hidden)]

use crate::wal::wal_entry::WalEntry;
use crate::wal::wal_type::WalType;
use crate::wal::wal_writer::WalWriter;
use paro_common::ddl::{
    CreateSchemaPayload, DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind,
};
use paro_common::effect::CatalogTxnOp;
use paro_common::error::Result;

/// Writes `TxnBegin` → `TxnCatalogOp` (create schema) → `TxnCommit`, then one `WalFlush` via [`WalWriter::flush`].
pub fn write_flushed_create_schema_txn(
    writer: &WalWriter,
    catalog_name: &str,
    schema_name: &str,
    txn_id: u64,
    commit_id: u64,
) -> Result<()> {
    write_flushed_create_schema_txn_with_object_id(
        writer,
        catalog_name,
        schema_name,
        0,
        txn_id,
        commit_id,
    )
}

/// Same as [`write_flushed_create_schema_txn`] but allows the caller to control
/// the catalog object identity stored in WAL.
pub fn write_flushed_create_schema_txn_with_object_id(
    writer: &WalWriter,
    catalog_name: &str,
    schema_name: &str,
    object_id: u64,
    txn_id: u64,
    commit_id: u64,
) -> Result<()> {
    let begin = WalEntry::TxnBegin {
        txn_id,
        start_time: 0,
    };
    writer.write_entry(WalType::TxnBegin, &begin.serialize_data())?;
    let op = CatalogTxnOp {
        change: DdlChangeRecord {
            key: DdlObjectKey::new(
                catalog_name,
                None::<String>,
                schema_name,
                DdlObjectKind::Schema,
            ),
            change: DdlChange::CreateSchema(CreateSchemaPayload {
                object_id,
                if_not_exists: false,
            }),
        },
        staged_artifacts: vec![],
        runtime_transitions: vec![],
        cleanups: vec![],
    };
    let cat = WalEntry::TxnCatalogOp { seq: 0, op };
    writer.write_entry(WalType::TxnCatalogOp, &cat.serialize_data())?;
    let commit = WalEntry::TxnCommit { txn_id, commit_id };
    writer.write_entry(WalType::TxnCommit, &commit.serialize_data())?;
    writer.flush()
}

/// Convenience wrapper that opens a writer at `path` with the supplied header metadata
/// and writes a flushed `CREATE SCHEMA` transaction.
pub fn write_flushed_create_schema_txn_at_path(
    path: &std::path::Path,
    metadata: crate::wal::wal_entry::WalHeaderMetadata,
    catalog_name: &str,
    schema_name: &str,
    txn_id: u64,
    commit_id: u64,
) -> Result<()> {
    let writer = WalWriter::with_header_metadata(
        path,
        crate::wal::wal_writer::WalInitState::Uninitialized,
        metadata,
    );
    write_flushed_create_schema_txn(&writer, catalog_name, schema_name, txn_id, commit_id)
}

/// Same as [`write_flushed_create_schema_txn`] but stops before `TxnCommit` / `WalFlush` (open txn tail).
pub fn append_open_create_schema_txn(
    writer: &WalWriter,
    catalog_name: &str,
    schema_name: &str,
    txn_id: u64,
) -> Result<()> {
    let begin = WalEntry::TxnBegin {
        txn_id,
        start_time: 0,
    };
    writer.write_entry(WalType::TxnBegin, &begin.serialize_data())?;
    let op = CatalogTxnOp {
        change: DdlChangeRecord {
            key: DdlObjectKey::new(
                catalog_name,
                None::<String>,
                schema_name,
                DdlObjectKind::Schema,
            ),
            change: DdlChange::CreateSchema(CreateSchemaPayload {
                object_id: 0,
                if_not_exists: false,
            }),
        },
        staged_artifacts: vec![],
        runtime_transitions: vec![],
        cleanups: vec![],
    };
    let cat = WalEntry::TxnCatalogOp { seq: 0, op };
    writer.write_entry(WalType::TxnCatalogOp, &cat.serialize_data())?;
    Ok(())
}

/// `RowsetCommit` followed by a single [`WalWriter::flush`] (WalFlush boundary).
pub fn write_flushed_rowset_commit(
    writer: &WalWriter,
    tablet_id: u64,
    rowset_id: u64,
    start_version: i64,
    end_version: i64,
    rowset_path: &str,
) -> Result<()> {
    writer.write_rowset_commit(
        tablet_id,
        rowset_id,
        start_version,
        end_version,
        rowset_path,
    )?;
    writer.flush()
}
