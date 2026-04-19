// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Helpers for tests that need a minimal flushed catalog journal commit.
#![doc(hidden)]

use crate::wal::wal_entry::WalEntry;
use crate::wal::wal_writer::WalWriter;
use paro_common::ddl::{
    CreateSchemaPayload, DdlChange, DdlChangeRecord, DdlObjectKey, DdlObjectKind,
};
use paro_common::effect::CatalogTxnOp;
use paro_common::error::Result;
use paro_common::journal::{CommitRecord, JournalRecord};

/// Writes one committed `CREATE SCHEMA` journal record, then one `WalFlush`.
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

/// Same as [`write_flushed_create_schema_txn`] but lets tests pick the journal
/// logical LSN independently from the durable commit id.
pub fn write_flushed_create_schema_txn_with_lsn(
    writer: &WalWriter,
    catalog_name: &str,
    schema_name: &str,
    txn_id: u64,
    commit_id: u64,
    lsn: u64,
) -> Result<()> {
    write_flushed_create_schema_txn_with_lsn_and_object_id(
        writer,
        catalog_name,
        schema_name,
        0,
        txn_id,
        commit_id,
        lsn,
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
    write_flushed_create_schema_txn_with_lsn_and_object_id(
        writer,
        catalog_name,
        schema_name,
        object_id,
        txn_id,
        commit_id,
        txn_id,
    )
}

/// Same as [`write_flushed_create_schema_txn_with_object_id`] but lets tests
/// choose the journal logical LSN explicitly.
pub fn write_flushed_create_schema_txn_with_lsn_and_object_id(
    writer: &WalWriter,
    catalog_name: &str,
    schema_name: &str,
    object_id: u64,
    txn_id: u64,
    commit_id: u64,
    lsn: u64,
) -> Result<()> {
    let record = CommitRecord {
        txn_id,
        start_time: 0,
        commit_id,
        catalog_ops: vec![CatalogTxnOp {
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
        }],
        storage_ops: vec![],
        apply_descriptors: vec![],
        deferred_tasks: vec![],
    };
    // Most tests use txn_id as the journal-frame logical LSN. The durable
    // commit visibility still comes from CommitRecord::commit_id.
    let entry = WalEntry::JournalRecord {
        lsn,
        record: JournalRecord::Commit(record),
    };
    writer.write_entry(entry.wal_type(), &entry.serialize_data())?;
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

/// Same as [`write_flushed_create_schema_txn`] but stops before `WalFlush` (open tail).
pub fn append_open_create_schema_txn(
    writer: &WalWriter,
    catalog_name: &str,
    schema_name: &str,
    txn_id: u64,
) -> Result<()> {
    let record = CommitRecord {
        txn_id,
        start_time: 0,
        commit_id: txn_id,
        catalog_ops: vec![CatalogTxnOp {
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
        }],
        storage_ops: vec![],
        apply_descriptors: vec![],
        deferred_tasks: vec![],
    };
    let entry = WalEntry::JournalRecord {
        lsn: txn_id,
        record: JournalRecord::Commit(record),
    };
    writer.write_entry(entry.wal_type(), &entry.serialize_data())
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
