// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

mod common;

use common::{create_table, create_table_from_specs};
use paro_catalog::catalog::Catalog;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{
    CatalogEntryEnum, ColumnDefinition, CreateIndexInfo, IndexCatalogEntry, IndexType,
    LogicalIndex, TableCatalogEntry,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::types::LogicalType;
use paro_storage::table::table_handle::TableColumnSpec;
use paro_storage::transaction::manager::TRANSACTION_ID_START;
use std::sync::Arc;

#[test]
fn catalog_descriptor_roundtrip_test() {
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    let transaction_id = TRANSACTION_ID_START + 100;
    let start_time = TRANSACTION_ID_START + 1_000;

    let columns = vec![
        ColumnDefinition {
            name: "id".to_string(),
            logical_type: LogicalType::BigInt,
            not_null: true,
            default_value: None,
            comment: None,
        },
        ColumnDefinition {
            name: "payload".to_string(),
            logical_type: LogicalType::Varchar,
            not_null: false,
            default_value: None,
            comment: None,
        },
    ];
    let storage = Arc::new(create_table_from_specs(&[
        TableColumnSpec {
            name: "id".to_string(),
            logical_type: LogicalType::BigInt,
            is_key: true,
            not_null: true,
        },
        TableColumnSpec {
            name: "payload".to_string(),
            logical_type: LogicalType::Varchar,
            is_key: false,
            not_null: false,
        },
    ]));
    let expected_descriptor = storage.to_descriptor().unwrap();

    catalog
        .create_table(
            transaction_id,
            start_time,
            "public",
            "descriptor_roundtrip",
            columns,
            storage,
        )
        .unwrap();

    let txn = CatalogSnapshot::writer(transaction_id, start_time);
    let table_entry = catalog
        .get_table(&txn, "public", "descriptor_roundtrip")
        .unwrap();
    let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
        panic!("expected table entry");
    };

    let bytes = table.serialize().unwrap();
    let restored = TableCatalogEntry::deserialize(&bytes, "test_db".to_string(), None).unwrap();

    assert_eq!(
        restored.get_storage_descriptor().unwrap(),
        &expected_descriptor
    );
    assert_eq!(
        restored.get_storage().unwrap().to_descriptor().unwrap(),
        expected_descriptor
    );
}

/// Table descriptor roundtrip with constraints (PK + NOT NULL).
#[test]
fn catalog_descriptor_roundtrip_with_constraints_test() {
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    let txn_id = TRANSACTION_ID_START + 200;
    let start_time = TRANSACTION_ID_START + 2_000;

    let columns = vec![
        ColumnDefinition {
            name: "pk_col".to_string(),
            logical_type: LogicalType::Integer,
            not_null: true,
            default_value: None,
            comment: None,
        },
        ColumnDefinition {
            name: "val".to_string(),
            logical_type: LogicalType::Double,
            not_null: false,
            default_value: None,
            comment: None,
        },
    ];
    let storage = Arc::new(create_table_from_specs(&[
        TableColumnSpec {
            name: "pk_col".to_string(),
            logical_type: LogicalType::Integer,
            is_key: true,
            not_null: true,
        },
        TableColumnSpec {
            name: "val".to_string(),
            logical_type: LogicalType::Double,
            is_key: false,
            not_null: false,
        },
    ]));
    let expected_descriptor = storage.to_descriptor().unwrap();

    catalog
        .create_table(
            txn_id,
            start_time,
            "public",
            "constrained_table",
            columns,
            storage,
        )
        .unwrap();

    let txn = CatalogSnapshot::writer(txn_id, start_time);
    let entry = catalog
        .get_table(&txn, "public", "constrained_table")
        .unwrap();
    let CatalogEntryEnum::Table(table) = entry.as_ref() else {
        panic!("expected table entry");
    };

    let bytes = table.serialize().unwrap();
    let restored = TableCatalogEntry::deserialize(&bytes, "test_db".to_string(), None).unwrap();

    assert_eq!(restored.columns.len(), 2);
    assert_eq!(restored.columns[0].name, "pk_col");
    assert!(restored.columns[0].not_null);
    assert_eq!(restored.columns[1].name, "val");
    assert!(!restored.columns[1].not_null);
    assert_eq!(
        restored.get_storage_descriptor().unwrap(),
        &expected_descriptor
    );
}

/// Index catalog entry serialize/deserialize roundtrip.
#[test]
fn catalog_index_descriptor_roundtrip_test() {
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    let txn_id = TRANSACTION_ID_START + 300;
    let start_time = TRANSACTION_ID_START + 3_000;
    let txn = CatalogSnapshot::writer(txn_id, start_time);

    let columns = vec![ColumnDefinition::new(
        "embedding".to_string(),
        LogicalType::Float,
    )];
    let storage = Arc::new(create_table(&[LogicalType::Float]));
    catalog
        .create_table(
            txn_id,
            start_time,
            "public",
            "vectors",
            columns,
            Arc::clone(&storage),
        )
        .unwrap();

    let table_entry = catalog.get_table(&txn, "public", "vectors").unwrap();
    let CatalogEntryEnum::Table(table) = table_entry.as_ref() else {
        panic!("expected table entry");
    };

    let schema = catalog.get_schema(&txn, "public").unwrap();
    let info = CreateIndexInfo::new(
        "public".to_string(),
        "vectors".to_string(),
        "idx_vectors_hnsw".to_string(),
        vec![LogicalIndex::new(0)],
        vec![LogicalType::Float],
    )
    .with_catalog("test_db".to_string())
    .with_index_type(IndexType::HNSW);
    schema.create_index(&txn, info, table.as_ref()).unwrap();

    let idx_entry = schema
        .get_index(txn_id, start_time, "idx_vectors_hnsw")
        .unwrap();
    let CatalogEntryEnum::Index(index) = idx_entry.as_ref() else {
        panic!("expected index entry");
    };

    let bytes = index.serialize_to_bytes().unwrap();
    let restored = IndexCatalogEntry::deserialize(&bytes, "test_db".to_string()).unwrap();

    assert_eq!(restored.base.base.name, "idx_vectors_hnsw");
    assert_eq!(restored.table_name, "vectors");
    assert_eq!(restored.index_type, IndexType::HNSW);
    assert_eq!(restored.column_ids.len(), 1);
    assert_eq!(restored.column_ids[0].index, 0);
    assert_eq!(restored.column_types, vec![LogicalType::Float]);
}
