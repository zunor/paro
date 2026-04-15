// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for CREATE TABLE visibility and constraints.

mod common;

use common::{create_table, create_table_from_specs};
use paro_catalog::catalog::Catalog;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{ColumnDefinition, Constraint, CreateTableInfo};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::types::LogicalType;
use paro_storage::table::table_handle::TableColumnSpec;
use paro_storage::transaction::manager::TRANSACTION_ID_START;
use std::sync::Arc;

#[test]
fn test_create_table_then_insert_same_transaction() {
    // Create catalog and initialize
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    // Simulate a transaction
    let transaction_id = TRANSACTION_ID_START + 1;
    let start_time = 100;

    // CREATE TABLE
    let columns = vec![ColumnDefinition {
        name: "a".to_string(),
        logical_type: LogicalType::Integer,
        not_null: false,
        default_value: None,
        comment: None,
    }];
    let storage = Arc::new(create_table(&[LogicalType::Integer]));

    catalog
        .create_table(transaction_id, start_time, "public", "t1", columns, storage)
        .expect("CREATE TABLE should succeed");

    // INSERT - should be able to find the table
    let txn = CatalogSnapshot::writer(transaction_id, start_time);
    let table = catalog.get_table(&txn, "public", "t1");

    assert!(
        table.is_ok(),
        "Table 't1' should be found after CREATE TABLE in the same transaction"
    );

    let table_entry = table.unwrap();
    assert_eq!(table_entry.as_table().unwrap().base.base.name, "t1");
}

#[test]
fn test_create_table_visible_to_later_transaction() {
    // Create catalog and initialize
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    // Transaction 1: CREATE TABLE
    let t1_id = TRANSACTION_ID_START + 1;
    let t1_start = 100;

    let columns = vec![ColumnDefinition {
        name: "a".to_string(),
        logical_type: LogicalType::Integer,
        not_null: false,
        default_value: None,
        comment: None,
    }];
    let storage = Arc::new(create_table(&[LogicalType::Integer]));

    catalog
        .create_table(t1_id, t1_start, "public", "t2", columns, storage)
        .expect("CREATE TABLE should succeed");

    // Transaction 2: Should NOT see the table (created by another active transaction)
    let t2_id = TRANSACTION_ID_START + 2;
    let t2_start = 200;
    let txn2 = CatalogSnapshot::writer(t2_id, t2_start);
    let table2 = catalog.get_table(&txn2, "public", "t2");

    assert!(
        table2.is_err(),
        "Table 't2' should NOT be visible to another active transaction"
    );
}

#[test]
fn test_create_table_visible_after_commit() {
    // Create catalog and initialize
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    let columns = vec![ColumnDefinition {
        name: "a".to_string(),
        logical_type: LogicalType::Integer,
        not_null: false,
        default_value: None,
        comment: None,
    }];
    let storage = Arc::new(create_table(&[LogicalType::Integer]));

    let committed_txn = CatalogSnapshot::permanent_writer(u64::MAX);
    catalog
        .create_table_in_snapshot(&committed_txn, "public", "t3", columns, storage)
        .expect("CREATE TABLE should succeed");

    // Transaction 2: should see a committed/permanent table.
    let t2_id = TRANSACTION_ID_START + 2;
    let t2_start = TRANSACTION_ID_START + 10; // Started after T1's transaction_id
    let txn2 = CatalogSnapshot::writer(t2_id, t2_start);
    let table2 = catalog.get_table(&txn2, "public", "t3");

    assert!(
        table2.is_ok(),
        "Committed table 't3' should be visible to T2"
    );
}

#[test]
fn test_system_transaction_creates_permanent_schema() {
    // Create catalog
    let catalog = ParoCatalog::new("test_db".to_string());

    // Initialize with system transaction
    catalog.initialize(false);

    // Regular transaction should see the default schemas
    let txn = CatalogSnapshot::writer(TRANSACTION_ID_START + 1, 100);
    let schema = catalog.get_schema(&txn, "public");

    assert!(
        schema.is_ok(),
        "Default schema 'public' should be visible to all transactions"
    );
}

#[test]
fn test_create_table_with_constraints_persists_runtime_descriptor_sync() {
    let catalog = ParoCatalog::new("test_db".to_string());
    catalog.initialize(false);

    let transaction_id = TRANSACTION_ID_START + 10;
    let start_time = 500;

    let columns = vec![
        ColumnDefinition {
            name: "id".to_string(),
            logical_type: LogicalType::BigInt,
            not_null: true,
            default_value: None,
            comment: None,
        },
        ColumnDefinition {
            name: "v".to_string(),
            logical_type: LogicalType::Integer,
            not_null: false,
            default_value: None,
            comment: None,
        },
    ];
    let constraints = vec![Constraint::primary_key(vec![0]), Constraint::not_null(0)];
    let info = CreateTableInfo::new(
        "test_db".to_string(),
        "public".to_string(),
        "pk_tbl".to_string(),
        columns,
    )
    .with_constraints(constraints.clone());

    let storage = Arc::new(create_table_from_specs(&[
        TableColumnSpec {
            name: "id".to_string(),
            logical_type: LogicalType::BigInt,
            is_key: true,
            not_null: true,
        },
        TableColumnSpec {
            name: "v".to_string(),
            logical_type: LogicalType::Integer,
            is_key: false,
            not_null: false,
        },
    ]));

    catalog
        .create_table_with_info(transaction_id, start_time, info, Arc::clone(&storage))
        .expect("CREATE TABLE with constraints should succeed");

    let txn = CatalogSnapshot::writer(transaction_id, start_time);
    let table = catalog
        .get_table(&txn, "public", "pk_tbl")
        .expect("table should be visible to current transaction");
    let table = table.as_table().unwrap();

    assert_eq!(table.constraints.len(), constraints.len());
    assert_eq!(
        table
            .constraints
            .iter()
            .find(|c| c.constraint_type == paro_catalog::entry::ConstraintType::PrimaryKey)
            .map(|c| c.columns.clone()),
        Some(vec![0])
    );

    let runtime_descriptor = table.get_storage().unwrap().to_descriptor().unwrap();
    let catalog_descriptor = table.get_storage_descriptor().unwrap().clone();
    assert_eq!(runtime_descriptor, catalog_descriptor);
    assert_eq!(
        catalog_descriptor.keys_type_enum().unwrap(),
        paro_storage::tablet::KeysType::PrimaryKeys
    );
}
