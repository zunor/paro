// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use std::sync::Arc;

use exec_ok::exec_ok;
use instance_persistent::create_persistent_instance;
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::types::LogicalType;
use paro_session::{CollectingSink, Session};
use unique_test_dir::create_unique_test_dir;

fn routine_overloads(
    session: &Session,
    schema_name: &str,
    routine_name: &str,
) -> Vec<paro_catalog::entry::StoredRoutineOverload> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = session
        .current_database
        .catalog()
        .get_schema(&txn, schema_name)
        .expect("schema should exist");
    let entry = schema
        .get_routine(txn.transaction_id, txn.start_time, routine_name)
        .expect("routine should exist");
    entry
        .as_routine()
        .expect("entry should be a routine")
        .overloads()
        .to_vec()
}

fn maybe_routine_overloads(
    session: &Session,
    schema_name: &str,
    routine_name: &str,
) -> Option<Vec<paro_catalog::entry::StoredRoutineOverload>> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = session
        .current_database
        .catalog()
        .get_schema(&txn, schema_name)
        .expect("schema should exist");
    schema
        .get_routine(txn.transaction_id, txn.start_time, routine_name)
        .map(|entry| {
            entry
                .as_routine()
                .expect("entry should be a routine")
                .overloads()
                .to_vec()
        })
}

#[tokio::test]
async fn routine_ddl_is_transactional_and_persistent() {
    let base_dir = create_unique_test_dir("routine_ddl_runtime", "transactional");
    let instance = create_persistent_instance(&base_dir);
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    session
        .begin_explicit_transaction()
        .expect("BEGIN should succeed");
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE FUNCTION public.py_add(a INTEGER, b INTEGER) RETURNS INTEGER \
         LANGUAGE python IMMUTABLE STRICT AS $$return a + b$$",
    )
    .await;
    session
        .rollback_transaction()
        .expect("ROLLBACK should succeed");
    assert!(
        maybe_routine_overloads(&session, "public", "py_add").is_none(),
        "rolled back CREATE FUNCTION must not remain visible"
    );

    session
        .begin_explicit_transaction()
        .expect("BEGIN should succeed");
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE FUNCTION \"PUBLIC\".py_add(a INTEGER, b INTEGER) RETURNS INTEGER \
         LANGUAGE python IMMUTABLE STRICT AS $$return a + b$$",
    )
    .await;
    session.commit_transaction().expect("COMMIT should succeed");

    let overloads = routine_overloads(&session, "public", "py_add");
    assert_eq!(overloads.len(), 1);
    assert_eq!(overloads[0].spec.schema, "public");
    assert_eq!(overloads[0].spec.owner.principal, "paro");
    let original_routine_id = overloads[0].spec.identity.id;
    assert_eq!(overloads[0].spec.identity.generation, 1);
    assert_eq!(
        overloads[0].spec.signature().argument_types,
        vec![LogicalType::Integer, LogicalType::Integer]
    );

    session
        .begin_explicit_transaction()
        .expect("BEGIN should succeed");
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE OR REPLACE FUNCTION public.py_add(a INTEGER, b INTEGER) RETURNS INTEGER \
         LANGUAGE python VOLATILE AS $$return a - b$$",
    )
    .await;
    session.commit_transaction().expect("COMMIT should succeed");

    let overloads = routine_overloads(&session, "public", "py_add");
    assert_eq!(overloads.len(), 1);
    assert_eq!(overloads[0].spec.identity.id, original_routine_id);
    assert_eq!(overloads[0].spec.identity.generation, 2);
    assert!(overloads[0].sql.contains("CREATE OR REPLACE FUNCTION"));
    assert!(overloads[0].sql.contains("return a - b"));

    session
        .begin_explicit_transaction()
        .expect("BEGIN should succeed");
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE FUNCTION public.py_add(a BIGINT, b BIGINT) RETURNS BIGINT \
         LANGUAGE python AS $$return a + b$$",
    )
    .await;
    session.commit_transaction().expect("COMMIT should succeed");

    let overloads = routine_overloads(&session, "public", "py_add");
    assert_eq!(overloads.len(), 2);
    assert!(overloads.iter().any(|overload| {
        overload.spec.signature().argument_types == vec![LogicalType::Integer, LogicalType::Integer]
    }));
    assert!(overloads.iter().any(|overload| {
        overload.spec.signature().argument_types == vec![LogicalType::BigInt, LogicalType::BigInt]
    }));
    assert!(
        overloads
            .iter()
            .all(|overload| overload.spec.schema == "public"),
        "all overloads in one family must use the owning schema's canonical name"
    );
    assert_eq!(
        overloads
            .iter()
            .find(|overload| {
                overload.spec.signature().argument_types
                    == vec![LogicalType::Integer, LogicalType::Integer]
            })
            .expect("integer overload should exist")
            .spec
            .identity
            .id,
        original_routine_id
    );
    assert_ne!(
        overloads
            .iter()
            .find(|overload| {
                overload.spec.signature().argument_types
                    == vec![LogicalType::BigInt, LogicalType::BigInt]
            })
            .expect("bigint overload should exist")
            .spec
            .identity
            .id,
        original_routine_id
    );

    session
        .begin_explicit_transaction()
        .expect("BEGIN should succeed");
    exec_ok(
        &mut session,
        &mut sink,
        "DROP FUNCTION public.py_add(BIGINT, BIGINT)",
    )
    .await;
    session.commit_transaction().expect("COMMIT should succeed");

    let overloads = routine_overloads(&session, "public", "py_add");
    assert_eq!(overloads.len(), 1);
    assert_eq!(
        overloads[0].spec.signature().argument_types,
        vec![LogicalType::Integer, LogicalType::Integer]
    );
    assert_eq!(overloads[0].spec.identity.generation, 2);
    assert_eq!(overloads[0].spec.schema, "public");

    drop(session);
    drop(instance);

    let restarted = create_persistent_instance(&base_dir);
    let restarted_session = Session::new(2, Arc::clone(&restarted));
    let overloads = routine_overloads(&restarted_session, "public", "py_add");
    assert_eq!(overloads.len(), 1);
    assert_eq!(overloads[0].spec.identity.id, original_routine_id);
    assert_eq!(overloads[0].spec.identity.generation, 2);
    assert_eq!(
        overloads[0].spec.signature().argument_types,
        vec![LogicalType::Integer, LogicalType::Integer]
    );
}
