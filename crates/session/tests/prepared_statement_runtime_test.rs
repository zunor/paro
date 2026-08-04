// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_err.rs"]
mod exec_err;
#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use exec_err::exec_err;
use exec_ok::exec_ok;
use instance_persistent::create_persistent_instance;
use paro_context::{StatementCancelReason, StatementTimeoutDriver};
use paro_instance::{DatabaseCloseAction, Instance};
use paro_session::{CollectingSink, Session, StatementCompletion, TestSessionBuilder};
use query_i64_col::query_i64_col;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use unique_test_dir::create_unique_test_dir;

#[derive(Default)]
struct ToggleTimeoutDriver {
    enabled: AtomicBool,
}

impl ToggleTimeoutDriver {
    fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }
}

impl StatementTimeoutDriver for ToggleTimeoutDriver {
    fn arm(
        &self,
        statement_token: &CancellationToken,
        cancel_reason: &Arc<OnceLock<StatementCancelReason>>,
        _timeout_lifetime: &Arc<CancellationToken>,
        _timeout: Duration,
    ) {
        if self.enabled.load(Ordering::SeqCst) {
            let _ = cancel_reason.set(StatementCancelReason::StatementTimeout);
            statement_token.cancel();
        }
    }
}

fn create_test_session_with_timeout_driver(driver: Arc<dyn StatementTimeoutDriver>) -> Session {
    TestSessionBuilder::minimal()
        .with_timeout_driver(driver)
        .build()
}

fn run_async_test_with_large_stack<Fut>(name: &str, future: Fut)
where
    Fut: Future<Output = ()> + Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime");
            runtime.block_on(future);
        })
        .expect("spawn large-stack test thread")
        .join()
        .expect("join large-stack test thread");
}

async fn run_prepare_execute_deallocate_updates_metadata() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "CREATE TABLE prep_t (v INT)").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO prep_t VALUES (1), (2), (3)",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "PREPARE stmt1 AS SELECT v FROM prep_t ORDER BY v",
    )
    .await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Prepare
    );

    exec_ok(&mut session, &mut sink, "EXECUTE stmt1").await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Select { rows: 3 }
    );
    assert_eq!(query_i64_col(&sink, 0), vec![1, 2, 3]);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM pg_catalog.pg_prepared_statements WHERE name = 'stmt1'",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);

    exec_ok(&mut session, &mut sink, "EXECUTE stmt1").await;
    assert_eq!(query_i64_col(&sink, 0), vec![1, 2, 3]);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT generic_plans, custom_plans FROM pg_catalog.pg_prepared_statements WHERE name = 'stmt1'",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![2]);
    assert_eq!(query_i64_col(&sink, 1), vec![0]);

    exec_ok(&mut session, &mut sink, "DEALLOCATE stmt1").await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Deallocate { all: false }
    );

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM pg_catalog.pg_prepared_statements WHERE name = 'stmt1'",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![0]);
}

#[test]
fn prepare_execute_deallocate_updates_metadata() {
    run_async_test_with_large_stack(
        "prepared-statement-metadata",
        run_prepare_execute_deallocate_updates_metadata(),
    );
}

#[tokio::test]
async fn execute_accepts_expressions_and_returns_underlying_completion() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "PREPARE stmt_expr(INT) AS SELECT ? + 1",
    )
    .await;
    exec_ok(&mut session, &mut sink, "EXECUTE stmt_expr(1 + 2)").await;

    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Select { rows: 1 }
    );
    assert_eq!(query_i64_col(&sink, 0), vec![4]);
}

#[tokio::test]
async fn explain_execute_accepts_parameters() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "PREPARE stmt_explain(INT) AS SELECT ? + 1",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "EXPLAIN EXECUTE stmt_explain(1 + 2)",
    )
    .await;

    let result = sink.assert_single_result();
    assert_eq!(result.completion, StatementCompletion::Explain);
    assert!(!result.chunks.is_empty());
}

#[tokio::test]
async fn prepare_rejects_duplicate_statement_name() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "PREPARE stmt1 AS SELECT 1").await;
    let err = exec_err(&mut session, &mut sink, "PREPARE stmt1 AS SELECT 2").await;
    assert!(err.contains("prepared statement \"stmt1\" already exists"));
}

async fn run_cursor_lifecycle_matches_holdability() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "CREATE TABLE cursor_t (v INT)").await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO cursor_t VALUES (1), (2), (3)",
    )
    .await;

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c1 CURSOR FOR SELECT v FROM cursor_t ORDER BY v",
    )
    .await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::DeclareCursor
    );

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM pg_catalog.pg_cursors WHERE name = 'c1'",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);

    exec_ok(&mut session, &mut sink, "FETCH 2 FROM c1").await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Fetch { rows: 2 }
    );
    assert_eq!(query_i64_col(&sink, 0), vec![1, 2]);

    exec_ok(&mut session, &mut sink, "MOVE 1 FROM c1").await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Move { rows: 1 }
    );

    exec_ok(&mut session, &mut sink, "COMMIT").await;
    let err = exec_err(&mut session, &mut sink, "FETCH NEXT FROM c1").await;
    assert!(err.contains("cursor \"c1\" does not exist"));

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c_hold CURSOR WITH HOLD FOR SELECT v FROM cursor_t ORDER BY v",
    )
    .await;
    exec_ok(&mut session, &mut sink, "COMMIT").await;

    exec_ok(&mut session, &mut sink, "FETCH NEXT FROM c_hold").await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);

    exec_ok(&mut session, &mut sink, "FETCH 2 FROM c_hold").await;
    assert_eq!(query_i64_col(&sink, 0), vec![2, 3]);

    exec_ok(&mut session, &mut sink, "CLOSE c_hold").await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM pg_catalog.pg_cursors WHERE name = 'c_hold'",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![0]);
}

#[test]
fn cursor_lifecycle_matches_holdability() {
    run_async_test_with_large_stack(
        "prepared-statement-cursor-lifecycle",
        run_cursor_lifecycle_matches_holdability(),
    );
}

#[tokio::test]
async fn declare_cursor_requires_transaction_block_without_hold() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    let err = exec_err(&mut session, &mut sink, "DECLARE c1 CURSOR FOR SELECT 1").await;
    assert!(err.contains("DECLARE CURSOR can only be used in transaction blocks"));
}

#[tokio::test]
async fn declare_cursor_rejects_duplicate_name() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(&mut session, &mut sink, "DECLARE c1 CURSOR FOR SELECT 1").await;

    let err = exec_err(&mut session, &mut sink, "DECLARE c1 CURSOR FOR SELECT 2").await;
    assert!(err.contains("cursor \"c1\" already exists"));
}

#[tokio::test]
async fn holdable_cursor_is_dropped_on_rollback() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c_hold CURSOR WITH HOLD FOR SELECT 1",
    )
    .await;
    exec_ok(&mut session, &mut sink, "ROLLBACK").await;

    let err = exec_err(&mut session, &mut sink, "FETCH NEXT FROM c_hold").await;
    assert!(err.contains("cursor \"c_hold\" does not exist"));
}

#[tokio::test]
async fn cursor_defaults_to_scroll_but_no_scroll_rejects_backward_fetch() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(&mut session, &mut sink, "BEGIN").await;
    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c_scroll CURSOR FOR SELECT * FROM (VALUES (1), (2), (3)) AS t(v)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "FETCH 2 FROM c_scroll").await;
    exec_ok(&mut session, &mut sink, "FETCH PRIOR FROM c_scroll").await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);

    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c_no_scroll NO SCROLL CURSOR FOR SELECT * FROM (VALUES (1), (2), (3)) AS t(v)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "FETCH 2 FROM c_no_scroll").await;
    let err = exec_err(&mut session, &mut sink, "FETCH PRIOR FROM c_no_scroll").await;
    assert!(err.contains("cursor can only scan forward"));
}

#[tokio::test]
async fn cancelled_declare_cursor_does_not_poison_future_cursor_scope() {
    let driver = Arc::new(ToggleTimeoutDriver::default());
    let mut session = create_test_session_with_timeout_driver(driver.clone());
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE cursor_cancel_t (v INT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO cursor_cancel_t VALUES (1), (2), (3)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "SET statement_timeout = 1").await;

    driver.enable();
    let err = exec_err(
        &mut session,
        &mut sink,
        "DECLARE c_hold CURSOR WITH HOLD FOR SELECT v FROM cursor_cancel_t ORDER BY v",
    )
    .await;
    driver.disable();
    assert!(err.contains("canceling statement due to statement timeout"));

    let err = exec_err(&mut session, &mut sink, "FETCH NEXT FROM c_hold").await;
    assert!(err.contains("cursor \"c_hold\" does not exist"));

    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c_hold CURSOR WITH HOLD FOR SELECT v FROM cursor_cancel_t ORDER BY v",
    )
    .await;
    exec_ok(&mut session, &mut sink, "FETCH NEXT FROM c_hold").await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);
}

#[tokio::test]
async fn cancelled_fetch_keeps_cursor_cleanup_paths_usable() {
    let driver = Arc::new(ToggleTimeoutDriver::default());
    let mut session = create_test_session_with_timeout_driver(driver.clone());
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE cursor_fetch_cancel_t (v INT)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO cursor_fetch_cancel_t VALUES (1), (2), (3)",
    )
    .await;
    exec_ok(&mut session, &mut sink, "SET statement_timeout = 1").await;
    exec_ok(
        &mut session,
        &mut sink,
        "DECLARE c_hold CURSOR WITH HOLD FOR SELECT v FROM cursor_fetch_cancel_t ORDER BY v",
    )
    .await;
    exec_ok(&mut session, &mut sink, "PREPARE stmt_cleanup AS SELECT 1").await;

    driver.enable();
    let err = exec_err(&mut session, &mut sink, "FETCH NEXT FROM c_hold").await;
    driver.disable();
    assert!(err.contains("canceling statement due to statement timeout"));

    exec_ok(&mut session, &mut sink, "CLOSE c_hold").await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::CloseCursor { all: false }
    );

    exec_ok(&mut session, &mut sink, "DEALLOCATE stmt_cleanup").await;
    assert_eq!(
        sink.assert_single_result().completion,
        StatementCompletion::Deallocate { all: false }
    );
}

async fn run_prepared_metadata_catalog_views_survive_restart() {
    let base_dir = create_unique_test_dir("prepared_statement_runtime", "catalog_views");

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(&mut session, &mut sink, "PREPARE stmt_restart AS SELECT 42").await;
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT COUNT(*) FROM pg_catalog.pg_prepared_statements WHERE name = 'stmt_restart'",
        )
        .await;
        assert_eq!(query_i64_col(&sink, 0), vec![1]);

        exec_ok(&mut session, &mut sink, "BEGIN").await;
        exec_ok(
            &mut session,
            &mut sink,
            "DECLARE c_restart CURSOR FOR SELECT 1",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT COUNT(*) FROM pg_catalog.pg_cursors WHERE name = 'c_restart'",
        )
        .await;
        assert_eq!(query_i64_col(&sink, 0), vec![1]);

        instance
            .database_registry()
            .get_database("postgres")
            .expect("default database")
            .close(DatabaseCloseAction::Checkpoint)
            .expect("checkpoint close should persist catalog state");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));
        let mut sink = CollectingSink::new();

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT COUNT(*) FROM pg_catalog.pg_prepared_statements WHERE name = 'stmt_restart'",
        )
        .await;
        assert_eq!(query_i64_col(&sink, 0), vec![0]);

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT COUNT(*) FROM pg_catalog.pg_cursors WHERE name = 'c_restart'",
        )
        .await;
        assert_eq!(query_i64_col(&sink, 0), vec![0]);
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[test]
fn prepared_metadata_catalog_views_survive_restart() {
    run_async_test_with_large_stack(
        "prepared-statement-catalog-views-restart",
        run_prepared_metadata_catalog_views_survive_restart(),
    );
}

#[tokio::test]
async fn prepared_statement_recompiles_when_current_database_changes() {
    let instance = Instance::new_in_memory();
    instance
        .create_database("db2")
        .expect("failed to create secondary database");

    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();
    let default_database = session.current_database.name().to_string();

    exec_ok(&mut session, &mut sink, "CREATE TABLE lookup_t (v INT)").await;
    exec_ok(&mut session, &mut sink, "INSERT INTO lookup_t VALUES (1)").await;

    session
        .set_current_database("db2")
        .expect("failed to switch to secondary database");
    exec_ok(&mut session, &mut sink, "CREATE TABLE lookup_t (v INT)").await;
    exec_ok(&mut session, &mut sink, "INSERT INTO lookup_t VALUES (2)").await;

    session
        .set_current_database(&default_database)
        .expect("failed to switch back to default database");
    exec_ok(
        &mut session,
        &mut sink,
        "PREPARE stmt_lookup AS SELECT v FROM lookup_t",
    )
    .await;
    exec_ok(&mut session, &mut sink, "EXECUTE stmt_lookup").await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);

    session
        .set_current_database("db2")
        .expect("failed to switch to secondary database");
    exec_ok(&mut session, &mut sink, "EXECUTE stmt_lookup").await;
    assert_eq!(query_i64_col(&sink, 0), vec![2]);
}
