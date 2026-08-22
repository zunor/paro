// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use paro_common::error::{self as paro_error, ParoError};
use paro_session::{
    PreparedStatementEntry, PreparedStatementSource, Session, SessionContextState,
    TestSessionBuilder,
};

fn create_test_session() -> Session {
    TestSessionBuilder::minimal().build()
}

fn parse_single(sql: &str) -> paro_parser::ast::Statement {
    paro_parser::parse_one(sql).unwrap().stmt
}

#[tokio::test]
async fn test_reset_session_state_preserves_active_statement_scope() {
    let mut session = create_test_session();

    session
        .state
        .add_prepared_statement(PreparedStatementEntry {
            name: "stmt1".to_string(),
            source_sql: Arc::from("SELECT 1"),
            raw_stmt: Arc::new(parse_single("SELECT 1")),
            parameter_types: Vec::new(),
            result_schema: Vec::new(),
            generic_plan: None,
            generic_plan_uses: 0,
            source: PreparedStatementSource::Sql,
        });
    session.state.enable_profiler();
    session
        .run_in_statement_scope("DISCARD ALL", async |session| {
            session.cancel_active_statement();
            session.reset_session_state();

            assert!(session.has_active_query());
            assert!(session.check_active_statement_cancellation().is_err());
            Ok(())
        })
        .await
        .unwrap();

    assert!(session.state.prepared.statements().next().is_none());
    assert!(!session.state.is_profiling_enabled());
    assert!(!session.connection_shutdown_requested());
    assert!(!session.has_active_query());
    assert!(session.check_active_statement_cancellation().is_ok());
}

#[tokio::test]
async fn test_active_query_context_lifecycle() {
    let mut session = create_test_session();

    assert!(!session.has_active_query());
    assert!(session.get_current_query().is_none());
    assert!(session.active_query().is_none());

    session
        .run_in_statement_scope("SELECT 1", async |session| {
            assert!(session.has_active_query());
            assert_eq!(session.get_current_query(), Some("SELECT 1"));
            assert!(session.active_query().is_some());
            Ok(())
        })
        .await
        .unwrap();

    assert!(!session.has_active_query());
    assert!(session.get_current_query().is_none());
}

#[tokio::test]
async fn test_active_query_context_failure() {
    let mut session = create_test_session();

    let error = session
        .run_in_statement_scope("SELECT * FROM nonexistent", async |session| {
            assert!(session.has_active_query());
            Err::<(), _>(paro_error::internal("planned statement failure"))
        })
        .await
        .expect_err("operation error should be returned");

    assert!(error.to_string().contains("planned statement failure"));
    assert!(!session.has_active_query());
    assert!(session.current_statement_cancellation().is_none());
}

#[tokio::test]
async fn test_query_progress_tracking() {
    let mut session = create_test_session();

    session
        .run_in_statement_scope("SELECT * FROM large_table", async |session| {
            let progress = session.get_query_progress().unwrap();
            assert_eq!(progress.percentage, 0.0);
            assert_eq!(progress.rows_processed, 0);

            session.update_query_progress(50, 100).unwrap();
            let progress = session.get_query_progress().unwrap();
            assert_eq!(progress.percentage, 50.0);
            assert_eq!(progress.rows_processed, 50);
            assert_eq!(progress.total_rows_to_process, 100);
            Ok(())
        })
        .await
        .unwrap();

    assert!(session.get_query_progress().is_none());
    assert!(session.update_query_progress(1, 1).is_err());
}

#[tokio::test]
async fn test_statement_cancellation_is_scoped_to_active_statement() {
    let mut session = create_test_session();

    session
        .run_in_statement_scope("SELECT * FROM large_table", async |session| {
            assert!(session.check_active_statement_cancellation().is_ok());

            session.cancel_active_statement();
            assert!(session.check_active_statement_cancellation().is_err());
            Ok(())
        })
        .await
        .unwrap();

    session
        .run_in_statement_scope("SELECT 1", async |session| {
            assert!(session.check_active_statement_cancellation().is_ok());
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_elapsed_time() {
    let mut session = create_test_session();

    assert!(session.query_elapsed().is_none());

    session
        .run_in_statement_scope("SELECT 1", async |session| {
            std::thread::sleep(std::time::Duration::from_millis(10));
            let elapsed = session.query_elapsed();
            assert!(elapsed.is_some());
            assert!(elapsed.unwrap().as_millis() >= 10);
            Ok(())
        })
        .await
        .unwrap();

    assert!(session.query_elapsed().is_none());
}

#[tokio::test]
async fn test_active_query_mut_access() {
    let mut session = create_test_session();

    session
        .run_in_statement_scope("SELECT 1", async |session| {
            let ctx = session.active_query_mut();
            assert!(ctx.is_some());

            let ctx = ctx.unwrap();
            ctx.set_open_result(42);
            assert!(ctx.is_open_result(42));
            Ok(())
        })
        .await
        .unwrap();
}

#[derive(Debug, Default)]
struct TestState {
    #[allow(dead_code)]
    value: AtomicU32,
}

impl SessionContextState for TestState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_registered_state_basic() {
    let session = create_test_session();

    assert_eq!(session.state_count(), 0);

    session.register_state("test", TestState::default());
    assert_eq!(session.state_count(), 1);

    let state = session.get_state("test");
    assert!(state.is_some());

    assert!(session.has_state::<TestState>("test"));
}

#[derive(Debug, Default)]
struct CounterState {
    count: AtomicU32,
}

impl SessionContextState for CounterState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_registered_state_get_or_create() {
    let session = create_test_session();

    let state1 = session.get_or_create_state::<CounterState>("counter");
    assert_eq!(session.state_count(), 1);

    {
        let mut guard = state1.lock().unwrap();
        if let Some(counter) = guard.as_any_mut().downcast_mut::<CounterState>() {
            counter.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    let state2 = session.get_or_create_state::<CounterState>("counter");
    assert_eq!(session.state_count(), 1);

    {
        let guard = state2.lock().unwrap();
        if let Some(counter) = guard.as_any().downcast_ref::<CounterState>() {
            assert_eq!(counter.count.load(Ordering::Relaxed), 1);
        }
    }
}

#[derive(Debug, Default)]
struct DummyState;

impl SessionContextState for DummyState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_registered_state_remove() {
    let session = create_test_session();

    session.register_state("dummy", DummyState);
    assert_eq!(session.state_count(), 1);

    assert!(session.remove_state("dummy"));
    assert_eq!(session.state_count(), 0);

    assert!(!session.remove_state("nonexistent"));
}

#[derive(Debug, Default)]
struct LifecycleState {
    query_begin_count: AtomicU32,
    query_end_count: AtomicU32,
    query_error_count: AtomicU32,
}

impl SessionContextState for LifecycleState {
    fn query_begin(&mut self) {
        self.query_begin_count.fetch_add(1, Ordering::Relaxed);
    }

    fn query_end(&mut self, error: Option<&ParoError>) {
        self.query_end_count.fetch_add(1, Ordering::Relaxed);
        if error.is_some() {
            self.query_error_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[tokio::test]
async fn test_registered_state_query_lifecycle_notifications() {
    let mut session = create_test_session();

    session.register_state("lifecycle", LifecycleState::default());

    session
        .run_in_statement_scope("SELECT 1", async |_| Ok(()))
        .await
        .unwrap();
    session
        .run_in_statement_scope("SELECT broken", async |_| {
            Err::<(), _>(paro_error::internal("planned lifecycle failure"))
        })
        .await
        .expect_err("operation error should reach lifecycle callbacks");

    let state = session.get_state("lifecycle").unwrap();
    let guard = state.lock().unwrap();
    let lifecycle = guard.as_any().downcast_ref::<LifecycleState>().unwrap();
    assert_eq!(lifecycle.query_begin_count.load(Ordering::Relaxed), 2);
    assert_eq!(lifecycle.query_end_count.load(Ordering::Relaxed), 2);
    assert_eq!(lifecycle.query_error_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dropped_statement_future_aborts_scope_and_allows_reuse() {
    let mut session = create_test_session();
    session.register_state("lifecycle", LifecycleState::default());

    let mut execution = Box::pin(session.run_in_statement_scope("SELECT pending", async |_| {
        std::future::pending::<paro_common::error::Result<()>>().await
    }));
    let timed_out =
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut execution).await;
    assert!(timed_out.is_err());
    drop(execution);

    assert!(!session.has_active_query());
    assert!(session.current_statement_cancellation().is_none());
    assert!(session.get_query_progress().is_none());

    let state = session.get_state("lifecycle").unwrap();
    {
        let guard = state.lock().unwrap();
        let lifecycle = guard.as_any().downcast_ref::<LifecycleState>().unwrap();
        assert_eq!(lifecycle.query_begin_count.load(Ordering::Relaxed), 1);
        assert_eq!(lifecycle.query_end_count.load(Ordering::Relaxed), 1);
        assert_eq!(lifecycle.query_error_count.load(Ordering::Relaxed), 1);
    }

    session
        .run_in_statement_scope("SELECT reusable", async |_| Ok(()))
        .await
        .unwrap();
}

async fn panic_statement_operation(_: &mut Session) -> paro_common::error::Result<()> {
    panic!("planned statement panic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_statement_future_clears_shared_execution_control() {
    let mut session = create_test_session();
    let execution_control = session.execution_control().clone();

    let task = tokio::spawn(async move {
        let _ = session
            .run_in_statement_scope("SELECT panic", panic_statement_operation)
            .await;
    });
    let panic = task.await.expect_err("statement operation should panic");

    assert!(panic.is_panic());
    assert!(execution_control.active_statement().is_none());
}

#[derive(Debug)]
struct PanickingQueryBeginState;

impl SessionContextState for PanickingQueryBeginState {
    fn query_begin(&mut self) {
        panic!("planned query-begin panic");
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_query_begin_callback_clears_shared_execution_control() {
    let mut session = create_test_session();
    session.register_state("panicking", PanickingQueryBeginState);
    let execution_control = session.execution_control().clone();

    let task = tokio::spawn(async move {
        let _ = session
            .run_in_statement_scope("SELECT panic", async |_| Ok(()))
            .await;
    });
    let panic = task.await.expect_err("query-begin callback should panic");

    assert!(panic.is_panic());
    assert!(execution_control.active_statement().is_none());
}

#[derive(Debug, Default)]
struct TxnState {
    begin_count: AtomicU32,
    commit_count: AtomicU32,
    rollback_count: AtomicU32,
}

impl SessionContextState for TxnState {
    fn transaction_begin(&mut self) {
        self.begin_count.fetch_add(1, Ordering::Relaxed);
    }

    fn transaction_commit(&mut self) {
        self.commit_count.fetch_add(1, Ordering::Relaxed);
    }

    fn transaction_rollback(&mut self, _error: Option<&ParoError>) {
        self.rollback_count.fetch_add(1, Ordering::Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_registered_state_transaction_lifecycle_notifications() {
    let mut session = create_test_session();

    session.register_state("txn", TxnState::default());

    session.begin_explicit_transaction().unwrap();
    session.commit_transaction().unwrap();

    let state = session.get_state("txn").unwrap();
    let guard = state.lock().unwrap();
    let txn = guard.as_any().downcast_ref::<TxnState>().unwrap();
    assert_eq!(txn.begin_count.load(Ordering::Relaxed), 1);
    assert_eq!(txn.commit_count.load(Ordering::Relaxed), 1);
    assert_eq!(txn.rollback_count.load(Ordering::Relaxed), 0);
}

#[test]
fn test_registered_state_implicit_transaction_commit_notifications() {
    let mut session = create_test_session();

    session.register_state("txn", TxnState::default());

    session.begin_implicit_transaction_block().unwrap();
    session.begin_implicit_transaction_block().unwrap();
    session.end_implicit_transaction_block().unwrap();

    let state = session.get_state("txn").unwrap();
    let guard = state.lock().unwrap();
    let txn = guard.as_any().downcast_ref::<TxnState>().unwrap();
    assert_eq!(txn.begin_count.load(Ordering::Relaxed), 1);
    assert_eq!(txn.commit_count.load(Ordering::Relaxed), 1);
    assert_eq!(txn.rollback_count.load(Ordering::Relaxed), 0);
}

#[test]
fn test_registered_state_implicit_transaction_rollback_notifications() {
    let mut session = create_test_session();

    session.register_state("txn", TxnState::default());

    session.begin_implicit_transaction_block().unwrap();
    session.rollback_implicit_transaction().unwrap();

    let state = session.get_state("txn").unwrap();
    let guard = state.lock().unwrap();
    let txn = guard.as_any().downcast_ref::<TxnState>().unwrap();
    assert_eq!(txn.begin_count.load(Ordering::Relaxed), 1);
    assert_eq!(txn.commit_count.load(Ordering::Relaxed), 0);
    assert_eq!(txn.rollback_count.load(Ordering::Relaxed), 1);
}
