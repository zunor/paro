// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::runtime_value::Value;
use paro_instance::Instance;
use paro_session::{CollectingSink, Session};
use std::sync::{Arc, Mutex, Weak};

type ObservedOwner = Arc<Mutex<Option<Weak<dyn paro_common::vector::VectorLifetimeOwner>>>>;
struct TransportSink {
    blocked: bool,
    owner: ObservedOwner,
}

#[async_trait::async_trait]
impl paro_session::ResultSink for TransportSink {
    async fn start_result(
        &mut self,
        _: &[String],
        _: &[paro_common::types::LogicalType],
    ) -> paro_common::error::Result<()> {
        Ok(())
    }
    async fn push_chunk(
        &mut self,
        _: &paro_common::chunk::Chunk,
    ) -> paro_common::error::Result<()> {
        panic!("diagnostic must use owned transport")
    }
    async fn push_diagnostic_chunk(
        &mut self,
        _: &paro_common::chunk::Chunk,
        owner: Arc<dyn paro_common::vector::VectorLifetimeOwner>,
    ) -> paro_common::error::Result<()> {
        *self.owner.lock().unwrap() = Some(Arc::downgrade(&owner));
        if self.blocked {
            std::future::pending::<()>().await;
        }
        drop(owner);
        Err(paro_common::error::internal(
            "injected diagnostic writer failure",
        ))
    }
    async fn finish_result(
        &mut self,
        _: &paro_session::StatementCompletion,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }
}
impl paro_session::ProtocolResultSink for TransportSink {}

async fn document(session: &mut Session, sql: &str) -> String {
    let mut sink = CollectingSink::new();
    session.execute_simple_query(sql, &mut sink).await.unwrap();
    let result = sink.assert_single_result();
    assert_eq!(result.names, ["QUERY PLAN"]);
    assert_eq!(result.chunks.iter().map(|c| c.len()).sum::<usize>(), 1);
    match result.chunks[0].column(0).unwrap().get_value(0) {
        Value::Varchar(s) => s,
        value => panic!("unexpected diagnostic value {value:?}"),
    }
}

#[test]
fn compile_query_cte_and_reject_unimplemented_options() {
    std::thread::Builder::new().stack_size(32 << 20).spawn(|| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let mut session = Session::new(1, Instance::new_in_memory());
            for sql in ["EXPLAIN (COMPILE, FORMAT JSON) SELECT 42 AS answer", "explain (format json, compile) WITH t AS (SELECT 3 AS x) SELECT x FROM t"] {
                let text = document(&mut session, sql).await;
                let record: serde_json::Value = serde_json::from_str(&text).unwrap();
                paro_execution::explain::compile_render::validate_json(text.as_bytes()).unwrap();
                assert_eq!(record["cache"], "ForcedCompile");
                assert_eq!(record["outcome"], "Success");
                assert_eq!(record["artifact"], "CompiledArtifactReady");
                assert_eq!(record["admission"], "NotExecuted");
                assert_eq!(record["execution"], "NotExecuted");
                assert_eq!(record["output_columns"]["Observed"], 1);
                let phases = ["bind_ns", "optimizer_ns", "verify_ns", "finish_ns", "compiler_other_ns"];
                assert_eq!(phases.iter().map(|k| record[*k]["Observed"].as_u64().unwrap()).sum::<u64>(), record["compiler_ns"]["Observed"].as_u64().unwrap());
                assert!(!text.contains("answer"));
            }
            assert!(document(&mut session, "EXPLAIN (COMPILE) SELECT 1").await.starts_with("EXPLAIN (COMPILE)"));
            let analyzed = document(
                &mut session,
                "EXPLAIN (COMPILE, ANALYZE, FORMAT JSON) SELECT 1",
            )
            .await;
            let analyzed_record: serde_json::Value = serde_json::from_str(&analyzed).unwrap();
            let analyzed_receipt = analyzed_record["execution_receipt"].as_object().unwrap();
            assert_eq!(analyzed_receipt["admission"], "Selected");
            assert_eq!(analyzed_receipt["image"], "Ready");
            assert_eq!(analyzed_receipt["terminal"], "Completed");
            session
                .execute_simple_query("SELECT 7", &mut CollectingSink::new())
                .await
                .unwrap();
            session
                .execute_simple_query("SELECT 7", &mut CollectingSink::new())
                .await
                .unwrap();
            let mut receipt_sink = CollectingSink::new();
            session
                .execute_simple_query(
                    "SELECT name, record_type, record_id, payload_json FROM paro_optimizers()",
                    &mut receipt_sink,
                )
                .await
                .unwrap();
            let receipt_result = receipt_sink.assert_single_result();
            let mut receipt_names = Vec::new();
            let mut receipt_types = Vec::new();
            let mut receipt_ids = Vec::new();
            let mut receipt_payloads = Vec::new();
            for chunk in &receipt_result.chunks {
                for row in 0..chunk.len() {
                    if let Value::Varchar(name) = chunk.column(0).unwrap().get_value(row) {
                        receipt_names.push(name);
                    }
                    if let Value::Varchar(record_type) = chunk.column(1).unwrap().get_value(row) {
                        receipt_types.push(record_type);
                    }
                    if let Value::BigInt(record_id) = chunk.column(2).unwrap().get_value(row) {
                        receipt_ids.push(record_id);
                    }
                    if let Value::Varchar(payload) = chunk.column(3).unwrap().get_value(row) {
                        receipt_payloads.push(payload);
                    }
                }
            }
            assert!(
                receipt_types
                    .iter()
                    .any(|record_type| record_type == "statement_cache"),
                "normal SELECT did not publish a typed statement decision: {receipt_names:?}"
            );
            assert!(
                receipt_types
                    .iter()
                    .any(|record_type| record_type == "execution_receipt"),
                "normal SELECT did not publish a typed execution receipt: {receipt_names:?}"
            );
            assert!(
                receipt_ids
                    .iter()
                    .any(|id| *id >= 0),
                "typed receipt ids were not exported: {receipt_names:?}"
            );
            let detail = document(&mut session, "EXPLAIN (COMPILE, DETAIL, FORMAT JSON) SELECT 1").await;
            let detail_record: serde_json::Value = serde_json::from_str(&detail).unwrap();
            assert_eq!(detail_record["capture_level"], "Detail");
            let detail_events = detail_record["detail"].as_array().unwrap();
            assert!(!detail_events.is_empty());
            assert!(detail_events.iter().any(|event| {
                event["kind"]
                    == paro_context::compile_diagnostics::detail_kind::PROPOSAL
            }));
            assert!(receipt_payloads.iter().any(|payload| payload.contains("schema_version")));
            for sql in ["EXPLAIN (COMPILE) CREATE TABLE forbidden (x INT)", "EXPLAIN (COMPILE) SELECT 1 FORMAT JSON", "EXPLAIN (COMPILE) EXPLAIN SELECT 1"] {
                let mut sink = CollectingSink::new();
                assert!(session.execute_simple_query(sql, &mut sink).await.is_err(), "{sql}");
            }
            // Error identity must survive request-level observation.
            let mut sink = CollectingSink::new();
            let ordinary = session.execute_simple_query("SELECT missing_compile_column", &mut sink).await.unwrap_err();
            let observed = session.execute_simple_query("EXPLAIN (COMPILE) SELECT missing_compile_column", &mut sink).await.unwrap_err();
            assert_eq!(ordinary.sqlstate(), observed.sqlstate());
            assert_eq!(ordinary.to_string(), observed.to_string());
            session.execute_simple_query("CREATE TABLE compile_no_execution (v VARCHAR)", &mut CollectingSink::new()).await.unwrap();
            session.execute_simple_query("INSERT INTO compile_no_execution VALUES ('not-an-integer')", &mut CollectingSink::new()).await.unwrap();
            let _ = document(&mut session, "EXPLAIN (COMPILE, FORMAT JSON) SELECT CAST(v AS INTEGER) FROM compile_no_execution").await;
            let mut sink = CollectingSink::new();
            assert!(session.execute_simple_query("SELECT CAST(v AS INTEGER) FROM compile_no_execution", &mut sink).await.is_err());
            // Wide output hashes identities but never copies names/literals to trace.
            let wide = (0..600).map(|i| format!("{i} AS c{i}")).collect::<Vec<_>>().join(",");
            let text = document(&mut session, &format!("EXPLAIN (COMPILE, FORMAT JSON) SELECT {wide}")).await;
            let record = paro_execution::explain::compile_render::validate_json(text.as_bytes()).unwrap();
            assert_eq!(record.summary().unwrap().output_columns, paro_context::compile_diagnostics::Observation::Observed(600));
            assert!(text.len() < paro_context::compile_diagnostics::ENCODED_LIMIT);
            // Deep supported expressions exercise the real binder without a trace tree.
            let deep = (0..48).fold("1".to_owned(), |sql, _| format!("CASE WHEN TRUE THEN ({sql}) ELSE 0 END"));
            let text = document(&mut session, &format!("EXPLAIN (COMPILE, FORMAT JSON) SELECT {deep}")).await;
            paro_execution::explain::compile_render::validate_json(text.as_bytes()).unwrap();
            // Collector keeps its diagnostic reservation after the call returns.
            let mut retained = Vec::new();
            for _ in 0..paro_context::compile_diagnostics::MAX_CAPTURES {
                let mut sink = CollectingSink::new();
                session.execute_simple_query("EXPLAIN (COMPILE, FORMAT JSON) SELECT 7", &mut sink).await.unwrap();
                retained.push(sink);
            }
            let capacity = session.execute_simple_query("EXPLAIN (COMPILE, FORMAT JSON) SELECT 7", &mut CollectingSink::new()).await.unwrap_err();
            assert!(capacity.to_string().contains("compile diagnostic capacity unavailable"));
            // Even at capacity, the compiler's original error takes precedence.
            let binding = session.execute_simple_query("EXPLAIN (COMPILE) SELECT missing_compile_column", &mut CollectingSink::new()).await.unwrap_err();
            assert_eq!(binding.sqlstate(), ordinary.sqlstate());
            assert_eq!(binding.to_string(), ordinary.to_string());
            drop(retained);
            assert!(!document(&mut session, "EXPLAIN (COMPILE, FORMAT JSON) SELECT 7").await.contains("Unavailable"));
            for blocked in [false, true] {
                let mut session = Session::new(2, Instance::new_in_memory());
                let owner: ObservedOwner = Arc::new(Mutex::new(None));
                let mut sink = TransportSink { blocked, owner: owner.clone() };
                let result = tokio::time::timeout(std::time::Duration::from_millis(50), session.execute_simple_query("EXPLAIN (COMPILE) SELECT 7", &mut sink)).await;
                if blocked { assert!(result.is_err()); }
                else { assert!(result.unwrap().unwrap_err().to_string().contains("injected diagnostic writer failure")); }
                // A failed writer and a dropped/backpressured request both return
                // their reservation; no disconnected-client history remains.
                assert!(owner.lock().unwrap().as_ref().unwrap().upgrade().is_none());
                assert_eq!(session.transaction_state(), paro_session::TransactionState::Idle);
                session.execute_simple_query("SELECT 8", &mut CollectingSink::new()).await.unwrap();
                assert_eq!(session.transaction_state(), paro_session::TransactionState::Idle);
                assert!(!document(&mut session, "EXPLAIN (COMPILE, FORMAT JSON) SELECT 7").await.contains("Unavailable"));
            }
        });
    }).unwrap().join().unwrap();
}

#[test]
fn compile_cancellation_closes_only_its_own_transaction() {
    std::thread::Builder::new()
        .stack_size(32 << 20)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    for explicit in [false, true] {
                        let mut session = Session::new(3, Instance::new_in_memory());
                        if explicit {
                            session.begin_explicit_transaction().unwrap();
                        }
                        let owner: ObservedOwner = Arc::new(Mutex::new(None));
                        let mut sink = TransportSink {
                            blocked: true,
                            owner: owner.clone(),
                        };
                        let control = session.execution_control().clone();
                        let observed = owner.clone();
                        let cancel = async move {
                            while observed.lock().unwrap().is_none() {
                                tokio::task::yield_now().await;
                            }
                            assert!(control.cancel_active_statement(
                                paro_context::StatementCancelReason::UserRequest
                            ));
                        };
                        let (result, ()) =
                            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                                tokio::join!(
                                    session.execute_simple_query(
                                        "EXPLAIN (COMPILE) SELECT 7",
                                        &mut sink
                                    ),
                                    cancel
                                )
                            })
                            .await
                            .expect("accepted cancellation must unblock transport");
                        assert!(result.unwrap_err().is_query_canceled());
                        assert!(owner.lock().unwrap().as_ref().unwrap().upgrade().is_none());
                        assert!(session.execution_control().active_statement().is_none());
                        if explicit {
                            // The ordinary statement error handler marks the caller's
                            // transaction failed; the compile request must not end it.
                            assert_eq!(
                                session.transaction_state(),
                                paro_session::TransactionState::Failed
                            );
                            session.rollback_transaction().unwrap();
                        } else {
                            assert_eq!(
                                session.transaction_state(),
                                paro_session::TransactionState::Idle
                            );
                            session
                                .execute_simple_query("SELECT 8", &mut CollectingSink::new())
                                .await
                                .unwrap();
                            assert_eq!(
                                session.transaction_state(),
                                paro_session::TransactionState::Idle
                            );
                        }
                    }
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn parser_compile_options_are_unambiguous() {
    for sql in [
        "EXPLAIN COMPILE SELECT 1",
        "EXPLAIN (COMPILE, COMPILE) SELECT 1",
        "EXPLAIN (FORMAT JSON) SELECT 1",
        "EXPLAIN (COMPILE, FORMAT JSON, FORMAT TEXT) SELECT 1",
        "EXPLAIN (COMPILE, VERBOSE) SELECT 1",
        "EXPLAIN (COMPILE ON) SELECT 1",
        "EXPLAIN (COMPILE, UNKNOWN) SELECT 1",
        "EXPLAIN ANALYZE (COMPILE) SELECT 1",
    ] {
        assert!(paro_parser::parse_one(sql).is_err(), "{sql}");
    }
    assert!(paro_parser::parse_one("EXPLAIN (SELECT 1)").is_ok());
    assert!(paro_parser::parse_one("EXPLAIN SELECT 1 FORMAT JSON").is_ok());
}
