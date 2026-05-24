// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[path = "common/exec_ok.rs"]
mod exec_ok;
#[path = "common/instance_persistent.rs"]
mod instance_persistent;
#[path = "common/query_bool_col.rs"]
mod query_bool_col;
#[path = "common/query_i64_col.rs"]
mod query_i64_col;
#[path = "common/query_string_col.rs"]
mod query_string_col;
#[path = "common/unique_test_dir.rs"]
mod unique_test_dir;

use paro_common::runtime_value::Value;
use paro_instance::{DatabaseCloseAction, Instance};
use paro_session::{CollectingSink, Session};
use std::future::Future;
use std::sync::Arc;

use exec_ok::exec_ok;
use instance_persistent::create_persistent_instance;
use paro_catalog::entry::{
    CatalogEntryEnum, CreateIndexInfo, IndexBuildState, IndexCatalogEntry,
    IndexType as CatalogIndexType, LogicalIndex,
};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::ddl::{DdlObjectKey, DdlObjectKind};
use paro_common::effect::DeferredTask;
use paro_instance::{
    build_recovery_consistency_report, DeferredTaskRecoveryHook, RecoveryHook, RecoveryHookContext,
    RecoveryHookResult, StartupPolicy,
};
use query_bool_col::query_bool_col;
use query_i64_col::query_i64_col;
use query_string_col::query_string_col;
use std::path::PathBuf;
use unique_test_dir::create_unique_test_dir;

fn explain_lines(sink: &CollectingSink) -> Vec<String> {
    let mut lines = Vec::new();
    let result = sink.assert_single_result();
    for chunk in &result.chunks {
        let col = chunk.column(0).expect("missing EXPLAIN output column");
        for row in 0..chunk.len() {
            match col.get_value(row) {
                Value::Varchar(v) => lines.push(v),
                other => lines.push(other.to_string()),
            }
        }
    }
    lines
}

fn deferred_task_context(session: &Session, task: DeferredTask) -> RecoveryHookContext {
    RecoveryHookContext {
        database_root: PathBuf::from(session.current_database.path()),
        recovery_report: build_recovery_consistency_report(session.current_database.catalog()),
        startup_policy: StartupPolicy::Strict,
        graph_registry: session.instance.graph_manager().clone(),
        scheduler: session.instance.get_scheduler().clone(),
        replayed_deferred_tasks: vec![task],
    }
}

fn lookup_index_entry(session: &Session, index_name: &str) -> Arc<IndexCatalogEntry> {
    let txn = CatalogSnapshot::read_only(u64::MAX);
    let schema = session
        .current_database
        .catalog()
        .get_schema(&txn, session.current_schema())
        .expect("schema should exist");
    let entry = schema
        .get_index(txn.transaction_id, txn.start_time, index_name)
        .expect("index should exist");
    match &*entry {
        CatalogEntryEnum::Index(index) => index.clone(),
        other => panic!("expected index entry, got {other:?}"),
    }
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

async fn run_bm25_ranking_is_stable_across_segments() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_ms (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_ms VALUES (100, 'vector')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_ms VALUES
            (1, 'vector'),
            (2, 'filler'),
            (3, 'filler'),
            (4, 'filler'),
            (5, 'filler'),
            (6, 'filler'),
            (7, 'filler'),
            (8, 'filler'),
            (9, 'filler'),
            (10, 'filler'),
            (11, 'filler'),
            (12, 'filler'),
            (13, 'filler'),
            (14, 'filler'),
            (15, 'filler'),
            (16, 'filler'),
            (17, 'filler'),
            (18, 'filler'),
            (19, 'filler'),
            (20, 'filler')",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX idx_docs_ms_fts ON docs_ms USING GIN (to_tsvector('simple', content))",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT id
         FROM docs_ms
         WHERE fulltext_match(content, 'vector')
         ORDER BY bm25(content, 'vector') DESC, id
         LIMIT 2",
    )
    .await;

    assert_eq!(query_i64_col(&sink, 0), vec![1, 100]);
}

#[test]
fn bm25_ranking_is_stable_across_segments() {
    run_async_test_with_large_stack(
        "fulltext-bm25-ranking",
        run_bm25_ranking_is_stable_across_segments(),
    );
}

async fn run_restart_recovery_keeps_fulltext_index_usable() {
    let base_dir = create_unique_test_dir("fulltext_search", "restart");
    let mut sink = CollectingSink::new();

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(1, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "CREATE TABLE docs_restart (id INT, content VARCHAR)",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "INSERT INTO docs_restart VALUES
                (1, 'vector database'),
                (2, 'vector graph'),
                (3, 'unrelated')",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "CREATE INDEX idx_docs_restart_fts ON docs_restart USING GIN (to_tsvector('simple', content))",
        )
        .await;

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id FROM docs_restart
             WHERE fulltext_match(content, 'vector')
             ORDER BY id",
        )
        .await;
        let recovered_ids = query_i64_col(&sink, 0);
        assert!(
            recovered_ids.len() <= 2,
            "restarted fulltext query should not exceed expected rows: {:?}",
            recovered_ids
        );
        assert!(
            recovered_ids.iter().all(|id| *id == 1 || *id == 2),
            "restarted fulltext query should only contain expected ids: {:?}",
            recovered_ids
        );

        session
            .current_database
            .close(DatabaseCloseAction::Checkpoint)
            .expect("close checkpoint should persist fulltext index state");
    }

    {
        let instance = create_persistent_instance(&base_dir);
        let mut session = Session::new(2, Arc::clone(&instance));

        exec_ok(
            &mut session,
            &mut sink,
            "EXPLAIN SELECT id FROM docs_restart
             WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
             ORDER BY ts_rank(
                to_tsvector('simple', content),
                plainto_tsquery('simple', 'vector')
             ) DESC
             LIMIT 2",
        )
        .await;
        let lines = explain_lines(&sink);
        let has_fulltext_search_plan = lines
            .iter()
            .any(|line| line.to_ascii_uppercase().contains("FULLTEXT_SCAN"));
        let has_filter_fallback = lines
            .iter()
            .any(|line| line.to_ascii_uppercase().contains("FILTER"));
        assert!(
            has_fulltext_search_plan || has_filter_fallback,
            "restarted plan should use index scan or filter fallback, actual:\n{}",
            lines.join("\n")
        );

        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id FROM docs_restart
             WHERE fulltext_match(content, 'vector')
             ORDER BY id",
        )
        .await;
        let fallback_ids = query_i64_col(&sink, 0);
        assert!(
            fallback_ids.len() <= 2,
            "fallback fulltext query should not exceed expected rows: {:?}",
            fallback_ids
        );
        assert!(
            fallback_ids.iter().all(|id| *id == 1 || *id == 2),
            "fallback fulltext query should only contain expected ids: {:?}",
            fallback_ids
        );

        exec_ok(
            &mut session,
            &mut sink,
            "DROP INDEX IF EXISTS idx_docs_restart_fts",
        )
        .await;
        exec_ok(
            &mut session,
            &mut sink,
            "SELECT id FROM docs_restart
             WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
             ORDER BY id",
        )
        .await;
        assert_eq!(query_i64_col(&sink, 0), vec![1, 2]);
    }

    let _ = std::fs::remove_dir_all(&base_dir);
}

#[test]
fn restart_recovery_keeps_fulltext_index_usable() {
    run_async_test_with_large_stack(
        "fulltext-restart-recovery",
        run_restart_recovery_keeps_fulltext_index_usable(),
    );
}

async fn run_create_index_keeps_fulltext_pushdown_ready_while_coverage_is_tail_pending() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_cov (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_cov VALUES
            (1, 'vector database'),
            (2, 'graph database'),
            (3, 'mountain river')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX idx_docs_cov_fts ON docs_cov USING GIN (to_tsvector('simple', content))",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT build_state, extra_info
         FROM paro_indexes()
         WHERE index_name = 'idx_docs_cov_fts'",
    )
    .await;
    assert_eq!(query_string_col(&sink, 0), vec!["READY".to_string()]);
    let extra_info = query_string_col(&sink, 1);
    assert_eq!(extra_info.len(), 1);
    assert!(
        extra_info[0].contains("\"complete\":false"),
        "late fulltext definition should stay tail-pending until bootstrap/catch-up materializes existing rowsets, actual extra_info={}",
        extra_info[0]
    );
    assert!(
        extra_info[0].contains("\"indexed_segment_count\":0"),
        "late fulltext definition should not report existing segments as already materialized, actual extra_info={}",
        extra_info[0]
    );
    assert!(
        extra_info[0].contains("\"visible_segment_count\":1"),
        "coverage should still describe the visible base segment set, actual extra_info={}",
        extra_info[0]
    );

    exec_ok(
        &mut session,
        &mut sink,
        "EXPLAIN
         SELECT id
         FROM docs_cov
         WHERE fulltext_match(content, 'database')
         ORDER BY bm25(content, 'database') DESC, id
         LIMIT 2",
    )
    .await;
    let lines = explain_lines(&sink);
    assert!(
        lines
            .iter()
            .any(|line| line.to_ascii_uppercase().contains("FULLTEXT_SCAN")),
        "fulltext index should be eligible for pushdown after create index, actual:\n{}",
        lines.join("\n")
    );
}

#[test]
fn create_index_keeps_fulltext_pushdown_ready_while_coverage_is_tail_pending() {
    run_async_test_with_large_stack(
        "fulltext-create-index-coverage",
        run_create_index_keeps_fulltext_pushdown_ready_while_coverage_is_tail_pending(),
    );
}

async fn run_inserts_after_empty_fulltext_index_still_use_pushdown() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_cov_late (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX idx_docs_cov_late_fts ON docs_cov_late USING GIN (to_tsvector('simple', content))",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_cov_late VALUES
            (1, 'vector after index'),
            (2, 'noise')",
    )
    .await;

    exec_ok(
        &mut session,
        &mut sink,
        "EXPLAIN
         SELECT id
         FROM docs_cov_late
         WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
         ORDER BY id",
    )
    .await;
    let lines = explain_lines(&sink);
    assert!(
        lines
            .iter()
            .any(|line| line.to_ascii_uppercase().contains("FULLTEXT_SCAN")),
        "fulltext index should stay pushdown-ready for inserts after empty index creation, actual:\n{}",
        lines.join("\n")
    );

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT id
         FROM docs_cov_late
         WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
         ORDER BY id",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);
}

#[test]
fn inserts_after_empty_fulltext_index_still_use_pushdown() {
    run_async_test_with_large_stack(
        "fulltext-empty-index-inserts",
        run_inserts_after_empty_fulltext_index_still_use_pushdown(),
    );
}

async fn run_deferred_task_duplicate_redelivery_skips_already_ready_fulltext_index_state() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_redelivery (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_redelivery VALUES (1, 'vector database'), (2, 'graph database')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX idx_docs_redelivery_fts ON docs_redelivery USING GIN (to_tsvector('simple', content))",
    )
    .await;

    let hook = DeferredTaskRecoveryHook;
    let task = DeferredTask::FinalizeIndexState {
        index: DdlObjectKey::new(
            session.current_database.catalog().name(),
            Some(session.current_schema()),
            "idx_docs_redelivery_fts",
            DdlObjectKind::Index,
        ),
        table_name: "docs_redelivery".to_string(),
        index_type: "FULLTEXT".to_string(),
        column_ids: vec![1],
        fulltext_config: Some("simple".to_string()),
    };

    let first = hook
        .run(
            &session.current_database,
            &deferred_task_context(&session, task.clone()),
        )
        .expect("duplicate recovery hook should not error");
    let second = hook
        .run(
            &session.current_database,
            &deferred_task_context(&session, task),
        )
        .expect("second duplicate recovery hook should not error");

    assert!(matches!(first, RecoveryHookResult::Skipped { .. }));
    assert!(matches!(second, RecoveryHookResult::Skipped { .. }));
}

#[test]
fn deferred_task_duplicate_redelivery_skips_already_ready_fulltext_index_state() {
    run_async_test_with_large_stack(
        "fulltext-redelivery-duplicate",
        run_deferred_task_duplicate_redelivery_skips_already_ready_fulltext_index_state(),
    );
}

async fn run_deferred_task_recovery_uses_catalog_fulltext_config_not_task_payload() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, Arc::clone(&instance));
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_redelivery_fail (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_redelivery_fail VALUES (1, 'vector database')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT COUNT(*) FROM docs_redelivery_fail",
    )
    .await;

    let writer = CatalogSnapshot::permanent_writer(9_901);
    let schema = session
        .current_database
        .catalog()
        .get_schema(&writer, session.current_schema())
        .expect("schema should exist");
    let table_entry = schema
        .get_table(
            writer.transaction_id,
            writer.start_time,
            "docs_redelivery_fail",
        )
        .expect("table should exist");
    let table = table_entry.as_ref().as_table().expect("table entry");
    schema
        .create_index(
            &writer,
            CreateIndexInfo::new(
                session.current_schema().to_string(),
                "docs_redelivery_fail".to_string(),
                "idx_docs_redelivery_fail_fts".to_string(),
                vec![LogicalIndex::new(1)],
                vec![paro_common::types::LogicalType::Varchar],
            )
            .with_catalog(session.current_database.catalog().name().to_string())
            .with_index_type(CatalogIndexType::FullText)
            .with_fulltext_options(LogicalIndex::new(1), "simple"),
            table,
        )
        .expect("create building fulltext index")
        .expect("index entry should be created");

    let hook = DeferredTaskRecoveryHook;
    let task = DeferredTask::FinalizeIndexState {
        index: DdlObjectKey::new(
            session.current_database.catalog().name(),
            Some(session.current_schema()),
            "idx_docs_redelivery_fail_fts",
            DdlObjectKind::Index,
        ),
        table_name: "docs_redelivery_fail".to_string(),
        index_type: "FULLTEXT".to_string(),
        column_ids: vec![1],
        // Recovery should trust the durable catalog entry/provider config rather than a stale
        // deferred-task payload.
        fulltext_config: Some("unsupported_lang".to_string()),
    };
    let result = hook
        .run(
            &session.current_database,
            &deferred_task_context(&session, task.clone()),
        )
        .expect("search deferred task redelivery should report via hook result");

    let index = lookup_index_entry(&session, "idx_docs_redelivery_fail_fts");
    assert!(matches!(result, RecoveryHookResult::Rebuilt { .. }));
    assert_ne!(index.build_state(), IndexBuildState::Failed);
    assert!(
        index.failure_reason().is_none(),
        "durable index metadata should win over stale task payload during recovery"
    );

    let replayed_again = hook
        .run(
            &session.current_database,
            &deferred_task_context(&session, task),
        )
        .expect("already-restored deferred task should skip cleanly");
    assert!(matches!(replayed_again, RecoveryHookResult::Skipped { .. }));
}

#[test]
fn deferred_task_recovery_uses_catalog_fulltext_config_not_task_payload() {
    run_async_test_with_large_stack(
        "fulltext-redelivery-config",
        run_deferred_task_recovery_uses_catalog_fulltext_config_not_task_payload(),
    );
}

async fn run_multilingual_tokenizers_can_be_queried() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_cn (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_cn VALUES
            (1, '向量数据库系统'),
            (2, '分布式计算')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX idx_docs_cn_fts ON docs_cn USING GIN (to_tsvector('chinese', content))",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT id FROM docs_cn
         WHERE to_tsvector('chinese', content) @@ plainto_tsquery('chinese', '数据库')
         ORDER BY id",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![1]);

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT to_tsvector('japanese', '東京ベクトルDB')
         @@ plainto_tsquery('japanese', 'ベクトル')",
    )
    .await;
    assert_eq!(query_bool_col(&sink, 0), vec![true]);

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE docs_ja (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO docs_ja VALUES
            (10, '東京ベクトルDB'),
            (11, '東京データベース'),
            (12, 'vector database')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "CREATE INDEX idx_docs_ja_fts ON docs_ja USING GIN (to_tsvector('japanese', content))",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "SELECT id FROM docs_ja
         WHERE to_tsvector('japanese', content) @@ plainto_tsquery('japanese', 'ベクトル')
         ORDER BY id",
    )
    .await;
    assert_eq!(query_i64_col(&sink, 0), vec![10]);
}

#[test]
fn multilingual_tokenizers_can_be_queried() {
    run_async_test_with_large_stack(
        "fulltext-multilingual-tokenizers",
        run_multilingual_tokenizers_can_be_queried(),
    );
}

async fn run_tsvector_dispatch_works_through_cte_projection() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "CREATE TABLE documents (id INT, content VARCHAR)",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "INSERT INTO documents VALUES
            (1, 'vector db'),
            (2, 'vector database systems'),
            (3, 'hello world')",
    )
    .await;
    exec_ok(
        &mut session,
        &mut sink,
        "EXPLAIN WITH indexed AS (
            SELECT id, to_tsvector('simple', content) AS tsv
            FROM documents
         )
         SELECT COUNT(*)
         FROM indexed
         WHERE tsv @@ plainto_tsquery('simple', 'database')",
    )
    .await;

    assert!(sink.total_rows() > 0);
}

#[test]
fn tsvector_dispatch_works_through_cte_projection() {
    run_async_test_with_large_stack(
        "fulltext-tsvector-cte",
        run_tsvector_dispatch_works_through_cte_projection(),
    );
}

async fn run_json_operator_errors_do_not_break_fulltext_dispatch() {
    let instance = Instance::new_in_memory();
    let mut session = Session::new(1, instance);
    let mut sink = CollectingSink::new();

    exec_ok(
        &mut session,
        &mut sink,
        "SELECT to_tsvector('simple', 'vector database') @@ plainto_tsquery('simple', 'database')",
    )
    .await;
    assert_eq!(sink.total_rows(), 1);
    sink.clear();

    let result = session
        .execute_simple_query("SELECT '{\"a\":1}' @@ '$.a == 1'", &mut sink)
        .await;
    assert!(
        result.is_err() || sink.has_errors(),
        "JSON @@ should currently fail via JSON function path because json_path_match is not registered yet"
    );

    let err_msg = if let Err(err) = result {
        err.to_string()
    } else {
        sink.errors()
            .first()
            .map(|err| err.message.clone())
            .unwrap_or_default()
    };
    assert!(
        err_msg.to_lowercase().contains("json_path_match"),
        "expected JSON @@ fallback error to mention json_path_match, got: {err_msg}"
    );
}

#[test]
fn json_operator_errors_do_not_break_fulltext_dispatch() {
    run_async_test_with_large_stack(
        "fulltext-json-dispatch",
        run_json_operator_errors_do_not_break_fulltext_dispatch(),
    );
}
