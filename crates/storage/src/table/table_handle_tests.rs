// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::compaction::compaction_manager::CompactionManager;
use crate::compaction::compaction_task::{CompactionTask, HorizontalCompactionTask};
use crate::compaction::execution::index_rebuild::rebuild_compaction_indexes;
use crate::compaction::execution::rowset_merger::RowsetMerger;
use crate::compaction::execution::workspace::{CompactionBuildOutput, CompactionWorkspace};
use crate::compaction::plan::CompactionPlanner;
use crate::index::fulltext::query_parser::ParsedQuery;
use crate::index::fulltext::scoring::FullTextScoreMode;
use crate::index::fulltext::text_index::{FullTextIndex, FullTextIndexConfig};
use crate::index::fulltext::tokenizer::TokenizerKind;
use crate::index::hnsw::{DistanceMetric, SearchParams};
use crate::index::{BoundIndex, Predicate, PredicateResult, PredicateTree};
use crate::meta::{FileMetadataStore, GlobalSchemaMap, MetadataStore, TabletMetaManager};
use crate::metrics::storage_metrics;
use crate::rowset::{SegmentIterator, SparseVector};
use crate::search::providers::fulltext::search::FullTextTopKProvider;
use crate::search::{
    ArtifactLocation, CoverageState, OpenedSearchCursor, ResourceBudget, SearchBatchConfig,
    SearchBatchState, SearchCapabilityState, SearchFreshnessPolicy, SearchIndexDefinition,
    SearchIndexKind, SearchMaintenanceAction, SearchProviderStats,
};
use crate::statistics::IndexType;
use crate::table::storage_descriptor::TableStorageDescriptor;
use crate::table::table_factory::{stable_data_dir, TableFactory};
use crate::tablet::tablet_reader::TabletReaderParams;
use crate::tablet::{KeysType, TabletColumn, TabletSchema};
use crate::test_utils::*;
use crate::transaction::txn::Transaction;
use paro_common::allocator::default_allocator;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_scheduler::scheduler::TaskScheduler;
use paro_transaction::{
    CommandId, CommitTs, IsolationLevel, ParticipantStateSet, ReadSnapshot, ReadTs,
    RetentionLeaseKind, TransactionView,
};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

static NEXT_TEST_TABLET_ID: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(now_micros()));
static NEXT_TEST_SEARCH_DEFINITION_ID: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(now_micros()));
static TEST_STORAGE_ROOT: LazyLock<PathBuf> = LazyLock::new(|| {
    std::env::temp_dir().join(format!(
        "paro_table_handle_tests_{}_{}",
        std::process::id(),
        now_micros()
    ))
});

fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default()
        .create_table(types)
        .expect("create table")
}

fn create_table_with_keys(types: &[LogicalType], keys_type: KeysType) -> TableHandle {
    TableFactory::default()
        .create_table_with_keys(types, keys_type)
        .expect("create keyed table")
}

fn create_table_with_keys_with_meta_manager(
    types: &[LogicalType],
    keys_type: KeysType,
    meta_manager: Option<Arc<TabletMetaManager>>,
) -> TableHandle {
    TableFactory::new(meta_manager)
        .create_table_with_keys(types, keys_type)
        .expect("create keyed table with meta manager")
}

fn create_table_from_specs(specs: &[TableColumnSpec]) -> TableHandle {
    TableFactory::default()
        .create_table_from_specs(specs)
        .expect("create table from specs")
}

fn read_view(table: &TableHandle) -> TransactionView {
    let visible = u64::try_from(table.max_version()).unwrap_or(0);
    TransactionView::autocommit(ReadTs::new(visible))
}

fn transaction_view_for_command(txn: &Transaction, command_id: u32) -> TransactionView {
    TransactionView::new(
        txn.writer_id(),
        txn.read_ts(),
        ReadSnapshot::without_lease(ReadTs::new(txn.visible_commit_ts().into_raw())),
        IsolationLevel::Snapshot,
        CommandId::new(command_id),
        paro_transaction::ReadTrackerHandle::noop(),
        ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]),
    )
}

fn commit_transaction(txn: &Transaction, commit_id: u64) -> paro_common::error::Result<()> {
    let apply_result = txn.apply_prepared_storage_for_commit(commit_id);
    txn.release_transaction_locks();
    apply_result?;
    txn.finalize_applied_commit(commit_id)
}

fn open_table_from_descriptor_with_meta_manager(
    types: &[LogicalType],
    descriptor: &TableStorageDescriptor,
    meta_manager: Option<Arc<TabletMetaManager>>,
) -> TableHandle {
    TableFactory::new(meta_manager)
        .open_from_descriptor(types, descriptor)
        .expect("open table from descriptor with meta manager")
}

fn next_test_tablet_id() -> u64 {
    NEXT_TEST_TABLET_ID.fetch_add(1, AtomicOrdering::Relaxed)
}

fn stable_test_data_dir(table_id: u64, partition_id: u64, tablet_id: u64) -> PathBuf {
    stable_data_dir(&TEST_STORAGE_ROOT, table_id, partition_id, tablet_id)
}

fn register_fulltext_definition(table: &TableHandle, column_id: u32, config: &str) -> u64 {
    register_fulltext_definition_with_freshness(
        table,
        column_id,
        config,
        SearchFreshnessPolicy::default_for_kind(SearchIndexKind::FullText),
    )
}

fn register_fulltext_definition_with_freshness(
    table: &TableHandle,
    column_id: u32,
    config: &str,
    freshness_policy: SearchFreshnessPolicy,
) -> u64 {
    let definition_id = NEXT_TEST_SEARCH_DEFINITION_ID.fetch_add(1, AtomicOrdering::Relaxed);
    let provider_config = json!({"version": 1, "config": config });
    let expression = format!("to_tsvector('{}', col_{})", config, column_id);
    let definition = SearchIndexDefinition {
        definition_id,
        table_id: table.tablet().table_id(),
        name: format!("__test_fulltext_{}_{}", column_id, definition_id),
        kind: SearchIndexKind::FullText,
        column_ids: vec![column_id],
        expression: Some(expression.clone()),
        freshness_policy,
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::FullText,
            &[column_id],
            Some(&expression),
            &provider_config,
        ),
        provider_config,
    };
    table
        .register_search_definition(definition)
        .expect("register test fulltext definition");
    definition_id
}

fn register_sparse_definition(table: &TableHandle, column_id: u32) -> u64 {
    let definition_id = NEXT_TEST_SEARCH_DEFINITION_ID.fetch_add(1, AtomicOrdering::Relaxed);
    let expression = format!("sparse_vector(col_{})", column_id);
    let provider_config = json!({"version": 1, "physical_encoding": "binary-v1" });
    let definition = SearchIndexDefinition {
        definition_id,
        table_id: table.tablet().table_id(),
        name: format!("__test_sparse_{}_{}", column_id, definition_id),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![column_id],
        expression: Some(expression.clone()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Sparse,
            &[column_id],
            Some(&expression),
            &provider_config,
        ),
        provider_config,
    };
    table
        .register_search_definition(definition)
        .expect("register test sparse definition");
    definition_id
}

fn test_sparse_blob_vector(values: &[SparseVector]) -> Vector {
    let mut vector = Vector::try_new(LogicalType::Blob, values.len(), test_allocator())
        .expect("blob vector allocation");
    for (idx, value) in values.iter().enumerate() {
        vector.set_blob(idx, &value.to_row_image_v1().expect("sparse row image"));
    }
    vector.set_count(values.len());
    vector
}

fn collect_row_ids_by_id(table: &TableHandle) -> HashMap<i32, u64> {
    let mut reader = table
        .create_reader(TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true))
        .expect("create reader");
    reader.prepare().expect("prepare reader");

    let mut row_ids = HashMap::new();
    while let Some(chunk) = reader.get_next_chunk().expect("read chunk") {
        let ids = chunk.column(0).expect("id column");
        let row_id_col = chunk
            .column(chunk.column_count() - 1)
            .expect("row_id column");
        for idx in 0..chunk.size() {
            row_ids.insert(
                ids.get_i32(idx).expect("id as i32"),
                row_id_col.get_i64(idx).expect("row_id as i64") as u64,
            );
        }
    }
    row_ids
}

fn collect_rows_i32_pair(table: &TableHandle) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for chunk in table.scan_chunks().expect("scan chunks") {
        let c0 = chunk.column(0).expect("column 0");
        let c1 = chunk.column(1).expect("column 1");
        for idx in 0..chunk.size() {
            rows.push((
                c0.get_i32(idx).expect("column 0 as i32"),
                c1.get_i32(idx).expect("column 1 as i32"),
            ));
        }
    }
    rows.sort_unstable_by_key(|(id, _)| *id);
    rows
}

fn collect_rows_i32_triple(table: &TableHandle) -> Vec<(i32, i32, i32)> {
    let mut rows = Vec::new();
    for chunk in table.scan_chunks().expect("scan chunks") {
        let c0 = chunk.column(0).expect("column 0");
        let c1 = chunk.column(1).expect("column 1");
        let c2 = chunk.column(2).expect("column 2");
        for idx in 0..chunk.size() {
            rows.push((
                c0.get_i32(idx).expect("column 0 as i32"),
                c1.get_i32(idx).expect("column 1 as i32"),
                c2.get_i32(idx).expect("column 2 as i32"),
            ));
        }
    }
    rows.sort_unstable_by_key(|(id, _, _)| *id);
    rows
}

fn collect_rows_i32_i32_string(table: &TableHandle) -> Vec<(i32, i32, String)> {
    let mut rows = Vec::new();
    for chunk in table.scan_chunks().expect("scan chunks") {
        let c0 = chunk.column(0).expect("column 0");
        let c1 = chunk.column(1).expect("column 1");
        let c2 = chunk.column(2).expect("column 2");
        for idx in 0..chunk.size() {
            rows.push((
                c0.get_i32(idx).expect("column 0 as i32"),
                c1.get_i32(idx).expect("column 1 as i32"),
                c2.get_string(idx).expect("column 2 as string").to_string(),
            ));
        }
    }
    rows.sort_unstable_by_key(|(id, _, _)| *id);
    rows
}

fn collect_i32_column(chunks: &[Chunk], col_idx: usize) -> Vec<i32> {
    let mut values = Vec::new();
    for chunk in chunks {
        let col = chunk.column(col_idx).expect("column not found");
        for row in 0..chunk.size() {
            values.push(col.get_i32(row).expect("column value as i32"));
        }
    }
    values.sort_unstable();
    values
}

fn collect_i32_score_pairs(
    chunks: &[Chunk],
    id_col_idx: usize,
    score_col_idx: usize,
) -> Vec<(i32, f32)> {
    let mut values = Vec::new();
    for chunk in chunks {
        let ids = chunk.column(id_col_idx).expect("id column not found");
        let scores = chunk.column(score_col_idx).expect("score column not found");
        for row in 0..chunk.size() {
            values.push((
                ids.get_i32(row).expect("id column value as i32"),
                scores.get_f32(row).expect("score column value as f32"),
            ));
        }
    }
    values.sort_unstable_by_key(|(id, _)| *id);
    values
}

fn fulltext_degraded_score_metric_count(table_id: u64, reason: &str) -> u64 {
    storage_metrics()
        .snapshot()
        .search_fulltext_degraded_score_by_key
        .into_iter()
        .find(|series| series.key.table_id == table_id && series.key.reason == reason)
        .map(|series| series.degraded_queries)
        .unwrap_or(0)
}

fn drain_search_cursor(
    table: &TableHandle,
    opened: OpenedSearchCursor,
    projected_columns: &[usize],
    emit_score: bool,
    row_limit: usize,
    parallelism_slots: usize,
) -> paro_common::error::Result<Vec<Chunk>> {
    let mut chunks = Vec::new();
    let mut cursor = opened.cursor;
    let snapshot = opened.snapshot;
    let batch_config = SearchBatchConfig {
        row_limit: row_limit.max(1),
        preferred_bytes: 1 << 20,
    };
    let mut budget = ResourceBudget::standalone(
        64 * 1024 * 1024,
        row_limit.max(1024),
        parallelism_slots.max(1),
    );

    loop {
        match cursor.next_batch(&batch_config, &mut budget)? {
            SearchBatchState::Ready(batch) if batch.is_empty() => continue,
            SearchBatchState::Ready(batch) => chunks.push(table.materialize_search_batch(
                &snapshot,
                batch,
                projected_columns,
                emit_score,
                Arc::new(default_allocator()),
            )?),
            SearchBatchState::Exhausted => return Ok(chunks),
        }
    }
}

fn run_vector_cursor_with_slots(
    table: &TableHandle,
    column_id: usize,
    query: &[f32],
    k: usize,
    params: SearchParams,
    predicate: Option<&PredicateTree>,
    projected_columns: &[usize],
    parallelism_slots: usize,
) -> paro_common::error::Result<Vec<Chunk>> {
    let opened = table.open_vector_search_cursor(
        column_id,
        query,
        DistanceMetric::Euclidean,
        k,
        params,
        predicate.cloned(),
        table.max_version(),
        &crate::search::SearchReadOptions::ungoverned(),
    )?;
    drain_search_cursor(
        table,
        opened,
        projected_columns,
        false,
        k.clamp(1, 1024),
        parallelism_slots,
    )
}

fn infer_fulltext_config(table: &TableHandle, column_id: usize) -> String {
    table
        .search_write_context()
        .expect("search write context")
        .plan
        .fulltext
        .into_iter()
        .find_map(|binding| (binding.column_id as usize == column_id).then_some(binding.config))
        .unwrap_or_else(|| "simple".to_string())
}

fn sparse_search(
    table: &TableHandle,
    column_id: usize,
    query: &SparseVector,
    k: usize,
    projected_columns: &[usize],
) -> paro_common::error::Result<Vec<Chunk>> {
    let opened = table.open_sparse_vector_search_cursor(
        column_id,
        query,
        k,
        None,
        table.max_version(),
        &crate::search::SearchReadOptions::ungoverned(),
    )?;
    drain_search_cursor(table, opened, projected_columns, false, k.clamp(1, 1024), 4)
}

trait SearchCursorTestExt {
    fn vector_search(
        &self,
        column_id: usize,
        query: &[f32],
        k: usize,
        params: &SearchParams,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> paro_common::error::Result<Vec<Chunk>>;

    fn vector_search_many(
        &self,
        column_id: usize,
        queries: &[&[f32]],
        k: usize,
        params: &SearchParams,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> paro_common::error::Result<Vec<Vec<Chunk>>>;

    fn fulltext_filter(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> paro_common::error::Result<Vec<Chunk>>;
}

impl SearchCursorTestExt for TableHandle {
    fn vector_search(
        &self,
        column_id: usize,
        query: &[f32],
        k: usize,
        params: &SearchParams,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> paro_common::error::Result<Vec<Chunk>> {
        run_vector_cursor_with_slots(
            self,
            column_id,
            query,
            k,
            *params,
            predicate,
            projected_columns,
            4,
        )
    }

    fn vector_search_many(
        &self,
        column_id: usize,
        queries: &[&[f32]],
        k: usize,
        params: &SearchParams,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> paro_common::error::Result<Vec<Vec<Chunk>>> {
        queries
            .iter()
            .map(|query| {
                run_vector_cursor_with_slots(
                    self,
                    column_id,
                    query,
                    k,
                    *params,
                    predicate,
                    projected_columns,
                    4,
                )
            })
            .collect()
    }

    fn fulltext_filter(
        &self,
        column_id: usize,
        query: &ParsedQuery,
        predicate: Option<&PredicateTree>,
        projected_columns: &[usize],
    ) -> paro_common::error::Result<Vec<Chunk>> {
        let config = infer_fulltext_config(self, column_id);
        let opened = self.open_fulltext_filter_cursor(
            column_id,
            query,
            &config,
            predicate.cloned(),
            self.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )?;
        drain_search_cursor(self, opened, projected_columns, false, 1024, 4)
    }
}

fn chunk_with_i32_range(start: i32, end: i32, offset: i32) -> Chunk {
    let ids: Vec<i32> = (start..end).collect();
    let values: Vec<i32> = ids.iter().map(|id| *id + offset).collect();
    test_chunk_from_vectors(vec![test_i32_vector(&ids), test_i32_vector(&values)])
}

#[test]
fn compaction_does_not_rewrite_a_single_fresh_rowset() {
    let table = create_table(&[LogicalType::Integer]);
    table
        .append(&test_chunk_from_vectors(vec![test_i32_vector(
            &(0..4_096).collect::<Vec<_>>(),
        )]))
        .unwrap();

    assert_eq!(table.tablet().num_rowsets(), 1);
    assert!(
        CompactionPlanner::plan(table.tablet().as_ref())
            .unwrap()
            .is_none(),
        "one fresh rowset is already a canonical publish unit"
    );
}

fn build_duplicate_key_compaction_output(
    table: &TableHandle,
    job_id: u64,
) -> crate::compaction::execution::workspace::StagedArtifact {
    let plan = CompactionPlanner::plan(table.tablet().as_ref())
        .unwrap()
        .expect("duplicate-key compaction plan");
    let workspace = CompactionWorkspace::create(
        table.tablet().as_ref(),
        crate::compaction::plan::types::CompactionJobId(job_id),
        plan.output_rowset_id,
    )
    .unwrap();
    let output = RowsetMerger::build(
        table.tablet().as_ref(),
        Arc::new(plan.clone()),
        workspace,
        test_allocator(),
    )
    .unwrap()
    .expect("compaction output rowset");
    match output {
        CompactionBuildOutput::Rowset(artifact) => artifact,
        CompactionBuildOutput::PrimaryKey { .. } => panic!("expected duplicate-key output"),
    }
}

fn create_hnsw_table(dim: usize) -> TableHandle {
    let vector_type = LogicalType::Array(Box::new(LogicalType::Float), dim);
    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![
                TabletColumn::key(0, "id", LogicalType::Integer),
                TabletColumn::new(1, "vec", vector_type.clone()).with_hnsw_index(8, 64, 0),
            ],
            KeysType::PrimaryKeys,
        )
        .expect("hnsw schema"),
    );

    let tablet_id = next_test_tablet_id();
    let table_id = tablet_id;
    let partition_id = 0;
    let data_dir = stable_test_data_dir(table_id, partition_id, tablet_id);
    let tablet = Tablet::new(tablet_id, table_id, partition_id, schema, &data_dir, None)
        .expect("create tablet");
    tablet.init().expect("init tablet");
    tablet.save_meta().expect("save tablet meta");

    TableHandle::from_runtime_tablet(tablet, vec![LogicalType::Integer, vector_type])
}

fn create_hnsw_table_with_note(dim: usize) -> TableHandle {
    let vector_type = LogicalType::Array(Box::new(LogicalType::Float), dim);
    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![
                TabletColumn::key(0, "id", LogicalType::Integer),
                TabletColumn::new(1, "vec", vector_type.clone()).with_hnsw_index(8, 64, 0),
                TabletColumn::new(2, "note", LogicalType::Varchar),
            ],
            KeysType::PrimaryKeys,
        )
        .expect("hnsw schema with note"),
    );

    let tablet_id = next_test_tablet_id();
    let table_id = tablet_id;
    let partition_id = 0;
    let data_dir = stable_test_data_dir(table_id, partition_id, tablet_id);
    let tablet = Tablet::new(tablet_id, table_id, partition_id, schema, &data_dir, None)
        .expect("create tablet");
    tablet.init().expect("init tablet");
    tablet.save_meta().expect("save tablet meta");

    TableHandle::from_runtime_tablet(
        tablet,
        vec![LogicalType::Integer, vector_type, LogicalType::Varchar],
    )
}

fn chunk_with_embeddings(ids: &[i32], embeddings: &[Vec<f32>], dim: usize) -> Chunk {
    assert_eq!(ids.len(), embeddings.len());
    test_chunk_from_vectors(vec![
        test_i32_vector(ids),
        test_embedding_vector(embeddings, dim),
    ])
}

fn chunk_with_embeddings_and_notes(
    ids: &[i32],
    embeddings: &[Vec<f32>],
    notes: &[Option<&str>],
    dim: usize,
) -> Chunk {
    assert_eq!(ids.len(), embeddings.len());
    assert_eq!(ids.len(), notes.len());

    let mut note_vec = test_vector_with_capacity(LogicalType::Varchar, notes.len());
    note_vec.set_count(notes.len());
    for (idx, note) in notes.iter().enumerate() {
        match note {
            Some(note) => note_vec.set_string(idx, note),
            None => note_vec.set_null(idx, true),
        }
    }

    test_chunk_from_vectors(vec![
        test_i32_vector(ids),
        test_embedding_vector(embeddings, dim),
        note_vec,
    ])
}

#[test]
fn append_and_scan_roundtrip() {
    let types = vec![LogicalType::Integer];
    let table = create_table(&types);

    let vec = test_i32_vector(&[1, 2, 3]);
    let chunk = test_chunk_from_vectors(vec![vec]);

    table.append(&chunk).unwrap();
    assert_eq!(table.total_rows().unwrap(), 3);
    assert_eq!(table.rowset_count(), 1);

    let mut out = test_empty_data_chunk();
    table.scan_legacy(&mut out).unwrap();
    assert_eq!(out.size(), 3);
}

#[tokio::test]
async fn shutdown_sweep_drains_bound_compaction_manager_before_removing_tablet_dir() {
    let table = Arc::new(create_table(&[LogicalType::Integer, LogicalType::Integer]));
    table.append(&chunk_with_i32_range(0, 2_000, 0)).unwrap();
    table
        .append(&chunk_with_i32_range(2_000, 4_000, 10))
        .unwrap();
    table
        .append(&chunk_with_i32_range(4_000, 6_000, 20))
        .unwrap();

    let manager = Arc::new(CompactionManager::new(1));
    manager.register_tablet(table.tablet());
    table.bind_compaction_manager(&manager);
    manager.schedule().await;

    let start = std::time::Instant::now();
    while !manager
        .observability()
        .running_tablets
        .contains(&table.tablet_id())
        && table.rowset_count() > 1
    {
        assert!(
            start.elapsed() <= Duration::from_secs(10),
            "timed out waiting for compaction to start"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let data_dir = PathBuf::from(table.to_descriptor().unwrap().data_dir);
    let table_for_drop = table.clone();
    tokio::task::spawn_blocking(move || {
        table_for_drop
            .mark_shutdown_and_schedule_sweep(false)
            .unwrap();
    })
    .await
    .unwrap();

    assert!(manager.observability().running_tablets.is_empty());
    assert_eq!(manager.observability().registered_tablets, 0);

    let start = std::time::Instant::now();
    while data_dir.exists() {
        assert!(
            start.elapsed() <= Duration::from_secs(10),
            "timed out waiting for shutdown sweep to remove {}",
            data_dir.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn primary_key_append_and_delete() {
    let types = vec![LogicalType::Integer, LogicalType::Integer];
    let table = create_table_with_keys(&types, KeysType::PrimaryKeys);

    // Build chunk: key=id, value=v
    let keys = test_i32_vector(&[1, 2, 2]); // duplicate key 2
    let vals = test_i32_vector(&[10, 20, 30]);
    let chunk = test_chunk_from_vectors(vec![keys, vals]);

    table.append(&chunk).unwrap();

    // PrimaryIndex should dedup to 2 rows (keys 1 and 2 last win)
    assert_eq!(
        table
            .tablet()
            .snapshot_primary_index_entries()
            .unwrap()
            .len(),
        2
    );

    // Delete key 2
    let del_keys = test_chunk_from_vectors(vec![test_i32_vector(&[2])]);
    let removed = table
        .delete_by_primary_keys_direct(&read_view(&table), &del_keys)
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(
        table
            .tablet()
            .snapshot_primary_index_entries()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn row_id_delete_persists_delete_vector() {
    let table = create_table(&[LogicalType::Integer]);
    let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]);
    table.append(&chunk).unwrap();

    let mut reader = table
        .create_reader(TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true))
        .unwrap();
    reader.prepare().unwrap();

    let mut target_row_id = None;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        let values = chunk.column(0).unwrap();
        let row_ids = chunk.column(1).unwrap();
        for idx in 0..chunk.size() {
            if values.get_i32(idx) == Some(2) {
                target_row_id = Some(row_ids.get_i64(idx).unwrap() as u64);
            }
        }
    }

    let target_row_id = target_row_id.expect("row_id for value=2");

    let deleted = table
        .delete_direct(&read_view(&table), &[target_row_id])
        .unwrap();
    assert_eq!(deleted, 1);

    let mut values_after_delete = Vec::new();
    for chunk in table.scan_chunks().unwrap() {
        let col = chunk.column(0).unwrap();
        for idx in 0..chunk.size() {
            values_after_delete.push(col.get_i32(idx).unwrap());
        }
    }
    values_after_delete.sort_unstable();
    assert_eq!(values_after_delete, vec![1, 3]);
}

#[test]
fn delete_all_uses_transaction_view_read_ts_for_target_discovery() {
    let table = create_table(&[LogicalType::Integer]);
    table
        .append(&test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]))
        .unwrap();
    let view = read_view(&table);

    table
        .append(&test_chunk_from_vectors(vec![test_i32_vector(&[4])]))
        .unwrap();

    let deleted = table.delete_all_direct(&view).unwrap();
    assert_eq!(deleted, 3);

    let rows = collect_i32_column(&table.scan_chunks().unwrap(), 0);
    assert_eq!(rows, vec![4]);
}

#[test]
fn update_target_discovery_uses_transaction_view_read_ts() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1]),
            test_i32_vector(&[10]),
        ]))
        .unwrap();
    let view = read_view(&table);

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[2]),
            test_i32_vector(&[20]),
        ]))
        .unwrap();
    let row_ids = collect_row_ids_by_id(&table);

    let err = table
        .update_direct(&view, &[row_ids[&2]], &[1], &[vec![Value::Integer(200)]])
        .unwrap_err();
    assert!(
        err.to_string().contains("UPDATE target row not found"),
        "unexpected error: {err}"
    );

    let updated = table
        .update_direct(&view, &[row_ids[&1]], &[1], &[vec![Value::Integer(100)]])
        .unwrap();
    assert_eq!(updated, 1);
    assert_eq!(collect_rows_i32_pair(&table), vec![(1, 100), (2, 20)]);
}

#[test]
fn primary_key_update_changes_key_and_row_values() {
    let table = create_table_with_keys(
        &[LogicalType::Integer, LogicalType::Integer],
        KeysType::PrimaryKeys,
    );
    let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2]), test_i32_vector(&[10, 20])]);
    table.append(&chunk).unwrap();

    let mut reader = table
        .create_reader(TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true))
        .unwrap();
    reader.prepare().unwrap();

    let mut target_row_id = None;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        let ids = chunk.column(0).unwrap();
        let row_ids = chunk.column(2).unwrap();
        for idx in 0..chunk.size() {
            if ids.get_i32(idx) == Some(1) {
                target_row_id = Some(row_ids.get_i64(idx).unwrap() as u64);
            }
        }
    }
    let target_row_id = target_row_id.expect("row_id for id=1");

    let updated = table
        .update_direct(
            &read_view(&table),
            &[target_row_id],
            &[0, 1],
            &[vec![Value::Integer(3)], vec![Value::Integer(15)]],
        )
        .unwrap();
    assert_eq!(updated, 1);

    let mut rows = Vec::new();
    for chunk in table.scan_chunks().unwrap() {
        let id_col = chunk.column(0).unwrap();
        let v_col = chunk.column(1).unwrap();
        for idx in 0..chunk.size() {
            rows.push((id_col.get_i32(idx).unwrap(), v_col.get_i32(idx).unwrap()));
        }
    }
    rows.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(rows, vec![(2, 20), (3, 15)]);

    let schema = table.tablet().schema().unwrap();
    let serializer = crate::primary_key::PrimaryKeySerializer::from_schema_ref(&schema).unwrap();
    let old_key_chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1])]);
    let new_key_chunk = test_chunk_from_vectors(vec![test_i32_vector(&[3])]);
    let old_key = serializer.encode_row(&old_key_chunk, 0).unwrap();
    let new_key = serializer.encode_row(&new_key_chunk, 0).unwrap();
    assert!(table
        .tablet()
        .lookup_primary_key(&old_key)
        .unwrap()
        .is_none());
    assert!(table
        .tablet()
        .lookup_primary_key(&new_key)
        .unwrap()
        .is_some());
}

#[test]
fn duplicate_key_update_rewrites_row_by_row_id() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2]), test_i32_vector(&[10, 20])]);
    table.append(&chunk).unwrap();

    let mut reader = table
        .create_reader(TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true))
        .unwrap();
    reader.prepare().unwrap();

    let mut target_row_id = None;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        let ids = chunk.column(0).unwrap();
        let row_ids = chunk.column(2).unwrap();
        for idx in 0..chunk.size() {
            if ids.get_i32(idx) == Some(2) {
                target_row_id = Some(row_ids.get_i64(idx).unwrap() as u64);
            }
        }
    }
    let target_row_id = target_row_id.expect("row_id for id=2");

    let updated = table
        .update_direct(
            &read_view(&table),
            &[target_row_id],
            &[1],
            &[vec![Value::Integer(99)]],
        )
        .unwrap();
    assert_eq!(updated, 1);

    let mut rows = Vec::new();
    for chunk in table.scan_chunks().unwrap() {
        let id_col = chunk.column(0).unwrap();
        let v_col = chunk.column(1).unwrap();
        for idx in 0..chunk.size() {
            rows.push((id_col.get_i32(idx).unwrap(), v_col.get_i32(idx).unwrap()));
        }
    }
    rows.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(rows, vec![(1, 10), (2, 99)]);
}

#[test]
fn insert_on_conflict_do_nothing_keeps_existing_primary_key_rows() {
    let table = create_table_with_keys(
        &[LogicalType::Integer, LogicalType::Integer],
        KeysType::PrimaryKeys,
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
        ]))
        .unwrap();

    let affected = table
        .insert_on_conflict_direct(
            &read_view(&table),
            &test_chunk_from_vectors(vec![test_i32_vector(&[2, 3]), test_i32_vector(&[200, 30])]),
            &InsertOnConflictAction::DoNothing,
        )
        .unwrap();

    assert_eq!(affected, 1);
    assert_eq!(
        collect_rows_i32_pair(&table),
        vec![(1, 10), (2, 20), (3, 30)]
    );
}

#[test]
fn insert_on_conflict_do_update_writes_partial_rowset_for_non_key_columns() {
    let table = create_table_with_keys(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ],
        KeysType::PrimaryKeys,
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
            test_i32_vector(&[100, 200]),
        ]))
        .unwrap();

    let affected = table
        .insert_on_conflict_direct(
            &read_view(&table),
            &test_chunk_from_vectors(vec![
                test_i32_vector(&[2, 3]),
                test_i32_vector(&[999, 30]),
                test_i32_vector(&[222, 300]),
            ]),
            &InsertOnConflictAction::DoUpdate {
                target_columns: vec![2],
                source_columns: vec![2],
            },
        )
        .unwrap();

    assert_eq!(affected, 2);
    assert_eq!(
        collect_rows_i32_triple(&table),
        vec![(1, 10, 100), (2, 20, 222), (3, 30, 300)]
    );

    let latest = table
        .tablet()
        .rowset_with_max_version()
        .expect("latest rowset after ON CONFLICT DO UPDATE");
    latest.load().unwrap();
    let base_rowids = crate::rowset::load_base_rowids(latest.rowset_path(), 0)
        .unwrap()
        .expect("partial row sidecar should exist");
    assert_eq!(base_rowids.len(), 1);

    let segment = latest.get_segment(0).expect("latest segment");
    assert!(segment.get_column_meta(0).is_some());
    assert!(segment.get_column_meta(2).is_some());
    assert!(
        segment.get_column_meta(1).is_none(),
        "partial rowset should only store key columns plus updated columns"
    );
}

#[test]
fn tablet_reader_get_by_rowids_resolves_partial_update_columns() {
    let table = create_table_with_keys(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ],
        KeysType::PrimaryKeys,
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
            test_i32_vector(&[100, 200]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    table
        .update_direct(
            &read_view(&table),
            &[row_ids[&2]],
            &[2],
            &[vec![Value::Integer(222)]],
        )
        .unwrap();
    let row_ids_after = collect_row_ids_by_id(&table);

    let mut reader = table
        .create_reader(TabletReaderParams::with_version(table.max_version()))
        .unwrap();
    reader.prepare().unwrap();
    let chunk = reader
        .get_by_rowids(&[row_ids_after[&2]], &[0, 1, 2])
        .unwrap();

    assert_eq!(chunk.size(), 1);
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(2));
    assert_eq!(chunk.column(1).unwrap().get_i32(0), Some(20));
    assert_eq!(chunk.column(2).unwrap().get_i32(0), Some(222));

    let rowid_reader = crate::tablet::TabletRowIdReader::new(
        table.tablet(),
        table
            .tablet()
            .capture_consistent_rowsets(table.max_version())
            .unwrap(),
        &[0, 1, 2],
        Arc::new(paro_common::allocator::default_allocator()),
    )
    .unwrap();
    let chunk = rowid_reader
        .get_by_rowids(&[row_ids_after[&2]], &[0, 1, 2])
        .unwrap();
    assert_eq!(chunk.column(0).unwrap().get_i32(0), Some(2));
    assert_eq!(chunk.column(1).unwrap().get_i32(0), Some(20));
    assert_eq!(chunk.column(2).unwrap().get_i32(0), Some(222));
}

#[test]
fn pk_compaction_materializes_partial_update_chains_into_full_rows() {
    let table = create_table_with_keys(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ],
        KeysType::PrimaryKeys,
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
            test_i32_vector(&[100, 200]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    let updated = table
        .update_direct(
            &read_view(&table),
            &[row_ids[&2]],
            &[2],
            &[vec![Value::Integer(222)]],
        )
        .unwrap();
    assert_eq!(updated, 1);

    assert!(
        table
            .tablet()
            .capture_consistent_rowsets(i64::MAX)
            .unwrap()
            .len()
            >= 2,
        "expected base rowset plus partial update rowset before compaction"
    );
    let plan = CompactionPlanner::plan(table.tablet().as_ref())
        .unwrap()
        .expect("primary-key compaction plan");
    let mut task = HorizontalCompactionTask::new(table.tablet(), plan, test_allocator());
    task.run().unwrap();

    assert_eq!(table.tablet().num_rowsets(), 1);
    assert_eq!(
        collect_rows_i32_triple(&table),
        vec![(1, 10, 100), (2, 20, 222)]
    );

    let output = table
        .tablet()
        .rowset_with_max_version()
        .expect("compaction output rowset");
    output.load().unwrap();
    assert!(
        crate::rowset::load_base_rowids(output.rowset_path(), 0)
            .unwrap()
            .is_none(),
        "compaction output should materialize full rows instead of keeping a partial sidecar"
    );

    let segment = output.get_segment(0).expect("compaction output segment");
    assert!(segment.get_column_meta(0).is_some());
    assert!(segment.get_column_meta(1).is_some());
    assert!(segment.get_column_meta(2).is_some());
}

#[test]
fn partial_update_restart_rebuilds_primary_key_row_visibility() {
    let tmp = TempDir::new().unwrap();
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(tmp.path().join("meta")).unwrap());
    let manager = Arc::new(TabletMetaManager::new(
        store,
        Arc::new(GlobalSchemaMap::new()),
    ));
    let table = create_table_with_keys_with_meta_manager(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        KeysType::PrimaryKeys,
        Some(Arc::clone(&manager)),
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[100, 200]),
            test_string_vector(&["before-restart", "stable"]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    let updated = table
        .update_direct(
            &read_view(&table),
            &[row_ids[&1]],
            &[2],
            &[vec![Value::Varchar("after-restart".to_string())]],
        )
        .unwrap();
    assert_eq!(updated, 1);
    assert_eq!(
        collect_rows_i32_i32_string(&table),
        vec![
            (1, 100, "after-restart".to_string()),
            (2, 200, "stable".to_string()),
        ]
    );

    table.tablet().save_meta().unwrap();
    let descriptor = table.to_descriptor().unwrap();
    let restored =
        open_table_from_descriptor_with_meta_manager(table.types(), &descriptor, Some(manager));

    assert_eq!(
        collect_rows_i32_i32_string(&restored),
        vec![
            (1, 100, "after-restart".to_string()),
            (2, 200, "stable".to_string()),
        ]
    );
}

#[test]
fn partial_update_restart_with_meta_manager_rebuilds_primary_key_row_visibility() {
    let tmp = TempDir::new().unwrap();
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(tmp.path().join("meta")).unwrap());
    let manager = Arc::new(TabletMetaManager::new(
        store,
        Arc::new(GlobalSchemaMap::new()),
    ));
    let table = create_table_with_keys_with_meta_manager(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        KeysType::PrimaryKeys,
        Some(Arc::clone(&manager)),
    );

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[100, 200]),
            test_string_vector(&["before-restart", "stable"]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    let updated = table
        .update_direct(
            &read_view(&table),
            &[row_ids[&1]],
            &[2],
            &[vec![Value::Varchar("after-restart".to_string())]],
        )
        .unwrap();
    assert_eq!(updated, 1);

    // The metadata manager represents a checkpoint image. Direct test writes
    // bypass the database journal, so publish the checkpoint explicitly before
    // exercising provenance-gated primary-index recovery.
    table.tablet().persist_meta_snapshot().unwrap();
    let descriptor = table.to_descriptor().unwrap();
    let restored =
        open_table_from_descriptor_with_meta_manager(table.types(), &descriptor, Some(manager));

    assert_eq!(
        collect_rows_i32_i32_string(&restored),
        vec![
            (1, 100, "after-restart".to_string()),
            (2, 200, "stable".to_string()),
        ]
    );
}

#[test]
fn append_and_scan_varchar_roundtrip() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    let chunk = test_chunk_from_vectors(vec![
        test_i32_vector(&[1, 2]),
        test_string_vector(&["alice", "bob"]),
    ]);
    table.append(&chunk).unwrap();

    let mut rows = Vec::new();
    for chunk in table.scan_chunks().unwrap() {
        let id_col = chunk.column(0).unwrap();
        let name_col = chunk.column(1).unwrap();
        for idx in 0..chunk.size() {
            rows.push((
                id_col.get_i32(idx).unwrap(),
                name_col.get_string(idx).unwrap().to_string(),
            ));
        }
    }
    rows.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(rows, vec![(1, "alice".to_string()), (2, "bob".to_string())]);
}

#[test]
fn delete_update_roundtrip_preserves_latest_rows() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2, 3]),
            test_i32_vector(&[10, 20, 30]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);

    let updated = table
        .update_direct(
            &read_view(&table),
            &[row_ids[&2]],
            &[1],
            &[vec![Value::Integer(200)]],
        )
        .unwrap();
    assert_eq!(updated, 1);

    let deleted = table
        .delete_direct(&read_view(&table), &[row_ids[&1]])
        .unwrap();
    assert_eq!(deleted, 1);

    // Re-resolve row_id after DML to avoid stale physical references.
    let row_ids_after = collect_row_ids_by_id(&table);
    let updated_again = table
        .update_direct(
            &read_view(&table),
            &[row_ids_after[&3]],
            &[1],
            &[vec![Value::Integer(330)]],
        )
        .unwrap();
    assert_eq!(updated_again, 1);

    assert_eq!(collect_rows_i32_pair(&table), vec![(2, 200), (3, 330)]);
}

#[test]
fn vector_search_filters_rows_deleted_by_primary_keys() {
    let table = create_hnsw_table(2);

    let ids: Vec<i32> = (0..100).collect();
    let embeddings: Vec<Vec<f32>> = ids.iter().map(|id| vec![*id as f32, 0.0]).collect();
    table
        .append(&chunk_with_embeddings(&ids, &embeddings, 2))
        .unwrap();

    let delete_ids: Vec<i32> = (0..50).collect();
    let removed = table
        .delete_by_primary_keys_direct(
            &read_view(&table),
            &test_chunk_from_vectors(vec![test_i32_vector(&delete_ids)]),
        )
        .unwrap();
    assert_eq!(removed, 50);

    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    let chunks = table
        .vector_search(1, &[99.0, 0.0], 80, &params, None, &[0])
        .unwrap();

    let mut result_ids = Vec::new();
    for chunk in chunks {
        let id_col = chunk.column(0).unwrap();
        for row in 0..chunk.size() {
            result_ids.push(id_col.get_i32(row).unwrap());
        }
    }

    assert!(
        result_ids.len() <= 50,
        "expected at most 50 live rows after delete, got {}",
        result_ids.len()
    );
    assert!(
        result_ids.iter().all(|id| *id >= 50),
        "deleted ids should not appear in vector search results: {:?}",
        result_ids
    );
}

#[test]
fn hnsw_capability_exposes_generation_level_provider_stats() {
    let table = create_hnsw_table(2);
    table
        .append(&chunk_with_embeddings(
            &[1, 2, 3, 4, 5],
            &[
                vec![0.0_f32, 0.0],
                vec![1.0_f32, 0.0],
                vec![0.0_f32, 1.0],
                vec![1.0_f32, 1.0],
                vec![2.0_f32, 2.0],
            ],
            2,
        ))
        .unwrap();
    table.bind_search_task_scheduler(Some(Arc::new(TaskScheduler::new())));
    table
        .bootstrap_search_generations()
        .expect("materialize deferred HNSW generation");

    let capability = table
        .vector_capability(1, DistanceMetric::Euclidean)
        .expect("hnsw capability");
    let SearchProviderStats::Hnsw(provider_stats) = capability
        .generation_stats
        .provider_stats
        .clone()
        .expect("hnsw provider stats")
    else {
        panic!("expected hnsw provider stats");
    };
    assert_eq!(provider_stats.vector_count, 5);
    assert_eq!(provider_stats.dimension, 2);
    assert_eq!(provider_stats.m, 8);
    assert_eq!(provider_stats.ef_construction, 64);
    assert!(provider_stats.graph_memory_bytes > 0);
    assert_eq!(
        provider_stats.vector_storage_bytes,
        5 * 2 * std::mem::size_of::<f32>() as u64
    );
    assert!(provider_stats.total_graph_links >= provider_stats.level0_graph_links);
    assert!(provider_stats.avg_level0_degree >= 0.0);
}

#[test]
fn vector_search_returns_updated_vector_after_update() {
    let table = create_hnsw_table(2);
    let ids = [1, 2];
    let embeddings = vec![vec![10.0_f32, 0.0], vec![0.0_f32, 10.0]];
    table
        .append(&chunk_with_embeddings(&ids, &embeddings, 2))
        .unwrap();

    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    let warmup = table
        .vector_search(1, &[10.0, 0.0], 1, &params, None, &[0])
        .unwrap();
    let mut warmup_ids = Vec::new();
    for chunk in warmup {
        let id_col = chunk.column(0).unwrap();
        for row in 0..chunk.size() {
            warmup_ids.push(id_col.get_i32(row).unwrap());
        }
    }
    assert_eq!(warmup_ids, vec![1], "warmup should hit original vector");

    let row_ids = collect_row_ids_by_id(&table);
    let updated = table
        .update_direct(
            &read_view(&table),
            &[row_ids[&1]],
            &[1],
            &[vec![Value::Array(
                vec![Value::Float(0.0), Value::Float(10.0)],
                LogicalType::Float,
                2,
            )]],
        )
        .unwrap();
    assert_eq!(updated, 1);

    let chunks = table
        .vector_search(1, &[10.0, 0.0], 3, &params, None, &[0, 1])
        .unwrap();

    let mut saw_id_1 = false;
    let mut saw_old_vector = false;
    for chunk in chunks {
        let id_col = chunk.column(0).unwrap();
        let vec_col = chunk.column(1).unwrap();
        let child = vec_col.child().expect("array child");
        for row in 0..chunk.size() {
            let x = child.get_f32(row * 2).unwrap();
            let y = child.get_f32(row * 2 + 1).unwrap();
            if (x - 10.0).abs() < 1e-6 && y.abs() < 1e-6 {
                saw_old_vector = true;
            }
            if id_col.get_i32(row) == Some(1) {
                saw_id_1 = true;
                assert!(
                    x.abs() < 1e-6 && (y - 10.0).abs() < 1e-6,
                    "id=1 should return updated vector [0.0, 10.0], got [{x}, {y}]"
                );
            }
        }
    }

    assert!(saw_id_1, "expected updated id=1 to appear in top-k results");
    assert!(
        !saw_old_vector,
        "stale deleted vector [10.0, 0.0] should not appear in search results"
    );
}

#[test]
fn vector_search_parallel_matches_sequential_topk() {
    let table = create_hnsw_table(2);

    for batch in 0..4 {
        let start = batch * 50;
        let ids: Vec<i32> = (start..start + 50).collect();
        let embeddings: Vec<Vec<f32>> = ids.iter().map(|id| vec![*id as f32, 0.0_f32]).collect();
        table
            .append(&chunk_with_embeddings(&ids, &embeddings, 2))
            .unwrap();
    }

    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    let query = [173.25_f32, 0.0_f32];
    let projected = [0usize];

    let parallel_chunks =
        run_vector_cursor_with_slots(&table, 1, &query, 25, params, None, &projected, 4).unwrap();
    let sequential_chunks =
        run_vector_cursor_with_slots(&table, 1, &query, 25, params, None, &projected, 1).unwrap();

    let extract_ids = |chunks: &[Chunk]| {
        let mut ids = Vec::new();
        for chunk in chunks {
            let id_col = chunk.column(0).unwrap();
            for row in 0..chunk.size() {
                ids.push(id_col.get_i32(row).unwrap());
            }
        }
        ids
    };

    assert_eq!(
        extract_ids(&parallel_chunks),
        extract_ids(&sequential_chunks)
    );
}

#[test]
fn vector_search_many_parallel_matches_repeated_single_search() {
    let table = create_hnsw_table(2);

    for batch in 0..4 {
        let start = batch * 50;
        let ids: Vec<i32> = (start..start + 50).collect();
        let embeddings: Vec<Vec<f32>> = ids.iter().map(|id| vec![*id as f32, 0.0_f32]).collect();
        table
            .append(&chunk_with_embeddings(&ids, &embeddings, 2))
            .unwrap();
    }

    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    let queries = [
        vec![173.25_f32, 0.0_f32],
        vec![12.5_f32, 0.0_f32],
        vec![88.75_f32, 0.0_f32],
    ];
    let query_refs = queries.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let projected = [0usize];

    let parallel = table
        .vector_search_many(1, &query_refs, 12, &params, None, &projected)
        .unwrap();
    let sequential = query_refs
        .iter()
        .map(|query| {
            run_vector_cursor_with_slots(&table, 1, query, 12, params, None, &projected, 1)
        })
        .collect::<paro_common::error::Result<Vec<_>>>()
        .unwrap();

    let extract_ids = |chunks: &[Chunk]| {
        let mut ids = Vec::new();
        for chunk in chunks {
            let id_col = chunk.column(0).unwrap();
            for row in 0..chunk.size() {
                ids.push(id_col.get_i32(row).unwrap());
            }
        }
        ids
    };

    for ((parallel_chunks, sequential_chunks), query) in
        parallel.iter().zip(sequential.iter()).zip(queries.iter())
    {
        let single = table
            .vector_search(1, query, 12, &params, None, &projected)
            .unwrap();
        assert_eq!(extract_ids(parallel_chunks), extract_ids(sequential_chunks));
        assert_eq!(extract_ids(parallel_chunks), extract_ids(&single));
    }
}

#[test]
fn vector_search_materializes_varlen_and_null_columns() {
    let table = create_hnsw_table_with_note(2);
    table
        .append(&chunk_with_embeddings_and_notes(
            &[1, 2],
            &[vec![10.0_f32, 0.0], vec![9.0_f32, 0.0]],
            &[Some("alpha"), None],
            2,
        ))
        .unwrap();

    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    let chunks = table
        .vector_search(1, &[10.0, 0.0], 2, &params, None, &[0, 2])
        .unwrap();

    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0];
    let id_col = chunk.column(0).unwrap();
    let note_col = chunk.column(1).unwrap();

    assert_eq!(chunk.size(), 2);
    assert_eq!(id_col.get_i32(0), Some(1));
    assert_eq!(note_col.get_string(0), Some("alpha"));
    assert_eq!(id_col.get_i32(1), Some(2));
    assert_eq!(note_col.get_string(1), None);
    assert!(note_col.is_null(1));
}

#[test]
fn vector_search_for_view_filters_overlay_deleted_rows() {
    let table = create_hnsw_table(2);
    table
        .append(&chunk_with_embeddings(
            &[1, 2, 3],
            &[vec![10.0_f32, 0.0], vec![9.0_f32, 0.0], vec![8.0_f32, 0.0]],
            2,
        ))
        .unwrap();

    let params = SearchParams {
        ef: Some(64),
        ..Default::default()
    };
    let outside_txn = table
        .vector_search(1, &[10.0, 0.0], 1, &params, None, &[0])
        .unwrap();
    assert_eq!(collect_i32_column(&outside_txn, 0), vec![1]);

    let row_ids = collect_row_ids_by_id(&table);
    let txn = Arc::new(Transaction::new(4101, table.max_version() as u64 + 1));
    let deleted = table
        .delete(&read_view(&table), &[row_ids[&1]], txn.clone())
        .unwrap();
    assert_eq!(deleted, 1);
    txn.publish_command_boundary(CommandId::new(1));
    let view = transaction_view_for_command(&txn, 1);

    let opened = table
        .open_vector_search_cursor_for_view(
            1,
            &[10.0, 0.0],
            DistanceMetric::Euclidean,
            3,
            params,
            None,
            &view,
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("open vector search cursor for txn view");
    let chunks = drain_search_cursor(&table, opened, &[0], false, 3, 1)
        .expect("drain txn view search cursor");

    assert_eq!(collect_i32_column(&chunks, 0), vec![2, 3]);
}

#[test]
fn vector_search_keeps_delete_vector_touches_bounded_per_segment() {
    let table = create_hnsw_table(2);

    let ids: Vec<i32> = (0..120).collect();
    let embeddings: Vec<Vec<f32>> = ids.iter().map(|id| vec![*id as f32, 0.0_f32]).collect();
    table
        .append(&chunk_with_embeddings(&ids, &embeddings, 2))
        .unwrap();

    let version = table.max_version();
    let rowsets = table
        .tablet()
        .capture_consistent_rowsets(version)
        .expect("capture rowsets");

    let mut segments = Vec::new();
    for rowset in &rowsets {
        rowset.load().expect("load rowset");
        segments.extend(rowset.segments());
    }
    assert!(!segments.is_empty(), "expected at least one segment");
    for segment in &segments {
        segment.reset_delete_vector_load_requests_for_test();
    }

    let params = SearchParams {
        ef: Some(128),
        ..Default::default()
    };
    table
        .vector_search(1, &[60.0, 0.0], 10, &params, None, &[0])
        .unwrap();

    let total_calls: u64 = segments
        .iter()
        .map(|segment| segment.delete_vector_load_requests_for_test())
        .sum();
    assert!(
        total_calls as usize >= segments.len(),
        "vector search should touch each segment delete vector at least once"
    );
    assert!(
        total_calls as usize <= segments.len() * 2,
        "generation snapshot plus execution should keep delete-vector touches bounded to two per segment"
    );
}

#[test]
fn vector_column_from_specs_requires_an_explicit_search_definition() {
    let specs = vec![
        TableColumnSpec {
            name: "id".to_string(),
            logical_type: LogicalType::Integer,
            is_key: true,
            not_null: true,
        },
        TableColumnSpec {
            name: "emb".to_string(),
            logical_type: LogicalType::Array(Box::new(LogicalType::Float), 2),
            is_key: false,
            not_null: false,
        },
    ];
    let table = create_table_from_specs(&specs);
    let schema = table.tablet().schema().unwrap();
    assert!(!schema.column_by_id(1).unwrap().index_hnsw);
    let chunk = chunk_with_embeddings(
        &[1, 2, 3],
        &[vec![1.0_f32, 0.0], vec![2.0_f32, 0.0], vec![3.0_f32, 0.0]],
        2,
    );
    table.append(&chunk).unwrap();

    let visible = table.max_version();
    let rowsets = table.tablet().capture_consistent_rowsets(visible).unwrap();
    assert!(!rowsets.is_empty(), "expected at least one rowset");

    for rowset in rowsets {
        rowset.load().unwrap();
        for segment in rowset.segments() {
            assert!(segment.hnsw_index(1).is_none());
        }
    }
}

#[test]
fn full_table_delete_non_pk_marks_all_rows() {
    let table = create_table(&[LogicalType::Integer]);
    let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2, 3])]);
    table.append(&chunk).unwrap();

    let deleted = table.delete_all_direct(&read_view(&table)).unwrap();
    assert_eq!(deleted, 3);

    let visible_rows: usize = table.scan_chunks().unwrap().iter().map(|c| c.size()).sum();
    assert_eq!(visible_rows, 0);

    let deleted_again = table.delete_all_direct(&read_view(&table)).unwrap();
    assert_eq!(deleted_again, 0);
}

#[test]
fn full_table_delete_primary_key_clears_index() {
    let table = create_table_with_keys(
        &[LogicalType::Integer, LogicalType::Integer],
        KeysType::PrimaryKeys,
    );
    let chunk = test_chunk_from_vectors(vec![
        test_i32_vector(&[1, 2, 3]),
        test_i32_vector(&[10, 20, 30]),
    ]);
    table.append(&chunk).unwrap();
    assert_eq!(
        table
            .tablet()
            .snapshot_primary_index_entries()
            .unwrap()
            .len(),
        3
    );

    let deleted = table.delete_all_direct(&read_view(&table)).unwrap();
    assert_eq!(deleted, 3);
    assert_eq!(
        table
            .tablet()
            .snapshot_primary_index_entries()
            .unwrap()
            .len(),
        0
    );

    let visible_rows: usize = table.scan_chunks().unwrap().iter().map(|c| c.size()).sum();
    assert_eq!(visible_rows, 0);

    let deleted_again = table.delete_all_direct(&read_view(&table)).unwrap();
    assert_eq!(deleted_again, 0);
}

#[test]
fn transactional_delete_update_commit_applies_in_single_commit() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    let chunk = test_chunk_from_vectors(vec![
        test_i32_vector(&[1, 2, 3]),
        test_i32_vector(&[10, 20, 30]),
    ]);
    table.append(&chunk).unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    let txn = Arc::new(Transaction::new(1001, 1001));

    let updated_2 = table
        .update(
            &read_view(&table),
            &[row_ids[&2]],
            &[1],
            &[vec![Value::Integer(200)]],
            txn.clone(),
        )
        .unwrap();
    assert_eq!(updated_2, 1);

    let deleted_1 = table
        .delete(&read_view(&table), &[row_ids[&1]], txn.clone())
        .unwrap();
    assert_eq!(deleted_1, 1);

    let updated_3 = table
        .update(
            &read_view(&table),
            &[row_ids[&3]],
            &[1],
            &[vec![Value::Integer(300)]],
            txn.clone(),
        )
        .unwrap();
    assert_eq!(updated_3, 1);

    // Transactional writes are invisible before commit.
    assert_eq!(
        collect_rows_i32_pair(&table),
        vec![(1, 10), (2, 20), (3, 30)]
    );

    commit_transaction(&txn, 2001).unwrap();

    assert_eq!(collect_rows_i32_pair(&table), vec![(2, 200), (3, 300)]);
}

#[test]
fn transactional_delete_update_rollback_keeps_storage_unchanged() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    let chunk = test_chunk_from_vectors(vec![
        test_i32_vector(&[1, 2, 3]),
        test_i32_vector(&[10, 20, 30]),
    ]);
    table.append(&chunk).unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    let txn = Arc::new(Transaction::new(1002, 1002));

    let updated_2 = table
        .update(
            &read_view(&table),
            &[row_ids[&2]],
            &[1],
            &[vec![Value::Integer(220)]],
            txn.clone(),
        )
        .unwrap();
    assert_eq!(updated_2, 1);

    let deleted_1 = table
        .delete(&read_view(&table), &[row_ids[&1]], txn.clone())
        .unwrap();
    assert_eq!(deleted_1, 1);

    assert_eq!(
        collect_rows_i32_pair(&table),
        vec![(1, 10), (2, 20), (3, 30)]
    );

    txn.rollback().unwrap();

    assert_eq!(
        collect_rows_i32_pair(&table),
        vec![(1, 10), (2, 20), (3, 30)]
    );
}

#[test]
fn transactional_concurrent_delete_conflict_on_same_primary_key() {
    let table = create_table_with_keys(
        &[LogicalType::Integer, LogicalType::Integer],
        KeysType::PrimaryKeys,
    );
    let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2]), test_i32_vector(&[10, 20])]);
    table.append(&chunk).unwrap();

    let key_chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1])]);
    let txn1 = Arc::new(Transaction::new(3001, 3001));
    let txn2 = Arc::new(Transaction::new(3002, 3002));

    let removed = table
        .delete_by_primary_keys(&read_view(&table), &key_chunk, txn1.clone())
        .unwrap();
    assert_eq!(removed, 1);

    let err = table
        .delete_by_primary_keys(&read_view(&table), &key_chunk, txn2.clone())
        .unwrap_err();
    assert!(
        err.to_string().contains("write-write conflict"),
        "expected write-write conflict, got: {err}"
    );

    commit_transaction(&txn1, 4001).unwrap();
    assert_eq!(collect_rows_i32_pair(&table), vec![(2, 20)]);
}

#[test]
fn transactional_delete_and_update_conflict_on_same_row() {
    let table = create_table_with_keys(
        &[LogicalType::Integer, LogicalType::Integer],
        KeysType::PrimaryKeys,
    );
    let chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1, 2]), test_i32_vector(&[10, 20])]);
    table.append(&chunk).unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    let update_txn = Arc::new(Transaction::new(3101, 3101));
    let updated = table
        .update(
            &read_view(&table),
            &[row_ids[&1]],
            &[1],
            &[vec![Value::Integer(999)]],
            update_txn.clone(),
        )
        .unwrap();
    assert_eq!(updated, 1);

    let delete_txn = Arc::new(Transaction::new(3102, 3102));
    let key_chunk = test_chunk_from_vectors(vec![test_i32_vector(&[1])]);
    let err = table
        .delete_by_primary_keys(&read_view(&table), &key_chunk, delete_txn.clone())
        .unwrap_err();
    assert!(
        err.to_string().contains("write-write conflict"),
        "expected write-write conflict, got: {err}"
    );

    update_txn.rollback().unwrap();
}

#[test]
fn transactional_delete_survives_pk_compaction_relocation() {
    let table = create_table_with_keys(
        &[LogicalType::Integer, LogicalType::Integer],
        KeysType::PrimaryKeys,
    );

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
        ]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[3, 4]),
            test_i32_vector(&[30, 40]),
        ]))
        .unwrap();

    let key_chunk = test_chunk_from_vectors(vec![test_i32_vector(&[2])]);
    let txn = Arc::new(Transaction::new(3201, 3201));
    let removed = table
        .delete_by_primary_keys(&read_view(&table), &key_chunk, txn.clone())
        .unwrap();
    assert_eq!(removed, 1);
    let row_ids_before_compaction = collect_row_ids_by_id(&table);

    let visible_rowsets = table.tablet().capture_consistent_rowsets(i64::MAX).unwrap();
    assert!(
        !visible_rowsets.is_empty(),
        "expected at least one visible rowset before compaction"
    );
    let candidate_ids: Vec<u64> = visible_rowsets.iter().map(|rs| rs.rowset_id()).collect();
    let plan = CompactionPlanner::plan(table.tablet().as_ref())
        .unwrap()
        .expect("primary-key compaction plan");
    let mut task = HorizontalCompactionTask::new(table.tablet(), plan, test_allocator());
    task.run().unwrap();
    let visible_after: std::collections::HashSet<u64> = table
        .tablet()
        .capture_consistent_rowsets(i64::MAX)
        .unwrap()
        .into_iter()
        .map(|rowset| rowset.rowset_id())
        .collect();
    assert!(
        candidate_ids.iter().all(|id| !visible_after.contains(id)),
        "expected compaction to replace all candidate rowsets in the visible snapshot"
    );
    let row_ids_after_compaction = collect_row_ids_by_id(&table);
    assert_ne!(
        row_ids_before_compaction.get(&2),
        row_ids_after_compaction.get(&2),
        "expected compaction to relocate the staged delete target before commit"
    );

    commit_transaction(&txn, 4201).unwrap();
    assert_eq!(
        collect_rows_i32_pair(&table),
        vec![(1, 10), (3, 30), (4, 40)]
    );
}

#[test]
fn table_data_builds_primary_key_from_specs() {
    let specs = vec![
        TableColumnSpec {
            name: "id".to_string(),
            logical_type: LogicalType::Integer,
            is_key: true,
            not_null: true,
        },
        TableColumnSpec {
            name: "name".to_string(),
            logical_type: LogicalType::Varchar,
            is_key: false,
            not_null: false,
        },
    ];

    let table = create_table_from_specs(&specs);
    let schema = table.tablet().schema().unwrap();
    assert_eq!(schema.keys_type(), KeysType::PrimaryKeys);
    assert!(schema.column(0).unwrap().is_key);
    assert!(!schema.column(1).unwrap().is_key);
}

#[test]
fn table_data_builds_storage_descriptor() {
    let types = vec![LogicalType::Integer];
    let table = create_table_with_keys(&types, KeysType::PrimaryKeys);

    let descriptor = table.to_descriptor().unwrap();
    assert_eq!(descriptor.tablet_id, table.tablet().tablet_id());
    assert_eq!(descriptor.table_id, table.tablet().table_id());
    assert_eq!(descriptor.partition_id, table.tablet().partition_id());
    assert_eq!(descriptor.keys_type_enum().unwrap(), KeysType::PrimaryKeys);
    assert!(!descriptor.data_dir.is_empty());
}

#[test]
fn table_data_descriptor_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let store: Arc<dyn MetadataStore> =
        Arc::new(FileMetadataStore::new(tmp.path().join("meta")).unwrap());
    let manager = Arc::new(TabletMetaManager::new(
        store,
        Arc::new(GlobalSchemaMap::new()),
    ));
    let types = vec![LogicalType::Integer, LogicalType::BigInt];
    let table = create_table_with_keys_with_meta_manager(
        &types,
        KeysType::PrimaryKeys,
        Some(manager.clone()),
    );
    let descriptor = table.to_descriptor().unwrap();

    let restored = open_table_from_descriptor_with_meta_manager(&types, &descriptor, Some(manager));
    let restored_descriptor = restored.to_descriptor().unwrap();
    assert_eq!(restored_descriptor, descriptor);
}

#[test]
fn table_data_uses_stable_data_dir() {
    let table = create_table(&[LogicalType::Integer]);
    let descriptor = table.to_descriptor().unwrap();
    assert!(descriptor.data_dir.contains("table_"));
    assert!(descriptor.data_dir.contains("tablet_"));
    assert!(!descriptor.data_dir.contains("paro_tablet_"));
}

#[test]
fn art_index_build_and_remove_tracks_visible_segments() {
    let table = create_table(&[LogicalType::Integer]);
    let values = (0..16).collect::<Vec<i32>>();

    table
        .append(&test_chunk_from_vectors(vec![test_i32_vector(&values)]))
        .unwrap();

    let before_build = table.collect_segments(table.max_version()).unwrap();
    assert!(!before_build.is_empty());
    assert!(before_build
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_none()));

    table.declare_art_index("test_idx", 0);
    assert_eq!(table.declared_art_columns(), vec![0]);
    assert_eq!(table.tablet().declared_art_columns(), vec![0]);
    table.rebuild_art_index(0).unwrap();

    let after_build = table.collect_segments(table.max_version()).unwrap();
    assert!(after_build
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_some()));
    assert!(after_build
        .iter()
        .all(|(_, segment)| segment.bitmap_index(0).is_none()));

    table.release_art_index("test_idx", 0).unwrap();
    assert!(table.declared_art_columns().is_empty());
    assert!(table.tablet().declared_art_columns().is_empty());

    let after_remove = table.collect_segments(table.max_version()).unwrap();
    assert!(after_remove
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_none()));
    assert!(after_remove
        .iter()
        .all(|(_, segment)| segment.bitmap_index(0).is_none()));
}

#[test]
fn declared_scalar_index_selects_one_dense_posting_representation() {
    let table = create_table(&[LogicalType::Integer]);
    let values = (0..128).map(|value| value % 2).collect::<Vec<i32>>();
    table
        .append(&test_chunk_from_vectors(vec![test_i32_vector(&values)]))
        .unwrap();

    table.install_art_index("dense_idx", 0).unwrap();
    let segments = table.collect_segments(table.max_version()).unwrap();
    assert!(segments
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_none()));
    assert!(segments
        .iter()
        .all(|(_, segment)| segment.bitmap_index(0).is_some()));

    for (_, segment) in segments {
        let result = segment
            .bitmap_index(0)
            .unwrap()
            .evaluate_predicate(&Predicate::Eq {
                column_id: 0,
                value: Value::Integer(1),
            });
        let PredicateResult::Bitmap(rows) = result else {
            panic!("dense scalar access path must return an exact posting bitmap");
        };
        assert_eq!(rows.len(), segment.num_rows() / 2);
    }
}

#[test]
fn duplicate_art_declarations_release_only_the_last_physical_owner() {
    let table = create_table(&[LogicalType::Integer]);
    table
        .append(&test_chunk_from_vectors(vec![test_i32_vector(&[
            1, 2, 3, 4,
        ])]))
        .unwrap();

    table.install_art_index("idx_a", 0).unwrap();
    table.install_art_index("idx_a", 0).unwrap();
    table.install_art_index("idx_b", 0).unwrap();
    table.release_art_index("idx_a", 0).unwrap();

    assert!(table.has_declared_art_index(0));
    assert!(table
        .collect_segments(table.max_version())
        .unwrap()
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_some()));

    // A retry for the same owner is idempotent and cannot consume idx_b.
    table.release_art_index("idx_a", 0).unwrap();
    assert!(table.has_declared_art_index(0));
    table.release_art_index("idx_b", 0).unwrap();
    assert!(!table.has_declared_art_index(0));
    assert!(table
        .collect_segments(table.max_version())
        .unwrap()
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_none()));
}

#[test]
fn art_declared_index_auto_builds_for_inserts() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    table.declare_art_index("test_idx", 0);

    table.append(&chunk_with_i32_range(0, 4, 100)).unwrap();
    table.append(&chunk_with_i32_range(10, 14, 100)).unwrap();

    let segments = table.collect_segments(table.max_version()).unwrap();
    assert!(segments.len() >= 2);
    assert!(segments
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_some()));

    let target_value = Value::Integer(12);
    let target_segment = segments
        .iter()
        .find_map(|(_, segment)| {
            let art = segment.art_index(0)?;
            match art.evaluate_predicate(&Predicate::Eq {
                column_id: 0,
                value: target_value.clone(),
            }) {
                PredicateResult::Bitmap(bitmap) if !bitmap.is_empty() => Some(segment.clone()),
                _ => None,
            }
        })
        .expect("segment with ART match for inserted row");
    let art = target_segment.art_index(0).unwrap();
    assert!(matches!(
        art.evaluate_predicate(&Predicate::Eq {
            column_id: 0,
            value: Value::Integer(12),
        }),
        PredicateResult::Bitmap(_)
    ));
    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: target_value,
    });
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            target_segment.as_ref(),
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    assert!(!iter.uses_late_materialize());

    let (rowids, batch) = iter.next_batch(8).unwrap();
    assert_eq!(rowids, vec![2]);
    let value = i32::from_le_bytes(batch[0].1.data[0..4].try_into().unwrap());
    assert_eq!(value, 112);
}

#[test]
fn runtime_art_predicate_returns_all_duplicate_matches() {
    let table = create_table(&[
        LogicalType::Integer,
        LogicalType::Integer,
        LogicalType::Integer,
    ]);
    table.declare_art_index("test_idx", 1);

    let chunk = test_chunk_from_vectors(vec![
        test_i32_vector(&[1, 2, 3, 4]),
        test_i32_vector(&[10, 20, 20, 30]),
        test_i32_vector(&[100, 200, 300, 400]),
    ]);
    table.append(&chunk).unwrap();

    let segments = table.collect_segments(table.max_version()).unwrap();
    let (_, segment) = segments.first().expect("single visible segment");

    let art = segment
        .art_index(1)
        .expect("runtime ART on duplicate column");
    let bitmap = match art.evaluate_predicate(&Predicate::Eq {
        column_id: 1,
        value: Value::Integer(20),
    }) {
        PredicateResult::Bitmap(bitmap) => bitmap,
        other => panic!("expected bitmap predicate result, got {other:?}"),
    };
    assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![1, 2]);

    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 1,
        value: Value::Integer(20),
    });
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            segment.as_ref(),
            vec![0, 2],
            vec![1],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    assert!(!iter.uses_late_materialize());

    let (rowids, batch) = iter.next_batch(8).unwrap();
    assert_eq!(rowids, vec![1, 2]);

    let ids = batch[0]
        .1
        .data
        .chunks_exact(std::mem::size_of::<i32>())
        .map(|bytes| i32::from_le_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    let payloads = batch[1]
        .1
        .data
        .chunks_exact(std::mem::size_of::<i32>())
        .map(|bytes| i32::from_le_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![2, 3]);
    assert_eq!(payloads, vec![200, 300]);
}

#[test]
fn art_declared_index_auto_builds_on_transaction_commit() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    table.declare_art_index("test_idx", 0);

    let txn = Arc::new(Transaction::new(9101, 9101));
    table
        .append_with_transaction(
            &read_view(&table),
            &chunk_with_i32_range(20, 24, 200),
            txn.clone(),
        )
        .unwrap();

    assert!(table
        .collect_segments(table.max_version())
        .unwrap()
        .is_empty());

    commit_transaction(&txn, 9102).unwrap();

    let segments = table.collect_segments(table.max_version()).unwrap();
    assert!(!segments.is_empty());
    assert!(segments
        .iter()
        .all(|(_, segment)| segment.art_index(0).is_some()));

    let target_value = Value::Integer(22);
    let target_segment = segments
        .iter()
        .find_map(|(_, segment)| {
            let art = segment.art_index(0)?;
            match art.evaluate_predicate(&Predicate::Eq {
                column_id: 0,
                value: target_value.clone(),
            }) {
                PredicateResult::Bitmap(bitmap) if !bitmap.is_empty() => Some(segment.clone()),
                _ => None,
            }
        })
        .expect("segment with ART match for transaction row");
    let art = target_segment.art_index(0).unwrap();
    assert!(matches!(
        art.evaluate_predicate(&Predicate::Eq {
            column_id: 0,
            value: Value::Integer(22),
        }),
        PredicateResult::Bitmap(_)
    ));
    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: target_value,
    });
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            target_segment.as_ref(),
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    assert!(!iter.uses_late_materialize());

    let (rowids, batch) = iter.next_batch(8).unwrap();
    assert_eq!(rowids, vec![2]);
    let value = i32::from_le_bytes(batch[0].1.data[0..4].try_into().unwrap());
    assert_eq!(value, 222);
}

#[test]
fn art_backfill_failure_does_not_block_insert_or_scan_fallback() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    table.declare_art_index("test_idx", 99);

    table.append(&chunk_with_i32_range(0, 4, 100)).unwrap();

    let segments = table.collect_segments(table.max_version()).unwrap();
    assert_eq!(segments.len(), 1);
    assert!(segments[0].1.art_index(0).is_none());
    assert!(segments[0].1.art_index(99).is_none());
    assert_eq!(
        collect_rows_i32_pair(&table),
        vec![(0, 100), (1, 101), (2, 102), (3, 103)]
    );

    let predicate = PredicateTree::leaf(Predicate::Eq {
        column_id: 0,
        value: Value::Integer(2),
    });
    let mut iter =
        SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
            segments[0].1.as_ref(),
            vec![1],
            vec![0],
            None,
            Some(predicate),
            None,
        )
        .unwrap();

    assert!(iter.uses_late_materialize());

    let (rowids, batch) = iter.next_batch(8).unwrap();
    assert_eq!(rowids, vec![2]);
    let value = i32::from_le_bytes(batch[0].1.data[0..4].try_into().unwrap());
    assert_eq!(value, 102);
}

#[test]
fn fulltext_generation_contributes_to_segment_index_statistics() {
    let table = create_table(&[LogicalType::Varchar]);
    register_fulltext_definition(&table, 0, "simple");
    table
        .append(&test_chunk_from_vectors(vec![test_string_vector(&[
            "vector alpha",
            "vector beta",
        ])]))
        .unwrap();

    let segments = table.collect_segments(table.max_version()).unwrap();
    assert!(!segments.is_empty());

    let mut saw_fulltext_stats = false;
    for (_, segment) in segments {
        if let Some(stats) = segment.index_statistics().column(0) {
            if let Some(fulltext) = stats
                .iter()
                .find(|stat| stat.index_type == IndexType::FullText)
            {
                assert!(fulltext.index_size_bytes > 0);
                assert!(fulltext.entry_count > 0);
                saw_fulltext_stats = true;
            }
        }
    }

    assert!(saw_fulltext_stats);
}

#[test]
fn fulltext_definition_on_empty_table_installs_empty_queryable_generation() {
    let table = create_table(&[LogicalType::Varchar]);
    let definition_id = register_fulltext_definition(&table, 0, "simple");

    let capability = table
        .fulltext_capability(0, "simple")
        .expect("empty fulltext capability");
    assert!(capability.coverage.is_complete());
    assert_eq!(capability.generation_stats.indexed_rows, 0);
    let SearchProviderStats::FullText(provider_stats) = capability
        .generation_stats
        .provider_stats
        .clone()
        .expect("empty generation provider stats")
    else {
        panic!("expected fulltext provider stats");
    };
    assert_eq!(provider_stats.total_docs, 0);
    assert_eq!(provider_stats.total_terms, 0);

    let snapshot = table
        .open_search_generation_snapshot(definition_id)
        .expect("open empty generation snapshot")
        .expect("empty generation snapshot");
    assert!(snapshot.coverage.is_complete());
    assert!(snapshot.artifacts.artifacts.is_empty());
    assert_eq!(
        snapshot.maintenance_state.build_watermarks.snapshot_version,
        table.max_version()
    );
    assert_eq!(snapshot.generation_stats.indexed_rows, 0);
    assert_eq!(
        snapshot.maintenance_state.recovery.priority,
        crate::search::MaintenancePriority::Idle
    );
    assert_eq!(
        snapshot.indexed_through_ts,
        u64::try_from(table.max_version()).unwrap_or(0)
    );
}

#[test]
fn search_snapshot_pins_derived_delta_when_generation_lags_read_ts() {
    let table = create_table(&[LogicalType::Varchar]);
    let definition_id = register_fulltext_definition(&table, 0, "simple");
    let target_version = table.max_version().saturating_add(10);
    let capability = table
        .fulltext_capability(0, "simple")
        .expect("fulltext capability");
    assert_eq!(capability.definition_id, definition_id);

    let snapshot = table
        .open_search_snapshot(
            &capability,
            target_version,
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("open lagging search snapshot");
    let lease_info = snapshot
        .derived_lag_lease_info()
        .expect("derived lag lease info")
        .expect("derived lag lease");

    assert_eq!(lease_info.kind, RetentionLeaseKind::DerivedLag);
    assert_eq!(
        lease_info.commit_ts_floor,
        Some(CommitTs::new(snapshot.generation.indexed_through_ts))
    );
    assert_eq!(
        lease_info.commit_ts_ceiling,
        Some(CommitTs::new(target_version as u64))
    );
}

#[test]
fn fulltext_capability_exposes_generation_level_provider_stats() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    register_fulltext_definition(&table, 1, "simple");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2, 3]),
            test_string_vector(&["vector alpha", "vector beta", "noise"]),
        ]))
        .unwrap();

    let capability = table
        .fulltext_capability(1, "simple")
        .expect("fulltext capability after append");
    let provider_stats = capability
        .generation_stats
        .fulltext_provider_stats()
        .expect("generation-level fulltext stats");
    assert_eq!(provider_stats.total_docs, 3);
    assert_eq!(provider_stats.total_terms, 5);
    assert_eq!(provider_stats.tokenizer, "simple");
}

#[test]
fn search_maintenance_pass_reports_tombstone_pressure() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    register_fulltext_definition(&table, 1, "simple");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_string_vector(&["vector alpha", "vector beta"]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    table
        .delete_direct(&read_view(&table), &[row_ids[&1]])
        .unwrap();
    table
        .delete_direct(&read_view(&table), &[row_ids[&2]])
        .unwrap();

    let report = table.run_search_maintenance_pass().unwrap();
    assert!(report.compaction_requested);
    assert!(report.definitions.iter().any(|definition| {
        matches!(definition.action, SearchMaintenanceAction::Rebuild)
            || matches!(definition.action, SearchMaintenanceAction::Compact)
    }));
}

#[test]
fn generation_snapshot_tracks_build_epoch_and_superseded_epochs() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    let definition_id = register_fulltext_definition(&table, 1, "simple");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1]),
            test_string_vector(&["vector alpha"]),
        ]))
        .unwrap();
    let first = table
        .open_search_generation_snapshot(definition_id)
        .unwrap()
        .expect("first generation snapshot");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[2]),
            test_string_vector(&["vector beta"]),
        ]))
        .unwrap();
    let second = table
        .open_search_generation_snapshot(definition_id)
        .unwrap()
        .expect("second generation snapshot");

    assert!(second.build_epoch > first.build_epoch);
    assert!(second
        .maintenance_state
        .recovery
        .superseded_build_epochs
        .contains(&first.build_epoch));
    assert_eq!(
        second.maintenance_state.build_watermarks.cutover_watermark,
        table.max_version()
    );
}

#[test]
fn fulltext_registry_definition_auto_builds_for_inserts() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    let definition_id = register_fulltext_definition(&table, 1, "simple");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_string_vector(&["vector alpha", "noise"]),
        ]))
        .unwrap();

    let cov_after_first = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after first append");
    assert!(cov_after_first.is_complete());

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[3, 4]),
            test_string_vector(&["vector beta", "other"]),
        ]))
        .unwrap();

    let cov_after_second = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after second append");
    assert!(cov_after_second.visible_segment_count >= 2);
    assert!(cov_after_second.is_complete());

    for (_, segment) in table.collect_segments(table.max_version()).unwrap() {
        let meta = segment.get_column_meta(1).expect("text column meta");
        assert!(
            meta.fulltext_index_pointer.is_some(),
            "insert path should materialize durable fulltext payload"
        );
    }

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let chunks = table.fulltext_filter(1, &query, None, &[0]).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![1, 3]);
}

#[test]
fn fulltext_registry_definition_builds_with_chinese_tokenizer() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    let definition_id = register_fulltext_definition(&table, 1, "chinese");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_string_vector(&["向量数据库", "分布式系统"]),
        ]))
        .unwrap();

    let cov = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage with chinese tokenizer");
    assert!(cov.is_complete());
    assert!(table.fulltext_capability(1, "chinese").is_some());
    assert!(table.fulltext_capability(1, "japanese").is_none());

    let chinese_query = FullTextIndex::new_with_tokenizer_kind(
        TokenizerKind::Chinese,
        FullTextIndexConfig::default(),
    )
    .parse_query("数据库")
    .unwrap();
    let chunks = table
        .fulltext_filter(1, &chinese_query, None, &[0])
        .unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![1]);
}

#[test]
fn fulltext_registry_definition_auto_builds_on_transaction_commit() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    let definition_id = register_fulltext_definition(&table, 1, "simple");

    let txn = Arc::new(Transaction::new(9001, 9001));
    table
        .append_with_transaction(
            &read_view(&table),
            &test_chunk_from_vectors(vec![
                test_i32_vector(&[10, 11]),
                test_string_vector(&["vector txn", "noise txn"]),
            ]),
            txn.clone(),
        )
        .unwrap();

    // Pending rowset is transaction-private before commit.
    let cov_before_commit = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage before commit");
    assert_eq!(cov_before_commit.visible_segment_count, 0);
    assert_eq!(cov_before_commit.indexed_segment_count, 0);

    commit_transaction(&txn, 9002).unwrap();
    let cov_after_commit = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after commit");
    assert!(cov_after_commit.visible_segment_count >= 1);
    assert!(cov_after_commit.is_complete());

    for (_, segment) in table.collect_segments(table.max_version()).unwrap() {
        let meta = segment.get_column_meta(1).expect("text column meta");
        assert!(
            meta.fulltext_index_pointer.is_some(),
            "transaction commit should publish durable fulltext payload"
        );
    }

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let chunks = table.fulltext_filter(1, &query, None, &[0]).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![10]);
}

#[test]
fn sparse_registry_definition_auto_builds_for_inserts() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Blob]);
    register_sparse_definition(&table, 1);

    let v1 = SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap();
    let v2 = SparseVector::new(vec![2], vec![1.0]).unwrap();
    let v3 = SparseVector::new(vec![1, 2], vec![0.7, 0.2]).unwrap();
    let v4 = SparseVector::new(vec![4], vec![1.0]).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_sparse_blob_vector(&[v1, v2]),
        ]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[3, 4]),
            test_sparse_blob_vector(&[v3, v4]),
        ]))
        .unwrap();

    assert!(table.sparse_capability(1).is_some());
    for (_, segment) in table.collect_segments(table.max_version()).unwrap() {
        let meta = segment.get_column_meta(1).expect("sparse column meta");
        assert!(
            meta.sparse_index_pointer.is_some(),
            "insert path should materialize durable sparse payload"
        );
    }

    let query = SparseVector::parse("{1:1.0}").unwrap();
    let chunks = sparse_search(&table, 1, &query, 10, &[0]).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![1, 3]);
}

#[test]
fn sparse_registry_rejects_varchar_sparse_input() {
    let table = create_table(&[LogicalType::Varchar]);
    let definition_id = NEXT_TEST_SEARCH_DEFINITION_ID.fetch_add(1, AtomicOrdering::Relaxed);
    let provider_config = json!({
        "version": 1,
        "physical_encoding": "binary-v1",
    });
    let expression = "sparse_vector(col_0)".to_string();
    let definition = SearchIndexDefinition {
        definition_id,
        table_id: table.tablet().table_id(),
        name: format!("__test_sparse_reject_{}", definition_id),
        kind: SearchIndexKind::Sparse,
        column_ids: vec![0],
        expression: Some(expression.clone()),
        freshness_policy: SearchFreshnessPolicy::default_for_kind(SearchIndexKind::Sparse),
        config_fingerprint: SearchIndexDefinition::compute_config_fingerprint(
            SearchIndexKind::Sparse,
            &[0],
            Some(&expression),
            &provider_config,
        ),
        provider_config,
    };

    let err = table
        .register_search_definition(definition)
        .expect_err("default sparse encoding should reject Varchar input");
    assert!(
        err.to_string()
            .contains("Blob binary sparse row image column"),
        "unexpected sparse validation error: {err}"
    );
}

#[test]
fn sparse_registry_rejects_textual_blob_payload_during_inline_build() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Blob]);
    register_sparse_definition(&table, 1);

    let mut textual_blob =
        Vector::try_new(LogicalType::Blob, 1, test_allocator()).expect("blob vector allocation");
    textual_blob.set_blob(0, b"{1:1.0}");
    textual_blob.set_count(1);

    let err = table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1]),
            textual_blob,
        ]))
        .expect_err("Sparse Blob payload must be a typed binary row image, not text");
    assert!(
        err.to_string()
            .contains("Sparse binary row image decode failed"),
        "unexpected sparse text payload error: {err}"
    );
}

#[test]
fn sparse_capability_exposes_generation_level_provider_stats() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Blob]);
    register_sparse_definition(&table, 1);

    let v1 = SparseVector::new(vec![1, 2], vec![3.0, 4.0]).unwrap();
    let v2 = SparseVector::new(vec![2], vec![2.0]).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_sparse_blob_vector(&[v1, v2]),
        ]))
        .unwrap();

    let capability = table.sparse_capability(1).expect("sparse capability");
    let SearchProviderStats::Sparse(provider_stats) = capability
        .generation_stats
        .provider_stats
        .clone()
        .expect("sparse provider stats")
    else {
        panic!("expected sparse provider stats");
    };
    assert_eq!(provider_stats.row_count, 2);
    assert_eq!(provider_stats.nnz, 3);
    assert_eq!(provider_stats.posting_fanout, 3);
    assert_eq!(provider_stats.unique_dimensions, 2);
    assert!((provider_stats.avg_vector_nnz - 1.5).abs() < 1e-6);
    assert!((provider_stats.l2_norm_sum - 7.0).abs() < 1e-6);
    assert_eq!(provider_stats.max_l2_norm, 5.0);
}

#[test]
fn sparse_registry_definition_auto_builds_on_transaction_commit() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Blob]);
    register_sparse_definition(&table, 1);

    let txn = Arc::new(Transaction::new(9101, 9101));
    let v1 = SparseVector::new(vec![5], vec![1.0]).unwrap();
    let v2 = SparseVector::new(vec![2, 5], vec![0.6, 0.4]).unwrap();
    table
        .append_with_transaction(
            &read_view(&table),
            &test_chunk_from_vectors(vec![
                test_i32_vector(&[10, 11]),
                test_sparse_blob_vector(&[v1, v2]),
            ]),
            txn.clone(),
        )
        .unwrap();

    assert!(table.sparse_capability(1).is_some());
    commit_transaction(&txn, 9102).unwrap();
    assert!(table.sparse_capability(1).is_some());

    for (_, segment) in table.collect_segments(table.max_version()).unwrap() {
        let meta = segment.get_column_meta(1).expect("sparse column meta");
        assert!(
            meta.sparse_index_pointer.is_some(),
            "transaction commit should publish durable sparse payload"
        );
    }

    let query = SparseVector::parse("{5:1.0}").unwrap();
    let chunks = sparse_search(&table, 1, &query, 10, &[0]).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![10, 11]);
}

#[test]
fn fulltext_late_definition_bootstrap_publishes_sidecar_generation() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_string_vector(&["vector one", "noise"]),
        ]))
        .unwrap();
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[3, 4]),
            test_string_vector(&["vector two", "other"]),
        ]))
        .unwrap();

    let definition_id = register_fulltext_definition(&table, 1, "simple");

    let coverage_before = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage before bootstrap");
    assert_eq!(coverage_before.visible_segment_count, 2);
    assert_eq!(coverage_before.indexed_segment_count, 0);
    assert!(!coverage_before.is_complete());
    assert!(matches!(
        coverage_before.coverage,
        CoverageState::TailPending {
            pending_segments: 2,
            exact_tail_merge: true,
            ..
        }
    ));
    let capability_before = table
        .fulltext_capability(1, "simple")
        .expect("fulltext capability before bootstrap");
    assert_eq!(capability_before.tail_summary.pending_rowsets, 2);
    assert_eq!(capability_before.tail_summary.pending_segments, 2);
    assert_eq!(capability_before.tail_summary.pending_rows, 4);
    assert!(capability_before.tail_summary.pending_bytes > 0);
    assert!(capability_before.tail_summary.exact_tail_merge);

    let snapshot_before = table
        .open_search_generation_snapshot(definition_id)
        .unwrap()
        .expect("generation before bootstrap");
    assert!(matches!(
        snapshot_before.coverage,
        CoverageState::TailPending { .. }
    ));
    assert!(snapshot_before.artifacts.artifacts.is_empty());

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let before = collect_i32_column(&table.fulltext_filter(1, &query, None, &[0]).unwrap(), 0);
    assert_eq!(before, vec![1, 3]);

    let report = table.bootstrap_search_generations().unwrap();
    assert_eq!(report.definitions_considered, 1);
    assert_eq!(report.definitions_updated, 1);
    assert_eq!(report.rowsets_materialized, 2);

    let coverage_after = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after bootstrap");
    assert!(coverage_after.is_complete());
    assert_eq!(
        coverage_after.visible_segment_count,
        coverage_after.indexed_segment_count
    );

    let snapshot_after = table
        .open_search_generation_snapshot(definition_id)
        .unwrap()
        .expect("generation after bootstrap");
    assert!(snapshot_after.coverage.is_complete());
    assert_eq!(snapshot_after.artifacts.artifacts.len(), 2);
    assert!(snapshot_after.artifacts.artifacts.iter().all(|artifact| {
        matches!(
            artifact.location,
            ArtifactLocation::SidecarArtifactFile { .. }
        )
    }));

    let after = collect_i32_column(&table.fulltext_filter(1, &query, None, &[0]).unwrap(), 0);
    assert_eq!(after, vec![1, 3]);

    for (_, segment) in table.collect_segments(table.max_version()).unwrap() {
        let meta = segment.get_column_meta(1).expect("text column meta");
        assert!(
            meta.fulltext_index_pointer.is_none(),
            "late bootstrap must publish sidecar artifacts instead of patching immutable segment footers"
        );
    }
}

#[test]
fn fulltext_late_definition_exact_tail_merge_resolves_partial_rows() {
    let table = create_table_with_keys(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        KeysType::PrimaryKeys,
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
            test_string_vector(&["vector alpha", "noise beta"]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    table
        .update_direct(
            &read_view(&table),
            &[row_ids[&1]],
            &[1],
            &[vec![Value::Integer(15)]],
        )
        .unwrap();

    let partial = table
        .tablet()
        .rowset_with_max_version()
        .expect("partial update rowset");
    partial.load().unwrap();
    let partial_segment = partial.get_segment(0).expect("partial segment");
    assert!(
        partial_segment.get_column_meta(2).is_none(),
        "partial rowset should not physically store the search column"
    );

    let definition_id = register_fulltext_definition(&table, 2, "simple");
    let coverage_before = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage before bootstrap");
    assert!(!coverage_before.is_complete());
    assert!(matches!(
        coverage_before.coverage,
        CoverageState::TailPending {
            exact_tail_merge: true,
            ..
        }
    ));
    assert!(table.fulltext_capability(2, "simple").is_some());

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let chunks = table.fulltext_filter(2, &query, None, &[0, 1]).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![1]);
    assert_eq!(collect_i32_column(&chunks, 1), vec![15]);

    let report = table.bootstrap_search_generations().unwrap();
    assert_eq!(report.definitions_considered, 1);
    assert_eq!(report.definitions_updated, 1);
    assert_eq!(report.rowsets_materialized, 1);

    let coverage_after = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after bootstrap");
    assert!(!coverage_after.is_complete());
    assert_eq!(coverage_after.visible_segment_count, 2);
    assert_eq!(coverage_after.indexed_segment_count, 1);
}

#[test]
fn fulltext_tail_watermark_does_not_disable_exact_fallback() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1]),
            test_string_vector(&["vector tail"]),
        ]))
        .unwrap();

    register_fulltext_definition_with_freshness(
        &table,
        1,
        "simple",
        SearchFreshnessPolicy::BoundedLag {
            max_tail_rows: 0,
            max_lag_millis: 0,
        },
    );

    let capability = table
        .fulltext_capability(1, "simple")
        .expect("fulltext capability");
    assert_eq!(
        capability.capability_state(),
        SearchCapabilityState::Queryable
    );

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    table
        .open_fulltext_search_cursor(
            1,
            &query,
            1,
            "simple",
            None,
            None,
            FullTextScoreMode::Bm25,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("exact tail fallback remains executable beyond its maintenance watermark");
}

#[test]
fn fulltext_topk_mixed_artifact_tail_uses_unified_generation_stats() {
    let table = create_table_with_keys(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Varchar,
        ],
        KeysType::PrimaryKeys,
    );
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
            test_string_vector(&["vector database", "vector database"]),
        ]))
        .unwrap();

    let definition_id = register_fulltext_definition(&table, 2, "simple");
    let report = table.bootstrap_search_generations().unwrap();
    assert_eq!(report.definitions_updated, 1);

    let coverage_after_bootstrap = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after bootstrap");
    assert!(coverage_after_bootstrap.is_complete());

    let row_ids = collect_row_ids_by_id(&table);
    table
        .update_direct(
            &read_view(&table),
            &[row_ids[&1]],
            &[1],
            &[vec![Value::Integer(15)]],
        )
        .unwrap();

    let coverage_after_partial = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("fulltext coverage after partial update");
    assert!(matches!(
        coverage_after_partial.coverage,
        CoverageState::TailPending {
            exact_tail_merge: true,
            ..
        }
    ));

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let opened = table
        .open_fulltext_search_cursor(
            2,
            &query,
            2,
            "simple",
            None,
            None,
            FullTextScoreMode::Bm25,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .unwrap();
    let chunks = drain_search_cursor(&table, opened, &[0], true, 2, 4).unwrap();
    let scored_rows = collect_i32_score_pairs(&chunks, 0, 1);
    assert_eq!(scored_rows.len(), 2);
    assert_eq!(scored_rows[0].0, 1);
    assert_eq!(scored_rows[1].0, 2);

    let delta = (scored_rows[0].1 - scored_rows[1].1).abs();
    assert!(
        delta < 1e-6,
        "artifact and tail rows with identical text should share final score, rows={scored_rows:?}"
    );
}

#[test]
fn fulltext_topk_missing_generation_stats_records_degraded_metric() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    register_fulltext_definition(&table, 1, "simple");
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_string_vector(&["vector database", "noise"]),
        ]))
        .unwrap();

    let query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let capability = table
        .fulltext_capability(1, "simple")
        .expect("fulltext capability");
    let snapshot = table
        .open_search_snapshot(
            &capability,
            table.max_version(),
            &crate::search::SearchReadOptions::ungoverned(),
        )
        .expect("search snapshot");
    let table_id = table.table_id();
    let reason = "missing_generation_stats";
    let before = fulltext_degraded_score_metric_count(table_id, reason);

    let opened = FullTextTopKProvider::new(
        table.tablet(),
        1,
        &query,
        1,
        "simple",
        None,
        None,
        FullTextScoreMode::Bm25,
    )
    .open(snapshot)
    .unwrap();
    let chunks = drain_search_cursor(&table, opened, &[0], false, 1, 4).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![1]);

    let after = fulltext_degraded_score_metric_count(table_id, reason);
    assert_eq!(after, before + 1);
}

#[test]
fn sparse_late_definition_exact_tail_merge_resolves_partial_rows() {
    let table = create_table_with_keys(
        &[
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Blob,
        ],
        KeysType::PrimaryKeys,
    );
    let v1 = SparseVector::new(vec![1, 3], vec![1.0, 0.5]).unwrap();
    let v2 = SparseVector::new(vec![2], vec![1.0]).unwrap();
    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_i32_vector(&[10, 20]),
            test_sparse_blob_vector(&[v1, v2]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    table
        .update_direct(
            &read_view(&table),
            &[row_ids[&1]],
            &[1],
            &[vec![Value::Integer(15)]],
        )
        .unwrap();

    let partial = table
        .tablet()
        .rowset_with_max_version()
        .expect("partial update rowset");
    partial.load().unwrap();
    let partial_segment = partial.get_segment(0).expect("partial segment");
    assert!(
        partial_segment.get_column_meta(2).is_none(),
        "partial rowset should not physically store the sparse column"
    );

    let definition_id = register_sparse_definition(&table, 2);
    let coverage_before = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("sparse coverage before bootstrap");
    assert!(!coverage_before.is_complete());
    assert!(matches!(
        coverage_before.coverage,
        CoverageState::TailPending {
            exact_tail_merge: true,
            ..
        }
    ));
    assert!(table.sparse_capability(2).is_some());

    let query = SparseVector::parse("{1:1.0}").unwrap();
    let chunks = sparse_search(&table, 2, &query, 10, &[0, 1]).unwrap();
    assert_eq!(collect_i32_column(&chunks, 0), vec![1]);
    assert_eq!(collect_i32_column(&chunks, 1), vec![15]);

    let report = table.bootstrap_search_generations().unwrap();
    assert_eq!(report.definitions_considered, 1);
    assert_eq!(report.definitions_updated, 1);
    assert_eq!(report.rowsets_materialized, 1);

    let coverage_after = table
        .search_generation_coverage(definition_id)
        .unwrap()
        .expect("sparse coverage after bootstrap");
    assert!(!coverage_after.is_complete());
    assert_eq!(coverage_after.visible_segment_count, 2);
    assert_eq!(coverage_after.indexed_segment_count, 1);
}

#[test]
fn fulltext_update_delete_respect_delete_bitmap() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Varchar]);
    register_fulltext_definition(&table, 1, "simple");

    table
        .append(&test_chunk_from_vectors(vec![
            test_i32_vector(&[1, 2]),
            test_string_vector(&["vector alpha", "vector beta"]),
        ]))
        .unwrap();

    let row_ids = collect_row_ids_by_id(&table);
    table
        .update_direct(
            &read_view(&table),
            &[row_ids[&2]],
            &[1],
            &[vec![Value::Varchar("noise beta".to_string())]],
        )
        .unwrap();
    table
        .delete_direct(&read_view(&table), &[row_ids[&1]])
        .unwrap();

    let vector_query = FullTextIndex::new_default().parse_query("vector").unwrap();
    let vector_chunks = table.fulltext_filter(1, &vector_query, None, &[0]).unwrap();
    assert!(collect_i32_column(&vector_chunks, 0).is_empty());

    let noise_query = FullTextIndex::new_default().parse_query("noise").unwrap();
    let noise_chunks = table.fulltext_filter(1, &noise_query, None, &[0]).unwrap();
    assert_eq!(collect_i32_column(&noise_chunks, 0), vec![2]);
}

#[test]
fn art_compaction_rebuild_preserves_predicate_results() {
    let table = create_table(&[LogicalType::Integer, LogicalType::Integer]);
    table.declare_art_index("test_idx", 0);

    table.append(&chunk_with_i32_range(1, 3, 100)).unwrap();
    table.append(&chunk_with_i32_range(3, 6, 100)).unwrap();

    let artifact = build_duplicate_key_compaction_output(&table, 77_002);
    let output_rowset = artifact.rowset.clone();
    output_rowset.load().unwrap();

    for segment in output_rowset.segments() {
        assert!(
            segment.art_index(0).is_none(),
            "compaction output should not have runtime ART before rebuild"
        );
    }

    rebuild_compaction_indexes(
        table.tablet().as_ref(),
        output_rowset.clone(),
        &artifact.plan,
        &crate::search::SearchInlineBuilderSet::default(),
    )
    .unwrap();

    let predicate = PredicateTree::leaf(Predicate::Range {
        column_id: 0,
        lower: Value::Integer(2),
        upper: Value::Integer(4),
    });
    let mut payloads = Vec::new();
    for segment in output_rowset.segments() {
        assert!(
            segment.art_index(0).is_some(),
            "compaction output segment should have runtime ART"
        );

        let mut iter =
            SegmentIterator::new_with_delete_vector_predicate_and_prefetcher_late_materialize(
                segment.as_ref(),
                vec![1],
                vec![0],
                None,
                Some(predicate.clone()),
                None,
            )
            .unwrap();

        loop {
            let (rowids, batch) = iter.next_batch(8).unwrap();
            if rowids.is_empty() {
                break;
            }
            let column = &batch[0].1;
            for row_idx in 0..rowids.len() {
                let start = row_idx * std::mem::size_of::<i32>();
                let end = start + std::mem::size_of::<i32>();
                payloads.push(i32::from_le_bytes(
                    column.data[start..end].try_into().unwrap(),
                ));
            }
        }
    }

    payloads.sort_unstable();
    assert_eq!(payloads, vec![102, 103, 104]);
}
