// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::OnceLock;

use divan::black_box;
use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::test_utils::test_allocator;
use paro_common::types::LogicalType;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::{InsertOnConflictAction, TableHandle};
use paro_storage::tablet::{KeysType, TabletReaderParams};

const BENCH_ROWS: i32 = 4_096;

fn main() {
    divan::main();
}

fn build_chunk(ids: &[i32], prices: &[i32], stocks: &[i32]) -> Chunk {
    let allocator = test_allocator();
    Chunk::from_vectors(
        vec![
            paro_common::test_utils::test_i32_vector_with_allocator(ids, allocator.clone()),
            paro_common::test_utils::test_i32_vector_with_allocator(prices, allocator.clone()),
            paro_common::test_utils::test_i32_vector_with_allocator(stocks, allocator.clone()),
        ],
        allocator,
    )
}

fn build_seed_table() -> TableHandle {
    let table = TableFactory::default()
        .create_table_with_keys(
            &[
                LogicalType::Integer,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
            KeysType::PrimaryKeys,
        )
        .unwrap();
    let ids: Vec<i32> = (0..BENCH_ROWS).collect();
    let prices: Vec<i32> = ids.iter().map(|id| id * 10).collect();
    let stocks: Vec<i32> = ids.iter().map(|id| id * 100).collect();
    table.append(&build_chunk(&ids, &prices, &stocks)).unwrap();
    table
}

fn collect_row_ids_by_id(table: &TableHandle) -> HashMap<i32, u64> {
    let mut reader = table
        .create_reader(TabletReaderParams::with_version(table.max_version()).with_emit_row_id(true))
        .unwrap();
    reader.prepare().unwrap();

    let mut row_ids = HashMap::new();
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        let ids = chunk.column(0).unwrap();
        let row_id_col = chunk.column(chunk.column_count() - 1).unwrap();
        for idx in 0..chunk.size() {
            row_ids.insert(
                ids.get_i32(idx).unwrap(),
                row_id_col.get_i64(idx).unwrap() as u64,
            );
        }
    }
    row_ids
}

struct DoNothingBenchState {
    table: TableHandle,
    conflict_chunk: Chunk,
}

fn do_nothing_state() -> &'static DoNothingBenchState {
    static STATE: OnceLock<DoNothingBenchState> = OnceLock::new();
    STATE.get_or_init(|| {
        let table = build_seed_table();
        let ids: Vec<i32> = (0..BENCH_ROWS).collect();
        let prices: Vec<i32> = ids.iter().map(|id| id * 10 + 1).collect();
        let stocks: Vec<i32> = ids.iter().map(|id| id * 100 + 1).collect();
        let conflict_chunk = build_chunk(&ids, &prices, &stocks);
        DoNothingBenchState {
            table,
            conflict_chunk,
        }
    })
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn pk_insert_on_conflict_do_nothing_all_conflicts() {
    let state = do_nothing_state();
    let affected = state
        .table
        .insert_on_conflict(
            &state.conflict_chunk,
            &InsertOnConflictAction::DoNothing,
            None,
        )
        .unwrap();
    black_box(affected);
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn pk_insert_on_conflict_do_update_non_key_columns() {
    let table = build_seed_table();
    let ids: Vec<i32> = (0..BENCH_ROWS).collect();
    let prices: Vec<i32> = ids.iter().map(|id| id * 10 + 7).collect();
    let stocks: Vec<i32> = ids.iter().map(|id| id * 100 + 7).collect();
    let affected = table
        .insert_on_conflict(
            &build_chunk(&ids, &prices, &stocks),
            &InsertOnConflictAction::DoUpdate {
                target_columns: vec![2],
                source_columns: vec![2],
            },
            None,
        )
        .unwrap();
    black_box(affected);
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn pk_partial_update_scan_latest_rows() {
    let table = build_seed_table();
    let row_ids = collect_row_ids_by_id(&table);
    let target_row_ids: Vec<u64> = (0..256).map(|id| row_ids[&(id * 8)]).collect();
    let new_values: Vec<Value> = (0..256).map(|idx| Value::Integer(10_000 + idx)).collect();
    table
        .update(&target_row_ids, &[2], &[new_values], None)
        .unwrap();

    let visible_rows: usize = table
        .scan_chunks()
        .unwrap()
        .iter()
        .map(|chunk| chunk.size())
        .sum();
    black_box(visible_rows);
}
