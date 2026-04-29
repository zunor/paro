// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use divan::black_box;
use paro_common::chunk::Chunk;
use paro_common::test_utils::{test_allocator, test_i32_vector_with_allocator};
use paro_common::types::LogicalType;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::TabletReaderParams;
use paro_storage::transaction::overlay_reader::TxnOverlayReader;
use paro_storage::transaction::txn::Transaction;
use paro_transaction::{
    CommandId, IsolationLevel, ParticipantStateSet, ReadSnapshot, ReadTrackerHandle, ReadTs,
    TransactionView,
};

const ROWS: i32 = 16_384;
const CHUNK_ROWS: i32 = 1_024;

fn main() {
    divan::main();
}

fn build_chunk(start: i32, rows: i32) -> Chunk {
    let allocator = test_allocator();
    let values = (start..start + rows).collect::<Vec<_>>();
    Chunk::from_vectors(
        vec![test_i32_vector_with_allocator(&values, allocator.clone())],
        allocator,
    )
}

fn view_for_command(txn: &Transaction, command_id: u32) -> TransactionView {
    TransactionView::new(
        txn.writer_id(),
        txn.read_ts(),
        ReadSnapshot::without_lease(ReadTs::new(txn.visible_commit_ts().into_raw())),
        IsolationLevel::Snapshot,
        CommandId::new(command_id),
        ReadTrackerHandle::noop(),
        ParticipantStateSet::from_vec(vec![txn.storage_participant_state()]),
    )
}

fn build_txn(rows: i32, memory_budget: u64) -> (TableHandle, Arc<Transaction>, TransactionView) {
    let table = TableFactory::default()
        .create_table(&[LogicalType::Integer])
        .unwrap();
    let txn = Arc::new(Transaction::new(99_001, 1));
    let view0 = view_for_command(&txn, 0);
    let mut start = 0;
    while start < rows {
        let take = (rows - start).min(CHUNK_ROWS);
        table
            .append_with_transaction(&view0, &build_chunk(start, take), txn.clone())
            .unwrap();
        start += take;
    }
    txn.set_write_buffer_memory_budget_bytes(memory_budget);
    txn.publish_command_boundary(CommandId::new(1));
    let view1 = view_for_command(&txn, 1);
    (table, txn, view1)
}

fn overlay_segment_count(table: &TableHandle, view: &TransactionView) -> usize {
    let overlay = TxnOverlayReader::for_tablet(&table.tablet(), view)
        .unwrap()
        .unwrap();
    overlay
        .segments_with_options(Default::default())
        .unwrap()
        .len()
}

fn scan_overlay_rows(table: &TableHandle, view: &TransactionView) -> usize {
    let snapshot = table
        .storage_snapshot(view.read_ts(), view.read_snapshot().lease())
        .unwrap();
    let overlay = TxnOverlayReader::for_tablet(&table.tablet(), view)
        .unwrap()
        .unwrap();
    let mut rowsets = snapshot.rowsets().unwrap();
    rowsets.extend(overlay.all_rowsets());
    let mut reader = table
        .create_reader(TabletReaderParams::with_version(snapshot.visible_version()))
        .unwrap();
    reader.prepare_with_pinned_rowsets(rowsets).unwrap();
    let mut rows = 0usize;
    while let Some(chunk) = reader.get_next_chunk().unwrap() {
        rows += chunk.size();
    }
    rows
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn overlay_prepare_memory_only() {
    let (table, _txn, view) = build_txn(ROWS, u64::MAX);
    black_box(overlay_segment_count(&table, &view));
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn overlay_prepare_spilled_rowset() {
    let (table, _txn, view) = build_txn(ROWS, 1);
    black_box(overlay_segment_count(&table, &view));
}

#[divan::bench(sample_count = 10, sample_size = 1)]
fn scan_own_write_spilled_rowset() {
    let (table, _txn, view) = build_txn(ROWS, 1);
    black_box(scan_overlay_rows(&table, &view));
}
