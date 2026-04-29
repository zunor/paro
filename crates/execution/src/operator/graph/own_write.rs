// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::Result;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::{TabletReader, TabletReaderParams};
use paro_storage::transaction::overlay_reader::TxnOverlayReader;
use paro_transaction::TransactionView;

pub(super) fn open_overlay_table_reader(
    storage: &TableHandle,
    txn_view: &TransactionView,
    columns: Vec<usize>,
    emit_row_id: bool,
) -> Result<TabletReader> {
    let snapshot =
        storage.storage_snapshot(txn_view.read_ts(), txn_view.read_snapshot().lease())?;
    let overlay = TxnOverlayReader::for_tablet(&storage.tablet(), txn_view)?;
    let mut rowsets = snapshot.rowsets()?;
    if let Some(overlay) = &overlay {
        rowsets.extend(overlay.all_rowsets());
    }

    let mut params = TabletReaderParams::with_version(snapshot.visible_version())
        .with_columns(columns)
        .with_emit_row_id(emit_row_id);
    if let Some(delete_vectors) = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors) {
        params = params.with_overlay_delete_vectors(delete_vectors);
    }

    let mut reader = storage.create_reader(params)?;
    reader.prepare_with_pinned_rowsets(rowsets)?;
    Ok(reader)
}
