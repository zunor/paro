// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;
use paro_storage::tablet::{KeysType, Tablet, TabletColumn, TabletSchema};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn wait_until_removed(path: &Path) {
    for _ in 0..60 {
        if !path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !path.exists(),
        "expected path to be removed by shutdown sweep: {:?}",
        path
    );
}

#[test]
fn drop_table_cleanup_idempotent_test() {
    let tmp = tempdir().unwrap();
    let data_dir = tmp.path().join("drop_table_cleanup_idempotent");

    let schema = Arc::new(
        TabletSchema::new(
            1,
            vec![TabletColumn::new(0, "id".to_string(), LogicalType::Integer)],
            KeysType::DuplicateKeys,
        )
        .unwrap(),
    );
    let tablet = Tablet::new(7, 7, 0, schema, &data_dir, None).unwrap();
    tablet.init().unwrap();
    tablet.save_meta().unwrap();
    assert!(data_dir.exists());

    Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, true).unwrap();
    Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, true).unwrap();
    wait_until_removed(&data_dir);

    // Re-run after cleanup, should still be idempotent.
    Tablet::mark_shutdown_and_schedule_sweep_by_data_dir(&data_dir, true).unwrap();
}
