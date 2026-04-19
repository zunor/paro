// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

static SHUTDOWN_SWEEP_QUEUE: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) fn schedule_shutdown_sweep(data_dir: &Path, move_to_trash: bool) -> Result<()> {
    let queue_key = data_dir.to_string_lossy().to_string();
    {
        let mut queue = SHUTDOWN_SWEEP_QUEUE
            .lock()
            .map_err(|_| paro_error::internal("shutdown sweep queue lock poisoned"))?;
        if !queue.insert(queue_key.clone()) {
            return Ok(());
        }
    }

    let task_dir = data_dir.to_path_buf();
    let queue_key_for_task = queue_key.clone();
    let spawn = std::thread::Builder::new()
        .name("paro-tablet-sweep".to_string())
        .spawn(move || {
            if let Err(err) = sweep_shutdown_data_dir(&task_dir, move_to_trash) {
                warn!(
                    data_dir = %task_dir.display(),
                    error = %err,
                    "shutdown sweep failed"
                );
            }

            match SHUTDOWN_SWEEP_QUEUE.lock() {
                Ok(mut queue) => {
                    queue.remove(&queue_key_for_task);
                }
                Err(_) => warn!("shutdown sweep queue lock poisoned during release"),
            }
        });

    if let Err(err) = spawn {
        if let Ok(mut queue) = SHUTDOWN_SWEEP_QUEUE.lock() {
            queue.remove(&queue_key);
        }
        return Err(paro_error::internal(format!(
            "failed to spawn shutdown sweep task: {}",
            err
        )));
    }

    Ok(())
}

fn sweep_shutdown_data_dir(data_dir: &Path, move_to_trash: bool) -> Result<()> {
    if !data_dir.exists() {
        return Ok(());
    }

    if move_to_trash {
        return move_shutdown_data_dir_to_trash(data_dir);
    }

    match fs::remove_dir_all(data_dir) {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(paro_error::io_error(format!(
            "remove shutdown tablet dir {:?}: {}",
            data_dir, err
        ))),
    }
}

fn move_shutdown_data_dir_to_trash(data_dir: &Path) -> Result<()> {
    if !data_dir.exists() {
        return Ok(());
    }

    let parent = data_dir.parent().ok_or_else(|| {
        paro_error::io_error(format!(
            "cannot determine parent directory for {:?}",
            data_dir
        ))
    })?;
    let trash_dir = parent.join("trash");
    fs::create_dir_all(&trash_dir).map_err(|err| {
        paro_error::io_error(format!("create trash dir {:?}: {}", trash_dir, err))
    })?;

    let file_name = data_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("tablet");
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    for attempt in 0..1000u32 {
        let candidate = format!("{file_name}.{timestamp_ms}.{attempt}");
        let target = trash_dir.join(candidate);
        match fs::rename(data_dir, &target) {
            Ok(_) => return Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(paro_error::io_error(format!(
                    "move shutdown tablet dir {:?} -> {:?}: {}",
                    data_dir, target, err
                )));
            }
        }
    }

    Err(paro_error::io_error(format!(
        "failed to move {:?} into trash after many retries",
        data_dir
    )))
}
