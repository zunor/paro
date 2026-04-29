// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Transaction write-buffer spill artifacts.
//!
//! Spill stays storage-local: transaction-core only sees participant state,
//! while storage records typed staged rowsets/delete-vectors and expands them
//! during overlay preparation.

use crate::metrics::storage_metrics;
use crate::rowset::RowsetSharedPtr;
use crate::tablet::{PhysicalRowRef, TabletRef};
use paro_common::error::{self as paro_error, Result};
use paro_transaction::{CommandId, DatabaseId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

pub const DEFAULT_TXN_SPILL_BYTES_LIMIT: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_GLOBAL_TXN_SPILL_BYTES_LIMIT: u64 = 64 * 1024 * 1024 * 1024;
pub const DEFAULT_TXN_SPILL_FOREGROUND_WAIT_BUDGET_US: u64 = 0;

const MANIFEST_RECORD_VERSION: u16 = 1;
const ROW_REF_MAGIC: &[u8; 8] = b"PTXNDV1\n";

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TxnSpillCleanupReport {
    pub removed_artifacts: usize,
    pub removed_manifest_dirs: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactCleanupOutcome {
    Removed,
    NotNeeded,
    KeptForDiagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TxnSpillArtifactKind {
    Rowset,
    DeleteVector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TxnSpillManifestState {
    Staged,
    CommittedDescriptorWritten,
    Abandoned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxnSpillManifestRecord {
    record_version: u16,
    artifact_id: u64,
    sequence: u64,
    kind: TxnSpillArtifactKind,
    state: TxnSpillManifestState,
    database_id: u64,
    txn_id: u64,
    tablet_id: u64,
    command_id: u32,
    rowset_id: Option<u64>,
    path: String,
    row_count: u64,
    bytes: u64,
}

impl TxnSpillManifestRecord {
    fn staged(artifact: &TxnSpillArtifactMeta) -> Self {
        Self::with_state(artifact, TxnSpillManifestState::Staged)
    }

    fn with_state(artifact: &TxnSpillArtifactMeta, state: TxnSpillManifestState) -> Self {
        Self {
            record_version: MANIFEST_RECORD_VERSION,
            artifact_id: artifact.artifact_id,
            sequence: artifact.sequence,
            kind: artifact.kind,
            state,
            database_id: artifact.database_id,
            txn_id: artifact.txn_id,
            tablet_id: artifact.tablet_id,
            command_id: artifact.command_id.into_raw(),
            rowset_id: artifact.rowset_id,
            path: artifact.path.to_string_lossy().to_string(),
            row_count: artifact.row_count,
            bytes: artifact.bytes,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TxnSpillAdmission {
    global_limit_bytes: AtomicU64,
    inflight_bytes: AtomicU64,
    staged_bytes: AtomicU64,
    device_pressure_high: AtomicBool,
    foreground_wait_budget_us: AtomicU64,
}

impl Default for TxnSpillAdmission {
    fn default() -> Self {
        Self {
            global_limit_bytes: AtomicU64::new(DEFAULT_GLOBAL_TXN_SPILL_BYTES_LIMIT),
            inflight_bytes: AtomicU64::new(0),
            staged_bytes: AtomicU64::new(0),
            device_pressure_high: AtomicBool::new(false),
            foreground_wait_budget_us: AtomicU64::new(DEFAULT_TXN_SPILL_FOREGROUND_WAIT_BUDGET_US),
        }
    }
}

impl TxnSpillAdmission {
    #[inline]
    pub(crate) fn global() -> &'static Self {
        static INSTANCE: OnceLock<TxnSpillAdmission> = OnceLock::new();
        INSTANCE.get_or_init(TxnSpillAdmission::default)
    }

    pub(crate) fn preflight_foreground(&self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if self.device_pressure_high.load(Ordering::Acquire) {
            storage_metrics().inc_txn_spill_device_pressure_rejects();
            return Err(paro_error::out_of_memory(
                "transaction spill rejected by device pressure coordinator",
            ));
        }
        let limit = self.global_limit_bytes.load(Ordering::Acquire);
        let used = self
            .staged_bytes
            .load(Ordering::Acquire)
            .saturating_add(self.inflight_bytes.load(Ordering::Acquire));
        if limit > 0 && used.saturating_add(bytes) > limit {
            storage_metrics().inc_txn_spill_admission_rejects();
            return Err(paro_error::out_of_memory(format!(
                "transaction spill global budget exceeded: projected={} bytes budget={} bytes",
                used.saturating_add(bytes),
                limit
            )));
        }
        Ok(())
    }

    fn begin_foreground_write(&self, bytes: u64) -> Result<TxnSpillAdmissionGuard<'_>> {
        let start = Instant::now();
        self.preflight_foreground(bytes)?;
        loop {
            let inflight = self.inflight_bytes.load(Ordering::Acquire);
            let staged = self.staged_bytes.load(Ordering::Acquire);
            let limit = self.global_limit_bytes.load(Ordering::Acquire);
            let projected = staged.saturating_add(inflight).saturating_add(bytes);
            if limit > 0 && projected > limit {
                storage_metrics().inc_txn_spill_admission_rejects();
                return Err(paro_error::out_of_memory(format!(
                    "transaction spill global budget exceeded: projected={} bytes budget={} bytes",
                    projected, limit
                )));
            }
            match self.inflight_bytes.compare_exchange_weak(
                inflight,
                inflight.saturating_add(bytes),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    storage_metrics().add_txn_spill_wait_time(start.elapsed());
                    return Ok(TxnSpillAdmissionGuard {
                        admission: self,
                        bytes,
                        finished: false,
                    });
                }
                Err(_) => {
                    let waited_us = start.elapsed().as_micros() as u64;
                    let budget = self.foreground_wait_budget_us.load(Ordering::Acquire);
                    if budget > 0 && waited_us > budget {
                        storage_metrics().inc_txn_spill_admission_rejects();
                        return Err(paro_error::out_of_memory(format!(
                            "transaction spill admission wait exceeded: waited={}us budget={}us",
                            waited_us, budget
                        )));
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }

    fn finish_staged(&self, bytes: u64) {
        let _ = self
            .inflight_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
        self.staged_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    pub(crate) fn release_staged(&self, bytes: u64) {
        if bytes > 0 {
            let _ =
                self.staged_bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        Some(current.saturating_sub(bytes))
                    });
        }
    }

    #[cfg(test)]
    pub(crate) fn set_device_pressure_for_tests(&self, high: bool) {
        self.device_pressure_high.store(high, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn reset_for_tests(&self) {
        self.global_limit_bytes
            .store(DEFAULT_GLOBAL_TXN_SPILL_BYTES_LIMIT, Ordering::Release);
        self.inflight_bytes.store(0, Ordering::Release);
        self.staged_bytes.store(0, Ordering::Release);
        self.device_pressure_high.store(false, Ordering::Release);
        self.foreground_wait_budget_us.store(
            DEFAULT_TXN_SPILL_FOREGROUND_WAIT_BUDGET_US,
            Ordering::Release,
        );
    }
}

struct TxnSpillAdmissionGuard<'a> {
    admission: &'a TxnSpillAdmission,
    bytes: u64,
    finished: bool,
}

impl TxnSpillAdmissionGuard<'_> {
    fn finish(mut self) {
        self.admission.finish_staged(self.bytes);
        self.finished = true;
    }
}

impl Drop for TxnSpillAdmissionGuard<'_> {
    fn drop(&mut self) {
        if !self.finished && self.bytes > 0 {
            let _ = self.admission.inflight_bytes.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |current| Some(current.saturating_sub(self.bytes)),
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TxnSpillArtifactMeta {
    pub(crate) artifact_id: u64,
    pub(crate) sequence: u64,
    pub(crate) kind: TxnSpillArtifactKind,
    pub(crate) database_id: u64,
    pub(crate) txn_id: u64,
    pub(crate) tablet_id: u64,
    pub(crate) command_id: CommandId,
    pub(crate) rowset_id: Option<u64>,
    pub(crate) path: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) row_count: u64,
    pub(crate) bytes: u64,
}

impl TxnSpillArtifactMeta {
    #[inline]
    pub(crate) fn estimated_handle_bytes(&self) -> u64 {
        160_u64
            .saturating_add(self.path.to_string_lossy().len() as u64)
            .saturating_add(self.manifest_path.to_string_lossy().len() as u64)
    }

    fn append_state(&self, state: TxnSpillManifestState) -> Result<()> {
        append_manifest_record(
            &self.manifest_path,
            &TxnSpillManifestRecord::with_state(self, state),
        )
    }

    pub(crate) fn mark_committed_descriptor_written(&self) {
        let _ = self.append_state(TxnSpillManifestState::CommittedDescriptorWritten);
        TxnSpillAdmission::global().release_staged(self.bytes);
    }

    pub(crate) fn abandon(&self) {
        let _ = self.append_state(TxnSpillManifestState::Abandoned);
        TxnSpillAdmission::global().release_staged(self.bytes);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedRowsetArtifact {
    meta: TxnSpillArtifactMeta,
}

impl StagedRowsetArtifact {
    #[inline]
    pub(crate) fn artifact_id(&self) -> u64 {
        self.meta.artifact_id
    }

    #[inline]
    pub(crate) fn sequence(&self) -> u64 {
        self.meta.sequence
    }

    #[inline]
    pub(crate) fn tablet_id(&self) -> u64 {
        self.meta.tablet_id
    }

    #[inline]
    pub(crate) fn command_id(&self) -> CommandId {
        self.meta.command_id
    }

    #[inline]
    pub(crate) fn bytes(&self) -> u64 {
        self.meta.bytes
    }

    #[inline]
    pub(crate) fn estimated_handle_bytes(&self) -> u64 {
        self.meta.estimated_handle_bytes()
    }

    pub(crate) fn mark_committed_descriptor_written(&self) {
        self.meta.mark_committed_descriptor_written();
    }

    pub(crate) fn abandon_and_remove(&self) {
        self.meta.abandon();
        debug_assert!(is_safe_rowset_artifact_path(&self.meta));
        if is_safe_rowset_artifact_path(&self.meta) {
            let _ = fs::remove_dir_all(&self.meta.path);
        } else {
            tracing::warn!(
                path = %self.meta.path.display(),
                manifest = %self.meta.manifest_path.display(),
                rowset_id = ?self.meta.rowset_id,
                "skip unsafe transaction rowset artifact removal"
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedDeleteVectorArtifact {
    meta: TxnSpillArtifactMeta,
}

impl StagedDeleteVectorArtifact {
    #[inline]
    pub(crate) fn bytes(&self) -> u64 {
        self.meta.bytes
    }

    #[inline]
    pub(crate) fn estimated_handle_bytes(&self) -> u64 {
        self.meta.estimated_handle_bytes()
    }

    pub(crate) fn load_row_refs(&self) -> Result<Vec<PhysicalRowRef>> {
        load_row_refs(&self.meta.path)
    }

    pub(crate) fn mark_committed_descriptor_written(&self) {
        self.meta.mark_committed_descriptor_written();
        let _ = fs::remove_file(&self.meta.path);
    }

    pub(crate) fn abandon_and_remove(&self) {
        self.meta.abandon();
        let _ = fs::remove_file(&self.meta.path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxnSpillMark {
    pub(crate) next_sequence: u64,
}

#[derive(Debug)]
pub(crate) struct TxnSpillState {
    database_id: DatabaseId,
    next_sequence: u64,
    per_txn_limit_bytes: u64,
}

impl TxnSpillState {
    pub(crate) fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            next_sequence: 0,
            per_txn_limit_bytes: DEFAULT_TXN_SPILL_BYTES_LIMIT,
        }
    }

    #[inline]
    pub(crate) fn mark(&self) -> TxnSpillMark {
        TxnSpillMark {
            next_sequence: self.next_sequence,
        }
    }

    pub(crate) fn preflight_foreground_spill(
        &self,
        current_spilled_bytes: u64,
        bytes: u64,
    ) -> Result<()> {
        self.ensure_per_txn_limit(current_spilled_bytes, bytes)?;
        TxnSpillAdmission::global().preflight_foreground(bytes)
    }

    #[inline]
    pub(crate) fn rollback_to_mark(&mut self, mark: TxnSpillMark) {
        debug_assert!(self.next_sequence >= mark.next_sequence);
        self.next_sequence = mark.next_sequence;
    }

    pub(crate) fn stage_rowset(
        &mut self,
        txn_id: u64,
        command_id: CommandId,
        tablet: &TabletRef,
        rowset: &RowsetSharedPtr,
        current_spilled_bytes: u64,
    ) -> Result<StagedRowsetArtifact> {
        let bytes = rowset.total_disk_size().max(1);
        self.preflight_foreground_spill(current_spilled_bytes, bytes)?;
        let sequence = self.allocate_sequence();
        let manifest_path = manifest_path(tablet, self.database_id, txn_id)?;
        let meta = TxnSpillArtifactMeta {
            artifact_id: sequence,
            sequence,
            kind: TxnSpillArtifactKind::Rowset,
            database_id: self.database_id.into_raw(),
            txn_id,
            tablet_id: tablet.tablet_id(),
            command_id,
            rowset_id: Some(rowset.rowset_id()),
            path: rowset.rowset_path().to_path_buf(),
            manifest_path,
            row_count: rowset.num_rows(),
            bytes,
        };
        persist_manifest_stage(&meta)?;
        storage_metrics().add_txn_spill_bytes(bytes);
        storage_metrics().inc_txn_spill_artifacts();
        Ok(StagedRowsetArtifact { meta })
    }

    pub(crate) fn stage_delete_vectors(
        &mut self,
        txn_id: u64,
        command_id: CommandId,
        tablet: &TabletRef,
        locations: &[PhysicalRowRef],
        current_spilled_bytes: u64,
    ) -> Result<StagedDeleteVectorArtifact> {
        if locations.is_empty() {
            return Err(paro_error::invalid_input(
                "cannot stage an empty transaction delete-vector artifact",
            ));
        }
        let bytes = encoded_row_refs_len(locations.len());
        self.preflight_foreground_spill(current_spilled_bytes, bytes)?;
        let sequence = self.allocate_sequence();
        let manifest_path = manifest_path(tablet, self.database_id, txn_id)?;
        let path = manifest_path
            .parent()
            .ok_or_else(|| paro_error::internal("spill manifest path has no parent"))?
            .join(format!(
                "delvec_{}_{}_{}.tdv",
                tablet.tablet_id(),
                command_id.into_raw(),
                sequence
            ));
        write_row_refs(&path, locations)?;
        let meta = TxnSpillArtifactMeta {
            artifact_id: sequence,
            sequence,
            kind: TxnSpillArtifactKind::DeleteVector,
            database_id: self.database_id.into_raw(),
            txn_id,
            tablet_id: tablet.tablet_id(),
            command_id,
            rowset_id: None,
            path,
            manifest_path,
            row_count: locations.len() as u64,
            bytes,
        };
        persist_manifest_stage(&meta)?;
        storage_metrics().add_txn_spill_bytes(bytes);
        storage_metrics().inc_txn_spill_artifacts();
        Ok(StagedDeleteVectorArtifact { meta })
    }

    fn allocate_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        sequence
    }

    fn ensure_per_txn_limit(&self, current_spilled_bytes: u64, new_bytes: u64) -> Result<()> {
        let projected = current_spilled_bytes.saturating_add(new_bytes);
        if self.per_txn_limit_bytes > 0 && projected > self.per_txn_limit_bytes {
            storage_metrics().inc_txn_spill_admission_rejects();
            return Err(paro_error::out_of_memory(format!(
                "transaction spill budget exceeded: projected={} bytes budget={} bytes",
                projected, self.per_txn_limit_bytes
            )));
        }
        Ok(())
    }
}

fn persist_manifest_stage(meta: &TxnSpillArtifactMeta) -> Result<()> {
    let guard = TxnSpillAdmission::global().begin_foreground_write(meta.bytes)?;
    append_manifest_record(&meta.manifest_path, &TxnSpillManifestRecord::staged(meta))?;
    guard.finish();
    Ok(())
}

fn manifest_path(tablet: &TabletRef, database_id: DatabaseId, txn_id: u64) -> Result<PathBuf> {
    let dir = tablet
        .data_dir()
        .join("txn_staging")
        .join(format!("database={}", database_id.into_raw()))
        .join(format!("txn={txn_id}"))
        .join("storage");
    fs::create_dir_all(&dir).map_err(|err| {
        paro_error::io_error(format!(
            "create transaction spill staging dir {}: {}",
            dir.display(),
            err
        ))
    })?;
    Ok(dir.join("manifest.jsonl"))
}

fn tablet_data_dir_from_manifest_path(manifest_path: &Path) -> Option<&Path> {
    manifest_path
        .parent()?
        .parent()?
        .parent()?
        .parent()?
        .parent()
}

fn is_safe_rowset_artifact_path(meta: &TxnSpillArtifactMeta) -> bool {
    let Some(rowset_id) = meta.rowset_id else {
        return false;
    };
    let Some(data_dir) = tablet_data_dir_from_manifest_path(&meta.manifest_path) else {
        return false;
    };
    let rowsets_dir = data_dir.join("rowsets");
    let expected_name = format!("rowset_{rowset_id}");
    meta.kind == TxnSpillArtifactKind::Rowset
        && meta.path.starts_with(&rowsets_dir)
        && meta.path.file_name().and_then(|name| name.to_str()) == Some(expected_name.as_str())
}

fn append_manifest_record(path: &Path, record: &TxnSpillManifestRecord) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            paro_error::io_error(format!(
                "create transaction spill manifest dir {}: {}",
                parent.display(),
                err
            ))
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| {
            paro_error::io_error(format!(
                "open transaction spill manifest {}: {}",
                path.display(),
                err
            ))
        })?;
    let payload = serde_json::to_vec(record).map_err(|err| {
        paro_error::internal(format!(
            "serialize transaction spill manifest record {}: {}",
            record.artifact_id, err
        ))
    })?;
    file.write_all(&payload).map_err(|err| {
        paro_error::io_error(format!(
            "write transaction spill manifest {}: {}",
            path.display(),
            err
        ))
    })?;
    file.write_all(b"\n").map_err(|err| {
        paro_error::io_error(format!(
            "write transaction spill manifest newline {}: {}",
            path.display(),
            err
        ))
    })?;
    file.sync_data().map_err(|err| {
        paro_error::io_error(format!(
            "sync transaction spill manifest {}: {}",
            path.display(),
            err
        ))
    })?;
    Ok(())
}

pub fn cleanup_stale_spill_artifacts_under(
    root: &Path,
    limit: usize,
) -> Result<TxnSpillCleanupReport> {
    if limit == 0 || !root.exists() {
        return Ok(TxnSpillCleanupReport::default());
    }

    let mut manifests = Vec::new();
    collect_spill_manifests(root, limit, &mut manifests)?;

    let mut report = TxnSpillCleanupReport::default();
    for manifest_path in manifests {
        if report
            .removed_artifacts
            .saturating_add(report.removed_manifest_dirs)
            >= limit
        {
            break;
        }
        let remaining = limit.saturating_sub(
            report
                .removed_artifacts
                .saturating_add(report.removed_manifest_dirs),
        );
        let cleanup = cleanup_spill_manifest(&manifest_path, remaining)?;
        report.removed_artifacts = report
            .removed_artifacts
            .saturating_add(cleanup.removed_artifacts);
        report.removed_manifest_dirs = report
            .removed_manifest_dirs
            .saturating_add(cleanup.removed_manifest_dirs);
    }

    Ok(report)
}

fn collect_spill_manifests(root: &Path, limit: usize, manifests: &mut Vec<PathBuf>) -> Result<()> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        if manifests.len() >= limit {
            break;
        }
        let Ok(entries) = fs::read_dir(&path) else {
            continue;
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                paro_error::io_error(format!(
                    "read transaction spill cleanup dir {}: {}",
                    path.display(),
                    err
                ))
            })?;
            let child = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                paro_error::io_error(format!(
                    "read transaction spill cleanup entry {}: {}",
                    child.display(),
                    err
                ))
            })?;
            if file_type.is_file() && is_spill_manifest_path(&child) {
                manifests.push(child);
                if manifests.len() >= limit {
                    break;
                }
            } else if file_type.is_dir() {
                stack.push(child);
            }
        }
    }
    Ok(())
}

fn is_spill_manifest_path(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some("manifest.jsonl")
        && path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some("storage")
        && path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("txn="))
        && path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("database="))
        && path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some("txn_staging")
}

fn cleanup_spill_manifest(manifest_path: &Path, limit: usize) -> Result<TxnSpillCleanupReport> {
    if limit == 0 {
        return Ok(TxnSpillCleanupReport::default());
    }

    let mut latest = read_manifest_latest_states(manifest_path)?;
    let mut report = TxnSpillCleanupReport::default();
    let mut records: Vec<_> = latest.drain().map(|(_, record)| record).collect();
    records.sort_by_key(|record| record.sequence);
    let mut kept_for_diagnostics = false;

    for record in records {
        if report.removed_artifacts >= limit {
            break;
        }
        match cleanup_spill_artifact_record(manifest_path, &record)? {
            ArtifactCleanupOutcome::Removed => {
                report.removed_artifacts = report.removed_artifacts.saturating_add(1);
            }
            ArtifactCleanupOutcome::NotNeeded => {}
            ArtifactCleanupOutcome::KeptForDiagnostics => {
                kept_for_diagnostics = true;
            }
        }
    }

    if !kept_for_diagnostics
        && report.removed_artifacts < limit
        && remove_manifest_dir(manifest_path)?
    {
        report.removed_manifest_dirs = 1;
    }
    Ok(report)
}

fn read_manifest_latest_states(
    manifest_path: &Path,
) -> Result<HashMap<u64, TxnSpillManifestRecord>> {
    let payload = fs::read_to_string(manifest_path).map_err(|err| {
        paro_error::io_error(format!(
            "read transaction spill manifest {}: {}",
            manifest_path.display(),
            err
        ))
    })?;
    let mut latest = HashMap::new();
    for line in payload.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let record = match serde_json::from_str::<TxnSpillManifestRecord>(line) {
            Ok(record) => record,
            Err(err) => {
                tracing::warn!(
                    manifest = %manifest_path.display(),
                    error = %err,
                    "stop transaction spill cleanup at unreadable manifest line"
                );
                break;
            }
        };
        if record.record_version != MANIFEST_RECORD_VERSION {
            tracing::warn!(
                manifest = %manifest_path.display(),
                record_version = record.record_version,
                "skip unsupported transaction spill manifest record"
            );
            continue;
        }
        latest.insert(record.artifact_id, record);
    }
    Ok(latest)
}

fn cleanup_spill_artifact_record(
    manifest_path: &Path,
    record: &TxnSpillManifestRecord,
) -> Result<ArtifactCleanupOutcome> {
    match (record.kind, record.state) {
        (TxnSpillArtifactKind::Rowset, TxnSpillManifestState::CommittedDescriptorWritten) => {
            Ok(ArtifactCleanupOutcome::NotNeeded)
        }
        (TxnSpillArtifactKind::DeleteVector, TxnSpillManifestState::CommittedDescriptorWritten)
        | (_, TxnSpillManifestState::Staged)
        | (_, TxnSpillManifestState::Abandoned) => {
            remove_spill_artifact_path(manifest_path, record)
        }
    }
}

fn remove_spill_artifact_path(
    manifest_path: &Path,
    record: &TxnSpillManifestRecord,
) -> Result<ArtifactCleanupOutcome> {
    let path = PathBuf::from(&record.path);
    if !path.exists() {
        return Ok(ArtifactCleanupOutcome::NotNeeded);
    }
    match record.kind {
        TxnSpillArtifactKind::Rowset => {
            let meta = TxnSpillArtifactMeta {
                artifact_id: record.artifact_id,
                sequence: record.sequence,
                kind: record.kind,
                database_id: record.database_id,
                txn_id: record.txn_id,
                tablet_id: record.tablet_id,
                command_id: CommandId::new(record.command_id),
                rowset_id: record.rowset_id,
                path,
                manifest_path: manifest_path.to_path_buf(),
                row_count: record.row_count,
                bytes: record.bytes,
            };
            if !is_safe_rowset_artifact_path(&meta) {
                tracing::warn!(
                    path = %meta.path.display(),
                    manifest = %manifest_path.display(),
                    rowset_id = ?record.rowset_id,
                    "skip unsafe stale transaction rowset artifact cleanup"
                );
                return Ok(ArtifactCleanupOutcome::KeptForDiagnostics);
            }
            fs::remove_dir_all(&meta.path).map_err(|err| {
                paro_error::io_error(format!(
                    "remove stale transaction rowset artifact {}: {}",
                    meta.path.display(),
                    err
                ))
            })?;
            Ok(ArtifactCleanupOutcome::Removed)
        }
        TxnSpillArtifactKind::DeleteVector => {
            if !is_safe_delete_vector_artifact_path(manifest_path, &path) {
                tracing::warn!(
                    path = %path.display(),
                    manifest = %manifest_path.display(),
                    "skip unsafe stale transaction delete-vector artifact cleanup"
                );
                return Ok(ArtifactCleanupOutcome::KeptForDiagnostics);
            }
            fs::remove_file(&path).map_err(|err| {
                paro_error::io_error(format!(
                    "remove stale transaction delete-vector artifact {}: {}",
                    path.display(),
                    err
                ))
            })?;
            Ok(ArtifactCleanupOutcome::Removed)
        }
    }
}

fn is_safe_delete_vector_artifact_path(manifest_path: &Path, path: &Path) -> bool {
    let Some(manifest_dir) = manifest_path.parent() else {
        return false;
    };
    path.starts_with(manifest_dir)
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("delvec_") && name.ends_with(".tdv"))
}

fn remove_manifest_dir(manifest_path: &Path) -> Result<bool> {
    let Some(manifest_dir) = manifest_path.parent() else {
        return Ok(false);
    };
    if !manifest_dir.exists() {
        return Ok(false);
    }
    fs::remove_dir_all(manifest_dir).map_err(|err| {
        paro_error::io_error(format!(
            "remove transaction spill manifest dir {}: {}",
            manifest_dir.display(),
            err
        ))
    })?;
    remove_empty_spill_ancestors(manifest_dir);
    Ok(true)
}

fn remove_empty_spill_ancestors(manifest_dir: &Path) {
    let mut current = manifest_dir.parent();
    while let Some(path) = current {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            break;
        };
        let is_spill_ancestor =
            name == "txn_staging" || name.starts_with("database=") || name.starts_with("txn=");
        if !is_spill_ancestor {
            break;
        }
        match fs::remove_dir(path) {
            Ok(()) => current = path.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => current = path.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to remove empty transaction spill ancestor"
                );
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_meta(
        path: PathBuf,
        manifest_path: PathBuf,
        rowset_id: Option<u64>,
    ) -> TxnSpillArtifactMeta {
        TxnSpillArtifactMeta {
            artifact_id: 0,
            sequence: 0,
            kind: TxnSpillArtifactKind::Rowset,
            database_id: 7,
            txn_id: 42,
            tablet_id: 9,
            command_id: CommandId::new(1),
            rowset_id,
            path,
            manifest_path,
            row_count: 0,
            bytes: 1,
        }
    }

    #[test]
    fn rowset_artifact_removal_is_limited_to_tablet_rowsets_dir() {
        let manifest =
            PathBuf::from("/data/tablet/txn_staging/database=7/txn=42/storage/manifest.jsonl");
        assert!(is_safe_rowset_artifact_path(&artifact_meta(
            PathBuf::from("/data/tablet/rowsets/rowset_123"),
            manifest.clone(),
            Some(123),
        )));
        assert!(!is_safe_rowset_artifact_path(&artifact_meta(
            PathBuf::from("/data/tablet/rowsets/rowset_124"),
            manifest.clone(),
            Some(123),
        )));
        assert!(!is_safe_rowset_artifact_path(&artifact_meta(
            PathBuf::from("/data/tablet"),
            manifest,
            Some(123),
        )));
    }

    #[test]
    fn spill_state_rollback_restores_next_sequence() {
        let mut spill = TxnSpillState::new(DatabaseId::new(7));
        let mark = spill.mark();
        assert_eq!(spill.allocate_sequence(), 0);
        assert_eq!(spill.allocate_sequence(), 1);

        spill.rollback_to_mark(mark);

        assert_eq!(spill.allocate_sequence(), 0);
    }

    #[test]
    fn startup_cleanup_removes_staged_spill_artifacts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let tablet_dir = temp.path().join("tablet-9");
        let manifest = tablet_dir
            .join("txn_staging")
            .join("database=7")
            .join("txn=42")
            .join("storage")
            .join("manifest.jsonl");
        let rowset_path = tablet_dir.join("rowsets").join("rowset_123");
        fs::create_dir_all(&rowset_path).expect("create rowset artifact");
        fs::write(rowset_path.join("segment.dat"), b"rowset").expect("write rowset");
        let rowset_meta = artifact_meta(rowset_path.clone(), manifest.clone(), Some(123));
        append_manifest_record(&manifest, &TxnSpillManifestRecord::staged(&rowset_meta))
            .expect("append rowset stage");

        let delete_path = manifest
            .parent()
            .expect("manifest parent")
            .join("delvec_9_0_1.tdv");
        fs::write(&delete_path, b"delete-vector").expect("write delete vector");
        let delete_meta = TxnSpillArtifactMeta {
            artifact_id: 1,
            sequence: 1,
            kind: TxnSpillArtifactKind::DeleteVector,
            database_id: 7,
            txn_id: 42,
            tablet_id: 9,
            command_id: CommandId::new(0),
            rowset_id: None,
            path: delete_path.clone(),
            manifest_path: manifest.clone(),
            row_count: 1,
            bytes: 32,
        };
        append_manifest_record(&manifest, &TxnSpillManifestRecord::staged(&delete_meta))
            .expect("append delete stage");

        let report = cleanup_stale_spill_artifacts_under(temp.path(), usize::MAX)
            .expect("cleanup stale spill");

        assert_eq!(report.removed_artifacts, 2);
        assert_eq!(report.removed_manifest_dirs, 1);
        assert!(!rowset_path.exists());
        assert!(!delete_path.exists());
        assert!(!manifest.parent().expect("manifest parent").exists());
    }
}

fn encoded_row_refs_len(count: usize) -> u64 {
    (ROW_REF_MAGIC.len() as u64)
        .saturating_add(8)
        .saturating_add((count as u64).saturating_mul(16))
}

fn write_row_refs(path: &Path, locations: &[PhysicalRowRef]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            paro_error::io_error(format!(
                "create transaction delete-vector dir {}: {}",
                parent.display(),
                err
            ))
        })?;
    }
    let mut file = File::create(path).map_err(|err| {
        paro_error::io_error(format!(
            "create transaction delete-vector artifact {}: {}",
            path.display(),
            err
        ))
    })?;
    file.write_all(ROW_REF_MAGIC).map_err(|err| {
        paro_error::io_error(format!(
            "write transaction delete-vector artifact {}: {}",
            path.display(),
            err
        ))
    })?;
    file.write_all(&(locations.len() as u64).to_le_bytes())
        .map_err(|err| {
            paro_error::io_error(format!(
                "write transaction delete-vector count {}: {}",
                path.display(),
                err
            ))
        })?;
    for location in locations {
        file.write_all(&location.rowset_id.to_le_bytes())
            .and_then(|_| file.write_all(&location.segment_id.to_le_bytes()))
            .and_then(|_| file.write_all(&location.row_offset.to_le_bytes()))
            .map_err(|err| {
                paro_error::io_error(format!(
                    "write transaction delete-vector rows {}: {}",
                    path.display(),
                    err
                ))
            })?;
    }
    file.sync_data().map_err(|err| {
        paro_error::io_error(format!(
            "sync transaction delete-vector artifact {}: {}",
            path.display(),
            err
        ))
    })?;
    Ok(())
}

fn load_row_refs(path: &Path) -> Result<Vec<PhysicalRowRef>> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|err| {
            paro_error::io_error(format!(
                "read transaction delete-vector artifact {}: {}",
                path.display(),
                err
            ))
        })?;
    if bytes.len() < ROW_REF_MAGIC.len() + 8 || &bytes[..ROW_REF_MAGIC.len()] != ROW_REF_MAGIC {
        return Err(paro_error::internal(format!(
            "invalid transaction delete-vector artifact header {}",
            path.display()
        )));
    }
    let mut offset = ROW_REF_MAGIC.len();
    let count = read_u64(&bytes, &mut offset, path)? as usize;
    let expected_len = ROW_REF_MAGIC.len() + 8 + count.saturating_mul(16);
    if bytes.len() != expected_len {
        return Err(paro_error::internal(format!(
            "invalid transaction delete-vector artifact length {}: expected {} got {}",
            path.display(),
            expected_len,
            bytes.len()
        )));
    }
    let mut locations = Vec::with_capacity(count);
    for _ in 0..count {
        let rowset_id = read_u64(&bytes, &mut offset, path)?;
        let segment_id = read_u32(&bytes, &mut offset, path)?;
        let row_offset = read_u32(&bytes, &mut offset, path)?;
        locations.push(PhysicalRowRef {
            rowset_id,
            segment_id,
            row_offset,
        });
    }
    Ok(locations)
}

fn read_u64(bytes: &[u8], offset: &mut usize, path: &Path) -> Result<u64> {
    let end = offset.saturating_add(8);
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        paro_error::internal(format!(
            "truncated u64 in transaction delete-vector artifact {}",
            path.display()
        ))
    })?;
    *offset = end;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u32(bytes: &[u8], offset: &mut usize, path: &Path) -> Result<u32> {
    let end = offset.saturating_add(4);
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        paro_error::internal(format!(
            "truncated u32 in transaction delete-vector artifact {}",
            path.display()
        ))
    })?;
    *offset = end;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}
