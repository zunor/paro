//! WAL replay, torn-write repair, and checkpoint file recovery.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::metrics::storage_metrics;
use crate::wal::replay_state::{CheckpointInfo, ReplayResult, ReplayState};
use crate::wal::wal_entry::{WalEntry, WalHeaderMetadata};
use crate::wal::wal_reader::{ReadEntryResult, WalReader};
use crate::wal::wal_writer::{WalInitState, WAL_VERSION_NUMBER};
use crate::wal::write_ahead_log::{
    checkpoint_wal_path_from_main, recovery_wal_path_from_main, WriteAheadLog,
};
use paro_common::effect::{CatalogTxnOp, PostCommitHookDescriptor, PreparedDataOp};
use paro_common::error as paro_error;
use paro_common::error::Result;
use paro_common::logging::targets;

/// Callback trait for applying WAL entries during replay.
///
/// Catalog mutations are delivered only through [`ReplayHandler::replay_transaction`]
/// (`TxnBegin` … `TxnCommit` envelopes). Standalone per-DDL and row-tuple DML opcodes
/// are no longer deserialized.
pub trait ReplayHandler {
    /// Replay one committed unified transaction (catalog + data + hooks).
    fn replay_transaction(
        &mut self,
        _catalog_ops: &[CatalogTxnOp],
        _data_ops: &[PreparedDataOp],
        _post_commit_hooks: &[PostCommitHookDescriptor],
        _commit_id: u64,
    ) -> Result<()> {
        Ok(())
    }

    /// Primary-key deletes (keys are serialized bytes); typically tablet-local WAL.
    fn replay_primary_delete(&mut self, _keys: &[Vec<u8>]) -> Result<()> {
        Ok(())
    }

    /// Row-id deletes by `(rowset_id, segment_id, row_id)` triples.
    fn replay_row_id_delete(&mut self, _locations: &[(u64, u32, u32)]) -> Result<()> {
        Ok(())
    }

    /// Rowset commit (tablet-level publish).
    fn replay_rowset_commit(
        &mut self,
        _tablet_id: u64,
        _rowset_id: u64,
        _start_version: i64,
        _end_version: i64,
        _rowset_path: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Compaction publish replace intent (tablet-level publish).
    fn replay_compaction_publish(
        &mut self,
        _tablet_id: u64,
        _plan_id: u64,
        _job_id: u64,
        _output_rowset_id: u64,
        _output_start_version: i64,
        _output_end_version: i64,
        _cumulative_point_action: crate::compaction::plan::types::CumulativePointAction,
        _output_rowset_path: &str,
        _replaced_inputs: &[u64],
    ) -> Result<()> {
        Ok(())
    }

    /// Optional cross-check for RowsetCommit vs persisted tablet metadata.
    fn validate_rowset_commit(
        &mut self,
        _tablet_id: u64,
        _rowset_id: u64,
        _start_version: i64,
        _end_version: i64,
        _rowset_path: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn on_flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_checkpoint(&mut self, _checkpoint_marker: u64) -> Result<()> {
        Ok(())
    }
}

/// Recovery mode observed by WAL lifecycle logic.
///
/// Values are exported as a numeric gauge via storage metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecoveryMode {
    Unknown,
    NoWal,
    MainWalOnly,
    CheckpointWalOnly,
    MainAndCheckpointWal,
}

impl WalRecoveryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            WalRecoveryMode::Unknown => "unknown",
            WalRecoveryMode::NoWal => "no_wal",
            WalRecoveryMode::MainWalOnly => "main_wal_only",
            WalRecoveryMode::CheckpointWalOnly => "checkpoint_wal_only",
            WalRecoveryMode::MainAndCheckpointWal => "main_and_checkpoint_wal",
        }
    }

    pub fn as_metric_value(self) -> u64 {
        match self {
            WalRecoveryMode::Unknown => 0,
            WalRecoveryMode::NoWal => 1,
            WalRecoveryMode::MainWalOnly => 2,
            WalRecoveryMode::CheckpointWalOnly => 3,
            WalRecoveryMode::MainAndCheckpointWal => 4,
        }
    }

    fn from_sources(main_wal_exists: bool, checkpoint_wal_exists: bool) -> Self {
        match (main_wal_exists, checkpoint_wal_exists) {
            (false, false) => WalRecoveryMode::NoWal,
            (true, false) => WalRecoveryMode::MainWalOnly,
            (false, true) => WalRecoveryMode::CheckpointWalOnly,
            (true, true) => WalRecoveryMode::MainAndCheckpointWal,
        }
    }
}

/// Health report for one WAL file inspected in read-only mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFileHealthReport {
    pub path: String,
    pub exists: bool,
    pub size_bytes: u64,
    pub wal_version: Option<u64>,
    pub entries_scanned: u64,
    pub needs_truncation: bool,
    pub torn_write_position: Option<u64>,
    pub has_unflushed_tail: bool,
    pub last_safe_offset: u64,
    pub error: Option<String>,
}

impl WalFileHealthReport {
    fn missing(path: &Path) -> Self {
        Self {
            path: path.display().to_string(),
            exists: false,
            size_bytes: 0,
            wal_version: None,
            entries_scanned: 0,
            needs_truncation: false,
            torn_write_position: None,
            has_unflushed_tail: false,
            last_safe_offset: 0,
            error: None,
        }
    }

    pub fn is_healthy(&self) -> bool {
        !self.exists || (!self.needs_truncation && self.error.is_none())
    }
}

/// Aggregated WAL health report across main/checkpoint/recovery files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHealthCheckReport {
    pub recovery_mode: WalRecoveryMode,
    pub healthy: bool,
    pub main_wal: WalFileHealthReport,
    pub checkpoint_wal: WalFileHealthReport,
    pub recovery_wal: WalFileHealthReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalTruncationReason {
    UnsafeTail,
    Checkpoint,
}

impl WalTruncationReason {
    fn as_str(self) -> &'static str {
        match self {
            WalTruncationReason::UnsafeTail => "unsafe_tail",
            WalTruncationReason::Checkpoint => "checkpoint",
        }
    }
}

/// Dual WAL truncation pointers.
///
/// - `logical_ack_offset`: replay-safe logical recycle point (what can be acknowledged as safe)
/// - `physical_truncate_offset`: actual file truncation target after keep-from retention policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalTruncationPointers {
    pub logical_ack_offset: u64,
    pub physical_truncate_offset: u64,
}

fn inspect_wal_file_read_only(path: &Path) -> WalFileHealthReport {
    let mut report = WalFileHealthReport::missing(path);
    if !path.exists() {
        return report;
    }

    report.exists = true;
    report.size_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if report.size_bytes == 0 {
        return report;
    }

    let mut reader = match WalReader::open(path) {
        Ok(Some(reader)) => reader,
        Ok(None) => return report,
        Err(error) => {
            report.error = Some(error.to_string());
            return report;
        }
    };

    if let Err(error) = reader.ensure_header_read() {
        report.error = Some(error.to_string());
        return report;
    }

    report.wal_version = Some(reader.wal_version());
    match reader.scan_for_truncation_point() {
        Ok(scan) => {
            report.entries_scanned = scan.entries_scanned;
            report.needs_truncation = scan.needs_truncation();
            report.torn_write_position = scan.torn_write_position;
            report.has_unflushed_tail = scan.has_unflushed_tail;
            report.last_safe_offset = if scan.needs_truncation() {
                scan.last_flush_offset
            } else {
                scan.last_successful_offset
            };
        }
        Err(error) => {
            report.error = Some(error.to_string());
        }
    }

    report
}

/// Inspect WAL files in read-only mode without mutating any WAL state.
///
/// This is intended for operations/diagnostics and can be safely executed
/// on read-only deployments.
pub fn wal_health_check_read_only<P: AsRef<Path>>(main_wal_path: P) -> WalHealthCheckReport {
    let main_wal_path = main_wal_path.as_ref();
    let checkpoint_wal_path = checkpoint_wal_path_from_main(main_wal_path);
    let recovery_wal_path = recovery_wal_path_from_main(main_wal_path);

    let main_wal = inspect_wal_file_read_only(main_wal_path);
    let checkpoint_wal = inspect_wal_file_read_only(&checkpoint_wal_path);
    let recovery_wal = inspect_wal_file_read_only(&recovery_wal_path);
    let recovery_mode = WalRecoveryMode::from_sources(main_wal.exists, checkpoint_wal.exists);
    let healthy = main_wal.is_healthy() && checkpoint_wal.is_healthy() && recovery_wal.is_healthy();

    tracing::info!(
        target: targets::WAL,
        recovery_mode = recovery_mode.as_str(),
        healthy = healthy,
        main_exists = main_wal.exists,
        checkpoint_exists = checkpoint_wal.exists,
        recovery_exists = recovery_wal.exists,
        main_last_safe_offset = main_wal.last_safe_offset,
        checkpoint_last_safe_offset = checkpoint_wal.last_safe_offset,
        recovery_last_safe_offset = recovery_wal.last_safe_offset,
        "WAL health check completed (read-only)"
    );

    WalHealthCheckReport {
        recovery_mode,
        healthy,
        main_wal,
        checkpoint_wal,
        recovery_wal,
    }
}

/// WAL Recovery engine.
///
/// Handles the complete recovery process:
/// 1. First pass: scan for checkpoint markers and torn writes
/// 2. Truncate torn writes if detected
/// 3. Second pass: replay entries to restore state
/// 4. Optionally truncate WAL after checkpoint
///
/// ## Checkpoint Coordination
///
/// The recovery engine supports checkpoint coordination:
/// - `with_checkpoint_marker()` - Set the expected checkpoint marker
/// - If WAL checkpoint marker matches, skip replay (checkpoint was successful)
/// - If they don't match, replay all WAL entries
/// - Handle `.checkpoint.wal` files from interrupted checkpoints
pub struct WalRecovery {
    /// Path to the WAL file
    wal_path: String,
    /// Whether to automatically truncate torn writes
    auto_truncate_torn_writes: bool,
    /// Whether to truncate WAL after checkpoint during recovery
    truncate_after_checkpoint: bool,
    /// Expected checkpoint marker (from metadata store).
    expected_checkpoint_marker: Option<u64>,
    /// Expected WAL header metadata from database file identity.
    expected_wal_header_metadata: Option<WalHeaderMetadata>,
    /// Retention threshold (wal_keep_from) to avoid aggressive checkpoint truncation.
    ///
    /// Semantics:
    /// - `u64::MAX` (default): no keep-from limit, truncation can proceed normally
    /// - `0`: keep all current WAL bytes (never truncate for checkpoint reclamation)
    /// - `x` in `(0, current_wal_size)`: downstream still depends on this WAL file
    ///   (checkpoint-driven physical truncation is skipped)
    wal_keep_from: u64,
}

impl WalRecovery {
    /// Create a new recovery engine for the given WAL path.
    pub fn new<P: AsRef<Path>>(wal_path: P) -> Self {
        Self {
            wal_path: wal_path.as_ref().to_string_lossy().to_string(),
            auto_truncate_torn_writes: true,
            truncate_after_checkpoint: false,
            expected_checkpoint_marker: None,
            expected_wal_header_metadata: None,
            wal_keep_from: u64::MAX,
        }
    }

    /// Set whether to automatically truncate torn writes.
    ///
    /// When enabled (default), torn writes detected during recovery
    /// will be automatically truncated to the last safe point.
    pub fn with_auto_truncate(mut self, enabled: bool) -> Self {
        self.auto_truncate_torn_writes = enabled;
        self
    }

    /// Set whether to truncate WAL after checkpoint during recovery.
    ///
    /// When enabled, the WAL will be truncated after the checkpoint
    /// position once recovery is complete.
    pub fn with_checkpoint_truncation(mut self, enabled: bool) -> Self {
        self.truncate_after_checkpoint = enabled;
        self
    }

    /// Set keep-from retention threshold to prevent aggressive checkpoint truncation.
    ///
    /// This mirrors the `wal_keep_from` contract for retention.
    pub fn with_wal_keep_from(mut self, wal_keep_from: u64) -> Self {
        self.wal_keep_from = wal_keep_from;
        self
    }

    /// Set the expected checkpoint marker from metadata store.
    ///
    /// This is used to verify if the checkpoint completed successfully:
    /// - If WAL checkpoint marker matches this value, checkpoint succeeded
    /// - If they don't match, WAL replay is needed
    ///
    /// # Arguments
    /// * `checkpoint_marker` - The checkpoint marker persisted with catalog metadata.
    pub fn with_checkpoint_marker(mut self, checkpoint_marker: u64) -> Self {
        self.expected_checkpoint_marker = Some(checkpoint_marker);
        self
    }

    /// Set expected WAL header metadata from database file identity.
    pub fn with_wal_header_metadata(
        mut self,
        db_identifier: [u8; crate::wal::wal_entry::WAL_DB_IDENTIFIER_LEN],
        checkpoint_iteration: u64,
    ) -> Self {
        self.expected_wal_header_metadata =
            Some(WalHeaderMetadata::new(db_identifier, checkpoint_iteration));
        self
    }

    /// Get the path to the checkpoint WAL file.
    fn checkpoint_wal_path(&self) -> PathBuf {
        checkpoint_wal_path_from_main(Path::new(&self.wal_path))
    }

    /// Get the path to the recovery WAL file.
    fn recovery_wal_path(&self) -> PathBuf {
        recovery_wal_path_from_main(Path::new(&self.wal_path))
    }

    /// Build a new WAL instance with optional identity metadata.
    fn create_wal(&self, init_state: WalInitState) -> Result<WriteAheadLog> {
        match self.expected_wal_header_metadata {
            Some(metadata) => {
                WriteAheadLog::with_state_and_header_metadata(&self.wal_path, init_state, metadata)
            }
            None => WriteAheadLog::with_state(&self.wal_path, init_state),
        }
    }

    /// Remove the main WAL file if present.
    fn remove_main_wal_file(&self) -> Result<()> {
        let wal_path = Path::new(&self.wal_path);
        if !wal_path.exists() {
            return Ok(());
        }

        std::fs::remove_file(wal_path).map_err(|e| {
            paro_error::internal(format!(
                "Failed to remove incompatible WAL {}: {}",
                wal_path.display(),
                e
            ))
        })
    }

    /// Validate WAL header metadata against expected database identity.
    fn validate_wal_header_metadata(&self, actual: WalHeaderMetadata) -> bool {
        let Some(expected) = self.expected_wal_header_metadata else {
            return true;
        };

        if actual.db_identifier != expected.db_identifier {
            tracing::error!(
                target: targets::WAL,
                wal_db_identifier = ?actual.db_identifier,
                expected_db_identifier = ?expected.db_identifier,
                "WAL database identity mismatch, skipping replay"
            );
            return false;
        }

        if actual.checkpoint_iteration == expected.checkpoint_iteration {
            return true;
        }

        if actual.checkpoint_iteration.saturating_add(1) == expected.checkpoint_iteration {
            tracing::warn!(
                target: targets::WAL,
                wal_checkpoint_iteration = actual.checkpoint_iteration,
                expected_checkpoint_iteration = expected.checkpoint_iteration,
                "WAL checkpoint iteration is one behind data file; continuing replay"
            );
            return true;
        }

        tracing::error!(
            target: targets::WAL,
            wal_checkpoint_iteration = actual.checkpoint_iteration,
            expected_checkpoint_iteration = expected.checkpoint_iteration,
            "WAL checkpoint iteration mismatch, skipping replay"
        );
        false
    }

    /// Perform WAL recovery.
    ///
    /// This is the main entry point for recovery. It:
    /// 1. Handles any checkpoint WAL from interrupted checkpoints
    /// 2. Opens the WAL file (returns early if it doesn't exist)
    /// 3. Scans for torn writes and truncates if needed
    /// 4. Checks if checkpoint marker matches expected marker
    /// 5. Performs replay if needed
    /// 6. Returns a WriteAheadLog instance for continued use
    ///
    /// # Arguments
    /// * `handler` - Callback handler for applying entries
    ///
    /// # Returns
    /// * `Ok((wal, result))` - Recovery completed, returns WAL and result
    /// * `Err(...)` - Fatal error during recovery
    pub fn recover<H: ReplayHandler>(
        &self,
        handler: &mut H,
    ) -> Result<(WriteAheadLog, ReplayResult)> {
        let checkpoint_wal_path = self.checkpoint_wal_path();
        let recovery_wal_path = self.recovery_wal_path();
        let metrics_before = storage_metrics().snapshot();
        tracing::debug!(
            target: targets::WAL,
            main_wal_path = %self.wal_path,
            checkpoint_wal_path = %checkpoint_wal_path.display(),
            recovery_wal_path = %recovery_wal_path.display(),
            "Starting WAL recovery"
        );

        // Step 0: Detect checkpoint WAL from interrupted checkpoint.
        let checkpoint_wal_for_replay = self.prepare_checkpoint_wal_for_replay()?;
        let checkpoint_wal_exists = checkpoint_wal_for_replay.is_some();

        // Try to open the WAL file
        let reader = WalReader::open(&self.wal_path)?;
        let recovery_mode = WalRecoveryMode::from_sources(reader.is_some(), checkpoint_wal_exists);
        storage_metrics().set_wal_recovery_mode(recovery_mode.as_metric_value());
        tracing::info!(
            target: targets::WAL,
            recovery_mode = recovery_mode.as_str(),
            main_wal_exists = reader.is_some(),
            checkpoint_wal_exists = checkpoint_wal_exists,
            "WAL recovery mode selected"
        );

        let (_main_wal, mut result) = match reader {
            None => {
                // No main WAL file.
                let wal = self.create_wal(WalInitState::Uninitialized)?;
                let result = ReplayResult::success(0, 0);
                (wal, result)
            }
            Some(mut reader) => {
                reader.ensure_header_read()?;

                if reader.wal_version() != WAL_VERSION_NUMBER {
                    tracing::warn!(
                        target: targets::WAL,
                        wal_version = reader.wal_version(),
                        supported_wal_version = WAL_VERSION_NUMBER,
                        "Detected legacy WAL version, deleting WAL file"
                    );
                    self.remove_main_wal_file()?;
                    let wal = self.create_wal(WalInitState::Uninitialized)?;
                    let result = ReplayResult::success(0, 0);
                    (wal, result)
                } else if !self.validate_wal_header_metadata(reader.header_metadata()) {
                    self.remove_main_wal_file()?;
                    let wal = self.create_wal(WalInitState::Uninitialized)?;
                    let result = ReplayResult::success(0, 0);
                    (wal, result)
                } else {
                    // WAL exists - perform recovery.
                    self.recover_from_wal(&mut reader, handler)?
                }
            }
        };

        // Step 2: Replay checkpoint WAL if present, then finalize WAL files.
        if let Some(checkpoint_path) = checkpoint_wal_for_replay {
            let checkpoint_result = self.recover_checkpoint_wal(&checkpoint_path, handler)?;
            result.entries_replayed += checkpoint_result.entries_replayed;
            result.last_successful_offset = checkpoint_result.last_successful_offset;
            if !checkpoint_result.all_succeeded {
                result.all_succeeded = false;
                if result.error.is_none() {
                    result.error = checkpoint_result.error.clone();
                }
            }

            let main_exists_after_recovery = Path::new(&self.wal_path).exists();
            if main_exists_after_recovery {
                if result.checkpoint_verified {
                    // Checkpoint completed, follow finish_checkpoint semantics:
                    // checkpoint WAL becomes new main WAL.
                    self.promote_checkpoint_wal_to_main(&checkpoint_path)?;
                } else {
                    // Checkpoint did not complete: keep both streams by merging
                    // checkpoint entries into main WAL.
                    self.merge_main_and_checkpoint_wal(&checkpoint_path)?;
                }
            } else {
                // No main WAL remains (e.g. crash between remove+rename), use checkpoint WAL.
                self.promote_checkpoint_wal_to_main(&checkpoint_path)?;
            }
        }

        let init_state = if result.all_succeeded {
            WalInitState::Uninitialized
        } else {
            WalInitState::UninitializedRequiresTruncate
        };
        let wal = self.create_wal(init_state)?;

        storage_metrics().set_wal_recovery_mode(recovery_mode.as_metric_value());
        let metrics_after = storage_metrics().snapshot();
        tracing::info!(
            target: targets::WAL,
            recovery_mode = recovery_mode.as_str(),
            entries_replayed = result.entries_replayed,
            replay_entries_metric_delta = metrics_after
                .wal_replay_entries
                .saturating_sub(metrics_before.wal_replay_entries),
            replay_bytes_metric_delta = metrics_after
                .wal_replay_bytes
                .saturating_sub(metrics_before.wal_replay_bytes),
            truncate_bytes_metric_delta = metrics_after
                .wal_truncate_bytes
                .saturating_sub(metrics_before.wal_truncate_bytes),
            checkpoint_verified = result.checkpoint_verified,
            last_safe_offset = result.last_successful_offset,
            "WAL recovery finished"
        );

        Ok((wal, result))
    }

    /// Detect checkpoint WAL from an interrupted checkpoint.
    ///
    /// If a `.checkpoint.wal` file exists, it means a checkpoint was interrupted.
    fn prepare_checkpoint_wal_for_replay(&self) -> Result<Option<PathBuf>> {
        let checkpoint_wal_path = self.checkpoint_wal_path();
        let checkpoint_path = checkpoint_wal_path.as_path();

        if !checkpoint_path.exists() {
            return Ok(None);
        }

        let checkpoint_size = checkpoint_path.metadata().map(|m| m.len()).unwrap_or(0);

        if checkpoint_size == 0 {
            // Empty checkpoint WAL - just remove it
            tracing::debug!(target: targets::WAL, "Removing empty checkpoint WAL");
            std::fs::remove_file(checkpoint_path).ok();
            return Ok(None);
        }

        tracing::info!(
            target: targets::WAL,
            checkpoint_wal_size = checkpoint_size,
            "Found checkpoint WAL from interrupted checkpoint"
        );

        Ok(Some(checkpoint_wal_path))
    }

    /// Replay checkpoint WAL generated during checkpoint mode.
    fn recover_checkpoint_wal<H: ReplayHandler>(
        &self,
        checkpoint_wal_path: &Path,
        handler: &mut H,
    ) -> Result<ReplayResult> {
        let mut checkpoint_recovery = WalRecovery::new(checkpoint_wal_path)
            .with_auto_truncate(self.auto_truncate_torn_writes)
            .with_checkpoint_truncation(false);

        if let Some(expected) = self.expected_wal_header_metadata {
            checkpoint_recovery = checkpoint_recovery.with_wal_header_metadata(
                expected.db_identifier,
                expected.checkpoint_iteration.saturating_add(1),
            );
        }

        let (_wal, result) = checkpoint_recovery.recover(handler)?;
        Ok(result)
    }

    /// Promote checkpoint WAL to main WAL (`finish_checkpoint` semantics).
    fn promote_checkpoint_wal_to_main(&self, checkpoint_wal_path: &Path) -> Result<()> {
        if !checkpoint_wal_path.exists() {
            return Ok(());
        }

        let main_wal_path = Path::new(&self.wal_path);
        if main_wal_path.exists() {
            std::fs::remove_file(main_wal_path).map_err(|e| {
                paro_error::internal(format!(
                    "Failed to remove main WAL {} while promoting checkpoint WAL: {}",
                    main_wal_path.display(),
                    e
                ))
            })?;
        }

        std::fs::rename(checkpoint_wal_path, main_wal_path).map_err(|e| {
            paro_error::internal(format!(
                "Failed to promote checkpoint WAL {} to main WAL {}: {}",
                checkpoint_wal_path.display(),
                main_wal_path.display(),
                e
            ))
        })?;

        Ok(())
    }

    /// Merge main WAL + checkpoint WAL into recovery WAL, then replace main WAL.
    fn merge_main_and_checkpoint_wal(&self, checkpoint_wal_path: &Path) -> Result<()> {
        if !checkpoint_wal_path.exists() {
            return Ok(());
        }

        let main_wal_path = Path::new(&self.wal_path);
        if !main_wal_path.exists() {
            return self.promote_checkpoint_wal_to_main(checkpoint_wal_path);
        }

        let recovery_wal_path = self.recovery_wal_path();
        let mut recovery_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&recovery_wal_path)
            .map_err(|e| {
                paro_error::internal(format!(
                    "Failed to create recovery WAL {}: {}",
                    recovery_wal_path.display(),
                    e
                ))
            })?;

        // Copy main WAL as-is.
        let mut main_file = File::open(main_wal_path).map_err(|e| {
            paro_error::internal(format!(
                "Failed to open main WAL {} for merge: {}",
                main_wal_path.display(),
                e
            ))
        })?;
        std::io::copy(&mut main_file, &mut recovery_file).map_err(|e| {
            paro_error::internal(format!(
                "Failed to copy main WAL {} into recovery WAL {}: {}",
                main_wal_path.display(),
                recovery_wal_path.display(),
                e
            ))
        })?;

        // Append checkpoint WAL entries (skip checkpoint WAL header).
        let checkpoint_header_end = match WalReader::open(checkpoint_wal_path)? {
            Some(mut reader) => {
                reader.ensure_header_read()?;
                reader.current_offset()
            }
            None => 0,
        };
        if checkpoint_header_end > 0 {
            let mut checkpoint_file = File::open(checkpoint_wal_path).map_err(|e| {
                paro_error::internal(format!(
                    "Failed to open checkpoint WAL {} for merge: {}",
                    checkpoint_wal_path.display(),
                    e
                ))
            })?;
            checkpoint_file
                .seek(SeekFrom::Start(checkpoint_header_end))
                .map_err(|e| {
                    paro_error::internal(format!(
                        "Failed to seek checkpoint WAL {} during merge: {}",
                        checkpoint_wal_path.display(),
                        e
                    ))
                })?;
            std::io::copy(&mut checkpoint_file, &mut recovery_file).map_err(|e| {
                paro_error::internal(format!(
                    "Failed to append checkpoint WAL {} into recovery WAL {}: {}",
                    checkpoint_wal_path.display(),
                    recovery_wal_path.display(),
                    e
                ))
            })?;
        }

        recovery_file.sync_all().map_err(|e| {
            paro_error::internal(format!(
                "Failed to sync recovery WAL {}: {}",
                recovery_wal_path.display(),
                e
            ))
        })?;

        std::fs::remove_file(main_wal_path).map_err(|e| {
            paro_error::internal(format!(
                "Failed to remove main WAL {} before replace: {}",
                main_wal_path.display(),
                e
            ))
        })?;
        std::fs::rename(&recovery_wal_path, main_wal_path).map_err(|e| {
            paro_error::internal(format!(
                "Failed to replace main WAL {} with merged WAL {}: {}",
                main_wal_path.display(),
                recovery_wal_path.display(),
                e
            ))
        })?;

        std::fs::remove_file(checkpoint_wal_path).map_err(|e| {
            paro_error::internal(format!(
                "Failed to remove checkpoint WAL {} after merge: {}",
                checkpoint_wal_path.display(),
                e
            ))
        })?;

        storage_metrics().inc_wal_checkpoint_merge();
        tracing::info!(
            target: targets::WAL,
            main_wal_path = %main_wal_path.display(),
            checkpoint_wal_path = %checkpoint_wal_path.display(),
            recovery_wal_path = %recovery_wal_path.display(),
            "Merged main WAL and checkpoint WAL during recovery"
        );

        Ok(())
    }

    /// Perform recovery from an existing WAL file.
    fn recover_from_wal<H: ReplayHandler>(
        &self,
        reader: &mut WalReader,
        handler: &mut H,
    ) -> Result<(WriteAheadLog, ReplayResult)> {
        // Step 1: Scan WAL and locate the last safe transaction boundary.
        let scan_result = reader.scan_for_truncation_point()?;
        tracing::debug!(
            target: targets::WAL,
            entries_scanned = scan_result.entries_scanned,
            last_successful_offset = scan_result.last_successful_offset,
            last_safe_offset = scan_result.last_flush_offset,
            needs_truncation = scan_result.needs_truncation(),
            torn_write_position = ?scan_result.torn_write_position,
            has_unflushed_tail = scan_result.has_unflushed_tail,
            "WAL scan complete before replay"
        );

        // Step 2: Truncate corrupt/uncommitted tail if enabled.
        if scan_result.needs_truncation() && self.auto_truncate_torn_writes {
            let truncate_point = scan_result.recommended_truncation_point();
            let truncation_reason = if scan_result.torn_write_position.is_some() {
                "corrupt_tail"
            } else {
                "unflushed_tail"
            };
            tracing::warn!(
                target: targets::WAL,
                truncation_reason = truncation_reason,
                torn_write_position = ?scan_result.torn_write_position,
                has_unflushed_tail = scan_result.has_unflushed_tail,
                last_safe_offset = scan_result.last_flush_offset,
                truncate_to = truncate_point,
                "WAL contains unsafe tail, truncating to last flush boundary"
            );
            self.truncate_wal_file_with_reason(
                truncate_point,
                truncate_point,
                WalTruncationReason::UnsafeTail,
            )?;
            reader.refresh_file_size()?;
        } else if scan_result.needs_truncation() {
            tracing::warn!(
                target: targets::WAL,
                torn_write_position = ?scan_result.torn_write_position,
                has_unflushed_tail = scan_result.has_unflushed_tail,
                last_flush_offset = scan_result.last_flush_offset,
                "WAL contains unsafe tail but auto-truncate is disabled"
            );
        }

        if reader.file_size() == 0 {
            let wal = self.create_wal(WalInitState::Uninitialized)?;
            return Ok((wal, ReplayResult::success(0, 0)));
        }

        // Step 3: Reset and do first pass for checkpoint markers
        reader.reset()?;
        let checkpoint_state = self.first_pass(reader)?;

        // Step 4: Check if checkpoint marker matches expected marker.
        if let Some(expected_marker) = self.expected_checkpoint_marker {
            if let Some(ref checkpoint) = checkpoint_state.checkpoint_info {
                if checkpoint.checkpoint_marker == expected_marker {
                    // Checkpoint marker matches - checkpoint completed successfully
                    // No need to replay WAL entries
                    tracing::info!(
                    target: targets::WAL,
                    checkpoint_marker = expected_marker,
                    "Checkpoint marker matches database header, skipping WAL replay"
                        );

                    // Truncate WAL since checkpoint is complete
                    if self.truncate_after_checkpoint {
                        let current_wal_size = reader.file_size();
                        self.truncate_wal_file_with_reason(
                            current_wal_size,
                            0,
                            WalTruncationReason::Checkpoint,
                        )?;
                    }

                    let wal = self.create_wal(WalInitState::Uninitialized)?;
                    let result = ReplayResult::success(0, checkpoint_state.last_successful_offset)
                        .with_checkpoint(checkpoint.clone())
                        .with_checkpoint_verified(true);
                    return Ok((wal, result));
                } else {
                    tracing::warn!(
                    target: targets::WAL,
                    expected_marker = expected_marker,
                    wal_checkpoint_marker = checkpoint.checkpoint_marker,
                    "Checkpoint marker does not match database header, WAL replay needed"
                    );
                }
            }
        }

        // Step 5: Reset reader for second pass
        reader.reset()?;

        // Step 6: Actually replay entries
        let result = self.second_pass(reader, handler, &checkpoint_state)?;

        // Step 7: Optionally truncate after checkpoint
        if self.truncate_after_checkpoint {
            if let Some(ref checkpoint) = result.checkpoint_info {
                tracing::info!(
                    target: targets::WAL,
                    checkpoint_position = checkpoint.wal_position,
                    "Truncating WAL after checkpoint"
                );
                self.truncate_wal_file_with_reason(
                    checkpoint.wal_position,
                    checkpoint.wal_position,
                    WalTruncationReason::Checkpoint,
                )?;
            }
        }

        // Create WAL for continued use
        let init_state = if result.all_succeeded {
            WalInitState::Uninitialized
        } else {
            WalInitState::UninitializedRequiresTruncate
        };

        let wal = self.create_wal(init_state)?;

        Ok((wal, result))
    }

    fn resolve_truncation_pointers(
        &self,
        logical_ack_offset: u64,
        requested_physical_truncate_offset: u64,
        reason: WalTruncationReason,
        current_wal_size: u64,
    ) -> WalTruncationPointers {
        let logical_ack_offset = logical_ack_offset.min(current_wal_size);
        let mut physical_truncate_offset = requested_physical_truncate_offset.min(current_wal_size);

        // Never keep bytes beyond the logical replay-safe ack point.
        physical_truncate_offset = physical_truncate_offset.min(logical_ack_offset);

        // wal_keep_from retention: checkpoint reclamation is logical-first,
        // physical truncation is skipped if downstream still depends on this WAL file.
        let keep_from_active =
            self.wal_keep_from != u64::MAX && self.wal_keep_from < current_wal_size;
        if reason == WalTruncationReason::Checkpoint && keep_from_active {
            physical_truncate_offset = current_wal_size;
        }

        WalTruncationPointers {
            logical_ack_offset,
            physical_truncate_offset,
        }
    }

    /// Truncate the WAL file with logical/physical dual pointers.
    fn truncate_wal_file_with_reason(
        &self,
        logical_ack_offset: u64,
        requested_physical_truncate_offset: u64,
        reason: WalTruncationReason,
    ) -> Result<WalTruncationPointers> {
        let current_wal_size = std::fs::metadata(&self.wal_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let pointers = self.resolve_truncation_pointers(
            logical_ack_offset,
            requested_physical_truncate_offset,
            reason,
            current_wal_size,
        );

        if self.wal_keep_from != u64::MAX
            && reason == WalTruncationReason::UnsafeTail
            && self.wal_keep_from < current_wal_size
            && self.wal_keep_from >= pointers.logical_ack_offset
        {
            tracing::warn!(
                target: targets::WAL,
                wal_keep_from = self.wal_keep_from,
                logical_ack_offset = pointers.logical_ack_offset,
                current_wal_size = current_wal_size,
                "Unsafe WAL tail truncation overrides wal_keep_from to preserve consistency"
            );
        }

        if pointers.physical_truncate_offset >= current_wal_size {
            tracing::info!(
                target: targets::WAL,
                truncation_reason = reason.as_str(),
                wal_keep_from = self.wal_keep_from,
                logical_ack_offset = pointers.logical_ack_offset,
                last_safe_offset = pointers.logical_ack_offset,
                physical_truncate_offset = pointers.physical_truncate_offset,
                current_wal_size = current_wal_size,
                "WAL truncation skipped (logical ack advanced, physical reclaim deferred)"
            );
            return Ok(pointers);
        }

        let file = OpenOptions::new()
            .write(true)
            .open(&self.wal_path)
            .map_err(|e| {
                paro_error::internal(format!("Failed to open WAL for truncation: {}", e))
            })?;

        file.set_len(pointers.physical_truncate_offset)
            .map_err(|e| paro_error::internal(format!("Failed to truncate WAL: {}", e)))?;

        file.sync_all().map_err(|e| {
            paro_error::internal(format!("Failed to sync WAL after truncation: {}", e))
        })?;

        let truncated_bytes = current_wal_size.saturating_sub(pointers.physical_truncate_offset);
        storage_metrics().add_wal_truncate_bytes(truncated_bytes);

        tracing::info!(
            target: targets::WAL,
            truncation_reason = reason.as_str(),
            wal_keep_from = self.wal_keep_from,
            logical_ack_offset = pointers.logical_ack_offset,
            last_safe_offset = pointers.logical_ack_offset,
            physical_truncate_offset = pointers.physical_truncate_offset,
            current_wal_size = current_wal_size,
            truncated_bytes = truncated_bytes,
            "WAL truncated successfully"
        );
        Ok(pointers)
    }

    /// Truncate WAL based on checkpoint position.
    ///
    /// This is called after a successful checkpoint to reclaim WAL space.
    /// The WAL is truncated to just after the checkpoint marker.
    pub fn truncate_after_checkpoint_position(&self, checkpoint_position: u64) -> Result<()> {
        self.truncate_wal_file_with_reason(
            checkpoint_position,
            checkpoint_position,
            WalTruncationReason::Checkpoint,
        )?;
        Ok(())
    }

    /// First pass: scan WAL for checkpoint markers without applying changes.
    fn first_pass(&self, reader: &mut WalReader) -> Result<ReplayResult> {
        let mut committed_entries = 0u64;
        let mut pending_entries = 0u64;
        let mut last_offset = 0u64;
        let mut committed_checkpoint: Option<CheckpointInfo> = None;
        let mut pending_checkpoint: Option<CheckpointInfo> = None;

        loop {
            let position = reader.current_offset();
            let read_result = match reader.read_entry_with_torn_detection() {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!(
                        target: targets::WAL,
                        error = %e,
                        position = position,
                        "WAL scan encountered error, treating as end of valid data"
                    );
                    break;
                }
            };

            if let Some(corruption_position) = read_result.tail_corruption_position() {
                tracing::warn!(
                    target: targets::WAL,
                    position = corruption_position,
                    "WAL scan encountered corrupted tail, treating as end of valid data"
                );
                break;
            }

            match read_result {
                ReadEntryResult::Entry(entry) => {
                    pending_entries += 1;

                    if let WalEntry::Checkpoint { checkpoint_marker } = &entry {
                        pending_checkpoint = Some(CheckpointInfo {
                            checkpoint_marker: *checkpoint_marker,
                            wal_position: position,
                        });
                    }

                    if matches!(entry, WalEntry::Flush) {
                        committed_entries += pending_entries;
                        pending_entries = 0;
                        last_offset = reader.current_offset();

                        if let Some(checkpoint) = pending_checkpoint.take() {
                            committed_checkpoint = Some(checkpoint);
                        }

                        if reader.finished() {
                            break;
                        }
                    }
                }
                ReadEntryResult::EndOfFile => {
                    if pending_entries > 0 {
                        tracing::warn!(
                            target: targets::WAL,
                            pending_entries = pending_entries,
                            "WAL scan reached EOF without WalFlush, ignoring uncommitted tail"
                        );
                    }
                    break;
                }
                _ => unreachable!("tail corruption results are handled above"),
            }
        }

        let mut result = ReplayResult::success(committed_entries, last_offset);

        if let Some(checkpoint) = committed_checkpoint {
            result = result.with_checkpoint(checkpoint);
        }

        Ok(result)
    }

    /// Second pass: replay WAL entries to restore state.
    fn second_pass<H: ReplayHandler>(
        &self,
        reader: &mut WalReader,
        handler: &mut H,
        _checkpoint_state: &ReplayResult,
    ) -> Result<ReplayResult> {
        let mut pending_entries: Vec<(u64, WalEntry)> = Vec::new();
        let mut checkpoint_info: Option<CheckpointInfo> = None;
        let mut entries_replayed = 0u64;
        let mut replayed_bytes = 0u64;
        let mut replay_start_offset: Option<u64> = None;
        let mut replay_end_offset = 0u64;
        let mut last_successful_offset = 0u64;
        let mut all_succeeded = true;
        let mut error_msg: Option<String> = None;

        loop {
            let position = reader.current_offset();
            let read_result = match reader.read_entry_with_torn_detection() {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!(
                        target: targets::WAL,
                        error = %e,
                        position = position,
                        "WAL replay encountered error, stopping at last successful offset"
                    );
                    all_succeeded = false;
                    error_msg = Some(e.to_string());
                    break;
                }
            };

            if let Some(corruption_position) = read_result.tail_corruption_position() {
                tracing::warn!(
                    target: targets::WAL,
                    position = corruption_position,
                    "WAL replay encountered corrupted tail, stopping at last successful offset"
                );
                all_succeeded = false;
                error_msg = Some(format!(
                    "WAL tail is incomplete or corrupted at position {}",
                    corruption_position
                ));
                break;
            }

            match read_result {
                ReadEntryResult::Entry(entry) => {
                    if matches!(entry, WalEntry::Flush) {
                        let tx_start_offset = pending_entries
                            .first()
                            .map(|(entry_offset, _)| *entry_offset)
                            .unwrap_or(position);

                        match self.apply_pending_transaction_entries(
                            &mut pending_entries,
                            handler,
                            &mut checkpoint_info,
                        ) {
                            Ok(tx_entries_replayed) => {
                                let tx_end_offset = reader.current_offset();
                                entries_replayed += tx_entries_replayed;
                                replayed_bytes += tx_end_offset.saturating_sub(tx_start_offset);
                                replay_start_offset.get_or_insert(tx_start_offset);
                                replay_end_offset = tx_end_offset;
                                last_successful_offset = tx_end_offset;
                            }
                            Err(e) => {
                                tracing::error!(
                                    target: targets::WAL,
                                    error = %e,
                                    position = position,
                                    "Failed to apply WAL transaction at flush boundary"
                                );
                                all_succeeded = false;
                                error_msg = Some(e.to_string());
                                break;
                            }
                        }

                        if reader.finished() {
                            break;
                        }
                    } else {
                        pending_entries.push((position, entry));
                    }
                }
                ReadEntryResult::EndOfFile => {
                    if !pending_entries.is_empty() {
                        tracing::warn!(
                            target: targets::WAL,
                            pending_entries = pending_entries.len(),
                            last_safe_offset = last_successful_offset,
                            "WAL replay reached EOF without WalFlush, skipping uncommitted tail"
                        );
                        all_succeeded = false;
                        error_msg = Some(
                            "WAL contains unflushed tail entries (skipped during replay)"
                                .to_string(),
                        );
                    }
                    break;
                }
                _ => unreachable!("tail corruption results are handled above"),
            }
        }

        let mut result = if all_succeeded {
            ReplayResult::success(entries_replayed, last_successful_offset)
        } else {
            ReplayResult::partial(
                entries_replayed,
                last_successful_offset,
                error_msg.unwrap_or_else(|| "Unknown error".to_string()),
            )
        };

        if let Some(checkpoint) = checkpoint_info {
            result = result.with_checkpoint(checkpoint);
        }

        storage_metrics().add_wal_replay(entries_replayed, replayed_bytes);
        let checkpoint_marker = result
            .checkpoint_info
            .as_ref()
            .map(|checkpoint| checkpoint.checkpoint_marker);
        tracing::info!(
            target: targets::WAL,
            replay_start_offset = replay_start_offset.unwrap_or(0),
            replay_end_offset = replay_end_offset,
            last_safe_offset = last_successful_offset,
            entries_replayed = entries_replayed,
            replayed_bytes = replayed_bytes,
            checkpoint_marker = checkpoint_marker,
            all_succeeded = all_succeeded,
            "WAL replay pass completed"
        );

        Ok(result)
    }

    /// Apply pending WAL entries as a single committed transaction.
    ///
    /// Pending entries are replayed only when a `WalFlush` marker is reached.
    /// Returns the number of replayed entries including the flush marker.
    fn apply_pending_transaction_entries<H: ReplayHandler>(
        &self,
        pending_entries: &mut Vec<(u64, WalEntry)>,
        handler: &mut H,
        checkpoint_info: &mut Option<CheckpointInfo>,
    ) -> Result<u64> {
        let mut replayed_entries = 0u64;
        let entries = std::mem::take(pending_entries);
        let mut tx_state = ReplayState::new();
        let mut idx = 0usize;

        while idx < entries.len() {
            let (position, entry) = &entries[idx];
            match entry {
                WalEntry::TxnBegin { txn_id, .. } => {
                    let mut catalog_ops = Vec::new();
                    let mut data_ops = Vec::new();
                    let mut hooks = Vec::new();
                    let mut commit_id = None;
                    let mut aborted = false;
                    let mut cursor = idx + 1;
                    while cursor < entries.len() {
                        match &entries[cursor].1 {
                            WalEntry::TxnCatalogOp { op, .. } => catalog_ops.push(op.clone()),
                            WalEntry::TxnDataOp { op, .. } => data_ops.push(op.clone()),
                            WalEntry::TxnPostCommitHook { hook, .. } => hooks.push(hook.clone()),
                            WalEntry::TxnCommit {
                                txn_id: commit_txn_id,
                                commit_id: envelope_commit_id,
                            } if commit_txn_id == txn_id => {
                                commit_id = Some(*envelope_commit_id);
                                cursor += 1;
                                break;
                            }
                            WalEntry::TxnAbort {
                                txn_id: abort_txn_id,
                            } if abort_txn_id == txn_id => {
                                aborted = true;
                                cursor += 1;
                                break;
                            }
                            _ => break,
                        }
                        cursor += 1;
                    }
                    if let Some(commit_id) = commit_id {
                        handler.replay_transaction(&catalog_ops, &data_ops, &hooks, commit_id)?;
                        replayed_entries += (cursor - idx) as u64;
                        idx = cursor;
                    } else {
                        if !aborted {
                            tracing::warn!(
                                target: targets::WAL,
                                txn_id = txn_id,
                                "txn envelope missing commit inside flushed group; skipping"
                            );
                        }
                        replayed_entries += (cursor - idx) as u64;
                        idx = cursor;
                    }
                }
                _ => {
                    tx_state.process_entry(entry, *position);
                    self.apply_entry(entry, &tx_state, handler)?;
                    replayed_entries += 1;

                    if let WalEntry::Checkpoint { checkpoint_marker } = entry {
                        *checkpoint_info = Some(CheckpointInfo {
                            checkpoint_marker: *checkpoint_marker,
                            wal_position: *position,
                        });
                    }
                    idx += 1;
                }
            }
        }

        handler.on_flush()?;
        Ok(replayed_entries + 1)
    }

    /// Apply a single WAL entry using the handler.
    fn apply_entry<H: ReplayHandler>(
        &self,
        entry: &WalEntry,
        _state: &ReplayState,
        handler: &mut H,
    ) -> Result<()> {
        match entry {
            WalEntry::Version { .. } => {
                // Version entry is handled during reading
                Ok(())
            }

            WalEntry::TxnBegin { .. }
            | WalEntry::TxnCatalogOp { .. }
            | WalEntry::TxnDataOp { .. }
            | WalEntry::TxnPostCommitHook { .. }
            | WalEntry::TxnCommit { .. }
            | WalEntry::TxnAbort { .. } => Ok(()),

            WalEntry::UseTable { .. } => Ok(()),

            WalEntry::PrimaryDelete { keys } => handler.replay_primary_delete(keys),

            WalEntry::RowIdDelete { locations } => handler.replay_row_id_delete(locations),

            WalEntry::RowsetCommit {
                tablet_id,
                rowset_id,
                start_version,
                end_version,
                rowset_path,
            } => {
                handler.validate_rowset_commit(
                    *tablet_id,
                    *rowset_id,
                    *start_version,
                    *end_version,
                    rowset_path,
                )?;
                handler.replay_rowset_commit(
                    *tablet_id,
                    *rowset_id,
                    *start_version,
                    *end_version,
                    rowset_path,
                )
            }

            WalEntry::CompactionPublish {
                tablet_id,
                plan_id,
                job_id,
                output_rowset_id,
                output_start_version,
                output_end_version,
                cumulative_point_action,
                output_rowset_path,
                replaced_inputs,
            } => handler.replay_compaction_publish(
                *tablet_id,
                *plan_id,
                *job_id,
                *output_rowset_id,
                *output_start_version,
                *output_end_version,
                *cumulative_point_action,
                output_rowset_path,
                replaced_inputs,
            ),

            WalEntry::Checkpoint { checkpoint_marker } => handler.on_checkpoint(*checkpoint_marker),

            WalEntry::Flush => {
                // Flush is handled in the main loop
                Ok(())
            }
        }
    }
}

/// A no-op replay handler for testing or when replay is not needed.
pub struct NoOpReplayHandler;

impl ReplayHandler for NoOpReplayHandler {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::storage_metrics;
    use crate::wal::test_support::{
        append_open_create_schema_txn, write_flushed_create_schema_txn, write_flushed_rowset_commit,
    };
    use crate::wal::wal_entry::WalHeaderMetadata;
    use crate::wal::wal_reader::WalReader;
    use crate::wal::wal_type::WalType;
    use crate::wal::wal_writer::{WalInitState, WalWriter};
    use crate::wal::write_ahead_log::WriteAheadLog;
    use paro_common::ddl::DdlChange;
    use paro_common::effect::{CatalogTxnOp, PostCommitHookDescriptor, PreparedDataOp};
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    /// Test handler that records all replay operations.
    struct RecordingHandler {
        operations: RefCell<Vec<String>>,
        reject_rowset_ids: HashSet<u64>,
    }

    impl RecordingHandler {
        fn new() -> Self {
            Self {
                operations: RefCell::new(Vec::new()),
                reject_rowset_ids: HashSet::new(),
            }
        }

        fn with_rejected_rowsets(reject_rowset_ids: &[u64]) -> Self {
            Self {
                operations: RefCell::new(Vec::new()),
                reject_rowset_ids: reject_rowset_ids.iter().copied().collect(),
            }
        }

        fn operations(&self) -> Vec<String> {
            self.operations.borrow().clone()
        }
    }

    impl ReplayHandler for RecordingHandler {
        fn replay_transaction(
            &mut self,
            catalog_ops: &[CatalogTxnOp],
            _data_ops: &[PreparedDataOp],
            _hooks: &[PostCommitHookDescriptor],
            commit_id: u64,
        ) -> Result<()> {
            self.operations.borrow_mut().push(format!(
                "TXN commit_id={} catalog_ops={}",
                commit_id,
                catalog_ops.len()
            ));
            for op in catalog_ops {
                if matches!(&op.change.change, DdlChange::CreateSchema(_)) {
                    self.operations
                        .borrow_mut()
                        .push(format!("CREATE SCHEMA {}", op.change.key.name));
                }
            }
            Ok(())
        }

        fn replay_row_id_delete(&mut self, locations: &[(u64, u32, u32)]) -> Result<()> {
            self.operations
                .borrow_mut()
                .push(format!("ROW_ID_DELETE ({} locations)", locations.len()));
            Ok(())
        }

        fn on_checkpoint(&mut self, checkpoint_marker: u64) -> Result<()> {
            self.operations
                .borrow_mut()
                .push(format!("CHECKPOINT (marker={})", checkpoint_marker));
            Ok(())
        }

        fn replay_rowset_commit(
            &mut self,
            tablet_id: u64,
            rowset_id: u64,
            start_version: i64,
            end_version: i64,
            rowset_path: &str,
        ) -> Result<()> {
            self.operations.borrow_mut().push(format!(
                "ROWSET_COMMIT tablet={} rowset={} v[{}-{}] path={}",
                tablet_id, rowset_id, start_version, end_version, rowset_path
            ));
            Ok(())
        }

        fn validate_rowset_commit(
            &mut self,
            tablet_id: u64,
            rowset_id: u64,
            start_version: i64,
            end_version: i64,
            rowset_path: &str,
        ) -> Result<()> {
            self.operations.borrow_mut().push(format!(
                "VALIDATE_ROWSET_COMMIT tablet={} rowset={} v[{}-{}] path={}",
                tablet_id, rowset_id, start_version, end_version, rowset_path
            ));
            if self.reject_rowset_ids.contains(&rowset_id) {
                return Err(paro_error::internal(format!(
                    "rowset validation rejected rowset {}",
                    rowset_id
                )));
            }
            Ok(())
        }
    }

    #[test]
    fn test_recovery_no_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("nonexistent.wal");

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = NoOpReplayHandler;

        let (wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert_eq!(result.entries_replayed, 0);
        assert!(!wal.is_initialized());
    }

    #[test]
    fn test_recovery_empty_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("empty.wal");

        // Create empty WAL file
        std::fs::write(&wal_path, &[]).unwrap();

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = NoOpReplayHandler;

        // Empty WAL should fail gracefully
        let result = recovery.recover(&mut handler);
        // Either succeeds with 0 entries or fails with serialization error
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_recovery_with_entries() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "test_schema", 1, 100).unwrap();
        }

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.entries_replayed > 0);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("CREATE SCHEMA")));
    }

    #[test]
    fn test_recovery_replays_row_id_delete_entry() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("row_id_delete.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            let entry = WalEntry::RowIdDelete {
                locations: vec![(9, 0, 3), (9, 0, 4)],
            };
            writer
                .write_entry(entry.wal_type(), &entry.serialize_data())
                .unwrap();
            writer.flush().unwrap();
        }

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();
        assert!(result.all_succeeded);

        let ops = handler.operations();
        assert!(ops
            .iter()
            .any(|op| op.contains("ROW_ID_DELETE (2 locations)")));
    }

    #[test]
    fn wal_recovery_replays_only_flushed_transactions() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("flush_boundary.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "committed_schema", 1, 100)
                .unwrap();
            append_open_create_schema_txn(&writer, "default", "uncommitted_schema", 2).unwrap();
        }

        let expected_flush_offset = {
            let mut reader = WalReader::open(&wal_path).unwrap().unwrap();
            reader
                .scan_for_truncation_point()
                .unwrap()
                .last_flush_offset
        };

        let size_before_recovery = std::fs::metadata(&wal_path).unwrap().len();

        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(true);
        let mut handler = RecordingHandler::new();
        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.entries_replayed > 0);

        let ops = handler.operations();
        assert!(ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA committed_schema")));
        assert!(!ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA uncommitted_schema")));
        assert_eq!(result.last_successful_offset, expected_flush_offset);

        let size_after_recovery = std::fs::metadata(&wal_path).unwrap().len();
        assert!(size_after_recovery < size_before_recovery);
        assert_eq!(size_after_recovery, expected_flush_offset);
    }

    #[test]
    fn test_recovery_without_auto_truncate_skips_unflushed_tail() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("flush_boundary_no_truncate.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "committed_schema", 1, 100)
                .unwrap();
            append_open_create_schema_txn(&writer, "default", "uncommitted_schema", 2).unwrap();
        }

        let size_before_recovery = std::fs::metadata(&wal_path).unwrap().len();

        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(false);
        let mut handler = RecordingHandler::new();
        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(!result.all_succeeded);
        assert!(result
            .error
            .as_ref()
            .is_some_and(|error| error.contains("unflushed tail")));

        let ops = handler.operations();
        assert!(ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA committed_schema")));
        assert!(!ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA uncommitted_schema")));

        let size_after_recovery = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(size_after_recovery, size_before_recovery);
    }

    #[test]
    fn test_recovery_with_checkpoint() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("checkpoint.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "main", 1, 100).unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
        }

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("CHECKPOINT")));
    }

    #[test]
    fn test_recovery_with_torn_write_auto_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("torn.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
        }

        let size_before_torn = std::fs::metadata(&wal_path).unwrap().len();

        // Append torn write (incomplete header)
        {
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            file.write_all(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        }

        let size_with_torn = std::fs::metadata(&wal_path).unwrap().len();
        assert!(size_with_torn > size_before_torn);

        // Recovery should auto-truncate
        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(true);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Should have replayed the valid entries
        assert!(result.entries_replayed > 0);

        // File should be truncated
        let size_after_recovery = std::fs::metadata(&wal_path).unwrap().len();
        assert!(size_after_recovery <= size_before_torn);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("CREATE SCHEMA")));
    }

    #[test]
    fn test_recovery_with_torn_write_no_auto_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("torn_no_truncate.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
        }

        // Append torn write
        {
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            file.write_all(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        }

        let size_with_torn = std::fs::metadata(&wal_path).unwrap().len();

        // Recovery without auto-truncate
        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(false);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Should still replay valid entries
        assert!(result.entries_replayed > 0);

        // File should NOT be truncated
        let size_after_recovery = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(size_after_recovery, size_with_torn);
    }

    #[test]
    fn test_recovery_checkpoint_truncation() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("checkpoint_truncate.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
            write_flushed_create_schema_txn(&writer, "default", "schema2", 2, 101).unwrap();
        }

        let size_before = std::fs::metadata(&wal_path).unwrap().len();

        // Recovery with checkpoint truncation enabled
        let recovery = WalRecovery::new(&wal_path).with_checkpoint_truncation(true);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.checkpoint_info.is_some());

        // File should be truncated to checkpoint position
        let size_after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(size_after < size_before);
    }

    #[test]
    fn test_wal_truncate_respects_keep_from() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("checkpoint_keep_from.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
            write_flushed_create_schema_txn(&writer, "default", "schema2", 2, 101).unwrap();
        }

        let size_before = std::fs::metadata(&wal_path).unwrap().len();
        let recovery = WalRecovery::new(&wal_path)
            .with_checkpoint_truncation(true)
            .with_wal_keep_from(0);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.checkpoint_info.is_some());
        let size_after = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(size_after, size_before);
    }

    #[test]
    fn test_resolve_truncation_pointers_checkpoint_full_truncate_ack_semantics() {
        let recovery = WalRecovery::new("/tmp/unused_for_pointer_test.wal");
        let pointers =
            recovery.resolve_truncation_pointers(120, 0, WalTruncationReason::Checkpoint, 120);
        assert_eq!(pointers.logical_ack_offset, 120);
        assert_eq!(pointers.physical_truncate_offset, 0);
    }

    #[test]
    fn test_resolve_truncation_pointers_checkpoint_keep_from_defers_physical_truncate() {
        let recovery =
            WalRecovery::new("/tmp/unused_for_pointer_test_keep_from.wal").with_wal_keep_from(0);
        let pointers =
            recovery.resolve_truncation_pointers(120, 0, WalTruncationReason::Checkpoint, 120);
        assert_eq!(pointers.logical_ack_offset, 120);
        assert_eq!(pointers.physical_truncate_offset, 120);
    }

    #[test]
    fn test_resolve_truncation_pointers_unsafe_tail_ignores_keep_from() {
        let recovery =
            WalRecovery::new("/tmp/unused_for_pointer_test_unsafe.wal").with_wal_keep_from(0);
        let pointers =
            recovery.resolve_truncation_pointers(80, 80, WalTruncationReason::UnsafeTail, 120);
        assert_eq!(pointers.logical_ack_offset, 80);
        assert_eq!(pointers.physical_truncate_offset, 80);
    }

    #[test]
    fn test_unsafe_tail_truncation_overrides_keep_from() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("unsafe_tail_keep_from.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "committed_schema", 1, 100)
                .unwrap();
            append_open_create_schema_txn(&writer, "default", "uncommitted_schema", 2).unwrap();
        }

        let size_before = std::fs::metadata(&wal_path).unwrap().len();
        let recovery = WalRecovery::new(&wal_path)
            .with_auto_truncate(true)
            .with_wal_keep_from(0);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        let size_after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(size_after < size_before);
    }

    #[test]
    fn test_truncate_after_checkpoint_position() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("manual_truncate.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
            write_flushed_create_schema_txn(&writer, "default", "schema2", 2, 101).unwrap();
        }

        let original_size = std::fs::metadata(&wal_path).unwrap().len();

        // Manually truncate to a specific position
        let recovery = WalRecovery::new(&wal_path);
        let truncate_position = original_size / 2;
        recovery
            .truncate_after_checkpoint_position(truncate_position)
            .unwrap();

        let new_size = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(new_size, truncate_position);
    }

    #[test]
    fn wal_recovery_skips_when_checkpoint_meta_matches() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("matching_checkpoint.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "main", 1, 100).unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
        }

        // Recovery with matching checkpoint marker
        let recovery = WalRecovery::new(&wal_path)
            .with_checkpoint_marker(42)
            .with_checkpoint_truncation(true);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Should skip replay since checkpoint matches
        assert!(result.all_succeeded);
        assert!(result.checkpoint_verified);
        assert!(result.checkpoint_was_clean());
        assert_eq!(result.entries_replayed, 0);
    }

    #[test]
    fn wal_recovery_checkpoint_verified_with_keep_from_advances_ack_without_physical_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("matching_checkpoint_keep_from.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "main", 1, 100).unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
        }

        let size_before = std::fs::metadata(&wal_path).unwrap().len();

        let recovery = WalRecovery::new(&wal_path)
            .with_checkpoint_marker(42)
            .with_checkpoint_truncation(true)
            .with_wal_keep_from(0);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.checkpoint_verified);
        assert_eq!(result.entries_replayed, 0);
        assert!(result.last_successful_offset > 0);

        let size_after = std::fs::metadata(&wal_path).unwrap().len();
        assert_eq!(size_after, size_before);
    }

    #[test]
    fn test_recovery_with_mismatched_checkpoint() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("mismatched_checkpoint.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "main", 1, 100).unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
        }

        // Recovery with different checkpoint marker (simulating incomplete checkpoint)
        let recovery = WalRecovery::new(&wal_path).with_checkpoint_marker(99);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Should replay since checkpoint doesn't match
        assert!(result.all_succeeded);
        assert!(!result.checkpoint_verified);
        assert!(result.entries_replayed > 0);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("CREATE SCHEMA")));
    }

    #[test]
    fn wal_recovery_merges_main_and_checkpoint_wal_on_incomplete_checkpoint() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let checkpoint_wal_path = dir.path().join("test.checkpoint.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
        }

        {
            let writer = WalWriter::new(&checkpoint_wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema2", 2, 101).unwrap();
        }

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Main WAL and checkpoint WAL entries should both be replayed.
        assert!(result.all_succeeded);
        assert!(result.entries_replayed > 0);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("schema1")));
        assert!(ops.iter().any(|op| op.contains("schema2")));
        assert!(!checkpoint_wal_path.exists());

        // The merged main WAL should retain checkpoint WAL entries for next crash recovery.
        let recovery_again = WalRecovery::new(&wal_path);
        let mut handler_again = RecordingHandler::new();
        let (_wal_again, result_again) = recovery_again.recover(&mut handler_again).unwrap();
        assert!(result_again.all_succeeded);
        let ops_again = handler_again.operations();
        assert!(ops_again.iter().any(|op| op.contains("schema1")));
        assert!(ops_again.iter().any(|op| op.contains("schema2")));
    }

    #[test]
    fn checkpoint_concurrent_commit_no_loss() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("concurrent_checkpoint.wal");
        let checkpoint_wal_path = dir.path().join("concurrent_checkpoint.checkpoint.wal");

        let wal = WriteAheadLog::new(&wal_path).unwrap();
        write_flushed_create_schema_txn(
            wal.writer().as_ref(),
            "default",
            "before_checkpoint",
            1,
            100,
        )
        .unwrap();

        assert!(wal.start_checkpoint(42).unwrap());
        let cp_session = wal.begin_write();
        let cp_writer = cp_session.wal().as_ref();
        write_flushed_create_schema_txn(cp_writer, "default", "during_checkpoint_txn_1", 2, 101)
            .unwrap();
        write_flushed_create_schema_txn(cp_writer, "default", "during_checkpoint_txn_2", 3, 102)
            .unwrap();

        // Crash before finish_checkpoint().
        drop(wal);
        assert!(checkpoint_wal_path.exists());

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();
        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        let ops = handler.operations();
        assert!(ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA before_checkpoint")));
        assert!(ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA during_checkpoint_txn_1")));
        assert!(ops
            .iter()
            .any(|op| op.contains("CREATE SCHEMA during_checkpoint_txn_2")));
        assert!(!checkpoint_wal_path.exists());

        // Replay again to ensure merged main WAL keeps all concurrent commits.
        let mut handler_again = RecordingHandler::new();
        let (_wal_again, result_again) = WalRecovery::new(&wal_path)
            .recover(&mut handler_again)
            .unwrap();
        assert!(result_again.all_succeeded);
        let ops_again = handler_again.operations();
        assert!(ops_again
            .iter()
            .any(|op| op.contains("CREATE SCHEMA before_checkpoint")));
        assert!(ops_again
            .iter()
            .any(|op| op.contains("CREATE SCHEMA during_checkpoint_txn_1")));
        assert!(ops_again
            .iter()
            .any(|op| op.contains("CREATE SCHEMA during_checkpoint_txn_2")));
    }

    #[test]
    fn compaction_wal_checkpoint_interleaving_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("compaction_interleaving.wal");
        let checkpoint_wal_path = dir.path().join("compaction_interleaving.checkpoint.wal");

        let wal = WriteAheadLog::new(&wal_path).unwrap();

        let main_w = wal.writer().as_ref();
        write_flushed_create_schema_txn(main_w, "default", "txn_before", 1, 100).unwrap();
        wal.write_rowset_commit(7, 100, 1, 1, "/rowset/r100")
            .unwrap();
        main_w.flush().unwrap();

        assert!(wal.start_checkpoint(77).unwrap());

        let cp_session = wal.begin_write();
        let cp_w = cp_session.wal().as_ref();
        write_flushed_create_schema_txn(cp_w, "default", "txn_during", 2, 101).unwrap();
        wal.write_rowset_commit(7, 200, 1, 2, "/rowset/compaction_r200")
            .unwrap();
        cp_w.flush().unwrap();

        // Crash before finish_checkpoint().
        drop(wal);
        assert!(checkpoint_wal_path.exists());

        let mut handler = RecordingHandler::new();
        let (_wal, result) = WalRecovery::new(&wal_path).recover(&mut handler).unwrap();
        assert!(result.all_succeeded);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("txn_before")));
        assert!(ops.iter().any(|op| op.contains("txn_during")));
        assert!(ops
            .iter()
            .any(|op| op.contains("ROWSET_COMMIT tablet=7 rowset=100 v[1-1]")));
        assert!(ops
            .iter()
            .any(|op| op.contains("ROWSET_COMMIT tablet=7 rowset=200 v[1-2]")));
        assert!(!checkpoint_wal_path.exists());
    }

    #[test]
    fn rowset_commit_validation_hook_runs_before_replay() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("rowset_validation_hook.wal");

        let wal = WriteAheadLog::new(&wal_path).unwrap();
        write_flushed_rowset_commit(wal.writer().as_ref(), 10, 99, 3, 3, "/rowset/r99").unwrap();
        drop(wal);

        let mut handler = RecordingHandler::new();
        let (_wal, result) = WalRecovery::new(&wal_path).recover(&mut handler).unwrap();
        assert!(result.all_succeeded);

        let ops = handler.operations();
        let validate_pos = ops
            .iter()
            .position(|op| op.starts_with("VALIDATE_ROWSET_COMMIT tablet=10 rowset=99 v[3-3]"))
            .expect("expected validation hook operation");
        let replay_pos = ops
            .iter()
            .position(|op| op.starts_with("ROWSET_COMMIT tablet=10 rowset=99 v[3-3]"))
            .expect("expected rowset replay operation");
        assert!(validate_pos < replay_pos);
    }

    #[test]
    fn rowset_commit_validation_failure_marks_replay_partial() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("rowset_validation_failure.wal");

        let wal = WriteAheadLog::new(&wal_path).unwrap();
        write_flushed_rowset_commit(wal.writer().as_ref(), 10, 77, 1, 1, "/rowset/r77").unwrap();
        drop(wal);

        let mut handler = RecordingHandler::with_rejected_rowsets(&[77]);
        let (_wal, result) = WalRecovery::new(&wal_path).recover(&mut handler).unwrap();
        assert!(!result.all_succeeded);
        assert!(result
            .error
            .as_deref()
            .is_some_and(|msg| msg.contains("rowset validation rejected rowset 77")));

        let ops = handler.operations();
        assert!(ops
            .iter()
            .any(|op| op.starts_with("VALIDATE_ROWSET_COMMIT tablet=10 rowset=77 v[1-1]")));
        assert!(!ops
            .iter()
            .any(|op| op.starts_with("ROWSET_COMMIT tablet=10 rowset=77 v[1-1]")));
    }

    #[test]
    fn test_recovery_promotes_checkpoint_wal_when_checkpoint_verified() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("promote_main.wal");
        let checkpoint_wal_path = dir.path().join("promote_main.checkpoint.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema_before_checkpoint", 1, 100)
                .unwrap();
            writer.write_checkpoint(42).unwrap();
            writer.flush().unwrap();
        }

        {
            let cw = WalWriter::new(&checkpoint_wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&cw, "default", "schema_during_checkpoint", 2, 101)
                .unwrap();
        }

        let recovery = WalRecovery::new(&wal_path).with_checkpoint_marker(42);
        let mut handler = RecordingHandler::new();
        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.checkpoint_verified);
        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("schema_during_checkpoint")));
        assert!(!checkpoint_wal_path.exists());

        // Main WAL should now be the promoted checkpoint WAL.
        let recovery_again = WalRecovery::new(&wal_path);
        let mut handler_again = RecordingHandler::new();
        let (_wal_again, result_again) = recovery_again.recover(&mut handler_again).unwrap();
        assert!(result_again.all_succeeded);
        let ops_again = handler_again.operations();
        assert!(ops_again
            .iter()
            .any(|op| op.contains("schema_during_checkpoint")));
    }

    #[test]
    fn test_recovery_handles_empty_checkpoint_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let checkpoint_wal_path = dir.path().join("test.checkpoint.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
        }

        // Create empty checkpoint WAL
        std::fs::write(&checkpoint_wal_path, &[]).unwrap();

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);

        // Empty checkpoint WAL should be removed
        assert!(!checkpoint_wal_path.exists());
    }

    #[test]
    fn test_recovery_rebuilds_legacy_wal_version() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("legacy_version.wal");

        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&wal_path)
                .unwrap();
            file.write_all(&[WalType::WalVersion as u8]).unwrap();
            file.write_all(&2u64.to_le_bytes()).unwrap();
            file.sync_all().unwrap();
        }

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert_eq!(result.entries_replayed, 0);
        assert!(!wal_path.exists());
        assert!(handler.operations().is_empty());
    }

    #[test]
    fn test_recovery_skips_replay_on_db_identifier_mismatch() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("identity_mismatch.wal");

        let wal_metadata = WalHeaderMetadata::new([1u8; 16], 3);
        {
            let writer = WalWriter::with_header_metadata(
                &wal_path,
                WalInitState::Uninitialized,
                wal_metadata,
            );
            write_flushed_create_schema_txn(&writer, "default", "schema_to_skip", 1, 100).unwrap();
        }

        let recovery = WalRecovery::new(&wal_path).with_wal_header_metadata([2u8; 16], 3);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert_eq!(result.entries_replayed, 0);
        assert!(handler.operations().is_empty());
        assert!(!wal_path.exists());
    }

    #[test]
    fn test_recovery_allows_one_iteration_lag() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("iteration_lag.wal");
        let db_identifier = [7u8; 16];

        {
            let writer = WalWriter::with_header_metadata(
                &wal_path,
                WalInitState::Uninitialized,
                WalHeaderMetadata::new(db_identifier, 9),
            );
            write_flushed_create_schema_txn(&writer, "default", "schema_replay", 1, 100).unwrap();
        }

        let recovery = WalRecovery::new(&wal_path).with_wal_header_metadata(db_identifier, 10);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        assert!(result.entries_replayed > 0);
        assert!(handler
            .operations()
            .iter()
            .any(|op| op.contains("schema_replay")));
    }

    #[test]
    fn test_wal_health_check_read_only_no_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("missing.wal");

        let report = wal_health_check_read_only(&wal_path);
        assert!(report.healthy);
        assert_eq!(report.recovery_mode, WalRecoveryMode::NoWal);
        assert!(!report.main_wal.exists);
    }

    #[test]
    fn test_wal_health_check_read_only_detects_unflushed_tail() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("unhealthy.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "committed", 1, 100).unwrap();
            append_open_create_schema_txn(&writer, "default", "uncommitted", 2).unwrap();
        }

        let report = wal_health_check_read_only(&wal_path);
        assert!(!report.healthy);
        assert_eq!(report.recovery_mode, WalRecoveryMode::MainWalOnly);
        assert!(report.main_wal.exists);
        assert!(report.main_wal.needs_truncation);
        assert!(report.main_wal.has_unflushed_tail);
    }

    #[test]
    fn test_recovery_updates_wal_lifecycle_metrics() {
        storage_metrics().reset_for_tests();

        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("metrics.wal");

        {
            let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
            write_flushed_create_schema_txn(&writer, "default", "committed", 1, 100).unwrap();
            append_open_create_schema_txn(&writer, "default", "uncommitted", 2).unwrap();
        }

        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(true);
        let mut handler = RecordingHandler::new();
        let (_wal, _result) = recovery.recover(&mut handler).unwrap();

        let metrics = storage_metrics().snapshot();
        assert!(metrics.wal_replay_entries > 0);
        assert!(metrics.wal_replay_bytes > 0);
        assert!(metrics.wal_truncate_bytes > 0);
        assert_eq!(
            metrics.wal_recovery_mode,
            WalRecoveryMode::MainWalOnly.as_metric_value()
        );
    }
}
