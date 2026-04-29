// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical WAL replay, torn-write repair, and read-only segment health
//! probes.

use std::fs::OpenOptions;
use std::path::Path;

use crate::segments::{ReplayCursor, SegmentCatalogStore};
use crate::wal::replay_state::{ReplayResult, ReplayState};
use crate::wal::wal_entry::{WalEntry, WalHeaderMetadata};
use crate::wal::wal_reader::{ReadEntryResult, WalReader};
use crate::wal::wal_writer::{WalInitState, WAL_VERSION_NUMBER};
use crate::wal::write_ahead_log::WriteAheadLog;
use paro_common::effect::{
    CatalogTxnOp, CompactionCumulativePointAction, PostCommitHookDescriptor, PreparedDataOp,
};
use paro_common::error as paro_error;
use paro_common::error::Result;
use paro_common::journal::JournalRecord;
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

    /// Durable journal record emitted by the segment-based journal path.
    fn replay_journal_record(&mut self, _lsn: u64, _record: &JournalRecord) -> Result<()> {
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
    #[allow(clippy::too_many_arguments)]
    fn replay_compaction_publish(
        &mut self,
        _tablet_id: u64,
        _plan_id: u64,
        _job_id: u64,
        _output_rowset_id: u64,
        _output_start_version: i64,
        _output_end_version: i64,
        _cumulative_point_action: CompactionCumulativePointAction,
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
}

/// Recovery mode observed by WAL lifecycle logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecoveryMode {
    Unknown,
    NoWal,
    MainWalOnly,
}

impl WalRecoveryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            WalRecoveryMode::Unknown => "unknown",
            WalRecoveryMode::NoWal => "no_wal",
            WalRecoveryMode::MainWalOnly => "main_wal_only",
        }
    }

    pub fn as_metric_value(self) -> u64 {
        match self {
            WalRecoveryMode::Unknown => 0,
            WalRecoveryMode::NoWal => 1,
            WalRecoveryMode::MainWalOnly => 2,
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

/// Aggregated WAL health report for the segment-backed main WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHealthCheckReport {
    pub recovery_mode: WalRecoveryMode,
    pub healthy: bool,
    pub main_wal: WalFileHealthReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalTruncationReason {
    UnsafeTail,
}

impl WalTruncationReason {
    fn as_str(self) -> &'static str {
        match self {
            WalTruncationReason::UnsafeTail => "unsafe_tail",
        }
    }
}

/// Dual WAL truncation pointers for torn-tail repair.
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

    let main_wal = inspect_wal_file_read_only(main_wal_path);
    let recovery_mode = if main_wal.exists {
        WalRecoveryMode::MainWalOnly
    } else {
        WalRecoveryMode::NoWal
    };
    let healthy = main_wal.is_healthy();

    tracing::info!(
        target: targets::WAL,
        recovery_mode = recovery_mode.as_str(),
        healthy = healthy,
        main_exists = main_wal.exists,
        main_last_safe_offset = main_wal.last_safe_offset,
        "WAL health check completed (read-only)"
    );

    WalHealthCheckReport {
        recovery_mode,
        healthy,
        main_wal,
    }
}

/// Inspect a segment-catalog-backed WAL in read-only mode.
///
/// When a segment catalog is present, every physical segment is scanned and the
/// result is folded into one aggregate health report for instance-level
/// observability. Missing or not-yet-initialized seed paths still report as
/// healthy/no-WAL. Legacy standalone seed files are surfaced as unsupported.
pub fn segment_catalog_health_check_read_only<P: AsRef<Path>>(
    seed_path: P,
) -> WalHealthCheckReport {
    let seed_path = seed_path.as_ref();
    let store = SegmentCatalogStore::from_seed_path(seed_path);
    let Ok(Some(catalog)) = store.load() else {
        if seed_path.exists() {
            let mut report = WalFileHealthReport::missing(seed_path);
            report.exists = true;
            report.size_bytes = std::fs::metadata(seed_path).map(|m| m.len()).unwrap_or(0);
            report.error = Some(format!(
                "legacy single-file WAL layout is unsupported at {}; expected segment catalog {}",
                seed_path.display(),
                store.layout().catalog_path().display()
            ));
            return WalHealthCheckReport {
                recovery_mode: WalRecoveryMode::MainWalOnly,
                healthy: false,
                main_wal: report,
            };
        }
        return wal_health_check_read_only(seed_path);
    };

    let mut segments = catalog.segments;
    segments.sort_by_key(|segment| segment.segment_id);

    let mut wal_version = None;
    let mut version_mismatch = false;
    let mut aggregated = WalFileHealthReport::missing(seed_path);
    aggregated.path = seed_path.display().to_string();

    for segment in segments {
        let segment_path = store.layout().segment_path(segment.segment_id);
        let report = inspect_wal_file_read_only(&segment_path);
        if !report.exists && aggregated.error.is_none() {
            aggregated.exists = true;
            aggregated.error = Some(format!(
                "segment catalog references missing WAL segment {} at {}",
                segment.segment_id,
                segment_path.display()
            ));
        }
        aggregated.exists |= report.exists;
        aggregated.size_bytes = aggregated.size_bytes.saturating_add(report.size_bytes);
        aggregated.entries_scanned = aggregated
            .entries_scanned
            .saturating_add(report.entries_scanned);
        aggregated.needs_truncation |= report.needs_truncation;
        aggregated.has_unflushed_tail |= report.has_unflushed_tail;
        aggregated.last_safe_offset = report.last_safe_offset;

        if aggregated.torn_write_position.is_none() {
            aggregated.torn_write_position = report.torn_write_position;
        }
        if aggregated.error.is_none() {
            aggregated.error = report.error.clone();
        }

        if let Some(version) = report.wal_version {
            match wal_version {
                None => wal_version = Some(version),
                Some(existing) if existing == version => {}
                Some(_) => version_mismatch = true,
            }
        }
    }

    aggregated.wal_version = if version_mismatch { None } else { wal_version };

    WalHealthCheckReport {
        recovery_mode: if aggregated.exists {
            WalRecoveryMode::MainWalOnly
        } else {
            WalRecoveryMode::NoWal
        },
        healthy: aggregated.is_healthy(),
        main_wal: aggregated,
    }
}

/// Physical WAL recovery engine.
///
/// `recover()` operates on a segment-catalog seed path and replays every
/// physical segment selected by the catalog. `replay_only()` is the lower-level
/// helper for replaying one explicit physical WAL segment file.
pub struct WalRecovery {
    /// Seed path for segment-catalog recovery, or an explicit physical segment
    /// path when using `replay_only()`.
    wal_path: String,
    /// Whether to automatically truncate torn writes
    auto_truncate_torn_writes: bool,
    /// Expected WAL header metadata from database file identity.
    expected_wal_header_metadata: Option<WalHeaderMetadata>,
    /// Retention threshold carried through recovery.
    ///
    /// Unsafe-tail repair still truncates past this bound to preserve consistency.
    wal_keep_from: u64,
    /// Logical LSN assigned to the first committed record in this physical WAL.
    logical_start_lsn: u64,
    /// Logical replay lower bound. Records below this LSN are scanned but not applied.
    replay_from_lsn: u64,
}

impl WalRecovery {
    /// Create a new recovery engine for the given WAL path.
    pub fn new<P: AsRef<Path>>(wal_path: P) -> Self {
        Self {
            wal_path: wal_path.as_ref().to_string_lossy().to_string(),
            auto_truncate_torn_writes: true,
            expected_wal_header_metadata: None,
            wal_keep_from: u64::MAX,
            logical_start_lsn: 1,
            replay_from_lsn: 1,
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

    /// Set keep-from retention threshold recorded alongside recovery.
    ///
    /// This mirrors the `wal_keep_from` contract for retention.
    pub fn with_wal_keep_from(mut self, wal_keep_from: u64) -> Self {
        self.wal_keep_from = wal_keep_from;
        self
    }

    /// Configure the logical LSN range covered by this physical WAL replay.
    pub fn with_replay_lsn_bounds(mut self, logical_start_lsn: u64, replay_from_lsn: u64) -> Self {
        self.logical_start_lsn = logical_start_lsn.max(1);
        self.replay_from_lsn = replay_from_lsn.max(self.logical_start_lsn);
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

    /// Build a new segment-backed WAL instance with optional identity metadata.
    fn create_wal(&self, init_state: WalInitState) -> Result<WriteAheadLog> {
        match self.expected_wal_header_metadata {
            Some(metadata) => {
                WriteAheadLog::with_state_and_header_metadata(&self.wal_path, init_state, metadata)
            }
            None => WriteAheadLog::with_state(&self.wal_path, init_state),
        }
    }

    fn segment_catalog_store(&self) -> SegmentCatalogStore {
        SegmentCatalogStore::from_seed_path(&self.wal_path)
    }

    fn legacy_seed_layout_error(&self, store: &SegmentCatalogStore) -> paro_error::ParoError {
        paro_error::not_supported(format!(
            "legacy single-file WAL layout is unsupported at {}; expected segment catalog {}",
            self.wal_path,
            store.layout().catalog_path().display()
        ))
    }

    /// Validate WAL header metadata against expected database identity.
    fn validate_wal_header_metadata(&self, actual: WalHeaderMetadata) -> Result<()> {
        let Some(expected) = self.expected_wal_header_metadata else {
            return Ok(());
        };

        if actual.db_identifier != expected.db_identifier {
            let message = format!(
                "WAL database identity mismatch: wal={:?}, expected={:?}",
                actual.db_identifier, expected.db_identifier
            );
            tracing::error!(
                target: targets::WAL,
                wal_db_identifier = ?actual.db_identifier,
                expected_db_identifier = ?expected.db_identifier,
                "{message}"
            );
            return Err(paro_error::data_corrupted(message));
        }

        if actual.checkpoint_iteration == expected.checkpoint_iteration {
            return Ok(());
        }

        if actual.checkpoint_iteration.saturating_add(1) == expected.checkpoint_iteration {
            tracing::warn!(
                target: targets::WAL,
                wal_checkpoint_iteration = actual.checkpoint_iteration,
                expected_checkpoint_iteration = expected.checkpoint_iteration,
                "WAL checkpoint iteration is one behind data file; continuing replay"
            );
            return Ok(());
        }

        let message = format!(
            "WAL checkpoint iteration mismatch: wal={}, expected={}",
            actual.checkpoint_iteration, expected.checkpoint_iteration
        );
        tracing::error!(
            target: targets::WAL,
            wal_checkpoint_iteration = actual.checkpoint_iteration,
            expected_checkpoint_iteration = expected.checkpoint_iteration,
            "{message}"
        );
        Err(paro_error::data_corrupted(message))
    }

    /// Ensure recovery only replays the current segment-catalog WAL format.
    fn validate_replayable_wal(&self, reader: &WalReader) -> Result<()> {
        if reader.wal_version() != WAL_VERSION_NUMBER {
            return Err(paro_error::not_supported(format!(
                "unsupported WAL version {}; only segment-catalog WAL version {} is replayable; requires a clean data directory or a fresh checkpoint written by unified Txn* journal",
                reader.wal_version(),
                WAL_VERSION_NUMBER
            )));
        }

        self.validate_wal_header_metadata(reader.header_metadata())
    }

    /// Perform WAL recovery.
    ///
    /// This is the main entry point for recovery. It:
    /// 1. Loads the segment catalog from the WAL seed path
    /// 2. Replays each physical segment in logical-LSN order
    /// 3. Repairs unsafe tails on the active segment when configured
    /// 4. Returns a segment-backed `WriteAheadLog` instance for continued use
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
        let catalog_store = self.segment_catalog_store();
        tracing::debug!(
            target: targets::WAL,
            wal_seed_path = %self.wal_path,
            catalog_path = %catalog_store.layout().catalog_path().display(),
            "Starting WAL recovery"
        );

        let replay_cursor = match catalog_store.load()? {
            Some(_) => ReplayCursor::from_catalog(
                &catalog_store,
                0,
                self.replay_from_lsn.max(self.logical_start_lsn),
            )?,
            None if Path::new(&self.wal_path).exists() => {
                return Err(self.legacy_seed_layout_error(&catalog_store));
            }
            None => ReplayCursor::default(),
        };
        let recovery_mode = if replay_cursor.is_empty() {
            WalRecoveryMode::NoWal
        } else {
            WalRecoveryMode::MainWalOnly
        };
        tracing::info!(
            target: targets::WAL,
            recovery_mode = recovery_mode.as_str(),
            replay_segment_count = replay_cursor.entries().len(),
            "WAL recovery mode selected"
        );

        let mut result = ReplayResult::success(0, 0);
        for entry in replay_cursor.entries() {
            let mut segment_recovery = WalRecovery::new(&entry.path)
                .with_auto_truncate(self.auto_truncate_torn_writes)
                .with_wal_keep_from(self.wal_keep_from)
                .with_replay_lsn_bounds(entry.starting_lsn, entry.replay_from_lsn);
            if let Some(metadata) = self.expected_wal_header_metadata {
                segment_recovery = segment_recovery.with_wal_header_metadata(
                    metadata.db_identifier,
                    metadata.checkpoint_iteration,
                );
            }
            let segment_result = segment_recovery.replay_only(handler)?;
            result.entries_replayed = result
                .entries_replayed
                .saturating_add(segment_result.entries_replayed);
            result.last_successful_offset = segment_result.last_successful_offset;
            if !segment_result.all_succeeded {
                result.all_succeeded = false;
                if result.error.is_none() {
                    result.error = segment_result.error.clone();
                }
                break;
            }
        }

        let init_state = if result.all_succeeded {
            WalInitState::Uninitialized
        } else {
            WalInitState::UninitializedRequiresTruncate
        };
        let wal = self.create_wal(init_state)?;

        tracing::info!(
            target: targets::WAL,
            recovery_mode = recovery_mode.as_str(),
            entries_replayed = result.entries_replayed,
            last_safe_offset = result.last_successful_offset,
            "WAL recovery finished"
        );

        Ok((wal, result))
    }

    /// Replay an existing physical WAL file without constructing a follow-up WAL handle.
    ///
    /// Returns an empty replay result when the physical file does not exist.
    /// The segment catalog may reference an active segment whose file has not
    /// been created yet (the WAL writer is lazy), so a missing file is not
    /// necessarily corruption — it may simply mean nothing was ever written.
    pub fn replay_only<H: ReplayHandler>(&self, handler: &mut H) -> Result<ReplayResult> {
        let Some(mut reader) = WalReader::open(&self.wal_path)? else {
            return Ok(ReplayResult::success(0, 0));
        };

        reader.ensure_header_read()?;
        self.validate_replayable_wal(&reader)?;
        self.recover_from_wal(&mut reader, handler)
    }

    /// Perform recovery from an existing WAL file.
    fn recover_from_wal<H: ReplayHandler>(
        &self,
        reader: &mut WalReader,
        handler: &mut H,
    ) -> Result<ReplayResult> {
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
            return Ok(ReplayResult::success(0, 0));
        }

        // Step 3: Reset reader for replay
        reader.reset()?;

        // Step 4: Actually replay entries
        let result = self.second_pass(reader, handler)?;

        Ok(result)
    }

    fn resolve_truncation_pointers(
        &self,
        logical_ack_offset: u64,
        requested_physical_truncate_offset: u64,
        _reason: WalTruncationReason,
        current_wal_size: u64,
    ) -> WalTruncationPointers {
        let logical_ack_offset = logical_ack_offset.min(current_wal_size);
        let mut physical_truncate_offset = requested_physical_truncate_offset.min(current_wal_size);

        // Never keep bytes beyond the logical replay-safe ack point.
        physical_truncate_offset = physical_truncate_offset.min(logical_ack_offset);

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

    /// Second pass: replay WAL entries to restore state.
    fn second_pass<H: ReplayHandler>(
        &self,
        reader: &mut WalReader,
        handler: &mut H,
    ) -> Result<ReplayResult> {
        let mut pending_entries: Vec<(u64, WalEntry)> = Vec::new();
        let mut entries_replayed = 0u64;
        let mut replayed_bytes = 0u64;
        let mut replay_start_offset: Option<u64> = None;
        let mut replay_end_offset = 0u64;
        let mut last_successful_offset = 0u64;
        let mut all_succeeded = true;
        let mut error_msg: Option<String> = None;
        let mut next_lsn = self.logical_start_lsn;

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
                            &mut next_lsn,
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

        let result = if all_succeeded {
            ReplayResult::success(entries_replayed, last_successful_offset)
        } else {
            ReplayResult::partial(
                entries_replayed,
                last_successful_offset,
                error_msg.unwrap_or_else(|| "Unknown error".to_string()),
            )
        };

        tracing::info!(
            target: targets::WAL,
            replay_start_offset = replay_start_offset.unwrap_or(0),
            replay_end_offset = replay_end_offset,
            last_safe_offset = last_successful_offset,
            entries_replayed = entries_replayed,
            replayed_bytes = replayed_bytes,
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
        next_lsn: &mut u64,
    ) -> Result<u64> {
        let mut replayed_entries = 0u64;
        let entries = std::mem::take(pending_entries);
        let mut tx_state = ReplayState::new();
        let mut idx = 0usize;

        while idx < entries.len() {
            let (position, entry) = &entries[idx];
            match entry {
                WalEntry::JournalRecord { lsn, record } => {
                    *next_lsn = (*next_lsn).max(lsn.saturating_add(1));
                    if *lsn >= self.replay_from_lsn {
                        handler.replay_journal_record(*lsn, record)?;
                        replayed_entries += 1;
                    }
                    idx += 1;
                }
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
                        let logical_lsn = *next_lsn;
                        *next_lsn = (*next_lsn).saturating_add(1);
                        if logical_lsn >= self.replay_from_lsn {
                            handler.replay_transaction(
                                &catalog_ops,
                                &data_ops,
                                &hooks,
                                commit_id,
                            )?;
                            replayed_entries += 1;
                        }
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
                    tx_state.process_entry(*position);
                    let should_apply = !matches!(entry, WalEntry::Version { .. } | WalEntry::Flush);
                    let logical_lsn = if should_apply {
                        let logical_lsn = *next_lsn;
                        *next_lsn = (*next_lsn).saturating_add(1);
                        Some(logical_lsn)
                    } else {
                        None
                    };
                    if logical_lsn.is_none_or(|logical_lsn| logical_lsn >= self.replay_from_lsn) {
                        self.apply_entry(entry, &tx_state, handler)?;
                        if should_apply {
                            replayed_entries += 1;
                        }
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
            | WalEntry::TxnAbort { .. }
            | WalEntry::JournalRecord { .. } => Ok(()),

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
    use paro_common::journal::JournalRecord;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn create_segment_wal(wal_path: &Path) -> (WriteAheadLog, PathBuf) {
        let wal = WriteAheadLog::new(wal_path).unwrap();
        let active_segment_path = wal.writer().path().to_path_buf();
        (wal, active_segment_path)
    }

    fn create_segment_wal_with_header_metadata(
        wal_path: &Path,
        metadata: WalHeaderMetadata,
    ) -> (WriteAheadLog, PathBuf) {
        let wal = WriteAheadLog::new_with_header_metadata(wal_path, metadata).unwrap();
        let active_segment_path = wal.writer().path().to_path_buf();
        (wal, active_segment_path)
    }

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

        fn record_catalog_ops(&self, catalog_ops: &[CatalogTxnOp], commit_id: u64) {
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
            self.record_catalog_ops(catalog_ops, commit_id);
            Ok(())
        }

        fn replay_journal_record(&mut self, _lsn: u64, record: &JournalRecord) -> Result<()> {
            if let JournalRecord::Commit(commit) = record {
                self.record_catalog_ops(&commit.catalog_ops, commit.commit_id);
            }
            Ok(())
        }

        fn replay_row_id_delete(&mut self, locations: &[(u64, u32, u32)]) -> Result<()> {
            self.operations
                .borrow_mut()
                .push(format!("ROW_ID_DELETE ({} locations)", locations.len()));
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
    fn test_recovery_rejects_legacy_single_file_seed_layout() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("empty.wal");

        // A bare seed-path file is a removed legacy layout: recovery must
        // reject it instead of silently opening it as a standalone WAL.
        std::fs::write(&wal_path, []).unwrap();

        let recovery = WalRecovery::new(&wal_path);
        let mut handler = NoOpReplayHandler;

        let err = recovery
            .recover(&mut handler)
            .expect_err("legacy single-file seed layout should fail recovery");
        assert!(err.is_feature_not_supported());
        assert!(err.to_string().contains("legacy single-file WAL layout"));
    }

    #[test]
    fn test_recovery_handles_missing_segment_referenced_by_catalog() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("missing_segment.wal");
        let missing_segment_path = {
            let (wal, active_segment_path) = create_segment_wal(&wal_path);
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "schema_before_missing_segment",
                1,
                100,
            )
            .unwrap();
            active_segment_path
        };
        std::fs::remove_file(&missing_segment_path).unwrap();

        // Health check detects the missing segment.
        let report = segment_catalog_health_check_read_only(&wal_path);
        assert!(!report.healthy);
        assert!(report
            .main_wal
            .error
            .as_deref()
            .is_some_and(|msg| msg.contains("missing WAL segment")));

        // Recovery treats a missing physical file as an empty segment
        // (the WAL writer is lazy, so a missing file is indistinguishable
        // from "never written").  The data that was in the deleted file is
        // lost, but recovery does not crash.
        let recovery = WalRecovery::new(&wal_path);
        let mut handler = NoOpReplayHandler;
        let (_wal, result) = recovery
            .recover(&mut handler)
            .expect("recovery should succeed with missing segment treated as empty");
        assert_eq!(result.entries_replayed, 0);
    }

    #[test]
    fn test_recovery_with_entries() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let (wal, _active_segment_path) = create_segment_wal(&wal_path);
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "test_schema",
                1,
                100,
            )
            .unwrap();
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
            let (wal, _active_segment_path) = create_segment_wal(&wal_path);
            let entry = WalEntry::RowIdDelete {
                locations: vec![(9, 0, 3), (9, 0, 4)],
            };
            wal.writer()
                .as_ref()
                .write_entry(entry.wal_type(), &entry.serialize_data())
                .unwrap();
            wal.writer().flush().unwrap();
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
        let active_segment_path;

        {
            let (wal, segment_path) = create_segment_wal(&wal_path);
            active_segment_path = segment_path;
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "committed_schema",
                1,
                100,
            )
            .unwrap();
            append_open_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "uncommitted_schema",
                2,
            )
            .unwrap();
        }

        let expected_flush_offset = {
            let mut reader = WalReader::open(&active_segment_path).unwrap().unwrap();
            reader
                .scan_for_truncation_point()
                .unwrap()
                .last_flush_offset
        };

        let size_before_recovery = std::fs::metadata(&active_segment_path).unwrap().len();

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

        let size_after_recovery = std::fs::metadata(&active_segment_path).unwrap().len();
        assert!(size_after_recovery < size_before_recovery);
        assert_eq!(size_after_recovery, expected_flush_offset);
    }

    #[test]
    fn test_recovery_without_auto_truncate_skips_unflushed_tail() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("flush_boundary_no_truncate.wal");
        let active_segment_path;

        {
            let (wal, segment_path) = create_segment_wal(&wal_path);
            active_segment_path = segment_path;
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "committed_schema",
                1,
                100,
            )
            .unwrap();
            append_open_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "uncommitted_schema",
                2,
            )
            .unwrap();
        }

        let size_before_recovery = std::fs::metadata(&active_segment_path).unwrap().len();

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

        let size_after_recovery = std::fs::metadata(&active_segment_path).unwrap().len();
        assert_eq!(size_after_recovery, size_before_recovery);
    }

    #[test]
    fn test_recovery_with_torn_write_auto_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("torn.wal");
        let active_segment_path;

        {
            let (wal, segment_path) = create_segment_wal(&wal_path);
            active_segment_path = segment_path;
            write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "schema1", 1, 100)
                .unwrap();
        }

        let size_before_torn = std::fs::metadata(&active_segment_path).unwrap().len();

        // Append torn write (incomplete header)
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&active_segment_path)
                .unwrap();
            file.write_all(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        }

        let size_with_torn = std::fs::metadata(&active_segment_path).unwrap().len();
        assert!(size_with_torn > size_before_torn);

        // Recovery should auto-truncate
        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(true);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Should have replayed the valid entries
        assert!(result.entries_replayed > 0);

        // File should be truncated
        let size_after_recovery = std::fs::metadata(&active_segment_path).unwrap().len();
        assert!(size_after_recovery <= size_before_torn);

        let ops = handler.operations();
        assert!(ops.iter().any(|op| op.contains("CREATE SCHEMA")));
    }

    #[test]
    fn test_recovery_with_torn_write_no_auto_truncate() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("torn_no_truncate.wal");
        let active_segment_path;

        {
            let (wal, segment_path) = create_segment_wal(&wal_path);
            active_segment_path = segment_path;
            write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "schema1", 1, 100)
                .unwrap();
        }

        // Append torn write
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&active_segment_path)
                .unwrap();
            file.write_all(&[0x01, 0x02, 0x03, 0x04]).unwrap();
        }

        let size_with_torn = std::fs::metadata(&active_segment_path).unwrap().len();

        // Recovery without auto-truncate
        let recovery = WalRecovery::new(&wal_path).with_auto_truncate(false);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        // Should still replay valid entries
        assert!(result.entries_replayed > 0);

        // File should NOT be truncated
        let size_after_recovery = std::fs::metadata(&active_segment_path).unwrap().len();
        assert_eq!(size_after_recovery, size_with_torn);
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
        let active_segment_path;

        {
            let (wal, segment_path) = create_segment_wal(&wal_path);
            active_segment_path = segment_path;
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "committed_schema",
                1,
                100,
            )
            .unwrap();
            append_open_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "uncommitted_schema",
                2,
            )
            .unwrap();
        }

        let size_before = std::fs::metadata(&active_segment_path).unwrap().len();
        let recovery = WalRecovery::new(&wal_path)
            .with_auto_truncate(true)
            .with_wal_keep_from(0);
        let mut handler = RecordingHandler::new();

        let (_wal, result) = recovery.recover(&mut handler).unwrap();

        assert!(result.all_succeeded);
        let size_after = std::fs::metadata(&active_segment_path).unwrap().len();
        assert!(size_after < size_before);
    }

    #[test]
    fn replay_only_skips_prefix_before_replay_from_lsn() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("skip_prefix.wal");

        let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
        write_flushed_create_schema_txn(&writer, "default", "schema1", 1, 100).unwrap();
        write_flushed_create_schema_txn(&writer, "default", "schema2", 2, 101).unwrap();

        let mut handler = RecordingHandler::new();
        let result = WalRecovery::new(&wal_path)
            .with_replay_lsn_bounds(1, 2)
            .replay_only(&mut handler)
            .unwrap();

        assert!(result.all_succeeded);
        let ops = handler.operations();
        assert!(!ops.iter().any(|op| op.contains("schema1")));
        assert!(ops.iter().any(|op| op.contains("schema2")));
    }

    #[test]
    fn replay_only_applies_rowset_commit_after_logical_lsn_cut() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("rowset_after_cut.wal");

        let writer = WalWriter::new(&wal_path, WalInitState::Uninitialized);
        write_flushed_create_schema_txn(&writer, "default", "txn_before", 1, 100).unwrap();
        write_flushed_rowset_commit(&writer, 7, 100, 1, 1, "/rowset/r100").unwrap();
        write_flushed_rowset_commit(&writer, 7, 200, 1, 2, "/rowset/r200").unwrap();

        let mut handler = RecordingHandler::new();
        let result = WalRecovery::new(&wal_path)
            .with_replay_lsn_bounds(1, 3)
            .replay_only(&mut handler)
            .unwrap();
        assert!(result.all_succeeded);

        let ops = handler.operations();
        assert!(!ops.iter().any(|op| op.contains("txn_before")));
        assert!(!ops
            .iter()
            .any(|op| op.contains("ROWSET_COMMIT tablet=7 rowset=100 v[1-1]")));
        assert!(ops
            .iter()
            .any(|op| op.contains("ROWSET_COMMIT tablet=7 rowset=200 v[1-2]")));
    }

    #[test]
    fn rowset_commit_validation_hook_runs_before_replay() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("rowset_validation_hook.wal");

        {
            let (wal, _active_segment_path) = create_segment_wal(&wal_path);
            write_flushed_rowset_commit(wal.writer().as_ref(), 10, 99, 3, 3, "/rowset/r99")
                .unwrap();
        }

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

        {
            let (wal, _active_segment_path) = create_segment_wal(&wal_path);
            write_flushed_rowset_commit(wal.writer().as_ref(), 10, 77, 1, 1, "/rowset/r77")
                .unwrap();
        }

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
    fn test_recovery_rejects_legacy_wal_version() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("legacy_version_segment.wal");

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

        let err = recovery
            .replay_only(&mut handler)
            .expect_err("legacy WAL version should fail physical segment replay");

        assert!(err.is_feature_not_supported());
        assert!(err.to_string().contains("unsupported WAL version 2"));
        assert!(wal_path.exists());
        assert!(handler.operations().is_empty());
    }

    #[test]
    fn test_recovery_fails_on_db_identifier_mismatch() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("identity_mismatch.wal");

        let wal_metadata = WalHeaderMetadata::new([1u8; 16], 3);
        {
            let (wal, _active_segment_path) =
                create_segment_wal_with_header_metadata(&wal_path, wal_metadata);
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "schema_to_skip",
                1,
                100,
            )
            .unwrap();
        }

        let recovery = WalRecovery::new(&wal_path).with_wal_header_metadata([2u8; 16], 3);
        let mut handler = RecordingHandler::new();

        let err = recovery
            .recover(&mut handler)
            .expect_err("db identity mismatch should fail recovery");

        assert!(err.to_string().contains("WAL database identity mismatch"));
        assert!(handler.operations().is_empty());
        assert!(WriteAheadLog::exists_for_seed(&wal_path));
    }

    #[test]
    fn test_recovery_allows_one_iteration_lag() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("iteration_lag.wal");
        let db_identifier = [7u8; 16];

        {
            let (wal, _active_segment_path) = create_segment_wal_with_header_metadata(
                &wal_path,
                WalHeaderMetadata::new(db_identifier, 9),
            );
            write_flushed_create_schema_txn(
                wal.writer().as_ref(),
                "default",
                "schema_replay",
                1,
                100,
            )
            .unwrap();
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
    fn test_segment_catalog_health_check_detects_unflushed_tail() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("segment_unhealthy.wal");

        // Drop the segment-backed handle to flush the buffered tail into the
        // active segment without appending a `WalFlush` marker.
        {
            let wal = WriteAheadLog::new(&wal_path).unwrap();
            write_flushed_create_schema_txn(wal.writer().as_ref(), "default", "committed", 1, 100)
                .unwrap();
            append_open_create_schema_txn(wal.writer().as_ref(), "default", "uncommitted", 2)
                .unwrap();
        }

        let report = segment_catalog_health_check_read_only(&wal_path);
        assert!(!report.healthy);
        assert_eq!(report.recovery_mode, WalRecoveryMode::MainWalOnly);
        assert!(report.main_wal.exists);
        assert!(report.main_wal.needs_truncation);
        assert!(report.main_wal.has_unflushed_tail);
        assert!(report.main_wal.entries_scanned > 0);
    }
}
