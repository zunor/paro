// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Commit-specific durable append wrapper around the generic journal runtime.

use super::{
    CommitDurableBatch, DurableCommitHandle, DurableCommitHandleError, SequencedCommitJob,
    SequencedCommitJobStateError,
};
use crate::types::CommitTs;
use paro_common::error as paro_error;
use paro_common::error::{ParoError, Result as ParoResult};
use paro_common::journal::JournalRecord;
use paro_journal::{AppendResult, JournalAppender, JournalCoordinator};
use std::fmt;
use std::sync::Arc;

pub trait CommitJournal: Send + Sync + 'static {
    fn append_records(&self, records: &[JournalRecord]) -> ParoResult<Vec<AppendResult>>;
}

impl CommitJournal for JournalCoordinator {
    fn append_records(&self, records: &[JournalRecord]) -> ParoResult<Vec<AppendResult>> {
        JournalCoordinator::append_records(self, records)
    }
}

impl CommitJournal for JournalAppender {
    fn append_records(&self, records: &[JournalRecord]) -> ParoResult<Vec<AppendResult>> {
        JournalAppender::append_records(self, records)
    }
}

#[derive(Debug)]
pub struct CommitAppendBatch {
    pub handles: Vec<DurableCommitHandle>,
    pub durable_jobs: Vec<super::DurableCommitJob>,
    pub batch: Arc<CommitDurableBatch>,
}

#[derive(Debug, Clone)]
pub enum AppendCommitError {
    AppendFailed {
        inner: ParoError,
    },
    DurableProtocolViolation {
        durable_range: (CommitTs, CommitTs),
        durable_batch_lsn: u64,
        details: JournalProtocolViolationKind,
    },
}

impl fmt::Display for AppendCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppendFailed { inner } => {
                write!(f, "commit append failed before durability: {inner}")
            }
            Self::DurableProtocolViolation {
                durable_range,
                durable_batch_lsn,
                details,
            } => write!(
                f,
                "durable commit append protocol violation for {}..={} in durable batch lsn {}: {}",
                durable_range.0, durable_range.1, durable_batch_lsn, details
            ),
        }
    }
}

impl std::error::Error for AppendCommitError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalProtocolViolationKind {
    ResultCountMismatch {
        expected: usize,
        actual: usize,
    },
    NonContiguousLsn {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    InconsistentDurableBatchLsn {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    InconsistentDurableBatchSize {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    InconsistentDurableBatchBytes {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    InconsistentSyncLatencyMicros {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    DurableBatchInvalid {
        reason: String,
    },
    DurableJobStateInvalid {
        offset: usize,
        reason: String,
    },
}

impl fmt::Display for JournalProtocolViolationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResultCountMismatch { expected, actual } => {
                write!(f, "result count mismatch: expected {expected}, got {actual}")
            }
            Self::NonContiguousLsn {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "non-contiguous lsn at offset {offset}: expected {expected}, got {actual}"
            ),
            Self::InconsistentDurableBatchLsn {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "inconsistent durable_batch_lsn at offset {offset}: expected {expected}, got {actual}"
            ),
            Self::InconsistentDurableBatchSize {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "inconsistent durable_batch_size at offset {offset}: expected {expected}, got {actual}"
            ),
            Self::InconsistentDurableBatchBytes {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "inconsistent durable_batch_bytes at offset {offset}: expected {expected}, got {actual}"
            ),
            Self::InconsistentSyncLatencyMicros {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "inconsistent sync_latency_micros at offset {offset}: expected {expected}, got {actual}"
            ),
            Self::DurableBatchInvalid { reason } => write!(f, "durable batch invalid: {reason}"),
            Self::DurableJobStateInvalid { offset, reason } => write!(
                f,
                "durable job state invalid at offset {offset}: {reason}"
            ),
        }
    }
}

/// Appends a sequenced commit batch and consumes the durable side of each job.
///
/// On success every `accepted` job has had its `CommitRecord` and post-append
/// sidecar taken. Callers must continue from the returned `handles`,
/// `durable_jobs`, and shared batch metadata rather than reading payload or
/// lifecycle state back out of `accepted`.
pub fn append_durable_commit_batch(
    journal: &(impl CommitJournal + ?Sized),
    accepted: &mut [SequencedCommitJob],
) -> Result<CommitAppendBatch, AppendCommitError> {
    if accepted.is_empty() {
        return Err(AppendCommitError::AppendFailed {
            inner: paro_error::internal("empty durable commit append batch"),
        });
    }

    let commit_ts_list = collect_commit_ts(accepted)?;
    let estimated_record_bytes_by_offset = accepted
        .iter()
        .map(|job| job.estimated_record_bytes)
        .collect::<Vec<_>>();
    for job in accepted.iter() {
        match job.record_commit_id() {
            Some(commit_id) if commit_id == job.commit_ts.into_raw() => {}
            Some(commit_id) => {
                return Err(AppendCommitError::AppendFailed {
                    inner: paro_error::internal(format!(
                        "sequenced commit record id {} does not match commit_ts {}",
                        commit_id, job.commit_ts
                    )),
                });
            }
            None => {
                return Err(non_durable_state_error(
                    SequencedCommitJobStateError::RecordAlreadyTaken {
                        commit_ts: job.commit_ts,
                    },
                ))
            }
        }
    }
    let mut records = Vec::with_capacity(accepted.len());
    for job in accepted.iter_mut() {
        let record = job.take_record().map_err(non_durable_state_error)?;
        records.push(JournalRecord::Commit(record));
    }

    let results = journal
        .append_records(&records)
        .map_err(|inner| AppendCommitError::AppendFailed { inner })?;
    validate_append_results(
        &results,
        accepted.len(),
        commit_ts_list[0],
        *commit_ts_list.last().unwrap(),
    )?;

    let first = results[0];
    let batch = Arc::new(
        CommitDurableBatch::new(
            first.lsn,
            first.durable_batch_lsn,
            first.durable_batch_size,
            first.durable_batch_bytes,
            Arc::from(estimated_record_bytes_by_offset),
            first.sync_latency_micros,
            commit_ts_list[0],
            *commit_ts_list.last().unwrap(),
        )
        .map_err(|err| protocol_error(&results, commit_ts_list[0], err))?,
    );

    let mut handles = Vec::with_capacity(accepted.len());
    let mut durable_jobs = Vec::with_capacity(accepted.len());
    for (offset, job) in accepted.iter_mut().enumerate() {
        let handle = batch
            .handle_at(offset as u32)
            .map_err(|err| protocol_error(&results, commit_ts_list[0], err))?;
        let durable_job = job
            .take_durable_job(&handle)
            .map_err(|err| job_state_protocol_error(&results, commit_ts_list[0], offset, err))?;
        handles.push(handle);
        durable_jobs.push(durable_job);
    }

    Ok(CommitAppendBatch {
        handles,
        durable_jobs,
        batch,
    })
}

fn collect_commit_ts(accepted: &[SequencedCommitJob]) -> Result<Vec<CommitTs>, AppendCommitError> {
    let mut commit_ts_list = Vec::with_capacity(accepted.len());
    let first = accepted[0].commit_ts;
    for (offset, job) in accepted.iter().enumerate() {
        let expected = first
            .into_raw()
            .checked_add(offset as u64)
            .map(CommitTs::new)
            .ok_or_else(|| AppendCommitError::AppendFailed {
                inner: paro_error::internal("sequenced commit timestamp range overflow"),
            })?;
        if job.commit_ts != expected {
            return Err(AppendCommitError::AppendFailed {
                inner: paro_error::internal(format!(
                    "non-contiguous sequenced commit timestamp at offset {offset}: expected {expected}, got {}",
                    job.commit_ts
                )),
            });
        }
        commit_ts_list.push(job.commit_ts);
    }
    Ok(commit_ts_list)
}

fn validate_append_results(
    results: &[AppendResult],
    expected_len: usize,
    first_commit_ts: CommitTs,
    last_commit_ts: CommitTs,
) -> Result<(), AppendCommitError> {
    if results.len() != expected_len {
        return Err(AppendCommitError::DurableProtocolViolation {
            durable_range: (first_commit_ts, last_commit_ts),
            durable_batch_lsn: results
                .last()
                .map(|result| result.durable_batch_lsn)
                .unwrap_or(0),
            details: JournalProtocolViolationKind::ResultCountMismatch {
                expected: expected_len,
                actual: results.len(),
            },
        });
    }

    let first = results[0];
    for (offset, result) in results.iter().enumerate() {
        let expected_lsn = first.lsn.saturating_add(offset as u64);
        if result.lsn != expected_lsn {
            return Err(protocol_violation(
                results,
                first_commit_ts,
                last_commit_ts,
                JournalProtocolViolationKind::NonContiguousLsn {
                    offset,
                    expected: expected_lsn,
                    actual: result.lsn,
                },
            ));
        }
        if result.durable_batch_lsn != first.durable_batch_lsn {
            return Err(protocol_violation(
                results,
                first_commit_ts,
                last_commit_ts,
                JournalProtocolViolationKind::InconsistentDurableBatchLsn {
                    offset,
                    expected: first.durable_batch_lsn,
                    actual: result.durable_batch_lsn,
                },
            ));
        }
        if result.durable_batch_size != first.durable_batch_size {
            return Err(protocol_violation(
                results,
                first_commit_ts,
                last_commit_ts,
                JournalProtocolViolationKind::InconsistentDurableBatchSize {
                    offset,
                    expected: first.durable_batch_size,
                    actual: result.durable_batch_size,
                },
            ));
        }
        if result.durable_batch_bytes != first.durable_batch_bytes {
            return Err(protocol_violation(
                results,
                first_commit_ts,
                last_commit_ts,
                JournalProtocolViolationKind::InconsistentDurableBatchBytes {
                    offset,
                    expected: first.durable_batch_bytes,
                    actual: result.durable_batch_bytes,
                },
            ));
        }
        if result.sync_latency_micros != first.sync_latency_micros {
            return Err(protocol_violation(
                results,
                first_commit_ts,
                last_commit_ts,
                JournalProtocolViolationKind::InconsistentSyncLatencyMicros {
                    offset,
                    expected: first.sync_latency_micros,
                    actual: result.sync_latency_micros,
                },
            ));
        }
    }
    Ok(())
}

fn protocol_violation(
    results: &[AppendResult],
    first_commit_ts: CommitTs,
    last_commit_ts: CommitTs,
    details: JournalProtocolViolationKind,
) -> AppendCommitError {
    AppendCommitError::DurableProtocolViolation {
        durable_range: (first_commit_ts, last_commit_ts),
        durable_batch_lsn: results
            .last()
            .map(|result| result.durable_batch_lsn)
            .unwrap_or(0),
        details,
    }
}

fn protocol_error(
    results: &[AppendResult],
    first_commit_ts: CommitTs,
    err: DurableCommitHandleError,
) -> AppendCommitError {
    let last_commit_ts = first_commit_ts
        .into_raw()
        .checked_add(results.len().saturating_sub(1) as u64)
        .map(CommitTs::new)
        .unwrap_or(first_commit_ts);
    protocol_violation(
        results,
        first_commit_ts,
        last_commit_ts,
        JournalProtocolViolationKind::DurableBatchInvalid {
            reason: err.to_string(),
        },
    )
}

fn job_state_protocol_error(
    results: &[AppendResult],
    first_commit_ts: CommitTs,
    offset: usize,
    err: SequencedCommitJobStateError,
) -> AppendCommitError {
    let last_commit_ts = first_commit_ts
        .into_raw()
        .checked_add(results.len().saturating_sub(1) as u64)
        .map(CommitTs::new)
        .unwrap_or(first_commit_ts);
    protocol_violation(
        results,
        first_commit_ts,
        last_commit_ts,
        JournalProtocolViolationKind::DurableJobStateInvalid {
            offset,
            reason: err.to_string(),
        },
    )
}

fn non_durable_state_error(err: SequencedCommitJobStateError) -> AppendCommitError {
    AppendCommitError::AppendFailed {
        inner: paro_error::internal(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AppendFailureRollbackPlan, CommitAckPolicy, CommitCompletionHandle,
        CommitFinalizeReservation, LockReleasePlan, PrePublishReleasePlan, RequiredPublishPlan,
        SequencedCommitPostAppend,
    };
    use paro_common::durability::PreparedCommitPlan;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingCommitJournal {
        appended: Mutex<Vec<Vec<JournalRecord>>>,
        result_override: Mutex<Option<ParoResult<Vec<AppendResult>>>>,
    }

    impl CommitJournal for RecordingCommitJournal {
        fn append_records(&self, records: &[JournalRecord]) -> ParoResult<Vec<AppendResult>> {
            self.appended.lock().unwrap().push(records.to_vec());
            if let Some(result) = self.result_override.lock().unwrap().take() {
                return result;
            }
            Ok((0..records.len())
                .map(|offset| AppendResult {
                    lsn: 10 + offset as u64,
                    durable_batch_lsn: 10 + records.len() as u64 - 1,
                    durable_batch_size: records.len() as u64,
                    durable_batch_bytes: 4096,
                    sync_latency_micros: 33,
                })
                .collect())
        }
    }

    fn empty_plan(txn_id: u64) -> PreparedCommitPlan {
        PreparedCommitPlan {
            txn_id,
            start_time: txn_id,
            catalog_ops: Vec::new(),
            storage_ops: Vec::new(),
            apply_descriptors: Vec::new(),
            deferred_tasks: Vec::new(),
            tablets: Vec::new(),
        }
    }

    fn post_append() -> SequencedCommitPostAppend {
        SequencedCommitPostAppend {
            finalize_reservation: CommitFinalizeReservation::default(),
            lock_release_plan: LockReleasePlan::noop(),
            pre_publish_release_plan: PrePublishReleasePlan::noop(),
            append_failure_rollback_plan: AppendFailureRollbackPlan::noop(),
            required_publish: RequiredPublishPlan::noop_for_tests(),
            ack_policy: CommitAckPolicy::RequiredPublished,
            completion: CommitCompletionHandle::default(),
        }
    }

    fn job(commit_ts: u64, estimated_record_bytes: u32) -> SequencedCommitJob {
        SequencedCommitJob::new(
            CommitTs::new(commit_ts),
            empty_plan(commit_ts).into_record(commit_ts),
            estimated_record_bytes,
            1024,
            post_append(),
        )
    }

    #[test]
    fn append_batch_moves_records_and_returns_handles_and_durable_jobs() {
        let journal = RecordingCommitJournal::default();
        let mut jobs = vec![job(7, 111), job(8, 222)];

        let appended = append_durable_commit_batch(&journal, &mut jobs).unwrap();

        assert_eq!(journal.appended.lock().unwrap()[0].len(), 2);
        assert_eq!(appended.handles.len(), 2);
        assert_eq!(appended.durable_jobs.len(), 2);
        assert_eq!(appended.handles[0].commit_ts(), CommitTs::new(7));
        assert_eq!(appended.handles[1].commit_ts(), CommitTs::new(8));
        assert_eq!(appended.handles[0].durable_lsn(), 10);
        assert_eq!(appended.handles[1].commit_record_bytes(), 222);
        assert_eq!(appended.batch.commit_record_bytes_by_offset(), &[111, 222]);
        assert!(jobs.iter().all(|job| !job.has_record()));
    }

    #[test]
    fn append_failure_is_reported_as_not_durable() {
        let journal = RecordingCommitJournal::default();
        *journal.result_override.lock().unwrap() =
            Some(Err(paro_error::internal("injected append failure")));
        let mut jobs = vec![job(7, 111)];

        let err = append_durable_commit_batch(&journal, &mut jobs).unwrap_err();

        assert!(matches!(err, AppendCommitError::AppendFailed { .. }));
        let cleanup = jobs[0].take_append_failure_cleanup().unwrap();
        assert_eq!(cleanup.reservation.write_conflict.slot_id, 0);
        assert_eq!(cleanup.reservation.summary.slot_id, 0);
    }

    #[test]
    fn append_result_count_mismatch_is_durable_protocol_violation() {
        let journal = RecordingCommitJournal::default();
        *journal.result_override.lock().unwrap() = Some(Ok(Vec::new()));
        let mut jobs = vec![job(7, 111)];

        let err = append_durable_commit_batch(&journal, &mut jobs).unwrap_err();

        assert!(matches!(
            err,
            AppendCommitError::DurableProtocolViolation {
                details: JournalProtocolViolationKind::ResultCountMismatch {
                    expected: 1,
                    actual: 0
                },
                ..
            }
        ));
    }
}
