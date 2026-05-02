// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Cheap durable commit handle derived from shared append-batch metadata.

use super::CommitFrontierHandle;
use crate::types::CommitTs;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableCommitHandleError {
    EmptyRecordBytes,
    CommitRangeMismatch {
        first_commit_ts: CommitTs,
        last_commit_ts: CommitTs,
        record_count: usize,
    },
    OffsetOutOfRange {
        offset: u32,
        record_count: usize,
    },
    LsnOverflow {
        first_lsn: u64,
        offset: u32,
    },
    CommitTsOverflow {
        first_commit_ts: CommitTs,
        offset: u32,
    },
}

impl fmt::Display for DurableCommitHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRecordBytes => write!(f, "durable commit batch cannot be empty"),
            Self::CommitRangeMismatch {
                first_commit_ts,
                last_commit_ts,
                record_count,
            } => write!(
                f,
                "durable commit batch range {}..={} does not match {} records",
                first_commit_ts, last_commit_ts, record_count
            ),
            Self::OffsetOutOfRange {
                offset,
                record_count,
            } => write!(
                f,
                "durable commit handle offset {} out of range for {} records",
                offset, record_count
            ),
            Self::LsnOverflow { first_lsn, offset } => write!(
                f,
                "durable commit lsn overflow: first_lsn={} offset={}",
                first_lsn, offset
            ),
            Self::CommitTsOverflow {
                first_commit_ts,
                offset,
            } => write!(
                f,
                "durable commit timestamp overflow: first_commit_ts={} offset={}",
                first_commit_ts, offset
            ),
        }
    }
}

impl std::error::Error for DurableCommitHandleError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitDurableBatch {
    first_lsn: u64,
    durable_batch_lsn: u64,
    durable_batch_size: u64,
    durable_batch_bytes: u64,
    commit_record_bytes_by_offset: Arc<[u32]>,
    sync_latency_micros: u64,
    first_commit_ts: CommitTs,
    last_commit_ts: CommitTs,
}

impl CommitDurableBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        first_lsn: u64,
        durable_batch_lsn: u64,
        durable_batch_size: u64,
        durable_batch_bytes: u64,
        commit_record_bytes_by_offset: Arc<[u32]>,
        sync_latency_micros: u64,
        first_commit_ts: CommitTs,
        last_commit_ts: CommitTs,
    ) -> Result<Self, DurableCommitHandleError> {
        if commit_record_bytes_by_offset.is_empty() {
            return Err(DurableCommitHandleError::EmptyRecordBytes);
        }
        first_lsn
            .checked_add((commit_record_bytes_by_offset.len() - 1) as u64)
            .ok_or(DurableCommitHandleError::LsnOverflow {
                first_lsn,
                offset: (commit_record_bytes_by_offset.len() - 1) as u32,
            })?;
        let expected_last = first_commit_ts
            .into_raw()
            .checked_add((commit_record_bytes_by_offset.len() - 1) as u64)
            .map(CommitTs::new)
            .ok_or(DurableCommitHandleError::CommitTsOverflow {
                first_commit_ts,
                offset: (commit_record_bytes_by_offset.len() - 1) as u32,
            })?;
        if expected_last != last_commit_ts {
            return Err(DurableCommitHandleError::CommitRangeMismatch {
                first_commit_ts,
                last_commit_ts,
                record_count: commit_record_bytes_by_offset.len(),
            });
        }
        Ok(Self {
            first_lsn,
            durable_batch_lsn,
            durable_batch_size,
            durable_batch_bytes,
            commit_record_bytes_by_offset,
            sync_latency_micros,
            first_commit_ts,
            last_commit_ts,
        })
    }

    #[inline]
    pub const fn first_lsn(&self) -> u64 {
        self.first_lsn
    }

    #[inline]
    pub const fn durable_batch_lsn(&self) -> u64 {
        self.durable_batch_lsn
    }

    #[inline]
    pub const fn durable_batch_size(&self) -> u64 {
        self.durable_batch_size
    }

    #[inline]
    pub const fn durable_batch_bytes(&self) -> u64 {
        self.durable_batch_bytes
    }

    #[inline]
    pub fn commit_record_bytes_by_offset(&self) -> &[u32] {
        &self.commit_record_bytes_by_offset
    }

    #[inline]
    pub const fn sync_latency_micros(&self) -> u64 {
        self.sync_latency_micros
    }

    #[inline]
    pub const fn first_commit_ts(&self) -> CommitTs {
        self.first_commit_ts
    }

    #[inline]
    pub const fn last_commit_ts(&self) -> CommitTs {
        self.last_commit_ts
    }

    #[inline]
    pub fn record_count(&self) -> usize {
        self.commit_record_bytes_by_offset.len()
    }

    pub fn handle_at(
        self: &Arc<Self>,
        offset_in_batch: u32,
    ) -> Result<DurableCommitHandle, DurableCommitHandleError> {
        DurableCommitHandle::new(Arc::clone(self), offset_in_batch)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableCommitHandle {
    batch: Arc<CommitDurableBatch>,
    offset_in_batch: u32,
}

impl DurableCommitHandle {
    pub fn new(
        batch: Arc<CommitDurableBatch>,
        offset_in_batch: u32,
    ) -> Result<Self, DurableCommitHandleError> {
        if offset_in_batch as usize >= batch.record_count() {
            return Err(DurableCommitHandleError::OffsetOutOfRange {
                offset: offset_in_batch,
                record_count: batch.record_count(),
            });
        }
        Ok(Self {
            batch,
            offset_in_batch,
        })
    }

    #[inline]
    pub fn batch(&self) -> &Arc<CommitDurableBatch> {
        &self.batch
    }

    #[inline]
    pub const fn offset_in_batch(&self) -> u32 {
        self.offset_in_batch
    }

    pub fn commit_ts(&self) -> CommitTs {
        self.batch
            .first_commit_ts
            .into_raw()
            .checked_add(self.offset_in_batch as u64)
            .map(CommitTs::new)
            .expect("validated durable commit handle timestamp range")
    }

    pub fn durable_lsn(&self) -> u64 {
        self.batch
            .first_lsn
            .checked_add(self.offset_in_batch as u64)
            .expect("validated durable commit handle lsn range")
    }

    #[inline]
    pub fn durable_batch_lsn(&self) -> u64 {
        self.batch.durable_batch_lsn
    }

    #[inline]
    pub fn durable_batch_size(&self) -> u64 {
        self.batch.durable_batch_size
    }

    #[inline]
    pub fn durable_batch_bytes(&self) -> u64 {
        self.batch.durable_batch_bytes
    }

    #[inline]
    pub fn commit_record_bytes(&self) -> u32 {
        self.batch.commit_record_bytes_by_offset[self.offset_in_batch as usize]
    }

    #[inline]
    pub fn sync_latency_micros(&self) -> u64 {
        self.batch.sync_latency_micros
    }
}

impl CommitFrontierHandle for DurableCommitHandle {
    #[inline]
    fn commit_ts(&self) -> CommitTs {
        self.commit_ts()
    }

    #[inline]
    fn commit_record_bytes(&self) -> u32 {
        self.commit_record_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_derives_commit_lsn_and_bytes_from_batch() {
        let batch = Arc::new(
            CommitDurableBatch::new(
                9,
                11,
                3,
                4096,
                Arc::from([100_u32, 120, 140]),
                77,
                CommitTs::new(20),
                CommitTs::new(22),
            )
            .unwrap(),
        );

        let handle = batch.handle_at(1).unwrap();

        assert_eq!(handle.commit_ts(), CommitTs::new(21));
        assert_eq!(handle.durable_lsn(), 10);
        assert_eq!(handle.durable_batch_lsn(), 11);
        assert_eq!(handle.commit_record_bytes(), 120);
        assert_eq!(handle.sync_latency_micros(), 77);
    }
}
