mod version_chain;
mod visibility;

use paro_common::error::{self as paro_error, Result};
use paro_storage::transaction::manager::TRANSACTION_ID_START;

pub(crate) use version_chain::VersionedEntry;
pub(crate) use visibility::{has_conflict, is_committed, is_permanent, is_provisional, is_visible};

/// Dedicated writer timestamp for permanent catalog state.
pub const PERMANENT_WRITER_ID: u64 = 0;

/// Dedicated provisional writer identity used during WAL replay.
pub const REPLAY_WRITER_ID: u64 = TRANSACTION_ID_START;

/// Immutable MVCC snapshot used at the catalog boundary.
///
/// `writer_id` is present only for snapshots that are allowed to stage catalog
/// mutations. Read-only snapshots never see provisional versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub transaction_id: u64,
    pub start_time: u64,
    writer_id: Option<u64>,
}

impl CatalogSnapshot {
    pub fn read_only(start_time: u64) -> Self {
        Self {
            transaction_id: PERMANENT_WRITER_ID,
            start_time,
            writer_id: None,
        }
    }

    pub fn writer(writer_id: u64, start_time: u64) -> Self {
        debug_assert!(
            writer_id >= TRANSACTION_ID_START,
            "CatalogSnapshot::writer requires a provisional writer id"
        );
        Self {
            transaction_id: writer_id,
            start_time,
            writer_id: Some(writer_id),
        }
    }

    pub fn permanent_writer(start_time: u64) -> Self {
        Self {
            transaction_id: PERMANENT_WRITER_ID,
            start_time,
            writer_id: Some(PERMANENT_WRITER_ID),
        }
    }

    pub fn replay_writer(start_time: u64) -> Self {
        Self::writer(REPLAY_WRITER_ID, start_time)
    }

    pub fn writer_id(&self) -> Option<u64> {
        self.writer_id
    }

    pub fn is_read_only(&self) -> bool {
        self.writer_id.is_none()
    }

    pub fn write_timestamp(&self) -> Result<u64> {
        self.writer_id.ok_or_else(|| {
            paro_error::invalid_transaction_state("catalog mutation requires a writer snapshot")
        })
    }

    pub fn can_see(&self, timestamp: u64) -> bool {
        visibility::is_visible(timestamp, self.writer_id, self.start_time)
    }

    pub fn has_conflict(&self, timestamp: u64) -> bool {
        visibility::has_conflict(timestamp, self.writer_id, self.start_time)
    }
}

impl Default for CatalogSnapshot {
    fn default() -> Self {
        Self::read_only(TRANSACTION_ID_START)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_snapshot_has_no_writer_identity() {
        let snapshot = CatalogSnapshot::read_only(123);

        assert!(snapshot.is_read_only());
        assert_eq!(snapshot.transaction_id, PERMANENT_WRITER_ID);
        assert_eq!(snapshot.writer_id(), None);
        assert_eq!(snapshot.start_time, 123);
    }

    #[test]
    fn writer_snapshot_exposes_write_timestamp() {
        let writer_id = TRANSACTION_ID_START + 5;
        let snapshot = CatalogSnapshot::writer(writer_id, 77);

        assert_eq!(snapshot.writer_id(), Some(writer_id));
        assert_eq!(snapshot.write_timestamp().unwrap(), writer_id);
        assert!(!snapshot.is_read_only());
    }

    #[test]
    fn read_only_snapshot_rejects_write_timestamp_requests() {
        let err = CatalogSnapshot::read_only(88)
            .write_timestamp()
            .unwrap_err();
        assert!(err.to_string().contains("writer snapshot"));
    }

    #[test]
    fn replay_writer_is_a_valid_provisional_snapshot() {
        let snapshot = CatalogSnapshot::replay_writer(55);

        assert_eq!(snapshot.writer_id(), Some(REPLAY_WRITER_ID));
        assert_eq!(snapshot.write_timestamp().unwrap(), REPLAY_WRITER_ID);
        assert!(!snapshot.is_read_only());
    }

    #[test]
    fn permanent_writer_exposes_committed_write_timestamp() {
        let snapshot = CatalogSnapshot::permanent_writer(99);

        assert_eq!(snapshot.transaction_id, PERMANENT_WRITER_ID);
        assert_eq!(snapshot.writer_id(), Some(PERMANENT_WRITER_ID));
        assert_eq!(snapshot.write_timestamp().unwrap(), PERMANENT_WRITER_ID);
        assert!(!snapshot.is_read_only());
    }
}
