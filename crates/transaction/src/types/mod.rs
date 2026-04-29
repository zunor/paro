// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use core::fmt;
use core::num::NonZeroU64;

/// Starting value for live transaction / provisional writer IDs.
///
/// Commit timestamps remain below this boundary; provisional transaction-owned
/// timestamps live at or above it. Disk codecs may still persist the raw value,
/// but runtime APIs should carry the typed wrappers from this crate.
pub const TRANSACTION_ID_START: u64 = 1_u64 << 62;

/// Sentinel used when no active transaction exists.
pub const MAX_TRANSACTION_ID: u64 = u64::MAX;

macro_rules! txn_scalar {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[repr(transparent)]
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            #[inline]
            pub const fn new(raw: u64) -> Self {
                Self(raw)
            }

            #[inline]
            pub const fn into_raw(self) -> u64 {
                self.0
            }

            #[inline]
            pub const fn get(self) -> u64 {
                self.0
            }

            #[inline]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }
        }

        impl From<u64> for $name {
            #[inline]
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            #[inline]
            fn from(value: $name) -> Self {
                value.into_raw()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

txn_scalar!(TxnId, "Unique live transaction identity.");
txn_scalar!(WriterId, "Catalog/storage provisional writer identity.");
txn_scalar!(ReadTs, "Snapshot read timestamp.");
txn_scalar!(CommitTs, "Durable commit timestamp.");
txn_scalar!(SnapshotId, "Opaque snapshot lease identity.");
txn_scalar!(LayoutEpoch, "Storage layout/catalog epoch.");
txn_scalar!(DatabaseId, "Database identity.");
txn_scalar!(TableId, "Table identity.");
txn_scalar!(ParticipantId, "Transaction participant identity.");

impl TxnId {
    #[inline]
    pub const fn first() -> Self {
        Self(TRANSACTION_ID_START)
    }

    #[inline]
    pub const fn as_writer_id(self) -> WriterId {
        WriterId::new(self.0)
    }

    #[inline]
    pub const fn is_provisional(self) -> bool {
        self.0 >= TRANSACTION_ID_START
    }
}

impl WriterId {
    #[inline]
    pub const fn replay() -> Self {
        Self(TRANSACTION_ID_START)
    }

    #[inline]
    pub const fn permanent() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn is_provisional(self) -> bool {
        self.0 >= TRANSACTION_ID_START
    }
}

impl ReadTs {
    #[inline]
    pub const fn no_active_transaction() -> Self {
        Self(TRANSACTION_ID_START)
    }

    /// Converts a transaction start timestamp into the last commit timestamp
    /// visible before that transaction starts.
    ///
    /// New snapshot code should prefer storing the already-published `read_ts`
    /// directly and compare `commit_ts <= read_ts`. This helper is only for
    /// legacy paths where `ReadTs` still represents `start_time`.
    #[inline]
    pub const fn visible_before_start(self) -> CommitTs {
        CommitTs::new(self.0.saturating_sub(1))
    }
}

impl CommitTs {
    #[inline]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Coarse participant category used by the commit coordinator and journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ParticipantKind {
    Storage = 1,
    Catalog = 2,
    Search = 3,
    Graph = 4,
    Maintenance = 5,
    External = 6,
    BulkLoad = 7,
}

/// Opaque transaction resource key.
///
/// The core layer stores only a compact namespace plus two raw words. Storage
/// and catalog own the interpretation, hashing preimages, and any richer keys.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxnResourceKey {
    kind: ParticipantKind,
    database_id: DatabaseId,
    table_id: Option<TableId>,
    local_hi: u64,
    local_lo: u64,
}

impl TxnResourceKey {
    #[inline]
    pub const fn database(kind: ParticipantKind, database_id: DatabaseId) -> Self {
        Self {
            kind,
            database_id,
            table_id: None,
            local_hi: 0,
            local_lo: 0,
        }
    }

    #[inline]
    pub const fn table(kind: ParticipantKind, database_id: DatabaseId, table_id: TableId) -> Self {
        Self {
            kind,
            database_id,
            table_id: Some(table_id),
            local_hi: 0,
            local_lo: 0,
        }
    }

    #[inline]
    pub const fn opaque(
        kind: ParticipantKind,
        database_id: DatabaseId,
        table_id: Option<TableId>,
        local_hi: u64,
        local_lo: u64,
    ) -> Self {
        Self {
            kind,
            database_id,
            table_id,
            local_hi,
            local_lo,
        }
    }

    #[inline]
    pub const fn kind(self) -> ParticipantKind {
        self.kind
    }

    #[inline]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    #[inline]
    pub const fn table_id(self) -> Option<TableId> {
        self.table_id
    }

    #[inline]
    pub const fn local_words(self) -> (u64, u64) {
        (self.local_hi, self.local_lo)
    }
}

impl fmt::Debug for TxnResourceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxnResourceKey")
            .field("kind", &self.kind)
            .field("database_id", &self.database_id)
            .field("table_id", &self.table_id)
            .field("local_hi", &self.local_hi)
            .field("local_lo", &self.local_lo)
            .finish()
    }
}

impl From<NonZeroU64> for DatabaseId {
    #[inline]
    fn from(value: NonZeroU64) -> Self {
        Self::new(value.get())
    }
}

impl From<NonZeroU64> for TableId {
    #[inline]
    fn from(value: NonZeroU64) -> Self {
        Self::new(value.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_id_boundary_is_writer_domain() {
        let txn = TxnId::first();
        assert!(txn.is_provisional());
        assert_eq!(txn.as_writer_id().into_raw(), TRANSACTION_ID_START);
    }

    #[test]
    fn resource_key_keeps_participant_payload_opaque() {
        let key = TxnResourceKey::opaque(
            ParticipantKind::Storage,
            DatabaseId::new(7),
            Some(TableId::new(9)),
            11,
            13,
        );

        assert_eq!(key.kind(), ParticipantKind::Storage);
        assert_eq!(key.database_id(), DatabaseId::new(7));
        assert_eq!(key.table_id(), Some(TableId::new(9)));
        assert_eq!(key.local_words(), (11, 13));
    }
}
