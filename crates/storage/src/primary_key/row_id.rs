// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Unified 64-bit RowID for primary-key and row-id based paths.

const RSSID_SHIFT: u64 = 32;
const ROW_OFFSET_MASK: u64 = 0xFFFF_FFFF;

/// Sentinel value for an invalid row identifier.
pub const NULL_ROW_ID: u64 = u64::MAX;

/// 64-bit row identifier: `rssid:32 | row_offset:32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(transparent)]
pub struct RowID(u64);

impl RowID {
    #[inline]
    pub const fn new(rssid: u32, row_offset: u32) -> Self {
        Self(((rssid as u64) << RSSID_SHIFT) | (row_offset as u64))
    }

    #[inline]
    pub const fn rssid(&self) -> u32 {
        (self.0 >> RSSID_SHIFT) as u32
    }

    #[inline]
    pub const fn row_offset(&self) -> u32 {
        (self.0 & ROW_OFFSET_MASK) as u32
    }

    #[inline]
    pub const fn is_null(&self) -> bool {
        self.0 == NULL_ROW_ID
    }

    #[inline]
    pub const fn to_raw(self) -> u64 {
        self.0
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

impl From<u64> for RowID {
    fn from(value: u64) -> Self {
        Self::from_raw(value)
    }
}

impl From<RowID> for u64 {
    fn from(value: RowID) -> Self {
        value.to_raw()
    }
}

#[cfg(test)]
mod tests {
    use super::{RowID, NULL_ROW_ID};

    #[test]
    fn row_id_roundtrip() {
        let row_id = RowID::new(42, 99);
        assert_eq!(row_id.rssid(), 42);
        assert_eq!(row_id.row_offset(), 99);
        assert_eq!(RowID::from_raw(row_id.to_raw()), row_id);
    }

    #[test]
    fn row_id_from_into_u64() {
        let row_id = RowID::new(u32::MAX, u32::MAX - 1);
        let raw: u64 = row_id.into();
        assert_eq!(RowID::from(raw), row_id);
    }

    #[test]
    fn null_row_id_is_reserved() {
        let row_id = RowID::from_raw(NULL_ROW_ID);
        assert!(row_id.is_null());
    }
}
