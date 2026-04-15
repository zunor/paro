use std::fmt;

use paro_common::error::{self as paro_error, Result};

/// Maximum encoded region index in a [`RowAddr`].
pub const MAX_REGION_INDEX: u32 = (1 << 24) - 1;
/// Maximum encoded block index in a [`RowAddr`].
pub const MAX_BLOCK_INDEX: u32 = (1 << 16) - 1;
/// Maximum encoded row offset inside a block in a [`RowAddr`].
pub const MAX_ROW_WITHIN_BLOCK: u32 = (1 << 24) - 1;

const BLOCK_SHIFT: u64 = 24;
const REGION_SHIFT: u64 = 40;
const ROW_MASK: u64 = (1 << 24) - 1;
const BLOCK_MASK: u64 = (1 << 16) - 1;
const REGION_MASK: u64 = (1 << 24) - 1;

/// Stable row handle for one sealed [`RowStore`](crate::row::RowStore).
///
/// The address is opaque to callers. Internally it is encoded as:
///
/// ```text
/// region_index:24 | block_index:16 | row_within_block:24
/// ```
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RowAddr(u64);

impl RowAddr {
    /// Invalid sentinel. This value is never emitted for a real row.
    pub const INVALID: RowAddr = RowAddr(u64::MAX);

    /// Build a row address from its encoded parts.
    pub fn new(region_index: u32, block_index: u32, row_within_block: u32) -> Result<Self> {
        if region_index > MAX_REGION_INDEX {
            return Err(paro_error::internal(format!(
                "row region index {} exceeds {}",
                region_index, MAX_REGION_INDEX
            )));
        }
        if block_index > MAX_BLOCK_INDEX {
            return Err(paro_error::internal(format!(
                "row block index {} exceeds {}",
                block_index, MAX_BLOCK_INDEX
            )));
        }
        if row_within_block > MAX_ROW_WITHIN_BLOCK {
            return Err(paro_error::internal(format!(
                "row offset {} exceeds {}",
                row_within_block, MAX_ROW_WITHIN_BLOCK
            )));
        }

        let encoded = ((region_index as u64) << REGION_SHIFT)
            | ((block_index as u64) << BLOCK_SHIFT)
            | row_within_block as u64;
        if encoded == Self::INVALID.0 {
            return Err(paro_error::internal(
                "row address encodes the invalid sentinel",
            ));
        }
        Ok(RowAddr(encoded))
    }

    /// Return the raw opaque bits. Prefer using the typed accessors when possible.
    #[inline]
    pub fn to_u64(self) -> u64 {
        self.0
    }

    /// Rebuild an address from raw opaque bits.
    pub fn from_u64(raw: u64) -> Self {
        RowAddr(raw)
    }

    /// Whether this address is the invalid sentinel.
    #[inline]
    pub fn is_invalid(self) -> bool {
        self == Self::INVALID
    }

    /// Region index component.
    #[inline]
    pub fn region_index(self) -> u32 {
        ((self.0 >> REGION_SHIFT) & REGION_MASK) as u32
    }

    /// Block index component.
    #[inline]
    pub fn block_index(self) -> u32 {
        ((self.0 >> BLOCK_SHIFT) & BLOCK_MASK) as u32
    }

    /// Row offset inside the encoded block.
    #[inline]
    pub fn row_within_block(self) -> u32 {
        (self.0 & ROW_MASK) as u32
    }
}

impl fmt::Debug for RowAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_invalid() {
            return f.write_str("RowAddr::INVALID");
        }
        f.debug_struct("RowAddr")
            .field("region", &self.region_index())
            .field("block", &self.block_index())
            .field("row", &self.row_within_block())
            .field("raw", &format_args!("{:#018x}", self.0))
            .finish()
    }
}

impl fmt::Display for RowAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_invalid() {
            return f.write_str("RowAddr::INVALID");
        }
        write!(
            f,
            "rowaddr(region={}, block={}, row={})",
            self.region_index(),
            self.block_index(),
            self.row_within_block()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_three_level_address() {
        let addr = RowAddr::new(0xabcde, 0x1234, 0xfedcb).unwrap();
        assert_eq!(addr.region_index(), 0xabcde);
        assert_eq!(addr.block_index(), 0x1234);
        assert_eq!(addr.row_within_block(), 0xfedcb);
        assert_eq!(RowAddr::from_u64(addr.to_u64()), addr);
    }

    #[test]
    fn rejects_invalid_sentinel() {
        assert!(RowAddr::INVALID.is_invalid());
        assert!(RowAddr::new(MAX_REGION_INDEX, MAX_BLOCK_INDEX, MAX_ROW_WITHIN_BLOCK).is_err());
    }
}
