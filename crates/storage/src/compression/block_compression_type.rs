//! # Block Compression Type
//!
//! Defines compression algorithm types for page-level compression.
//!
//! This is separate from the existing `CompressionType` which is used for
//! column-level encoding selection. `BlockCompressionType` is specifically
//! for LZ4/ZSTD block compression applied after encoding.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Block compression algorithm types.
///
/// These are applied to encoded page data for additional space savings.
/// Unlike column encoding (RLE, Dictionary, etc.), block compression
/// operates on raw byte buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum BlockCompressionType {
    /// No compression - store raw data.
    #[default]
    None = 0,

    /// LZ4 compression.
    /// Fast compression/decompression, moderate compression ratio.
    /// Recommended for hot data where speed is critical.
    Lz4 = 1,

    /// ZSTD compression.
    /// Higher compression ratio, slower than LZ4.
    /// Recommended for cold data where space savings matter more.
    Zstd = 2,
}

impl BlockCompressionType {
    /// Convert from u8 tag for deserialization.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(BlockCompressionType::None),
            1 => Some(BlockCompressionType::Lz4),
            2 => Some(BlockCompressionType::Zstd),
            _ => None,
        }
    }

    /// Convert to u8 tag for serialization.
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Parse compression type from string (case-insensitive).
    pub fn from_string(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "no_compression" | "uncompressed" => Some(BlockCompressionType::None),
            "lz4" => Some(BlockCompressionType::Lz4),
            "zstd" | "zstandard" => Some(BlockCompressionType::Zstd),
            _ => None,
        }
    }

    /// Check if this type represents actual compression.
    pub fn is_compressed(&self) -> bool {
        !matches!(self, BlockCompressionType::None)
    }
}

impl fmt::Display for BlockCompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockCompressionType::None => write!(f, "None"),
            BlockCompressionType::Lz4 => write!(f, "LZ4"),
            BlockCompressionType::Zstd => write!(f, "ZSTD"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_compression_type_default() {
        assert_eq!(BlockCompressionType::default(), BlockCompressionType::None);
    }

    #[test]
    fn test_block_compression_type_roundtrip() {
        let types = [
            BlockCompressionType::None,
            BlockCompressionType::Lz4,
            BlockCompressionType::Zstd,
        ];

        for ct in types {
            let tag = ct.to_u8();
            let recovered = BlockCompressionType::from_u8(tag);
            assert_eq!(recovered, Some(ct), "Roundtrip failed for {:?}", ct);
        }
    }

    #[test]
    fn test_block_compression_type_from_string() {
        assert_eq!(
            BlockCompressionType::from_string("none"),
            Some(BlockCompressionType::None)
        );
        assert_eq!(
            BlockCompressionType::from_string("LZ4"),
            Some(BlockCompressionType::Lz4)
        );
        assert_eq!(
            BlockCompressionType::from_string("zstd"),
            Some(BlockCompressionType::Zstd)
        );
        assert_eq!(
            BlockCompressionType::from_string("ZSTANDARD"),
            Some(BlockCompressionType::Zstd)
        );
        assert_eq!(BlockCompressionType::from_string("invalid"), None);
    }

    #[test]
    fn test_is_compressed() {
        assert!(!BlockCompressionType::None.is_compressed());
        assert!(BlockCompressionType::Lz4.is_compressed());
        assert!(BlockCompressionType::Zstd.is_compressed());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", BlockCompressionType::None), "None");
        assert_eq!(format!("{}", BlockCompressionType::Lz4), "LZ4");
        assert_eq!(format!("{}", BlockCompressionType::Zstd), "ZSTD");
    }
}
