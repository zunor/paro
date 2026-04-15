// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # No Compression (Passthrough)
//!
//! A no-op compression codec that passes data through unchanged.
//!
//! Used when compression is disabled or when data is incompressible.

use super::{BlockCompressionCodec, BlockCompressionType};
use paro_common::error::Result;

/// No-op compression codec.
///
/// Simply copies data without any transformation.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoBlockCompression;

impl NoBlockCompression {
    /// Create a new no-compression codec.
    pub fn new() -> Self {
        NoBlockCompression
    }
}

impl BlockCompressionCodec for NoBlockCompression {
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    fn decompress(&self, input: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>> {
        Ok(input.to_vec())
    }

    fn max_compressed_len(&self, input_len: usize) -> usize {
        input_len
    }

    fn compression_type(&self) -> BlockCompressionType {
        BlockCompressionType::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression_roundtrip() {
        let codec = NoBlockCompression::new();
        let data = b"Hello, World!";

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();

        assert_eq!(compressed, data);
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_no_compression_empty() {
        let codec = NoBlockCompression::new();
        let data: &[u8] = &[];

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, 0).unwrap();

        assert!(compressed.is_empty());
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_no_compression_max_len() {
        let codec = NoBlockCompression::new();
        assert_eq!(codec.max_compressed_len(100), 100);
        assert_eq!(codec.max_compressed_len(0), 0);
    }

    #[test]
    fn test_no_compression_type() {
        let codec = NoBlockCompression::new();
        assert_eq!(codec.compression_type(), BlockCompressionType::None);
    }
}
