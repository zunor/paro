//! # LZ4 Block Compression
//!
//! LZ4 compression implementation for page-level compression.
//!
//! LZ4 is a fast compression algorithm optimized for speed over compression ratio.
//! It's the default choice for hot data.

use super::{BlockCompressionCodec, BlockCompressionType};
use paro_common::error::{self as paro_error, Result};

/// LZ4 block compression codec.
///
/// Uses `lz4_flex` crate for compression/decompression.
/// The compressed format includes a 4-byte size prefix for decompression.
#[derive(Debug, Clone, Copy, Default)]
pub struct Lz4BlockCompression;

impl Lz4BlockCompression {
    /// Create a new LZ4 compression codec.
    pub fn new() -> Self {
        Lz4BlockCompression
    }
}

impl BlockCompressionCodec for Lz4BlockCompression {
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        // lz4_flex::compress_prepend_size prepends a 4-byte little-endian size
        let compressed = lz4_flex::compress_prepend_size(input);
        Ok(compressed)
    }

    fn decompress(&self, input: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>> {
        // lz4_flex::decompress_size_prepended reads the size prefix
        lz4_flex::decompress_size_prepended(input)
            .map_err(|e| paro_error::data_corrupted(format!("LZ4 decompression failed: {}", e)))
    }

    fn max_compressed_len(&self, input_len: usize) -> usize {
        // +4 for the size prefix
        lz4_flex::block::get_maximum_output_size(input_len) + 4
    }

    fn compression_type(&self) -> BlockCompressionType {
        BlockCompressionType::Lz4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip() {
        let codec = Lz4BlockCompression::new();
        let data = b"Hello, World! This is a test of LZ4 compression.";

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compressible_data() {
        let codec = Lz4BlockCompression::new();
        // Highly compressible data (repeated pattern)
        let data: Vec<u8> = (0..10000).map(|i| (i % 10) as u8).collect();

        let compressed = codec.compress(&data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        // Should achieve significant compression
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_lz4_incompressible_data() {
        let codec = Lz4BlockCompression::new();
        // Random-ish data that doesn't compress well
        let data: Vec<u8> = (0..1000).map(|i| ((i * 17 + 31) % 256) as u8).collect();

        let compressed = codec.compress(&data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_empty_data() {
        let codec = Lz4BlockCompression::new();
        let data: &[u8] = &[];

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, 0).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_max_compressed_len() {
        let codec = Lz4BlockCompression::new();
        let input_len = 1000;
        let max_len = codec.max_compressed_len(input_len);

        // Max compressed length should be at least input length + overhead
        assert!(max_len >= input_len);
    }

    #[test]
    fn test_lz4_compression_type() {
        let codec = Lz4BlockCompression::new();
        assert_eq!(codec.compression_type(), BlockCompressionType::Lz4);
    }

    #[test]
    fn test_lz4_invalid_compressed_data() {
        let codec = Lz4BlockCompression::new();
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];

        let result = codec.decompress(&invalid_data, 100);
        assert!(result.is_err());
    }
}
