//! # ZSTD Block Compression
//!
//! ZSTD (Zstandard) compression implementation for page-level compression.
//!
//! ZSTD provides higher compression ratios than LZ4 at the cost of speed.
//! It's recommended for cold data where space savings are more important.

use super::{BlockCompressionCodec, BlockCompressionType};
use paro_common::error::{self as paro_error, Result};

/// ZSTD block compression codec.
///
/// Uses the `zstd` crate for compression/decompression.
#[derive(Debug, Clone, Copy)]
pub struct ZstdBlockCompression {
    /// Compression level (1-22, default 3)
    level: i32,
}

impl ZstdBlockCompression {
    /// Create a new ZSTD compression codec with specified level.
    ///
    /// # Arguments
    /// * `level` - Compression level (1-22). Higher = better compression, slower.
    ///   - 1-3: Fast compression
    ///   - 4-9: Balanced
    ///   - 10-22: High compression
    pub fn new(level: i32) -> Self {
        ZstdBlockCompression {
            level: level.clamp(1, 22),
        }
    }

    /// Create a codec with default compression level (3).
    pub fn default_level() -> Self {
        ZstdBlockCompression { level: 3 }
    }

    /// Get the compression level.
    pub fn level(&self) -> i32 {
        self.level
    }
}

impl Default for ZstdBlockCompression {
    fn default() -> Self {
        Self::default_level()
    }
}

impl BlockCompressionCodec for ZstdBlockCompression {
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        zstd::encode_all(input, self.level)
            .map_err(|e| paro_error::data_corrupted(format!("ZSTD compression failed: {}", e)))
    }

    fn decompress(&self, input: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(uncompressed_size);
        zstd::stream::copy_decode(input, &mut output)
            .map_err(|e| paro_error::data_corrupted(format!("ZSTD decompression failed: {}", e)))?;
        Ok(output)
    }

    fn max_compressed_len(&self, input_len: usize) -> usize {
        zstd::zstd_safe::compress_bound(input_len)
    }

    fn compression_type(&self) -> BlockCompressionType {
        BlockCompressionType::Zstd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_roundtrip() {
        let codec = ZstdBlockCompression::default();
        let data = b"Hello, World! This is a test of ZSTD compression.";

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_compressible_data() {
        let codec = ZstdBlockCompression::default();
        // Highly compressible data (repeated pattern)
        let data: Vec<u8> = (0..10000).map(|i| (i % 10) as u8).collect();

        let compressed = codec.compress(&data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();

        assert_eq!(decompressed, data);
        // ZSTD should achieve good compression
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_zstd_different_levels() {
        let data: Vec<u8> = (0..5000).map(|i| (i % 20) as u8).collect();

        let codec_fast = ZstdBlockCompression::new(1);
        let codec_default = ZstdBlockCompression::default();
        let codec_high = ZstdBlockCompression::new(19);

        let compressed_fast = codec_fast.compress(&data).unwrap();
        let compressed_default = codec_default.compress(&data).unwrap();
        let compressed_high = codec_high.compress(&data).unwrap();

        // All should decompress correctly
        assert_eq!(
            codec_fast.decompress(&compressed_fast, data.len()).unwrap(),
            data
        );
        assert_eq!(
            codec_default
                .decompress(&compressed_default, data.len())
                .unwrap(),
            data
        );
        assert_eq!(
            codec_high.decompress(&compressed_high, data.len()).unwrap(),
            data
        );

        // Higher levels should generally produce smaller output (for compressible data)
        // Note: This isn't always true for small inputs, so we just verify they work
    }

    #[test]
    fn test_zstd_empty_data() {
        let codec = ZstdBlockCompression::default();
        let data: &[u8] = &[];

        let compressed = codec.compress(data).unwrap();
        let decompressed = codec.decompress(&compressed, 0).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_max_compressed_len() {
        let codec = ZstdBlockCompression::default();
        let input_len = 1000;
        let max_len = codec.max_compressed_len(input_len);

        // Max compressed length should be at least input length
        assert!(max_len >= input_len);
    }

    #[test]
    fn test_zstd_compression_type() {
        let codec = ZstdBlockCompression::default();
        assert_eq!(codec.compression_type(), BlockCompressionType::Zstd);
    }

    #[test]
    fn test_zstd_level_clamping() {
        let codec_low = ZstdBlockCompression::new(-5);
        let codec_high = ZstdBlockCompression::new(100);

        assert_eq!(codec_low.level(), 1);
        assert_eq!(codec_high.level(), 22);
    }

    #[test]
    fn test_zstd_invalid_compressed_data() {
        let codec = ZstdBlockCompression::default();
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];

        let result = codec.decompress(&invalid_data, 100);
        assert!(result.is_err());
    }
}
