//! # Compression Module
//!
//! Provides block compression algorithms for columnar storage.
//!
//! ## Block Compression
//!
//! Page-level compression using LZ4/ZSTD. Applied after column encoding.
//!
//! ```ignore
//! use paro_storage::compression::{
//!     BlockCompressionCodec, BlockCompressionType, get_block_compression_codec
//! };
//!
//! // Get a codec by type
//! let codec = get_block_compression_codec(BlockCompressionType::Lz4);
//!
//! // Compress data
//! let compressed = codec.compress(&data)?;
//!
//! // Decompress data
//! let decompressed = codec.decompress(&compressed, original_size)?;
//! ```

// ============================================================================
// Block Compression
// ============================================================================

mod block_compression;
mod block_compression_type;
mod lz4;
mod no_compression;
mod parallel_decompress;
mod zstd;

pub use block_compression::BlockCompressionCodec;
pub use block_compression_type::BlockCompressionType;
pub use lz4::Lz4BlockCompression;
pub use no_compression::NoBlockCompression;
pub use parallel_decompress::{ParallelDecompressTask, ParallelDecompressor};
pub use zstd::ZstdBlockCompression;

/// Get a block compression codec by type.
///
/// This is the factory function for obtaining compression codecs.
///
/// # Arguments
/// * `compression_type` - The type of compression to use
///
/// # Returns
/// A boxed codec implementing `BlockCompressionCodec`
///
/// # Example
/// ```ignore
/// let codec = get_block_compression_codec(BlockCompressionType::Lz4);
/// let compressed = codec.compress(&data)?;
/// ```
pub fn get_block_compression_codec(
    compression_type: BlockCompressionType,
) -> Box<dyn BlockCompressionCodec> {
    match compression_type {
        BlockCompressionType::None => Box::new(NoBlockCompression::new()),
        BlockCompressionType::Lz4 => Box::new(Lz4BlockCompression::new()),
        BlockCompressionType::Zstd => Box::new(ZstdBlockCompression::default()),
    }
}

/// Get a ZSTD codec with custom compression level.
///
/// # Arguments
/// * `level` - Compression level (1-22). Higher = better compression, slower.
pub fn get_zstd_codec(level: i32) -> Box<dyn BlockCompressionCodec> {
    Box::new(ZstdBlockCompression::new(level))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_block_compression_codec() {
        let codecs = [
            BlockCompressionType::None,
            BlockCompressionType::Lz4,
            BlockCompressionType::Zstd,
        ];

        for ct in codecs {
            let codec = get_block_compression_codec(ct);
            assert_eq!(codec.compression_type(), ct);
        }
    }

    #[test]
    fn test_get_zstd_codec_custom_level() {
        let codec = get_zstd_codec(15);
        assert_eq!(codec.compression_type(), BlockCompressionType::Zstd);

        let data: Vec<u8> = (0..500).map(|i| (i % 20) as u8).collect();
        let compressed = codec.compress(&data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }
}
