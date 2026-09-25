// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Block Compression Codec
//!
//! Defines the `BlockCompressionCodec` trait for page-level compression.
//!
//! This module provides a unified interface for block compression algorithms
//! used in columnar storage. Unlike column encoding (RLE, Dictionary, etc.),
//! block compression operates on raw byte buffers and is applied after encoding.

use paro_common::error::Result;

/// Block compression codec trait.
///
/// Implementations provide compress/decompress operations for page bodies.
/// This is separate from column encoding - compression is applied after encoding.
pub trait BlockCompressionCodec: Send + Sync {
    /// Compress input data.
    ///
    /// # Arguments
    /// * `input` - Uncompressed data
    ///
    /// # Returns
    /// Compressed data as a new Vec
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>>;

    /// Decompress input data.
    ///
    /// # Arguments
    /// * `input` - Compressed data
    /// * `uncompressed_size` - Expected size after decompression
    ///
    /// # Returns
    /// Decompressed data as a new Vec
    fn decompress(&self, input: &[u8], uncompressed_size: usize) -> Result<Vec<u8>>;

    /// Decompress into an exact-size caller-owned destination.
    ///
    /// The default keeps existing codecs source-compatible while allowing
    /// codecs with a native bounded-output API to avoid an intermediate Vec.
    fn decompress_into(
        &self,
        input: &[u8],
        uncompressed_size: usize,
        output: &mut [u8],
    ) -> Result<()> {
        if output.len() != uncompressed_size {
            return Err(paro_common::error::invalid_input(format!(
                "decompression destination size {} does not match expected size {}",
                output.len(),
                uncompressed_size
            )));
        }
        let decompressed = self.decompress(input, uncompressed_size)?;
        if decompressed.len() != uncompressed_size {
            return Err(paro_common::error::data_corrupted(format!(
                "decompressed size {} does not match expected size {}",
                decompressed.len(),
                uncompressed_size
            )));
        }
        output.copy_from_slice(&decompressed);
        Ok(())
    }

    /// Get maximum possible compressed size for given input length.
    ///
    /// Used for pre-allocating output buffers.
    fn max_compressed_len(&self, input_len: usize) -> usize;

    /// Check if input exceeds maximum allowed size for this codec.
    ///
    /// Some codecs have input size limits.
    fn exceed_max_input_size(&self, _input_len: usize) -> bool {
        false
    }

    /// Get the compression type identifier.
    fn compression_type(&self) -> super::BlockCompressionType;
}
