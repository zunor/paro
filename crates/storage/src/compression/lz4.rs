// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # LZ4 Block Compression
//!
//! LZ4 compression implementation for page-level compression.
//!
//! LZ4 is a fast compression algorithm optimized for speed over compression ratio.
//! It's the default choice for hot data.

use super::{BlockCompressionCodec, BlockCompressionType};
use paro_common::cold_work::{Kind, WorkScope};
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

/// Decompress an LZ4 block whose size prefix must agree with trusted page
/// metadata. The output buffer is sized from `expected_size`, never from the
/// compressed payload, so corrupt input cannot choose a second allocation size.
pub(crate) fn decompress_size_prepended_exact(
    input: &[u8],
    expected_size: usize,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let reserved = {
        let _work = WorkScope::bitshuffle_decompress_child(Kind::Lz4Reserve, expected_size);
        output.try_reserve_exact(expected_size)
    };
    reserved.map_err(|error| {
        paro_error::out_of_memory(format!(
            "Failed to reserve {expected_size} bytes for LZ4 decompression: {error}"
        ))
    })?;
    {
        let _work = WorkScope::bitshuffle_decompress_child(Kind::Lz4Initialize, expected_size);
        output.resize(expected_size, 0);
    }
    decompress_size_prepended_into(input, expected_size, &mut output)?;
    Ok(output)
}

pub(crate) fn decompress_size_prepended_into(
    input: &[u8],
    expected_size: usize,
    output: &mut [u8],
) -> Result<()> {
    if output.len() != expected_size {
        return Err(paro_error::invalid_input(format!(
            "LZ4 destination size {} does not match expected size {}",
            output.len(),
            expected_size
        )));
    }
    let size_prefix = input
        .get(..4)
        .ok_or_else(|| paro_error::data_corrupted("LZ4 block is shorter than its size prefix"))?;
    let advertised_size =
        u32::from_le_bytes(size_prefix.try_into().expect("four-byte size prefix")) as usize;
    if advertised_size != expected_size {
        return Err(paro_error::data_corrupted(format!(
            "LZ4 decoded size prefix {advertised_size} does not match expected size {expected_size}"
        )));
    }

    let decoded = {
        let _work = WorkScope::bitshuffle_decompress_child(Kind::Lz4Core, input.len() - 4);
        lz4_flex::block::decompress_into(&input[4..], output)
    };
    let decoded_size = decoded.map_err(|error| {
        paro_error::data_corrupted(format!("LZ4 decompression failed: {error}"))
    })?;
    if decoded_size != expected_size {
        return Err(paro_error::data_corrupted(format!(
            "LZ4 decoded size {decoded_size} does not match expected size {expected_size}"
        )));
    }
    Ok(())
}

impl BlockCompressionCodec for Lz4BlockCompression {
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>> {
        // lz4_flex::compress_prepend_size prepends a 4-byte little-endian size
        let compressed = lz4_flex::compress_prepend_size(input);
        Ok(compressed)
    }

    fn decompress(&self, input: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        decompress_size_prepended_exact(input, uncompressed_size)
    }

    fn decompress_into(
        &self,
        input: &[u8],
        uncompressed_size: usize,
        output: &mut [u8],
    ) -> Result<()> {
        decompress_size_prepended_into(input, uncompressed_size, output)
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
    use paro_common::cold_work::with_test_window;

    #[test]
    fn test_lz4_cold_work_children_only_split_bitshuffle_parent() {
        let data = b"bounded diagnostic output";
        let compressed = lz4_flex::compress_prepend_size(data);
        let (_, plain) = with_test_window(|| {
            assert_eq!(
                decompress_size_prepended_exact(&compressed, data.len()).unwrap(),
                data
            );
        });
        for kind in [Kind::Lz4Reserve, Kind::Lz4Initialize, Kind::Lz4Core] {
            assert_eq!(plain.metrics[kind as usize][0], 0);
        }
        let (_, split) = with_test_window(|| {
            let _parent = WorkScope::new(Kind::BitShuffleDecompress, compressed.len()).unwrap();
            assert_eq!(
                decompress_size_prepended_exact(&compressed, data.len()).unwrap(),
                data
            );
        });
        assert!(split.valid);
        for kind in [Kind::Lz4Reserve, Kind::Lz4Initialize] {
            assert_eq!(split.metrics[kind as usize][..2], [1, data.len() as u64]);
        }
        assert_eq!(
            split.metrics[Kind::Lz4Core as usize][..2],
            [1, (compressed.len() - 4) as u64]
        );
        assert_eq!(split.metrics[Kind::BitShuffleDecompress as usize][0], 1);
    }

    #[test]
    fn test_lz4_cold_work_bad_input_and_reserve_failure_restore_parent() {
        // A matching prefix followed by a truncated literal reaches LZ4 core.
        let corrupt = [1, 0, 0, 0, 0x10];
        let (_, record) = with_test_window(|| {
            let _parent = WorkScope::new(Kind::BitShuffleDecompress, corrupt.len()).unwrap();
            assert!(decompress_size_prepended_exact(&corrupt, 1).is_err());
            assert!(decompress_size_prepended_exact(&[], 1).is_err());
            // Capacity overflow is deterministic and cannot request real memory.
            assert!(decompress_size_prepended_exact(&[], isize::MAX as usize + 1).is_err());
            let compressed = lz4_flex::compress_prepend_size(b"ok");
            assert_eq!(
                decompress_size_prepended_exact(&compressed, 2).unwrap(),
                b"ok"
            );
        });
        assert!(record.valid);
        assert_eq!(record.metrics[Kind::Lz4Reserve as usize][0], 4);
        assert_eq!(record.metrics[Kind::Lz4Initialize as usize][0], 3);
        assert_eq!(record.metrics[Kind::Lz4Core as usize][0], 2);
        assert_eq!(record.metrics[Kind::BitShuffleDecompress as usize][0], 1);
    }

    #[test]
    fn test_lz4_cold_work_existing_destination_times_only_validated_core() {
        let compressed = lz4_flex::compress_prepend_size(b"ok");
        let (_, record) = with_test_window(|| {
            let _parent = WorkScope::new(Kind::BitShuffleDecompress, compressed.len()).unwrap();
            let mut output = [0; 2];
            assert!(decompress_size_prepended_into(&compressed, 1, &mut output).is_err());
            assert!(decompress_size_prepended_into(&[0, 0, 0, 0], 2, &mut output).is_err());
            decompress_size_prepended_into(&compressed, 2, &mut output).unwrap();
            assert_eq!(output, *b"ok");
        });
        assert!(record.valid);
        assert_eq!(record.metrics[Kind::Lz4Reserve as usize][0], 0);
        assert_eq!(record.metrics[Kind::Lz4Initialize as usize][0], 0);
        assert_eq!(record.metrics[Kind::Lz4Core as usize][0], 1);
    }

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

    #[test]
    fn test_lz4_rejects_size_prefix_that_disagrees_with_page_metadata() {
        let codec = Lz4BlockCompression::new();
        let compressed = codec.compress(b"bounded output").unwrap();

        let error = codec.decompress(&compressed, 1).unwrap_err();
        assert!(error.to_string().contains("does not match expected size"));
    }
}
