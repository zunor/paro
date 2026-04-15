// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # BitShuffle Page Encoding
//!
//! Bit-level shuffling combined with LZ4 compression for numeric types.
//! Highly effective for columns with similar values or patterns.
//!
//! ## Page Layout
//!
//! ```text
//! +------------------------+
//! | num_elements (4)       |  <- Header (16 bytes total)
//! | compressed_size (4)    |
//! | padded_num_elements (4)|
//! | elem_size_bytes (4)    |
//! +------------------------+
//! | bitshuffle+lz4 data    |
//! +------------------------+
//! ```
//!
//! ## Algorithm
//!
//! 1. Pad input to multiple of 8 elements
//! 2. Transpose bits across elements (bitshuffle)
//! 3. Compress with LZ4
//!
//! This is effective because similar values have similar bit patterns,
//! and transposing groups similar bits together for better compression.

use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;

/// Header size for bitshuffle pages.
pub const BITSHUFFLE_PAGE_HEADER_SIZE: usize = 16;

/// Align value up to multiple of 8.
fn align_up_8(n: u32) -> u32 {
    (n + 7) & !7
}

/// Builder for bitshuffle-encoded pages.
pub struct BitShufflePageBuilder {
    /// Raw data buffer
    data: BytesMut,
    /// Element size in bytes
    type_size: usize,
    /// Maximum elements per page
    max_count: u32,
    /// Current element count
    count: u32,
    /// Reserved head size
    reserved_head_size: u8,
    /// First value
    first_value: Option<Bytes>,
    /// Last value
    last_value: Option<Bytes>,
    /// Whether finish() has been called
    finished: bool,
}

impl BitShufflePageBuilder {
    /// Create a new BitShuffle page builder.
    ///
    /// # Arguments
    /// * `type_size` - Size of each element in bytes
    /// * `page_size` - Target page size
    pub fn new(type_size: usize, page_size: usize) -> Self {
        let max_count = (page_size / type_size) as u32;
        let capacity = align_up_8(max_count) as usize * type_size;

        BitShufflePageBuilder {
            data: BytesMut::with_capacity(capacity),
            type_size,
            max_count,
            count: 0,
            reserved_head_size: 0,
            first_value: None,
            last_value: None,
            finished: false,
        }
    }

    /// Reserve space at the head.
    pub fn reserve_head(&mut self, head_size: u8) {
        assert_eq!(self.reserved_head_size, 0);
        self.reserved_head_size = head_size;
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        self.count >= self.max_count
    }

    /// Add values to the page.
    pub fn add(&mut self, vals: &[u8], count: u32) -> u32 {
        if self.finished {
            return 0;
        }

        let to_add = std::cmp::min(self.max_count - self.count, count) as usize;
        let bytes_to_add = to_add * self.type_size;

        if bytes_to_add > vals.len() {
            return 0;
        }

        // Store first value
        if self.count == 0 && to_add > 0 {
            self.first_value = Some(Bytes::copy_from_slice(&vals[..self.type_size]));
        }

        // Store last value
        if to_add > 0 {
            let last_offset = (to_add - 1) * self.type_size;
            self.last_value = Some(Bytes::copy_from_slice(
                &vals[last_offset..last_offset + self.type_size],
            ));
        }

        self.data.extend_from_slice(&vals[..bytes_to_add]);
        self.count += to_add as u32;
        to_add as u32
    }

    /// Add a single element.
    pub fn add_one(&mut self, elem: &[u8]) -> bool {
        if self.count >= self.max_count || elem.len() != self.type_size {
            return false;
        }

        if self.count == 0 {
            self.first_value = Some(Bytes::copy_from_slice(elem));
        }
        self.last_value = Some(Bytes::copy_from_slice(elem));

        self.data.extend_from_slice(elem);
        self.count += 1;
        true
    }

    /// Finish building the page.
    pub fn finish(&mut self) -> Result<Bytes> {
        assert!(!self.finished);
        self.finished = true;

        // Pad to multiple of 8
        let padded_count = align_up_8(self.count);
        let padding_bytes = (padded_count - self.count) as usize * self.type_size;
        for _ in 0..padding_bytes {
            self.data.put_u8(0);
        }

        // Perform bitshuffle
        let shuffled = bitshuffle(&self.data, self.type_size);

        // Compress with LZ4
        let compressed = lz4_flex::compress_prepend_size(&shuffled);

        // Build output with header
        let total_size =
            self.reserved_head_size as usize + BITSHUFFLE_PAGE_HEADER_SIZE + compressed.len();
        let mut output = BytesMut::with_capacity(total_size);

        // Reserved header space
        output.resize(self.reserved_head_size as usize, 0);

        // Write header
        output.put_u32_le(self.count);
        output.put_u32_le((BITSHUFFLE_PAGE_HEADER_SIZE + compressed.len()) as u32);
        output.put_u32_le(padded_count);
        output.put_u32_le(self.type_size as u32);

        // Write compressed data
        output.extend_from_slice(&compressed);

        Ok(output.freeze())
    }

    /// Reset the builder.
    pub fn reset(&mut self) {
        self.data.clear();
        self.count = 0;
        self.first_value = None;
        self.last_value = None;
        self.finished = false;
    }

    /// Get element count.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get current data size.
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Get first value.
    pub fn get_first_value(&self) -> Option<Bytes> {
        self.first_value.clone()
    }

    /// Get last value.
    pub fn get_last_value(&self) -> Option<Bytes> {
        self.last_value.clone()
    }

    /// Get value at index.
    pub fn cell(&self, idx: u32) -> Option<Bytes> {
        if idx >= self.count {
            return None;
        }
        let start = idx as usize * self.type_size;
        let end = start + self.type_size;
        Some(Bytes::copy_from_slice(&self.data[start..end]))
    }
}

/// Decoder for bitshuffle-encoded pages.
pub struct BitShufflePageDecoder {
    /// Page data
    data: Bytes,
    /// Decompressed and unshuffled data
    decoded_data: Option<Bytes>,
    /// Number of elements
    num_elements: u32,
    /// Compressed size
    compressed_size: u32,
    /// Padded element count
    padded_num_elements: u32,
    /// Element size
    type_size: usize,
    /// Current index
    cur_index: u32,
    /// Whether init() has been called
    parsed: bool,
}

impl BitShufflePageDecoder {
    /// Create a new decoder.
    pub fn new(data: Bytes) -> Self {
        BitShufflePageDecoder {
            data,
            decoded_data: None,
            num_elements: 0,
            compressed_size: 0,
            padded_num_elements: 0,
            type_size: 0,
            cur_index: 0,
            parsed: false,
        }
    }

    /// Initialize the decoder.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.len() < BITSHUFFLE_PAGE_HEADER_SIZE {
            return Err(paro_common::error::data_corrupted(
                "BitShufflePageDecoder: data too small for header",
            ));
        }

        // Parse header
        self.num_elements =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]);
        self.compressed_size =
            u32::from_le_bytes([self.data[4], self.data[5], self.data[6], self.data[7]]);
        self.padded_num_elements =
            u32::from_le_bytes([self.data[8], self.data[9], self.data[10], self.data[11]]);
        self.type_size =
            u32::from_le_bytes([self.data[12], self.data[13], self.data[14], self.data[15]])
                as usize;

        // Validate
        if self.padded_num_elements != align_up_8(self.num_elements) {
            return Err(paro_common::error::data_corrupted(
                "BitShufflePageDecoder: invalid padded element count",
            ));
        }

        // Validate type size
        match self.type_size {
            1 | 2 | 4 | 8 | 16 => {}
            _ => {
                return Err(paro_common::error::data_corrupted(format!(
                    "BitShufflePageDecoder: invalid type size {}",
                    self.type_size
                )));
            }
        }

        // Decompress
        let compressed_data = &self.data[BITSHUFFLE_PAGE_HEADER_SIZE..];
        let decompressed = lz4_flex::decompress_size_prepended(compressed_data).map_err(|e| {
            paro_common::error::data_corrupted(format!("LZ4 decompress failed: {}", e))
        })?;

        // Unshuffle
        let unshuffled = bitunshuffle(&decompressed, self.type_size);
        self.decoded_data = Some(Bytes::from(unshuffled));

        self.parsed = true;
        self.cur_index = 0;
        Ok(())
    }

    /// Seek to a position.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BitShufflePageDecoder: not initialized",
            ));
        }
        if pos > self.num_elements {
            return Err(paro_common::error::out_of_range(format!(
                "position {} > num_elements {}",
                pos, self.num_elements
            )));
        }
        self.cur_index = pos;
        Ok(())
    }

    /// Read the next batch of values.
    pub fn next_batch(&mut self, n: usize) -> Result<(usize, Bytes)> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BitShufflePageDecoder: not initialized",
            ));
        }

        let remaining = (self.num_elements - self.cur_index) as usize;
        let to_read = std::cmp::min(n, remaining);

        if to_read == 0 {
            return Ok((0, Bytes::new()));
        }

        let decoded = self.decoded_data.as_ref().unwrap();
        let start = self.cur_index as usize * self.type_size;
        let end = start + to_read * self.type_size;

        self.cur_index += to_read as u32;
        Ok((to_read, decoded.slice(start..end)))
    }

    /// Get value at index.
    pub fn value_at(&self, idx: u32) -> Option<Bytes> {
        if !self.parsed || idx >= self.num_elements {
            return None;
        }
        let decoded = self.decoded_data.as_ref()?;
        let start = idx as usize * self.type_size;
        let end = start + self.type_size;
        Some(decoded.slice(start..end))
    }

    /// Get element count.
    pub fn count(&self) -> u32 {
        self.num_elements
    }

    /// Get current index.
    pub fn current_index(&self) -> u32 {
        self.cur_index
    }

    /// Get type size.
    pub fn type_size(&self) -> usize {
        self.type_size
    }
}

/// Perform bitshuffle on data.
///
/// Transposes bits across elements to group similar bits together.
fn bitshuffle(data: &[u8], type_size: usize) -> Vec<u8> {
    let num_elements = data.len() / type_size;
    if num_elements == 0 {
        return Vec::new();
    }

    let mut output = vec![0u8; data.len()];

    // For each bit position
    for bit in 0..(type_size * 8) {
        let byte_idx = bit / 8;
        let bit_idx = bit % 8;

        // For each element
        for elem in 0..num_elements {
            let src_byte = data[elem * type_size + byte_idx];
            let src_bit = (src_byte >> bit_idx) & 1;

            // Output position: bit * num_elements + elem
            let out_bit_pos = bit * num_elements + elem;
            let out_byte_idx = out_bit_pos / 8;
            let out_bit_idx = out_bit_pos % 8;

            output[out_byte_idx] |= src_bit << out_bit_idx;
        }
    }

    output
}

/// Reverse bitshuffle operation.
fn bitunshuffle(data: &[u8], type_size: usize) -> Vec<u8> {
    let total_bits = data.len() * 8;
    let bits_per_element = type_size * 8;
    let num_elements = total_bits / bits_per_element;

    if num_elements == 0 {
        return Vec::new();
    }

    let mut output = vec![0u8; num_elements * type_size];

    // For each bit position
    for bit in 0..bits_per_element {
        let out_byte_idx = bit / 8;
        let out_bit_idx = bit % 8;

        // For each element
        for elem in 0..num_elements {
            // Input position: bit * num_elements + elem
            let in_bit_pos = bit * num_elements + elem;
            let in_byte_idx = in_bit_pos / 8;
            let in_bit_idx = in_bit_pos % 8;

            if in_byte_idx < data.len() {
                let src_bit = (data[in_byte_idx] >> in_bit_idx) & 1;
                output[elem * type_size + out_byte_idx] |= src_bit << out_bit_idx;
            }
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitshuffle_roundtrip() {
        let data: Vec<u8> = vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];
        let shuffled = bitshuffle(&data, 4);
        let unshuffled = bitunshuffle(&shuffled, 4);
        assert_eq!(data, unshuffled);
    }

    #[test]
    fn test_bitshuffle_page_i32() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);

        // Add some i32 values
        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let added = builder.add(&bytes, 100);
        assert_eq!(added, 100);
        assert_eq!(builder.count(), 100);

        let page_data = builder.finish().unwrap();

        // Decode
        let mut decoder = BitShufflePageDecoder::new(page_data);
        decoder.init().unwrap();
        assert_eq!(decoder.count(), 100);
        assert_eq!(decoder.type_size(), 4);

        // Read all values
        let (count, data) = decoder.next_batch(100).unwrap();
        assert_eq!(count, 100);
        assert_eq!(data.len(), 400);

        // Verify values
        for i in 0..100 {
            let offset = i * 4;
            let value = i32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            assert_eq!(value, i as i32);
        }
    }

    #[test]
    fn test_bitshuffle_page_i64() {
        let mut builder = BitShufflePageBuilder::new(8, 256 * 1024);

        let values: Vec<i64> = (0..50).map(|i| i * 1000).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        builder.add(&bytes, 50);
        let page_data = builder.finish().unwrap();

        let mut decoder = BitShufflePageDecoder::new(page_data);
        decoder.init().unwrap();

        let (count, data) = decoder.next_batch(50).unwrap();
        assert_eq!(count, 50);

        for i in 0..50 {
            let offset = i * 8;
            let value = i64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            assert_eq!(value, i as i64 * 1000);
        }
    }

    #[test]
    fn test_bitshuffle_seek() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 100);

        let page_data = builder.finish().unwrap();

        let mut decoder = BitShufflePageDecoder::new(page_data);
        decoder.init().unwrap();

        // Seek to position 50
        decoder.seek_to_position(50).unwrap();
        assert_eq!(decoder.current_index(), 50);

        let (count, data) = decoder.next_batch(10).unwrap();
        assert_eq!(count, 10);

        // First value should be 50
        let first = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(first, 50);
    }

    #[test]
    fn test_bitshuffle_compression() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);

        // Repeated values should compress well
        let values: Vec<i32> = vec![42; 1000];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 1000);

        let page_data = builder.finish().unwrap();

        // Compressed page should be much smaller than raw data
        assert!(page_data.len() < 1000 * 4);

        // Verify data
        let mut decoder = BitShufflePageDecoder::new(page_data);
        decoder.init().unwrap();

        let (count, data) = decoder.next_batch(1000).unwrap();
        assert_eq!(count, 1000);

        for i in 0..1000 {
            let offset = i * 4;
            let value = i32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            assert_eq!(value, 42);
        }
    }

    #[test]
    fn test_bitshuffle_first_last_value() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = vec![10, 20, 30, 40, 50];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 5);

        let first = builder.get_first_value().unwrap();
        let last = builder.get_last_value().unwrap();

        assert_eq!(
            i32::from_le_bytes([first[0], first[1], first[2], first[3]]),
            10
        );
        assert_eq!(i32::from_le_bytes([last[0], last[1], last[2], last[3]]), 50);
    }

    #[test]
    fn test_bitshuffle_reset() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = vec![1, 2, 3];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 3);
        assert_eq!(builder.count(), 3);

        builder.reset();
        assert_eq!(builder.count(), 0);

        let values2: Vec<i32> = vec![10, 20];
        let bytes2: Vec<u8> = values2.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes2, 2);
        assert_eq!(builder.count(), 2);
    }
}
