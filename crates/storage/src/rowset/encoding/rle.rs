// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # RLE (Run-Length Encoding) Page
//!
//! Efficient encoding for columns with many consecutive repeated values.
//! Particularly effective for boolean columns.
//!
//! ## Page Layout
//!
//! ```text
//! +------------------+
//! | num_elements (4) |  <- Header
//! +------------------+
//! | RLE encoded data |
//! +------------------+
//! ```
//!
//! ## RLE Format
//!
//! Values are encoded as runs of (value, count) pairs using bit-packing.
//! For boolean values, uses 1-bit width. For integers, uses full bit width.

use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;

/// Header size for RLE pages.
pub const RLE_PAGE_HEADER_SIZE: usize = 4;

/// RLE encoder for a stream of values.
pub struct RleEncoder<T: Copy + Eq> {
    /// Output buffer
    buffer: BytesMut,
    /// Bit width for encoding (reserved for future use)
    #[allow(dead_code)]
    bit_width: u8,
    /// Current run value
    current_value: Option<T>,
    /// Current run length
    run_length: u32,
    /// Total values encoded
    count: u32,
}

impl<T: Copy + Eq + Default> RleEncoder<T> {
    /// Create a new RLE encoder.
    pub fn new(bit_width: u8) -> Self {
        let mut buffer = BytesMut::with_capacity(1024);
        // Reserve space for header
        buffer.resize(RLE_PAGE_HEADER_SIZE, 0);

        RleEncoder {
            buffer,
            bit_width,
            current_value: None,
            run_length: 0,
            count: 0,
        }
    }

    /// Put a value into the encoder.
    pub fn put(&mut self, value: T) {
        if let Some(current) = self.current_value {
            if value == current {
                self.run_length += 1;
            } else {
                self.flush_run();
                self.current_value = Some(value);
                self.run_length = 1;
            }
        } else {
            self.current_value = Some(value);
            self.run_length = 1;
        }
        self.count += 1;
    }

    /// Flush the current run to the buffer.
    fn flush_run(&mut self) {
        if self.run_length == 0 {
            return;
        }

        // Encode run length using varint
        let mut run_len = self.run_length;
        while run_len >= 0x80 {
            self.buffer.put_u8((run_len as u8) | 0x80);
            run_len >>= 7;
        }
        self.buffer.put_u8(run_len as u8);

        // For now, we store the value as raw bytes
        // This is simplified - real RLE would use bit-packing
        let value_bytes = unsafe {
            std::slice::from_raw_parts(
                self.current_value.as_ref().unwrap() as *const T as *const u8,
                std::mem::size_of::<T>(),
            )
        };
        self.buffer.extend_from_slice(value_bytes);

        self.run_length = 0;
    }

    /// Finish encoding and return the buffer.
    pub fn finish(&mut self) -> Bytes {
        self.flush_run();

        // Write count to header
        let count_bytes = self.count.to_le_bytes();
        self.buffer[0..4].copy_from_slice(&count_bytes);

        self.buffer.clone().freeze()
    }

    /// Get the current buffer length.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the encoder for reuse.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.buffer.resize(RLE_PAGE_HEADER_SIZE, 0);
        self.current_value = None;
        self.run_length = 0;
        self.count = 0;
    }
}

/// RLE decoder for reading encoded values.
pub struct RleDecoder<T: Copy + Default> {
    /// Input data
    data: Bytes,
    /// Current position in data
    pos: usize,
    /// Bit width (reserved for future use)
    #[allow(dead_code)]
    bit_width: u8,
    /// Current run value
    current_value: T,
    /// Remaining count in current run
    remaining_in_run: u32,
}

impl<T: Copy + Default> RleDecoder<T> {
    /// Create a new decoder.
    pub fn new(data: Bytes, bit_width: u8) -> Self {
        RleDecoder {
            data,
            pos: RLE_PAGE_HEADER_SIZE,
            bit_width,
            current_value: T::default(),
            remaining_in_run: 0,
        }
    }

    /// Read the next run from the data.
    fn read_next_run(&mut self) -> bool {
        if self.pos >= self.data.len() {
            return false;
        }

        // Read varint run length
        let mut run_length: u32 = 0;
        let mut shift = 0;
        loop {
            if self.pos >= self.data.len() {
                return false;
            }
            let byte = self.data[self.pos];
            self.pos += 1;
            run_length |= ((byte & 0x7F) as u32) << shift;
            if byte < 0x80 {
                break;
            }
            shift += 7;
        }

        // Read value
        let value_size = std::mem::size_of::<T>();
        if self.pos + value_size > self.data.len() {
            return false;
        }

        unsafe {
            std::ptr::copy_nonoverlapping(
                self.data[self.pos..].as_ptr(),
                &mut self.current_value as *mut T as *mut u8,
                value_size,
            );
        }
        self.pos += value_size;

        self.remaining_in_run = run_length;
        true
    }

    /// Get the next value.
    pub fn get(&mut self) -> Option<T> {
        if self.remaining_in_run == 0 && !self.read_next_run() {
            return None;
        }

        self.remaining_in_run -= 1;
        Some(self.current_value)
    }

    /// Skip n values.
    pub fn skip(&mut self, mut n: u32) -> bool {
        while n > 0 {
            if self.remaining_in_run == 0 && !self.read_next_run() {
                return false;
            }

            let to_skip = std::cmp::min(n, self.remaining_in_run);
            self.remaining_in_run -= to_skip;
            n -= to_skip;
        }
        true
    }

    /// Reset to the beginning.
    pub fn reset(&mut self) {
        self.pos = RLE_PAGE_HEADER_SIZE;
        self.remaining_in_run = 0;
    }
}

/// Builder for RLE-encoded pages.
pub struct RlePageBuilder<T: Copy + Eq + Default> {
    /// RLE encoder
    encoder: RleEncoder<T>,
    /// Target page size
    page_size: usize,
    /// First value
    first_value: Option<T>,
    /// Last value
    last_value: Option<T>,
    /// Whether finish() has been called
    finished: bool,
}

impl<T: Copy + Eq + Default> RlePageBuilder<T> {
    /// Create a new RLE page builder.
    pub fn new(bit_width: u8, page_size: usize) -> Self {
        RlePageBuilder {
            encoder: RleEncoder::new(bit_width),
            page_size,
            first_value: None,
            last_value: None,
            finished: false,
        }
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        self.encoder.len() >= self.page_size
    }

    /// Add values to the page.
    pub fn add(&mut self, vals: &[T]) -> u32 {
        let mut added = 0;
        for &val in vals {
            if self.is_page_full() {
                break;
            }

            if self.first_value.is_none() {
                self.first_value = Some(val);
            }
            self.last_value = Some(val);

            self.encoder.put(val);
            added += 1;
        }
        added
    }

    /// Finish building the page.
    pub fn finish(&mut self) -> Result<Bytes> {
        assert!(!self.finished);
        self.finished = true;
        Ok(self.encoder.finish())
    }

    /// Reset the builder.
    pub fn reset(&mut self) {
        self.encoder.clear();
        self.first_value = None;
        self.last_value = None;
        self.finished = false;
    }

    /// Get the element count.
    pub fn count(&self) -> u32 {
        self.encoder.count
    }

    /// Get the current size.
    pub fn size(&self) -> u64 {
        self.encoder.len() as u64
    }

    /// Get the first value.
    pub fn get_first_value(&self) -> Option<T> {
        self.first_value
    }

    /// Get the last value.
    pub fn get_last_value(&self) -> Option<T> {
        self.last_value
    }
}

/// Decoder for RLE-encoded pages.
pub struct RlePageDecoder<T: Copy + Default> {
    /// Page data
    data: Bytes,
    /// Number of elements
    num_elements: u32,
    /// RLE decoder
    decoder: Option<RleDecoder<T>>,
    /// Bit width
    bit_width: u8,
    /// Current index
    cur_index: u32,
    /// Whether init() has been called
    parsed: bool,
}

impl<T: Copy + Default> RlePageDecoder<T> {
    /// Create a new decoder.
    pub fn new(data: Bytes, bit_width: u8) -> Self {
        RlePageDecoder {
            data,
            num_elements: 0,
            decoder: None,
            bit_width,
            cur_index: 0,
            parsed: false,
        }
    }

    /// Initialize the decoder.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.len() < RLE_PAGE_HEADER_SIZE {
            return Err(paro_common::error::data_corrupted(
                "RlePageDecoder: data too small",
            ));
        }

        self.num_elements =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]);

        self.decoder = Some(RleDecoder::new(self.data.clone(), self.bit_width));
        self.parsed = true;
        self.cur_index = 0;
        Ok(())
    }

    /// Seek to a position.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "RlePageDecoder: not initialized",
            ));
        }

        if pos > self.num_elements {
            return Err(paro_common::error::out_of_range(format!(
                "position {} > num_elements {}",
                pos, self.num_elements
            )));
        }

        if pos < self.cur_index {
            // Need to reset and skip forward
            if let Some(ref mut decoder) = self.decoder {
                decoder.reset();
            }
            self.cur_index = 0;
        }

        let to_skip = pos - self.cur_index;
        if to_skip > 0 {
            if let Some(ref mut decoder) = self.decoder {
                if !decoder.skip(to_skip) {
                    return Err(paro_common::error::data_corrupted("RLE skip failed"));
                }
            }
        }

        self.cur_index = pos;
        Ok(())
    }

    /// Read the next batch of values.
    pub fn next_batch(&mut self, n: usize) -> Result<Vec<T>> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "RlePageDecoder: not initialized",
            ));
        }

        let remaining = (self.num_elements - self.cur_index) as usize;
        let to_read = std::cmp::min(n, remaining);

        let mut result = Vec::with_capacity(to_read);
        if let Some(ref mut decoder) = self.decoder {
            for _ in 0..to_read {
                if let Some(val) = decoder.get() {
                    result.push(val);
                    self.cur_index += 1;
                } else {
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Get the element count.
    pub fn count(&self) -> u32 {
        self.num_elements
    }

    /// Get the current index.
    pub fn current_index(&self) -> u32 {
        self.cur_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_bool() {
        let mut builder: RlePageBuilder<u8> = RlePageBuilder::new(1, 256 * 1024);

        // Add boolean values (0 or 1)
        let values: Vec<u8> = vec![1, 1, 1, 1, 0, 0, 1, 1, 1, 0];
        builder.add(&values);

        assert_eq!(builder.count(), 10);

        let page_data = builder.finish().unwrap();

        // Decode
        let mut decoder: RlePageDecoder<u8> = RlePageDecoder::new(page_data, 1);
        decoder.init().unwrap();

        assert_eq!(decoder.count(), 10);

        let decoded = decoder.next_batch(10).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_rle_i32() {
        let mut builder: RlePageBuilder<i32> = RlePageBuilder::new(32, 256 * 1024);

        // Values with runs
        let values: Vec<i32> = vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 3];
        builder.add(&values);

        let page_data = builder.finish().unwrap();

        let mut decoder: RlePageDecoder<i32> = RlePageDecoder::new(page_data, 32);
        decoder.init().unwrap();

        let decoded = decoder.next_batch(10).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_rle_seek() {
        let mut builder: RlePageBuilder<i32> = RlePageBuilder::new(32, 256 * 1024);

        let values: Vec<i32> = vec![1, 1, 1, 2, 2, 3, 3, 3, 3, 3];
        builder.add(&values);

        let page_data = builder.finish().unwrap();

        let mut decoder: RlePageDecoder<i32> = RlePageDecoder::new(page_data, 32);
        decoder.init().unwrap();

        // Seek to position 5
        decoder.seek_to_position(5).unwrap();
        assert_eq!(decoder.current_index(), 5);

        let decoded = decoder.next_batch(5).unwrap();
        assert_eq!(decoded, vec![3, 3, 3, 3, 3]);
    }

    #[test]
    fn test_rle_first_last_value() {
        let mut builder: RlePageBuilder<i32> = RlePageBuilder::new(32, 256 * 1024);

        let values: Vec<i32> = vec![10, 10, 20, 20, 30];
        builder.add(&values);

        assert_eq!(builder.get_first_value(), Some(10));
        assert_eq!(builder.get_last_value(), Some(30));
    }

    #[test]
    fn test_rle_single_run() {
        let mut builder: RlePageBuilder<i32> = RlePageBuilder::new(32, 256 * 1024);

        // All same value - should compress well
        let values: Vec<i32> = vec![42; 1000];
        builder.add(&values);

        let page_data = builder.finish().unwrap();

        // Page should be much smaller than raw data
        assert!(page_data.len() < 1000 * 4);

        let mut decoder: RlePageDecoder<i32> = RlePageDecoder::new(page_data, 32);
        decoder.init().unwrap();

        let decoded = decoder.next_batch(1000).unwrap();
        assert_eq!(decoded.len(), 1000);
        assert!(decoded.iter().all(|&v| v == 42));
    }

    #[test]
    fn test_rle_reset() {
        let mut builder: RlePageBuilder<i32> = RlePageBuilder::new(32, 256 * 1024);

        builder.add(&[1, 2, 3]);
        assert_eq!(builder.count(), 3);

        builder.reset();
        assert_eq!(builder.count(), 0);

        builder.add(&[4, 5]);
        assert_eq!(builder.count(), 2);
    }
}
