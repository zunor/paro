// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Plain Page Encoding
//!
//! Simple encoding for fixed-width types. Values are stored contiguously
//! with a 4-byte header containing the element count.
//!
//! ## Page Layout
//!
//! ```text
//! +------------------+
//! | num_elements (4) |  <- Header
//! +------------------+
//! | value[0]         |
//! | value[1]         |
//! | ...              |
//! | value[n-1]       |
//! +------------------+
//! ```

use bytes::{Bytes, BytesMut};
use paro_common::error::Result;

/// Header size for plain pages (4 bytes for element count).
pub const PLAIN_PAGE_HEADER_SIZE: usize = 4;

/// Builder for plain-encoded pages of fixed-width types.
pub struct PlainPageBuilder {
    /// Buffer for page data
    buffer: BytesMut,
    /// Size of each element in bytes
    type_size: usize,
    /// Target page size
    page_size: usize,
    /// Number of elements added
    count: u32,
    /// Maximum elements that fit in page
    max_count: u32,
    /// Reserved head size for external headers
    reserved_head_size: u8,
    /// First value (for get_first_value)
    first_value: Option<Bytes>,
    /// Last value (for get_last_value)
    last_value: Option<Bytes>,
}

impl PlainPageBuilder {
    /// Create a new PlainPageBuilder.
    ///
    /// # Arguments
    /// * `type_size` - Size of each element in bytes
    /// * `page_size` - Target page size in bytes (default 256KB)
    pub fn new(type_size: usize, page_size: usize) -> Self {
        let max_count = (page_size / type_size) as u32;
        let mut buffer = BytesMut::with_capacity(page_size + 1024);
        buffer.resize(PLAIN_PAGE_HEADER_SIZE, 0);

        PlainPageBuilder {
            buffer,
            type_size,
            page_size,
            count: 0,
            max_count,
            reserved_head_size: 0,
            first_value: None,
            last_value: None,
        }
    }

    /// Reserve space at the head of the buffer for external headers.
    pub fn reserve_head(&mut self, head_size: u8) {
        assert_eq!(
            self.reserved_head_size, 0,
            "reserve_head can only be called once"
        );
        assert_eq!(
            self.count, 0,
            "reserve_head must be called before adding data"
        );
        self.reserved_head_size = head_size;
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        self.buffer.len() > self.page_size
    }

    /// Add values to the page.
    ///
    /// # Arguments
    /// * `vals` - Raw bytes of values (must be aligned to type_size)
    /// * `count` - Number of values to add
    ///
    /// # Returns
    /// Number of values actually added
    pub fn add(&mut self, vals: &[u8], count: u32) -> u32 {
        if self.is_page_full() {
            return 0;
        }

        let to_add = std::cmp::min(self.max_count - self.count, count) as usize;
        let bytes_to_add = to_add * self.type_size;

        if bytes_to_add > vals.len() {
            return 0;
        }

        // Store first value if this is the first add
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

        self.buffer.extend_from_slice(&vals[..bytes_to_add]);
        self.count += to_add as u32;
        to_add as u32
    }

    /// Finish building the page and return the encoded data.
    pub fn finish(&mut self) -> Result<Bytes> {
        // Write element count to header
        let count_bytes = self.count.to_le_bytes();
        self.buffer[0..4].copy_from_slice(&count_bytes);

        if self.reserved_head_size == 0 {
            Ok(self.buffer.clone().freeze())
        } else {
            // Prepend reserved header space
            let mut result =
                BytesMut::with_capacity(self.reserved_head_size as usize + self.buffer.len());
            result.resize(self.reserved_head_size as usize, 0);
            result.extend_from_slice(&self.buffer);
            Ok(result.freeze())
        }
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.buffer.resize(PLAIN_PAGE_HEADER_SIZE, 0);
        self.count = 0;
        self.first_value = None;
        self.last_value = None;
    }

    /// Get the number of elements in the page.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the current size of the page data.
    pub fn size(&self) -> u64 {
        self.buffer.len() as u64
    }

    /// Get the first value in the page.
    pub fn get_first_value(&self) -> Option<Bytes> {
        self.first_value.clone()
    }

    /// Get the last value in the page.
    pub fn get_last_value(&self) -> Option<Bytes> {
        self.last_value.clone()
    }
}

/// Decoder for plain-encoded pages.
pub struct PlainPageDecoder {
    /// Page data
    data: Bytes,
    /// Size of each element
    type_size: usize,
    /// Number of elements in page
    num_elements: u32,
    /// Current read position
    cur_index: u32,
    /// Whether init() has been called
    parsed: bool,
}

impl PlainPageDecoder {
    /// Create a new decoder for the given page data.
    pub fn new(data: Bytes, type_size: usize) -> Self {
        PlainPageDecoder {
            data,
            type_size,
            num_elements: 0,
            cur_index: 0,
            parsed: false,
        }
    }

    /// Initialize the decoder by parsing the page header.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.len() < PLAIN_PAGE_HEADER_SIZE {
            return Err(paro_common::error::data_corrupted(format!(
                "PlainPageDecoder: data too small ({} < {})",
                self.data.len(),
                PLAIN_PAGE_HEADER_SIZE
            )));
        }

        self.num_elements =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]);

        let expected_size = PLAIN_PAGE_HEADER_SIZE + self.num_elements as usize * self.type_size;
        if self.data.len() != expected_size {
            return Err(paro_common::error::data_corrupted(format!(
                "PlainPageDecoder: size mismatch (got {}, expected {})",
                self.data.len(),
                expected_size
            )));
        }

        self.parsed = true;
        self.cur_index = 0;
        Ok(())
    }

    /// Seek to a position within the page.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "PlainPageDecoder: not initialized",
            ));
        }
        if pos > self.num_elements {
            return Err(paro_common::error::out_of_range(format!(
                "PlainPageDecoder: position {} > num_elements {}",
                pos, self.num_elements
            )));
        }
        self.cur_index = pos;
        Ok(())
    }

    /// Read the next batch of values.
    ///
    /// # Arguments
    /// * `n` - Maximum number of values to read
    ///
    /// # Returns
    /// Tuple of (values_read, data)
    pub fn next_batch(&mut self, n: usize) -> Result<(usize, Bytes)> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "PlainPageDecoder: not initialized",
            ));
        }

        let remaining = (self.num_elements - self.cur_index) as usize;
        let to_read = std::cmp::min(n, remaining);

        if to_read == 0 {
            return Ok((0, Bytes::new()));
        }

        let start = PLAIN_PAGE_HEADER_SIZE + self.cur_index as usize * self.type_size;
        let end = start + to_read * self.type_size;

        self.cur_index += to_read as u32;
        Ok((to_read, self.data.slice(start..end)))
    }

    /// Get value at a specific index.
    pub fn value_at(&self, idx: u32) -> Option<Bytes> {
        if !self.parsed || idx >= self.num_elements {
            return None;
        }
        let start = PLAIN_PAGE_HEADER_SIZE + idx as usize * self.type_size;
        let end = start + self.type_size;
        Some(self.data.slice(start..end))
    }

    /// Get the total number of elements.
    pub fn count(&self) -> u32 {
        self.num_elements
    }

    /// Get the current read position.
    pub fn current_index(&self) -> u32 {
        self.cur_index
    }

    /// Binary search for a value (assumes sorted data).
    pub fn seek_at_or_after_value<F>(&mut self, value: &[u8], cmp: F) -> Result<bool>
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "PlainPageDecoder: not initialized",
            ));
        }

        if self.num_elements == 0 {
            return Err(paro_common::error::not_supported("page is empty"));
        }

        let mut left = 0usize;
        let mut right = self.num_elements as usize;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_offset = PLAIN_PAGE_HEADER_SIZE + mid * self.type_size;
            let mid_value = &self.data[mid_offset..mid_offset + self.type_size];

            match cmp(mid_value, value) {
                std::cmp::Ordering::Less => left = mid + 1,
                _ => right = mid,
            }
        }

        if left >= self.num_elements as usize {
            return Err(paro_common::error::not_supported("value not found"));
        }

        let found_offset = PLAIN_PAGE_HEADER_SIZE + left * self.type_size;
        let found_value = &self.data[found_offset..found_offset + self.type_size];
        let exact_match = cmp(found_value, value) == std::cmp::Ordering::Equal;

        self.cur_index = left as u32;
        Ok(exact_match)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_page_i32() {
        let mut builder = PlainPageBuilder::new(4, 256 * 1024);

        // Add some i32 values
        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let added = builder.add(&bytes, 100);
        assert_eq!(added, 100);
        assert_eq!(builder.count(), 100);

        let page_data = builder.finish().unwrap();

        // Decode
        let mut decoder = PlainPageDecoder::new(page_data, 4);
        decoder.init().unwrap();
        assert_eq!(decoder.count(), 100);

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
    fn test_plain_page_seek() {
        let mut builder = PlainPageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = (0..50).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 50);

        let page_data = builder.finish().unwrap();

        let mut decoder = PlainPageDecoder::new(page_data, 4);
        decoder.init().unwrap();

        // Seek to position 25
        decoder.seek_to_position(25).unwrap();
        assert_eq!(decoder.current_index(), 25);

        // Read remaining
        let (count, data) = decoder.next_batch(100).unwrap();
        assert_eq!(count, 25);

        // First value should be 25
        let first = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(first, 25);
    }

    #[test]
    fn test_plain_page_first_last_value() {
        let mut builder = PlainPageBuilder::new(4, 256 * 1024);

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
    fn test_plain_page_binary_search() {
        let mut builder = PlainPageBuilder::new(4, 256 * 1024);

        // Sorted values
        let values: Vec<i32> = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 10);

        let page_data = builder.finish().unwrap();

        let mut decoder = PlainPageDecoder::new(page_data, 4);
        decoder.init().unwrap();

        // Search for 50 (exact match)
        let target = 50i32.to_le_bytes();
        let exact = decoder
            .seek_at_or_after_value(&target, |a, b| {
                let va = i32::from_le_bytes([a[0], a[1], a[2], a[3]]);
                let vb = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                va.cmp(&vb)
            })
            .unwrap();

        assert!(exact);
        assert_eq!(decoder.current_index(), 4);

        // Search for 55 (not exact, should find 60)
        let target = 55i32.to_le_bytes();
        let exact = decoder
            .seek_at_or_after_value(&target, |a, b| {
                let va = i32::from_le_bytes([a[0], a[1], a[2], a[3]]);
                let vb = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                va.cmp(&vb)
            })
            .unwrap();

        assert!(!exact);
        assert_eq!(decoder.current_index(), 5);
    }

    #[test]
    fn test_plain_page_reset() {
        let mut builder = PlainPageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = vec![1, 2, 3];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 3);
        assert_eq!(builder.count(), 3);

        builder.reset();
        assert_eq!(builder.count(), 0);

        // Add new values
        let values2: Vec<i32> = vec![10, 20];
        let bytes2: Vec<u8> = values2.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes2, 2);
        assert_eq!(builder.count(), 2);
    }
}
