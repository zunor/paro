//! # Binary Plain Page Encoding
//!
//! Encoding for variable-length strings. Values are stored contiguously
//! followed by an offset table and element count.
//!
//! ## Page Layout
//!
//! ```text
//! +------------------+
//! | string[0] data   |
//! | string[1] data   |
//! | ...              |
//! | string[n-1] data |
//! +------------------+
//! | offset[0] (4)    |  <- Offset table (relative to data start)
//! | offset[1] (4)    |
//! | ...              |
//! | offset[n-1] (4)  |
//! +------------------+
//! | num_elements (4) |  <- Trailer
//! +------------------+
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;

/// Builder for binary plain-encoded pages (variable-length strings).
pub struct BinaryPlainPageBuilder {
    /// Buffer for string data
    buffer: BytesMut,
    /// Offsets of each string (relative to data start)
    offsets: Vec<u32>,
    /// Target page size
    page_size: usize,
    /// Current data offset
    next_offset: u32,
    /// Estimated total size
    size_estimate: usize,
    /// Reserved head size
    reserved_head_size: u8,
    /// First value
    first_value: Option<Bytes>,
    /// Last value
    last_value: Option<Bytes>,
    /// Whether finish() has been called
    finished: bool,
}

impl BinaryPlainPageBuilder {
    /// Create a new BinaryPlainPageBuilder.
    pub fn new(page_size: usize) -> Self {
        BinaryPlainPageBuilder {
            buffer: BytesMut::with_capacity(page_size),
            offsets: Vec::new(),
            page_size,
            next_offset: 0,
            size_estimate: 4, // Initial size for num_elements
            reserved_head_size: 0,
            first_value: None,
            last_value: None,
            finished: false,
        }
    }

    /// Reserve space at the head of the buffer.
    pub fn reserve_head(&mut self, head_size: u8) {
        assert_eq!(self.reserved_head_size, 0);
        self.reserved_head_size = head_size;
        self.buffer.resize(head_size as usize, 0);
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        self.page_size > 0 && self.size_estimate > self.page_size
    }

    /// Add a single string slice.
    pub fn add_slice(&mut self, s: &[u8]) -> bool {
        if self.is_page_full() {
            return false;
        }

        self.offsets.push(self.next_offset);
        self.buffer.extend_from_slice(s);

        self.next_offset += s.len() as u32;
        self.size_estimate += s.len() + 4; // data + offset entry

        true
    }

    /// Add multiple string slices.
    ///
    /// # Arguments
    /// * `slices` - Slice of (offset, length) pairs into `data`
    /// * `data` - Raw string data
    ///
    /// # Returns
    /// Number of strings added
    pub fn add(&mut self, slices: &[(u32, u32)], data: &[u8]) -> u32 {
        let mut added = 0;
        for &(offset, len) in slices {
            let start = offset as usize;
            let end = start + len as usize;
            if end > data.len() {
                break;
            }
            if !self.add_slice(&data[start..end]) {
                break;
            }
            added += 1;
        }
        added
    }

    /// Add strings from raw bytes with length-prefixed format.
    /// Each string is prefixed with a 4-byte length.
    pub fn add_length_prefixed(&mut self, data: &[u8]) -> u32 {
        let mut offset = 0;
        let mut added = 0;

        while offset + 4 <= data.len() {
            let len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + len > data.len() {
                break;
            }

            if !self.add_slice(&data[offset..offset + len]) {
                break;
            }

            offset += len;
            added += 1;
        }

        added
    }

    /// Finish building the page.
    pub fn finish(&mut self) -> Result<Bytes> {
        assert!(!self.finished);

        // Store first and last values
        if !self.offsets.is_empty() {
            self.first_value = Some(self.get_value(0));
            self.last_value = Some(self.get_value(self.offsets.len() - 1));
        }

        // Append offset table
        for &offset in &self.offsets {
            self.buffer.put_u32_le(offset);
        }

        // Append element count
        self.buffer.put_u32_le(self.offsets.len() as u32);

        self.finished = true;
        Ok(self.buffer.clone().freeze())
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.offsets.clear();
        self.buffer.clear();
        if self.reserved_head_size > 0 {
            self.buffer.resize(self.reserved_head_size as usize, 0);
        }
        self.next_offset = 0;
        self.size_estimate = 4;
        self.first_value = None;
        self.last_value = None;
        self.finished = false;
    }

    /// Get the number of strings in the page.
    pub fn count(&self) -> u32 {
        self.offsets.len() as u32
    }

    /// Get the estimated size.
    pub fn size(&self) -> u64 {
        self.size_estimate as u64
    }

    /// Get a string value by index (before finish).
    fn get_value(&self, idx: usize) -> Bytes {
        let start = self.reserved_head_size as usize + self.offsets[idx] as usize;
        let end = if idx + 1 < self.offsets.len() {
            self.reserved_head_size as usize + self.offsets[idx + 1] as usize
        } else {
            self.reserved_head_size as usize + self.next_offset as usize
        };
        Bytes::copy_from_slice(&self.buffer[start..end])
    }

    /// Get the first value.
    pub fn get_first_value(&self) -> Option<Bytes> {
        self.first_value.clone()
    }

    /// Get the last value.
    pub fn get_last_value(&self) -> Option<Bytes> {
        self.last_value.clone()
    }
}

/// Decoder for binary plain-encoded pages.
pub struct BinaryPlainPageDecoder {
    /// Page data
    data: Bytes,
    /// Number of elements
    num_elements: u32,
    /// Offset of the offset table
    offsets_pos: usize,
    /// Current read position
    cur_index: u32,
    /// Whether init() has been called
    parsed: bool,
}

impl BinaryPlainPageDecoder {
    /// Create a new decoder.
    pub fn new(data: Bytes) -> Self {
        BinaryPlainPageDecoder {
            data,
            num_elements: 0,
            offsets_pos: 0,
            cur_index: 0,
            parsed: false,
        }
    }

    /// Initialize the decoder.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.len() < 4 {
            return Err(paro_common::error::data_corrupted(
                "BinaryPlainPageDecoder: data too small for trailer",
            ));
        }

        // Read element count from trailer
        let trailer_pos = self.data.len() - 4;
        self.num_elements = u32::from_le_bytes([
            self.data[trailer_pos],
            self.data[trailer_pos + 1],
            self.data[trailer_pos + 2],
            self.data[trailer_pos + 3],
        ]);

        // Calculate offset table position
        self.offsets_pos = self.data.len() - 4 - (self.num_elements as usize * 4);

        self.parsed = true;
        self.cur_index = 0;
        Ok(())
    }

    /// Get offset for element at index.
    fn offset(&self, idx: u32) -> u32 {
        if idx >= self.num_elements {
            return self.offsets_pos as u32;
        }
        let pos = self.offsets_pos + idx as usize * 4;
        u32::from_le_bytes([
            self.data[pos],
            self.data[pos + 1],
            self.data[pos + 2],
            self.data[pos + 3],
        ])
    }

    /// Seek to a position.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryPlainPageDecoder: not initialized",
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

    /// Get string at index.
    pub fn string_at(&self, idx: u32) -> Option<Bytes> {
        if !self.parsed || idx >= self.num_elements {
            return None;
        }
        let start = self.offset(idx) as usize;
        let end = self.offset(idx + 1) as usize;
        Some(self.data.slice(start..end))
    }

    /// Read the next batch of strings.
    ///
    /// Returns a vector of string slices.
    pub fn next_batch(&mut self, n: usize) -> Result<Vec<Bytes>> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryPlainPageDecoder: not initialized",
            ));
        }

        let remaining = (self.num_elements - self.cur_index) as usize;
        let to_read = std::cmp::min(n, remaining);

        let mut result = Vec::with_capacity(to_read);
        for _ in 0..to_read {
            if let Some(s) = self.string_at(self.cur_index) {
                result.push(s);
                self.cur_index += 1;
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Get the number of elements.
    pub fn count(&self) -> u32 {
        self.num_elements
    }

    /// Get the current index.
    pub fn current_index(&self) -> u32 {
        self.cur_index
    }

    /// Find a string in the page (linear search).
    pub fn find(&self, target: &[u8]) -> Option<u32> {
        if !self.parsed {
            return None;
        }
        for i in 0..self.num_elements {
            if let Some(s) = self.string_at(i) {
                if s.as_ref() == target {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Get the maximum string length in the page.
    pub fn max_value_length(&self) -> u32 {
        if !self.parsed {
            return 0;
        }
        let mut max_len = 0u32;
        for i in 0..self.num_elements {
            let len = self.offset(i + 1) - self.offset(i);
            if len > max_len {
                max_len = len;
            }
        }
        max_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_plain_basic() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        // Add some strings
        builder.add_slice(b"hello");
        builder.add_slice(b"world");
        builder.add_slice(b"foo");
        builder.add_slice(b"bar");

        assert_eq!(builder.count(), 4);

        let page_data = builder.finish().unwrap();

        // Decode
        let mut decoder = BinaryPlainPageDecoder::new(page_data);
        decoder.init().unwrap();

        assert_eq!(decoder.count(), 4);

        // Read strings
        assert_eq!(decoder.string_at(0).unwrap().as_ref(), b"hello");
        assert_eq!(decoder.string_at(1).unwrap().as_ref(), b"world");
        assert_eq!(decoder.string_at(2).unwrap().as_ref(), b"foo");
        assert_eq!(decoder.string_at(3).unwrap().as_ref(), b"bar");
    }

    #[test]
    fn test_binary_plain_next_batch() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"one");
        builder.add_slice(b"two");
        builder.add_slice(b"three");
        builder.add_slice(b"four");
        builder.add_slice(b"five");

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPlainPageDecoder::new(page_data);
        decoder.init().unwrap();

        // Read first 3
        let batch1 = decoder.next_batch(3).unwrap();
        assert_eq!(batch1.len(), 3);
        assert_eq!(batch1[0].as_ref(), b"one");
        assert_eq!(batch1[1].as_ref(), b"two");
        assert_eq!(batch1[2].as_ref(), b"three");

        // Read remaining
        let batch2 = decoder.next_batch(10).unwrap();
        assert_eq!(batch2.len(), 2);
        assert_eq!(batch2[0].as_ref(), b"four");
        assert_eq!(batch2[1].as_ref(), b"five");
    }

    #[test]
    fn test_binary_plain_seek() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"a");
        builder.add_slice(b"b");
        builder.add_slice(b"c");
        builder.add_slice(b"d");

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPlainPageDecoder::new(page_data);
        decoder.init().unwrap();

        decoder.seek_to_position(2).unwrap();
        assert_eq!(decoder.current_index(), 2);

        let batch = decoder.next_batch(10).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].as_ref(), b"c");
        assert_eq!(batch[1].as_ref(), b"d");
    }

    #[test]
    fn test_binary_plain_find() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"apple");
        builder.add_slice(b"banana");
        builder.add_slice(b"cherry");

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPlainPageDecoder::new(page_data);
        decoder.init().unwrap();

        assert_eq!(decoder.find(b"banana"), Some(1));
        assert_eq!(decoder.find(b"cherry"), Some(2));
        assert_eq!(decoder.find(b"grape"), None);
    }

    #[test]
    fn test_binary_plain_empty_strings() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"");
        builder.add_slice(b"non-empty");
        builder.add_slice(b"");

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPlainPageDecoder::new(page_data);
        decoder.init().unwrap();

        assert_eq!(decoder.count(), 3);
        assert_eq!(decoder.string_at(0).unwrap().as_ref(), b"");
        assert_eq!(decoder.string_at(1).unwrap().as_ref(), b"non-empty");
        assert_eq!(decoder.string_at(2).unwrap().as_ref(), b"");
    }

    #[test]
    fn test_binary_plain_first_last_value() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"first");
        builder.add_slice(b"middle");
        builder.add_slice(b"last");

        builder.finish().unwrap();

        assert_eq!(builder.get_first_value().unwrap().as_ref(), b"first");
        assert_eq!(builder.get_last_value().unwrap().as_ref(), b"last");
    }

    #[test]
    fn test_binary_plain_max_length() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"short");
        builder.add_slice(b"medium length");
        builder.add_slice(b"this is a longer string");
        builder.add_slice(b"x");

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPlainPageDecoder::new(page_data);
        decoder.init().unwrap();

        assert_eq!(decoder.max_value_length(), 23); // "this is a longer string" is 23 bytes
    }

    #[test]
    fn test_binary_plain_reset() {
        let mut builder = BinaryPlainPageBuilder::new(256 * 1024);

        builder.add_slice(b"test1");
        builder.add_slice(b"test2");
        assert_eq!(builder.count(), 2);

        builder.reset();
        assert_eq!(builder.count(), 0);

        builder.add_slice(b"new1");
        assert_eq!(builder.count(), 1);
    }
}
