//! # Binary Prefix Page Encoding
//!
//! Prefix compression for sorted strings. Stores shared prefix length
//! and unique suffix for each entry, with restart points for random access.
//!
//! ## Page Layout
//!
//! ```text
//! +---------------------------+
//! | Entry 0                   |
//! |   shared_len (varint)     |
//! |   unshared_len (varint)   |
//! |   unshared_data           |
//! +---------------------------+
//! | Entry 1                   |
//! | ...                       |
//! +---------------------------+
//! | Trailer                   |
//! |   num_entries (4)         |
//! |   restart_interval (1)    |
//! |   restart_offsets[]       |
//! |   num_restarts (4)        |
//! +---------------------------+
//! ```
//!
//! Restart points store full values at regular intervals for seeking.

use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;

/// Default restart point interval.
const RESTART_POINT_INTERVAL: usize = 16;

/// Builder for binary prefix encoded pages.
pub struct BinaryPrefixPageBuilder {
    /// Output buffer
    buffer: BytesMut,
    /// Restart point offsets
    restart_offsets: Vec<u32>,
    /// Last entry for prefix calculation
    last_entry: Vec<u8>,
    /// First entry
    first_entry: Vec<u8>,
    /// Entry count
    count: u32,
    /// Target page size
    page_size: usize,
    /// Whether finish() has been called
    finished: bool,
}

impl BinaryPrefixPageBuilder {
    /// Create a new builder.
    pub fn new(page_size: usize) -> Self {
        BinaryPrefixPageBuilder {
            buffer: BytesMut::with_capacity(page_size),
            restart_offsets: Vec::new(),
            last_entry: Vec::new(),
            first_entry: Vec::new(),
            count: 0,
            page_size,
            finished: false,
        }
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        self.size() >= self.page_size as u64
    }

    /// Get current size estimate.
    pub fn size(&self) -> u64 {
        if self.finished {
            self.buffer.len() as u64
        } else {
            // Buffer + trailer size estimate
            self.buffer.len() as u64 + (self.restart_offsets.len() + 2) as u64 * 4 + 1
        }
    }

    /// Add a single string value.
    ///
    /// Values should be added in sorted order for best compression.
    pub fn add(&mut self, vals: &[u8], count: u32) -> u32 {
        if self.finished {
            return 0;
        }

        // For binary prefix, we expect Slice format: 4-byte length + data
        let mut offset = 0;
        let mut added = 0;

        for _ in 0..count {
            if offset + 4 > vals.len() {
                break;
            }

            let len = u32::from_le_bytes([
                vals[offset],
                vals[offset + 1],
                vals[offset + 2],
                vals[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + len > vals.len() {
                break;
            }

            let value = &vals[offset..offset + len];
            offset += len;

            if !self.add_one(value) {
                break;
            }
            added += 1;
        }

        added
    }

    /// Add a single value.
    pub fn add_one(&mut self, value: &[u8]) -> bool {
        if self.finished || self.is_page_full() {
            return false;
        }

        // Store first entry
        if self.count == 0 {
            self.first_entry = value.to_vec();
        }

        // Check if this is a restart point
        if (self.count as usize).is_multiple_of(RESTART_POINT_INTERVAL) {
            self.restart_offsets.push(self.buffer.len() as u32);
            // At restart points, store full value (shared_len = 0)
            put_varint(&mut self.buffer, 0);
            put_varint(&mut self.buffer, value.len() as u32);
            self.buffer.extend_from_slice(value);
        } else {
            // Calculate shared prefix with last entry
            let shared_len = common_prefix_len(&self.last_entry, value);
            let unshared_len = value.len() - shared_len;

            put_varint(&mut self.buffer, shared_len as u32);
            put_varint(&mut self.buffer, unshared_len as u32);
            self.buffer.extend_from_slice(&value[shared_len..]);
        }

        self.last_entry = value.to_vec();
        self.count += 1;
        true
    }

    /// Finish building the page.
    pub fn finish(&mut self) -> Result<Bytes> {
        if self.finished {
            return Err(paro_common::error::internal("already finished"));
        }
        self.finished = true;

        // Write trailer
        self.buffer.put_u32_le(self.count);
        self.buffer.put_u8(RESTART_POINT_INTERVAL as u8);

        for &offset in &self.restart_offsets {
            self.buffer.put_u32_le(offset);
        }
        self.buffer.put_u32_le(self.restart_offsets.len() as u32);

        Ok(self.buffer.clone().freeze())
    }

    /// Reset the builder.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restart_offsets.clear();
        self.last_entry.clear();
        self.first_entry.clear();
        self.count = 0;
        self.finished = false;
    }

    /// Get entry count.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get first value.
    pub fn get_first_value(&self) -> Option<Bytes> {
        if self.count == 0 {
            None
        } else {
            Some(Bytes::copy_from_slice(&self.first_entry))
        }
    }

    /// Get last value.
    pub fn get_last_value(&self) -> Option<Bytes> {
        if self.count == 0 {
            None
        } else {
            Some(Bytes::copy_from_slice(&self.last_entry))
        }
    }
}

/// Decoder for binary prefix encoded pages.
pub struct BinaryPrefixPageDecoder {
    /// Page data
    data: Bytes,
    /// Number of entries
    num_entries: u32,
    /// Restart interval
    restart_interval: usize,
    /// Number of restart points
    num_restarts: u32,
    /// Pointer to restart offsets
    restarts_offset: usize,
    /// Footer start offset
    footer_start: usize,
    /// Current position
    cur_pos: u32,
    /// Current decoded value
    current_value: Vec<u8>,
    /// Next read pointer
    next_ptr: usize,
    /// Whether init() has been called
    parsed: bool,
}

impl BinaryPrefixPageDecoder {
    /// Create a new decoder.
    pub fn new(data: Bytes) -> Self {
        BinaryPrefixPageDecoder {
            data,
            num_entries: 0,
            restart_interval: 0,
            num_restarts: 0,
            restarts_offset: 0,
            footer_start: 0,
            cur_pos: 0,
            current_value: Vec::new(),
            next_ptr: 0,
            parsed: false,
        }
    }

    /// Initialize the decoder.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.len() < 9 {
            return Err(paro_common::error::data_corrupted(
                "BinaryPrefixPageDecoder: data too small",
            ));
        }

        // Read trailer from end
        let len = self.data.len();
        self.num_restarts = u32::from_le_bytes([
            self.data[len - 4],
            self.data[len - 3],
            self.data[len - 2],
            self.data[len - 1],
        ]);

        let restarts_size = self.num_restarts as usize * 4;
        self.restarts_offset = len - 4 - restarts_size;

        self.restart_interval = self.data[self.restarts_offset - 1] as usize;
        self.num_entries = u32::from_le_bytes([
            self.data[self.restarts_offset - 5],
            self.data[self.restarts_offset - 4],
            self.data[self.restarts_offset - 3],
            self.data[self.restarts_offset - 2],
        ]);

        self.footer_start = self.restarts_offset - 5;
        self.next_ptr = 0;
        self.cur_pos = 0;
        self.current_value.clear();

        // Read first value
        if self.num_entries > 0 {
            self.read_next_value()?;
        }

        self.parsed = true;
        Ok(())
    }

    /// Seek to a position.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryPrefixPageDecoder: not initialized",
            ));
        }
        if pos > self.num_entries {
            return Err(paro_common::error::out_of_range(format!(
                "position {} > num_entries {}",
                pos, self.num_entries
            )));
        }

        if pos == 0 {
            self.next_ptr = 0;
            self.cur_pos = 0;
            self.current_value.clear();
            if self.num_entries > 0 {
                self.read_next_value()?;
            }
            return Ok(());
        }

        // Find the restart point at or before pos
        let restart_idx = pos as usize / self.restart_interval;
        let restart_idx = std::cmp::min(restart_idx, self.num_restarts as usize - 1);

        self.seek_to_restart_point(restart_idx)?;

        // Read forward to pos
        while self.cur_pos < pos {
            self.read_next_value()?;
            self.cur_pos += 1;
        }

        Ok(())
    }

    fn seek_to_restart_point(&mut self, restart_idx: usize) -> Result<()> {
        let offset_pos = self.restarts_offset + restart_idx * 4;
        let offset = u32::from_le_bytes([
            self.data[offset_pos],
            self.data[offset_pos + 1],
            self.data[offset_pos + 2],
            self.data[offset_pos + 3],
        ]) as usize;

        self.next_ptr = offset;
        self.cur_pos = (restart_idx * self.restart_interval) as u32;
        self.current_value.clear();

        // Read the value at restart point
        self.read_next_value()?;
        Ok(())
    }

    fn read_next_value(&mut self) -> Result<()> {
        if self.next_ptr >= self.footer_start {
            return Err(paro_common::error::not_supported("no more values"));
        }

        let (shared_len, bytes_read1) = get_varint(&self.data[self.next_ptr..])?;
        self.next_ptr += bytes_read1;

        let (unshared_len, bytes_read2) = get_varint(&self.data[self.next_ptr..])?;
        self.next_ptr += bytes_read2;

        // Build value from shared prefix + unshared suffix
        self.current_value.truncate(shared_len as usize);
        self.current_value
            .extend_from_slice(&self.data[self.next_ptr..self.next_ptr + unshared_len as usize]);
        self.next_ptr += unshared_len as usize;

        Ok(())
    }

    /// Read the next batch of values.
    pub fn next_batch(&mut self, n: usize) -> Result<(usize, Vec<Bytes>)> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryPrefixPageDecoder: not initialized",
            ));
        }

        let remaining = (self.num_entries - self.cur_pos) as usize;
        let to_read = std::cmp::min(n, remaining);

        if to_read == 0 {
            return Ok((0, Vec::new()));
        }

        let mut results = Vec::with_capacity(to_read);

        for _ in 0..to_read {
            results.push(Bytes::copy_from_slice(&self.current_value));
            self.cur_pos += 1;

            if self.cur_pos < self.num_entries {
                self.read_next_value()?;
            }
        }

        Ok((to_read, results))
    }

    /// Get value at index.
    pub fn value_at(&mut self, idx: u32) -> Result<Option<Bytes>> {
        if !self.parsed || idx >= self.num_entries {
            return Ok(None);
        }

        self.seek_to_position(idx)?;
        Ok(Some(Bytes::copy_from_slice(&self.current_value)))
    }

    /// Binary search for a value.
    pub fn seek_at_or_after_value(&mut self, target: &[u8]) -> Result<bool> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryPrefixPageDecoder: not initialized",
            ));
        }

        if self.num_entries == 0 {
            return Err(paro_common::error::not_supported("page is empty"));
        }

        // Binary search on restart points
        let mut left = 0usize;
        let mut right = self.num_restarts as usize;

        while left < right {
            let mid = left + (right - left) / 2;
            self.seek_to_restart_point(mid)?;

            if self.current_value.as_slice() < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Go back one restart point if needed
        if left > 0 {
            self.seek_to_restart_point(left - 1)?;
        } else {
            self.seek_to_restart_point(0)?;
        }

        // Linear search within the block
        while self.cur_pos < self.num_entries {
            if self.current_value.as_slice() >= target {
                return Ok(self.current_value.as_slice() == target);
            }
            self.cur_pos += 1;
            if self.cur_pos < self.num_entries {
                self.read_next_value()?;
            }
        }

        Err(paro_common::error::not_supported("value not found"))
    }

    /// Get entry count.
    pub fn count(&self) -> u32 {
        self.num_entries
    }

    /// Get current position.
    pub fn current_index(&self) -> u32 {
        self.cur_pos
    }
}

/// Calculate common prefix length between two byte slices.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

/// Write a varint to buffer.
fn put_varint(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// Read a varint from buffer.
fn get_varint(data: &[u8]) -> Result<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in data {
        bytes_read += 1;
        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }

        shift += 7;
        if shift >= 32 {
            return Err(paro_common::error::data_corrupted("varint too long"));
        }
    }

    Err(paro_common::error::data_corrupted("incomplete varint"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_prefix_basic() {
        let mut builder = BinaryPrefixPageBuilder::new(256 * 1024);

        // Add sorted strings
        let values = vec!["apple", "application", "apply", "banana", "bandana"];

        for v in &values {
            assert!(builder.add_one(v.as_bytes()));
        }

        assert_eq!(builder.count(), 5);

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPrefixPageDecoder::new(page_data);
        decoder.init().unwrap();
        assert_eq!(decoder.count(), 5);

        let (count, results) = decoder.next_batch(5).unwrap();
        assert_eq!(count, 5);

        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.as_ref(), values[i].as_bytes());
        }
    }

    #[test]
    fn test_binary_prefix_seek() {
        let mut builder = BinaryPrefixPageBuilder::new(256 * 1024);

        let values: Vec<String> = (0..100).map(|i| format!("key_{:04}", i)).collect();

        for v in &values {
            builder.add_one(v.as_bytes());
        }

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPrefixPageDecoder::new(page_data);
        decoder.init().unwrap();

        // Seek to position 50
        decoder.seek_to_position(50).unwrap();
        assert_eq!(decoder.current_index(), 50);

        let (count, results) = decoder.next_batch(1).unwrap();
        assert_eq!(count, 1);
        assert_eq!(results[0].as_ref(), b"key_0050");
    }

    #[test]
    fn test_binary_prefix_compression() {
        let mut builder = BinaryPrefixPageBuilder::new(256 * 1024);

        // Strings with common prefix should compress well
        let values: Vec<String> = (0..100)
            .map(|i| format!("very_long_common_prefix_{:04}", i))
            .collect();

        for v in &values {
            builder.add_one(v.as_bytes());
        }

        let page_data = builder.finish().unwrap();

        // Should be smaller than raw data
        let raw_size: usize = values.iter().map(|v| v.len()).sum();
        assert!(page_data.len() < raw_size);

        // Verify data
        let mut decoder = BinaryPrefixPageDecoder::new(page_data);
        decoder.init().unwrap();

        let (count, results) = decoder.next_batch(100).unwrap();
        assert_eq!(count, 100);

        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.as_ref(), values[i].as_bytes());
        }
    }

    #[test]
    fn test_binary_prefix_first_last() {
        let mut builder = BinaryPrefixPageBuilder::new(256 * 1024);

        builder.add_one(b"first");
        builder.add_one(b"middle");
        builder.add_one(b"last");

        let first = builder.get_first_value().unwrap();
        let last = builder.get_last_value().unwrap();

        assert_eq!(first.as_ref(), b"first");
        assert_eq!(last.as_ref(), b"last");
    }

    #[test]
    fn test_binary_prefix_binary_search() {
        let mut builder = BinaryPrefixPageBuilder::new(256 * 1024);

        let values: Vec<String> = (0..100).map(|i| format!("key_{:04}", i)).collect();

        for v in &values {
            builder.add_one(v.as_bytes());
        }

        let page_data = builder.finish().unwrap();

        let mut decoder = BinaryPrefixPageDecoder::new(page_data);
        decoder.init().unwrap();

        // Search for exact match
        let exact = decoder.seek_at_or_after_value(b"key_0050").unwrap();
        assert!(exact);
        assert_eq!(decoder.current_index(), 50);

        // Search for value between entries
        decoder.seek_to_position(0).unwrap();
        let exact = decoder.seek_at_or_after_value(b"key_0050a").unwrap();
        assert!(!exact);
        assert_eq!(decoder.current_index(), 51);
    }

    #[test]
    fn test_varint() {
        let mut buf = BytesMut::new();

        put_varint(&mut buf, 0);
        put_varint(&mut buf, 127);
        put_varint(&mut buf, 128);
        put_varint(&mut buf, 16383);
        put_varint(&mut buf, 16384);

        let data = buf.freeze();
        let mut offset = 0;

        let (v, n) = get_varint(&data[offset..]).unwrap();
        assert_eq!(v, 0);
        offset += n;

        let (v, n) = get_varint(&data[offset..]).unwrap();
        assert_eq!(v, 127);
        offset += n;

        let (v, n) = get_varint(&data[offset..]).unwrap();
        assert_eq!(v, 128);
        offset += n;

        let (v, n) = get_varint(&data[offset..]).unwrap();
        assert_eq!(v, 16383);
        offset += n;

        let (v, _) = get_varint(&data[offset..]).unwrap();
        assert_eq!(v, 16384);
    }
}
