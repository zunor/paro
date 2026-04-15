//! # Frame-of-Reference Page Encoding
//!
//! Delta encoding with min value reference for numeric types.
//! Effective for sorted or clustered numeric data.
//!
//! ## Page Layout
//!
//! ```text
//! +------------------------+
//! | num_elements (4)       |  <- Header
//! | frame_count (4)        |
//! | frame_size (1)         |
//! | last_frame_size (1)    |
//! | type_size (1)          |
//! | reserved (1)           |
//! +------------------------+
//! | Frame 0                |
//! |   min_value            |
//! |   bit_width (1)        |
//! |   storage_format (1)   |
//! |   packed deltas        |
//! +------------------------+
//! | Frame 1                |
//! | ...                    |
//! +------------------------+
//! ```
//!
//! ## Storage Formats
//!
//! - Format 0: Delta from min value (value - min)
//! - Format 1: Delta from previous (for ascending data)
//! - Format 2: Raw values (fallback when overflow)

use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;

/// Header size for FOR pages.
pub const FOR_PAGE_HEADER_SIZE: usize = 12;

/// Frame size (number of values per frame).
const FRAME_VALUE_NUM: usize = 128;

/// Storage format: delta from min value.
#[allow(dead_code)]
const FORMAT_DELTA_MIN: u8 = 0;
/// Storage format: delta from previous (ascending).
#[allow(dead_code)]
const FORMAT_DELTA_PREV: u8 = 1;
/// Storage format: raw values (fallback).
#[allow(dead_code)]
const FORMAT_RAW: u8 = 2;

/// Calculate bits needed to represent a value.
fn bits_needed(v: u64) -> u8 {
    if v == 0 {
        0
    } else {
        64 - v.leading_zeros() as u8
    }
}

/// Calculate bits needed for i128.
fn bits_needed_i128(v: u128) -> u8 {
    if v == 0 {
        0
    } else {
        128 - v.leading_zeros() as u8
    }
}

/// Builder for Frame-of-Reference encoded pages.
pub struct FrameOfReferencePageBuilder {
    /// Raw data buffer
    data: BytesMut,
    /// Element size in bytes (1, 2, 4, 8, or 16)
    type_size: usize,
    /// Target page size
    page_size: usize,
    /// Current element count
    count: u32,
    /// First value
    first_value: Option<Bytes>,
    /// Last value
    last_value: Option<Bytes>,
    /// Whether finish() has been called
    finished: bool,
}

impl FrameOfReferencePageBuilder {
    /// Create a new FOR page builder.
    pub fn new(type_size: usize, page_size: usize) -> Self {
        assert!(matches!(type_size, 1 | 2 | 4 | 8 | 16));
        FrameOfReferencePageBuilder {
            data: BytesMut::with_capacity(page_size),
            type_size,
            page_size,
            count: 0,
            first_value: None,
            last_value: None,
            finished: false,
        }
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        self.data.len() >= self.page_size
    }

    /// Add values to the page.
    pub fn add(&mut self, vals: &[u8], count: u32) -> u32 {
        if self.finished || self.is_page_full() {
            return 0;
        }

        let to_add = count as usize;
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

    /// Finish building the page.
    pub fn finish(&mut self) -> Result<Bytes> {
        assert!(!self.finished);
        self.finished = true;

        if self.count == 0 {
            let mut output = BytesMut::with_capacity(FOR_PAGE_HEADER_SIZE);
            output.put_u32_le(0); // num_elements
            output.put_u32_le(0); // frame_count
            output.put_u8(FRAME_VALUE_NUM as u8);
            output.put_u8(0);
            output.put_u8(self.type_size as u8);
            output.put_u8(0);
            return Ok(output.freeze());
        }

        let frame_count = (self.count as usize).div_ceil(FRAME_VALUE_NUM);
        let last_frame_size = if (self.count as usize).is_multiple_of(FRAME_VALUE_NUM) {
            FRAME_VALUE_NUM
        } else {
            self.count as usize % FRAME_VALUE_NUM
        };

        let mut output = BytesMut::with_capacity(self.page_size);

        // Write header
        output.put_u32_le(self.count);
        output.put_u32_le(frame_count as u32);
        output.put_u8(FRAME_VALUE_NUM as u8);
        output.put_u8(last_frame_size as u8);
        output.put_u8(self.type_size as u8);
        output.put_u8(0); // reserved

        // Encode each frame
        for frame_idx in 0..frame_count {
            let frame_start = frame_idx * FRAME_VALUE_NUM;
            let frame_size = if frame_idx == frame_count - 1 {
                last_frame_size
            } else {
                FRAME_VALUE_NUM
            };

            self.encode_frame(&mut output, frame_start, frame_size);
        }

        Ok(output.freeze())
    }

    fn encode_frame(&self, output: &mut BytesMut, start: usize, count: usize) {
        match self.type_size {
            1 => self.encode_frame_typed::<i8>(output, start, count),
            2 => self.encode_frame_typed::<i16>(output, start, count),
            4 => self.encode_frame_typed::<i32>(output, start, count),
            8 => self.encode_frame_typed::<i64>(output, start, count),
            16 => self.encode_frame_typed_i128(output, start, count),
            _ => unreachable!(),
        }
    }

    fn encode_frame_typed<T>(&self, output: &mut BytesMut, start: usize, count: usize)
    where
        T: Copy + Ord + std::ops::Sub<Output = T> + Into<i64> + TryFrom<i64>,
        i64: TryFrom<T>,
    {
        let type_size = std::mem::size_of::<T>();
        let mut values = Vec::with_capacity(count);

        for i in 0..count {
            let offset = (start + i) * type_size;
            let bytes = &self.data[offset..offset + type_size];
            let value = read_value::<T>(bytes);
            values.push(value);
        }

        let min_val = *values.iter().min().unwrap();
        let max_val = *values.iter().max().unwrap();

        // Try delta from min
        let max_delta: i64 =
            i64::try_from(max_val).unwrap_or(i64::MAX) - i64::try_from(min_val).unwrap_or(i64::MIN);

        let bit_width = if max_delta >= 0 {
            bits_needed(max_delta as u64)
        } else {
            (type_size * 8) as u8
        };

        // Write min value
        write_value(output, min_val);

        // Write metadata
        output.put_u8(bit_width);
        output.put_u8(FORMAT_DELTA_MIN);

        // Pack deltas
        if bit_width == 0 {
            // All values are the same, no data needed
            return;
        }

        let mut bit_buffer = BitPackWriter::new(output, bit_width);
        for &val in &values {
            let delta = i64::try_from(val).unwrap_or(0) - i64::try_from(min_val).unwrap_or(0);
            bit_buffer.write(delta as u64);
        }
        bit_buffer.flush();
    }

    fn encode_frame_typed_i128(&self, output: &mut BytesMut, start: usize, count: usize) {
        let mut values = Vec::with_capacity(count);

        for i in 0..count {
            let offset = (start + i) * 16;
            let bytes = &self.data[offset..offset + 16];
            let value = i128::from_le_bytes(bytes.try_into().unwrap());
            values.push(value);
        }

        let min_val = *values.iter().min().unwrap();
        let max_val = *values.iter().max().unwrap();

        let max_delta = max_val.wrapping_sub(min_val) as u128;
        let bit_width = bits_needed_i128(max_delta);

        // Write min value
        output.extend_from_slice(&min_val.to_le_bytes());

        // Write metadata
        output.put_u8(bit_width);
        output.put_u8(FORMAT_DELTA_MIN);

        if bit_width == 0 {
            return;
        }

        let mut bit_buffer = BitPackWriter::new(output, bit_width);
        for &val in &values {
            let delta = val.wrapping_sub(min_val) as u128;
            bit_buffer.write_u128(delta);
        }
        bit_buffer.flush();
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
}

/// Decoder for Frame-of-Reference encoded pages.
pub struct FrameOfReferencePageDecoder {
    /// Page data
    data: Bytes,
    /// Number of elements
    num_elements: u32,
    /// Number of frames
    frame_count: u32,
    /// Values per frame
    frame_size: usize,
    /// Last frame size
    last_frame_size: usize,
    /// Element size
    type_size: usize,
    /// Frame offsets in data
    frame_offsets: Vec<usize>,
    /// Current index
    cur_index: u32,
    /// Decoded frame cache
    decoded_frame: Option<(u32, Vec<u8>)>,
    /// Whether init() has been called
    parsed: bool,
}

impl FrameOfReferencePageDecoder {
    /// Create a new decoder.
    pub fn new(data: Bytes) -> Self {
        FrameOfReferencePageDecoder {
            data,
            num_elements: 0,
            frame_count: 0,
            frame_size: FRAME_VALUE_NUM,
            last_frame_size: 0,
            type_size: 0,
            frame_offsets: Vec::new(),
            cur_index: 0,
            decoded_frame: None,
            parsed: false,
        }
    }

    /// Initialize the decoder.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.len() < FOR_PAGE_HEADER_SIZE {
            return Err(paro_common::error::data_corrupted(
                "FrameOfReferencePageDecoder: data too small",
            ));
        }

        // Parse header
        self.num_elements =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]);
        self.frame_count =
            u32::from_le_bytes([self.data[4], self.data[5], self.data[6], self.data[7]]);
        self.frame_size = self.data[8] as usize;
        self.last_frame_size = self.data[9] as usize;
        self.type_size = self.data[10] as usize;

        if self.num_elements == 0 {
            self.parsed = true;
            return Ok(());
        }

        // Calculate frame offsets
        self.frame_offsets = Vec::with_capacity(self.frame_count as usize);
        let mut offset = FOR_PAGE_HEADER_SIZE;

        for frame_idx in 0..self.frame_count as usize {
            self.frame_offsets.push(offset);

            let frame_size = if frame_idx == self.frame_count as usize - 1 {
                self.last_frame_size
            } else {
                self.frame_size
            };

            // Skip min_value + bit_width + format
            offset += self.type_size + 2;

            // Read bit_width to calculate packed data size
            let bit_width = self.data[offset - 2] as usize;
            if bit_width > 0 {
                let packed_bits = frame_size * bit_width;
                let packed_bytes = packed_bits.div_ceil(8);
                offset += packed_bytes;
            }
        }

        self.parsed = true;
        self.cur_index = 0;
        Ok(())
    }

    /// Seek to a position.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "FrameOfReferencePageDecoder: not initialized",
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
                "FrameOfReferencePageDecoder: not initialized",
            ));
        }

        let remaining = (self.num_elements - self.cur_index) as usize;
        let to_read = std::cmp::min(n, remaining);

        if to_read == 0 {
            return Ok((0, Bytes::new()));
        }

        let mut output = BytesMut::with_capacity(to_read * self.type_size);

        let mut read_count = 0;
        while read_count < to_read {
            let frame_idx = (self.cur_index as usize) / self.frame_size;
            let pos_in_frame = (self.cur_index as usize) % self.frame_size;

            // Decode frame if needed
            if self.decoded_frame.is_none()
                || self.decoded_frame.as_ref().unwrap().0 != frame_idx as u32
            {
                let frame_data = self.decode_frame(frame_idx)?;
                self.decoded_frame = Some((frame_idx as u32, frame_data));
            }

            let frame_data = &self.decoded_frame.as_ref().unwrap().1;
            let frame_size = if frame_idx == self.frame_count as usize - 1 {
                self.last_frame_size
            } else {
                self.frame_size
            };

            let available = frame_size - pos_in_frame;
            let to_copy = std::cmp::min(available, to_read - read_count);

            let start = pos_in_frame * self.type_size;
            let end = start + to_copy * self.type_size;
            output.extend_from_slice(&frame_data[start..end]);

            read_count += to_copy;
            self.cur_index += to_copy as u32;
        }

        Ok((to_read, output.freeze()))
    }

    fn decode_frame(&self, frame_idx: usize) -> Result<Vec<u8>> {
        match self.type_size {
            1 => self.decode_frame_typed::<i8>(frame_idx),
            2 => self.decode_frame_typed::<i16>(frame_idx),
            4 => self.decode_frame_typed::<i32>(frame_idx),
            8 => self.decode_frame_typed::<i64>(frame_idx),
            16 => self.decode_frame_typed_i128(frame_idx),
            _ => Err(paro_common::error::internal("invalid type size")),
        }
    }

    fn decode_frame_typed<T>(&self, frame_idx: usize) -> Result<Vec<u8>>
    where
        T: Copy + std::ops::Add<Output = T> + TryFrom<i64>,
        i64: TryFrom<T>,
    {
        let type_size = std::mem::size_of::<T>();
        let offset = self.frame_offsets[frame_idx];
        let frame_size = if frame_idx == self.frame_count as usize - 1 {
            self.last_frame_size
        } else {
            self.frame_size
        };

        // Read min value
        let min_bytes = &self.data[offset..offset + type_size];
        let min_val = read_value::<T>(min_bytes);

        // Read metadata
        let bit_width = self.data[offset + type_size] as usize;
        let _format = self.data[offset + type_size + 1];

        let mut output = Vec::with_capacity(frame_size * type_size);

        if bit_width == 0 {
            // All values are the same
            for _ in 0..frame_size {
                write_value_vec(&mut output, min_val);
            }
        } else {
            // Unpack deltas
            let packed_start = offset + type_size + 2;
            let reader = BitPackReader::new(&self.data[packed_start..], bit_width);

            for i in 0..frame_size {
                let delta = reader.read(i) as i64;
                let min_i64 = i64::try_from(min_val).unwrap_or(0);
                let val = T::try_from(min_i64 + delta).unwrap_or(min_val);
                write_value_vec(&mut output, val);
            }
        }

        Ok(output)
    }

    fn decode_frame_typed_i128(&self, frame_idx: usize) -> Result<Vec<u8>> {
        let offset = self.frame_offsets[frame_idx];
        let frame_size = if frame_idx == self.frame_count as usize - 1 {
            self.last_frame_size
        } else {
            self.frame_size
        };

        // Read min value
        let min_bytes = &self.data[offset..offset + 16];
        let min_val = i128::from_le_bytes(min_bytes.try_into().unwrap());

        // Read metadata
        let bit_width = self.data[offset + 16] as usize;

        let mut output = Vec::with_capacity(frame_size * 16);

        if bit_width == 0 {
            for _ in 0..frame_size {
                output.extend_from_slice(&min_val.to_le_bytes());
            }
        } else {
            let packed_start = offset + 16 + 2;
            let reader = BitPackReader::new(&self.data[packed_start..], bit_width);

            for i in 0..frame_size {
                let delta = reader.read_u128(i);
                let val = min_val.wrapping_add(delta as i128);
                output.extend_from_slice(&val.to_le_bytes());
            }
        }

        Ok(output)
    }

    /// Get value at index.
    pub fn value_at(&mut self, idx: u32) -> Result<Option<Bytes>> {
        if !self.parsed || idx >= self.num_elements {
            return Ok(None);
        }

        let old_idx = self.cur_index;
        self.cur_index = idx;
        let (count, data) = self.next_batch(1)?;
        self.cur_index = old_idx;

        if count == 1 {
            Ok(Some(data))
        } else {
            Ok(None)
        }
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

// Helper functions for reading/writing typed values

fn read_value<T: Copy>(bytes: &[u8]) -> T {
    let size = std::mem::size_of::<T>();
    assert!(bytes.len() >= size);
    unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
}

fn write_value<T: Copy>(output: &mut BytesMut, val: T) {
    let size = std::mem::size_of::<T>();
    let bytes = unsafe { std::slice::from_raw_parts(&val as *const T as *const u8, size) };
    output.extend_from_slice(bytes);
}

fn write_value_vec<T: Copy>(output: &mut Vec<u8>, val: T) {
    let size = std::mem::size_of::<T>();
    let bytes = unsafe { std::slice::from_raw_parts(&val as *const T as *const u8, size) };
    output.extend_from_slice(bytes);
}

/// Bit packing writer for variable-width integers.
struct BitPackWriter<'a> {
    output: &'a mut BytesMut,
    bit_width: u8,
    buffer: u64,
    bits_in_buffer: u8,
}

impl<'a> BitPackWriter<'a> {
    fn new(output: &'a mut BytesMut, bit_width: u8) -> Self {
        BitPackWriter {
            output,
            bit_width,
            buffer: 0,
            bits_in_buffer: 0,
        }
    }

    fn write(&mut self, value: u64) {
        let mask = if self.bit_width >= 64 {
            u64::MAX
        } else {
            (1u64 << self.bit_width) - 1
        };
        let value = value & mask;

        self.buffer |= value << self.bits_in_buffer;
        self.bits_in_buffer += self.bit_width;

        while self.bits_in_buffer >= 8 {
            self.output.put_u8(self.buffer as u8);
            self.buffer >>= 8;
            self.bits_in_buffer -= 8;
        }
    }

    fn write_u128(&mut self, value: u128) {
        // For values > 64 bits, write in two parts
        if self.bit_width <= 64 {
            self.write(value as u64);
        } else {
            self.write(value as u64);
            self.write((value >> 64) as u64);
        }
    }

    fn flush(&mut self) {
        if self.bits_in_buffer > 0 {
            self.output.put_u8(self.buffer as u8);
        }
    }
}

/// Bit packing reader for variable-width integers.
struct BitPackReader<'a> {
    data: &'a [u8],
    bit_width: usize,
}

impl<'a> BitPackReader<'a> {
    fn new(data: &'a [u8], bit_width: usize) -> Self {
        BitPackReader { data, bit_width }
    }

    fn read(&self, index: usize) -> u64 {
        if self.bit_width == 0 {
            return 0;
        }

        let bit_offset = index * self.bit_width;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        let mut result: u64 = 0;
        let mut bits_read = 0;

        while bits_read < self.bit_width {
            let current_byte = byte_offset + (bit_in_byte + bits_read) / 8;
            if current_byte >= self.data.len() {
                break;
            }

            let bit_pos = (bit_in_byte + bits_read) % 8;
            let bits_available = 8 - bit_pos;
            let bits_to_read = std::cmp::min(bits_available, self.bit_width - bits_read);

            let mask = ((1u64 << bits_to_read) - 1) as u8;
            let value = (self.data[current_byte] >> bit_pos) & mask;
            result |= (value as u64) << bits_read;

            bits_read += bits_to_read;
        }

        result
    }

    fn read_u128(&self, index: usize) -> u128 {
        if self.bit_width <= 64 {
            self.read(index) as u128
        } else {
            let low = self.read(index * 2) as u128;
            let high = self.read(index * 2 + 1) as u128;
            low | (high << 64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_page_i32() {
        let mut builder = FrameOfReferencePageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = (100..200).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let added = builder.add(&bytes, 100);
        assert_eq!(added, 100);
        assert_eq!(builder.count(), 100);

        let page_data = builder.finish().unwrap();

        let mut decoder = FrameOfReferencePageDecoder::new(page_data);
        decoder.init().unwrap();
        assert_eq!(decoder.count(), 100);

        let (count, data) = decoder.next_batch(100).unwrap();
        assert_eq!(count, 100);

        for i in 0..100 {
            let offset = i * 4;
            let value = i32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            assert_eq!(value, 100 + i as i32);
        }
    }

    #[test]
    fn test_for_page_i64() {
        let mut builder = FrameOfReferencePageBuilder::new(8, 256 * 1024);

        let values: Vec<i64> = (0..50).map(|i| 1000000 + i).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        builder.add(&bytes, 50);
        let page_data = builder.finish().unwrap();

        let mut decoder = FrameOfReferencePageDecoder::new(page_data);
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
            assert_eq!(value, 1000000 + i as i64);
        }
    }

    #[test]
    fn test_for_page_constant() {
        let mut builder = FrameOfReferencePageBuilder::new(4, 256 * 1024);

        // All same values - should use 0 bits
        let values: Vec<i32> = vec![42; 100];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        builder.add(&bytes, 100);
        let page_data = builder.finish().unwrap();

        // Should be very small
        assert!(page_data.len() < 50);

        let mut decoder = FrameOfReferencePageDecoder::new(page_data);
        decoder.init().unwrap();

        let (count, data) = decoder.next_batch(100).unwrap();
        assert_eq!(count, 100);

        for i in 0..100 {
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
    fn test_for_page_seek() {
        let mut builder = FrameOfReferencePageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 100);

        let page_data = builder.finish().unwrap();

        let mut decoder = FrameOfReferencePageDecoder::new(page_data);
        decoder.init().unwrap();

        decoder.seek_to_position(50).unwrap();
        assert_eq!(decoder.current_index(), 50);

        let (count, data) = decoder.next_batch(10).unwrap();
        assert_eq!(count, 10);

        let first = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(first, 50);
    }

    #[test]
    fn test_for_page_multiple_frames() {
        let mut builder = FrameOfReferencePageBuilder::new(4, 256 * 1024);

        // More than one frame (128 values per frame)
        let values: Vec<i32> = (0..300).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        builder.add(&bytes, 300);
        let page_data = builder.finish().unwrap();

        let mut decoder = FrameOfReferencePageDecoder::new(page_data);
        decoder.init().unwrap();
        assert_eq!(decoder.count(), 300);

        let (count, data) = decoder.next_batch(300).unwrap();
        assert_eq!(count, 300);

        for i in 0..300 {
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
    fn test_for_page_first_last_value() {
        let mut builder = FrameOfReferencePageBuilder::new(4, 256 * 1024);

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
    fn test_bits_needed() {
        assert_eq!(bits_needed(0), 0);
        assert_eq!(bits_needed(1), 1);
        assert_eq!(bits_needed(2), 2);
        assert_eq!(bits_needed(3), 2);
        assert_eq!(bits_needed(255), 8);
        assert_eq!(bits_needed(256), 9);
    }
}
