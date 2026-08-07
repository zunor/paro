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
//! | num_elements (4)       |  <- Header (24 bytes total)
//! | compressed_size (4)    |
//! | padded_num_elements (4)|
//! | elem_size_bytes (4)    |
//! | format_magic (4)       |  <- "BSH2"
//! | block_elements (4)     |
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

use crate::compression::decompress_size_prepended_exact;
use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;

/// Header size for bitshuffle pages.
pub const BITSHUFFLE_PAGE_HEADER_SIZE: usize = 24;
const BITSHUFFLE_PAGE_MAGIC: [u8; 4] = *b"BSH2";
const BITSHUFFLE_BLOCK_ELEMENTS: usize = 1024;

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
        output.extend_from_slice(&BITSHUFFLE_PAGE_MAGIC);
        output.put_u32_le(BITSHUFFLE_BLOCK_ELEMENTS as u32);

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
    /// Element count supplied by the ordinal index.
    expected_num_elements: u32,
    /// Element width supplied by the column schema.
    expected_type_size: usize,
    /// Decompressed bit-sliced data. Kept so point lookups and predicate
    /// kernels do not need to materialize the full logical page.
    shuffled_data: Option<Bytes>,
    /// Lazily materialized logical values for sequential reads.
    decoded_data: Option<Bytes>,
    /// Number of elements
    num_elements: u32,
    /// Compressed size
    compressed_size: u32,
    /// Padded element count
    padded_num_elements: u32,
    /// Element size
    type_size: usize,
    /// Number of elements transposed together
    block_elements: usize,
    /// Current index
    cur_index: u32,
    /// Whether init() has been called
    parsed: bool,
}

impl BitShufflePageDecoder {
    /// Create a new decoder.
    pub fn new(data: Bytes, expected_num_elements: u32, expected_type_size: usize) -> Self {
        BitShufflePageDecoder {
            data,
            expected_num_elements,
            expected_type_size,
            shuffled_data: None,
            decoded_data: None,
            num_elements: 0,
            compressed_size: 0,
            padded_num_elements: 0,
            type_size: 0,
            block_elements: 0,
            cur_index: 0,
            parsed: false,
        }
    }

    /// Construct a decoder backed by a version-isolated logical page cache.
    /// The encoded header is still parsed and cross-checked against schema and
    /// ordinal metadata before cached bytes are accepted.
    pub fn with_decoded_data(
        data: Bytes,
        expected_num_elements: u32,
        expected_type_size: usize,
        decoded_data: Bytes,
    ) -> Self {
        let mut decoder = Self::new(data, expected_num_elements, expected_type_size);
        decoder.decoded_data = Some(decoded_data);
        decoder
    }

    /// Construct directly from a version-isolated decoded-page cache entry.
    /// Cache publication validates the encoded page first; lookup still
    /// validates the logical byte length against current ordinal/schema data.
    pub fn from_decoded_data(
        expected_num_elements: u32,
        expected_type_size: usize,
        decoded_data: Bytes,
    ) -> Result<Self> {
        if !matches!(expected_type_size, 1 | 2 | 4 | 8 | 16) {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: invalid cached type size {expected_type_size}"
            )));
        }
        let padded_num_elements = expected_num_elements
            .checked_add(7)
            .map(|count| count & !7)
            .ok_or_else(|| {
                paro_common::error::data_corrupted(
                    "BitShufflePageDecoder: cached padded element count overflow",
                )
            })?;
        let expected_size = (padded_num_elements as usize)
            .checked_mul(expected_type_size)
            .ok_or_else(|| {
                paro_common::error::data_corrupted(
                    "BitShufflePageDecoder: cached decoded page size overflow",
                )
            })?;
        if decoded_data.len() != expected_size {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: cached decoded size {} does not match expected size {expected_size}",
                decoded_data.len(),
            )));
        }
        Ok(Self {
            data: Bytes::new(),
            expected_num_elements,
            expected_type_size,
            shuffled_data: None,
            decoded_data: Some(decoded_data),
            num_elements: expected_num_elements,
            compressed_size: 0,
            type_size: expected_type_size,
            padded_num_elements,
            block_elements: BITSHUFFLE_BLOCK_ELEMENTS,
            cur_index: 0,
            parsed: true,
        })
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
        let format_magic = [self.data[16], self.data[17], self.data[18], self.data[19]];
        self.block_elements =
            u32::from_le_bytes([self.data[20], self.data[21], self.data[22], self.data[23]])
                as usize;

        // Validate
        if format_magic != BITSHUFFLE_PAGE_MAGIC {
            return Err(paro_common::error::data_corrupted(
                "BitShufflePageDecoder: unsupported page format",
            ));
        }
        if self.block_elements != BITSHUFFLE_BLOCK_ELEMENTS {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: unsupported block element count {}",
                self.block_elements,
            )));
        }
        if self.compressed_size as usize != self.data.len() {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: encoded size {} does not match page size {}",
                self.compressed_size,
                self.data.len()
            )));
        }
        if self.num_elements != self.expected_num_elements {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: element count {} does not match ordinal index {}",
                self.num_elements, self.expected_num_elements,
            )));
        }
        let expected_padded_num_elements = self
            .expected_num_elements
            .checked_add(7)
            .map(|count| count & !7)
            .ok_or_else(|| {
                paro_common::error::data_corrupted(
                    "BitShufflePageDecoder: padded element count overflow",
                )
            })?;
        if self.padded_num_elements != expected_padded_num_elements {
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
        if self.type_size != self.expected_type_size {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: type size {} does not match column schema {}",
                self.type_size, self.expected_type_size,
            )));
        }

        let expected_size = (expected_padded_num_elements as usize)
            .checked_mul(self.expected_type_size)
            .ok_or_else(|| {
                paro_common::error::data_corrupted(
                    "BitShufflePageDecoder: decoded page size overflow",
                )
            })?;
        if let Some(decoded) = &self.decoded_data {
            if decoded.len() != expected_size {
                return Err(paro_common::error::data_corrupted(format!(
                    "BitShufflePageDecoder: cached decoded size {} does not match expected size {expected_size}",
                    decoded.len(),
                )));
            }
            self.parsed = true;
            self.cur_index = 0;
            return Ok(());
        }
        // Decompress only after the page header and the embedded LZ4 size
        // prefix agree on the exact output allocation.
        let compressed_data = &self.data[BITSHUFFLE_PAGE_HEADER_SIZE..];
        let decompressed = decompress_size_prepended_exact(compressed_data, expected_size)?;

        self.shuffled_data = Some(Bytes::from(decompressed));

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

        self.ensure_materialized()?;
        let decoded = self
            .decoded_data
            .as_ref()
            .expect("initialized BitShuffle decoder has logical data");
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
        if let Some(decoded) = &self.decoded_data {
            let start = idx as usize * self.type_size;
            let end = start + self.type_size;
            return Some(decoded.slice(start..end));
        }

        let mut value = vec![0_u8; self.type_size];
        self.copy_value_at(idx, &mut value).ok()?;
        Some(Bytes::from(value))
    }

    /// Copy one logical value directly from the bit-sliced page.
    ///
    /// This is O(value width) and deliberately leaves the sequential cursor
    /// untouched. It is the encoded-domain primitive used by sparse gathers.
    pub fn copy_value_at(&self, idx: u32, output: &mut [u8]) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BitShufflePageDecoder: not initialized",
            ));
        }
        if idx >= self.num_elements {
            return Err(paro_common::error::out_of_range(format!(
                "BitShufflePageDecoder: value index {idx} exceeds element count {}",
                self.num_elements,
            )));
        }
        if output.len() != self.type_size {
            return Err(paro_common::error::invalid_input(format!(
                "BitShufflePageDecoder: output width {} does not match element width {}",
                output.len(),
                self.type_size,
            )));
        }
        if let Some(decoded) = &self.decoded_data {
            let start = idx as usize * self.type_size;
            output.copy_from_slice(&decoded[start..start + self.type_size]);
            return Ok(());
        }

        let shuffled = self
            .shuffled_data
            .as_ref()
            .expect("initialized BitShuffle decoder has bit-sliced data");
        copy_bit_sliced_value(
            shuffled,
            idx as usize,
            self.padded_num_elements as usize,
            self.type_size,
            self.block_elements,
            output,
        )
    }

    /// Gather logical values into arbitrary output slots.
    ///
    /// Adjacent source indices share an eight-row bit-slice group. Decoding
    /// that group once amortizes the transpose across every selected row in
    /// it, while preserving the sparse-gather property: unrelated rows and
    /// groups are never materialized.
    pub fn gather_values_at<I>(&self, values: I, output: &mut [u8]) -> Result<()>
    where
        I: IntoIterator<Item = (u32, usize)>,
    {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BitShufflePageDecoder: not initialized",
            ));
        }
        if output.len() % self.type_size != 0 {
            return Err(paro_common::error::invalid_input(
                "BitShuffle gather output is not aligned to the element width",
            ));
        }

        match self.type_size {
            1 => self.gather_fixed_values::<u8, _>(values, output),
            2 => self.gather_fixed_values::<u16, _>(values, output),
            4 => self.gather_fixed_values::<u32, _>(values, output),
            8 => self.gather_fixed_values::<u64, _>(values, output),
            16 => self.gather_fixed_values::<u128, _>(values, output),
            _ => Err(paro_common::error::internal(format!(
                "initialized BitShuffle decoder has invalid type size {}",
                self.type_size
            ))),
        }
    }

    fn gather_fixed_values<T, I>(&self, values: I, output: &mut [u8]) -> Result<()>
    where
        T: Copy,
        I: IntoIterator<Item = (u32, usize)>,
    {
        debug_assert_eq!(std::mem::size_of::<T>(), self.type_size);

        let output_rows = output.len() / self.type_size;
        if let Some(decoded) = &self.decoded_data {
            for (source_idx, output_idx) in values {
                self.validate_gather_indices(source_idx, output_idx, output_rows)?;
                let source_start = source_idx as usize * self.type_size;
                let output_start = output_idx * self.type_size;
                // SAFETY: the index validation above proves both fixed-width
                // ranges are in bounds. T is selected from the decoder's
                // validated physical width and unaligned access is required
                // because the byte buffers do not promise T's alignment.
                unsafe {
                    copy_fixed_unaligned::<T>(
                        decoded.as_ptr().add(source_start),
                        output.as_mut_ptr().add(output_start),
                    );
                }
            }
            return Ok(());
        }

        let shuffled = self
            .shuffled_data
            .as_ref()
            .expect("initialized BitShuffle decoder has bit-sliced data");
        let mut decoded_group = [0_u8; 16 * 8];
        let mut decoded_group_start = None;
        for (source_idx, output_idx) in values {
            self.validate_gather_indices(source_idx, output_idx, output_rows)?;
            let source_idx = source_idx as usize;
            let block_start = source_idx / self.block_elements * self.block_elements;
            let row_in_block = source_idx - block_start;
            let group_start = block_start + row_in_block / 8 * 8;
            if decoded_group_start != Some(group_start) {
                decode_bit_sliced_group(
                    shuffled,
                    group_start,
                    self.padded_num_elements as usize,
                    self.type_size,
                    self.block_elements,
                    &mut decoded_group,
                )?;
                decoded_group_start = Some(group_start);
            }

            let source_start = (source_idx - group_start) * self.type_size;
            let output_start = output_idx * self.type_size;
            // SAFETY: decoded_group contains eight values at the validated
            // physical width, and validate_gather_indices proves the output
            // slot is in bounds. Both buffers may be unaligned for T.
            unsafe {
                copy_fixed_unaligned::<T>(
                    decoded_group.as_ptr().add(source_start),
                    output.as_mut_ptr().add(output_start),
                );
            }
        }
        Ok(())
    }

    fn validate_gather_indices(
        &self,
        source_idx: u32,
        output_idx: usize,
        output_rows: usize,
    ) -> Result<()> {
        if source_idx >= self.num_elements {
            return Err(paro_common::error::out_of_range(format!(
                "BitShufflePageDecoder: value index {source_idx} exceeds element count {}",
                self.num_elements,
            )));
        }
        if output_idx >= output_rows {
            return Err(paro_common::error::out_of_range(format!(
                "BitShufflePageDecoder: gather output index {output_idx} exceeds row count {output_rows}",
            )));
        }
        Ok(())
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

    #[inline]
    pub fn is_materialized(&self) -> bool {
        self.decoded_data.is_some()
    }

    /// Return the logical page only when an operation has already required
    /// materialization. Sparse point reads deliberately keep returning `None`.
    #[cfg(test)]
    fn materialized_data(&self) -> Option<Bytes> {
        self.decoded_data.clone()
    }

    /// Exact logical allocation size, including encoded padding rows.
    pub fn decoded_size(&self) -> Result<usize> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BitShufflePageDecoder: not initialized",
            ));
        }
        (self.padded_num_elements as usize)
            .checked_mul(self.type_size)
            .ok_or_else(|| {
                paro_common::error::data_corrupted(
                    "BitShufflePageDecoder: decoded page size overflow",
                )
            })
    }

    /// Decode directly into a caller-owned allocation.
    pub fn materialize_into(&self, output: &mut [u8]) -> Result<()> {
        let expected_size = self.decoded_size()?;
        if output.len() != expected_size {
            return Err(paro_common::error::invalid_input(format!(
                "BitShufflePageDecoder: output size {} does not match decoded size {expected_size}",
                output.len(),
            )));
        }
        if let Some(decoded) = &self.decoded_data {
            output.copy_from_slice(decoded);
            return Ok(());
        }
        let shuffled = self.shuffled_data.as_ref().ok_or_else(|| {
            paro_common::error::internal(
                "BitShufflePageDecoder: initialized page has no bit-sliced data",
            )
        })?;
        bitunshuffle_into(shuffled, self.type_size, self.block_elements, output)
    }

    /// Attach a validated immutable logical representation.
    pub fn install_decoded(&mut self, decoded: Bytes) -> Result<()> {
        let expected_size = self.decoded_size()?;
        if decoded.len() != expected_size {
            return Err(paro_common::error::data_corrupted(format!(
                "BitShufflePageDecoder: decoded size {} does not match expected size {expected_size}",
                decoded.len(),
            )));
        }
        self.decoded_data = Some(decoded);
        Ok(())
    }

    /// Materialize the logical representation for sequential reads.
    pub fn materialize_all(&mut self) -> Result<Bytes> {
        self.ensure_materialized()?;
        self.decoded_data
            .clone()
            .ok_or_else(|| paro_common::error::internal("BitShuffle logical page is missing"))
    }

    fn ensure_materialized(&mut self) -> Result<()> {
        if self.decoded_data.is_some() {
            return Ok(());
        }
        let mut decoded = vec![0_u8; self.decoded_size()?];
        self.materialize_into(&mut decoded)?;
        self.decoded_data = Some(Bytes::from(decoded));
        Ok(())
    }
}

#[inline(always)]
unsafe fn copy_fixed_unaligned<T: Copy>(source: *const u8, output: *mut u8) {
    // SAFETY: callers guarantee that both pointers address at least
    // size_of::<T>() readable/writable bytes. read_unaligned/write_unaligned
    // deliberately impose no alignment requirement, and the buffers do not
    // overlap because the decoded page/group and destination are distinct.
    let value = unsafe { (source.cast::<T>()).read_unaligned() };
    unsafe { (output.cast::<T>()).write_unaligned(value) };
}

fn copy_bit_sliced_value(
    shuffled: &[u8],
    idx: usize,
    padded_num_elements: usize,
    type_size: usize,
    block_elements: usize,
    output: &mut [u8],
) -> Result<()> {
    if output.len() != type_size {
        return Err(paro_common::error::invalid_input(format!(
            "BitShuffle output width {} does not match element width {type_size}",
            output.len(),
        )));
    }
    let block_start = idx / block_elements * block_elements;
    let elements = (padded_num_elements - block_start).min(block_elements);
    let plane_bytes = elements / 8;
    let row_in_block = idx - block_start;
    let bit_mask = 1_u8 << (row_in_block % 8);
    let row_byte = row_in_block / 8;
    let block_offset = block_start * type_size;

    for (byte_idx, value_byte) in output.iter_mut().enumerate() {
        let plane_base = block_offset + byte_idx * 8 * plane_bytes;
        let mut value = 0_u8;
        for bit in 0..8 {
            let encoded = shuffled
                .get(plane_base + bit * plane_bytes + row_byte)
                .copied()
                .ok_or_else(|| {
                    paro_common::error::data_corrupted(
                        "BitShuffle value extends past decompressed page",
                    )
                })?;
            value |= u8::from(encoded & bit_mask != 0) << bit;
        }
        *value_byte = value;
    }
    Ok(())
}

fn decode_bit_sliced_group(
    shuffled: &[u8],
    group_start: usize,
    padded_num_elements: usize,
    type_size: usize,
    block_elements: usize,
    output: &mut [u8; 16 * 8],
) -> Result<()> {
    let block_start = group_start / block_elements * block_elements;
    let elements = (padded_num_elements - block_start).min(block_elements);
    let plane_bytes = elements / 8;
    let group_in_block = (group_start - block_start) / 8;
    let block_offset = block_start * type_size;

    for byte_idx in 0..type_size {
        let plane_base = block_offset + byte_idx * 8 * plane_bytes;
        let last_plane_byte = plane_base + 7 * plane_bytes + group_in_block;
        if last_plane_byte >= shuffled.len() {
            return Err(paro_common::error::data_corrupted(
                "BitShuffle group extends past decompressed page",
            ));
        }
        let planes =
            std::array::from_fn(|bit| shuffled[plane_base + bit * plane_bytes + group_in_block]);
        let values = transpose_8x8(u64::from_le_bytes(planes)).to_le_bytes();
        for (row_idx, value) in values.into_iter().enumerate() {
            output[row_idx * type_size + byte_idx] = value;
        }
    }
    Ok(())
}

/// Perform bitshuffle on data.
///
/// Transposes bits across elements to group similar bits together.
fn bitshuffle(data: &[u8], type_size: usize) -> Vec<u8> {
    let num_elements = data.len() / type_size;
    if num_elements == 0 {
        return Vec::new();
    }
    debug_assert!(num_elements.is_multiple_of(8));

    let mut output = vec![0u8; data.len()];

    for block_start in (0..num_elements).step_by(BITSHUFFLE_BLOCK_ELEMENTS) {
        let block_elements = (num_elements - block_start).min(BITSHUFFLE_BLOCK_ELEMENTS);
        let plane_bytes = block_elements / 8;
        let block_output = block_start * type_size;
        for bit in 0..type_size * 8 {
            let byte_idx = bit / 8;
            let bit_idx = bit % 8;
            let plane_output = block_output + bit * plane_bytes;
            for element in 0..block_elements {
                let value = data[(block_start + element) * type_size + byte_idx];
                output[plane_output + element / 8] |= ((value >> bit_idx) & 1) << (element % 8);
            }
        }
    }

    output
}

/// Reverse bitshuffle operation into an accounted caller-owned allocation.
fn bitunshuffle_into(
    data: &[u8],
    type_size: usize,
    block_elements: usize,
    output: &mut [u8],
) -> Result<()> {
    if type_size == 0 || data.len() % type_size != 0 || output.len() != data.len() {
        return Err(paro_common::error::data_corrupted(
            "BitShuffle page has an invalid decoded layout",
        ));
    }
    let total_bits = data.len() * 8;
    let bits_per_element = type_size * 8;
    let num_elements = total_bits / bits_per_element;

    if num_elements == 0 {
        return Ok(());
    }
    if !num_elements.is_multiple_of(8) || block_elements == 0 {
        return Err(paro_common::error::data_corrupted(
            "BitShuffle page has an invalid block layout",
        ));
    }
    output.fill(0);

    for block_start in (0..num_elements).step_by(block_elements) {
        let current_block_elements = (num_elements - block_start).min(block_elements);
        let plane_bytes = current_block_elements / 8;
        let block_input = block_start * type_size;
        for byte_idx in 0..type_size {
            let plane_base = block_input + byte_idx * 8 * plane_bytes;
            for group in 0..plane_bytes {
                let planes =
                    std::array::from_fn(|bit| data[plane_base + bit * plane_bytes + group]);
                let values = transpose_8x8(u64::from_le_bytes(planes)).to_le_bytes();
                let output_base = (block_start + group * 8) * type_size + byte_idx;
                for (element, value) in values.into_iter().enumerate() {
                    output[output_base + element * type_size] = value;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
fn bitunshuffle(data: &[u8], type_size: usize, block_elements: usize) -> Vec<u8> {
    let mut output = vec![0_u8; data.len()];
    bitunshuffle_into(data, type_size, block_elements, &mut output).unwrap();
    output
}

#[inline]
fn transpose_8x8(mut value: u64) -> u64 {
    let mut swap = (value ^ (value >> 7)) & 0x00AA_00AA_00AA_00AA;
    value ^= swap ^ (swap << 7);
    swap = (value ^ (value >> 14)) & 0x0000_CCCC_0000_CCCC;
    value ^= swap ^ (swap << 14);
    swap = (value ^ (value >> 28)) & 0x0000_0000_F0F0_F0F0;
    value ^ swap ^ (swap << 28)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitunshuffle_reference(data: &[u8], type_size: usize) -> Vec<u8> {
        let total_bits = data.len() * 8;
        let bits_per_element = type_size * 8;
        let num_elements = total_bits / bits_per_element;
        let mut output = vec![0u8; num_elements * type_size];
        for bit in 0..bits_per_element {
            let out_byte_idx = bit / 8;
            let out_bit_idx = bit % 8;
            for elem in 0..num_elements {
                let in_bit_pos = bit * num_elements + elem;
                let src_bit = (data[in_bit_pos / 8] >> (in_bit_pos % 8)) & 1;
                output[elem * type_size + out_byte_idx] |= src_bit << out_bit_idx;
            }
        }
        output
    }

    #[test]
    fn test_bitshuffle_roundtrip() {
        let data = (0_u8..32).collect::<Vec<_>>();
        let shuffled = bitshuffle(&data, 4);
        let unshuffled = bitunshuffle(&shuffled, 4, BITSHUFFLE_BLOCK_ELEMENTS);
        assert_eq!(data, unshuffled);
    }

    #[test]
    fn bitshuffle_roundtrip_across_blocks() {
        for type_size in [1_usize, 2, 4, 8, 16] {
            for elements in [1024_usize, 1032, 2056] {
                let data = (0..elements * type_size)
                    .map(|idx| (idx.wrapping_mul(73).wrapping_add(type_size * 11)) as u8)
                    .collect::<Vec<_>>();
                let shuffled = bitshuffle(&data, type_size);
                assert_eq!(
                    bitunshuffle(&shuffled, type_size, BITSHUFFLE_BLOCK_ELEMENTS),
                    data
                );
            }
        }
    }

    #[test]
    fn optimized_bitunshuffle_matches_reference() {
        for type_size in [1_usize, 2, 4, 8, 16] {
            for elements in [8_usize, 16, 24, 64] {
                let data = (0..elements * type_size)
                    .map(|idx| (idx.wrapping_mul(73).wrapping_add(type_size * 11)) as u8)
                    .collect::<Vec<_>>();
                let shuffled = bitshuffle(&data, type_size);
                assert_eq!(
                    bitunshuffle(&shuffled, type_size, BITSHUFFLE_BLOCK_ELEMENTS),
                    bitunshuffle_reference(&shuffled, type_size)
                );
            }
        }
    }

    #[test]
    fn decoder_rejects_unsupported_page_format() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);
        builder.add_one(&42_i32.to_le_bytes());
        let mut page = builder.finish().unwrap().to_vec();
        page[16..20].copy_from_slice(b"BSH1");

        let error = BitShufflePageDecoder::new(Bytes::from(page), 1, 4)
            .init()
            .unwrap_err();
        assert!(error.to_string().contains("unsupported page format"));
    }

    #[test]
    fn decoder_rejects_unsupported_block_layout() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);
        builder.add_one(&42_i32.to_le_bytes());
        let mut page = builder.finish().unwrap().to_vec();
        page[20..24].copy_from_slice(&512_u32.to_le_bytes());

        let error = BitShufflePageDecoder::new(Bytes::from(page), 1, 4)
            .init()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported block element count"));
    }

    #[test]
    fn decoder_rejects_page_header_that_disagrees_with_external_metadata() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);
        builder.add_one(&42_i32.to_le_bytes());
        let page = builder.finish().unwrap();

        let count_error = BitShufflePageDecoder::new(page.clone(), 2, 4)
            .init()
            .unwrap_err();
        assert!(count_error.to_string().contains("ordinal index"));

        let type_error = BitShufflePageDecoder::new(page, 1, 8).init().unwrap_err();
        assert!(type_error.to_string().contains("column schema"));
    }

    #[test]
    fn decoder_rejects_element_count_that_cannot_be_padded() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);
        builder.add_one(&42_i32.to_le_bytes());
        let mut page = builder.finish().unwrap().to_vec();
        page[..4].copy_from_slice(&u32::MAX.to_le_bytes());

        let error = BitShufflePageDecoder::new(Bytes::from(page), u32::MAX, 4)
            .init()
            .unwrap_err();
        assert!(error.to_string().contains("padded element count overflow"));
    }

    #[test]
    fn decoder_rejects_lz4_size_prefix_before_decompression() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);
        builder.add_one(&42_i32.to_le_bytes());
        let mut page = builder.finish().unwrap().to_vec();
        page[BITSHUFFLE_PAGE_HEADER_SIZE..BITSHUFFLE_PAGE_HEADER_SIZE + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());

        let error = BitShufflePageDecoder::new(Bytes::from(page), 1, 4)
            .init()
            .unwrap_err();
        assert!(error.to_string().contains("does not match expected size"));
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
        let mut decoder = BitShufflePageDecoder::new(page_data, 100, 4);
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

        let mut decoder = BitShufflePageDecoder::new(page_data, 50, 8);
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
    fn point_reads_do_not_materialize_logical_page() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);
        let values: Vec<i32> = (0..2056).map(|value| value * 17 - 9).collect();
        let bytes: Vec<u8> = values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect();
        assert_eq!(
            builder.add(&bytes, values.len() as u32),
            values.len() as u32
        );

        let mut decoder =
            BitShufflePageDecoder::new(builder.finish().unwrap(), values.len() as u32, 4);
        decoder.init().unwrap();
        assert!(decoder.materialized_data().is_none());

        for index in [0_usize, 1023, 1024, 2055] {
            let mut output = [0_u8; 4];
            decoder.copy_value_at(index as u32, &mut output).unwrap();
            assert_eq!(i32::from_le_bytes(output), values[index]);
        }
        assert!(decoder.materialized_data().is_none());

        decoder.next_batch(1).unwrap();
        assert!(decoder.materialized_data().is_some());
    }

    #[test]
    fn sparse_gather_reuses_bit_slice_groups_and_preserves_output_order() {
        let mut builder = BitShufflePageBuilder::new(8, 256 * 1024);
        let values: Vec<i64> = (0..2056).map(|value| value * 101 - 37).collect();
        let bytes: Vec<u8> = values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect();
        assert_eq!(
            builder.add(&bytes, values.len() as u32),
            values.len() as u32
        );

        let mut decoder =
            BitShufflePageDecoder::new(builder.finish().unwrap(), values.len() as u32, 8);
        decoder.init().unwrap();
        let gather = [(0, 3), (1, 0), (7, 5), (8, 1), (8, 4), (1024, 2)];
        let mut output = [0_u8; 6 * 8];
        decoder.gather_values_at(gather, &mut output).unwrap();

        for (source_idx, output_idx) in gather {
            let start = output_idx * 8;
            assert_eq!(
                i64::from_le_bytes(output[start..start + 8].try_into().unwrap()),
                values[source_idx as usize]
            );
        }
        assert!(decoder.materialized_data().is_none());
    }

    #[test]
    fn decoded_cache_entry_validates_current_layout() {
        let values = [17_i32, -4, 99, 0, 0, 0, 0, 0];
        let bytes = Bytes::from(
            values
                .into_iter()
                .flat_map(i32::to_le_bytes)
                .collect::<Vec<_>>(),
        );
        let mut decoder = BitShufflePageDecoder::from_decoded_data(3, 4, bytes).unwrap();
        let (count, decoded) = decoder.next_batch(3).unwrap();
        assert_eq!(count, 3);
        assert_eq!(decoded.len(), 12);

        let error = BitShufflePageDecoder::from_decoded_data(4, 4, decoded)
            .err()
            .expect("mismatched cache layout must fail")
            .to_string();
        assert!(error.contains("does not match expected size"));

        let invalid_width = BitShufflePageDecoder::from_decoded_data(1, 3, Bytes::from(vec![0; 8]))
            .err()
            .expect("unsupported cached physical width must fail")
            .to_string();
        assert!(invalid_width.contains("invalid cached type size"));
    }

    #[test]
    fn test_bitshuffle_seek() {
        let mut builder = BitShufflePageBuilder::new(4, 256 * 1024);

        let values: Vec<i32> = (0..100).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        builder.add(&bytes, 100);

        let page_data = builder.finish().unwrap();

        let mut decoder = BitShufflePageDecoder::new(page_data, 100, 4);
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
        let mut decoder = BitShufflePageDecoder::new(page_data, 1000, 4);
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
