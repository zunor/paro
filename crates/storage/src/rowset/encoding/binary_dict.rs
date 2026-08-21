// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Binary Dictionary Page Encoding
//!
//! Dictionary encoding for string columns. Maps distinct values to integer codes.
//! Effective for low-cardinality columns.
//!
//! ## Architecture
//!
//! - One dictionary page per column (shared by all data pages)
//! - Data pages store integer codes instead of actual strings
//! - When dictionary becomes too large, falls back to plain encoding
//!
//! ## Data Page Layout (DICT_ENCODING mode)
//!
//! ```text
//! +------------------+
//! | encoding_type(1) |  <- 0 = DICT, 1 = PLAIN
//! +------------------+
//! | code page data   |  <- BitShuffle encoded codes
//! +------------------+
//! ```
//!
//! ## Dictionary Page Layout
//!
//! Same as BinaryPlainPage - stores all distinct values.

use super::binary_plain::{BinaryPlainPageBuilder, BinaryPlainPageDecoder};
use super::bitshuffle::{BitShufflePageBuilder, BitShufflePageDecoder};
use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::Result;
use std::collections::HashMap;

/// Encoding type marker in data page header.
const DICT_ENCODING_MARKER: u8 = 0;
const PLAIN_ENCODING_MARKER: u8 = 1;

/// Default dictionary page size limit.
const DEFAULT_DICT_PAGE_SIZE: usize = 1024 * 1024; // 1MB

/// Builder for dictionary-encoded string pages.
pub struct BinaryDictPageBuilder {
    /// Page size limit
    page_size: usize,
    /// Dictionary page size limit
    dict_page_size: usize,
    /// Dictionary builder (stores distinct values)
    dict_builder: BinaryPlainPageBuilder,
    /// Code page builder (stores integer codes)
    code_builder: BitShufflePageBuilder,
    /// String -> code mapping
    dictionary: HashMap<Vec<u8>, u32>,
    /// Current encoding mode
    encoding_type: EncodingMode,
    /// Plain builder (used when dict is full)
    plain_builder: Option<BinaryPlainPageBuilder>,
    /// First value
    first_value: Option<Bytes>,
    /// Last value
    last_value: Option<Bytes>,
    /// Whether finish() has been called
    finished: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EncodingMode {
    Dict,
    Plain,
}

impl BinaryDictPageBuilder {
    /// Create a new dictionary page builder.
    pub fn new(page_size: usize) -> Self {
        BinaryDictPageBuilder {
            page_size,
            dict_page_size: DEFAULT_DICT_PAGE_SIZE,
            dict_builder: BinaryPlainPageBuilder::new(DEFAULT_DICT_PAGE_SIZE),
            code_builder: BitShufflePageBuilder::new(4, page_size), // 4 bytes per code
            dictionary: HashMap::new(),
            encoding_type: EncodingMode::Dict,
            plain_builder: None,
            first_value: None,
            last_value: None,
            finished: false,
        }
    }

    /// Set dictionary page size limit.
    pub fn with_dict_page_size(mut self, size: usize) -> Self {
        self.dict_page_size = size;
        self.dict_builder = BinaryPlainPageBuilder::new(size);
        self
    }

    /// Check if the page is full.
    pub fn is_page_full(&self) -> bool {
        match self.encoding_type {
            EncodingMode::Dict => self.code_builder.is_page_full(),
            EncodingMode::Plain => self
                .plain_builder
                .as_ref()
                .is_some_and(|b| b.is_page_full()),
        }
    }

    /// Add a string value.
    pub fn add_slice(&mut self, s: &[u8]) -> bool {
        if self.is_page_full() {
            return false;
        }

        let added = match self.encoding_type {
            EncodingMode::Dict => {
                // Try to add to dictionary
                let code = if let Some(&code) = self.dictionary.get(s) {
                    code
                } else {
                    // Check if dictionary is full
                    if self.dict_builder.size() as usize + s.len() + 4 > self.dict_page_size {
                        if self.code_builder.count() > 0 {
                            return false;
                        }
                        self.switch_to_plain();
                        let added = self.add_slice_plain(s);
                        if added {
                            self.record_value(s);
                        }
                        return added;
                    }

                    // Add to dictionary
                    let code = self.dictionary.len() as u32;
                    self.dict_builder.add_slice(s);
                    self.dictionary.insert(s.to_vec(), code);
                    code
                };

                // Add code to code page
                let code_bytes = code.to_le_bytes();
                self.code_builder.add_one(&code_bytes)
            }
            EncodingMode::Plain => self.add_slice_plain(s),
        };

        if added {
            self.record_value(s);
        }

        added
    }

    fn record_value(&mut self, s: &[u8]) {
        if self.first_value.is_none() {
            self.first_value = Some(Bytes::copy_from_slice(s));
        }
        self.last_value = Some(Bytes::copy_from_slice(s));
    }

    fn add_slice_plain(&mut self, s: &[u8]) -> bool {
        if let Some(ref mut builder) = self.plain_builder {
            builder.add_slice(s)
        } else {
            false
        }
    }

    fn switch_to_plain(&mut self) {
        self.encoding_type = EncodingMode::Plain;
        self.plain_builder = Some(BinaryPlainPageBuilder::new(self.page_size));
        // The caller only switches on an empty data page. Earlier pages keep using
        // the column-level dictionary, and later pages can safely use plain bytes.
    }

    /// Finish building the page.
    pub fn finish(&mut self) -> Result<Bytes> {
        assert!(!self.finished);
        self.finished = true;

        match self.encoding_type {
            EncodingMode::Dict => {
                let code_page = self.code_builder.finish()?;

                // Prepend encoding type marker
                let mut output = BytesMut::with_capacity(1 + code_page.len());
                output.put_u8(DICT_ENCODING_MARKER);
                output.extend_from_slice(&code_page);
                Ok(output.freeze())
            }
            EncodingMode::Plain => {
                let plain_page = self.plain_builder.as_mut().unwrap().finish()?;

                // Prepend encoding type marker
                let mut output = BytesMut::with_capacity(1 + plain_page.len());
                output.put_u8(PLAIN_ENCODING_MARKER);
                output.extend_from_slice(&plain_page);
                Ok(output.freeze())
            }
        }
    }

    /// Get the dictionary page.
    pub fn get_dictionary_page(&mut self) -> Option<Bytes> {
        if !self.dictionary.is_empty() {
            self.dict_builder.finish().ok()
        } else {
            None
        }
    }

    /// Reset the builder for a new data page.
    /// Note: Dictionary is preserved across pages.
    pub fn reset(&mut self) {
        self.code_builder.reset();
        if let Some(ref mut builder) = self.plain_builder {
            builder.reset();
        }
        self.first_value = None;
        self.last_value = None;
        self.finished = false;
    }

    /// Get element count.
    pub fn count(&self) -> u32 {
        match self.encoding_type {
            EncodingMode::Dict => self.code_builder.count(),
            EncodingMode::Plain => self.plain_builder.as_ref().map_or(0, |b| b.count()),
        }
    }

    /// Get current size.
    pub fn size(&self) -> u64 {
        match self.encoding_type {
            EncodingMode::Dict => self.code_builder.size() + 1,
            EncodingMode::Plain => self.plain_builder.as_ref().map_or(1, |b| b.size() + 1),
        }
    }

    /// Get first value.
    pub fn get_first_value(&self) -> Option<Bytes> {
        self.first_value.clone()
    }

    /// Get last value.
    pub fn get_last_value(&self) -> Option<Bytes> {
        self.last_value.clone()
    }

    /// Check if all pages used dictionary encoding.
    pub fn all_dict_encoded(&self) -> bool {
        self.encoding_type == EncodingMode::Dict
    }

    /// Check if a global dictionary is valid for this builder.
    pub fn is_valid_global_dict(&self, global_dict: &HashMap<Vec<u8>, u32>) -> bool {
        // Check that all our dictionary entries exist in global dict
        for (key, &local_code) in &self.dictionary {
            if let Some(&global_code) = global_dict.get(key) {
                if global_code != local_code {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

/// Decoder for dictionary-encoded string pages.
pub struct BinaryDictPageDecoder {
    /// Page data
    data: Bytes,
    /// Row count supplied by the ordinal index.
    expected_num_elements: u32,
    /// Encoding type
    encoding_type: EncodingMode,
    /// Code decoder (for dict mode)
    code_decoder: Option<BitShufflePageDecoder>,
    /// Plain decoder (for plain mode)
    plain_decoder: Option<BinaryPlainPageDecoder>,
    /// Dictionary decoder (set externally)
    dict_decoder: Option<BinaryPlainPageDecoder>,
    /// Version-isolated logical code stream supplied by the decoded-page cache.
    decoded_codes: Option<Bytes>,
    /// Whether init() has been called
    parsed: bool,
}

impl BinaryDictPageDecoder {
    /// Create a new decoder.
    pub fn new(data: Bytes, expected_num_elements: u32) -> Self {
        BinaryDictPageDecoder {
            data,
            expected_num_elements,
            encoding_type: EncodingMode::Dict,
            code_decoder: None,
            plain_decoder: None,
            dict_decoder: None,
            decoded_codes: None,
            parsed: false,
        }
    }

    /// Create a decoder backed by a cached logical dictionary-code stream.
    /// The physical marker and BitShuffle header are still validated by
    /// [`Self::init`] before the cached bytes become observable.
    pub fn with_decoded_codes(
        data: Bytes,
        expected_num_elements: u32,
        decoded_codes: Bytes,
    ) -> Self {
        let mut decoder = Self::new(data, expected_num_elements);
        decoder.decoded_codes = Some(decoded_codes);
        decoder
    }

    /// Set the dictionary decoder.
    pub fn set_dict_decoder(&mut self, dict_data: Bytes) -> Result<()> {
        let mut decoder = BinaryPlainPageDecoder::new(dict_data);
        decoder.init()?;
        self.dict_decoder = Some(decoder);
        Ok(())
    }

    /// Install a dictionary whose offset table was already validated by the
    /// owning column reader. Cloning the decoder only clones the immutable
    /// page owner and parsed metadata; no dictionary-sized validation loop is
    /// repeated per data page or scan morsel.
    pub(crate) fn set_prepared_dictionary(
        &mut self,
        mut decoder: BinaryPlainPageDecoder,
    ) -> Result<()> {
        decoder.seek_to_position(0)?;
        self.dict_decoder = Some(decoder);
        Ok(())
    }

    /// Initialize the decoder.
    pub fn init(&mut self) -> Result<()> {
        if self.parsed {
            return Ok(());
        }

        if self.data.is_empty() {
            return Err(paro_common::error::data_corrupted(
                "BinaryDictPageDecoder: empty data",
            ));
        }

        // Read encoding type marker
        let marker = self.data[0];
        let page_data = self.data.slice(1..);

        match marker {
            DICT_ENCODING_MARKER => {
                self.encoding_type = EncodingMode::Dict;
                let mut decoder = if let Some(decoded_codes) = self.decoded_codes.take() {
                    BitShufflePageDecoder::with_decoded_data(
                        page_data,
                        self.expected_num_elements,
                        4,
                        decoded_codes,
                    )
                } else {
                    BitShufflePageDecoder::new(page_data, self.expected_num_elements, 4)
                };
                decoder.init()?;
                self.code_decoder = Some(decoder);
            }
            PLAIN_ENCODING_MARKER => {
                self.encoding_type = EncodingMode::Plain;
                self.decoded_codes = None;
                let mut decoder = BinaryPlainPageDecoder::new(page_data);
                decoder.init()?;
                if decoder.count() != self.expected_num_elements {
                    return Err(paro_common::error::data_corrupted(format!(
                        "BinaryDictPageDecoder: element count {} does not match ordinal index {}",
                        decoder.count(),
                        self.expected_num_elements,
                    )));
                }
                self.plain_decoder = Some(decoder);
            }
            _ => {
                return Err(paro_common::error::data_corrupted(format!(
                    "BinaryDictPageDecoder: invalid encoding marker {}",
                    marker
                )));
            }
        }

        self.parsed = true;
        Ok(())
    }

    /// Seek to a position.
    pub fn seek_to_position(&mut self, pos: u32) -> Result<()> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryDictPageDecoder: not initialized",
            ));
        }

        match self.encoding_type {
            EncodingMode::Dict => {
                if let Some(ref mut decoder) = self.code_decoder {
                    decoder.seek_to_position(pos)
                } else {
                    Err(paro_common::error::internal("no code decoder"))
                }
            }
            EncodingMode::Plain => {
                if let Some(ref mut decoder) = self.plain_decoder {
                    decoder.seek_to_position(pos)
                } else {
                    Err(paro_common::error::internal("no plain decoder"))
                }
            }
        }
    }

    /// Read the next batch of strings.
    pub fn next_batch(&mut self, n: usize) -> Result<Vec<Bytes>> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryDictPageDecoder: not initialized",
            ));
        }

        match self.encoding_type {
            EncodingMode::Dict => {
                let dict = self
                    .dict_decoder
                    .as_ref()
                    .ok_or_else(|| paro_common::error::internal("dictionary not set"))?;

                let code_decoder = self
                    .code_decoder
                    .as_mut()
                    .ok_or_else(|| paro_common::error::internal("no code decoder"))?;

                let (count, code_data) = code_decoder.next_batch(n)?;
                let mut result = Vec::with_capacity(count);

                for i in 0..count {
                    let offset = i * 4;
                    let code = u32::from_le_bytes([
                        code_data[offset],
                        code_data[offset + 1],
                        code_data[offset + 2],
                        code_data[offset + 3],
                    ]);

                    if let Some(s) = dict.string_at(code) {
                        result.push(s);
                    } else {
                        return Err(paro_common::error::data_corrupted(format!(
                            "invalid dictionary code {}",
                            code
                        )));
                    }
                }

                Ok(result)
            }
            EncodingMode::Plain => {
                if let Some(ref mut decoder) = self.plain_decoder {
                    decoder.next_batch(n)
                } else {
                    Err(paro_common::error::internal("no plain decoder"))
                }
            }
        }
    }

    /// Read dictionary codes directly (for late materialization).
    pub fn next_dict_codes(&mut self, n: usize) -> Result<(usize, Bytes)> {
        if !self.parsed {
            return Err(paro_common::error::internal(
                "BinaryDictPageDecoder: not initialized",
            ));
        }

        if self.encoding_type != EncodingMode::Dict {
            return Err(paro_common::error::not_supported("not dictionary encoded"));
        }

        if let Some(ref mut decoder) = self.code_decoder {
            decoder.next_batch(n)
        } else {
            Err(paro_common::error::internal("no code decoder"))
        }
    }

    /// Get element count.
    pub fn count(&self) -> u32 {
        match self.encoding_type {
            EncodingMode::Dict => self.code_decoder.as_ref().map_or(0, |d| d.count()),
            EncodingMode::Plain => self.plain_decoder.as_ref().map_or(0, |d| d.count()),
        }
    }

    /// Get current index.
    pub fn current_index(&self) -> u32 {
        match self.encoding_type {
            EncodingMode::Dict => self.code_decoder.as_ref().map_or(0, |d| d.current_index()),
            EncodingMode::Plain => self.plain_decoder.as_ref().map_or(0, |d| d.current_index()),
        }
    }

    /// Check if this page uses dictionary encoding.
    pub fn is_dict_encoded(&self) -> bool {
        self.encoding_type == EncodingMode::Dict
    }

    /// Borrow one logical value without advancing the page cursor.
    ///
    /// Both physical modes are addressable: plain pages carry a validated
    /// offset table, while dictionary pages expose a fixed-width code at each
    /// row. Sparse column gathers use this API to avoid decoding the span
    /// between selected rows.
    pub(crate) fn value_at(&self, idx: u32) -> Result<Option<Bytes>> {
        if !self.parsed || idx >= self.count() {
            return Ok(None);
        }
        match self.encoding_type {
            EncodingMode::Plain => Ok(self
                .plain_decoder
                .as_ref()
                .and_then(|decoder| decoder.string_at(idx))),
            EncodingMode::Dict => {
                let decoder = self
                    .code_decoder
                    .as_ref()
                    .ok_or_else(|| paro_common::error::internal("no code decoder"))?;
                let mut encoded = [0_u8; std::mem::size_of::<u32>()];
                decoder.copy_value_at(idx, &mut encoded)?;
                let code = u32::from_le_bytes(encoded);
                let dictionary = self
                    .dict_decoder
                    .as_ref()
                    .ok_or_else(|| paro_common::error::internal("dictionary not set"))?;
                dictionary.string_at(code).map(Some).ok_or_else(|| {
                    paro_common::error::data_corrupted(format!(
                        "dictionary code {code} exceeds dictionary size {}",
                        dictionary.count()
                    ))
                })
            }
        }
    }

    pub(crate) fn code_decoder_mut(&mut self) -> Option<&mut BitShufflePageDecoder> {
        self.code_decoder.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_dict_basic() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);

        // Add strings with repetition
        builder.add_slice(b"apple");
        builder.add_slice(b"banana");
        builder.add_slice(b"apple");
        builder.add_slice(b"cherry");
        builder.add_slice(b"banana");
        builder.add_slice(b"apple");

        assert_eq!(builder.count(), 6);
        assert!(builder.all_dict_encoded());

        // Get dictionary page
        let dict_page = builder.get_dictionary_page().unwrap();

        // Finish data page
        let data_page = builder.finish().unwrap();

        // Decode
        let mut decoder = BinaryDictPageDecoder::new(data_page, 6);
        decoder.set_dict_decoder(dict_page).unwrap();
        decoder.init().unwrap();

        assert_eq!(decoder.count(), 6);
        assert!(decoder.is_dict_encoded());

        // Read strings
        let strings = decoder.next_batch(6).unwrap();
        assert_eq!(strings.len(), 6);
        assert_eq!(strings[0].as_ref(), b"apple");
        assert_eq!(strings[1].as_ref(), b"banana");
        assert_eq!(strings[2].as_ref(), b"apple");
        assert_eq!(strings[3].as_ref(), b"cherry");
        assert_eq!(strings[4].as_ref(), b"banana");
        assert_eq!(strings[5].as_ref(), b"apple");
    }

    #[test]
    fn test_binary_dict_compression() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);

        // Add many repeated strings
        for _ in 0..1000 {
            builder.add_slice(b"repeated_value");
        }

        let dict_page = builder.get_dictionary_page().unwrap();
        let data_page = builder.finish().unwrap();

        // Dictionary should be small (just one entry)
        assert!(dict_page.len() < 100);

        // Data page should be much smaller than raw data
        assert!(data_page.len() < 1000 * 14); // 14 = len("repeated_value")

        // Verify
        let mut decoder = BinaryDictPageDecoder::new(data_page, 1000);
        decoder.set_dict_decoder(dict_page).unwrap();
        decoder.init().unwrap();

        let strings = decoder.next_batch(1000).unwrap();
        assert_eq!(strings.len(), 1000);
        assert!(strings.iter().all(|s| s.as_ref() == b"repeated_value"));
    }

    #[test]
    fn test_binary_dict_seek() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);

        builder.add_slice(b"a");
        builder.add_slice(b"b");
        builder.add_slice(b"c");
        builder.add_slice(b"d");
        builder.add_slice(b"e");

        let dict_page = builder.get_dictionary_page().unwrap();
        let data_page = builder.finish().unwrap();

        let mut decoder = BinaryDictPageDecoder::new(data_page, 5);
        decoder.set_dict_decoder(dict_page).unwrap();
        decoder.init().unwrap();

        // Seek to position 2
        decoder.seek_to_position(2).unwrap();
        assert_eq!(decoder.current_index(), 2);

        let strings = decoder.next_batch(3).unwrap();
        assert_eq!(strings.len(), 3);
        assert_eq!(strings[0].as_ref(), b"c");
        assert_eq!(strings[1].as_ref(), b"d");
        assert_eq!(strings[2].as_ref(), b"e");
    }

    #[test]
    fn addressable_values_preserve_dict_and_plain_page_cursors() {
        let mut dict_builder = BinaryDictPageBuilder::new(256 * 1024);
        for value in [b"zero".as_slice(), b"one", b"two", b"one"] {
            assert!(dict_builder.add_slice(value));
        }
        let dictionary = dict_builder.get_dictionary_page().unwrap();
        let dict_page = dict_builder.finish().unwrap();
        let mut dict_decoder = BinaryDictPageDecoder::new(dict_page, 4);
        dict_decoder.set_dict_decoder(dictionary).unwrap();
        dict_decoder.init().unwrap();
        dict_decoder.seek_to_position(1).unwrap();
        assert_eq!(
            dict_decoder.value_at(3).unwrap().as_deref(),
            Some(b"one".as_slice())
        );
        assert_eq!(
            dict_decoder.value_at(0).unwrap().as_deref(),
            Some(b"zero".as_slice())
        );
        assert_eq!(dict_decoder.current_index(), 1);

        let mut plain_builder = BinaryDictPageBuilder::new(256 * 1024).with_dict_page_size(13);
        assert!(plain_builder.add_slice(b"alpha"));
        assert!(!plain_builder.add_slice(b"bravo"));
        let _ = plain_builder.finish().unwrap();
        plain_builder.reset();
        assert!(plain_builder.add_slice(b"bravo"));
        assert!(plain_builder.add_slice(b"charlie"));
        let plain_page = plain_builder.finish().unwrap();
        let mut plain_decoder = BinaryDictPageDecoder::new(plain_page, 2);
        plain_decoder.init().unwrap();
        plain_decoder.seek_to_position(1).unwrap();
        assert_eq!(
            plain_decoder.value_at(0).unwrap().as_deref(),
            Some(b"bravo".as_slice())
        );
        assert_eq!(
            plain_decoder.value_at(1).unwrap().as_deref(),
            Some(b"charlie".as_slice())
        );
        assert_eq!(plain_decoder.current_index(), 1);
    }

    #[test]
    fn test_binary_dict_first_last_value() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);

        builder.add_slice(b"first");
        builder.add_slice(b"middle");
        builder.add_slice(b"last");

        assert_eq!(builder.get_first_value().unwrap().as_ref(), b"first");
        assert_eq!(builder.get_last_value().unwrap().as_ref(), b"last");
    }

    #[test]
    fn test_binary_dict_reset() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);

        builder.add_slice(b"page1_value1");
        builder.add_slice(b"page1_value2");
        assert_eq!(builder.count(), 2);

        // Finish first page
        let _page1 = builder.finish().unwrap();

        // Reset for second page (dictionary preserved)
        builder.reset();
        assert_eq!(builder.count(), 0);

        // Add to second page - should reuse dictionary
        builder.add_slice(b"page1_value1"); // Already in dict
        builder.add_slice(b"page2_new");
        assert_eq!(builder.count(), 2);
    }

    #[test]
    fn test_binary_dict_flushes_before_plain_fallback() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024).with_dict_page_size(13);

        assert!(builder.add_slice(b"alpha"));
        assert!(!builder.add_slice(b"bravo"));
        assert_eq!(builder.count(), 1);
        assert_eq!(builder.get_last_value().unwrap().as_ref(), b"alpha");

        let dict_encoded_page = builder.finish().unwrap();
        builder.reset();

        assert!(builder.add_slice(b"bravo"));
        assert_eq!(builder.count(), 1);
        assert!(!builder.all_dict_encoded());

        let plain_page = builder.finish().unwrap();
        let global_dict = builder.get_dictionary_page().unwrap();

        let mut dict_decoder = BinaryDictPageDecoder::new(dict_encoded_page, 1);
        dict_decoder.set_dict_decoder(global_dict.clone()).unwrap();
        dict_decoder.init().unwrap();
        assert!(dict_decoder.is_dict_encoded());
        let values = dict_decoder.next_batch(1).unwrap();
        assert_eq!(values[0].as_ref(), b"alpha");

        let mut plain_decoder = BinaryDictPageDecoder::new(plain_page, 1);
        plain_decoder.init().unwrap();
        assert!(!plain_decoder.is_dict_encoded());
        let values = plain_decoder.next_batch(1).unwrap();
        assert_eq!(values[0].as_ref(), b"bravo");
    }

    #[test]
    fn test_binary_dict_codes() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);

        builder.add_slice(b"zero");
        builder.add_slice(b"one");
        builder.add_slice(b"two");
        builder.add_slice(b"zero");
        builder.add_slice(b"one");

        let dict_page = builder.get_dictionary_page().unwrap();
        let data_page = builder.finish().unwrap();

        let mut decoder = BinaryDictPageDecoder::new(data_page, 5);
        decoder.set_dict_decoder(dict_page).unwrap();
        decoder.init().unwrap();

        // Read codes directly
        let (count, codes) = decoder.next_dict_codes(5).unwrap();
        assert_eq!(count, 5);

        // Verify codes
        let code0 = u32::from_le_bytes([codes[0], codes[1], codes[2], codes[3]]);
        let code1 = u32::from_le_bytes([codes[4], codes[5], codes[6], codes[7]]);
        let code2 = u32::from_le_bytes([codes[8], codes[9], codes[10], codes[11]]);
        let code3 = u32::from_le_bytes([codes[12], codes[13], codes[14], codes[15]]);
        let code4 = u32::from_le_bytes([codes[16], codes[17], codes[18], codes[19]]);

        assert_eq!(code0, 0); // "zero"
        assert_eq!(code1, 1); // "one"
        assert_eq!(code2, 2); // "two"
        assert_eq!(code3, 0); // "zero" again
        assert_eq!(code4, 1); // "one" again
    }

    #[test]
    fn cached_codes_skip_inner_bitshuffle_decompression() {
        let mut builder = BinaryDictPageBuilder::new(256 * 1024);
        for value in [b"zero".as_slice(), b"one", b"two", b"zero", b"one"] {
            assert!(builder.add_slice(value));
        }
        let mut data_page = builder.finish().unwrap().to_vec();

        // Keep the physical marker and BitShuffle header intact, but make the
        // embedded LZ4 size prefix disagree with the validated logical size.
        let lz4_size_prefix = 1 + super::super::bitshuffle::BITSHUFFLE_PAGE_HEADER_SIZE;
        data_page[lz4_size_prefix..lz4_size_prefix + 4].fill(0);
        let corrupted = Bytes::from(data_page);
        let mut uncached = BinaryDictPageDecoder::new(corrupted.clone(), 5);
        assert!(uncached.init().is_err());

        let mut logical_codes = Vec::new();
        for code in [0_u32, 1, 2, 0, 1, 0, 0, 0] {
            logical_codes.extend_from_slice(&code.to_le_bytes());
        }
        let mut cached =
            BinaryDictPageDecoder::with_decoded_codes(corrupted, 5, Bytes::from(logical_codes));
        cached.init().unwrap();
        let (count, codes) = cached.next_dict_codes(5).unwrap();
        assert_eq!(count, 5);
        assert_eq!(
            codes
                .chunks_exact(4)
                .map(|code| u32::from_le_bytes(code.try_into().unwrap()))
                .collect::<Vec<_>>(),
            [0, 1, 2, 0, 1]
        );
    }
}
