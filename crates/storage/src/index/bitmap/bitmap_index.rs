// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Bitmap Index Implementation
//!
//! Bitmap index for low-cardinality columns using RoaringBitmap.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

const BITMAP_INDEX_HEADER_BYTES: usize = 6;

/// Bitmap type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BitmapType {
    /// Roaring bitmap (default)
    Roaring = 1,
}

impl BitmapType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(BitmapType::Roaring),
            _ => Err(paro_error::not_supported(format!(
                "Unknown bitmap type: {}",
                v
            ))),
        }
    }
}

/// Bitmap index writer.
///
/// Builds a bitmap index with:
/// - Dictionary: sorted distinct values
/// - Bitmaps: one RoaringBitmap per dictionary entry
#[derive(Debug)]
pub struct BitmapIndexWriter {
    /// Value -> row IDs mapping
    value_to_rows: BTreeMap<Bytes, RoaringBitmap>,
    /// Current row ID
    current_row_id: u32,
    /// Whether the column has null values
    has_null: bool,
    /// Null bitmap
    null_bitmap: RoaringBitmap,
}

impl BitmapIndexWriter {
    /// Create a new bitmap index writer.
    pub fn new() -> Self {
        BitmapIndexWriter {
            value_to_rows: BTreeMap::new(),
            current_row_id: 0,
            has_null: false,
            null_bitmap: RoaringBitmap::new(),
        }
    }

    /// Add a value at the current row.
    pub fn add_value(&mut self, value: &[u8]) {
        if let Some(rows) = self.value_to_rows.get_mut(value) {
            rows.insert(self.current_row_id);
        } else {
            self.value_to_rows.insert(
                Bytes::copy_from_slice(value),
                RoaringBitmap::from_iter([self.current_row_id]),
            );
        }
        self.current_row_id += 1;
    }

    /// Add an owned encoded value without copying it when it creates a new
    /// dictionary entry. Duplicate-heavy adaptive indexes therefore allocate
    /// once per distinct value rather than once per row.
    pub fn add_value_owned(&mut self, value: Vec<u8>) {
        if let Some(rows) = self.value_to_rows.get_mut(value.as_slice()) {
            rows.insert(self.current_row_id);
        } else {
            self.value_to_rows.insert(
                Bytes::from(value),
                RoaringBitmap::from_iter([self.current_row_id]),
            );
        }
        self.current_row_id += 1;
    }

    /// Add values for multiple rows.
    pub fn add_values(&mut self, values: &[&[u8]]) {
        for value in values {
            self.add_value(value);
        }
    }

    /// Add null values.
    pub fn add_nulls(&mut self, count: u32) {
        for _ in 0..count {
            self.null_bitmap.insert(self.current_row_id);
            self.current_row_id += 1;
        }
        if count > 0 {
            self.has_null = true;
        }
    }

    /// Get the number of distinct values.
    pub fn num_values(&self) -> usize {
        self.value_to_rows.len()
    }

    /// Finish and serialize the index.
    ///
    /// Format:
    /// ```text
    /// bitmap_type(1) | has_null(1) | num_values(4)
    /// [value_len(4) | value | bitmap_len(4) | bitmap_data] * num_values
    /// [null_bitmap_len(4) | null_bitmap_data] (if has_null)
    /// ```
    pub fn finish(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        buf.put_u8(BitmapType::Roaring as u8);
        buf.put_u8(if self.has_null { 1 } else { 0 });
        buf.put_u32_le(self.value_to_rows.len() as u32);

        // Dictionary and bitmaps
        for (value, bitmap) in &self.value_to_rows {
            // Write value
            buf.put_u32_le(value.len() as u32);
            buf.extend_from_slice(value);

            // Serialize bitmap
            let mut bitmap_buf = Vec::new();
            bitmap
                .serialize_into(&mut bitmap_buf)
                .map_err(|e| paro_error::internal(format!("Failed to serialize bitmap: {}", e)))?;
            buf.put_u32_le(bitmap_buf.len() as u32);
            buf.extend_from_slice(&bitmap_buf);
        }

        // Null bitmap
        if self.has_null {
            let mut null_buf = Vec::new();
            self.null_bitmap
                .serialize_into(&mut null_buf)
                .map_err(|e| {
                    paro_error::internal(format!("Failed to serialize null bitmap: {}", e))
                })?;
            buf.put_u32_le(null_buf.len() as u32);
            buf.extend_from_slice(&null_buf);
        }

        Ok(buf.freeze())
    }

    /// Get the estimated size in bytes.
    pub fn size(&self) -> usize {
        let mut size = BITMAP_INDEX_HEADER_BYTES;
        for (value, bitmap) in &self.value_to_rows {
            size += 4 + value.len(); // value
            size += 4 + bitmap.serialized_size(); // bitmap
        }
        if self.has_null {
            size += 4 + self.null_bitmap.serialized_size();
        }
        size
    }
}

impl Default for BitmapIndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Bitmap index reader.
#[derive(Debug)]
pub struct BitmapIndexReader {
    /// Number of segment-local rows covered by this artifact.
    row_count: u32,
    /// Bitmap type
    bitmap_type: BitmapType,
    /// Whether the index has null bitmap
    has_null: bool,
    /// Dictionary values (sorted)
    dictionary: Vec<Bytes>,
    /// Decoded immutable postings. The serialized artifact remains owned by
    /// `BitmapIndex`; query execution must not decode the same posting again.
    bitmaps: Vec<Arc<RoaringBitmap>>,
    /// Decoded NULL posting.
    null_bitmap: Option<Arc<RoaringBitmap>>,
    /// Dictionary ordinal for every segment-local row. This index-owned,
    /// dense representation turns broad predicate admission into one array
    /// lookup instead of materializing a query-wide union bitmap.
    row_ordinals: Option<Arc<[u16]>>,
    bitmap_cardinalities: Vec<u64>,
    null_cardinality: u64,
}

impl BitmapIndexReader {
    /// Create from serialized index data.
    pub fn from_bytes(data: &Bytes) -> Result<Self> {
        if data.len() < BITMAP_INDEX_HEADER_BYTES {
            return Err(paro_error::data_corrupted(
                "BitmapIndexReader: data too small",
            ));
        }

        let mut buf = data.as_ref();
        let bitmap_type = BitmapType::from_u8(buf.get_u8())?;
        let has_null = buf.get_u8() != 0;
        let num_values = buf.get_u32_le() as usize;

        let mut dictionary = Vec::with_capacity(num_values);
        let mut serialized_bitmaps = Vec::with_capacity(num_values);

        for _ in 0..num_values {
            // Read value
            if buf.remaining() < 4 {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: truncated value length",
                ));
            }
            let value_len = buf.get_u32_le() as usize;
            if buf.remaining() < value_len {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: truncated value",
                ));
            }
            dictionary.push(Bytes::copy_from_slice(&buf[..value_len]));
            buf.advance(value_len);

            // Read bitmap
            if buf.remaining() < 4 {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: truncated bitmap length",
                ));
            }
            let bitmap_len = buf.get_u32_le() as usize;
            if buf.remaining() < bitmap_len {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: truncated bitmap",
                ));
            }
            serialized_bitmaps.push(Bytes::copy_from_slice(&buf[..bitmap_len]));
            buf.advance(bitmap_len);
        }

        // Read null bitmap
        let serialized_null_bitmap = if has_null {
            if buf.remaining() < 4 {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: truncated null bitmap length",
                ));
            }
            let null_len = buf.get_u32_le() as usize;
            if buf.remaining() < null_len {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: truncated null bitmap",
                ));
            }
            let bitmap = Bytes::copy_from_slice(&buf[..null_len]);
            buf.advance(null_len);
            Some(bitmap)
        } else {
            None
        };

        if buf.has_remaining() {
            return Err(paro_error::data_corrupted(format!(
                "BitmapIndexReader: {} trailing bytes",
                buf.remaining()
            )));
        }

        if dictionary
            .windows(2)
            .any(|values| values[0].as_ref() >= values[1].as_ref())
        {
            return Err(paro_error::data_corrupted(
                "BitmapIndexReader: dictionary must be strictly ordered",
            ));
        }

        // Every segment-local row must occur in exactly one value or NULL
        // posting. A dense zero-based domain lets us derive covered cardinality
        // from the existing durable format; the segment loader then compares it
        // with the footer before issuing a completeness credential. The proof
        // therefore does not require a breaking auxiliary-index format change.
        let supports_ordinals = num_values < u16::MAX as usize;
        let mut row_ordinals = supports_ordinals.then(Vec::<u16>::new);
        let mut bitmap_cardinalities = Vec::with_capacity(serialized_bitmaps.len());
        let mut bitmaps = Vec::with_capacity(serialized_bitmaps.len());
        let mut covered = RoaringBitmap::new();
        for (ordinal, bitmap) in serialized_bitmaps.iter().enumerate() {
            let posting = deserialize_bitmap_exact(bitmap)?;
            bitmap_cardinalities.push(posting.len());
            if !covered.is_disjoint(&posting) {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: row occurs in multiple postings",
                ));
            }
            if let Some(row_ordinals) = row_ordinals.as_mut() {
                for row_id in posting.iter() {
                    let required = row_id as usize + 1;
                    if row_ordinals.len() < required {
                        row_ordinals.resize(required, u16::MAX);
                    }
                    row_ordinals[row_id as usize] = ordinal as u16;
                }
            }
            covered |= &posting;
            bitmaps.push(Arc::new(posting));
        }
        let null_posting = serialized_null_bitmap
            .as_ref()
            .map(deserialize_bitmap_exact)
            .transpose()?;
        let null_cardinality = null_posting.as_ref().map_or(0, RoaringBitmap::len);
        if let Some(posting) = null_posting.as_ref() {
            if !covered.is_disjoint(posting) {
                return Err(paro_error::data_corrupted(
                    "BitmapIndexReader: row occurs in multiple postings",
                ));
            }
            if let Some(row_ordinals) = row_ordinals.as_mut() {
                if let Some(last) = posting.max() {
                    let required = last as usize + 1;
                    if row_ordinals.len() < required {
                        row_ordinals.resize(required, u16::MAX);
                    }
                }
            }
            covered |= posting;
        }
        let row_count = covered.max().map_or(0, |last| last.saturating_add(1));
        if covered.len() != u64::from(row_count) {
            return Err(paro_error::data_corrupted(format!(
                "BitmapIndexReader: postings do not cover a dense segment-local row domain: {} rows through row id {}",
                covered.len(),
                row_count.saturating_sub(1)
            )));
        }
        if let Some(row_ordinals) = row_ordinals.as_mut() {
            row_ordinals.resize(row_count as usize, u16::MAX);
        }

        Ok(BitmapIndexReader {
            row_count,
            bitmap_type,
            has_null,
            dictionary,
            bitmaps,
            null_bitmap: null_posting.map(Arc::new),
            row_ordinals: row_ordinals.map(Arc::from),
            bitmap_cardinalities,
            null_cardinality,
        })
    }

    /// Number of segment-local rows covered by this artifact.
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Get the number of dictionary entries.
    pub fn num_values(&self) -> usize {
        self.dictionary.len()
    }

    /// Check if the index has null bitmap.
    pub fn has_null(&self) -> bool {
        self.has_null
    }

    /// Get the bitmap type.
    pub fn bitmap_type(&self) -> BitmapType {
        self.bitmap_type
    }

    /// Create a new iterator.
    pub fn new_iterator(&self) -> BitmapIndexIterator<'_> {
        BitmapIndexIterator {
            reader: self,
            current_ordinal: 0,
        }
    }

    /// Get dictionary value at ordinal.
    pub fn get_dict_value(&self, ordinal: usize) -> Option<&Bytes> {
        self.dictionary.get(ordinal)
    }

    pub(crate) fn row_ordinals(&self) -> Option<Arc<[u16]>> {
        self.row_ordinals.clone()
    }

    pub(crate) fn bitmap_cardinality(&self, ordinal: usize) -> Option<u64> {
        self.bitmap_cardinalities.get(ordinal).copied()
    }

    pub(crate) fn null_cardinality(&self) -> u64 {
        self.null_cardinality
    }

    pub(crate) fn bitmap(&self, ordinal: usize) -> Option<Arc<RoaringBitmap>> {
        self.bitmaps.get(ordinal).cloned()
    }

    pub(crate) fn null_bitmap(&self) -> Option<Arc<RoaringBitmap>> {
        self.null_bitmap.clone()
    }

    /// Read bitmap at ordinal.
    pub fn read_bitmap(&self, ordinal: usize) -> Result<RoaringBitmap> {
        let bitmap = self.bitmaps.get(ordinal).ok_or_else(|| {
            paro_error::out_of_range(format!(
                "BitmapIndexReader: ordinal {} out of range",
                ordinal
            ))
        })?;
        Ok(bitmap.as_ref().clone())
    }

    /// Read null bitmap.
    pub fn read_null_bitmap(&self) -> Result<RoaringBitmap> {
        match &self.null_bitmap {
            Some(bitmap) => Ok(bitmap.as_ref().clone()),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Binary search for a value in the dictionary.
    ///
    /// Returns (ordinal, exact_match).
    pub fn seek_dictionary(&self, value: &[u8]) -> (usize, bool) {
        match self.dictionary.binary_search_by(|v| v.as_ref().cmp(value)) {
            Ok(idx) => (idx, true),
            Err(idx) => (idx, false),
        }
    }

    /// Get rows matching a specific value.
    pub fn get_rows_for_value(&self, value: &[u8]) -> Result<RoaringBitmap> {
        let (ordinal, exact) = self.seek_dictionary(value);
        if exact {
            self.read_bitmap(ordinal)
        } else {
            Ok(RoaringBitmap::new())
        }
    }

    /// Get rows matching any value in a range [min, max].
    pub fn get_rows_in_range(&self, min: &[u8], max: &[u8]) -> Result<RoaringBitmap> {
        let (start, _) = self.seek_dictionary(min);
        let (end, exact_end) = self.seek_dictionary(max);
        let end = if exact_end { end + 1 } else { end };

        let mut result = RoaringBitmap::new();
        for ordinal in start..end {
            let bitmap = self.read_bitmap(ordinal)?;
            result |= bitmap;
        }
        Ok(result)
    }

    /// Calculate memory usage.
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.dictionary.iter().map(|v| v.len()).sum::<usize>()
            + self
                .bitmaps
                .iter()
                .map(|bitmap| bitmap.serialized_size())
                .sum::<usize>()
            + self
                .null_bitmap
                .as_ref()
                .map_or(0, |bitmap| bitmap.serialized_size())
            + self
                .row_ordinals
                .as_ref()
                .map_or(0, |ordinals| ordinals.len() * std::mem::size_of::<u16>())
            + self.bitmap_cardinalities.len() * std::mem::size_of::<u64>()
    }
}

fn deserialize_bitmap_exact(data: &Bytes) -> Result<RoaringBitmap> {
    let mut cursor = Cursor::new(data.as_ref());
    let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|error| {
        paro_error::data_corrupted(format!("Failed to deserialize bitmap: {error}"))
    })?;
    if cursor.position() != data.len() as u64 {
        return Err(paro_error::data_corrupted(format!(
            "BitmapIndexReader: {} trailing bytes in posting",
            data.len() as u64 - cursor.position()
        )));
    }
    Ok(bitmap)
}

/// Iterator over bitmap index.
#[derive(Debug)]
pub struct BitmapIndexIterator<'a> {
    reader: &'a BitmapIndexReader,
    current_ordinal: usize,
}

impl<'a> BitmapIndexIterator<'a> {
    /// Seek to the first value >= the given value.
    ///
    /// Returns true if exact match found.
    pub fn seek_dictionary(&mut self, value: &[u8]) -> bool {
        let (ordinal, exact) = self.reader.seek_dictionary(value);
        self.current_ordinal = ordinal;
        exact
    }

    /// Get the current ordinal.
    pub fn current_ordinal(&self) -> usize {
        self.current_ordinal
    }

    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.current_ordinal < self.reader.num_values()
    }

    /// Get the current dictionary value.
    pub fn current_value(&self) -> Option<&Bytes> {
        self.reader.get_dict_value(self.current_ordinal)
    }

    /// Read the bitmap at the current position.
    pub fn read_bitmap(&self) -> Result<RoaringBitmap> {
        self.reader.read_bitmap(self.current_ordinal)
    }

    /// Move to the next entry.
    pub fn next(&mut self) {
        self.current_ordinal += 1;
    }

    /// Read bitmap at a specific ordinal.
    pub fn read_bitmap_at(&self, ordinal: usize) -> Result<RoaringBitmap> {
        self.reader.read_bitmap(ordinal)
    }

    /// Read and union bitmaps in range [from, to).
    pub fn read_union_bitmap(&self, from: usize, to: usize) -> Result<RoaringBitmap> {
        let mut result = RoaringBitmap::new();
        for ordinal in from..to {
            let bitmap = self.reader.read_bitmap(ordinal)?;
            result |= bitmap;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_index_roundtrip() {
        let mut writer = BitmapIndexWriter::new();

        // Row 0: "apple"
        writer.add_value(b"apple");
        // Row 1: "banana"
        writer.add_value(b"banana");
        // Row 2: "apple"
        writer.add_value(b"apple");
        // Row 3: null
        writer.add_nulls(1);
        // Row 4: "cherry"
        writer.add_value(b"cherry");
        // Row 5: "apple"
        writer.add_value(b"apple");

        let data = writer.finish().unwrap();
        let reader = BitmapIndexReader::from_bytes(&data).unwrap();

        assert_eq!(reader.row_count(), 6);
        assert_eq!(reader.num_values(), 3); // apple, banana, cherry
        assert!(reader.has_null());

        // Check apple bitmap (rows 0, 2, 5)
        let apple_bitmap = reader.get_rows_for_value(b"apple").unwrap();
        assert!(apple_bitmap.contains(0));
        assert!(!apple_bitmap.contains(1));
        assert!(apple_bitmap.contains(2));
        assert!(!apple_bitmap.contains(3));
        assert!(!apple_bitmap.contains(4));
        assert!(apple_bitmap.contains(5));

        // Check banana bitmap (row 1)
        let banana_bitmap = reader.get_rows_for_value(b"banana").unwrap();
        assert!(!banana_bitmap.contains(0));
        assert!(banana_bitmap.contains(1));

        // Check null bitmap (row 3)
        let null_bitmap = reader.read_null_bitmap().unwrap();
        assert!(null_bitmap.contains(3));
        assert!(!null_bitmap.contains(0));
    }

    #[test]
    fn test_bitmap_index_seek() {
        let mut writer = BitmapIndexWriter::new();

        writer.add_value(b"apple");
        writer.add_value(b"cherry");
        writer.add_value(b"grape");

        let data = writer.finish().unwrap();
        let reader = BitmapIndexReader::from_bytes(&data).unwrap();

        // Exact match
        let (ordinal, exact) = reader.seek_dictionary(b"cherry");
        assert!(exact);
        assert_eq!(ordinal, 1);

        // Not found - between values
        let (ordinal, exact) = reader.seek_dictionary(b"banana");
        assert!(!exact);
        assert_eq!(ordinal, 1); // Would be inserted at position 1

        // Not found - before all
        let (ordinal, exact) = reader.seek_dictionary(b"aaa");
        assert!(!exact);
        assert_eq!(ordinal, 0);

        // Not found - after all
        let (ordinal, exact) = reader.seek_dictionary(b"zzz");
        assert!(!exact);
        assert_eq!(ordinal, 3);
    }

    #[test]
    fn test_bitmap_index_range_query() {
        let mut writer = BitmapIndexWriter::new();

        // Create values: a=0, b=1, c=2, d=3, e=4
        writer.add_value(b"a");
        writer.add_value(b"b");
        writer.add_value(b"c");
        writer.add_value(b"d");
        writer.add_value(b"e");

        let data = writer.finish().unwrap();
        let reader = BitmapIndexReader::from_bytes(&data).unwrap();

        // Range [b, d] should include rows 1, 2, 3
        let bitmap = reader.get_rows_in_range(b"b", b"d").unwrap();
        assert!(!bitmap.contains(0)); // a
        assert!(bitmap.contains(1)); // b
        assert!(bitmap.contains(2)); // c
        assert!(bitmap.contains(3)); // d
        assert!(!bitmap.contains(4)); // e
    }

    #[test]
    fn test_bitmap_index_iterator() {
        let mut writer = BitmapIndexWriter::new();

        writer.add_value(b"x");
        writer.add_value(b"y");
        writer.add_value(b"z");

        let data = writer.finish().unwrap();
        let reader = BitmapIndexReader::from_bytes(&data).unwrap();

        let mut iter = reader.new_iterator();

        // Seek to "y"
        let exact = iter.seek_dictionary(b"y");
        assert!(exact);
        assert_eq!(iter.current_ordinal(), 1);
        assert_eq!(iter.current_value().unwrap().as_ref(), b"y");

        // Read bitmap
        let bitmap = iter.read_bitmap().unwrap();
        assert!(bitmap.contains(1));

        // Move to next
        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.current_value().unwrap().as_ref(), b"z");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn reader_rejects_a_hole_in_the_segment_local_row_domain() {
        let mut posting_bytes = Vec::new();
        RoaringBitmap::from_iter([1])
            .serialize_into(&mut posting_bytes)
            .unwrap();
        let mut corrupted = BytesMut::new();
        corrupted.put_u8(BitmapType::Roaring as u8);
        corrupted.put_u8(0);
        corrupted.put_u32_le(1);
        corrupted.put_u32_le(1);
        corrupted.put_u8(b'a');
        corrupted.put_u32_le(posting_bytes.len() as u32);
        corrupted.extend_from_slice(&posting_bytes);

        let error = BitmapIndexReader::from_bytes(&corrupted.freeze())
            .expect_err("missing row zero must fail");
        assert!(error
            .to_string()
            .contains("do not cover a dense segment-local row domain"));
    }

    #[test]
    fn reader_rejects_envelope_trailing_bytes() {
        let mut writer = BitmapIndexWriter::new();
        writer.add_value(b"a");
        let mut corrupted = writer.finish().unwrap().to_vec();
        corrupted.push(0);

        assert!(BitmapIndexReader::from_bytes(&Bytes::from(corrupted)).is_err());
    }
}
