//! # Short Key Index Implementation
//!
//! Short key index for fast row block location using binary search.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use std::cmp::Ordering;

/// Minimal value marker for a field.
pub const KEY_MINIMAL_MARKER: u8 = 0x00;
/// Null value marker (sorted first).
pub const KEY_NULL_FIRST_MARKER: u8 = 0x01;
/// Normal non-null value marker.
pub const KEY_NORMAL_MARKER: u8 = 0x02;
/// Null value marker (sorted last).
pub const KEY_NULL_LAST_MARKER: u8 = 0xFE;
/// Maximal value marker for a field.
pub const KEY_MAXIMAL_MARKER: u8 = 0xFF;

/// Short key index footer.
#[derive(Debug, Clone, Default)]
pub struct ShortKeyFooter {
    /// Number of index items
    pub num_items: u32,
    /// Total bytes occupied by keys
    pub key_bytes: u32,
    /// Total bytes occupied by offsets
    pub offset_bytes: u32,
    /// Segment ID
    pub segment_id: u32,
    /// Number of rows per block
    pub num_rows_per_block: u32,
    /// Total rows in segment
    pub num_segment_rows: u32,
}

impl ShortKeyFooter {
    /// Serialize the footer.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(24);
        buf.put_u32_le(self.num_items);
        buf.put_u32_le(self.key_bytes);
        buf.put_u32_le(self.offset_bytes);
        buf.put_u32_le(self.segment_id);
        buf.put_u32_le(self.num_rows_per_block);
        buf.put_u32_le(self.num_segment_rows);
        buf.freeze()
    }

    /// Deserialize the footer.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 24 {
            return Err(paro_error::data_corrupted("ShortKeyFooter: data too small"));
        }

        let mut buf = data;
        Ok(ShortKeyFooter {
            num_items: buf.get_u32_le(),
            key_bytes: buf.get_u32_le(),
            offset_bytes: buf.get_u32_le(),
            segment_id: buf.get_u32_le(),
            num_rows_per_block: buf.get_u32_le(),
            num_segment_rows: buf.get_u32_le(),
        })
    }
}

/// Short key index builder.
///
/// Builds a short key index page with the following format:
/// ```text
/// Body := KeyContent^NumEntry, KeyOffset(u32)^NumEntry
/// ```
#[derive(Debug)]
pub struct ShortKeyIndexBuilder {
    /// Segment ID
    segment_id: u32,
    /// Number of rows per block
    num_rows_per_block: u32,
    /// Number of items added
    num_items: u32,
    /// Key data buffer
    key_buf: BytesMut,
    /// Offset buffer (u32 per entry)
    offset_buf: Vec<u32>,
}

impl ShortKeyIndexBuilder {
    /// Create a new short key index builder.
    pub fn new(segment_id: u32, num_rows_per_block: u32) -> Self {
        ShortKeyIndexBuilder {
            segment_id,
            num_rows_per_block,
            num_items: 0,
            key_buf: BytesMut::new(),
            offset_buf: Vec::new(),
        }
    }

    /// Add a key to the index.
    pub fn add_item(&mut self, key: &[u8]) -> Result<()> {
        // Record offset before adding key
        self.offset_buf.push(self.key_buf.len() as u32);
        // Add key data
        self.key_buf.extend_from_slice(key);
        self.num_items += 1;
        Ok(())
    }

    /// Get the current size in bytes.
    pub fn size(&self) -> usize {
        self.key_buf.len() + self.offset_buf.len() * 4
    }

    /// Finalize the index and return body and footer.
    pub fn finalize(&self, num_rows: u32) -> Result<(Bytes, ShortKeyFooter)> {
        // Build body: keys followed by offsets
        let mut body = BytesMut::with_capacity(self.key_buf.len() + self.offset_buf.len() * 4);
        body.extend_from_slice(&self.key_buf);
        for &offset in &self.offset_buf {
            body.put_u32_le(offset);
        }

        let footer = ShortKeyFooter {
            num_items: self.num_items,
            key_bytes: self.key_buf.len() as u32,
            offset_bytes: (self.offset_buf.len() * 4) as u32,
            segment_id: self.segment_id,
            num_rows_per_block: self.num_rows_per_block,
            num_segment_rows: num_rows,
        };

        Ok((body.freeze(), footer))
    }

    /// Get the number of items.
    pub fn num_items(&self) -> u32 {
        self.num_items
    }
}

/// Short key index decoder.
#[derive(Debug, Clone)]
pub struct ShortKeyIndexDecoder {
    /// Footer metadata
    footer: ShortKeyFooter,
    /// Key data
    key_data: Bytes,
    /// Offsets (one per key, plus end offset)
    offsets: Vec<u32>,
}

impl ShortKeyIndexDecoder {
    /// Parse the index from body and footer.
    pub fn parse(body: &Bytes, footer: &ShortKeyFooter) -> Result<Self> {
        let key_bytes = footer.key_bytes as usize;
        let num_items = footer.num_items as usize;

        if body.len() < key_bytes + num_items * 4 {
            return Err(paro_error::data_corrupted(
                "ShortKeyIndexDecoder: body too small",
            ));
        }

        // Extract key data
        let key_data = body.slice(0..key_bytes);

        // Extract offsets
        let mut offset_buf = &body[key_bytes..];
        let mut offsets = Vec::with_capacity(num_items + 1);
        for _ in 0..num_items {
            offsets.push(offset_buf.get_u32_le());
        }
        // Add end offset
        offsets.push(key_bytes as u32);

        Ok(ShortKeyIndexDecoder {
            footer: footer.clone(),
            key_data,
            offsets,
        })
    }

    /// Get an iterator at the beginning.
    pub fn begin(&self) -> ShortKeyIndexIterator<'_> {
        ShortKeyIndexIterator {
            decoder: self,
            ordinal: 0,
        }
    }

    /// Get an iterator at the end.
    pub fn end(&self) -> ShortKeyIndexIterator<'_> {
        ShortKeyIndexIterator {
            decoder: self,
            ordinal: self.num_items() as isize,
        }
    }

    /// Find the first key >= the given key.
    pub fn lower_bound(&self, key: &[u8]) -> ShortKeyIndexIterator<'_> {
        let mut left = 0isize;
        let mut right = self.num_items() as isize;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.key(mid as usize);
            if mid_key.cmp(key) == Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        ShortKeyIndexIterator {
            decoder: self,
            ordinal: left,
        }
    }

    /// Find the first key > the given key.
    pub fn upper_bound(&self, key: &[u8]) -> ShortKeyIndexIterator<'_> {
        let mut left = 0isize;
        let mut right = self.num_items() as isize;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = self.key(mid as usize);
            if mid_key.cmp(key) != Ordering::Greater {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        ShortKeyIndexIterator {
            decoder: self,
            ordinal: left,
        }
    }

    /// Get the number of items.
    pub fn num_items(&self) -> u32 {
        self.footer.num_items
    }

    /// Get the number of rows per block.
    pub fn num_rows_per_block(&self) -> u32 {
        self.footer.num_rows_per_block
    }

    /// Get the key at the given ordinal.
    pub fn key(&self, ordinal: usize) -> &[u8] {
        if ordinal >= self.offsets.len() - 1 {
            return &[];
        }
        let start = self.offsets[ordinal] as usize;
        let end = self.offsets[ordinal + 1] as usize;
        &self.key_data[start..end]
    }

    /// Get the footer.
    pub fn footer(&self) -> &ShortKeyFooter {
        &self.footer
    }

    /// Calculate memory usage.
    pub fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.key_data.len()
            + self.offsets.len() * std::mem::size_of::<u32>()
    }
}

/// Iterator over short key index entries.
#[derive(Debug, Clone)]
pub struct ShortKeyIndexIterator<'a> {
    decoder: &'a ShortKeyIndexDecoder,
    ordinal: isize,
}

impl<'a> ShortKeyIndexIterator<'a> {
    /// Check if the iterator is valid.
    pub fn valid(&self) -> bool {
        self.ordinal >= 0 && self.ordinal < self.decoder.num_items() as isize
    }

    /// Get the current ordinal.
    pub fn ordinal(&self) -> isize {
        self.ordinal
    }

    /// Get the current key.
    pub fn key(&self) -> &[u8] {
        if self.valid() {
            self.decoder.key(self.ordinal as usize)
        } else {
            &[]
        }
    }

    /// Move to the next entry.
    pub fn next(&mut self) {
        self.ordinal += 1;
    }

    /// Move to the previous entry.
    pub fn prev(&mut self) {
        self.ordinal -= 1;
    }

    /// Move forward by n entries.
    pub fn advance(&mut self, n: isize) {
        self.ordinal += n;
    }
}

impl<'a> PartialEq for ShortKeyIndexIterator<'a> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.decoder, other.decoder) && self.ordinal == other.ordinal
    }
}

impl<'a> Eq for ShortKeyIndexIterator<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_key_index_roundtrip() {
        let mut builder = ShortKeyIndexBuilder::new(0, 1024);

        builder.add_item(b"apple").unwrap();
        builder.add_item(b"banana").unwrap();
        builder.add_item(b"cherry").unwrap();
        builder.add_item(b"date").unwrap();

        let (body, footer) = builder.finalize(4096).unwrap();

        let decoder = ShortKeyIndexDecoder::parse(&body, &footer).unwrap();

        assert_eq!(decoder.num_items(), 4);
        assert_eq!(decoder.key(0), b"apple");
        assert_eq!(decoder.key(1), b"banana");
        assert_eq!(decoder.key(2), b"cherry");
        assert_eq!(decoder.key(3), b"date");
    }

    #[test]
    fn test_short_key_index_lower_bound() {
        let mut builder = ShortKeyIndexBuilder::new(0, 1024);

        builder.add_item(b"apple").unwrap();
        builder.add_item(b"cherry").unwrap();
        builder.add_item(b"grape").unwrap();

        let (body, footer) = builder.finalize(3072).unwrap();
        let decoder = ShortKeyIndexDecoder::parse(&body, &footer).unwrap();

        // Exact match
        let iter = decoder.lower_bound(b"cherry");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
        assert_eq!(iter.ordinal(), 1);

        // Between values
        let iter = decoder.lower_bound(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
        assert_eq!(iter.ordinal(), 1);

        // Before all values
        let iter = decoder.lower_bound(b"aaa");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"apple");
        assert_eq!(iter.ordinal(), 0);

        // After all values
        let iter = decoder.lower_bound(b"zzz");
        assert!(!iter.valid());
        assert_eq!(iter.ordinal(), 3);
    }

    #[test]
    fn test_short_key_index_upper_bound() {
        let mut builder = ShortKeyIndexBuilder::new(0, 1024);

        builder.add_item(b"apple").unwrap();
        builder.add_item(b"cherry").unwrap();
        builder.add_item(b"grape").unwrap();

        let (body, footer) = builder.finalize(3072).unwrap();
        let decoder = ShortKeyIndexDecoder::parse(&body, &footer).unwrap();

        // Exact match - should return next
        let iter = decoder.upper_bound(b"cherry");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"grape");
        assert_eq!(iter.ordinal(), 2);

        // Between values
        let iter = decoder.upper_bound(b"banana");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"cherry");
        assert_eq!(iter.ordinal(), 1);
    }

    #[test]
    fn test_short_key_index_iterator() {
        let mut builder = ShortKeyIndexBuilder::new(0, 1024);

        builder.add_item(b"a").unwrap();
        builder.add_item(b"b").unwrap();
        builder.add_item(b"c").unwrap();

        let (body, footer) = builder.finalize(3072).unwrap();
        let decoder = ShortKeyIndexDecoder::parse(&body, &footer).unwrap();

        let mut iter = decoder.begin();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"a");

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"b");

        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn test_short_key_footer_roundtrip() {
        let footer = ShortKeyFooter {
            num_items: 100,
            key_bytes: 5000,
            offset_bytes: 400,
            segment_id: 42,
            num_rows_per_block: 1024,
            num_segment_rows: 102400,
        };

        let data = footer.to_bytes();
        let footer2 = ShortKeyFooter::from_bytes(&data).unwrap();

        assert_eq!(footer2.num_items, 100);
        assert_eq!(footer2.key_bytes, 5000);
        assert_eq!(footer2.offset_bytes, 400);
        assert_eq!(footer2.segment_id, 42);
        assert_eq!(footer2.num_rows_per_block, 1024);
        assert_eq!(footer2.num_segment_rows, 102400);
    }
}
