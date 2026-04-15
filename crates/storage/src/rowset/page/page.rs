// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Page Structure
//!
//! Core page types and footer definitions for the Segment V2 format.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

/// Page type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum PageType {
    #[default]
    Unknown = 0,
    /// Data page containing column values
    Data = 1,
    /// Index page (B-tree node)
    Index = 2,
    /// Dictionary page for dictionary encoding
    Dictionary = 3,
    /// Short key index page
    ShortKey = 4,
}

impl PageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(PageType::Unknown),
            1 => Some(PageType::Data),
            2 => Some(PageType::Index),
            3 => Some(PageType::Dictionary),
            4 => Some(PageType::ShortKey),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Null encoding type for data pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum NullEncoding {
    #[default]
    BitShuffle = 0,
    Lz4 = 1,
    Rle = 2,
}

impl NullEncoding {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(NullEncoding::BitShuffle),
            1 => Some(NullEncoding::Lz4),
            2 => Some(NullEncoding::Rle),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Index page type (leaf or internal node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum IndexPageType {
    #[default]
    Unknown = 0,
    Leaf = 1,
    Internal = 2,
}

impl IndexPageType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(IndexPageType::Unknown),
            1 => Some(IndexPageType::Leaf),
            2 => Some(IndexPageType::Internal),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Data page footer containing metadata for data pages.
#[derive(Debug, Clone, Default)]
pub struct DataPageFooter {
    /// Ordinal of the first value in this page
    pub first_ordinal: u64,
    /// Number of values (including NULLs)
    pub num_values: u64,
    /// Size of null bitmap (0 if no NULLs)
    pub nullmap_size: u32,
    /// For array columns: corresponding element ordinal
    pub corresponding_element_ordinal: Option<u64>,
    /// Format version (1 or 2)
    /// - Version 1: No default value for NULL, RLE null encoding
    /// - Version 2: Default value for NULL, BitShuffle null encoding
    pub format_version: u32,
    /// Null encoding type
    pub null_encoding: NullEncoding,
}

impl DataPageFooter {
    /// Serialized size in bytes
    pub fn serialized_size(&self) -> usize {
        let mut size = 8 + 8 + 4 + 4 + 1; // first_ordinal + num_values + nullmap_size + format_version + null_encoding
        if self.corresponding_element_ordinal.is_some() {
            size += 1 + 8; // has_flag + value
        } else {
            size += 1; // has_flag only
        }
        size
    }

    /// Serialize to bytes
    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.first_ordinal);
        buf.put_u64_le(self.num_values);
        buf.put_u32_le(self.nullmap_size);
        if let Some(ordinal) = self.corresponding_element_ordinal {
            buf.put_u8(1);
            buf.put_u64_le(ordinal);
        } else {
            buf.put_u8(0);
        }
        buf.put_u32_le(self.format_version);
        buf.put_u8(self.null_encoding.to_u8());
    }

    /// Deserialize from bytes
    pub fn deserialize(buf: &mut impl Buf) -> io::Result<Self> {
        if buf.remaining() < 8 + 8 + 4 + 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "DataPageFooter: insufficient data",
            ));
        }
        let first_ordinal = buf.get_u64_le();
        let num_values = buf.get_u64_le();
        let nullmap_size = buf.get_u32_le();

        let has_element_ordinal = buf.get_u8();
        let corresponding_element_ordinal = if has_element_ordinal != 0 {
            if buf.remaining() < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "DataPageFooter: missing element ordinal",
                ));
            }
            Some(buf.get_u64_le())
        } else {
            None
        };

        if buf.remaining() < 4 + 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "DataPageFooter: missing format version",
            ));
        }
        let format_version = buf.get_u32_le();
        let null_encoding = NullEncoding::from_u8(buf.get_u8()).unwrap_or_default();

        Ok(DataPageFooter {
            first_ordinal,
            num_values,
            nullmap_size,
            corresponding_element_ordinal,
            format_version,
            null_encoding,
        })
    }
}

/// Index page footer.
#[derive(Debug, Clone, Default)]
pub struct IndexPageFooter {
    /// Number of index entries
    pub num_entries: u32,
    /// Index page type (leaf or internal)
    pub page_type: IndexPageType,
}

impl IndexPageFooter {
    pub fn serialized_size(&self) -> usize {
        4 + 1 // num_entries + page_type
    }

    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.num_entries);
        buf.put_u8(self.page_type.to_u8());
    }

    pub fn deserialize(buf: &mut impl Buf) -> io::Result<Self> {
        if buf.remaining() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "IndexPageFooter: insufficient data",
            ));
        }
        Ok(IndexPageFooter {
            num_entries: buf.get_u32_le(),
            page_type: IndexPageType::from_u8(buf.get_u8()).unwrap_or_default(),
        })
    }
}

/// Dictionary page footer.
#[derive(Debug, Clone, Default)]
pub struct DictPageFooter {
    /// Encoding type for dictionary values
    pub encoding: u8,
}

impl DictPageFooter {
    pub fn serialized_size(&self) -> usize {
        1
    }

    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u8(self.encoding);
    }

    pub fn deserialize(buf: &mut impl Buf) -> io::Result<Self> {
        if buf.remaining() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "DictPageFooter: insufficient data",
            ));
        }
        Ok(DictPageFooter {
            encoding: buf.get_u8(),
        })
    }
}

/// Short key page footer.
#[derive(Debug, Clone, Default)]
pub struct ShortKeyFooter {
    /// Number of index items
    pub num_items: u32,
    /// Total bytes for keys
    pub key_bytes: u32,
    /// Total bytes for offsets
    pub offset_bytes: u32,
    /// Segment ID
    pub segment_id: u32,
    /// Rows per block
    pub num_rows_per_block: u32,
    /// Total rows in segment
    pub num_segment_rows: u32,
}

impl ShortKeyFooter {
    pub fn serialized_size(&self) -> usize {
        4 * 6 // 6 u32 fields
    }

    pub fn serialize(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.num_items);
        buf.put_u32_le(self.key_bytes);
        buf.put_u32_le(self.offset_bytes);
        buf.put_u32_le(self.segment_id);
        buf.put_u32_le(self.num_rows_per_block);
        buf.put_u32_le(self.num_segment_rows);
    }

    pub fn deserialize(buf: &mut impl Buf) -> io::Result<Self> {
        if buf.remaining() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "ShortKeyFooter: insufficient data",
            ));
        }
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

/// Page footer containing type and type-specific metadata.
#[derive(Debug, Clone)]
pub enum PageFooter {
    Data(DataPageFooter),
    Index(IndexPageFooter),
    Dict(DictPageFooter),
    ShortKey(ShortKeyFooter),
}

impl PageFooter {
    /// Get the page type
    pub fn page_type(&self) -> PageType {
        match self {
            PageFooter::Data(_) => PageType::Data,
            PageFooter::Index(_) => PageType::Index,
            PageFooter::Dict(_) => PageType::Dictionary,
            PageFooter::ShortKey(_) => PageType::ShortKey,
        }
    }

    /// Serialized size in bytes (excluding type tag and uncompressed_size)
    fn type_specific_size(&self) -> usize {
        match self {
            PageFooter::Data(f) => f.serialized_size(),
            PageFooter::Index(f) => f.serialized_size(),
            PageFooter::Dict(f) => f.serialized_size(),
            PageFooter::ShortKey(f) => f.serialized_size(),
        }
    }

    /// Total serialized size: type(1) + uncompressed_size(4) + type_specific
    pub fn serialized_size(&self) -> usize {
        1 + 4 + self.type_specific_size()
    }

    /// Serialize footer to bytes
    pub fn serialize(&self, uncompressed_size: u32) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.serialized_size());
        buf.put_u8(self.page_type().to_u8());
        buf.put_u32_le(uncompressed_size);
        match self {
            PageFooter::Data(f) => f.serialize(&mut buf),
            PageFooter::Index(f) => f.serialize(&mut buf),
            PageFooter::Dict(f) => f.serialize(&mut buf),
            PageFooter::ShortKey(f) => f.serialize(&mut buf),
        }
        buf.freeze()
    }

    /// Deserialize footer from bytes
    pub fn deserialize(data: &[u8]) -> io::Result<(Self, u32)> {
        let mut buf = data;
        if buf.remaining() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "PageFooter: insufficient data for header",
            ));
        }

        let page_type = PageType::from_u8(buf.get_u8()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "PageFooter: invalid page type")
        })?;
        let uncompressed_size = buf.get_u32_le();

        let footer = match page_type {
            PageType::Data => PageFooter::Data(DataPageFooter::deserialize(&mut buf)?),
            PageType::Index => PageFooter::Index(IndexPageFooter::deserialize(&mut buf)?),
            PageType::Dictionary => PageFooter::Dict(DictPageFooter::deserialize(&mut buf)?),
            PageType::ShortKey => PageFooter::ShortKey(ShortKeyFooter::deserialize(&mut buf)?),
            PageType::Unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "PageFooter: unknown page type",
                ))
            }
        };

        Ok((footer, uncompressed_size))
    }

    /// Get data page footer if this is a data page
    pub fn as_data(&self) -> Option<&DataPageFooter> {
        match self {
            PageFooter::Data(f) => Some(f),
            _ => None,
        }
    }

    /// Get index page footer if this is an index page
    pub fn as_index(&self) -> Option<&IndexPageFooter> {
        match self {
            PageFooter::Index(f) => Some(f),
            _ => None,
        }
    }

    /// Get dict page footer if this is a dictionary page
    pub fn as_dict(&self) -> Option<&DictPageFooter> {
        match self {
            PageFooter::Dict(f) => Some(f),
            _ => None,
        }
    }

    /// Get short key footer if this is a short key page
    pub fn as_short_key(&self) -> Option<&ShortKeyFooter> {
        match self {
            PageFooter::ShortKey(f) => Some(f),
            _ => None,
        }
    }
}

impl Default for PageFooter {
    fn default() -> Self {
        PageFooter::Data(DataPageFooter::default())
    }
}

/// Page pointer: offset and size in file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PagePointer {
    /// Byte offset in file
    pub offset: u64,
    /// Total page size in bytes
    pub size: u32,
}

impl PagePointer {
    pub fn new(offset: u64, size: u32) -> Self {
        PagePointer { offset, size }
    }

    /// Check if this is a valid (non-empty) pointer
    pub fn is_valid(&self) -> bool {
        self.size > 0
    }

    /// Encode to variable-length bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        // Use varint encoding for compactness
        encode_varint(buf, self.offset);
        encode_varint(buf, self.size as u64);
    }

    /// Decode from variable-length bytes
    pub fn decode(buf: &mut impl Buf) -> io::Result<Self> {
        let offset = decode_varint(buf)?;
        let size = decode_varint(buf)? as u32;
        Ok(PagePointer { offset, size })
    }

    /// Encode to fixed-size bytes (for simpler cases)
    pub fn encode_fixed(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.offset);
        buf.put_u32_le(self.size);
    }

    /// Decode from fixed-size bytes
    pub fn decode_fixed(buf: &mut impl Buf) -> io::Result<Self> {
        if buf.remaining() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "PagePointer: insufficient data",
            ));
        }
        Ok(PagePointer {
            offset: buf.get_u64_le(),
            size: buf.get_u32_le(),
        })
    }
}

/// A page with its body and footer.
#[derive(Debug, Clone)]
pub struct Page {
    /// Page body (may be compressed)
    pub body: Bytes,
    /// Page footer
    pub footer: PageFooter,
    /// Uncompressed body size
    pub uncompressed_size: u32,
}

impl Page {
    pub fn new(body: Bytes, footer: PageFooter, uncompressed_size: u32) -> Self {
        Page {
            body,
            footer,
            uncompressed_size,
        }
    }

    /// Check if the body is compressed
    pub fn is_compressed(&self) -> bool {
        self.body.len() != self.uncompressed_size as usize
    }
}

// Helper functions for varint encoding/decoding

fn encode_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

fn decode_varint(buf: &mut impl Buf) -> io::Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        if buf.remaining() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "varint: unexpected end of data",
            ));
        }
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;
        if byte < 0x80 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint: overflow",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_type_roundtrip() {
        for i in 0..=4 {
            if let Some(pt) = PageType::from_u8(i) {
                assert_eq!(pt.to_u8(), i);
            }
        }
    }

    #[test]
    fn test_data_page_footer_serialize() {
        let footer = DataPageFooter {
            first_ordinal: 100,
            num_values: 1000,
            nullmap_size: 128,
            corresponding_element_ordinal: Some(500),
            format_version: 2,
            null_encoding: NullEncoding::BitShuffle,
        };

        let mut buf = BytesMut::new();
        footer.serialize(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = DataPageFooter::deserialize(&mut read_buf).unwrap();

        assert_eq!(decoded.first_ordinal, 100);
        assert_eq!(decoded.num_values, 1000);
        assert_eq!(decoded.nullmap_size, 128);
        assert_eq!(decoded.corresponding_element_ordinal, Some(500));
        assert_eq!(decoded.format_version, 2);
        assert_eq!(decoded.null_encoding, NullEncoding::BitShuffle);
    }

    #[test]
    fn test_page_footer_serialize() {
        let data_footer = DataPageFooter {
            first_ordinal: 0,
            num_values: 100,
            nullmap_size: 0,
            corresponding_element_ordinal: None,
            format_version: 2,
            null_encoding: NullEncoding::BitShuffle,
        };
        let footer = PageFooter::Data(data_footer);
        let serialized = footer.serialize(4096);

        let (decoded, uncompressed_size) = PageFooter::deserialize(&serialized).unwrap();
        assert_eq!(uncompressed_size, 4096);
        assert!(matches!(decoded, PageFooter::Data(_)));
    }

    #[test]
    fn test_page_pointer_varint() {
        let ptr = PagePointer::new(12345678, 65536);
        let mut buf = BytesMut::new();
        ptr.encode(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = PagePointer::decode(&mut read_buf).unwrap();

        assert_eq!(decoded.offset, 12345678);
        assert_eq!(decoded.size, 65536);
    }

    #[test]
    fn test_page_pointer_fixed() {
        let ptr = PagePointer::new(0xDEADBEEF, 0x1234);
        let mut buf = BytesMut::new();
        ptr.encode_fixed(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = PagePointer::decode_fixed(&mut read_buf).unwrap();

        assert_eq!(decoded, ptr);
    }
}
