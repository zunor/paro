//! # Page I/O
//!
//! Read and write pages with compression and checksum verification.
//!
//! ## Page Layout
//!
//! ```text
//! +------------------+
//! |    Page Body     |  (encoded data, may be compressed)
//! +------------------+
//! |   Page Footer    |  (serialized PageFooter)
//! +------------------+
//! |   Footer Size    |  (4 bytes, little-endian)
//! +------------------+
//! |    Checksum      |  (4 bytes, CRC32C)
//! +------------------+
//! ```

use super::{Page, PageFooter, PagePointer};
use bytes::{BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use std::io::{Read, Seek, SeekFrom, Write};

// Re-export compression types from the compression module
pub use crate::compression::BlockCompressionCodec;
pub use crate::compression::BlockCompressionType as CompressionType;
pub use crate::compression::Lz4BlockCompression as Lz4Codec;
pub use crate::compression::NoBlockCompression as NoCompressionCodec;
pub use crate::compression::ZstdBlockCompression as ZstdCodec;

/// Default minimum space saving ratio for compression (10%)
pub const DEFAULT_MIN_SPACE_SAVING: f64 = 0.1;

/// Options for reading pages.
#[derive(Debug, Clone)]
pub struct PageReadOptions {
    /// Page location in file
    pub page_pointer: PagePointer,
    /// Whether to verify checksum
    pub verify_checksum: bool,
    /// Compression codec (None means uncompressed)
    pub codec: Option<CompressionType>,
}

impl PageReadOptions {
    pub fn new(page_pointer: PagePointer) -> Self {
        PageReadOptions {
            page_pointer,
            verify_checksum: true,
            codec: None,
        }
    }

    pub fn with_verify_checksum(mut self, verify: bool) -> Self {
        self.verify_checksum = verify;
        self
    }

    pub fn with_codec(mut self, codec: CompressionType) -> Self {
        self.codec = Some(codec);
        self
    }
}

/// Page I/O operations.
pub struct PageIO;

impl PageIO {
    /// Read raw page bytes from file.
    pub fn read_page_bytes<R: Read + Seek>(
        reader: &mut R,
        opts: &PageReadOptions,
    ) -> Result<Vec<u8>> {
        let page_size = opts.page_pointer.size as usize;

        // Minimum page size: footer_size(4) + checksum(4)
        if page_size < 8 {
            return Err(paro_error::data_corrupted(format!(
                "Bad page: too small ({})",
                page_size
            )));
        }

        reader.seek(SeekFrom::Start(opts.page_pointer.offset))?;
        let mut page_data = vec![0u8; page_size];
        reader.read_exact(&mut page_data)?;
        Ok(page_data)
    }

    /// Parse page footer and uncompressed size from raw page bytes.
    ///
    /// Returns (footer, uncompressed_size, body_size).
    pub fn parse_page_footer(
        page_data: &[u8],
        verify_checksum: bool,
    ) -> Result<(PageFooter, u32, usize)> {
        let page_size = page_data.len();

        // Minimum page size: footer_size(4) + checksum(4)
        if page_size < 8 {
            return Err(paro_error::data_corrupted(format!(
                "Bad page: too small ({})",
                page_size
            )));
        }

        // Verify checksum if requested
        if verify_checksum {
            let expected_checksum = u32::from_le_bytes([
                page_data[page_size - 4],
                page_data[page_size - 3],
                page_data[page_size - 2],
                page_data[page_size - 1],
            ]);
            let actual_checksum = crc32c_checksum(&page_data[..page_size - 4]);

            if expected_checksum != actual_checksum {
                return Err(paro_error::data_corrupted(format!(
                    "Bad page: checksum mismatch (actual={} vs expect={})",
                    actual_checksum, expected_checksum
                )));
            }
        }

        // Parse footer size (4 bytes before checksum)
        let footer_size_offset = page_size - 8;
        let footer_size = u32::from_le_bytes([
            page_data[footer_size_offset],
            page_data[footer_size_offset + 1],
            page_data[footer_size_offset + 2],
            page_data[footer_size_offset + 3],
        ]) as usize;

        // Validate footer size
        if footer_size > footer_size_offset {
            return Err(paro_error::data_corrupted(format!(
                "Bad page: invalid footer size ({})",
                footer_size
            )));
        }

        let footer_offset = footer_size_offset - footer_size;
        let footer_data = &page_data[footer_offset..footer_size_offset];
        let (footer, uncompressed_size) = PageFooter::deserialize(footer_data)?;

        Ok((footer, uncompressed_size, footer_offset))
    }

    /// Decompress page body bytes.
    pub fn decompress_page_body(
        body_data: &[u8],
        uncompressed_size: u32,
        codec: Option<CompressionType>,
    ) -> Result<Bytes> {
        let body_size = body_data.len();
        if body_size == uncompressed_size as usize {
            return Ok(Bytes::copy_from_slice(body_data));
        }

        let codec = get_codec(codec.unwrap_or(CompressionType::Lz4));
        let decompressed = codec.decompress(body_data, uncompressed_size as usize)?;

        if decompressed.len() != uncompressed_size as usize {
            return Err(paro_error::data_corrupted(format!(
                "Bad page: uncompressed size mismatch ({} vs {})",
                decompressed.len(),
                uncompressed_size
            )));
        }

        Ok(Bytes::from(decompressed))
    }

    /// Compress page body if beneficial.
    ///
    /// Returns empty Vec if compression is skipped (codec is None or
    /// space saving is less than `min_space_saving`).
    pub fn compress_page_body(
        codec: Option<&dyn BlockCompressionCodec>,
        min_space_saving: f64,
        body: &[u8],
    ) -> Result<Vec<u8>> {
        let Some(codec) = codec else {
            return Ok(Vec::new());
        };

        if body.is_empty() || codec.exceed_max_input_size(body.len()) {
            return Ok(Vec::new());
        }

        let compressed = codec.compress(body)?;
        let space_saving = 1.0 - (compressed.len() as f64 / body.len() as f64);

        if space_saving > 0.0 && space_saving >= min_space_saving {
            Ok(compressed)
        } else {
            Ok(Vec::new())
        }
    }

    /// Write a page to file.
    ///
    /// # Arguments
    /// * `writer` - File writer
    /// * `body` - Page body (may be compressed)
    /// * `footer` - Page footer
    /// * `uncompressed_size` - Original uncompressed body size
    ///
    /// # Returns
    /// PagePointer with offset and size of written page
    pub fn write_page<W: Write + Seek>(
        writer: &mut W,
        body: &[u8],
        footer: &PageFooter,
        uncompressed_size: u32,
    ) -> Result<PagePointer> {
        let offset = writer.stream_position()?;

        // Serialize footer
        let footer_bytes = footer.serialize(uncompressed_size);
        let footer_size = footer_bytes.len() as u32;

        // Build page: body + footer + footer_size
        let mut page_data = BytesMut::with_capacity(body.len() + footer_bytes.len() + 8);
        page_data.extend_from_slice(body);
        page_data.extend_from_slice(&footer_bytes);
        page_data.put_u32_le(footer_size);

        // Calculate CRC32C checksum
        let checksum = crc32c_checksum(&page_data);
        page_data.put_u32_le(checksum);

        // Write to file
        writer.write_all(&page_data)?;

        let size = page_data.len() as u32;
        Ok(PagePointer::new(offset, size))
    }

    /// Compress and write a page in one operation.
    ///
    /// # Arguments
    /// * `codec` - Compression codec (None for no compression)
    /// * `min_space_saving` - Minimum space saving ratio to use compression
    /// * `writer` - File writer
    /// * `body` - Uncompressed page body
    /// * `footer` - Page footer
    ///
    /// # Returns
    /// PagePointer with offset and size of written page
    pub fn compress_and_write_page<W: Write + Seek>(
        codec: Option<&dyn BlockCompressionCodec>,
        min_space_saving: f64,
        writer: &mut W,
        body: &[u8],
        footer: &PageFooter,
    ) -> Result<PagePointer> {
        let uncompressed_size = body.len() as u32;
        let compressed = Self::compress_page_body(codec, min_space_saving, body)?;

        if compressed.is_empty() {
            // Use uncompressed body
            Self::write_page(writer, body, footer, uncompressed_size)
        } else {
            // Use compressed body
            Self::write_page(writer, &compressed, footer, uncompressed_size)
        }
    }

    /// Read and decompress a page from file.
    ///
    /// # Arguments
    /// * `reader` - File reader
    /// * `opts` - Read options including page pointer and codec
    ///
    /// # Returns
    /// Tuple of (page_body, page_footer, uncompressed_size)
    pub fn read_and_decompress_page<R: Read + Seek>(
        reader: &mut R,
        opts: &PageReadOptions,
    ) -> Result<(Bytes, PageFooter, u32)> {
        let page_data = Self::read_page_bytes(reader, opts)?;
        let (footer, uncompressed_size, body_size) =
            Self::parse_page_footer(&page_data, opts.verify_checksum)?;
        let body =
            Self::decompress_page_body(&page_data[..body_size], uncompressed_size, opts.codec)?;
        Ok((body, footer, uncompressed_size))
    }

    /// Read a page and return the full Page struct.
    pub fn read_page<R: Read + Seek>(reader: &mut R, opts: &PageReadOptions) -> Result<Page> {
        let (body, footer, uncompressed_size) = Self::read_and_decompress_page(reader, opts)?;
        Ok(Page::new(body, footer, uncompressed_size))
    }
}

/// Get compression codec by type.
fn get_codec(compression_type: CompressionType) -> Box<dyn BlockCompressionCodec> {
    crate::compression::get_block_compression_codec(compression_type)
}

/// Calculate CRC32C checksum.
fn crc32c_checksum(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rowset::page::{DataPageFooter, NullEncoding};
    use std::io::Cursor;

    fn create_test_footer() -> PageFooter {
        PageFooter::Data(DataPageFooter {
            first_ordinal: 0,
            num_values: 100,
            nullmap_size: 0,
            corresponding_element_ordinal: None,
            format_version: 2,
            null_encoding: NullEncoding::BitShuffle,
        })
    }

    #[test]
    fn test_write_and_read_uncompressed() {
        let body = b"Hello, World! This is test data for page I/O.";
        let footer = create_test_footer();

        let mut buffer = Cursor::new(Vec::new());

        // Write page
        let ptr = PageIO::write_page(&mut buffer, body, &footer, body.len() as u32).unwrap();

        assert!(ptr.offset == 0);
        assert!(ptr.size > body.len() as u32);

        // Read page
        let opts = PageReadOptions::new(ptr);
        let (read_body, read_footer, uncompressed_size) =
            PageIO::read_and_decompress_page(&mut buffer, &opts).unwrap();

        assert_eq!(read_body.as_ref(), body);
        assert_eq!(uncompressed_size, body.len() as u32);
        assert!(matches!(read_footer, PageFooter::Data(_)));
    }

    #[test]
    fn test_compress_and_write_with_lz4() {
        // Create compressible data (repeated pattern)
        let body: Vec<u8> = (0..1000).map(|i| (i % 10) as u8).collect();
        let footer = create_test_footer();

        let mut buffer = Cursor::new(Vec::new());
        let codec = Lz4Codec::new();

        // Write compressed page
        let ptr = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &body,
            &footer,
        )
        .unwrap();

        // Page should be smaller than uncompressed
        assert!(ptr.size < body.len() as u32 + 50); // Allow some overhead

        // Read and verify
        let opts = PageReadOptions::new(ptr).with_codec(CompressionType::Lz4);
        let (read_body, _, uncompressed_size) =
            PageIO::read_and_decompress_page(&mut buffer, &opts).unwrap();

        assert_eq!(read_body.as_ref(), body.as_slice());
        assert_eq!(uncompressed_size, body.len() as u32);
    }

    #[test]
    fn test_compress_and_write_with_zstd() {
        // Create compressible data
        let body: Vec<u8> = (0..2000).map(|i| (i % 20) as u8).collect();
        let footer = create_test_footer();

        let mut buffer = Cursor::new(Vec::new());
        let codec = ZstdCodec::default();

        // Write compressed page
        let ptr = PageIO::compress_and_write_page(
            Some(&codec),
            DEFAULT_MIN_SPACE_SAVING,
            &mut buffer,
            &body,
            &footer,
        )
        .unwrap();

        // Read and verify
        let opts = PageReadOptions::new(ptr).with_codec(CompressionType::Zstd);
        let (read_body, _, _) = PageIO::read_and_decompress_page(&mut buffer, &opts).unwrap();

        assert_eq!(read_body.as_ref(), body.as_slice());
    }

    #[test]
    fn test_skip_compression_when_not_beneficial() {
        // Random data that doesn't compress well
        let body: Vec<u8> = (0..100).map(|i| (i * 17 + 31) as u8).collect();
        let footer = create_test_footer();

        let mut buffer = Cursor::new(Vec::new());
        let codec = Lz4Codec::new();

        // Write with high min_space_saving threshold
        let ptr = PageIO::compress_and_write_page(
            Some(&codec),
            0.5, // Require 50% space saving
            &mut buffer,
            &body,
            &footer,
        )
        .unwrap();

        // Read and verify (should be uncompressed)
        let opts = PageReadOptions::new(ptr).with_verify_checksum(true);
        let (read_body, _, uncompressed_size) =
            PageIO::read_and_decompress_page(&mut buffer, &opts).unwrap();

        // Body size should equal uncompressed size (no compression applied)
        assert_eq!(read_body.len(), uncompressed_size as usize);
        assert_eq!(read_body.as_ref(), body.as_slice());
    }

    #[test]
    fn test_checksum_verification() {
        let body = b"Test data for checksum verification";
        let footer = create_test_footer();

        let mut buffer = Cursor::new(Vec::new());
        let ptr = PageIO::write_page(&mut buffer, body, &footer, body.len() as u32).unwrap();

        // Corrupt the data
        let data = buffer.get_mut();
        data[10] ^= 0xFF;

        // Read should fail with checksum error
        let opts = PageReadOptions::new(ptr).with_verify_checksum(true);
        let result = PageIO::read_and_decompress_page(&mut buffer, &opts);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));
    }

    #[test]
    fn test_checksum_skip_verification() {
        let body = b"Test data";
        let footer = create_test_footer();

        let mut buffer = Cursor::new(Vec::new());
        let ptr = PageIO::write_page(&mut buffer, body, &footer, body.len() as u32).unwrap();

        // Corrupt the data
        let data = buffer.get_mut();
        data[5] ^= 0xFF;

        // Read with verification disabled should succeed (but return corrupted data)
        let opts = PageReadOptions::new(ptr).with_verify_checksum(false);
        let result = PageIO::read_and_decompress_page(&mut buffer, &opts);

        // Should not error, but data will be corrupted
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_pages() {
        let mut buffer = Cursor::new(Vec::new());
        let codec = Lz4Codec::new();

        let pages_data: Vec<Vec<u8>> = vec![
            (0..500).map(|i| (i % 10) as u8).collect(),
            (0..300).map(|i| (i % 5) as u8).collect(),
            (0..800).map(|i| (i % 15) as u8).collect(),
        ];

        let mut pointers = Vec::new();

        // Write multiple pages
        for (i, body) in pages_data.iter().enumerate() {
            let footer = PageFooter::Data(DataPageFooter {
                first_ordinal: i as u64 * 100,
                num_values: body.len() as u64,
                nullmap_size: 0,
                corresponding_element_ordinal: None,
                format_version: 2,
                null_encoding: NullEncoding::BitShuffle,
            });

            let ptr = PageIO::compress_and_write_page(
                Some(&codec),
                DEFAULT_MIN_SPACE_SAVING,
                &mut buffer,
                body,
                &footer,
            )
            .unwrap();

            pointers.push(ptr);
        }

        // Read and verify each page
        for (i, ptr) in pointers.iter().enumerate() {
            let opts = PageReadOptions::new(*ptr).with_codec(CompressionType::Lz4);
            let (read_body, read_footer, _) =
                PageIO::read_and_decompress_page(&mut buffer, &opts).unwrap();

            assert_eq!(read_body.as_ref(), pages_data[i].as_slice());

            if let PageFooter::Data(df) = read_footer {
                assert_eq!(df.first_ordinal, i as u64 * 100);
                assert_eq!(df.num_values, pages_data[i].len() as u64);
            } else {
                panic!("Expected DataPageFooter");
            }
        }
    }
}
