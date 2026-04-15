//! # Page Decoder Trait
//!
//! Trait for decoding pages and reading column values.

use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};

/// Encoding type for pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum EncodingType {
    #[default]
    Unknown = 0,
    Default = 1,
    Plain = 2,
    Prefix = 3,
    Rle = 4,
    Dict = 5,
    BitShuffle = 6,
    FrameOfReference = 7,
}

impl EncodingType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(EncodingType::Unknown),
            1 => Some(EncodingType::Default),
            2 => Some(EncodingType::Plain),
            3 => Some(EncodingType::Prefix),
            4 => Some(EncodingType::Rle),
            5 => Some(EncodingType::Dict),
            6 => Some(EncodingType::BitShuffle),
            7 => Some(EncodingType::FrameOfReference),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Trait for decoding pages and reading column values.
///
/// PageDecoder reads encoded page data and provides access to individual values.
///
/// ## Lifecycle
///
/// 1. Create decoder with page data
/// 2. Call `init()` to parse page header
/// 3. Use `seek_to_position()` to position within page
/// 4. Call `next_batch()` to read values
///
/// ## Example
///
/// ```ignore
/// let mut decoder = PlainPageDecoder::new(page_data);
/// decoder.init()?;
///
/// decoder.seek_to_position(100)?;
/// let values = decoder.next_batch(50)?;
/// ```
pub trait PageDecoder: Send + Sync {
    /// Initialize the decoder by parsing page header.
    fn init(&mut self) -> Result<()>;

    /// Seek to a position within the page.
    ///
    /// Position 0 is the first value in the page.
    fn seek_to_position(&mut self, pos: u32) -> Result<()>;

    /// Read the next batch of values.
    ///
    /// # Arguments
    /// * `n` - Maximum number of values to read
    ///
    /// # Returns
    /// Tuple of (values_read, data)
    fn next_batch(&mut self, n: usize) -> Result<(usize, Bytes)>;

    /// Get the total number of values in the page.
    fn count(&self) -> u32;

    /// Get the current position within the page.
    fn current_index(&self) -> u32;

    /// Get the encoding type of this page.
    fn encoding_type(&self) -> EncodingType;

    /// Check if this decoder supports reading by row IDs.
    fn supports_read_by_rowids(&self) -> bool {
        false
    }

    /// Read values by row IDs (relative to page start).
    ///
    /// Default implementation falls back to sequential reads.
    fn read_by_rowids(&mut self, _rowids: &[u32]) -> Result<Bytes> {
        // Default: not optimized, subclasses can override
        Err(paro_error::not_supported("read_by_rowids"))
    }
}

/// Options for creating page decoders.
#[derive(Debug, Clone, Default)]
pub struct PageDecoderOptions {
    /// Expected encoding type
    pub encoding_type: EncodingType,
    /// Data type size in bytes (for fixed-width types)
    pub type_size: usize,
    /// Whether to verify data integrity
    pub verify: bool,
}

impl PageDecoderOptions {
    pub fn new(encoding_type: EncodingType) -> Self {
        PageDecoderOptions {
            encoding_type,
            ..Default::default()
        }
    }

    pub fn with_type_size(mut self, type_size: usize) -> Self {
        self.type_size = type_size;
        self
    }

    pub fn with_verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }
}
