// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Page Module
//!
//! Page structure and I/O operations for the Segment V2 format.
//!
//! ## Page Layout
//!
//! ```text
//! Page := PageBody, PageFooter, FooterSize(4 bytes), Checksum(4 bytes)
//! ```
//!
//! - `PageBody`: Encoded and optionally compressed data
//! - `PageFooter`: Serialized metadata (type, uncompressed size, type-specific footer)
//! - `FooterSize`: 4-byte little-endian footer length
//! - `Checksum`: CRC32C checksum of all preceding bytes
//!
//! ## Page Types
//!
//! - `DATA_PAGE`: Column data
//! - `INDEX_PAGE`: B-tree index nodes
//! - `DICTIONARY_PAGE`: Dictionary for dictionary encoding
//! - `SHORT_KEY_PAGE`: Short key index

mod page;
mod page_builder;
mod page_decoder;
mod page_io;

pub use page::{
    DataPageFooter, DictPageFooter, IndexPageFooter, IndexPageType, NullEncoding, Page, PageFooter,
    PagePointer, PageType, ShortKeyFooter, CURRENT_DATA_PAGE_FORMAT_VERSION,
};
pub use page_builder::{PageBuilder, PageBuilderOptions};
pub use page_decoder::{EncodingType, PageDecoder, PageDecoderOptions};
pub use page_io::{
    BlockCompressionCodec, CompressionType, Lz4Codec, NoCompressionCodec, PageIO, PageLayout,
    PageReadOptions, ZstdCodec, DEFAULT_MIN_SPACE_SAVING,
};
