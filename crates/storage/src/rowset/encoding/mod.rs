// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Column Encoding Module
//!
//! Implements column encoding schemes used by the rowset format.
//!
//! ## Supported Encodings
//!
//! - `Plain`: Raw values without encoding (fixed-width types)
//! - `BinaryPlain`: Variable-length strings with offset table
//! - `Dictionary`: Maps distinct values to integer codes
//! - `BitShuffle`: Bit-level shuffling + LZ4 compression
//! - `RLE`: Run-Length Encoding for repeated values
//! - `FrameOfReference`: Delta encoding with min value reference (P1)
//! - `BinaryPrefix`: Prefix compression for sorted strings (P2)
//!
//! ## Encoding Selection Rules
//!
//! | Type | Default Encoding | Alternative |
//! |------|-----------------|-------------|
//! | BOOLEAN | RLE | BitShuffle |
//! | TINYINT | BitShuffle | Plain |
//! | SMALLINT/INT/BIGINT | BitShuffle | Dictionary (low cardinality) |
//! | FLOAT/DOUBLE | BitShuffle | Plain |
//! | VARCHAR/CHAR | Dictionary | BinaryPlain |
//! | DATE/DATETIME | BitShuffle | FrameOfReference |

mod binary_dict;
mod binary_plain;
mod binary_prefix;
mod bitshuffle;
mod encoding_info;
mod frame_of_reference;
mod plain;
mod rle;

pub use binary_dict::{BinaryDictPageBuilder, BinaryDictPageDecoder};
pub(crate) use binary_plain::BinaryPlainPageSlice;
pub use binary_plain::{BinaryPlainPageBuilder, BinaryPlainPageDecoder};
pub use binary_prefix::{BinaryPrefixPageBuilder, BinaryPrefixPageDecoder};
pub use bitshuffle::{BitShufflePageBuilder, BitShufflePageDecoder, BITSHUFFLE_PAGE_HEADER_SIZE};
pub use encoding_info::{get_encoding_registry, EncodingInfo, EncodingRegistry, FieldType};
pub use frame_of_reference::{
    FrameOfReferencePageBuilder, FrameOfReferencePageDecoder, FOR_PAGE_HEADER_SIZE,
};
pub use plain::{PlainPageBuilder, PlainPageDecoder, PLAIN_PAGE_HEADER_SIZE};
pub use rle::{RlePageBuilder, RlePageDecoder, RLE_PAGE_HEADER_SIZE};
