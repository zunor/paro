// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Full-Text Index Components
//!
//! Tokenizer, posting lists, inverted index, and query parsing.

pub mod bm25;
pub mod builder;
pub mod compaction;
pub mod inverted_index;
pub mod persistence;
pub mod posting_list;
pub mod query_parser;
pub mod text_index;
pub mod tokenizer;
