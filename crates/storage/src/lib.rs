// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Paro Storage
//!
//! Storage engine for Paro database.
//!
//! ## Modules
//! - `buffer`: Buffer pool management (Pin/Unpin, RAII handles)
//! - `table`: Table adapter/storage metadata (TableHandle, IndexSet)
//! - `compression`: Compression algorithms
//! - `index`: Extensible index framework
//! - `transaction`: MVCC transaction support
//! - `row`: Sealed execution-time row storage
//!
//! Note: this crate does **not** re-export types at the root; import via submodules
//! (e.g. `paro_storage::buffer::BufferPool`). Nested areas such as `transaction`,
//! `compaction`, and `index::fulltext` likewise expose items only under their leaf modules
//! (e.g. `paro_storage::transaction::txn::Transaction`). Database journal and legacy
//! segment recovery utilities live in `paro_journal::wal`.

pub mod buffer;
mod codec;
pub mod column;
pub mod compaction;
pub mod compression;
pub mod index;
pub mod meta;
pub mod metrics;
mod mutation;
pub mod primary_key;
pub mod row;
mod rowid_resolver;
pub mod rowset;
pub mod search;
pub mod statistics;
pub mod table;
pub mod tablet;
pub mod transaction;
pub mod write;

#[cfg(test)]
pub(crate) mod test_utils;
