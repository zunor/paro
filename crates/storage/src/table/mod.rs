// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Table-level storage APIs, metadata, and tablet-facing adapters.

mod index_set;
pub(crate) mod runtime_indexes;
pub mod segment_reorderer;
pub mod storage_descriptor;
pub mod table_factory;
pub mod table_handle;
mod table_indexes;
mod table_maintenance;
mod table_read;
mod table_search_query;
mod table_statistics;
mod table_write;
