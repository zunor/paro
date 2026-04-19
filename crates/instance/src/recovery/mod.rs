// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub mod consistency_report;
mod ddl;
mod index_restore;
pub mod registry;
pub mod replay_handler;

pub(crate) use index_restore::{reconcile_fulltext_index_coverage, restore_runtime_art_indexes};
