// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

pub mod consistency_report;
mod ddl;
mod index_restore;
pub mod registry;
pub mod replay_handler;

pub(crate) use index_restore::{restore_runtime_art_indexes, restore_search_registry_definitions};
