// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::prepared::store::PortalStoreMark;
use paro_context::WriteClass;
use paro_storage::transaction::txn::StorageSavepointMark;

/// The kind of transaction block currently active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockKind {
    #[default]
    None,
    Explicit,
    Implicit,
}

#[derive(Debug, Clone)]
pub struct SavepointFrame {
    pub name: String,
    pub settings_journal_mark: usize,
    pub portal_mark: PortalStoreMark,
    pub write_class_mark: WriteClass,
    pub ddl_mark: usize,
    pub storage_mark: StorageSavepointMark,
}
