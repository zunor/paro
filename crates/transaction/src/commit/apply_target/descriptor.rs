// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical apply-target accounting descriptor.

use crate::types::{DatabaseId, TableId};
use std::sync::Arc;

pub type ApplyTargetSet = Arc<[ApplyTargetDescriptor]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplyTargetKind {
    Storage,
    Catalog,
    Search,
    Graph,
    Maintenance,
    External,
    BulkLoad,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApplyTargetDescriptor {
    pub database_id: DatabaseId,
    pub table_id: Option<TableId>,
    pub kind: ApplyTargetKind,
}
