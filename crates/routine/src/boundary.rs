// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

pub use crate::spec::RowSemantics;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementClass {
    Native,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBoundary {
    pub placement: PlacementClass,
    pub may_block: bool,
    pub row_semantics: RowSemantics,
}
