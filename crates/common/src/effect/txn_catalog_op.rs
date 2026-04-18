// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::ddl::DdlChangeRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTxnOp {
    pub change: DdlChangeRecord,
}
