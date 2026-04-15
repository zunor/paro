// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::ddl::DdlObjectKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeTransitionDescriptor {
    AttachIndexRuntime {
        index: DdlObjectKey,
        table_name: String,
        index_type: String,
        column_ids: Vec<u32>,
        fulltext_config: Option<String>,
    },
    DetachIndexRuntime {
        index: DdlObjectKey,
        table_name: String,
        index_type: String,
        column_ids: Vec<u32>,
        fulltext_config: Option<String>,
    },
    RegisterGraphRuntime {
        graph: DdlObjectKey,
    },
    UnregisterGraphRuntime {
        graph: DdlObjectKey,
    },
}
