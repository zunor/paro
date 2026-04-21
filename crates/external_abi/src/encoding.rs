// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnEncoding {
    Flat,
    Constant,
    Dictionary,
    Sequence,
    List,
    Struct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnPopulationMode {
    Eager,
    LazyLinuxUffd,
    GpuDirect,
}
