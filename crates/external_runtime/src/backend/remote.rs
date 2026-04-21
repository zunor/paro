// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteBackend {
    pub endpoint: String,
    pub region: Option<String>,
    pub transport_label: String,
}
