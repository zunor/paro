// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::identity::GraphId;

/// Default test graph id under `postgres.public`.
pub fn graph_runtime_key(graph_name: &str) -> String {
    GraphId::new("postgres", "public", graph_name).runtime_key()
}
