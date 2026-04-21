// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PredicatePartition {
    pub native_predicates: Vec<String>,
    pub external_predicates: Vec<String>,
}
