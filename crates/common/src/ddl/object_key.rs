// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DdlObjectKind {
    Schema,
    Table,
    View,
    Index,
    Sequence,
    PropertyGraph,
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DdlObjectKey {
    pub database: String,
    pub schema: Option<String>,
    pub name: String,
    pub kind: DdlObjectKind,
}

impl DdlObjectKey {
    pub fn new(
        database: impl Into<String>,
        schema: Option<impl Into<String>>,
        name: impl Into<String>,
        kind: DdlObjectKind,
    ) -> Self {
        Self {
            database: database.into(),
            schema: schema.map(|value| value.into()),
            name: name.into(),
            kind,
        }
    }
}
