use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDmlTableDelta {
    pub table_oid: u64,
    pub inserted: u64,
    pub deleted: u64,
    pub updated: u64,
    pub updated_columns: Vec<u32>,
}

impl GraphDmlTableDelta {
    pub fn from_parts(
        table_oid: u64,
        inserted: u64,
        deleted: u64,
        updated: u64,
        updated_columns: &BTreeSet<u32>,
    ) -> Self {
        Self {
            table_oid,
            inserted,
            deleted,
            updated,
            updated_columns: updated_columns.iter().copied().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostCommitHookDescriptor {
    GraphDmlMaintenance { deltas: Vec<GraphDmlTableDelta> },
}
