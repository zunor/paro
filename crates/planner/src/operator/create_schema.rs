//! Logical Create Schema Operator
//!
//!

use crate::binder::ir::statement::BoundCreateSchemaInfo;

/// CreateSchema represents a CREATE SCHEMA operation.
#[derive(Debug, Clone)]
pub struct CreateSchema {
    pub info: BoundCreateSchemaInfo,
}

impl CreateSchema {
    pub fn new(info: BoundCreateSchemaInfo) -> Self {
        Self { info }
    }
}
