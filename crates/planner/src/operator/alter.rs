//! Logical Alter Operator
//!

use crate::binder::ir::statement::BoundAlterEntryInfo;

#[derive(Debug, Clone)]
pub struct Alter {
    pub info: BoundAlterEntryInfo,
}

impl Alter {
    pub fn new(info: BoundAlterEntryInfo) -> Self {
        Self { info }
    }
}
