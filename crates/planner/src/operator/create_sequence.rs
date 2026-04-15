//! Logical Create Sequence Operator
//!

use crate::binder::ir::statement::BoundCreateSequenceInfo;

#[derive(Debug, Clone)]
pub struct CreateSequence {
    pub info: BoundCreateSequenceInfo,
}

impl CreateSequence {
    pub fn new(info: BoundCreateSequenceInfo) -> Self {
        Self { info }
    }
}
