//! Physical Create Sequence Operator
//!

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundCreateSequenceInfo;
use std::any::Any;

#[derive(Debug)]
pub struct CreateSequence {
    pub info: BoundCreateSequenceInfo,
}

impl CreateSequence {
    pub fn new(info: BoundCreateSequenceInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for CreateSequence {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreateSequence
    }

    fn types(&self) -> &[LogicalType] {
        &[]
    }

    fn is_source(&self) -> bool {
        true
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        let _db = ctx
            .session
            .database(&self.info.database_name)
            .ok_or_else(|| {
                paro_common::error::catalog(format!(
                    "Database not found: {}",
                    self.info.database_name
                ))
            })?;

        ctx.session
            .ddl()
            .expect("ddl context must exist inside transactions")
            .apply_create_sequence(self.info.clone().to_create_sequence_info())?;

        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
