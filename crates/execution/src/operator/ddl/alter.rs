//! Physical Alter Operator
//!

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundAlterEntryInfo;
use std::any::Any;

#[derive(Debug)]
pub struct Alter {
    pub info: BoundAlterEntryInfo,
}

impl Alter {
    pub fn new(info: BoundAlterEntryInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for Alter {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Alter
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
        ctx.session
            .ddl()
            .expect("ddl context must exist inside transactions")
            .apply_alter_entry(
                self.info.schema_name.clone(),
                self.info.info.clone(),
                self.info.sql.clone(),
            )?;
        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
