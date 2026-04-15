//! Physical Create Schema Operator
//!
//!

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_catalog::entry::CreateSchemaInfo;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundCreateSchemaInfo;
use std::any::Any;

/// CreateSchema represents a CREATE SCHEMA operation.
#[derive(Debug)]
pub struct CreateSchema {
    pub info: BoundCreateSchemaInfo,
}

impl CreateSchema {
    pub fn new(info: BoundCreateSchemaInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for CreateSchema {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreateSchema
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
            .apply_create_schema(CreateSchemaInfo::new(
                self.info.database_name.clone(),
                self.info.schema_name.clone(),
            ))?;

        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
