//! Physical Create Table Operator
//!
//!

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_catalog::entry::{CreateTableInfo, OnCreateConflict};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundCreateTableInfo;
use std::any::Any;

/// CreateTable represents a CREATE TABLE operation.
#[derive(Debug)]
pub struct CreateTable {
    pub info: BoundCreateTableInfo,
}

impl CreateTable {
    pub fn new(info: BoundCreateTableInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for CreateTable {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreateTable
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
        let _ = ctx
            .session
            .database(&self.info.database_name)
            .ok_or_else(|| {
                paro_common::error::catalog(format!(
                    "Database not found: {}",
                    self.info.database_name
                ))
            })?;

        let on_conflict = if self.info.if_not_exists {
            OnCreateConflict::IgnoreOnConflict
        } else {
            OnCreateConflict::ErrorOnConflict
        };
        let info = CreateTableInfo::new(
            self.info.database_name.clone(),
            self.info.schema_name.clone(),
            self.info.table_name.clone(),
            self.info.columns.clone(),
        )
        .with_constraints(self.info.constraints.clone())
        .with_on_conflict(on_conflict);

        ctx.session
            .ddl()
            .expect("ddl context must exist inside transactions")
            .apply_create_table(info)?;

        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
