// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Create View Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: N/A (DDL operation)
//! - BufferManager: N/A (DDL operation)
//!
use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundCreateViewInfo;
use std::any::Any;

/// CreateView represents a CREATE VIEW operation.
///
/// This operator executes the CREATE VIEW DDL statement by:
/// 1. Getting the target database from the cluster
/// 2. Converting BoundCreateViewInfo to CreateViewInfo
/// 3. Calling Catalog::create_view() to register the view
///
#[derive(Debug)]
pub struct CreateView {
    /// The bound view creation information
    pub info: BoundCreateViewInfo,
}

impl CreateView {
    /// Create a new CreateView operator.
    pub fn new(info: BoundCreateViewInfo) -> Self {
        Self { info }
    }

    /// Get the schema name for the view.
    pub fn schema_name(&self) -> &str {
        &self.info.schema_name
    }

    /// Get the view name.
    pub fn view_name(&self) -> &str {
        &self.info.view_name
    }
}

impl PhysicalOperator for CreateView {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreateView
    }

    fn types(&self) -> &[LogicalType] {
        // CREATE VIEW returns no data
        &[]
    }

    fn is_source(&self) -> bool {
        // CREATE VIEW is a source operator (no input required)
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
            .apply_create_view(self.info.clone().to_create_view_info())?;

        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
