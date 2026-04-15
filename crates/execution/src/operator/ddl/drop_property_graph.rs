// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical Drop Property Graph Operator
//!
//! Executes DROP PROPERTY GRAPH DDL by recording a typed drop op plus deferred
//! runtime-transition / cleanup descriptors. The actual unregister and directory
//! removal happen after the commit is durable.

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::BoundDropPropertyGraphInfo;
use std::any::Any;

#[derive(Debug)]
pub struct DropPropertyGraph {
    pub info: BoundDropPropertyGraphInfo,
}

impl DropPropertyGraph {
    pub fn new(info: BoundDropPropertyGraphInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for DropPropertyGraph {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::DropPropertyGraph
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
        let _ = ctx.session.catalog();
        let ddl = ctx.session.ddl().ok_or_else(|| {
            paro_error::internal("property graph DDL requires transaction DDL context")
        })?;
        ddl.apply_drop_property_graph(
            self.info.catalog_name.clone(),
            self.info.schema_name.clone(),
            self.info.graph_name.clone(),
            self.info.if_exists,
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
