// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical operator for `CREATE INDEX`.
//!
//! Current `CREATE INDEX` execution is metadata-only:
//! 1. Stage the catalog entry during execution.
//! 2. Finalize to a `PreparedIndexArtifact::MetadataOnly`.
//! 3. Let post-commit hooks attach any runtime index state (ART/HNSW/Sparse/FullText).
//!
//! Known limitations in the current phase:
//! - Runtime sink builds are intentionally gone; this operator never buffers row data
//! - Default `CREATE INDEX name ON t (col)` only stages metadata for runtime ART
//! - Any runtime materialization happens after commit, so execution stays a pure DDL flow

use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use paro_catalog::entry::{
    CreateIndexInfo, IndexCoverage, IndexType as CatalogIndexType, TableCatalogEntry,
};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_context::{DdlApplyContext, IndexBuildHandle, PreparedIndexArtifact};

use crate::execution_context::ExecutionContext;
use crate::operator::state::{
    EmptyGlobalSourceState, EmptyLocalSourceState, GlobalSinkState, GlobalSourceState,
    LocalSinkState, LocalSourceState, OperatorSinkCombineInput, OperatorSinkFinalizeInput,
    OperatorSinkInput, OperatorSourceInput,
};
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::{
    SinkCombineResultType, SinkFinalizeType, SinkResultType, SourceResultType,
};

#[derive(Debug)]
pub struct CreateIndex {
    /// Reference to the target table.
    pub table: Arc<TableCatalogEntry>,
    /// Index creation information.
    pub info: CreateIndexInfo,
    /// DDL operator output types (empty for CREATE INDEX).
    return_types: Vec<LogicalType>,
}

impl CreateIndex {
    pub fn new(table: Arc<TableCatalogEntry>, info: CreateIndexInfo) -> Self {
        Self {
            table,
            info,
            return_types: vec![],
        }
    }

    pub fn index_name(&self) -> &str {
        &self.info.name
    }

    pub fn is_unique(&self) -> bool {
        self.info.is_unique()
    }

    fn ensure_supported_index_type(index_type: CatalogIndexType) -> Result<()> {
        if index_type.supports_metadata_only_build() {
            return Ok(());
        }

        Err(paro_error::not_implemented(format!(
            "CREATE INDEX type '{}' is not supported in the current phase",
            index_type.as_str()
        )))
    }

    fn finish_metadata_only_index_build(
        &self,
        ctx: &ExecutionContext,
        gstate: &CreateIndexGlobalSinkState,
    ) -> Result<()> {
        let handle = {
            let mut guard = gstate
                .build_handle
                .lock()
                .map_err(|_| paro_error::internal("failed to lock CREATE INDEX build handle"))?;
            match guard.take() {
                Some(handle) => handle,
                None => {
                    let ddl = ctx.session.ddl().ok_or_else(|| {
                        paro_error::internal("CREATE INDEX requires transaction DDL context")
                    })?;
                    ddl.prepare_index_build(self.info.clone(), Arc::clone(&self.table))?
                }
            }
        };
        if handle.skip_build() {
            return Ok(());
        }

        let coverage = self.compute_index_coverage()?;

        gstate
            .ddl_context
            .commit_index_build(handle, PreparedIndexArtifact::MetadataOnly { coverage })
    }

    fn compute_index_coverage(&self) -> Result<Option<IndexCoverage>> {
        let _ = CatalogIndexType::FullText;
        Ok(None)
    }
}

pub struct CreateIndexGlobalSinkState {
    index_name: String,
    build_handle: Mutex<Option<Box<dyn IndexBuildHandle>>>,
    ddl_context: Arc<dyn DdlApplyContext>,
    registration_done: AtomicBool,
}

impl fmt::Debug for CreateIndexGlobalSinkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateIndexGlobalSinkState")
            .field("index_name", &self.index_name)
            .finish()
    }
}

impl GlobalSinkState for CreateIndexGlobalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct CreateIndexLocalSinkState;

impl LocalSinkState for CreateIndexLocalSinkState {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl PhysicalOperator for CreateIndex {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::CreateIndex
    }

    fn types(&self) -> &[LogicalType] {
        &self.return_types
    }

    fn is_source(&self) -> bool {
        true
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn parallel_sink(&self) -> bool {
        false
    }

    fn get_global_sink_state(&self, ctx: &ExecutionContext) -> Result<Box<dyn GlobalSinkState>> {
        let ddl = ctx
            .session
            .ddl()
            .ok_or_else(|| paro_error::internal("CREATE INDEX requires transaction DDL context"))?;

        Ok(Box::new(CreateIndexGlobalSinkState {
            index_name: self.info.name.clone(),
            build_handle: Mutex::new(None),
            ddl_context: ddl,
            registration_done: AtomicBool::new(false),
        }))
    }

    fn get_local_sink_state(&self, _ctx: &ExecutionContext) -> Result<Box<dyn LocalSinkState>> {
        Ok(Box::new(CreateIndexLocalSinkState))
    }

    fn sink(
        &self,
        _ctx: &ExecutionContext,
        _chunk: &Chunk,
        _input: &mut OperatorSinkInput,
    ) -> Result<SinkResultType> {
        Ok(SinkResultType::NeedMoreInput)
    }

    fn combine(
        &self,
        _ctx: &ExecutionContext,
        _input: &mut OperatorSinkCombineInput,
    ) -> Result<SinkCombineResultType> {
        Ok(SinkCombineResultType::Finished)
    }

    fn finalize(&self, _input: &OperatorSinkFinalizeInput) -> Result<SinkFinalizeType> {
        Ok(SinkFinalizeType::Ready)
    }

    fn get_global_source_state(
        &self,
        ctx: &ExecutionContext,
        sink_state: Option<&dyn GlobalSinkState>,
    ) -> Result<Box<dyn GlobalSourceState>> {
        if let Some(sink_state) = sink_state {
            let gstate = sink_state
                .as_any()
                .downcast_ref::<CreateIndexGlobalSinkState>()
                .ok_or_else(|| paro_error::internal("Invalid global sink state".to_string()))?;

            if gstate.registration_done.swap(true, Ordering::AcqRel) {
                return Ok(Box::new(EmptyGlobalSourceState));
            }

            self.finish_metadata_only_index_build(ctx, gstate)?;
            return Ok(Box::new(EmptyGlobalSourceState));
        }

        Self::ensure_supported_index_type(self.info.index_type)?;
        let ddl = ctx
            .session
            .ddl()
            .ok_or_else(|| paro_error::internal("CREATE INDEX requires transaction DDL context"))?;
        let handle = ddl.prepare_index_build(self.info.clone(), Arc::clone(&self.table))?;
        if handle.skip_build() {
            return Ok(Box::new(EmptyGlobalSourceState));
        }
        ddl.commit_index_build(
            handle,
            PreparedIndexArtifact::MetadataOnly {
                coverage: self.compute_index_coverage()?,
            },
        )?;

        Ok(Box::new(EmptyGlobalSourceState))
    }

    fn get_local_source_state(
        &self,
        _ctx: &ExecutionContext,
        _gstate: &dyn GlobalSourceState,
    ) -> Result<Box<dyn LocalSourceState>> {
        Ok(Box::new(EmptyLocalSourceState))
    }

    fn get_data(
        &self,
        _ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
