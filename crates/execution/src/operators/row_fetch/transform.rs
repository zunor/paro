// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use paro_catalog::entry::CatalogEntryEnum;
use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_function::scalar::FunctionExecContext;
use paro_storage::tablet::TabletRowIdReader;
use paro_storage::transaction::overlay_reader::TxnOverlayReader;
use paro_transaction::TableId;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::physical::specs::RowFetchSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    RowFetchTableState, RowFetchTransformLocal, TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::{read_u64_from_vector, ExpressionEvalInput};

#[derive(Debug, Clone)]
pub struct RowFetchTransformExec {
    pub spec: RowFetchSpec,
}

impl RowFetchTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::Empty)
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let table_fetches = self
            .spec
            .mappings
            .iter()
            .map(|mapping| {
                let entry = ctx.query.session.catalog().get_table(
                    &ctx.query.catalog,
                    &mapping.schema_name,
                    &mapping.table_name,
                )?;
                let table = match entry.as_ref() {
                    CatalogEntryEnum::Table(table) => table,
                    _ => {
                        return Err(paro_error::wrong_object_type("table", &mapping.table_name));
                    }
                };
                let storage = table.get_storage().ok_or_else(|| {
                    paro_error::internal(format!("table \"{}\" has no storage", mapping.table_name))
                })?;
                ctx.query
                    .transaction
                    .read_tracker()
                    .record_table_read(TableId::new(storage.table_id()));
                Ok(RowFetchTableState {
                    table_name: mapping.table_name.clone(),
                    rowid_col_idx: mapping.rowid_col_idx,
                    storage: storage.clone(),
                    storage_snapshot: ctx.query.storage_snapshot(storage)?,
                    reader: None,
                    rowids: Vec::new(),
                    column_ids: mapping.column_ids.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();

        let direct_project_columns = self.spec.projection.as_ref().and_then(|projection| {
            projection
                .expressions
                .iter()
                .map(|expression| match expression {
                    paro_planner::expression::Expression::Reference(reference) => {
                        Some(reference.index)
                    }
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .map(Vec::into_boxed_slice)
        });
        let project_executor = self.spec.projection.as_ref().and_then(|projection| {
            direct_project_columns.is_none().then(|| {
                ExpressionExecutor::with_expressions_for_session(
                    &projection.expressions,
                    ctx.query.session.as_ref(),
                )
            })
        });
        Ok(TransformLocal::RowFetch(RowFetchTransformLocal {
            table_fetches,
            direct_project_columns,
            project_executor,
            combined_columns: Vec::new(),
        }))
    }

    pub(crate) fn transform(
        &self,
        ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        local: &mut TransformLocal,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<TransformPoll> {
        let output_types = self
            .spec
            .projection
            .as_ref()
            .map_or(self.spec.raw_output_types.as_ref(), |projection| {
                projection.output_types.as_ref()
            });
        if input.is_empty() {
            *output = Chunk::try_init_empty(output_types, output.allocator().clone())?;
            return Ok(TransformPoll::NeedMoreInput);
        }
        let local = match local {
            TransformLocal::RowFetch(local) => local,
            _ => return Err(paro_error::internal("row-fetch local state mismatch")),
        };
        local.combined_columns.clear();
        local
            .combined_columns
            .extend((0..input.column_count()).filter_map(|index| input.column(index).cloned()));
        for fetch in local.table_fetches.iter_mut() {
            let rowid_column = input.column(fetch.rowid_col_idx).ok_or_else(|| {
                paro_error::internal(format!(
                    "missing rowid column {} for table \"{}\"",
                    fetch.rowid_col_idx, fetch.table_name
                ))
            })?;
            fetch.rowids.clear();
            fetch.rowids.reserve(input.size());
            for row in 0..input.size() {
                fetch.rowids.push(read_u64_from_vector(
                    rowid_column,
                    row,
                    "relational row fetch",
                )?);
            }
            if fetch.reader.is_none() {
                let mut rowsets = fetch.storage_snapshot.rowsets()?;
                if let Some(overlay) =
                    TxnOverlayReader::for_tablet(&fetch.storage.tablet(), &ctx.query.transaction)?
                {
                    let visible = rowsets
                        .iter()
                        .map(|rowset| rowset.rowset_id())
                        .collect::<HashSet<_>>();
                    rowsets.extend(
                        overlay
                            .all_rowsets()
                            .into_iter()
                            .filter(|rowset| !visible.contains(&rowset.rowset_id())),
                    );
                }
                fetch.reader = Some(TabletRowIdReader::new(
                    fetch.storage.tablet(),
                    rowsets,
                    &fetch.column_ids,
                    ctx.query.allocator(MemoryTag::ColumnData),
                )?);
            }
            let fetched = fetch
                .reader
                .as_ref()
                .expect("row-fetch reader initialized above")
                .get_by_rowids(&fetch.rowids, &fetch.column_ids)?;
            local.combined_columns.extend(
                (0..fetched.column_count()).filter_map(|index| fetched.column(index).cloned()),
            );
        }
        if local.combined_columns.len() != self.spec.raw_output_types.len() {
            return Err(paro_error::internal(format!(
                "row-fetch produced {} columns for declared width {}",
                local.combined_columns.len(),
                self.spec.raw_output_types.len()
            )));
        }
        let materialized = Chunk::try_from_arc_vectors_with_cardinality(
            local.combined_columns.clone(),
            input.size(),
            output.allocator().clone(),
        )?;

        let Some(projection) = self.spec.projection.as_ref() else {
            *output = materialized;
            return Ok(TransformPoll::Output);
        };
        if let Some(columns) = local.direct_project_columns.as_ref() {
            let columns = columns
                .iter()
                .map(|&index| {
                    materialized.column(index).cloned().ok_or_else(|| {
                        paro_error::internal(format!(
                            "row-fetch fused projection column {index} is missing"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            *output = Chunk::try_from_arc_vectors_with_cardinality(
                columns,
                input.size(),
                output.allocator().clone(),
            )?;
            return Ok(TransformPoll::Output);
        }
        let mut projected = Chunk::try_initialize(
            &projection.output_types,
            input.size(),
            output.allocator().clone(),
        )?;
        local
            .project_executor
            .as_mut()
            .ok_or_else(|| paro_error::internal("row-fetch projection executor is missing"))?
            .execute_all_kernel(
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: &materialized,
                }),
                ctx.query,
                &mut projected,
            )?;
        *output = projected;
        Ok(TransformPoll::Output)
    }

    pub(crate) fn flush(
        &self,
        _ctx: &mut OperatorCallContext,
        _global: &TransformGlobal,
        _local: &mut TransformLocal,
        _output: &mut Chunk,
    ) -> Result<TransformFlushPoll> {
        Ok(TransformFlushPoll::Done)
    }

    pub(crate) fn finish_global(
        &self,
        _ctx: &mut OperatorFinishContext,
        _global: &TransformGlobal,
    ) -> Result<TransformFinishPoll> {
        Ok(TransformFinishPoll::Done)
    }
}
