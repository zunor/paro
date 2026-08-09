// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use paro_catalog::entry::CatalogEntryEnum;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::ColumnBinding;
use paro_storage::tablet::TabletReaderParams;
use paro_transaction::TableId;

use crate::expression_executor::executor::{ExpressionExecutor, VectorKernelInput};
use crate::physical::specs::GraphProjectSpec;
use crate::runtime::context::{OperatorCallContext, OperatorFinishContext, PipelineInitContext};
use crate::runtime::state::{
    GraphProjectMaterializedRuntime, GraphProjectTableFetchPlan, GraphProjectTransformLocal,
    TransformGlobal, TransformLocal,
};
use crate::runtime::transform::{TransformFinishPoll, TransformFlushPoll, TransformPoll};
use crate::runtime::{read_u64_from_vector, visit_column_refs, ExpressionEvalInput};

#[derive(Debug, Clone)]
pub struct GraphProjectTransformExec {
    pub spec: GraphProjectSpec,
}

impl GraphProjectTransformExec {
    pub(crate) fn create_global(&self, _ctx: &mut PipelineInitContext) -> Result<TransformGlobal> {
        Ok(TransformGlobal::Empty)
    }

    pub(crate) fn create_local(
        &self,
        ctx: &mut PipelineInitContext,
        _global: &TransformGlobal,
    ) -> Result<TransformLocal> {
        let materialized = if self.spec.rowid_mappings.is_empty() {
            None
        } else {
            Some(build_graph_project_materialized_runtime(ctx, &self.spec)?)
        };
        let raw_filter_executors = if materialized.is_none() {
            graph_project_filter_executors(&self.spec.filters, ctx.query.session.as_ref())
        } else {
            Vec::new()
        };
        let raw_project_executor = if materialized.is_none() {
            Some(ExpressionExecutor::with_expressions_for_session(
                &self.spec.expressions,
                ctx.query.session.as_ref(),
            ))
        } else {
            None
        };
        Ok(TransformLocal::GraphProject(GraphProjectTransformLocal {
            filter_selection: None,
            raw_filter_executors,
            raw_project_executor,
            materialized,
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
        if input.is_empty() {
            *output = Chunk::try_init_empty(&self.spec.output_types, output.allocator().clone())?;
            return Ok(TransformPoll::NeedMoreInput);
        }
        let local = graph_project_local(local)?;
        let mut projected = build_graph_project_output(ctx, &self.spec, local, input, output)?;
        if projected.is_empty() {
            return Ok(TransformPoll::NeedMoreInput);
        }
        output.move_from(&mut projected);
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

#[inline(always)]
fn graph_project_local(local: &mut TransformLocal) -> Result<&mut GraphProjectTransformLocal> {
    match local {
        TransformLocal::GraphProject(state) => Ok(state),
        _ => Err(paro_error::internal("graph project local state mismatch")),
    }
}

fn build_graph_project_output(
    ctx: &mut OperatorCallContext,
    spec: &GraphProjectSpec,
    local: &mut GraphProjectTransformLocal,
    input: &Chunk,
    output: &mut Chunk,
) -> Result<Chunk> {
    if spec.rowid_mappings.is_empty() {
        let input = clone_chunk_refs(input);
        if local.raw_project_executor.is_none() {
            local.raw_project_executor = Some(ExpressionExecutor::with_expressions_for_session(
                &spec.expressions,
                ctx.query.session.as_ref(),
            ));
        }
        if local.raw_filter_executors.is_empty() && !spec.filters.is_empty() {
            local.raw_filter_executors =
                graph_project_filter_executors(&spec.filters, ctx.query.session.as_ref());
        }
        let Some(filtered) = apply_graph_project_filters(
            ctx,
            &mut local.filter_selection,
            input,
            &mut local.raw_filter_executors,
        )?
        else {
            return Chunk::try_init_empty(&spec.output_types, output.allocator().clone());
        };
        let mut projected = Chunk::try_initialize(
            &spec.output_types,
            filtered.size(),
            output.allocator().clone(),
        )?;
        local
            .raw_project_executor
            .as_mut()
            .expect("graph project raw executor initialized")
            .execute_all_kernel(
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: &filtered,
                }),
                ctx.query,
                &mut projected,
            )?;
        return Ok(projected);
    }

    let materialized_runtime = local
        .materialized
        .as_mut()
        .ok_or_else(|| paro_error::internal("graph project materialized runtime missing"))?;
    let materialized = materialize_graph_project_input(ctx, materialized_runtime, input)?;
    let Some(filtered) = apply_graph_project_filters(
        ctx,
        &mut local.filter_selection,
        materialized,
        &mut materialized_runtime.filter_executors,
    )?
    else {
        return Chunk::try_init_empty(&spec.output_types, output.allocator().clone());
    };
    let mut projected = Chunk::try_initialize(
        &spec.output_types,
        filtered.size(),
        output.allocator().clone(),
    )?;
    materialized_runtime.project_executor.execute_all_kernel(
        VectorKernelInput::from_eval_input(ExpressionEvalInput {
            params: ctx.query.params.as_ref(),
            columns: &filtered,
        }),
        ctx.query,
        &mut projected,
    )?;
    Ok(projected)
}

fn graph_project_filter_executors(
    filters: &[Expression],
    session: &paro_context::StatementContext,
) -> Vec<ExpressionExecutor> {
    filters
        .iter()
        .map(|filter| {
            ExpressionExecutor::with_expressions_for_session(std::slice::from_ref(filter), session)
        })
        .collect()
}

fn build_graph_project_materialized_runtime(
    ctx: &mut PipelineInitContext,
    spec: &GraphProjectSpec,
) -> Result<GraphProjectMaterializedRuntime> {
    let mut required_cols: HashMap<usize, Vec<usize>> = HashMap::new();
    for expr in spec.expressions.iter().chain(spec.filters.iter()) {
        collect_graph_project_table_refs(expr, spec.path_table_index, &mut required_cols);
    }
    for cols in required_cols.values_mut() {
        cols.sort_unstable();
        cols.dedup();
    }

    let mut mappings = spec
        .rowid_mappings
        .iter()
        .filter(|mapping| {
            required_cols
                .get(&mapping.table_index)
                .is_some_and(|required| !required.is_empty())
        })
        .collect::<Vec<_>>();
    mappings.sort_by_key(|mapping| mapping.table_index);
    mappings.dedup_by_key(|mapping| mapping.table_index);

    let mut table_fetches = Vec::with_capacity(mappings.len());
    let mut table_col_offsets = HashMap::new();
    let mut next_column_offset = 0usize;
    for mapping in mappings {
        let required = required_cols
            .get(&mapping.table_index)
            .expect("graph project required columns checked");
        let table_entry = ctx.query.session.catalog().get_table(
            &ctx.query.catalog,
            &mapping.schema_name,
            &mapping.table_name,
        )?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(table) => table,
            _ => return Err(paro_error::wrong_object_type("table", &mapping.table_name)),
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!("table \"{}\" has no storage", mapping.table_name))
        })?;
        ctx.query
            .transaction
            .read_tracker()
            .record_table_read(TableId::new(storage.table_id()));
        table_col_offsets.insert(mapping.table_index, next_column_offset);
        next_column_offset += table.columns.len();
        let column_ids = required
            .iter()
            .map(|&column| {
                u32::try_from(column).map_err(|_| {
                    paro_error::internal(format!("graph project column {column} exceeds u32"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let column_types = table
            .columns
            .iter()
            .map(|column| column.logical_type.clone())
            .collect::<Vec<_>>();
        table_fetches.push(GraphProjectTableFetchPlan {
            table_index: mapping.table_index,
            table_name: mapping.table_name.clone(),
            rowid_col_idx: mapping.rowid_col_idx,
            storage: storage.clone(),
            reader: None,
            rowids: Vec::new(),
            column_types: column_types.into_boxed_slice(),
            required_columns: required.clone().into_boxed_slice(),
            column_ids: column_ids.into_boxed_slice(),
            full_cols: vec![None; table.columns.len()],
        });
    }

    let mut path_columns = Vec::new();
    for expr in spec.expressions.iter().chain(spec.filters.iter()) {
        collect_graph_project_path_refs(expr, spec.path_table_index, &mut path_columns);
    }
    let mut seen = HashSet::new();
    path_columns.retain(|column| seen.insert(*column));

    let mut path_column_map = HashMap::new();
    for raw_col_idx in &path_columns {
        path_column_map.insert(*raw_col_idx, next_column_offset);
        next_column_offset += 1;
    }

    let remapped_filters = spec
        .filters
        .iter()
        .map(|expr| {
            remap_graph_project_expression(
                expr,
                spec.path_table_index,
                &table_col_offsets,
                &path_column_map,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let remapped_expressions = spec
        .expressions
        .iter()
        .map(|expr| {
            remap_graph_project_expression(
                expr,
                spec.path_table_index,
                &table_col_offsets,
                &path_column_map,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(GraphProjectMaterializedRuntime {
        table_fetches: table_fetches.into_boxed_slice(),
        path_columns: path_columns.into_boxed_slice(),
        filter_executors: graph_project_filter_executors(
            &remapped_filters,
            ctx.query.session.as_ref(),
        ),
        project_executor: ExpressionExecutor::with_expressions_for_session(
            &remapped_expressions,
            ctx.query.session.as_ref(),
        ),
    })
}

fn materialize_graph_project_input(
    ctx: &mut OperatorCallContext,
    runtime: &mut GraphProjectMaterializedRuntime,
    input: &Chunk,
) -> Result<Chunk> {
    let row_count = input.size();
    let mut combined_columns = Vec::new();
    for fetch in runtime.table_fetches.iter_mut() {
        let rowid_col = input.column(fetch.rowid_col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing graph rowid column {} for table \"{}\"",
                fetch.rowid_col_idx, fetch.table_name
            ))
        })?;
        fetch.rowids.clear();
        fetch.rowids.reserve(row_count);
        for row_idx in 0..row_count {
            fetch.rowids.push(read_u64_from_vector(
                rowid_col,
                row_idx,
                "graph project rowid",
            )?);
        }
        if fetch.reader.is_none() {
            let params =
                TabletReaderParams::with_version(ctx.query.transaction.visible_version_i64());
            let mut reader = fetch.storage.create_reader(params)?;
            reader.prepare()?;
            fetch.reader = Some(reader);
        }
        let reader = fetch
            .reader
            .as_ref()
            .expect("graph project table reader was initialized above");
        let fetched = reader.get_by_rowids(&fetch.rowids, &fetch.column_ids)?;

        fetch.full_cols.clear();
        fetch.full_cols.resize(fetch.column_types.len(), None);
        for (pos, &column_idx) in fetch.required_columns.iter().enumerate() {
            if let Some(column) = fetched.column(pos) {
                fetch.full_cols[column_idx] = Some(column.clone());
            }
        }
        for (column_idx, column) in fetch.full_cols.iter_mut().enumerate() {
            combined_columns.push(match column {
                Some(column) => column.clone(),
                None => graph_null_vector(
                    &fetch.column_types[column_idx],
                    row_count,
                    input.allocator().clone(),
                )?,
            });
        }
    }

    for &raw_col_idx in &runtime.path_columns {
        let raw_col = input.column(raw_col_idx).ok_or_else(|| {
            paro_error::internal(format!(
                "missing graph path column {} in expand output",
                raw_col_idx
            ))
        })?;
        combined_columns.push(raw_col.clone());
    }

    let chunk = if combined_columns.is_empty() {
        let mut empty = Chunk::try_initialize(&[], row_count.max(1), input.allocator().clone())?;
        empty.try_set_cardinality(row_count)?;
        empty
    } else {
        let mut chunk = Chunk::from_arc_vectors(combined_columns, input.allocator().clone());
        chunk.try_set_cardinality(row_count)?;
        chunk
    };
    Ok(chunk)
}

fn apply_graph_project_filters(
    ctx: &mut OperatorCallContext,
    filter_selection: &mut Option<SelectionVector>,
    mut chunk: Chunk,
    filter_executors: &mut [ExpressionExecutor],
) -> Result<Option<Chunk>> {
    if filter_executors.is_empty() {
        return Ok(Some(chunk));
    }
    let mut current_count = chunk.size();
    let allocator = chunk.allocator().clone();
    let mut current_selection: Option<SelectionVector> = None;
    let mut spare_selection = filter_selection.take();

    for executor in filter_executors {
        let mut output_selection = match spare_selection.take() {
            Some(mut selection) if selection.capacity() >= current_count.max(1) => {
                selection.set_len(current_count);
                selection
            }
            _ => SelectionVector::try_with_capacity(current_count.max(1), allocator.clone())?,
        };
        let selected = if let Some(selection) = current_selection.as_ref() {
            executor.select_kernel(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: &chunk,
                })
                .with_selection(Some(selection))
                .with_count(current_count),
                ctx.query,
                &mut output_selection,
            )?
        } else {
            executor.select_kernel(
                0,
                VectorKernelInput::from_eval_input(ExpressionEvalInput {
                    params: ctx.query.params.as_ref(),
                    columns: &chunk,
                })
                .with_count(current_count),
                ctx.query,
                &mut output_selection,
            )?
        };
        output_selection.set_len(selected);
        spare_selection = current_selection.take();
        current_selection = Some(output_selection);
        current_count = selected;
        if current_count == 0 {
            *filter_selection = current_selection.take().or(spare_selection);
            return Ok(None);
        }
    }

    if let Some(selection) = current_selection {
        chunk.try_slice(&selection, current_count)?;
        *filter_selection = Some(selection);
    } else {
        *filter_selection = spare_selection;
    }
    Ok(Some(chunk))
}

fn clone_chunk_refs(input: &Chunk) -> Chunk {
    let columns = (0..input.column_count())
        .filter_map(|idx| input.column(idx).cloned())
        .collect();
    let mut chunk = Chunk::from_arc_vectors(columns, input.allocator().clone());
    chunk.set_cardinality(input.size());
    chunk
}

fn remap_graph_project_expression(
    expr: &Expression,
    path_table_index: usize,
    table_col_offsets: &HashMap<usize, usize>,
    path_column_map: &HashMap<usize, usize>,
) -> Result<Expression> {
    let mut missing = None;
    visit_column_refs(expr, &mut |col_ref| {
        let binding = col_ref.binding;
        if binding.table_index == path_table_index {
            if !path_column_map.contains_key(&binding.column_index) {
                missing = Some(format!("path column {}", binding.column_index));
            }
        } else if !table_col_offsets.contains_key(&binding.table_index) {
            missing = Some(format!("table index {}", binding.table_index));
        }
    });
    if let Some(label) = missing {
        return Err(paro_error::internal(format!(
            "graph project expression references unmapped {label}"
        )));
    }
    Ok(expr.clone().replace_column_ref(&|col_ref| {
        let binding = col_ref.binding;
        let new_index = if binding.table_index == path_table_index {
            path_column_map
                .get(&binding.column_index)
                .copied()
                .expect("graph project path mapping validated")
        } else {
            table_col_offsets
                .get(&binding.table_index)
                .map(|offset| offset + binding.column_index)
                .expect("graph project table mapping validated")
        };
        Some(Expression::ColumnRef(ColumnRefExpression::new(
            ColumnBinding::new(binding.table_index, new_index),
            col_ref.return_type.clone(),
        )))
    }))
}

fn collect_graph_project_table_refs(
    expr: &Expression,
    path_table_index: usize,
    out: &mut HashMap<usize, Vec<usize>>,
) {
    visit_column_refs(expr, &mut |col_ref| {
        if col_ref.binding.table_index != path_table_index {
            out.entry(col_ref.binding.table_index)
                .or_default()
                .push(col_ref.binding.column_index);
        }
    });
}

fn collect_graph_project_path_refs(
    expr: &Expression,
    path_table_index: usize,
    out: &mut Vec<usize>,
) {
    visit_column_refs(expr, &mut |col_ref| {
        if col_ref.binding.table_index == path_table_index {
            out.push(col_ref.binding.column_index);
        }
    });
}

fn graph_null_vector(
    logical_type: &LogicalType,
    count: usize,
    allocator: Arc<dyn paro_common::allocator::Allocator>,
) -> Result<Arc<Vector>> {
    let mut vector = Vector::try_new(logical_type.clone(), count, allocator)?;
    vector.set_count(count);
    vector.validity_mut().set_all_invalid(count);
    Ok(Arc::new(vector))
}
