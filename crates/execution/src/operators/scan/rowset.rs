// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use paro_common::allocator::MemoryTag;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::VECTOR_SIZE;
use paro_function::scalar::FunctionExecContext;

use paro_planner::operator::JoinComparisonType;
use paro_storage::index::{collect_predicate_columns, Predicate, PredicateTree};
use paro_storage::rowset::{RowsetSharedPtr, SegmentOptions, SegmentSharedPtr};
use paro_storage::table::segment_reorderer::{reorder_segments, SegmentOrderOptions};
use paro_storage::tablet::{ColumnProjection, TabletReaderParams};
use paro_storage::transaction::overlay_reader::TxnOverlayReader;

use crate::physical::specs::{RowsetColumnProjection, RowsetScanSpec};
use crate::pipeline::graph::{RowsetSourceSpec, ScalarFilterSemantics};
use crate::runtime::breaker::{HandleRef, JoinBuildHandle, MaterializedHandle, MaterializedReader};
use crate::runtime::context::{OperatorCallContext, PipelineInitContext};
use crate::runtime::source::SourcePoll;
use crate::runtime::state::{
    RowsetScanMorsel, RowsetSourceGlobal, RowsetSourceLocal, SourceGlobal, SourceLocal,
};

/// Bounds for scheduler-aware scan morsels.
///
/// Large scans retain coarse morsels so reader construction stays amortized.
/// Smaller scans are split just far enough to occupy the query's worker set;
/// this matters for single-segment dimension tables feeding blocking joins.
const MIN_ROWSET_MORSEL_ROWS: u64 = VECTOR_SIZE as u64;
const MAX_ROWSET_MORSEL_ROWS: u64 = 256 * 1024;

#[derive(Debug, Clone)]
pub struct RowsetSourceExec {
    pub desc: RowsetSourceDesc,
}

#[derive(Debug, Clone)]
pub struct RowsetSourceDesc {
    pub table_index: usize,
    pub table: Arc<paro_catalog::entry::TableCatalogEntry>,
    pub column_projection: RowsetColumnProjection,
    pub emit_row_id: bool,
    pub returned_types: Box<[paro_common::types::LogicalType]>,
    pub predicate: Option<PredicateTree>,
    pub late_materialize: bool,
    pub scan_access_cost: paro_storage::rowset::scan_cost::ScanAccessCostModel,
    pub scan_order: Option<SegmentOrderOptions>,
    pub dynamic_runtime_filters: Box<[RowsetDynamicRuntimeFilterDesc]>,
    pub dynamic_scalar_filters: Box<[RowsetDynamicScalarFilterDesc]>,
}

#[derive(Debug, Clone)]
pub struct RowsetDynamicRuntimeFilterDesc {
    pub handle: HandleRef<JoinBuildHandle>,
    pub build_key_index: usize,
    pub probe_column_id: u32,
}

#[derive(Debug, Clone)]
pub struct RowsetDynamicScalarFilterDesc {
    pub handle: HandleRef<MaterializedHandle>,
    pub build_column_index: usize,
    pub probe_column_id: u32,
    pub probe_type: LogicalType,
    pub comparison: JoinComparisonType,
    pub semantics: ScalarFilterSemantics,
}

impl RowsetSourceDesc {
    pub fn from_plan_spec(spec: &RowsetScanSpec) -> Self {
        Self {
            table_index: spec.table_index,
            table: spec.table.clone(),
            column_projection: spec.column_projection.clone(),
            emit_row_id: spec.emit_row_id,
            returned_types: spec.returned_types.clone(),
            predicate: spec.predicate.clone(),
            late_materialize: spec.late_materialize,
            scan_access_cost: spec.scan_access_cost,
            scan_order: spec.scan_order.clone(),
            dynamic_runtime_filters: Vec::new().into_boxed_slice(),
            dynamic_scalar_filters: Vec::new().into_boxed_slice(),
        }
    }

    pub fn from_source_spec(spec: &RowsetSourceSpec) -> Self {
        let mut desc = Self::from_plan_spec(&spec.scan);
        desc.dynamic_runtime_filters = spec
            .dynamic_runtime_filters
            .iter()
            .map(|filter| RowsetDynamicRuntimeFilterDesc {
                handle: HandleRef::new(filter.handle),
                build_key_index: filter.build_key_index,
                probe_column_id: filter.probe_column_id,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        desc.dynamic_scalar_filters = spec
            .dynamic_scalar_filters
            .iter()
            .map(|filter| RowsetDynamicScalarFilterDesc {
                handle: HandleRef::new(filter.handle),
                build_column_index: filter.build_column_index,
                probe_column_id: filter.probe_column_id,
                probe_type: filter.probe_type.clone(),
                comparison: filter.comparison,
                semantics: filter.semantics,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        desc
    }
}

impl RowsetSourceExec {
    pub(crate) fn create_global(&self, ctx: &mut PipelineInitContext) -> Result<SourceGlobal> {
        let table = self
            .desc
            .table
            .get_storage()
            .cloned()
            .ok_or_else(|| paro_error::internal("rowset scan table has no storage handle"))?;
        let storage_snapshot = table.storage_snapshot(
            ctx.query.transaction.read_ts(),
            ctx.query.transaction.read_snapshot().lease(),
        )?;
        let overlay = TxnOverlayReader::for_tablet(&table.tablet(), &ctx.query.transaction)?;
        let segment_options = SegmentOptions::default()
            .with_page_cache(ctx.query.session.page_cache().clone())
            .with_cache_decoded(true)
            .with_scan_access_cost(self.desc.scan_access_cost);
        let mut segments = storage_snapshot.segments_with_options(segment_options.clone())?;
        if let Some(overlay) = &overlay {
            let visible_rowsets = segments
                .iter()
                .map(|(rowset, _)| rowset.rowset_id())
                .collect::<HashSet<_>>();
            segments.extend(
                overlay
                    .segments_with_options(segment_options)?
                    .into_iter()
                    .filter(|(rowset, _)| !visible_rowsets.contains(&rowset.rowset_id())),
            );
        }
        if let Some(order) = self.desc.scan_order.as_ref() {
            reorder_segments(&mut segments, order);
        }
        let overlay_delete_vectors = overlay.as_ref().and_then(TxnOverlayReader::delete_vectors);
        let column_projection =
            ColumnProjection::new(self.desc.column_projection.columns().to_vec());
        let predicate = self.effective_predicate(ctx)?;
        let predicate_columns = predicate
            .as_ref()
            .map(collect_predicate_columns)
            .unwrap_or_default()
            .into_boxed_slice();
        let morsels = build_scan_morsels(&segments, ctx.query.session.number_of_threads().max(1));

        Ok(SourceGlobal::Rowset(Arc::new(RowsetSourceGlobal {
            table_index: self.desc.table_index,
            table,
            storage_snapshot,
            segments: segments.into_boxed_slice(),
            morsels,
            next_morsel: Default::default(),
            column_projection,
            overlay_delete_vectors,
            predicate,
            predicate_columns,
        })))
    }

    pub(crate) fn create_local(
        &self,
        _ctx: &mut PipelineInitContext,
        global: &SourceGlobal,
    ) -> Result<SourceLocal> {
        global.rowset()?;
        Ok(SourceLocal::Rowset(RowsetSourceLocal::default()))
    }

    pub(crate) fn poll_next(
        &self,
        ctx: &mut OperatorCallContext,
        global: &SourceGlobal,
        local: &mut SourceLocal,
        output: &mut Chunk,
    ) -> Result<SourcePoll> {
        let global = global.rowset()?;
        let local = local.rowset_mut()?;

        loop {
            ctx.cancel.check()?;
            if local.reader.is_none() {
                let morsel_idx = global.next_morsel.fetch_add(1, Ordering::AcqRel);
                let Some(morsel) = global.morsels.get(morsel_idx) else {
                    return Ok(SourcePoll::Finished);
                };
                let (rowset, segment) =
                    global.segments.get(morsel.segment_idx).ok_or_else(|| {
                        paro_error::internal("rowset scan morsel references an invalid segment")
                    })?;

                let mut params =
                    TabletReaderParams::with_version(global.storage_snapshot.visible_version())
                        .with_projection(global.column_projection.clone())
                        .with_emit_row_id(self.desc.emit_row_id)
                        .with_segment_handle(Arc::clone(segment))
                        .with_segment_ordinal_range(morsel.start_ordinal, morsel.end_ordinal);
                if let Some(predicate) = &global.predicate {
                    params = params.with_predicates(predicate.clone());
                    if self.desc.late_materialize && !global.predicate_columns.is_empty() {
                        params = params.with_late_materialize(global.predicate_columns.to_vec());
                    }
                }
                if let Some(delete_vectors) = &global.overlay_delete_vectors {
                    params = params.with_overlay_delete_vectors(Arc::clone(delete_vectors));
                }
                let mut reader = global.table.create_reader_with_allocator(
                    params,
                    ctx.query.allocator(MemoryTag::ColumnData),
                )?;
                reader.prepare_with_pinned_rowsets(vec![rowset.clone()])?;
                local.reader = Some(reader);
            }

            let reader = local.reader.as_mut().expect("rowset reader initialized");
            match reader.get_next_chunk()? {
                Some(mut chunk) => {
                    // The scratch chunk is only an ownership slot here; move
                    // the reader-owned vector array into it without cloning.
                    output.move_from(&mut chunk);
                    return Ok(SourcePoll::Output);
                }
                None => {
                    local.reader = None;
                }
            }
        }
    }

    fn effective_predicate(&self, ctx: &PipelineInitContext<'_>) -> Result<Option<PredicateTree>> {
        let mut predicates = Vec::new();
        if let Some(predicate) = &self.desc.predicate {
            predicates.push(predicate.clone());
        }
        for filter in &self.desc.dynamic_runtime_filters {
            let handle = ctx.handles.get(filter.handle)?;
            if !handle.runtime_filter_ready() {
                return Err(paro_error::internal(format!(
                    "hash join runtime filter {} was not published before rowset scan of {}",
                    filter.handle.id().index(),
                    self.desc.table.name()
                )));
            }
            if let Some(predicate) =
                handle.runtime_filter_predicate(filter.build_key_index, filter.probe_column_id)
            {
                predicates.push(predicate);
            }
        }
        for filter in &self.desc.dynamic_scalar_filters {
            let handle = ctx.handles.get(filter.handle)?;
            let reader = MaterializedReader::new(handle, "rowset scalar runtime filter");
            let chunks = reader.sealed_chunks()?;
            match materialized_scalar_value(chunks, filter.build_column_index)? {
                MaterializedScalarValue::Multiple
                    if filter.semantics == ScalarFilterSemantics::ExactSingleRow =>
                {
                    return Err(paro_error::internal(
                        "exact scalar runtime filter build emitted multiple rows",
                    ));
                }
                MaterializedScalarValue::Multiple => {}
                MaterializedScalarValue::Empty
                    if filter.semantics == ScalarFilterSemantics::ExactSingleRow =>
                {
                    return Err(paro_error::internal(
                        "exact scalar runtime filter build emitted no rows",
                    ));
                }
                MaterializedScalarValue::Empty => {
                    predicates.push(empty_predicate(filter.probe_column_id))
                }
                MaterializedScalarValue::One(value) => {
                    if filter.semantics == ScalarFilterSemantics::ExactSingleRow {
                        match exact_scalar_runtime_predicate(filter, value)? {
                            ExactScalarPredicate::AllRows => {}
                            ExactScalarPredicate::NoRows => {
                                predicates.push(empty_predicate(filter.probe_column_id));
                            }
                            ExactScalarPredicate::Predicate(predicate) => {
                                predicates.push(predicate);
                            }
                        }
                    } else if let Some(predicate) = scalar_runtime_predicate(filter, value) {
                        predicates.push(predicate);
                    }
                }
            }
        }
        Ok(combine_predicates(predicates))
    }
}

enum MaterializedScalarValue {
    Empty,
    One(Value),
    Multiple,
}

fn materialized_scalar_value(
    chunks: &[Chunk],
    column_index: usize,
) -> Result<MaterializedScalarValue> {
    let mut value = None;
    for chunk in chunks {
        for row_idx in 0..chunk.size() {
            if value.is_some() {
                return Ok(MaterializedScalarValue::Multiple);
            }
            value = Some(chunk.get_value(column_index, row_idx).ok_or_else(|| {
                paro_error::internal("materialized scalar filter column is missing")
            })?);
        }
    }
    Ok(value.map_or(MaterializedScalarValue::Empty, MaterializedScalarValue::One))
}

fn empty_predicate(column_id: u32) -> PredicateTree {
    PredicateTree::leaf(Predicate::In {
        column_id,
        values: Vec::new(),
    })
}

enum ExactScalarPredicate {
    AllRows,
    NoRows,
    Predicate(PredicateTree),
}

/// Compile a widened decimal comparison onto the discrete probe domain.
///
/// Unlike the conservative pruning path below, this preserves strictness. A
/// value at probe scale is an integer lattice point, so `probe > 12.345` is
/// exactly `probe > 12.34`, while `probe >= 12.345` is exactly
/// `probe >= 12.35`. Bounds outside the declared domain become constant true
/// or false instead of disabling the filter.
fn exact_scalar_runtime_predicate(
    filter: &RowsetDynamicScalarFilterDesc,
    value: Value,
) -> Result<ExactScalarPredicate> {
    if matches!(value, Value::Null(_)) {
        return Ok(ExactScalarPredicate::NoRows);
    }
    let (LogicalType::Decimal { precision, scale }, Value::Decimal(value, _, value_scale)) =
        (&filter.probe_type, value)
    else {
        return Err(paro_error::internal(
            "exact scalar runtime filter received a non-decimal bound",
        ));
    };
    if value_scale < *scale {
        return Err(paro_error::internal(
            "exact scalar runtime filter requires a non-narrowing decimal scale",
        ));
    }
    let divisor = 10_i128
        .checked_pow(u32::from(value_scale - *scale))
        .ok_or_else(|| paro_error::internal("decimal scalar-filter scale exceeds i128"))?;
    let truncated = value / divisor;
    let remainder = value % divisor;
    let floor = if remainder < 0 {
        truncated
            .checked_sub(1)
            .ok_or_else(|| paro_error::internal("decimal scalar-filter floor overflow"))?
    } else {
        truncated
    };
    let ceil = if remainder > 0 {
        truncated
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("decimal scalar-filter ceil overflow"))?
    } else {
        truncated
    };
    let limit = 10_i128
        .checked_pow(u32::from(*precision))
        .ok_or_else(|| paro_error::internal("decimal probe precision exceeds i128"))?;
    let minimum = -limit + 1;
    let maximum = limit - 1;
    let decimal = |bound| Value::Decimal(bound, *precision, *scale);
    let leaf = |predicate| ExactScalarPredicate::Predicate(PredicateTree::leaf(predicate));

    Ok(match filter.comparison {
        JoinComparisonType::Equal => {
            if remainder != 0 || truncated < minimum || truncated > maximum {
                ExactScalarPredicate::NoRows
            } else {
                leaf(Predicate::Eq {
                    column_id: filter.probe_column_id,
                    value: decimal(truncated),
                })
            }
        }
        JoinComparisonType::GreaterThan => {
            if floor < minimum {
                ExactScalarPredicate::AllRows
            } else if floor >= maximum {
                ExactScalarPredicate::NoRows
            } else {
                leaf(Predicate::Gt {
                    column_id: filter.probe_column_id,
                    value: decimal(floor),
                })
            }
        }
        JoinComparisonType::GreaterThanOrEqual => {
            if ceil <= minimum {
                ExactScalarPredicate::AllRows
            } else if ceil > maximum {
                ExactScalarPredicate::NoRows
            } else {
                leaf(Predicate::Ge {
                    column_id: filter.probe_column_id,
                    value: decimal(ceil),
                })
            }
        }
        JoinComparisonType::LessThan => {
            if ceil <= minimum {
                ExactScalarPredicate::NoRows
            } else if ceil > maximum {
                ExactScalarPredicate::AllRows
            } else {
                leaf(Predicate::Lt {
                    column_id: filter.probe_column_id,
                    value: decimal(ceil),
                })
            }
        }
        JoinComparisonType::LessThanOrEqual => {
            if floor < minimum {
                ExactScalarPredicate::NoRows
            } else if floor >= maximum {
                ExactScalarPredicate::AllRows
            } else {
                leaf(Predicate::Le {
                    column_id: filter.probe_column_id,
                    value: decimal(floor),
                })
            }
        }
        JoinComparisonType::NotEqual
        | JoinComparisonType::NotDistinctFrom
        | JoinComparisonType::DistinctFrom => {
            return Err(paro_error::internal(
                "unsupported exact scalar runtime comparison",
            ));
        }
    })
}

fn scalar_runtime_predicate(
    filter: &RowsetDynamicScalarFilterDesc,
    value: Value,
) -> Option<PredicateTree> {
    if matches!(value, Value::Null(_)) {
        return Some(empty_predicate(filter.probe_column_id));
    }
    let bound = match (&filter.probe_type, value) {
        (LogicalType::Decimal { precision, scale }, Value::Decimal(value, _, value_scale)) => {
            let rounding = match filter.comparison {
                JoinComparisonType::Equal => DecimalBoundaryRounding::Exact,
                JoinComparisonType::GreaterThan | JoinComparisonType::GreaterThanOrEqual => {
                    DecimalBoundaryRounding::Floor
                }
                JoinComparisonType::LessThan | JoinComparisonType::LessThanOrEqual => {
                    DecimalBoundaryRounding::Ceil
                }
                _ => return None,
            };
            match rescale_decimal_boundary(value, value_scale, *precision, *scale, rounding) {
                DecimalBoundary::Value(value) => Value::Decimal(value, *precision, *scale),
                DecimalBoundary::NoExactValue => {
                    return Some(empty_predicate(filter.probe_column_id));
                }
                DecimalBoundary::OutOfDomain => return None,
            }
        }
        (probe_type, value) if value.logical_type() == *probe_type => value,
        _ => return None,
    };
    let predicate = match filter.comparison {
        JoinComparisonType::Equal => Predicate::Eq {
            column_id: filter.probe_column_id,
            value: bound,
        },
        JoinComparisonType::GreaterThan | JoinComparisonType::GreaterThanOrEqual => Predicate::Ge {
            column_id: filter.probe_column_id,
            value: bound,
        },
        JoinComparisonType::LessThan | JoinComparisonType::LessThanOrEqual => Predicate::Le {
            column_id: filter.probe_column_id,
            value: bound,
        },
        _ => return None,
    };
    Some(PredicateTree::leaf(predicate))
}

#[derive(Clone, Copy)]
enum DecimalBoundaryRounding {
    Exact,
    Floor,
    Ceil,
}

enum DecimalBoundary {
    Value(i128),
    NoExactValue,
    OutOfDomain,
}

fn rescale_decimal_boundary(
    value: i128,
    value_scale: u8,
    precision: u8,
    target_scale: u8,
    rounding: DecimalBoundaryRounding,
) -> DecimalBoundary {
    let (scaled, exact) = if value_scale <= target_scale {
        let Some(factor) = 10_i128.checked_pow(u32::from(target_scale - value_scale)) else {
            return DecimalBoundary::OutOfDomain;
        };
        let Some(scaled) = value.checked_mul(factor) else {
            return DecimalBoundary::OutOfDomain;
        };
        (scaled, true)
    } else {
        let Some(divisor) = 10_i128.checked_pow(u32::from(value_scale - target_scale)) else {
            return DecimalBoundary::OutOfDomain;
        };
        let quotient = value / divisor;
        let remainder = value % divisor;
        let scaled = match rounding {
            DecimalBoundaryRounding::Exact | DecimalBoundaryRounding::Floor if remainder < 0 => {
                quotient.checked_sub(1)
            }
            DecimalBoundaryRounding::Ceil if remainder > 0 => quotient.checked_add(1),
            _ => Some(quotient),
        };
        let Some(scaled) = scaled else {
            return DecimalBoundary::OutOfDomain;
        };
        (scaled, remainder == 0)
    };
    if matches!(rounding, DecimalBoundaryRounding::Exact) && !exact {
        return DecimalBoundary::NoExactValue;
    }
    let Some(limit) = 10_i128.checked_pow(u32::from(precision)) else {
        return DecimalBoundary::OutOfDomain;
    };
    if scaled <= -limit || scaled >= limit {
        DecimalBoundary::OutOfDomain
    } else {
        DecimalBoundary::Value(scaled)
    }
}

fn build_scan_morsels(
    segments: &[(RowsetSharedPtr, SegmentSharedPtr)],
    parallelism: usize,
) -> Box<[RowsetScanMorsel]> {
    let total_rows = segments.iter().fold(0u64, |total, (_, segment)| {
        total.saturating_add(segment.num_rows())
    });
    let morsel_rows = rowset_morsel_rows(total_rows, parallelism);
    segments
        .iter()
        .enumerate()
        .flat_map(|(segment_idx, (_, segment))| {
            let row_count = segment.num_rows();
            (0..row_count)
                .step_by(morsel_rows as usize)
                .map(move |start_ordinal| RowsetScanMorsel {
                    segment_idx,
                    start_ordinal,
                    end_ordinal: row_count.min(start_ordinal + morsel_rows),
                })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn rowset_morsel_rows(total_rows: u64, parallelism: usize) -> u64 {
    if parallelism <= 1 {
        // Morsels are scheduling units, not storage batches. With only one
        // worker there is nobody to steal trailing work, so splitting a
        // segment merely rebuilds its reader and reopens its columns. Keep one
        // morsel per segment while respecting step_by's platform-sized input.
        let max_step = u64::try_from(usize::MAX).unwrap_or(u64::MAX);
        return total_rows.max(MIN_ROWSET_MORSEL_ROWS).min(max_step);
    }

    let parallelism = u64::try_from(parallelism).unwrap_or(u64::MAX).max(1);
    total_rows
        .div_ceil(parallelism)
        .clamp(MIN_ROWSET_MORSEL_ROWS, MAX_ROWSET_MORSEL_ROWS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar_filter(comparison: JoinComparisonType) -> RowsetDynamicScalarFilterDesc {
        RowsetDynamicScalarFilterDesc {
            handle: HandleRef::new(crate::pipeline::handles::BreakerHandleId::new(0)),
            build_column_index: 0,
            probe_column_id: 7,
            probe_type: LogicalType::Decimal {
                precision: 5,
                scale: 2,
            },
            comparison,
            semantics: ScalarFilterSemantics::Conservative,
        }
    }

    #[test]
    fn morsels_expose_workers_without_fragmenting_large_scans() {
        assert_eq!(rowset_morsel_rows(25, 4), MIN_ROWSET_MORSEL_ROWS);
        assert_eq!(rowset_morsel_rows(10_000, 4), MIN_ROWSET_MORSEL_ROWS);
        assert_eq!(rowset_morsel_rows(200_000, 4), 50_000);
        assert_eq!(rowset_morsel_rows(800_000, 4), 200_000);
        assert_eq!(rowset_morsel_rows(6_000_000, 4), MAX_ROWSET_MORSEL_ROWS);
    }

    #[test]
    fn morsel_policy_handles_empty_and_single_thread_scans() {
        assert_eq!(rowset_morsel_rows(0, 0), MIN_ROWSET_MORSEL_ROWS);
        assert_eq!(rowset_morsel_rows(200_000, 1), 200_000);
        assert_eq!(rowset_morsel_rows(6_000_000, 1), 6_000_000);
        assert_eq!(
            rowset_morsel_rows(u64::MAX, usize::MAX),
            MIN_ROWSET_MORSEL_ROWS
        );
    }

    #[test]
    fn decimal_runtime_bound_rounds_outward_for_negative_values() {
        assert!(matches!(
            rescale_decimal_boundary(-123, 2, 5, 1, DecimalBoundaryRounding::Floor),
            DecimalBoundary::Value(-13)
        ));
        assert!(matches!(
            rescale_decimal_boundary(-123, 2, 5, 1, DecimalBoundaryRounding::Ceil),
            DecimalBoundary::Value(-12)
        ));
        assert!(matches!(
            rescale_decimal_boundary(-120, 2, 5, 1, DecimalBoundaryRounding::Exact),
            DecimalBoundary::Value(-12)
        ));
        assert!(matches!(
            rescale_decimal_boundary(-123, 2, 5, 1, DecimalBoundaryRounding::Exact),
            DecimalBoundary::NoExactValue
        ));
    }

    #[test]
    fn inexact_scalar_equality_proves_an_empty_scan() {
        let predicate = scalar_runtime_predicate(
            &scalar_filter(JoinComparisonType::Equal),
            Value::Decimal(12_345, 8, 3),
        )
        .expect("decimal equality should compile");

        assert!(matches!(
            predicate,
            PredicateTree::Leaf(Predicate::In { column_id: 7, values }) if values.is_empty()
        ));
    }

    #[test]
    fn scalar_range_uses_a_conservative_storage_bound() {
        let predicate = scalar_runtime_predicate(
            &scalar_filter(JoinComparisonType::GreaterThan),
            Value::Decimal(12_345, 8, 3),
        )
        .expect("decimal range should compile");

        assert!(matches!(
            predicate,
            PredicateTree::Leaf(Predicate::Ge {
                column_id: 7,
                value: Value::Decimal(1_234, 5, 2),
            })
        ));
    }

    #[test]
    fn exact_decimal_scalar_filter_preserves_strict_lattice_boundaries() {
        let filter = scalar_filter(JoinComparisonType::GreaterThan);
        let predicate = exact_scalar_runtime_predicate(&filter, Value::Decimal(12_345, 8, 3))
            .expect("exact decimal range should compile");

        assert!(matches!(
            predicate,
            ExactScalarPredicate::Predicate(PredicateTree::Leaf(Predicate::Gt {
                column_id: 7,
                value: Value::Decimal(1_234, 5, 2),
            }))
        ));

        let filter = scalar_filter(JoinComparisonType::GreaterThanOrEqual);
        let predicate = exact_scalar_runtime_predicate(&filter, Value::Decimal(12_345, 8, 3))
            .expect("exact decimal range should compile");
        assert!(matches!(
            predicate,
            ExactScalarPredicate::Predicate(PredicateTree::Leaf(Predicate::Ge {
                column_id: 7,
                value: Value::Decimal(1_235, 5, 2),
            }))
        ));
    }

    #[test]
    fn exact_decimal_scalar_filter_proves_out_of_domain_results() {
        let filter = scalar_filter(JoinComparisonType::GreaterThan);
        assert!(matches!(
            exact_scalar_runtime_predicate(&filter, Value::Decimal(-1_000_000, 8, 2)).unwrap(),
            ExactScalarPredicate::AllRows
        ));
        assert!(matches!(
            exact_scalar_runtime_predicate(&filter, Value::Decimal(1_000_000, 8, 2)).unwrap(),
            ExactScalarPredicate::NoRows
        ));
    }
}

fn combine_predicates(predicates: Vec<PredicateTree>) -> Option<PredicateTree> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(PredicateTree::And(predicates)),
    }
}
