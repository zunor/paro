// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;

use paro_catalog::entry::{ColumnDefinition, TableCatalogEntry};
use paro_common::chunk::Chunk;
use paro_common::types::LogicalType;
use paro_function::table::{BoundTableFunctionData, TableFunction};
use paro_planner::expression::Expression;
use paro_planner::operator::GetColumnSource;
use paro_storage::index::{collect_predicate_columns, ColumnId, PredicateTree};
use paro_storage::rowset::scan_cost::ScanAccessCostModel;
use paro_storage::table::segment_reorderer::SegmentOrderOptions;

#[derive(Debug, Clone)]
pub struct RowsetScanSpec {
    pub table_index: usize,
    pub output_names: Box<[String]>,
    pub returned_types: Box<[LogicalType]>,
    pub output_sources: Box<[GetColumnSource]>,
    pub relation_name: Option<String>,
    pub relation_alias: Option<String>,
    pub column_projection: RowsetColumnProjection,
    pub emit_row_id: bool,
    pub column_types: Box<[LogicalType]>,
    pub table: Arc<TableCatalogEntry>,
    pub predicate: Option<PredicateTree>,
    pub residual_predicates: Box<[Expression]>,
    pub access_policy: RowsetScanAccessPolicy,
    pub scan_order: Option<SegmentOrderOptions>,
    pub runtime_filter_expressions: Box<[Expression]>,
}

impl RowsetScanSpec {
    /// Access mode selected from predicates that are known while planning.
    ///
    /// Pipeline lowering may attach build-dependent predicates later. Runtime
    /// initialization resolves the policy again from that final predicate.
    pub fn planned_materialization(&self) -> RowsetScanMaterialization {
        let predicate_columns = self
            .predicate
            .as_ref()
            .map(collect_predicate_columns)
            .unwrap_or_default();
        self.access_policy.initial_materialization(
            &predicate_columns,
            &self.column_projection,
            &self.table.columns,
            false,
        )
    }
}

/// Compiled access policy for a rowset scan.
///
/// The policy deliberately stores inputs rather than a materialization choice:
/// hash-build and scalar bounds only exist when a pipeline starts, so the
/// final choice belongs to execution binding rather than physical planning.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowsetScanAccessPolicy {
    enabled: bool,
    static_selectivity: Option<f64>,
    cost_model: ScanAccessCostModel,
}

impl RowsetScanAccessPolicy {
    pub fn new(
        enabled: bool,
        static_selectivity: Option<f64>,
        cost_model: ScanAccessCostModel,
    ) -> Self {
        let static_selectivity = static_selectivity
            .filter(|selectivity| selectivity.is_finite())
            .map(|selectivity| selectivity.clamp(0.0, 1.0));
        Self {
            enabled,
            static_selectivity,
            cost_model,
        }
    }

    pub fn cost_model(self) -> ScanAccessCostModel {
        self.cost_model
    }

    /// Resolve the initial access mode from the effective execution predicate.
    ///
    /// Until runtime filters expose their own density estimates, the smaller
    /// of the static estimate and the model's unknown-selectivity hint is used
    /// as a stable starting heuristic for a conjunction. Segment readers then
    /// adapt in both directions from observed batch density.
    pub fn initial_materialization(
        self,
        predicate_columns: &[ColumnId],
        projection: &RowsetColumnProjection,
        table_columns: &[ColumnDefinition],
        has_runtime_conjunct: bool,
    ) -> RowsetScanMaterialization {
        if !self.enabled || predicate_columns.is_empty() {
            return RowsetScanMaterialization::Eager;
        }

        let predicate_column_ids = predicate_columns
            .iter()
            .map(|column_id| *column_id as usize)
            .collect::<HashSet<_>>();
        let output_columns = projection.columns().iter().copied().collect::<HashSet<_>>();
        let deferred_width = output_columns
            .iter()
            .filter(|column_id| !predicate_column_ids.contains(column_id))
            .filter_map(|column_id| table_columns.get(*column_id))
            .map(|column| self.cost_model.estimated_width(&column.logical_type))
            .sum::<usize>();
        if deferred_width == 0 {
            return RowsetScanMaterialization::Eager;
        }

        let predicate_width = predicate_column_ids
            .iter()
            .filter_map(|column_id| table_columns.get(*column_id))
            .map(|column| self.cost_model.estimated_width(&column.logical_type))
            .sum::<usize>();
        let eager_width = output_columns
            .union(&predicate_column_ids)
            .filter_map(|column_id| table_columns.get(*column_id))
            .map(|column| self.cost_model.estimated_width(&column.logical_type))
            .sum::<usize>();
        let selectivity = if has_runtime_conjunct {
            self.static_selectivity
                .map(|selectivity| selectivity.min(self.cost_model.unknown_selectivity()))
        } else {
            self.static_selectivity
        };
        if self.cost_model.late_materialization_is_cheaper(
            predicate_width,
            deferred_width,
            eager_width,
            selectivity,
        ) {
            RowsetScanMaterialization::Late
        } else {
            RowsetScanMaterialization::Eager
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsetScanMaterialization {
    Eager,
    Late,
}

impl RowsetScanMaterialization {
    pub fn is_late(self) -> bool {
        self == Self::Late
    }
}

/// Exact physical base-table projection.
///
/// An empty projection means that only row cardinality is requested. Reading
/// every table column is represented by listing every column explicitly at the
/// logical scan boundary, so no layer has to guess what an empty list means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsetColumnValueProjection {
    Stored,
    MatchedUtf8Prefix { byte_width: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsetColumnProjection {
    columns: Box<[usize]>,
    value_projections: Box<[RowsetColumnValueProjection]>,
}

impl RowsetColumnProjection {
    pub fn new(columns: impl Into<Box<[usize]>>) -> Self {
        let columns = columns.into();
        let value_projections =
            vec![RowsetColumnValueProjection::Stored; columns.len()].into_boxed_slice();
        Self {
            columns,
            value_projections,
        }
    }

    pub fn try_with_value_projections(
        columns: impl Into<Box<[usize]>>,
        value_projections: impl Into<Box<[RowsetColumnValueProjection]>>,
    ) -> paro_common::error::Result<Self> {
        let columns = columns.into();
        let value_projections = value_projections.into();
        if columns.len() != value_projections.len() {
            return Err(paro_common::error::internal(format!(
                "rowset projection width mismatch: columns={}, value projections={}",
                columns.len(),
                value_projections.len()
            )));
        }
        Ok(Self {
            columns,
            value_projections,
        })
    }

    pub fn column_id(&self, output_index: usize) -> Option<usize> {
        self.columns.get(output_index).copied()
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    pub fn value_projections(&self) -> &[RowsetColumnValueProjection] {
        &self.value_projections
    }
}

#[derive(Debug, Clone)]
pub struct DummyScanSpec;

#[derive(Debug, Clone)]
pub struct ValuesSpec {
    pub table_index: usize,
    pub relation_alias: Option<String>,
    pub expressions: Box<[Box<[Expression]>]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct EmptyResultSpec;

#[derive(Debug, Clone)]
pub struct FilterSpec {
    pub expressions: Box<[Expression]>,
    pub projection_map: Box<[usize]>,
}

#[derive(Debug, Clone)]
pub struct ProjectSpec {
    pub expressions: Box<[Expression]>,
    pub output_names: Box<[String]>,
    /// SQL-visible prefix owned by this projection. Remaining expressions are
    /// execution-only and either inherit a referenced identity or stay
    /// explicitly internal.
    pub visible_count: usize,
}

#[derive(Debug, Clone)]
pub struct LimitSpec {
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
    pub hnsw_ef_hint: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ChunkScanSpec {
    pub chunks: Arc<[Chunk]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct ExpressionScanSpec {
    pub table_index: usize,
    pub expressions: Box<[Box<[Expression]>]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
}

#[derive(Debug, Clone)]
pub struct TableFunctionScanSpec {
    pub function: Arc<TableFunction>,
    pub bind_data: Option<BoundTableFunctionData>,
    pub table_index: usize,
    pub arguments: Box<[Expression]>,
    pub projection_ids: Option<Box<[usize]>>,
    pub input_table_types: Box<[LogicalType]>,
    pub input_table_names: Box<[String]>,
    pub output_names: Box<[String]>,
    pub output_types: Box<[LogicalType]>,
    pub with_ordinality: bool,
}
