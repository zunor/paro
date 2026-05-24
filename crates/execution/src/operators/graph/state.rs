// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::VecDeque;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use paro_common::chunk::Chunk;
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::{SelectionVector, Vector};
use paro_storage::index::graph::GraphReadSnapshot;
use paro_storage::table::table_handle::TableHandle;
use paro_storage::tablet::TabletReader;

use crate::expression_executor::executor::ExpressionExecutor;

#[derive(Debug, Clone)]
pub struct GraphPathElement {
    pub(crate) table_oid: u64,
    pub(crate) rowid: u64,
}

#[derive(Debug, Clone)]
pub struct GraphPathPayload {
    pub(crate) vertices: Vec<GraphPathElement>,
    pub(crate) edges: Vec<GraphPathElement>,
}

impl GraphPathPayload {
    pub(crate) fn root(table_oid: u64, rowid: u64) -> Self {
        Self {
            vertices: vec![GraphPathElement { table_oid, rowid }],
            edges: Vec::new(),
        }
    }

    pub(crate) fn extend(
        &self,
        edge_table_oid: u64,
        edge_rowid: u64,
        target_table_oid: u64,
        target_rowid: u64,
    ) -> Self {
        let mut vertices = self.vertices.clone();
        vertices.push(GraphPathElement {
            table_oid: target_table_oid,
            rowid: target_rowid,
        });
        let mut edges = self.edges.clone();
        edges.push(GraphPathElement {
            table_oid: edge_table_oid,
            rowid: edge_rowid,
        });
        Self { vertices, edges }
    }

    pub(crate) fn hop_count(&self) -> i64 {
        self.edges.len() as i64
    }
}

pub(crate) fn graph_path_list_value(elements: &[GraphPathElement]) -> Value {
    Value::List(
        elements.iter().map(graph_path_element_value).collect(),
        graph_path_element_type(),
    )
}

pub(crate) fn graph_path_element_list_type() -> LogicalType {
    LogicalType::List(Box::new(graph_path_element_type()))
}

fn graph_path_element_value(element: &GraphPathElement) -> Value {
    Value::Struct(
        vec![
            Value::UBigInt(element.table_oid),
            Value::UBigInt(element.rowid),
        ],
        graph_path_element_fields(),
    )
}

fn graph_path_element_type() -> LogicalType {
    LogicalType::Struct(graph_path_element_fields())
}

fn graph_path_element_fields() -> Vec<(String, LogicalType)> {
    vec![
        ("table_oid".to_string(), LogicalType::UBigInt),
        ("rowid".to_string(), LogicalType::UBigInt),
    ]
}

#[derive(Debug)]
pub struct GraphExpandRow {
    pub(crate) input_row: usize,
    pub(crate) edge_rowid: u64,
    pub(crate) dst_local: u32,
    pub(crate) dst_rowid: u64,
    pub(crate) path: Option<GraphPathPayload>,
}

#[derive(Debug)]
pub struct GraphScanSourceGlobal {
    pub next_offset: AtomicU32,
    pub snapshot: GraphReadSnapshot,
    pub label: String,
    pub num_vertices: u32,
}

#[derive(Debug, Default)]
pub struct GraphScanSourceLocal {
    pub finished: bool,
    pub filter_scan: Option<GraphFilterScanState>,
    pub output: Option<Chunk>,
}

#[derive(Debug)]
pub struct GraphFilterScanState {
    pub reader: TabletReader,
    pub filter_executor: ExpressionExecutor,
    pub current_chunk: Option<Chunk>,
    pub current_filter: Option<Arc<Vector>>,
    pub current_row: usize,
}

#[derive(Debug)]
pub struct GraphExpandTransformGlobal {
    pub snapshot: GraphReadSnapshot,
    pub target_rowids: Arc<[u64]>,
    pub target_vertex_count: usize,
}

#[derive(Debug, Default)]
pub struct GraphExpandTransformLocal {
    pub ready: VecDeque<Chunk>,
    pub forward_scratch: Vec<(u32, u64)>,
    pub backward_scratch: Vec<(u32, u64)>,
    pub rows: Vec<GraphExpandRow>,
    pub input_selection: Option<SelectionVector>,
    pub seen_generation: Vec<u32>,
    pub current_generation: u32,
    pub frontier: Vec<u32>,
    pub next_frontier: Vec<u32>,
    pub path_frontier: Vec<GraphPathPayload>,
    pub path_next_frontier: Vec<GraphPathPayload>,
}

#[derive(Debug)]
pub struct GraphShortestPathTransformGlobal {
    pub snapshot: GraphReadSnapshot,
}

#[derive(Debug, Default)]
pub struct GraphShortestPathTransformLocal {
    pub ready: VecDeque<Chunk>,
    pub forward_scratch: Vec<(u32, u64)>,
    pub backward_scratch: Vec<(u32, u64)>,
    pub shortest_depths: Vec<u64>,
    pub frontier: VecDeque<u32>,
    pub next_frontier: VecDeque<u32>,
    pub path_frontier: VecDeque<GraphPathPayload>,
    pub path_next_frontier: VecDeque<GraphPathPayload>,
}

#[derive(Debug)]
pub struct GraphProjectTableFetchPlan {
    pub table_index: usize,
    pub table_name: String,
    pub rowid_col_idx: usize,
    pub storage: Arc<TableHandle>,
    pub reader: Option<TabletReader>,
    pub rowids: Vec<u64>,
    pub column_types: Box<[LogicalType]>,
    pub required_columns: Box<[usize]>,
    pub column_ids: Box<[u32]>,
    pub full_cols: Vec<Option<Arc<Vector>>>,
}

#[derive(Debug)]
pub struct GraphProjectMaterializedRuntime {
    pub table_fetches: Box<[GraphProjectTableFetchPlan]>,
    pub path_columns: Box<[usize]>,
    pub filter_executors: Vec<ExpressionExecutor>,
    pub project_executor: ExpressionExecutor,
}

#[derive(Debug, Default)]
pub struct GraphProjectTransformLocal {
    pub filter_selection: Option<SelectionVector>,
    pub raw_filter_executors: Vec<ExpressionExecutor>,
    pub raw_project_executor: Option<ExpressionExecutor>,
    pub materialized: Option<GraphProjectMaterializedRuntime>,
}
