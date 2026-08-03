// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared property graph build/refresh helpers.

use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CreatePropertyGraphInfo};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_storage::index::graph::{
    EdgeBuildInput, GraphBuildInput, GraphProjectionIndex, GraphStatistics, VertexBuildInput,
    VertexKey,
};
use paro_storage::tablet::TabletReaderParams;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ScannedGraphInputs {
    pub vertex_inputs: Vec<VertexBuildInput>,
    pub edge_inputs: Vec<EdgeBuildInput>,
    pub indexed_through_ts: u64,
}

pub fn graph_data_dir(db_path: &str, graph_name: &str) -> PathBuf {
    let base = if db_path.is_empty() {
        PathBuf::from("data")
    } else {
        PathBuf::from(db_path)
    };
    base.join("graph").join(graph_name)
}

pub fn graph_staging_dir(db_path: &str, txn_id: u64, graph_name: &str) -> PathBuf {
    let base = if db_path.is_empty() {
        PathBuf::from("data")
    } else {
        PathBuf::from(db_path)
    };
    base.join(".txn-staging")
        .join(txn_id.to_string())
        .join("graph")
        .join(graph_name)
}

pub fn scan_graph_inputs_with_catalog(
    catalog: &ParoCatalog,
    txn: &CatalogSnapshot,
    pg_info: &CreatePropertyGraphInfo,
) -> Result<ScannedGraphInputs> {
    let mut vertex_inputs = Vec::with_capacity(pg_info.vertex_tables.len());
    let mut indexed_through_ts = 0u64;
    for vt in &pg_info.vertex_tables {
        let table_entry = catalog.get_table(txn, &pg_info.schema, &vt.table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(t) => t,
            _ => {
                return Err(paro_error::wrong_object_type("table", &vt.table_name));
            }
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!("Vertex table \"{}\" has no storage", vt.table_name))
        })?;

        let visible_version = storage.max_version();
        indexed_through_ts = indexed_through_ts.max(visible_version.max(0) as u64);
        let (projected_columns, key_positions) =
            prepare_projected_columns(&[vt.key_column_ids.as_slice()]);
        let params = TabletReaderParams::with_version(visible_version)
            .with_columns(projected_columns)
            .with_emit_row_id(true);
        let mut reader = storage.create_reader(params)?;
        reader.prepare()?;

        let mut keys_and_rowids = Vec::new();
        while let Some(chunk) = reader.get_next_chunk()? {
            let rowid_col = chunk
                .column(chunk.column_count() - 1)
                .ok_or_else(|| paro_error::internal("Missing rowid column in vertex scan"))?;
            for idx in 0..chunk.size() {
                let rowid_value = rowid_col.get_value(idx);
                let key = graph_vertex_key_from_chunk(
                    &chunk,
                    &key_positions[0],
                    idx,
                    &format!("vertex table \"{}\" key at row {}", vt.table_name, idx),
                )?;
                let rowid = graph_rowid_from_value(
                    &rowid_value,
                    &format!("vertex table \"{}\" rowid at row {}", vt.table_name, idx),
                )?;
                keys_and_rowids.push((key, rowid));
            }
        }

        vertex_inputs.push(VertexBuildInput {
            label: vt.label.clone(),
            keys_and_rowids,
        });
    }

    let mut edge_inputs = Vec::with_capacity(pg_info.edge_tables.len());
    for et in &pg_info.edge_tables {
        let table_entry = catalog.get_table(txn, &pg_info.schema, &et.table_name)?;
        let table = match table_entry.as_ref() {
            CatalogEntryEnum::Table(t) => t,
            _ => {
                return Err(paro_error::wrong_object_type("table", &et.table_name));
            }
        };
        let storage = table.get_storage().ok_or_else(|| {
            paro_error::internal(format!("Edge table \"{}\" has no storage", et.table_name))
        })?;

        let visible_version = storage.max_version();
        indexed_through_ts = indexed_through_ts.max(visible_version.max(0) as u64);
        let (projected_columns, key_positions) = prepare_projected_columns(&[
            et.source_key_column_ids.as_slice(),
            et.destination_key_column_ids.as_slice(),
        ]);
        let params = TabletReaderParams::with_version(visible_version)
            .with_columns(projected_columns)
            .with_emit_row_id(true);
        let mut reader = storage.create_reader(params)?;
        reader.prepare()?;

        let mut edges = Vec::new();
        while let Some(chunk) = reader.get_next_chunk()? {
            let rowid_col = chunk
                .column(chunk.column_count() - 1)
                .ok_or_else(|| paro_error::internal("Missing rowid column in edge scan"))?;
            for idx in 0..chunk.size() {
                let src_key = graph_vertex_key_from_chunk(
                    &chunk,
                    &key_positions[0],
                    idx,
                    &format!("edge table \"{}\" source key at row {}", et.table_name, idx),
                )?;
                let dst_key = graph_vertex_key_from_chunk(
                    &chunk,
                    &key_positions[1],
                    idx,
                    &format!(
                        "edge table \"{}\" destination key at row {}",
                        et.table_name, idx
                    ),
                )?;
                let rowid = graph_rowid_from_value(
                    &rowid_col.get_value(idx),
                    &format!("edge table \"{}\" rowid at row {}", et.table_name, idx),
                )?;
                edges.push((src_key, dst_key, rowid));
            }
        }

        edge_inputs.push(EdgeBuildInput {
            label: et.label.clone(),
            source_vertex_label: et.source_vertex_table.clone(),
            destination_vertex_label: et.destination_vertex_table.clone(),
            edges,
        });
    }

    let table_to_label: HashMap<&str, &str> = pg_info
        .vertex_tables
        .iter()
        .map(|vt| (vt.table_name.as_str(), vt.label.as_str()))
        .collect();
    for edge_input in &mut edge_inputs {
        if let Some(label) = table_to_label.get(edge_input.source_vertex_label.as_str()) {
            edge_input.source_vertex_label = (*label).to_string();
        }
        if let Some(label) = table_to_label.get(edge_input.destination_vertex_label.as_str()) {
            edge_input.destination_vertex_label = (*label).to_string();
        }
    }

    Ok(ScannedGraphInputs {
        vertex_inputs,
        edge_inputs,
        indexed_through_ts,
    })
}

pub fn build_graph_index_from_scans(
    graph_name: &str,
    scanned: &ScannedGraphInputs,
) -> Result<GraphProjectionIndex> {
    let build_input = build_graph_input_from_scans(graph_name, scanned);
    GraphProjectionIndex::build(&build_input)
}

pub fn build_graph_input_from_scans(
    graph_name: &str,
    scanned: &ScannedGraphInputs,
) -> GraphBuildInput {
    GraphBuildInput {
        graph_name: graph_name.to_string(),
        vertex_tables: scanned.vertex_inputs.clone(),
        edge_tables: scanned.edge_inputs.clone(),
        build_backward_adjacency: true,
    }
}

pub fn graph_statistics_from_scans(
    graph_name: &str,
    scanned: &ScannedGraphInputs,
) -> GraphStatistics {
    GraphStatistics::from_build_input(&build_graph_input_from_scans(graph_name, scanned))
}

fn graph_vertex_key_from_value(value: &Value, context: &str) -> Result<VertexKey> {
    match value {
        Value::BigInt(v) => Ok(VertexKey::Int64(*v)),
        Value::Varchar(v) => Ok(VertexKey::String(v.clone().into_boxed_str())),
        Value::Null(_) => Err(paro_error::internal(format!("Missing key for {}", context))),
        _ => Err(paro_error::internal(format!(
            "Unsupported graph key type {} for {}",
            value.logical_type(),
            context
        ))),
    }
}

fn prepare_projected_columns(column_groups: &[&[u32]]) -> (Vec<usize>, Vec<Vec<usize>>) {
    let mut projected = Vec::new();
    let mut index_by_column = HashMap::new();
    let mut positions = Vec::with_capacity(column_groups.len());
    for group in column_groups {
        let mut group_positions = Vec::with_capacity(group.len());
        for &column_id in *group {
            let position = if let Some(existing) = index_by_column.get(&column_id) {
                *existing
            } else {
                let next = projected.len();
                projected.push(column_id as usize);
                index_by_column.insert(column_id, next);
                next
            };
            group_positions.push(position);
        }
        positions.push(group_positions);
    }
    (projected, positions)
}

fn graph_vertex_key_from_chunk(
    chunk: &Chunk,
    positions: &[usize],
    row_idx: usize,
    context: &str,
) -> Result<VertexKey> {
    if positions.len() == 1 {
        let value = chunk
            .column(positions[0])
            .ok_or_else(|| paro_error::internal(format!("Missing key column for {}", context)))?
            .get_value(row_idx);
        return graph_vertex_key_from_value(&value, context);
    }

    let mut encoded = Vec::new();
    let count = u32::try_from(positions.len())
        .map_err(|_| paro_error::out_of_range("Composite graph key column count overflow"))?;
    encoded.extend_from_slice(&count.to_le_bytes());
    for &position in positions {
        let value = chunk
            .column(position)
            .ok_or_else(|| paro_error::internal(format!("Missing key column for {}", context)))?
            .get_value(row_idx);
        encode_composite_vertex_key_part(&value, context, &mut encoded)?;
    }
    Ok(VertexKey::Composite(encoded.into_boxed_slice()))
}

fn encode_composite_vertex_key_part(
    value: &Value,
    context: &str,
    encoded: &mut Vec<u8>,
) -> Result<()> {
    match value {
        Value::BigInt(v) => {
            encoded.push(1);
            encoded.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Value::Varchar(v) => {
            encoded.push(2);
            let len = u32::try_from(v.len()).map_err(|_| {
                paro_error::out_of_range("Composite graph key string length overflow")
            })?;
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(v.as_bytes());
            Ok(())
        }
        Value::Null(_) => Err(paro_error::internal(format!("Missing key for {}", context))),
        _ => Err(paro_error::internal(format!(
            "Unsupported graph key type {} for {}",
            value.logical_type(),
            context
        ))),
    }
}

fn graph_rowid_from_value(value: &Value, context: &str) -> Result<u64> {
    value
        .as_i64()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(|| paro_error::internal(format!("Missing or invalid rowid for {}", context)))
}
