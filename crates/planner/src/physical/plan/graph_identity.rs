// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Typed graph and late-fetch payloads. Estimates and runtime handles are not
//! semantic identities; column mappings, predicates and path semantics are.

use super::*;

fn edge(b: &mut StableFingerprintBuilder, e: &paro_catalog::entry::EdgeTableInfo) {
    for value in [
        &e.table_name,
        &e.source_vertex_table,
        &e.destination_vertex_table,
        &e.label,
    ] {
        b.write_bytes(value.as_bytes());
    }
    b.write_u64(e.table_oid);
    for columns in [
        &e.key_column_ids,
        &e.source_key_column_ids,
        &e.source_ref_column_ids,
        &e.destination_key_column_ids,
        &e.destination_ref_column_ids,
        &e.property_column_ids,
    ] {
        write_hashed_slice(b, b"edge-columns", columns);
    }
}

fn direction(b: &mut StableFingerprintBuilder, d: crate::operator::graph_expand::ExpandDirection) {
    use crate::operator::graph_expand::ExpandDirection;
    b.write_u64(match d {
        ExpandDirection::Forward => 0,
        ExpandDirection::Backward => 1,
        ExpandDirection::Both => 2,
    });
}

fn expressions<'a>(
    b: &mut StableFingerprintBuilder,
    expressions: impl Iterator<Item = &'a Expression>,
) {
    let expressions = expressions.collect::<Vec<_>>();
    b.write_u64(expressions.len() as u64);
    for expr in expressions {
        b.write_fingerprint(
            crate::physical::scalar_identity::physical_expression_fingerprint(expr),
        );
    }
}

pub(super) fn write(b: &mut StableFingerprintBuilder, kind: &PhysicalNodeKind) -> bool {
    b.write_bytes(b"paro.graph-fetch-payload.v1");
    match kind {
        PhysicalNodeKind::GraphScan(s) => {
            b.write_u64(0);
            b.write_u64(s.vertex_info.table_oid);
            b.write_u64(s.table_index as u64);
            for value in [
                &s.vertex_info.table_name,
                &s.vertex_info.label,
                &s.label,
                &s.graph_name,
                &s.schema_name,
            ] {
                b.write_bytes(value.as_bytes());
            }
            write_hashed_slice(b, b"vertex-keys", &s.vertex_info.key_column_ids);
            write_hashed_slice(b, b"vertex-properties", &s.vertex_info.property_column_ids);
            expressions(b, s.filter.iter());
        }
        PhysicalNodeKind::GraphExpand(s) => {
            b.write_u64(1);
            edge(b, &s.edge_info);
            direction(b, s.direction);
            for value in [
                &s.graph_name,
                &s.schema_name,
                &s.source_label,
                &s.target_label,
                &s.target_table_name,
            ] {
                b.write_bytes(value.as_bytes());
            }
            for value in [
                s.source_table_index,
                s.edge_table_index,
                s.target_table_index,
                s.source_local_col_idx,
                s.source_rowid_col_idx,
            ] {
                b.write_u64(value as u64);
            }
            for value in [
                s.min_hops,
                s.max_hops,
                s.source_table_oid,
                s.target_table_oid,
                s.has_path_functions as u64,
            ] {
                b.write_u64(value);
            }
            expressions(b, s.edge_filter.iter());
            expressions(b, s.target_filter.iter());
        }
        PhysicalNodeKind::GraphShortestPath(s) => {
            b.write_u64(2);
            edge(b, &s.edge_info);
            direction(b, s.direction);
            for value in [
                &s.graph_name,
                &s.schema_name,
                &s.source_label,
                &s.target_label,
                &s.target_table_name,
            ] {
                b.write_bytes(value.as_bytes());
            }
            for value in [s.source_local_col_idx, s.source_rowid_col_idx] {
                b.write_u64(value as u64);
            }
            write_hashed(b, b"target-local-column", &s.target_local_col_idx);
            for value in [
                s.min_hops,
                s.max_hops,
                s.source_table_oid,
                s.target_table_oid,
                s.has_path_functions as u64,
            ] {
                b.write_u64(value);
            }
            use paro_parser::ast::PathMode;
            b.write_u64(match s.path_mode {
                None => 0,
                Some(PathMode::AnyShortest) => 1,
                Some(PathMode::AllShortest) => 2,
                Some(PathMode::Any) => 3,
                Some(PathMode::All) => 4,
            });
            expressions(b, s.target_filter.iter());
        }
        PhysicalNodeKind::GraphProject(s) => {
            b.write_u64(3);
            b.write_u64(s.carrier_table_index as u64);
            expressions(b, s.expressions.iter());
            expressions(b, s.filters.iter());
            b.write_u64(s.rowid_mappings.len() as u64);
            for m in &s.rowid_mappings {
                b.write_u64(m.table_index as u64);
                b.write_u64(m.rowid_col_idx as u64);
                b.write_bytes(m.table_name.as_bytes());
                b.write_bytes(m.schema_name.as_bytes());
            }
        }
        PhysicalNodeKind::RowFetch(s) => {
            b.write_u64(4);
            b.write_u64(s.mappings.len() as u64);
            for m in &s.mappings {
                b.write_u64(m.table_index as u64);
                b.write_u64(m.rowid_col_idx as u64);
                b.write_bytes(m.table_name.as_bytes());
                b.write_bytes(m.schema_name.as_bytes());
                write_hashed_slice(b, b"fetch-columns", &m.column_ids);
            }
            write_hashed_slice(b, b"raw-types", &s.raw_output_types);
            write_hashed_slice(b, b"raw-names", &s.raw_output_names);
            b.write_u64(s.projection.is_some() as u64);
            if let Some(p) = &s.projection {
                expressions(b, p.expressions.iter());
                b.write_u64(p.visible_count as u64);
                write_hashed_slice(b, b"projection-types", &p.output_types);
                write_hashed_slice(b, b"projection-names", &p.output_names);
            }
        }
        _ => return false,
    }
    true
}
