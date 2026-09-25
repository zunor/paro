// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search payloads retain typed provider semantics, not abbreviated EXPLAIN
//! strings. The catalog capability token remains part of the source contract.

use super::*;
use crate::physical::specs::*;

fn predicate(b: &mut StableFingerprintBuilder, p: &Option<SearchPredicateTemplate>) {
    b.write_u64(p.is_some() as u64);
    if let Some(p) = p {
        crate::physical::predicate_identity::encode_predicate(
            b,
            p.tree(),
            |b, value| match value {
                SearchPredicateValue::Bound(value) => {
                    b.write_u64(0);
                    crate::cascades::encode_value(b, value);
                }
                SearchPredicateValue::RuntimeParameter { slot, target_type } => {
                    b.write_u64(1);
                    b.write_u64(slot.index.index() as u64);
                    write_logical_type(b, &slot.ty);
                    write_logical_type(b, target_type);
                }
            },
        );
    }
}

fn source(
    b: &mut StableFingerprintBuilder,
    table: &TableCatalogEntry,
    token: &CapabilityToken,
    p: &Option<SearchPredicateTemplate>,
    columns: &[usize],
    contract: SearchFilterContract,
    materialization: Option<paro_storage::search::ExactFilterMaterialization>,
) {
    b.write_u64(table.base.base.object_id.raw());
    b.write_bytes(
        serde_json::to_string(token)
            .expect("typed search token")
            .as_bytes(),
    );
    predicate(b, p);
    write_hashed_slice(b, b"search-columns", columns);
    b.write_u64(match contract {
        SearchFilterContract::None => 0,
        SearchFilterContract::ExactSegmentRowSetNoResidual => 1,
    });
    use paro_storage::search::ExactFilterMaterialization as M;
    match materialization {
        None => b.write_u64(0),
        Some(M::ScalarIndex) => b.write_u64(1),
        Some(M::ColumnScan) => b.write_u64(2),
        Some(M::Mixed {
            indexed_rows,
            scanned_rows,
        }) => {
            b.write_u64(3);
            b.write_u64(indexed_rows);
            b.write_u64(scanned_rows);
        }
    }
}

fn vector(b: &mut StableFingerprintBuilder, s: &VectorSearchSpec) {
    b.write_u64(0);
    source(
        b,
        &s.table,
        &s.capability_token,
        &s.predicate,
        &s.projected_columns,
        s.filter_contract,
        s.filter_materialization,
    );
    b.write_u64(s.emit_score as u64);
    b.write_u64(s.column_id as u64);
    b.write_u64(s.k as u64);
    b.write_u64(s.distance as u64);
    use paro_storage::search::DenseVectorQuery;
    match &s.query {
        DenseVectorQuery::Literal(v) => {
            b.write_u64(0);
            b.write_u64(v.len() as u64);
            for f in v {
                b.write_u64(f.to_bits() as u64);
            }
        }
        DenseVectorQuery::RuntimeParameter { slot, dimension } => {
            b.write_u64(1);
            b.write_u64(slot.index.index() as u64);
            write_logical_type(b, &slot.ty);
            b.write_u64(*dimension as u64);
        }
    }
    // These structs have explicit serde fields and no maps or runtime state.
    b.write_bytes(
        serde_json::to_string(&s.params)
            .expect("typed search parameters")
            .as_bytes(),
    );
    b.write_bytes(
        serde_json::to_string(&s.filter_topology)
            .expect("typed filter topology")
            .as_bytes(),
    );
    b.write_u64(s.search_policy.ef_search as u64);
    b.write_bytes(
        serde_json::to_string(&s.search_policy.rerank_policy)
            .expect("typed rerank policy")
            .as_bytes(),
    );
    b.write_bytes(
        serde_json::to_string(&s.search_policy.distance_cost)
            .expect("typed distance profile")
            .as_bytes(),
    );
    b.write_bytes(
        serde_json::to_string(&s.search_policy.vector_encoding)
            .expect("typed encoding")
            .as_bytes(),
    );
}

fn sparse(b: &mut StableFingerprintBuilder, s: &SparseVectorSearchSpec) {
    b.write_u64(1);
    source(
        b,
        &s.table,
        &s.capability_token,
        &s.predicate,
        &s.projected_columns,
        s.filter_contract,
        s.filter_materialization,
    );
    b.write_u64(s.emit_score as u64);
    b.write_u64(s.column_id as u64);
    b.write_u64(s.k as u64);
    write_hashed_slice(b, b"sparse-dimensions", &s.query_vector.dims);
    b.write_u64(s.query_vector.weights.len() as u64);
    for weight in &s.query_vector.weights {
        b.write_u64(weight.to_bits() as u64);
    }
}

fn fulltext(b: &mut StableFingerprintBuilder, s: &FullTextSearchSpec) {
    b.write_u64(2);
    source(
        b,
        &s.table,
        &s.capability_token,
        &s.predicate,
        &s.projected_columns,
        s.filter_contract,
        s.filter_materialization,
    );
    b.write_u64(s.emit_score as u64);
    b.write_u64(s.column_id as u64);
    b.write_bytes(s.query.as_bytes());
    b.write_u64(s.query_kind as u64);
    b.write_bytes(s.config.as_bytes());
    b.write_u64(s.score_mode as u64);
    use paro_storage::search::SearchRequestMode;
    match s.mode {
        SearchRequestMode::Filter => b.write_u64(0),
        SearchRequestMode::TopK { limit } => {
            b.write_u64(1);
            b.write_u64(limit as u64);
        }
    }
}

pub(super) fn write(b: &mut StableFingerprintBuilder, kind: &PhysicalNodeKind) -> bool {
    b.write_bytes(b"paro.search-payload.v1");
    match kind {
        PhysicalNodeKind::VectorSearch(s) => vector(b, s),
        PhysicalNodeKind::SparseVectorSearch(s) => sparse(b, s),
        PhysicalNodeKind::FullTextSearch(s) => fulltext(b, s),
        PhysicalNodeKind::AdaptiveSearch(s) => {
            b.write_u64(3);
            crate::cascades::planner::encode_search_request(b, &s.request);
            match s.selected.as_ref() {
                SearchSourceSpec::Vector(s) => vector(b, s),
                SearchSourceSpec::Sparse(s) => sparse(b, s),
                SearchSourceSpec::FullText(s) => fulltext(b, s),
            }
        }
        _ => return false,
    }
    true
}
