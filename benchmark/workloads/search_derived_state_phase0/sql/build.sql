-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE INVERTED INDEX idx_sds_phase0_fts
ON bench_search_derived_state_phase0 (content);

CREATE VECTOR INDEX idx_sds_phase0_hnsw
ON bench_search_derived_state_phase0 (emb);

CREATE VECTOR INDEX idx_sds_phase0_sparse
ON bench_search_derived_state_phase0 (sparse_vec) mode = 'sparse';
