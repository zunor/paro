-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_sds_phase0_sparse;
DROP INDEX IF EXISTS idx_sds_phase0_hnsw;
DROP INDEX IF EXISTS idx_sds_phase0_fts;
DROP TABLE IF EXISTS bench_search_derived_state_phase0;
