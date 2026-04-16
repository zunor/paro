-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE INDEX idx_bench_docs_pg_long_fts
ON bench_docs_pg_long USING GIN (to_tsvector('simple', content));
