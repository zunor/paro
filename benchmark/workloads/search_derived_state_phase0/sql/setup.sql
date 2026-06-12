-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_sds_phase0_sparse;
DROP INDEX IF EXISTS idx_sds_phase0_hnsw;
DROP INDEX IF EXISTS idx_sds_phase0_fts;
DROP TABLE IF EXISTS bench_search_derived_state_phase0;

CREATE TABLE bench_search_derived_state_phase0 (
    id BIGINT PRIMARY KEY,
    category VARCHAR,
    content VARCHAR,
    sparse_vec VARCHAR,
    emb VECTOR(3),
    payload VARCHAR,
    fixed_payload BIGINT,
    nullable_payload VARCHAR
);

INSERT INTO bench_search_derived_state_phase0 VALUES
    (1, 'seed', 'vector database vector', '{1:1.0,3:0.5}', '[0.90,0.10,0.70]', 'payload alpha', 10, 'nullable alpha'),
    (2, 'seed', 'vector database', '{1:0.9,2:0.1}', '[0.80,0.20,0.60]', 'payload beta', 20, NULL),
    (3, 'seed', 'vector search', '{3:1.0}', '[0.10,0.90,0.20]', 'payload gamma', 30, 'nullable gamma'),
    (4, 'seed', 'database systems', '{2:1.0,4:1.0}', '[0.95,0.05,0.72]', 'payload delta', 40, 'nullable delta'),
    (5, 'seed', 'mountain river', '{1:0.2,3:0.1}', '[0.30,0.40,0.90]', 'payload epsilon', 50, NULL);

INSERT INTO bench_search_derived_state_phase0
SELECT
    i + 1000,
    'noise',
    'lorem ipsum durable rowset materialization',
    '{10:1.0,11:0.5}',
    '[5.0,5.0,5.0]',
    'payload noise ' || i::VARCHAR,
    (i + 1000) * 10,
    CASE WHEN i % 5 = 0 THEN NULL ELSE 'nullable noise ' || i::VARCHAR END
FROM generate_series(1, ${rows}) AS t(i);
