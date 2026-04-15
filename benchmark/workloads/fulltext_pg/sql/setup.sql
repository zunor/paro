-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_bench_docs_pg_fts;
DROP TABLE IF EXISTS bench_docs_pg;

CREATE TABLE bench_docs_pg (
    id BIGINT PRIMARY KEY,
    category VARCHAR,
    title VARCHAR,
    content VARCHAR
);

INSERT INTO bench_docs_pg VALUES
    (1, 'tech', 'Vector Intro', 'vector database vector'),
    (2, 'tech', 'Vector Basics', 'vector database'),
    (3, 'tech', 'Vector Only', 'vector'),
    (4, 'life', 'Travel Note', 'mountain river'),
    (5, 'tech', 'Hybrid Search', 'vector graph database');

INSERT INTO bench_docs_pg
SELECT
    i + 1000,
    'noise',
    'Noise ' || i::VARCHAR,
    'lorem ipsum dolor sit amet'
FROM generate_series(1, ${rows}) AS t(i);
