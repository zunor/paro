-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_bench_docs_pg_long_fts;
DROP TABLE IF EXISTS bench_docs_pg_long;

CREATE TABLE bench_docs_pg_long (
    id BIGINT PRIMARY KEY,
    category VARCHAR,
    title VARCHAR,
    content VARCHAR
);

INSERT INTO bench_docs_pg_long
SELECT
    1,
    'tech',
    'Long Rank',
    string_agg('vector database', ' ') WITHIN GROUP (ORDER BY i)
FROM generate_series(1, 5000) AS t(i);

INSERT INTO bench_docs_pg_long VALUES
    (2, 'noise', 'Noise 2', 'lorem ipsum dolor sit amet'),
    (3, 'noise', 'Noise 3', 'alpha beta gamma');
