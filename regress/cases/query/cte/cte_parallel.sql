# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS cte_parallel_case;
CREATE TABLE cte_parallel_case(id INT, bucket INT, payload INT);
INSERT INTO cte_parallel_case
SELECT g, g % 16, g % 100
FROM generate_series(1, 8000) AS t(g);

SET threads = 1;
WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_case
)
SELECT count(*) AS total_rows, sum(payload) AS total_payload
FROM base;

WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_case
)
SELECT bucket, count(*) AS cnt
FROM base
WHERE bucket < 4
GROUP BY bucket
ORDER BY bucket;

SET threads = 4;
WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_case
)
SELECT count(*) AS total_rows, sum(payload) AS total_payload
FROM base;

WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_case
)
SELECT bucket, count(*) AS cnt
FROM base
WHERE bucket < 4
GROUP BY bucket
ORDER BY bucket;

SET threads = DEFAULT;

-- @teardown
DROP TABLE IF EXISTS cte_parallel_case;
