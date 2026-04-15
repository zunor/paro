-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_src
)
SELECT COUNT(*)
FROM (
    SELECT id FROM base WHERE bucket < 64
    UNION ALL
    SELECT id FROM base WHERE bucket >= 64
) AS combined;
