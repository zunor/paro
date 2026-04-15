-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_src
)
SELECT COUNT(*)
FROM base
WHERE payload >= 0;
