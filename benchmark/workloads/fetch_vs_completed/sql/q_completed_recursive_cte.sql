-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Completed-output path: recursive CTE forces control-region root.
-- Measures: median latency, peak allocator bytes during iteration.
WITH RECURSIVE seq(n, acc) AS (
    SELECT 1, 1
    UNION ALL
    SELECT n + 1, acc + n + 1 FROM seq WHERE n < 1000
)
SELECT MAX(acc) FROM seq;
