-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Recursive CTE: control-region root, completed-output path.
-- Linear recursion depth = cte_depth. Verifies memory stays bounded
-- during iteration without unbounded materialization.
WITH RECURSIVE counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM counter WHERE n < ${cte_depth}
)
SELECT n FROM counter;
