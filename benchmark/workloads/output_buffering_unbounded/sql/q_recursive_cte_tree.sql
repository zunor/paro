-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Recursive CTE with wider output: control-region root with moderate result set.
WITH RECURSIVE tree(id, parent_id, depth) AS (
    SELECT 1, 0, 0
    UNION ALL
    SELECT id + 1, id, depth + 1
    FROM tree
    WHERE depth < 100
)
SELECT COUNT(*) AS total_nodes FROM tree;
