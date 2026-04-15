-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

EXPLAIN
SELECT
    part,
    id,
    ROW_NUMBER() OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS rn
FROM memory_window;
