-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

WITH vals(v) AS MATERIALIZED (VALUES (1), (2), (3))
SELECT v FROM vals WHERE v >= 2 ORDER BY v;

WITH RECURSIVE cnt(x) AS (
    VALUES (1)
    UNION ALL
    SELECT x + 1 FROM cnt WHERE x < 4
)
SELECT x FROM cnt ORDER BY x;

WITH RECURSIVE dedup(x) AS (
    VALUES (1), (1)
    UNION
    SELECT x + 1 FROM dedup WHERE x < 3
)
SELECT x FROM dedup ORDER BY x;
