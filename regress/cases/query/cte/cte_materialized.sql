-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

WITH nums(v) AS MATERIALIZED (VALUES (1), (2), (3))
SELECT SUM(a.v + b.v) AS total
FROM nums AS a
CROSS JOIN nums AS b;

WITH nums(v) AS NOT MATERIALIZED (VALUES (1), (2), (3))
SELECT SUM(a.v + b.v) AS total
FROM nums AS a
CROSS JOIN nums AS b;

WITH nums(v) AS MATERIALIZED (VALUES (1), (2), (3))
SELECT v FROM nums WHERE v >= 2 ORDER BY v;

WITH nums(v) AS NOT MATERIALIZED (VALUES (1), (2), (3))
SELECT v FROM nums WHERE v >= 2 ORDER BY v;
