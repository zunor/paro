# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

EXPLAIN
WITH nums(v) AS (VALUES (1), (2), (3))
SELECT v FROM nums WHERE v > 1 ORDER BY v;

EXPLAIN
WITH nums(v) AS MATERIALIZED (VALUES (1), (2), (3))
SELECT v FROM nums WHERE v > 1 ORDER BY v;

EXPLAIN
WITH nums(v) AS NOT MATERIALIZED (VALUES (1), (2), (3))
SELECT SUM(a.v + b.v) AS total
FROM nums AS a
CROSS JOIN nums AS b;

WITH nums(v) AS NOT MATERIALIZED (VALUES (1), (2), (3))
SELECT SUM(a.v + b.v) AS total
FROM nums AS a
CROSS JOIN nums AS b;

WITH nums(v) AS MATERIALIZED (VALUES (1), (2), (3), (4)),
left_side(v) AS (SELECT v FROM nums WHERE v >= 2),
right_side(v) AS (SELECT v FROM nums WHERE v <= 3)
SELECT l.v AS left_v, r.v AS right_v
FROM left_side AS l
JOIN right_side AS r ON l.v = r.v
ORDER BY left_v, right_v;
