# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

WITH nums(v) AS (VALUES (1), (2), (3))
SELECT v FROM nums ORDER BY v;

WITH nums(v) AS (VALUES (1), (2), (3))
SELECT a.v AS left_v, b.v AS right_v
FROM nums AS a
JOIN nums AS b ON a.v < b.v
ORDER BY left_v, right_v;

WITH base(v) AS (VALUES (1), (2), (3)),
filtered(v) AS (SELECT v FROM base WHERE v >= 2)
SELECT v FROM filtered ORDER BY v;

WITH vals(v) AS (VALUES (1), (2), (2), (3))
SELECT v FROM vals
UNION
SELECT v FROM vals WHERE v > 1
ORDER BY v;

WITH vals(v) AS (VALUES (1), (2), (2), (3))
SELECT v FROM vals
EXCEPT
SELECT v FROM vals WHERE v = 2
ORDER BY v;

WITH vals(v) AS (VALUES (1), (2), (2), (3))
SELECT v FROM vals
INTERSECT
SELECT v FROM vals WHERE v >= 2
ORDER BY v;
