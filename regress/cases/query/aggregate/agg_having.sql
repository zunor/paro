-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_h_t;
CREATE TABLE agg_h_t (
    grp INT,
    x INT,
    flag BOOLEAN
);
INSERT INTO agg_h_t VALUES
    (1, 1, true),
    (1, 2, false),
    (1, 3, true),
    (2, 10, true),
    (2, NULL, false),
    (3, 1, false);

-- HAVING by row count
SELECT
    grp,
    count(*) AS cnt
FROM agg_h_t
GROUP BY grp
HAVING count(*) >= 2
ORDER BY grp;

-- HAVING by aggregate value
SELECT
    grp,
    sum(x) AS sum_x
FROM agg_h_t
GROUP BY grp
HAVING sum(x) > 3
ORDER BY grp;

-- Combined HAVING predicates
SELECT
    grp,
    sum(x) AS sum_x,
    count(x) AS cnt_x
FROM agg_h_t
GROUP BY grp
HAVING sum(x) >= 3 AND count(x) >= 2
ORDER BY grp;

-- HAVING without GROUP BY
SELECT
    sum(x) AS total_sum
FROM agg_h_t
HAVING sum(x) > 0;

EXPLAIN SELECT
    grp,
    sum(x) AS sum_x
FROM (
    VALUES (1, 1),
           (1, 2),
           (2, 3),
           (2, NULL),
           (3, 1)
) AS agg_h_values(grp, x)
GROUP BY grp
HAVING sum(x) >= 3
ORDER BY grp;

-- @teardown
DROP TABLE IF EXISTS agg_h_t;
