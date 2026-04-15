-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_g_t;
CREATE TABLE agg_g_t (
    grp INT,
    subgrp INT,
    x INT,
    y INT
);
INSERT INTO agg_g_t VALUES
    (1, 1, 10, 1),
    (1, 1, 20, 2),
    (1, 2, NULL, 3),
    (2, 1, 30, 4),
    (2, NULL, 40, 5),
    (NULL, 1, 50, 6),
    (NULL, NULL, NULL, 7);

-- Single key grouped aggregates
SELECT
    grp,
    count(*) AS cnt_star,
    count(x) AS cnt_x,
    sum(x) AS sum_x,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_g_t
GROUP BY grp
ORDER BY grp;

-- Multi-key grouped aggregates with NULL key/payload
SELECT
    grp,
    subgrp,
    count(*) AS cnt_star,
    count(x) AS cnt_x,
    sum(x) AS sum_x
FROM agg_g_t
GROUP BY grp, subgrp
ORDER BY grp, subgrp;

-- Group on expression
SELECT
    coalesce(grp, -1) AS grp_key,
    sum(coalesce(x, 0)) AS sum_x
FROM agg_g_t
GROUP BY coalesce(grp, -1)
ORDER BY grp_key;

EXPLAIN SELECT
    grp,
    sum(x) AS sum_x
FROM (
    VALUES (1, 10),
           (1, 20),
           (2, 30),
           (2, NULL),
           (NULL, 40)
) AS agg_g_values(grp, x)
GROUP BY grp
ORDER BY grp;

-- @teardown
DROP TABLE IF EXISTS agg_g_t;
