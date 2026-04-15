-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_d_t;
CREATE TABLE agg_d_t (
    grp INT,
    x INT,
    y INT
);
INSERT INTO agg_d_t VALUES
    (1, 1, 10),
    (1, 1, 10),
    (1, 2, 20),
    (1, NULL, 30),
    (2, 2, 20),
    (2, 3, 20),
    (2, 3, NULL),
    (2, NULL, NULL);

-- Ungrouped DISTINCT aggregates
SELECT
    count(DISTINCT x) AS cd_x,
    sum(DISTINCT x) AS sd_x,
    count(DISTINCT y) AS cd_y
FROM agg_d_t;

-- Grouped DISTINCT aggregates
SELECT
    grp,
    count(DISTINCT x) AS cd_x,
    sum(DISTINCT x) AS sd_x,
    count(DISTINCT y) AS cd_y
FROM agg_d_t
GROUP BY grp
ORDER BY grp;

-- Mixed DISTINCT and non-DISTINCT aggregates
SELECT
    grp,
    sum(x) AS sum_all,
    sum(DISTINCT x) AS sum_distinct,
    count(*) AS cnt_star,
    count(DISTINCT x) AS cnt_distinct
FROM agg_d_t
GROUP BY grp
ORDER BY grp;

EXPLAIN SELECT
    grp,
    count(DISTINCT x) AS cd_x,
    sum(DISTINCT y) AS sd_y
FROM (
    VALUES (1, 1, 10),
           (1, 1, 10),
           (1, 2, 20),
           (2, 2, 20),
           (2, 3, 20),
           (2, 3, NULL)
) AS agg_d_values(grp, x, y)
GROUP BY grp
ORDER BY grp;

-- @teardown
DROP TABLE IF EXISTS agg_d_t;
