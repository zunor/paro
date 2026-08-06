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

-- DISTINCT keys are global across parallel sink-local states. The row count
-- spans many vectors and repeats every key in multiple scan morsels.
DROP TABLE IF EXISTS agg_d_parallel;
CREATE TABLE agg_d_parallel (
    grp INT,
    x INT,
    y INT,
    keep BOOLEAN
);
INSERT INTO agg_d_parallel
SELECT
    i % 8,
    i % 97,
    CASE WHEN i % 11 = 0 THEN NULL ELSE i % 53 END,
    i % 3 = 0
FROM generate_series(1, 50000) AS generated(i);

SET threads = 4;

SELECT
    count(DISTINCT x) AS cd_x,
    sum(DISTINCT x) AS sd_x,
    count(DISTINCT y) AS cd_y,
    sum(DISTINCT y) AS sd_y,
    count(DISTINCT x) FILTER (WHERE keep) AS cd_x_filtered
FROM agg_d_parallel;

SELECT
    grp,
    count(DISTINCT x) AS cd_x,
    sum(DISTINCT x) AS sd_x,
    count(DISTINCT y) AS cd_y,
    sum(DISTINCT y) AS sd_y,
    count(DISTINCT x) FILTER (WHERE keep) AS cd_x_filtered
FROM agg_d_parallel
GROUP BY grp
ORDER BY grp;

SET threads = DEFAULT;
DROP TABLE agg_d_parallel;

-- @teardown
DROP TABLE IF EXISTS agg_d_t;
