-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_u_t;
CREATE TABLE agg_u_t (
    id INT,
    x INT,
    y INT,
    label VARCHAR
);
INSERT INTO agg_u_t VALUES
    (1, 1, 10, 'a'),
    (2, 2, 20, 'b'),
    (3, 2, 30, 'c'),
    (4, 4, NULL, 'd'),
    (5, NULL, 50, 'e');

-- Basic ungrouped aggregates
SELECT
    count(*) AS cnt_star,
    count(x) AS cnt_x,
    sum(x) AS sum_x,
    avg(x) AS avg_x,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_u_t;

-- Another payload column
SELECT
    min(y) AS min_y,
    max(y) AS max_y,
    sum(y) AS sum_y,
    avg(y) AS avg_y
FROM agg_u_t;

-- Explain uses VALUES to keep baseline stable
EXPLAIN SELECT
    count(*) AS cnt_star,
    sum(x) AS sum_x
FROM (
    VALUES (1),
           (2),
           (3),
           (NULL)
) AS agg_u_values(x);

SELECT
    count(*) AS cnt_star,
    sum(x) AS sum_x
FROM (
    VALUES (1),
           (2),
           (3),
           (NULL)
) AS agg_u_values(x);

-- @teardown
DROP TABLE IF EXISTS agg_u_t;
