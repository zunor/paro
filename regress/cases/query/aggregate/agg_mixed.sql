# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_m_fact;
DROP TABLE IF EXISTS agg_m_dim;

CREATE TABLE agg_m_dim (
    dim_id INT,
    category VARCHAR,
    flag BOOLEAN
);
CREATE TABLE agg_m_fact (
    id INT,
    dim_id INT,
    x INT
);

INSERT INTO agg_m_dim VALUES
    (1, 'A', true),
    (2, 'B', false),
    (3, 'A', false),
    (4, 'C', true);

INSERT INTO agg_m_fact VALUES
    (1, 1, 1),
    (2, 1, 2),
    (3, 2, 3),
    (4, 2, 4),
    (5, 3, 5),
    (6, 3, NULL),
    (7, 4, 7),
    (8, 4, 8);

-- Aggregate + join（先聚合事实表，再关联维表）
SELECT
    d.category,
    f.cnt_star,
    f.sum_x
FROM (
    SELECT
        dim_id,
        count(*) AS cnt_star,
        sum(x) AS sum_x
    FROM agg_m_fact
    GROUP BY dim_id
) AS f
JOIN agg_m_dim d ON f.dim_id = d.dim_id
ORDER BY d.category;

-- Aggregate + subquery
SELECT
    category,
    sum_x
FROM (
    SELECT
        d.category AS category,
        f.sum_x AS sum_x
    FROM (
        SELECT
            dim_id,
            sum(x) AS sum_x
        FROM agg_m_fact
        GROUP BY dim_id
    ) AS f
    JOIN agg_m_dim d ON f.dim_id = d.dim_id
) AS agg_m_sub
WHERE sum_x >= 6
ORDER BY category;

-- Aggregate + ORDER BY
SELECT
    d.category,
    f.sum_x
FROM (
    SELECT
        dim_id,
        sum(x) AS sum_x
    FROM agg_m_fact
    GROUP BY dim_id
) AS f
JOIN agg_m_dim d ON f.dim_id = d.dim_id
ORDER BY f.sum_x DESC, d.category;

-- Aggregate + window（EXPLAIN-only for interaction coverage）
EXPLAIN SELECT
    category,
    sum_x,
    row_number() OVER (ORDER BY sum_x DESC, category) AS rn
FROM (
    SELECT
        category,
        sum(x) AS sum_x
    FROM (
        VALUES ('A', 1),
               ('A', 2),
               ('B', 3),
               ('C', 4)
    ) AS agg_m_values(category, x)
    GROUP BY category
) AS agg_m_window
ORDER BY rn;

-- @teardown
DROP TABLE IF EXISTS agg_m_fact;
DROP TABLE IF EXISTS agg_m_dim;
