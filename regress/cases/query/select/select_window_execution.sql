-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Window expressions must be lowered out of scalar projections before execution.
SELECT x, row_number() OVER (ORDER BY x) AS rn
FROM (VALUES (3), (1), (2)) AS values_input(x)
ORDER BY x;

-- Aggregates nested in window clauses belong to the aggregate operator below the window.
SELECT
    category,
    sum(x) AS total,
    row_number() OVER (ORDER BY sum(x) DESC) AS rn
FROM (
    VALUES ('A', 1),
           ('A', 2),
           ('B', 7),
           ('C', 4)
) AS aggregate_input(category, x)
GROUP BY category
ORDER BY category;

-- Equal windows shared by SELECT and QUALIFY should have one window-runtime output.
EXPLAIN SELECT x, row_number() OVER (ORDER BY x) AS rn
FROM (VALUES (3), (1), (2)) AS qualify_input(x)
QUALIFY row_number() OVER (ORDER BY x) <= 2
ORDER BY x;

SELECT x, row_number() OVER (ORDER BY x) AS rn
FROM (VALUES (3), (1), (2)) AS qualify_input(x)
QUALIFY row_number() OVER (ORDER BY x) <= 2
ORDER BY x;

-- Pruning the only window output should remove the now-empty window operator.
EXPLAIN SELECT x
FROM (
    SELECT x, row_number() OVER (ORDER BY x) AS unused_rn
    FROM (VALUES (3), (1), (2)) AS unused_window_input(x)
) AS unused_window
ORDER BY x;

-- Different partition/order layouts require separate window runtimes, with each output binding
-- preserved through the stacked operators.
SELECT
    category,
    x,
    row_number() OVER (PARTITION BY category ORDER BY x) AS category_rn,
    row_number() OVER (ORDER BY x DESC) AS global_desc_rn,
    rank() OVER (PARTITION BY category ORDER BY x) AS category_rank
FROM (VALUES ('A', 1), ('A', 3), ('B', 2), ('B', 4)) AS mixed_layout_input(category, x)
ORDER BY x;

-- QUALIFY references must be remapped when their window uses a different runtime layout
-- from the windows projected by SELECT.
SELECT
    category,
    x,
    row_number() OVER (PARTITION BY category ORDER BY x) AS category_rn
FROM (VALUES ('A', 1), ('A', 3), ('B', 2), ('B', 4)) AS mixed_qualify_input(category, x)
QUALIFY row_number() OVER (ORDER BY x DESC) <= 2
ORDER BY x;
