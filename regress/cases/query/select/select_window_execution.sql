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

-- A zero-key aggregate window retains detail rows while sharing one complete
-- aggregate state. FILTER belongs to that shared state, not to detail replay.
SELECT
    x,
    sum(x) FILTER (WHERE keep) OVER () AS kept_sum,
    count(x) FILTER (WHERE keep) OVER () AS kept_count
FROM (VALUES (1, true), (2, false), (3, true)) AS global_aggregate_input(x, keep)
ORDER BY x;

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

-- Frame-sensitive value functions must read from the current row's frame, including empty frames.
SELECT
    x,
    last_value(x) OVER (ORDER BY x) AS default_last,
    first_value(x) OVER (
        ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING
    ) AS framed_first,
    last_value(x) OVER (
        ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
    ) AS framed_last,
    nth_value(x, 2) OVER (
        ORDER BY x ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS framed_nth,
    nth_value(x, x) OVER (
        ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS dynamic_nth
FROM (VALUES (1), (2), (3), (4)) AS framed_input(x)
ORDER BY x;

-- NULL treatment is applied while navigating within the frame.
SELECT
    seq,
    x,
    first_value(x) RESPECT NULLS OVER (
        ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_respect,
    first_value(x) IGNORE NULLS OVER (
        ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_ignore,
    last_value(x) RESPECT NULLS OVER (
        ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_respect,
    last_value(x) IGNORE NULLS OVER (
        ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_ignore,
    nth_value(x, 2) RESPECT NULLS OVER (
        ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS nth_respect,
    nth_value(x, 2) IGNORE NULLS OVER (
        ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS nth_ignore
FROM (VALUES (1, NULL), (2, 10), (3, 20), (4, NULL)) AS null_input(seq, x)
ORDER BY seq;

-- NTILE assigns remainder rows to the leading buckets and propagates a NULL count.
SELECT
    x,
    ntile(4) OVER (ORDER BY x) AS four_buckets,
    ntile(12) OVER (ORDER BY x) AS excess_buckets,
    ntile(NULL) OVER (ORDER BY x) AS null_buckets
FROM (
    VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
) AS ntile_input(x)
ORDER BY x;

-- LEAD/LAG offsets are evaluated at the current row, including NULL and zero offsets.
SELECT
    seq,
    x,
    offset_value,
    lead(x, offset_value, -1) OVER (ORDER BY seq) AS dynamic_lead
FROM (
    VALUES (1, 10, 1), (2, 20, 2), (3, 30, 0), (4, 40, NULL)
) AS dynamic_offset_input(seq, x, offset_value)
ORDER BY seq;

-- IGNORE NULLS navigates over non-NULL values; offset zero still addresses the current row.
SELECT
    seq,
    x,
    lead(x, 1, 99) IGNORE NULLS OVER (ORDER BY seq) AS next_non_null,
    lag(x, 1, 99) IGNORE NULLS OVER (ORDER BY seq) AS previous_non_null,
    lead(x, 0, 99) IGNORE NULLS OVER (ORDER BY seq) AS current_value
FROM (VALUES (1, NULL), (2, 10), (3, NULL), (4, 20)) AS navigation_input(seq, x)
ORDER BY seq;

-- @statement error argument of ntile must be greater than zero
SELECT ntile(0) OVER () FROM (VALUES (1)) AS invalid_ntile_input(x);

-- @statement error window frame offset must not be negative
SELECT last_value(x) OVER (
    ORDER BY x ROWS BETWEEN -1 PRECEDING AND CURRENT ROW
) FROM (VALUES (1)) AS negative_frame_input(x);

-- @statement error window frame offset must not be null
SELECT last_value(x) OVER (
    ORDER BY x ROWS BETWEEN NULL PRECEDING AND CURRENT ROW
) FROM (VALUES (1)) AS null_frame_input(x);

-- @statement error SQLSTATE=42P20
SELECT last_value(x) OVER (
    ORDER BY x ROWS BETWEEN CURRENT ROW AND 1 PRECEDING
) FROM (VALUES (1)) AS invalid_frame_input(x);
