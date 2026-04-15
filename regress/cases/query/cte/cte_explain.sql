# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

EXPLAIN
WITH base(id, grp, amount) AS MATERIALIZED (
    VALUES
        (1, 'east', 10),
        (2, 'east', 20),
        (3, 'west', 15),
        (4, 'west', 30),
        (5, 'north', 5)
),
agg AS (
    SELECT grp, SUM(amount) AS total
    FROM base
    GROUP BY grp
),
peers AS (
    SELECT l.id, r.id AS peer_id, l.grp
    FROM base AS l
    JOIN base AS r ON l.grp = r.grp AND l.id < r.id
)
SELECT p.id, p.peer_id, a.total
FROM peers AS p
JOIN agg AS a ON a.grp = p.grp
ORDER BY p.id, p.peer_id;

-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE
WITH base(id, grp, amount) AS MATERIALIZED (
    VALUES
        (1, 'east', 10),
        (2, 'east', 20),
        (3, 'west', 15),
        (4, 'west', 30),
        (5, 'north', 5)
),
agg AS (
    SELECT grp, SUM(amount) AS total
    FROM base
    GROUP BY grp
),
peers AS (
    SELECT l.id, r.id AS peer_id, l.grp
    FROM base AS l
    JOIN base AS r ON l.grp = r.grp AND l.id < r.id
)
SELECT p.id, p.peer_id, a.total
FROM peers AS p
JOIN agg AS a ON a.grp = p.grp
ORDER BY p.id, p.peer_id;

WITH base(id, grp, amount) AS MATERIALIZED (
    VALUES
        (1, 'east', 10),
        (2, 'east', 20),
        (3, 'west', 15),
        (4, 'west', 30),
        (5, 'north', 5)
),
agg AS (
    SELECT grp, SUM(amount) AS total
    FROM base
    GROUP BY grp
),
peers AS (
    SELECT l.id, r.id AS peer_id, l.grp
    FROM base AS l
    JOIN base AS r ON l.grp = r.grp AND l.id < r.id
)
SELECT p.id, p.peer_id, a.total
FROM peers AS p
JOIN agg AS a ON a.grp = p.grp
ORDER BY p.id, p.peer_id;

-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE
WITH RECURSIVE
edges(src, dst) AS (
    VALUES (1, 2), (1, 3), (2, 4), (3, 5), (5, 6)
),
walk(node, depth) AS (
    VALUES (1, 0)
    UNION ALL
    SELECT e.dst, w.depth + 1
    FROM walk AS w
    JOIN edges AS e ON e.src = w.node
    WHERE w.depth < 2
)
SELECT node, depth FROM walk ORDER BY depth, node;
