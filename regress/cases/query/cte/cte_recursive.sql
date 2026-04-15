WITH RECURSIVE cnt(x) AS (
    VALUES (1)
    UNION ALL
    SELECT x + 1 FROM cnt WHERE x < 5
)
SELECT x FROM cnt ORDER BY x;

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

WITH RECURSIVE dedup(x) AS (
    VALUES (1), (1)
    UNION
    SELECT x + 1 FROM dedup WHERE x < 3
)
SELECT x FROM dedup ORDER BY x;

WITH RECURSIVE nums(v) AS (
    VALUES (1)
    UNION ALL
    VALUES (2)
)
SELECT v FROM nums ORDER BY v;
