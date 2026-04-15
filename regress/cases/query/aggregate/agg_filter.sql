-- @setup
DROP TABLE IF EXISTS agg_f_t;
CREATE TABLE agg_f_t (
    grp INT,
    x INT,
    y INT,
    flag BOOLEAN
);
INSERT INTO agg_f_t VALUES
    (1, 1, 1, true),
    (1, 2, 2, false),
    (1, 2, 3, true),
    (2, 3, 1, true),
    (2, 4, 4, false),
    (2, NULL, 5, true);

-- Ungrouped FILTER aggregates
SELECT
    sum(x) FILTER (WHERE flag) AS sum_true,
    sum(x) FILTER (WHERE NOT flag) AS sum_false,
    count(x) FILTER (WHERE x >= 2) AS cnt_x_ge2,
    sum(y) FILTER (WHERE x IS NULL) AS sum_y_x_null
FROM agg_f_t;

-- Grouped FILTER aggregates
SELECT
    grp,
    sum(x) FILTER (WHERE y >= 2) AS sum_y_ge2,
    sum(x) FILTER (WHERE flag) AS sum_true,
    count(DISTINCT y) FILTER (WHERE x >= 2) AS cd_y_x_ge2
FROM agg_f_t
GROUP BY grp
ORDER BY grp;

-- FILTER that matches no rows
SELECT
    grp,
    sum(x) FILTER (WHERE x > 100) AS sum_none
FROM agg_f_t
GROUP BY grp
ORDER BY grp;

EXPLAIN SELECT
    grp,
    sum(x) FILTER (WHERE flag) AS sum_true,
    count(x) FILTER (WHERE y >= 2) AS cnt_y_ge2
FROM (
    VALUES (1, 1, 1, true),
           (1, 2, 2, false),
           (1, 2, 3, true),
           (2, 3, 1, true),
           (2, 4, 4, false),
           (2, NULL, 5, true)
) AS agg_f_values(grp, x, y, flag)
GROUP BY grp
ORDER BY grp;

-- @teardown
DROP TABLE IF EXISTS agg_f_t;
