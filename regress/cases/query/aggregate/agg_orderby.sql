-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_o_t;
CREATE TABLE agg_o_t (
    grp INT,
    x INT,
    y INT,
    label VARCHAR
);
INSERT INTO agg_o_t VALUES
    (1, 1, 1, 'a'),
    (1, 2, 2, 'b'),
    (1, 2, 3, 'c'),
    (2, 3, 1, 'd'),
    (2, 4, 4, 'e'),
    (2, NULL, 5, 'f');

-- Ungrouped ordered aggregates
SELECT
    first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS first_desc,
    last(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS last_desc
FROM agg_o_t;

-- Grouped ordered aggregates
SELECT
    grp,
    first(x) WITHIN GROUP (ORDER BY y ASC NULLS LAST) AS first_asc,
    last(x) WITHIN GROUP (ORDER BY y ASC NULLS LAST) AS last_asc
FROM agg_o_t
GROUP BY grp
ORDER BY grp;

-- Ordered string aggregate
SELECT
    grp,
    string_agg(label, '|') WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS labels_desc
FROM agg_o_t
GROUP BY grp
ORDER BY grp;

-- Ordered aggregate with FILTER
SELECT
    array_agg(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) FILTER (WHERE grp = 1) AS xs_grp1
FROM agg_o_t;

EXPLAIN SELECT
    first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS first_desc,
    string_agg(label, '|') WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS labels_desc
FROM (
    VALUES (1, 1, 'a'),
           (2, 2, 'b'),
           (2, 3, 'c'),
           (3, 1, 'd')
) AS agg_o_values(x, y, label);

-- @teardown
DROP TABLE IF EXISTS agg_o_t;
