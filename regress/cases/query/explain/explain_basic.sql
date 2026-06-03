-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

EXPLAIN SELECT 1;

EXPLAIN (VERBOSE) SELECT 1;

-- @query json
EXPLAIN SELECT 1 FORMAT JSON;

DROP TABLE IF EXISTS explain_topn_rt;
CREATE TABLE explain_topn_rt (id INT, score INT);
INSERT INTO explain_topn_rt VALUES (1, 10), (2, 30), (3, 20), (4, 40);

EXPLAIN
SELECT id
FROM explain_topn_rt
ORDER BY score DESC
LIMIT 2;

EXPLAIN
SELECT id
FROM explain_topn_rt
ORDER BY score DESC
LIMIT 5 OFFSET 6000;

DROP TABLE explain_topn_rt;

DROP TABLE IF EXISTS explain_piecewise_left;
DROP TABLE IF EXISTS explain_piecewise_right;
CREATE TABLE explain_piecewise_left (id INT);
CREATE TABLE explain_piecewise_right (id INT);
INSERT INTO explain_piecewise_left VALUES (1), (3), (5);
INSERT INTO explain_piecewise_right VALUES (2), (4), (6);

EXPLAIN
SELECT *
FROM explain_piecewise_left AS l
JOIN explain_piecewise_right AS r ON l.id < r.id;

EXPLAIN (VERBOSE)
SELECT *
FROM explain_piecewise_left AS l
JOIN explain_piecewise_right AS r ON l.id < r.id;

-- @query json
EXPLAIN
SELECT *
FROM explain_piecewise_left AS l
JOIN explain_piecewise_right AS r ON l.id < r.id
FORMAT JSON;

DROP TABLE explain_piecewise_left;
DROP TABLE explain_piecewise_right;

DROP TABLE IF EXISTS explain_sort_range_left;
DROP TABLE IF EXISTS explain_sort_range_right;
CREATE TABLE explain_sort_range_left (x INT);
CREATE TABLE explain_sort_range_right (lo INT, hi INT);
INSERT INTO explain_sort_range_left VALUES (2), (5), (9);
INSERT INTO explain_sort_range_right VALUES (1, 3), (4, 6), (7, 10);

EXPLAIN
SELECT *
FROM explain_sort_range_left AS l
JOIN explain_sort_range_right AS r ON l.x BETWEEN r.lo AND r.hi;

EXPLAIN (VERBOSE)
SELECT *
FROM explain_sort_range_left AS l
JOIN explain_sort_range_right AS r ON l.x BETWEEN r.lo AND r.hi;

-- @query json
EXPLAIN
SELECT *
FROM explain_sort_range_left AS l
JOIN explain_sort_range_right AS r ON l.x BETWEEN r.lo AND r.hi
FORMAT JSON;

DROP TABLE explain_sort_range_left;
DROP TABLE explain_sort_range_right;
