-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Join/set operation NULL semantics coverage
-- @setup
DROP TABLE IF EXISTS join_null_left;
-- @setup
DROP TABLE IF EXISTS join_null_right;
-- @setup
DROP TABLE IF EXISTS setop_null_left;
-- @setup
DROP TABLE IF EXISTS setop_null_right;
-- @setup
CREATE TABLE join_null_left (
  k INT,
  label TEXT
);
-- @setup
CREATE TABLE join_null_right (
  k INT,
  tag TEXT
);
-- @setup
CREATE TABLE setop_null_left (v INT);
-- @setup
CREATE TABLE setop_null_right (v INT);
-- @setup
INSERT INTO join_null_left VALUES
  (1, 'l1'),
  (NULL, 'l_null_a'),
  (NULL, 'l_null_b'),
  (2, 'l2');
-- @setup
INSERT INTO join_null_right VALUES
  (1, 'r1'),
  (NULL, 'r_null_1'),
  (NULL, 'r_null_2'),
  (3, 'r3');
-- @setup
INSERT INTO setop_null_left VALUES
  (1),
  (1),
  (2),
  (NULL),
  (NULL);
-- @setup
INSERT INTO setop_null_right VALUES
  (2),
  (NULL),
  (3),
  (NULL);

-- 1. Scalar DISTINCT / NOT DISTINCT truth table
-- @query
SELECT
  NULL IS NOT DISTINCT FROM NULL AS null_eq_null,
  NULL IS DISTINCT FROM NULL AS null_neq_null,
  1 IS NOT DISTINCT FROM NULL AS one_eq_null,
  1 IS DISTINCT FROM NULL AS one_neq_null,
  1 IS NOT DISTINCT FROM 1 AS one_eq_one;

-- 2. Column expressions keep NULL-aware boolean semantics
-- @query rowsort
SELECT
  label,
  k IS NOT DISTINCT FROM NULL AS key_is_null,
  k IS DISTINCT FROM NULL AS key_is_not_null
FROM join_null_left
ORDER BY label;

-- 3. Equality join does not match NULL keys
-- @query rowsort
SELECT l.k, l.label, r.tag
FROM join_null_left AS l
JOIN join_null_right AS r ON l.k = r.k;

-- 4. IS NOT DISTINCT FROM matches NULL keys and preserves fanout
-- @query rowsort
SELECT l.k, l.label, r.tag
FROM (SELECT k, label FROM join_null_left) AS l
JOIN (SELECT k, tag FROM join_null_right) AS r ON l.k IS NOT DISTINCT FROM r.k;

-- 5. LEFT JOIN with equality keeps NULL-key probe rows unmatched
-- @query rowsort
SELECT l.k, l.label, r.tag
FROM join_null_left AS l
LEFT JOIN join_null_right AS r ON l.k = r.k;

-- 6. LEFT JOIN with IS NOT DISTINCT FROM matches NULL-key rows
-- @query rowsort
SELECT l.k, l.label, r.tag
FROM (SELECT k, label FROM join_null_left) AS l
LEFT JOIN (SELECT k, tag FROM join_null_right) AS r ON l.k IS NOT DISTINCT FROM r.k;

-- 7. ANTI JOIN with equality keeps NULL-key rows because NULL = NULL is unknown
-- @query rowsort
SELECT *
FROM join_null_left AS l
ANTI JOIN join_null_right AS r ON l.k = r.k;

-- 8. ANTI JOIN with IS NOT DISTINCT FROM removes NULL-key rows when RHS has NULL
-- @query rowsort
SELECT *
FROM (SELECT k, label FROM join_null_left) AS l
ANTI JOIN (SELECT k, tag FROM join_null_right) AS r ON l.k IS NOT DISTINCT FROM r.k;

-- 9. Scalar INTERSECT treats NULL as a comparable set value
-- @query rowsort
SELECT 2 INTERSECT SELECT 2;

-- 10. Scalar EXCEPT removes exact scalar matches
-- @query rowsort
SELECT 2 EXCEPT SELECT 2;

-- 11. Scalar INTERSECT treats NULL as a comparable set value
-- @query rowsort
SELECT NULL INTERSECT SELECT NULL;

-- 12. Scalar EXCEPT removes NULL when RHS also contains NULL
-- @query rowsort
SELECT NULL EXCEPT SELECT NULL;

-- 13. Table INTERSECT deduplicates NULL and non-NULL matches
-- @query rowsort
SELECT v
FROM (SELECT v FROM setop_null_left) AS l
INTERSECT
SELECT v
FROM (SELECT v FROM setop_null_right) AS r;

-- 14. Table EXCEPT removes NULL when RHS contains NULL
-- @query rowsort
SELECT v
FROM (SELECT v FROM setop_null_left) AS l
EXCEPT
SELECT v
FROM (SELECT v FROM setop_null_right) AS r;

-- @teardown
DROP TABLE IF EXISTS join_null_left;
-- @teardown
DROP TABLE IF EXISTS join_null_right;
-- @teardown
DROP TABLE IF EXISTS setop_null_left;
-- @teardown
DROP TABLE IF EXISTS setop_null_right;
