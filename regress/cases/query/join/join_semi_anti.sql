# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- Semi/anti join coverage
-- @setup
DROP TABLE IF EXISTS join_semi_left;
-- @setup
DROP TABLE IF EXISTS join_semi_right;
-- @setup
DROP TABLE IF EXISTS join_semi_empty_right;
-- @setup
DROP TABLE IF EXISTS join_semi_cover_right;
-- @setup
DROP TABLE IF EXISTS join_semi_cover_right_full;
-- @setup
CREATE TABLE join_semi_left (
  id INT,
  grp INT,
  amount INT,
  tag TEXT
);
-- @setup
CREATE TABLE join_semi_right (
  id INT,
  grp INT,
  budget INT,
  flag TEXT
);
-- @setup
CREATE TABLE join_semi_empty_right (
  id INT,
  grp INT,
  budget INT,
  flag TEXT
);
-- @setup
CREATE TABLE join_semi_cover_right (
  id INT,
  grp INT
);
-- @setup
CREATE TABLE join_semi_cover_right_full (
  id INT,
  grp INT,
  budget INT,
  flag TEXT
);
-- @setup
INSERT INTO join_semi_left VALUES
  (1, 10, 50, 'l1'),
  (2, 10, 75, 'l2'),
  (3, 20, 60, 'l3'),
  (3, 20, 65, 'l3_dup'),
  (4, 20, 90, 'l4'),
  (5, 30, 100, 'l5');
-- @setup
INSERT INTO join_semi_right VALUES
  (1, 10, 1000, 'r1'),
  (2, 10, 900, 'r2'),
  (3, 20, 1500, 'r3'),
  (3, 20, 1600, 'r3_dup'),
  (6, 30, 500, 'r6');
-- @setup
INSERT INTO join_semi_cover_right VALUES
  (1, 10),
  (2, 10),
  (3, 20),
  (4, 20),
  (5, 30);
-- @setup
INSERT INTO join_semi_cover_right_full VALUES
  (1, 10, 1000, 'cover_r1'),
  (2, 10, 900, 'cover_r2'),
  (3, 20, 1500, 'cover_r3'),
  (3, 20, 1600, 'cover_r3_dup');

-- 1. Basic LEFT SEMI JOIN preserves matching left rows without fanout
-- @query rowsort
SELECT *
FROM join_semi_left AS l
SEMI JOIN join_semi_right AS r ON l.id = r.id;

-- 2. Basic LEFT ANTI JOIN keeps only unmatched left rows
-- @query rowsort
SELECT *
FROM join_semi_left AS l
ANTI JOIN join_semi_right AS r ON l.id = r.id;

-- 3. LEFT SEMI JOIN against a filtered RHS subquery
-- @query rowsort
SELECT *
FROM join_semi_left AS l
SEMI JOIN (
  SELECT id
  FROM join_semi_right
  WHERE budget >= 1500
) AS r ON l.id = r.id;

-- 4. LEFT ANTI JOIN with residual predicate on non-key columns
-- @query rowsort
SELECT *
FROM join_semi_left AS l
ANTI JOIN join_semi_right AS r
  ON l.id = r.id AND l.amount + r.budget > 1100;

-- 5. LEFT SEMI JOIN with empty RHS returns no rows
-- @query rowsort
SELECT *
FROM join_semi_left AS l
SEMI JOIN join_semi_empty_right AS r ON l.id = r.id;

-- 6. LEFT ANTI JOIN with empty RHS keeps every left row
-- @query rowsort
SELECT *
FROM join_semi_left AS l
ANTI JOIN join_semi_empty_right AS r ON l.id = r.id;

-- 7. LEFT ANTI JOIN with full coverage RHS returns no rows
-- @query rowsort
SELECT *
FROM join_semi_left AS l
ANTI JOIN join_semi_cover_right AS r ON l.id = r.id;

-- 8. Basic RIGHT SEMI JOIN outputs only right-side columns
-- @query rowsort
SELECT *
FROM join_semi_left AS l
RIGHT SEMI JOIN join_semi_right AS r ON l.id = r.id;

-- 9. Basic RIGHT ANTI JOIN outputs only unmatched right-side columns
-- @query rowsort
SELECT *
FROM join_semi_left AS l
RIGHT ANTI JOIN join_semi_right AS r ON l.id = r.id;

-- 10. RIGHT ANTI JOIN with all matched right rows returns no rows
-- @query rowsort
SELECT *
FROM join_semi_left AS l
RIGHT ANTI JOIN join_semi_cover_right_full AS r ON l.id = r.id;

-- @teardown
DROP TABLE IF EXISTS join_semi_left;
-- @teardown
DROP TABLE IF EXISTS join_semi_right;
-- @teardown
DROP TABLE IF EXISTS join_semi_empty_right;
-- @teardown
DROP TABLE IF EXISTS join_semi_cover_right;
-- @teardown
DROP TABLE IF EXISTS join_semi_cover_right_full;
