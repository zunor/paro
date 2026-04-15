-- Non-equality join (NLJ fallback) coverage
-- The matrix is intentionally progressive:
-- 1) basic `<` join fanout
-- 2) `a.x BETWEEN b.lo AND b.hi`
-- 3) arbitrary expression join predicate
-- 4) non-equality join with filtered RHS derived table
-- 5) LEFT SEMI join on non-equality predicate
-- 6) LEFT ANTI join on non-equality predicate
-- 7) LEFT join against empty RHS with non-equality predicate
-- 8) non-equality `<>` join aggregate sanity check

-- @setup
DROP TABLE IF EXISTS join_nonequal_left;

-- @setup
DROP TABLE IF EXISTS join_nonequal_right;

-- @setup
DROP TABLE IF EXISTS join_nonequal_right_empty;

-- @setup
CREATE TABLE join_nonequal_left (
  id INT,
  x INT,
  lo INT,
  hi INT,
  tag TEXT
);

-- @setup
CREATE TABLE join_nonequal_right (
  id INT,
  y INT,
  lo INT,
  hi INT,
  cls TEXT
);

-- @setup
CREATE TABLE join_nonequal_right_empty (
  id INT,
  y INT,
  lo INT,
  hi INT,
  cls TEXT
);

-- @setup
INSERT INTO join_nonequal_left VALUES
  (1, 1, 0, 2, 'l1'),
  (2, 3, 2, 4, 'l2'),
  (3, 5, 4, 6, 'l3'),
  (4, 7, 6, 8, 'l4'),
  (5, NULL, 1, 3, 'l_null_x'),
  (6, 9, NULL, 10, 'l_null_lo'),
  (7, 11, 10, 12, 'l7');

-- @setup
INSERT INTO join_nonequal_right VALUES
  (10, 2, 1, 2, 'r1'),
  (11, 4, 3, 5, 'r2'),
  (12, 6, 5, 7, 'r3'),
  (13, 8, 7, 9, 'r4'),
  (14, NULL, 9, 11, 'r_null_y'),
  (15, 12, 10, 12, 'r5');

-- 1. Basic non-equality `<` join
-- @query
SELECT l.id AS l_id, l.x, r.id AS r_id, r.y
FROM join_nonequal_left AS l
JOIN join_nonequal_right AS r ON l.x < r.y
ORDER BY l.id, r.id;

-- 2. BETWEEN join: `a.x BETWEEN b.lo AND b.hi`
-- @query
SELECT l.id AS l_id, l.x, r.id AS r_id, r.lo, r.hi
FROM join_nonequal_left AS l
JOIN join_nonequal_right AS r ON l.x BETWEEN r.lo AND r.hi
ORDER BY l.id, r.id;

-- 3. Arbitrary expression join predicate
-- @query
SELECT l.id AS l_id, r.id AS r_id, l.x, r.y
FROM join_nonequal_left AS l
JOIN join_nonequal_right AS r
  ON (l.x * 2) BETWEEN (r.y - 1) AND (r.y + 2)
 AND (l.id + r.id) % 2 = 1
ORDER BY l.id, r.id;

-- 4. Non-equality join with filtered RHS derived table
-- @query
SELECT l.id AS l_id, l.x, r.id AS r_id, r.y
FROM join_nonequal_left AS l
JOIN (
  SELECT id, y, cls
  FROM join_nonequal_right
  WHERE cls IN ('r2', 'r3', 'r4')
) AS r ON l.x + 1 <= r.y
ORDER BY l.id, r.id;

-- 5. LEFT SEMI JOIN on non-equality predicate
-- @query rowsort
SELECT *
FROM join_nonequal_left AS l
SEMI JOIN join_nonequal_right AS r ON l.x < r.y;

-- 6. LEFT ANTI JOIN on non-equality predicate
-- @query rowsort
SELECT *
FROM join_nonequal_left AS l
ANTI JOIN join_nonequal_right AS r ON l.x < r.y;

-- 7. LEFT JOIN against empty RHS with non-equality predicate
-- @query
SELECT l.id, l.tag, r.id AS r_id
FROM join_nonequal_left AS l
LEFT JOIN join_nonequal_right_empty AS r ON l.x < r.y
ORDER BY l.id;

-- 8. Non-equality `<>` join aggregate sanity check
-- @query
SELECT COUNT(*) AS neq_pairs
FROM join_nonequal_left AS l
JOIN join_nonequal_right AS r ON l.x <> r.y;

-- @teardown
DROP TABLE IF EXISTS join_nonequal_left;

-- @teardown
DROP TABLE IF EXISTS join_nonequal_right;

-- @teardown
DROP TABLE IF EXISTS join_nonequal_right_empty;
