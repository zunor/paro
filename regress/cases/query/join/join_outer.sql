-- Outer join coverage
-- @setup
DROP TABLE IF EXISTS join_outer_left;
-- @setup
DROP TABLE IF EXISTS join_outer_right;
-- @setup
DROP TABLE IF EXISTS join_outer_empty_left;
-- @setup
DROP TABLE IF EXISTS join_outer_empty_right;
-- @setup
DROP TABLE IF EXISTS join_outer_no_left_rows;
-- @setup
DROP TABLE IF EXISTS join_outer_large_left;
-- @setup
DROP TABLE IF EXISTS join_outer_small_right;
-- @setup
CREATE TABLE join_outer_left (id INT, label TEXT);
-- @setup
CREATE TABLE join_outer_right (id INT, descr TEXT);
-- @setup
CREATE TABLE join_outer_empty_left (id INT, label TEXT);
-- @setup
CREATE TABLE join_outer_empty_right (id INT, descr TEXT);
-- @setup
CREATE TABLE join_outer_no_left_rows (id INT, label TEXT);
-- @setup
CREATE TABLE join_outer_large_left (id INT, payload TEXT);
-- @setup
CREATE TABLE join_outer_small_right (id INT, tag TEXT);
-- @setup
INSERT INTO join_outer_left VALUES
  (1, 'l1'),
  (2, 'l2'),
  (4, 'l4');
-- @setup
INSERT INTO join_outer_right VALUES
  (2, 'r2'),
  (3, 'r3'),
  (4, 'r4');
-- @setup
INSERT INTO join_outer_empty_left VALUES
  (10, 'el10'),
  (20, 'el20');
-- @setup
INSERT INTO join_outer_large_left VALUES
  (1, 'big-1'),
  (2, 'big-2'),
  (3, 'big-3'),
  (4, 'big-4');
-- @setup
INSERT INTO join_outer_small_right VALUES
  (2, 'small-2'),
  (4, 'small-4');

-- 1. Basic LEFT OUTER JOIN with NULL fill on unmatched left rows
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_left AS l
LEFT JOIN join_outer_right AS r ON l.id = r.id
ORDER BY COALESCE(l.id, r.id), COALESCE(l.label, ''), COALESCE(r.descr, '');

-- 2. Basic RIGHT OUTER JOIN with NULL fill on unmatched right rows
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_left AS l
RIGHT JOIN join_outer_right AS r ON l.id = r.id;

-- 3. Basic FULL OUTER JOIN with unmatched rows from both sides
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_left AS l
FULL JOIN join_outer_right AS r ON l.id = r.id;

-- 4. LEFT OUTER JOIN with empty RHS keeps every left row
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_empty_left AS l
LEFT JOIN join_outer_empty_right AS r ON l.id = r.id
ORDER BY l.id;

-- 5. RIGHT OUTER JOIN with empty LHS keeps every right row
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_no_left_rows AS l
RIGHT JOIN join_outer_right AS r ON l.id = r.id;

-- 6. FULL OUTER JOIN with empty RHS degenerates to left rows plus NULLs
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_empty_left AS l
FULL JOIN join_outer_empty_right AS r ON l.id = r.id;

-- 7. FULL OUTER JOIN with empty LHS degenerates to right rows plus NULLs
SELECT l.id AS left_id, l.label, r.id AS right_id, r.descr
FROM join_outer_no_left_rows AS l
FULL JOIN join_outer_right AS r ON l.id = r.id;

-- 8. Larger preserved side still keeps all rows in LEFT OUTER JOIN
SELECT l.id AS left_id, l.payload, r.tag
FROM join_outer_large_left AS l
LEFT JOIN join_outer_small_right AS r ON l.id = r.id
ORDER BY l.id;

-- 9. Swapping table sizes through RIGHT OUTER JOIN preserves the same rows
SELECT l.id AS left_id, l.payload, r.tag
FROM join_outer_small_right AS r
RIGHT JOIN join_outer_large_left AS l ON l.id = r.id;

-- @teardown
DROP TABLE IF EXISTS join_outer_left;
-- @teardown
DROP TABLE IF EXISTS join_outer_right;
-- @teardown
DROP TABLE IF EXISTS join_outer_empty_left;
-- @teardown
DROP TABLE IF EXISTS join_outer_empty_right;
-- @teardown
DROP TABLE IF EXISTS join_outer_no_left_rows;
-- @teardown
DROP TABLE IF EXISTS join_outer_large_left;
-- @teardown
DROP TABLE IF EXISTS join_outer_small_right;
