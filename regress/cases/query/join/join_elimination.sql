-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Join elimination coverage
-- @setup
DROP TABLE IF EXISTS join_elim_left;
-- @setup
DROP TABLE IF EXISTS join_elim_right_unique;
-- @setup
DROP TABLE IF EXISTS join_elim_left_unique;
-- @setup
DROP TABLE IF EXISTS join_elim_right;
-- @setup
DROP TABLE IF EXISTS join_elim_right_nonunique;
-- @setup
CREATE TABLE join_elim_left (id INT, payload TEXT);
-- @setup
CREATE TABLE join_elim_right_unique (id INT PRIMARY KEY, note TEXT);
-- @setup
CREATE TABLE join_elim_left_unique (id INT PRIMARY KEY, payload TEXT);
-- @setup
CREATE TABLE join_elim_right (id INT, note TEXT);
-- @setup
CREATE TABLE join_elim_right_nonunique (id INT, note TEXT);
-- @setup
INSERT INTO join_elim_left VALUES
  (1, 'left-1'),
  (2, 'left-2'),
  (3, 'left-3');
-- @setup
INSERT INTO join_elim_right_unique VALUES
  (2, 'right-2'),
  (3, 'right-3');
-- @setup
INSERT INTO join_elim_left_unique VALUES
  (1, 'uniq-left-1'),
  (2, 'uniq-left-2');
-- @setup
INSERT INTO join_elim_right VALUES
  (2, 'right-only-2'),
  (4, 'right-only-4');
-- @setup
INSERT INTO join_elim_right_nonunique VALUES
  (2, 'dup-a'),
  (2, 'dup-b');

-- 1. LEFT JOIN on UNIQUE/PRIMARY KEY side with left-only projection can be removed
SELECT l.id, l.payload
FROM join_elim_left AS l
LEFT JOIN join_elim_right_unique AS r ON l.id = r.id
ORDER BY l.id;

EXPLAIN
SELECT l.id, l.payload
FROM join_elim_left AS l
LEFT JOIN join_elim_right_unique AS r ON l.id = r.id;

-- 2. Referencing eliminated-side columns keeps the join in place
EXPLAIN
SELECT l.id, r.note
FROM join_elim_left AS l
LEFT JOIN join_elim_right_unique AS r ON l.id = r.id;

-- 3. RIGHT JOIN can be removed symmetrically when the left side is unique and unused
SELECT r.id, r.note
FROM join_elim_left_unique AS l
RIGHT JOIN join_elim_right AS r ON l.id = r.id
ORDER BY r.id, r.note;

EXPLAIN
SELECT r.id, r.note
FROM join_elim_left_unique AS l
RIGHT JOIN join_elim_right AS r ON l.id = r.id;

-- 4. Non-unique build side must not be eliminated because it can duplicate rows
SELECT l.id
FROM join_elim_left AS l
LEFT JOIN join_elim_right_nonunique AS r ON l.id = r.id
ORDER BY l.id;

EXPLAIN
SELECT l.id
FROM join_elim_left AS l
LEFT JOIN join_elim_right_nonunique AS r ON l.id = r.id;

-- @teardown
DROP TABLE IF EXISTS join_elim_left;
-- @teardown
DROP TABLE IF EXISTS join_elim_right_unique;
-- @teardown
DROP TABLE IF EXISTS join_elim_left_unique;
-- @teardown
DROP TABLE IF EXISTS join_elim_right;
-- @teardown
DROP TABLE IF EXISTS join_elim_right_nonunique;
