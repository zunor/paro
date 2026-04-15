-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Single-key, unique-match inner join coverage
-- @setup
DROP TABLE IF EXISTS join_inner_basic_left;
-- @setup
DROP TABLE IF EXISTS join_inner_basic_right;
-- @setup
CREATE TABLE join_inner_basic_left (id INT, label TEXT);
-- @setup
CREATE TABLE join_inner_basic_right (id INT, descr TEXT);
-- @setup
INSERT INTO join_inner_basic_left VALUES (1, 'l1'), (2, 'l2'), (4, 'l4');
-- @setup
INSERT INTO join_inner_basic_right VALUES (2, 'r2'), (3, 'r3'), (4, 'r4');

-- Duplicate-key fanout coverage
-- @setup
DROP TABLE IF EXISTS join_inner_dup_left;
-- @setup
DROP TABLE IF EXISTS join_inner_dup_right;
-- @setup
CREATE TABLE join_inner_dup_left (k INT, left_tag TEXT);
-- @setup
CREATE TABLE join_inner_dup_right (k INT, right_tag TEXT);
-- @setup
INSERT INTO join_inner_dup_left VALUES (1, 'a'), (1, 'b'), (2, 'c');
-- @setup
INSERT INTO join_inner_dup_right VALUES (1, 'x'), (1, 'y'), (3, 'z');

-- Higher-repeat key coverage with compact verification
-- @setup
DROP TABLE IF EXISTS join_inner_repeat_left;
-- @setup
DROP TABLE IF EXISTS join_inner_repeat_right;
-- @setup
CREATE TABLE join_inner_repeat_left (k INT, payload TEXT);
-- @setup
CREATE TABLE join_inner_repeat_right (k INT, payload TEXT);
-- @setup
INSERT INTO join_inner_repeat_left VALUES
  (1, 'l1'),
  (1, 'l2'),
  (1, 'l3'),
  (1, 'l4'),
  (2, 'l5'),
  (2, 'l6');
-- @setup
INSERT INTO join_inner_repeat_right VALUES
  (1, 'r1'),
  (1, 'r2'),
  (1, 'r3'),
  (2, 'r4');

-- Multi-key equality coverage
-- @setup
DROP TABLE IF EXISTS join_inner_multi_left;
-- @setup
DROP TABLE IF EXISTS join_inner_multi_right;
-- @setup
CREATE TABLE join_inner_multi_left (a INT, b INT, payload TEXT);
-- @setup
CREATE TABLE join_inner_multi_right (a INT, b INT, flag TEXT);
-- @setup
INSERT INTO join_inner_multi_left VALUES
  (1, 10, 'm1'),
  (1, 20, 'm2'),
  (2, 10, 'm3'),
  (3, 30, 'm4');
-- @setup
INSERT INTO join_inner_multi_right VALUES
  (1, 10, 'hit_110'),
  (2, 10, 'hit_210'),
  (2, 20, 'miss_220'),
  (3, 30, 'hit_330');

-- Wide payload coverage
-- @setup
DROP TABLE IF EXISTS join_inner_wide_left;
-- @setup
DROP TABLE IF EXISTS join_inner_wide_right;
-- @setup
CREATE TABLE join_inner_wide_left (
  id INT,
  c1 TEXT,
  c2 INT,
  c3 TEXT,
  c4 INT
);
-- @setup
CREATE TABLE join_inner_wide_right (
  id INT,
  d1 TEXT,
  d2 INT,
  d3 TEXT,
  d4 INT
);
-- @setup
INSERT INTO join_inner_wide_left VALUES
  (10, 'left-alpha', 100, 'left-extra-a', 1000),
  (20, 'left-beta', 200, 'left-extra-b', 2000),
  (30, 'left-gamma', 300, 'left-extra-c', 3000);
-- @setup
INSERT INTO join_inner_wide_right VALUES
  (10, 'right-alpha', 11, 'right-extra-a', 111),
  (30, 'right-gamma', 33, 'right-extra-c', 333),
  (40, 'right-delta', 44, 'right-extra-d', 444);

-- Empty-side coverage
-- @setup
DROP TABLE IF EXISTS join_inner_empty_left;
-- @setup
DROP TABLE IF EXISTS join_inner_empty_right;
-- @setup
CREATE TABLE join_inner_empty_left (id INT, payload TEXT);
-- @setup
CREATE TABLE join_inner_empty_right (id INT, payload TEXT);
-- @setup
INSERT INTO join_inner_empty_right VALUES (1, 'e1'), (2, 'e2');

-- Three-way equi-join coverage
-- @setup
DROP TABLE IF EXISTS join_inner_chain_a;
-- @setup
DROP TABLE IF EXISTS join_inner_chain_b;
-- @setup
DROP TABLE IF EXISTS join_inner_chain_c;
-- @setup
CREATE TABLE join_inner_chain_a (id INT, a_val TEXT);
-- @setup
CREATE TABLE join_inner_chain_b (id INT, b_val TEXT);
-- @setup
CREATE TABLE join_inner_chain_c (id INT, c_val TEXT);
-- @setup
INSERT INTO join_inner_chain_a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3'), (5, 'a5');
-- @setup
INSERT INTO join_inner_chain_b VALUES (2, 'b2'), (3, 'b3'), (4, 'b4'), (5, 'b5');
-- @setup
INSERT INTO join_inner_chain_c VALUES (3, 'c3'), (5, 'c5'), (6, 'c6');

-- 1. Basic single-key inner join
SELECT l.id AS id, l.label, r.descr
FROM join_inner_basic_left AS l
JOIN join_inner_basic_right AS r ON l.id = r.id
ORDER BY l.id;

-- 2. Same single-key join through filtered subquery inputs
SELECT l.id AS id, l.label, r.descr
FROM (
  SELECT id, label
  FROM join_inner_basic_left
  WHERE id >= 2
) AS l
JOIN (
  SELECT id, descr
  FROM join_inner_basic_right
  WHERE id <= 4
) AS r ON l.id = r.id
ORDER BY l.id;

-- 3. Duplicate-key fanout with full row materialization
SELECT l.k, l.left_tag, r.right_tag
FROM join_inner_dup_left AS l
JOIN join_inner_dup_right AS r ON l.k = r.k
ORDER BY l.k, l.left_tag, r.right_tag;

-- 4. Higher-repeat keys verified through grouped counts
SELECT l.k, COUNT(*) AS match_count
FROM join_inner_repeat_left AS l
JOIN join_inner_repeat_right AS r ON l.k = r.k
GROUP BY l.k
ORDER BY l.k;

-- 5. Multi-key equality join
SELECT l.a, l.b, l.payload, r.flag
FROM join_inner_multi_left AS l
JOIN join_inner_multi_right AS r ON l.a = r.a AND l.b = r.b
ORDER BY l.a, l.b;

-- 6. Wide payload join with multiple projected columns
SELECT
  l.id,
  l.c1,
  l.c2,
  l.c3,
  l.c4,
  r.d1,
  r.d2,
  r.d3,
  r.d4
FROM join_inner_wide_left AS l
JOIN join_inner_wide_right AS r ON l.id = r.id
ORDER BY l.id;

-- 7. Empty left side
SELECT COUNT(*) AS empty_left_count
FROM join_inner_empty_left AS l
JOIN join_inner_empty_right AS r ON l.id = r.id;

-- 8. Empty right side
SELECT COUNT(*) AS empty_right_count
FROM join_inner_empty_right AS r
JOIN join_inner_empty_left AS l ON r.id = l.id;

-- 9. Three-way chain inner join
SELECT a.id, a.a_val, b.b_val, c.c_val
FROM join_inner_chain_a AS a
JOIN join_inner_chain_b AS b ON a.id = b.id
JOIN join_inner_chain_c AS c ON b.id = c.id
ORDER BY a.id;

-- @teardown
DROP TABLE IF EXISTS join_inner_basic_left;
-- @teardown
DROP TABLE IF EXISTS join_inner_basic_right;
-- @teardown
DROP TABLE IF EXISTS join_inner_dup_left;
-- @teardown
DROP TABLE IF EXISTS join_inner_dup_right;
-- @teardown
DROP TABLE IF EXISTS join_inner_repeat_left;
-- @teardown
DROP TABLE IF EXISTS join_inner_repeat_right;
-- @teardown
DROP TABLE IF EXISTS join_inner_multi_left;
-- @teardown
DROP TABLE IF EXISTS join_inner_multi_right;
-- @teardown
DROP TABLE IF EXISTS join_inner_wide_left;
-- @teardown
DROP TABLE IF EXISTS join_inner_wide_right;
-- @teardown
DROP TABLE IF EXISTS join_inner_empty_left;
-- @teardown
DROP TABLE IF EXISTS join_inner_empty_right;
-- @teardown
DROP TABLE IF EXISTS join_inner_chain_a;
-- @teardown
DROP TABLE IF EXISTS join_inner_chain_b;
-- @teardown
DROP TABLE IF EXISTS join_inner_chain_c;
