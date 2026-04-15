# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- Inner equi-join + residual predicate coverage
-- @setup
DROP TABLE IF EXISTS join_mixed_left;
-- @setup
DROP TABLE IF EXISTS join_mixed_right;
-- @setup
CREATE TABLE join_mixed_left (
  id INT,
  ts INT,
  amount INT,
  grp INT,
  payload TEXT
);
-- @setup
CREATE TABLE join_mixed_right (
  id INT,
  ts INT,
  budget INT,
  grp INT,
  tag TEXT
);
-- @setup
INSERT INTO join_mixed_left VALUES
  (1, 10, 50, 1, 'l1_10'),
  (1, 20, 70, 2, 'l1_20'),
  (2, 15, 40, 1, 'l2_15'),
  (2, 25, 80, 2, 'l2_25'),
  (3, 30, 60, 1, 'l3_30'),
  (4, 5, 10, 3, 'l4_05');
-- @setup
INSERT INTO join_mixed_right VALUES
  (1, 12, 100, 1, 'r1_12'),
  (1, 18, 40, 2, 'r1_18'),
  (1, 30, 90, 1, 'r1_30'),
  (2, 20, 30, 1, 'r2_20'),
  (2, 28, 60, 2, 'r2_28'),
  (3, 25, 50, 1, 'r3_25'),
  (3, 35, 70, 2, 'r3_35'),
  (5, 50, 80, 1, 'r5_50');

-- 1. Basic equi join with a timestamp residual predicate
SELECT l.id, l.ts AS left_ts, l.payload, r.ts AS right_ts, r.tag
FROM join_mixed_left AS l
JOIN join_mixed_right AS r ON l.id = r.id AND l.ts < r.ts
ORDER BY l.id, left_ts, right_ts;

-- 2. Duplicate-key fanout summarized after residual filtering
SELECT l.id, l.ts AS left_ts, COUNT(*) AS match_count
FROM join_mixed_left AS l
JOIN join_mixed_right AS r ON l.id = r.id AND l.ts < r.ts
GROUP BY l.id, left_ts
ORDER BY l.id, left_ts;

-- 3. Residual predicates that reference non-key payload columns
SELECT l.id, l.payload, l.amount, r.tag, r.budget
FROM join_mixed_left AS l
JOIN join_mixed_right AS r
  ON l.id = r.id
 AND l.ts < r.ts
 AND l.amount < r.budget
ORDER BY l.id, l.payload, r.tag;

-- 4. Multiple residual predicates layered on top of the equality key
SELECT l.id, l.ts AS left_ts, l.amount, r.ts AS right_ts, r.budget
FROM join_mixed_left AS l
JOIN join_mixed_right AS r
  ON l.id = r.id
 AND l.ts < r.ts
 AND l.amount < r.budget
 AND l.grp + r.grp >= 3
ORDER BY l.id, left_ts, right_ts;

-- 5. Mixed predicates still work through filtered subquery inputs
SELECT l.id, l.payload, r.tag
FROM (
  SELECT id, ts, amount, grp, payload
  FROM join_mixed_left
  WHERE id <= 3 AND amount >= 50
) AS l
JOIN (
  SELECT id, ts, budget, grp, tag
  FROM join_mixed_right
  WHERE id <= 3 AND budget >= 60
) AS r
  ON l.id = r.id
 AND l.ts < r.ts
 AND l.amount < r.budget
ORDER BY l.id, l.payload, r.tag;

-- 6. Equality hits that are entirely filtered out by residual predicates
SELECT COUNT(*) AS zero_after_residual
FROM join_mixed_left AS l
JOIN join_mixed_right AS r
  ON l.id = r.id
 AND l.ts < r.ts
 AND l.amount > r.budget + 100;

-- @teardown
DROP TABLE IF EXISTS join_mixed_left;
-- @teardown
DROP TABLE IF EXISTS join_mixed_right;
