# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS join_spill_l;

-- @setup
DROP TABLE IF EXISTS join_spill_r;

-- @setup
DROP TABLE IF EXISTS join_spill_skew_l;

-- @setup
DROP TABLE IF EXISTS join_spill_skew_r;

-- @setup
CREATE TABLE join_spill_l(
  id INT,
  k1 INT,
  k2 INT,
  v INT
);

-- @setup
CREATE TABLE join_spill_r(
  id INT,
  k1 INT,
  k2 INT,
  v INT,
  payload TEXT
);

INSERT INTO join_spill_l VALUES
  (1, 1, 10, 5),
  (2, 1, 20, 7),
  (3, 2, 30, 9),
  (4, 3, 40, 11),
  (5, NULL, 50, 13),
  (6, 4, 60, 15);

INSERT INTO join_spill_r VALUES
  (10, 1, 10, 6, 'r10'),
  (11, 1, 20, 8, 'r11'),
  (12, 2, 30, 10, 'r12'),
  (13, 2, 99, 12, 'r13'),
  (14, 3, 40, 9, 'r14'),
  (15, NULL, 50, 20, 'r15_null'),
  (16, 5, 70, 30, 'r16');

SET temp_directory = '/tmp/paro_regress_join_spill';
SET max_temp_directory_size = '256MB';
SET force_external = true;
SET threads = 1;

SELECT l.id, r.payload
FROM join_spill_l l
INNER JOIN join_spill_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY l.id;

SELECT l.id AS l_id, r.id AS r_id, r.payload
FROM join_spill_l l
LEFT JOIN join_spill_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY l.id;

SELECT l.id AS l_id, r.id AS r_id, r.payload
FROM join_spill_l l
RIGHT JOIN join_spill_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY r.id;

SELECT l.id AS l_id, r.id AS r_id, r.payload
FROM join_spill_l l
FULL JOIN join_spill_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY COALESCE(l.id, 1000000), COALESCE(r.id, 1000000);

SELECT l.id AS l_id, r.id AS r_id
FROM join_spill_l l
JOIN join_spill_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2 AND l.v < r.v
ORDER BY l.id;

SELECT l.id
FROM join_spill_l l
SEMI JOIN join_spill_r r
  ON l.k1 = r.k1
ORDER BY l.id;

SELECT l.id
FROM join_spill_l l
ANTI JOIN join_spill_r r
  ON l.k1 = r.k1
ORDER BY l.id;

SELECT l.id, l.k1 IN (SELECT r.k1 FROM join_spill_r r) AS in_rhs
FROM join_spill_l l
ORDER BY l.id;

SELECT r.id
FROM join_spill_l l
RIGHT SEMI JOIN join_spill_r r
  ON l.k1 = r.k1
ORDER BY r.id;

SELECT r.id
FROM join_spill_l l
RIGHT ANTI JOIN join_spill_r r
  ON l.k1 = r.k1
ORDER BY r.id;

CREATE TABLE join_spill_skew_l(k INT, v INT);
CREATE TABLE join_spill_skew_r(k INT, v INT);

INSERT INTO join_spill_skew_l
SELECT
  CASE WHEN g <= 2000 THEN 1 ELSE g END,
  g
FROM generate_series(1, 3000) AS t(g);

INSERT INTO join_spill_skew_r
SELECT
  CASE WHEN g <= 1500 THEN 1 ELSE g END,
  g
FROM generate_series(1, 3000) AS t(g);

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT count(r.v)
FROM join_spill_skew_l l
LEFT JOIN join_spill_skew_r r
  ON l.k = r.k;

SET force_external = DEFAULT;
SET threads = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;

-- @teardown
DROP TABLE IF EXISTS join_spill_l;

-- @teardown
DROP TABLE IF EXISTS join_spill_r;

-- @teardown
DROP TABLE IF EXISTS join_spill_skew_l;

-- @teardown
DROP TABLE IF EXISTS join_spill_skew_r;
