-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Scalar subquery coverage
-- The matrix is intentionally progressive:
-- 1) uncorrelated single-row contract
-- 2) zero-row -> NULL
-- 3) scalar in SELECT / WHERE / aggregate argument
-- 4) correlated scalar in SELECT / WHERE / ORDER BY / aggregate argument
-- 5) NULL outer key and missing match behavior
-- 6) multi-row error contract

-- @setup
DROP TABLE IF EXISTS subquery_scalar_cfg;

-- @setup
DROP TABLE IF EXISTS subquery_scalar_outer;

-- @setup
DROP TABLE IF EXISTS subquery_scalar_lookup;

-- @setup
DROP TABLE IF EXISTS subquery_scalar_multi;

-- @setup
CREATE TABLE subquery_scalar_cfg (
  name TEXT,
  value INT
);

-- @setup
CREATE TABLE subquery_scalar_outer (
  id INT,
  grp INT,
  amt INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_scalar_lookup (
  grp INT,
  quota INT,
  ord INT
);

-- @setup
CREATE TABLE subquery_scalar_multi (v INT);

-- @setup
INSERT INTO subquery_scalar_cfg VALUES
  ('answer', 42),
  ('threshold', 6),
  ('bonus', 2);

-- @setup
INSERT INTO subquery_scalar_outer VALUES
  (1, 10, 5, 'alpha'),
  (2, 20, 7, 'beta'),
  (3, 20, 3, 'gamma'),
  (4, 30, 6, 'delta'),
  (5, NULL, 9, 'null_grp'),
  (6, 40, 1, 'missing_grp');

-- @setup
INSERT INTO subquery_scalar_lookup VALUES
  (10, 8, 30),
  (20, 4, 10),
  (30, 6, 20);

-- @setup
INSERT INTO subquery_scalar_multi VALUES
  (7),
  (8);

-- 1. Uncorrelated scalar in SELECT
-- @query
SELECT (SELECT value FROM subquery_scalar_cfg WHERE name = 'answer') AS answer;

-- 2. Uncorrelated scalar in WHERE
-- @query
SELECT id, label
FROM subquery_scalar_outer
WHERE amt < (SELECT value FROM subquery_scalar_cfg WHERE name = 'threshold')
ORDER BY id;

-- 3. Zero-row scalar subquery yields NULL
-- @query
SELECT (SELECT value FROM subquery_scalar_cfg WHERE name = 'missing') AS missing_value;

-- 4. Uncorrelated scalar as aggregate argument
-- @query
SELECT SUM((SELECT value FROM subquery_scalar_cfg WHERE name = 'bonus')) AS repeated_bonus
FROM (
  SELECT id
  FROM subquery_scalar_outer
  WHERE id <= 3
) AS base;

-- 5. Correlated scalar in SELECT, including NULL outer key and missing match
-- @query
SELECT
  o.id,
  o.grp,
  o.amt,
  (
    SELECT l.quota
    FROM subquery_scalar_lookup AS l
    WHERE l.grp = o.grp
  ) AS quota
FROM subquery_scalar_outer AS o
ORDER BY o.id;

-- 6. Correlated scalar in WHERE
-- @query
SELECT o.id, o.label
FROM subquery_scalar_outer AS o
WHERE o.amt < (
  SELECT l.quota
  FROM subquery_scalar_lookup AS l
  WHERE l.grp = o.grp
)
ORDER BY o.id;

-- 7. Correlated scalar in ORDER BY
-- @query
SELECT o.id, o.label
FROM subquery_scalar_outer AS o
ORDER BY
  COALESCE(
    (
      SELECT l.ord
      FROM subquery_scalar_lookup AS l
      WHERE l.grp = o.grp
    ),
    999
  ),
  o.id;

-- 8. Correlated scalar as aggregate argument
-- @query
SELECT
  o.grp,
  SUM(
    COALESCE(
      (
        SELECT l.quota
        FROM subquery_scalar_lookup AS l
        WHERE l.grp = o.grp
      ),
      0
    )
  ) AS quota_sum
FROM subquery_scalar_outer AS o
GROUP BY o.grp
ORDER BY o.grp;

-- 9. Multi-row scalar subquery must error
-- @statement error More than one row returned by a subquery
SELECT (SELECT v FROM subquery_scalar_multi) AS should_fail;

-- @teardown
DROP TABLE IF EXISTS subquery_scalar_cfg;

-- @teardown
DROP TABLE IF EXISTS subquery_scalar_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_scalar_lookup;

-- @teardown
DROP TABLE IF EXISTS subquery_scalar_multi;
