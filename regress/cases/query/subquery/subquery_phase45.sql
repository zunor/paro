-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Nested outer-correlation regressions with real executable results.
-- Focus:
-- 1) current-layer EXISTS / DISTINCT / HAVING / JOIN ON black-box coverage
-- 2) planner-only nested setop / nested scalar / nested EXISTS coverage lives in
--    crates/planner/src/binder/query_node/plan_subquery.rs
-- 3) DISTINCT ON / Window / execution-fragile shapes are intentionally excluded here;
--    they are covered by planner or LATERAL-specific tests instead of stale error baselines

-- @setup
DROP TABLE IF EXISTS subquery_phase45_outer;

-- @setup
DROP TABLE IF EXISTS subquery_phase45_detail;

-- @setup
DROP TABLE IF EXISTS subquery_phase45_probe;

-- @setup
CREATE TABLE subquery_phase45_outer (
  id INT,
  grp INT,
  threshold INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_phase45_detail (
  grp INT,
  seq INT,
  score INT,
  kind TEXT
);

-- @setup
CREATE TABLE subquery_phase45_probe (
  id INT,
  note TEXT
);

-- @setup
INSERT INTO subquery_phase45_outer VALUES
  (1, 10, 6, 'alpha'),
  (2, 20, 5, 'beta'),
  (3, 20, 8, 'gamma'),
  (4, 30, 2, 'delta'),
  (5, NULL, 4, 'null_grp'),
  (6, 40, 1, 'missing_grp');

-- @setup
INSERT INTO subquery_phase45_detail VALUES
  (10, 1, 4, 'seed'),
  (10, 2, 9, 'peak'),
  (10, 3, 7, 'trail'),
  (20, 1, 5, 'seed'),
  (20, 2, 8, 'peak'),
  (20, 3, 6, 'trail'),
  (20, 4, 8, 'peak'),
  (30, 1, 3, 'solo'),
  (30, 2, 1, 'tail'),
  (NULL, 1, 50, 'null_bucket');

-- @setup
INSERT INTO subquery_phase45_probe VALUES
  (1, 'p1'),
  (2, 'p2'),
  (4, 'p4'),
  (6, 'p6');

-- 1. Current-layer correlated EXISTS
-- @query
SELECT
  o.id,
  EXISTS(
    SELECT 1
    FROM subquery_phase45_detail AS d
    WHERE d.grp = o.grp
      AND d.score >= o.threshold
  ) AS has_match
FROM subquery_phase45_outer AS o
ORDER BY o.id;

-- 2. Correlated DISTINCT remains executable and returns real booleans
-- @query
SELECT
  o.id,
  EXISTS(
    SELECT DISTINCT d.score
    FROM subquery_phase45_detail AS d
    WHERE d.grp = o.grp
  ) AS has_distinct
FROM subquery_phase45_outer AS o
ORDER BY o.id;

-- 3. HAVING with a correlated current-layer EXISTS
-- @query
SELECT o.grp, COUNT(*) AS outer_rows
FROM subquery_phase45_outer AS o
GROUP BY o.grp
HAVING EXISTS(
  SELECT 1
  FROM subquery_phase45_detail AS d
  WHERE d.grp = o.grp
)
ORDER BY o.grp;

-- 4. INNER JOIN ON with a correlated current-layer EXISTS
-- @query
SELECT o.id, p.note
FROM subquery_phase45_outer AS o
JOIN subquery_phase45_probe AS p
  ON p.id = o.id
 AND EXISTS(
   SELECT 1
   FROM subquery_phase45_detail AS d
   WHERE d.grp = o.grp
     AND d.score >= o.threshold
 )
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS subquery_phase45_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_phase45_detail;

-- @teardown
DROP TABLE IF EXISTS subquery_phase45_probe;
