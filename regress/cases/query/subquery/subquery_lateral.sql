# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- LATERAL coverage focused on explicit join syntax.
-- The matrix is intentionally progressive:
-- 1) CROSS JOIN LATERAL for the explicit equivalent of `FROM t, LATERAL (...)`
-- 2) CROSS JOIN LATERAL with outer-aware filtering before top-1 selection
-- 3) INNER JOIN LATERAL with outer-aware filtering inside the RHS
-- 4) LEFT JOIN LATERAL ON true to preserve unmatched outer rows
-- 5) LEFT JOIN LATERAL with a comparison ON clause after RHS top-1 selection

-- @setup
DROP TABLE IF EXISTS subquery_lateral_outer;

-- @setup
DROP TABLE IF EXISTS subquery_lateral_detail;

-- @setup
CREATE TABLE subquery_lateral_outer (
  id INT,
  grp INT,
  threshold INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_lateral_detail (
  grp INT,
  seq INT,
  score INT,
  kind TEXT
);

-- @setup
INSERT INTO subquery_lateral_outer VALUES
  (1, 10, 8, 'alpha'),
  (2, 20, 6, 'beta'),
  (3, 20, 9, 'gamma'),
  (4, 30, 5, 'delta'),
  (5, NULL, 1, 'null_grp'),
  (6, 40, 3, 'missing_grp');

-- @setup
INSERT INTO subquery_lateral_detail VALUES
  (10, 1, 4, 'seed'),
  (10, 2, 9, 'peak'),
  (10, 3, 7, 'trail'),
  (20, 1, 5, 'seed'),
  (20, 2, 8, 'peak'),
  (20, 3, 6, 'trail'),
  (30, 1, 6, 'solo'),
  (NULL, 1, 50, 'null_bucket');

-- 1. CROSS JOIN LATERAL mirrors the explicit form of `FROM t, LATERAL (...)`
-- @query
SELECT
  o.id,
  o.grp,
  picked.kind,
  picked.score
FROM subquery_lateral_outer AS o
CROSS JOIN LATERAL (
  SELECT d.kind AS kind, d.score AS score
  FROM subquery_lateral_detail AS d
  WHERE d.grp = o.grp
  ORDER BY d.score DESC, d.seq
  LIMIT 1
) AS picked
ORDER BY o.id;

-- 2. CROSS JOIN LATERAL with outer-aware filtering before RHS top-1 selection
-- @query
SELECT
  o.id,
  picked.kind,
  picked.score
FROM subquery_lateral_outer AS o
CROSS JOIN LATERAL (
  SELECT
    d.kind AS kind,
    d.score AS score
  FROM subquery_lateral_detail AS d
  WHERE d.grp = o.grp
    AND d.score >= o.threshold
  ORDER BY d.score DESC, d.seq
  LIMIT 1
) AS picked
ORDER BY o.id;

-- 3. INNER JOIN LATERAL with outer-aware filtering inside the RHS
-- @query
SELECT
  o.id,
  o.threshold,
  candidate.kind,
  candidate.score
FROM subquery_lateral_outer AS o
JOIN LATERAL (
  SELECT d.kind AS kind, d.score AS score
  FROM subquery_lateral_detail AS d
  WHERE d.grp = o.grp
    AND d.score >= o.threshold
) AS candidate
ON true
ORDER BY o.id, candidate.score;

-- 4. LEFT JOIN LATERAL ON true preserves NULL outer keys and missing groups
-- @query
SELECT
  o.id,
  o.label,
  picked.kind,
  picked.score
FROM subquery_lateral_outer AS o
LEFT JOIN LATERAL (
  SELECT d.kind AS kind, d.score AS score
  FROM subquery_lateral_detail AS d
  WHERE d.grp = o.grp
  ORDER BY d.score DESC, d.seq
  LIMIT 1
) AS picked
ON true
ORDER BY o.id;

-- 5. LEFT JOIN LATERAL with a comparison after RHS top-1 selection
-- @query
SELECT
  o.id,
  o.threshold,
  picked.kind,
  picked.score
FROM subquery_lateral_outer AS o
LEFT JOIN LATERAL (
  SELECT d.kind AS kind, d.score AS score
  FROM subquery_lateral_detail AS d
  WHERE d.grp = o.grp
  ORDER BY d.score DESC, d.seq
  LIMIT 1
) AS picked
ON picked.score >= o.threshold
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS subquery_lateral_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_lateral_detail;
