-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Subquery/mark-join coverage in join matrix
-- The matrix is intentionally progressive:
-- 1) correlated EXISTS with joined RHS projected as boolean
-- 2) correlated IN/NOT IN with joined RHS and NULL-sensitive outcomes
-- 3) correlated ANY/ALL projected as booleans
-- 4) correlated EXISTS in WHERE with filtered joined derived table
-- 5) correlated NOT EXISTS in WHERE with joined RHS
-- 6) outer explicit JOIN + correlated IN filter
-- 7) correlated ALL in WHERE with empty-subquery behavior
-- 8) NULL-only RHS for correlated ANY through joined subquery

-- @setup
DROP TABLE IF EXISTS join_subquery_outer;

-- @setup
DROP TABLE IF EXISTS join_subquery_lookup;

-- @setup
DROP TABLE IF EXISTS join_subquery_dim;

-- @setup
DROP TABLE IF EXISTS join_subquery_probe;

-- @setup
CREATE TABLE join_subquery_outer (
  id INT,
  grp INT,
  amt INT,
  label TEXT
);

-- @setup
CREATE TABLE join_subquery_lookup (
  grp INT,
  quota INT,
  kind TEXT
);

-- @setup
CREATE TABLE join_subquery_dim (
  grp INT,
  bucket TEXT
);

-- @setup
CREATE TABLE join_subquery_probe (
  id INT,
  note TEXT
);

-- @setup
INSERT INTO join_subquery_outer VALUES
  (1, 10, 5, 'alpha'),
  (2, 20, 7, 'beta'),
  (3, 20, 3, 'gamma'),
  (4, 30, 6, 'delta'),
  (5, NULL, 9, 'null_grp'),
  (6, 40, 1, 'nullable_only_grp'),
  (7, 50, 2, 'missing_grp');

-- @setup
INSERT INTO join_subquery_lookup VALUES
  (10, 5, 'keep'),
  (10, 8, 'alt'),
  (20, 3, 'keep'),
  (20, NULL, 'nullable'),
  (20, 7, 'blocked'),
  (30, 6, 'keep'),
  (40, NULL, 'nullable_only');

-- @setup
INSERT INTO join_subquery_dim VALUES
  (10, 'A'),
  (20, 'A'),
  (30, 'B'),
  (40, 'C');

-- @setup
INSERT INTO join_subquery_probe VALUES
  (1, 'p1'),
  (2, 'p2'),
  (3, 'p3'),
  (6, 'p6'),
  (8, 'p8');

-- 1. Correlated EXISTS projected as boolean, with a joined RHS subquery
-- @query
SELECT
  o.id,
  o.grp,
  EXISTS(
    SELECT 1
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.grp = o.grp
      AND d.bucket IN ('A', 'B')
      AND l.kind <> 'blocked'
  ) AS has_join_match
FROM join_subquery_outer AS o
ORDER BY o.id;

-- 2. Correlated IN / NOT IN over a joined RHS, including NULL-sensitive rows
-- @query
SELECT
  o.id,
  o.amt IN (
    SELECT l.quota
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.grp = o.grp
      AND d.bucket <> 'C'
  ) AS amt_in_joined_lookup,
  o.amt NOT IN (
    SELECT l.quota
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.grp = o.grp
      AND d.bucket <> 'C'
  ) AS amt_not_in_joined_lookup
FROM join_subquery_outer AS o
ORDER BY o.id;

-- 3. Correlated ANY / ALL projected as booleans through joined RHS inputs
-- @query
SELECT
  o.id,
  o.amt > ANY (
    SELECT l.quota
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.grp = o.grp
      AND d.bucket IN ('A', 'B')
      AND l.quota IS NOT NULL
  ) AS gt_any_quota,
  o.amt = ALL (
    SELECT l.quota
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.grp = o.grp
      AND d.bucket IN ('A', 'B')
      AND l.kind = 'keep'
  ) AS eq_all_keep_quota
FROM join_subquery_outer AS o
ORDER BY o.id;

-- 4. Correlated EXISTS in WHERE, with a filtered joined derived table on RHS
-- @query
SELECT o.id, o.label
FROM join_subquery_outer AS o
WHERE EXISTS(
  SELECT 1
  FROM (
    SELECT l.grp AS grp, l.quota AS quota
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.kind IN ('keep', 'alt')
  ) AS rhs(grp, quota)
  WHERE rhs.grp = o.grp
    AND rhs.quota >= o.amt
)
ORDER BY o.id;

-- 5. Correlated NOT EXISTS in WHERE, using joined RHS constraints
-- @query
SELECT o.id, o.label
FROM join_subquery_outer AS o
WHERE NOT EXISTS(
  SELECT 1
  FROM join_subquery_lookup AS l
  JOIN join_subquery_dim AS d ON d.grp = l.grp
  WHERE l.grp = o.grp
    AND d.bucket = 'A'
    AND l.kind = 'keep'
)
ORDER BY o.id;

-- 6. Explicit outer join + correlated IN filter
-- @query
SELECT o.id, p.note
FROM join_subquery_outer AS o
JOIN join_subquery_probe AS p ON p.id = o.id
WHERE o.amt IN (
  SELECT l.quota
  FROM join_subquery_lookup AS l
  JOIN join_subquery_dim AS d ON d.grp = l.grp
  WHERE l.grp = o.grp
    AND d.bucket IN ('A', 'B')
    AND l.kind = 'keep'
)
ORDER BY o.id, p.note;

-- 7. Correlated ALL in WHERE, including empty-subquery behavior
-- @query
SELECT o.id, o.label
FROM join_subquery_outer AS o
WHERE o.amt = ALL (
  SELECT l.quota
  FROM join_subquery_lookup AS l
  JOIN join_subquery_dim AS d ON d.grp = l.grp
  WHERE l.grp = o.grp
    AND d.bucket = 'A'
    AND l.kind = 'keep'
)
ORDER BY o.id;

-- 8. NULL-only RHS for correlated ANY via joined subquery
-- @query
SELECT
  o.id,
  o.amt > ANY (
    SELECT l.quota
    FROM join_subquery_lookup AS l
    JOIN join_subquery_dim AS d ON d.grp = l.grp
    WHERE l.grp = o.grp
      AND d.bucket = 'C'
  ) AS gt_any_bucket_c
FROM join_subquery_outer AS o
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS join_subquery_outer;

-- @teardown
DROP TABLE IF EXISTS join_subquery_lookup;

-- @teardown
DROP TABLE IF EXISTS join_subquery_dim;

-- @teardown
DROP TABLE IF EXISTS join_subquery_probe;
