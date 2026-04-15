-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- IN / NOT IN subquery coverage
-- The matrix is intentionally progressive:
-- 1) uncorrelated IN / NOT IN scalar booleans
-- 2) NULL propagation from RHS subquery values
-- 3) correlated IN / NOT IN projected as booleans
-- 4) correlated IN / NOT IN in WHERE
-- 5) multi-column IN, including NULL-sensitive row comparison

-- @setup
DROP TABLE IF EXISTS subquery_in_outer;

-- @setup
DROP TABLE IF EXISTS subquery_in_lookup;

-- @setup
CREATE TABLE subquery_in_outer (
  id INT,
  grp INT,
  amt INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_in_lookup (
  grp INT,
  quota INT,
  kind TEXT
);

-- @setup
INSERT INTO subquery_in_outer VALUES
  (1, 10, 5, 'alpha'),
  (2, 20, 7, 'beta'),
  (3, 20, 3, 'gamma'),
  (4, 30, 6, 'delta'),
  (5, NULL, 9, 'null_grp'),
  (6, 40, 1, 'missing_grp');

-- @setup
INSERT INTO subquery_in_lookup VALUES
  (10, 5, 'keep'),
  (10, 8, 'alt'),
  (20, 3, 'keep'),
  (20, NULL, 'nullable'),
  (30, 6, 'keep'),
  (40, NULL, 'nullable_only');

-- 1. Uncorrelated IN / NOT IN without NULLs in RHS
-- @query
SELECT
  5 IN (
    SELECT quota
    FROM subquery_in_lookup
    WHERE quota IS NOT NULL
  ) AS in_present,
  7 NOT IN (
    SELECT quota
    FROM subquery_in_lookup
    WHERE quota IS NOT NULL
  ) AS not_in_absent;

-- 2. RHS NULL propagation for IN / NOT IN
-- @query
SELECT
  7 IN (SELECT quota FROM subquery_in_lookup) AS in_with_null_rhs,
  3 IN (SELECT quota FROM subquery_in_lookup) AS in_match_despite_null,
  7 NOT IN (SELECT quota FROM subquery_in_lookup) AS not_in_with_null_rhs,
  3 NOT IN (SELECT quota FROM subquery_in_lookup) AS not_in_match;

-- 3. Correlated IN / NOT IN projected as booleans
-- @query
SELECT
  o.id,
  o.grp,
  o.amt,
  o.amt IN (
    SELECT l.quota
    FROM subquery_in_lookup AS l
    WHERE l.grp = o.grp
  ) AS amt_in_group_quota,
  o.amt NOT IN (
    SELECT l.quota
    FROM subquery_in_lookup AS l
    WHERE l.grp = o.grp
  ) AS amt_not_in_group_quota
FROM subquery_in_outer AS o
ORDER BY o.id;

-- 4. Correlated IN in WHERE
-- @query
SELECT o.id, o.label
FROM subquery_in_outer AS o
WHERE o.amt IN (
  SELECT l.quota
  FROM subquery_in_lookup AS l
  WHERE l.grp = o.grp
)
ORDER BY o.id;

-- 5. Correlated NOT IN in WHERE
-- @query
SELECT o.id, o.label
FROM subquery_in_outer AS o
WHERE o.amt NOT IN (
  SELECT l.quota
  FROM subquery_in_lookup AS l
  WHERE l.grp = o.grp
)
ORDER BY o.id;

-- 6. Multi-column IN, including NULL-sensitive row comparison
-- @query
SELECT
  (10, 5) IN (
    SELECT grp, quota
    FROM subquery_in_lookup
  ) AS pair_match,
  (20, 7) IN (
    SELECT grp, quota
    FROM subquery_in_lookup
  ) AS pair_miss,
  (40, 1) IN (
    SELECT grp, quota
    FROM subquery_in_lookup
  ) AS pair_null_sensitive;

-- @teardown
DROP TABLE IF EXISTS subquery_in_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_in_lookup;
