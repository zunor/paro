-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- ANY / ALL / SOME subquery coverage
-- The matrix is intentionally progressive:
-- 1) uncorrelated scalar truth table
-- 2) empty-subquery semantics
-- 3) NULL propagation from RHS values
-- 4) type coercion between INT and DECIMAL
-- 5) correlated ANY / ALL / SOME projected as booleans
-- 6) correlated ANY / ALL in WHERE with filtered derived tables

-- @setup
DROP TABLE IF EXISTS subquery_any_all_outer;

-- @setup
DROP TABLE IF EXISTS subquery_any_all_lookup;

-- @setup
CREATE TABLE subquery_any_all_outer (
  id INT,
  grp INT,
  amt INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_any_all_lookup (
  grp INT,
  quota DECIMAL(6, 2),
  kind TEXT
);

-- @setup
INSERT INTO subquery_any_all_outer VALUES
  (1, 10, 5, 'alpha'),
  (2, 20, 7, 'beta'),
  (3, 20, 3, 'gamma'),
  (4, 30, 6, 'delta'),
  (5, NULL, 9, 'null_grp'),
  (6, 40, 1, 'nullable_only_grp'),
  (7, 50, 2, 'missing_grp');

-- @setup
INSERT INTO subquery_any_all_lookup VALUES
  (10, 4.00, 'base'),
  (10, 8.50, 'stretch'),
  (20, 2.50, 'base'),
  (20, NULL, 'nullable'),
  (30, 6.00, 'base'),
  (30, 7.50, 'stretch'),
  (40, NULL, 'nullable_only');

-- 1. Uncorrelated ANY / ALL / SOME in SELECT
-- @query
SELECT
  5 > ANY (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE quota IS NOT NULL
  ) AS gt_any,
  6 = ALL (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 30
      AND kind = 'base'
  ) AS eq_all,
  2 <> SOME (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 10
  ) AS ne_some;

-- 2. Empty RHS: ANY -> FALSE, ALL -> TRUE
-- @query
SELECT
  5 > ANY (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 999
  ) AS gt_any_empty,
  5 = ALL (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 999
  ) AS eq_all_empty;

-- 3. NULL propagation from RHS values
-- @query
SELECT
  7 > ANY (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 20
  ) AS gt_any_with_null_rhs,
  3 = ALL (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 20
  ) AS eq_all_with_null_rhs,
  1 <> SOME (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 40
  ) AS ne_some_only_null_rhs;

-- 4. Type coercion between INT outer expressions and DECIMAL subquery outputs
-- @query
SELECT
  7 > ANY (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp IN (10, 20)
  ) AS int_vs_decimal_any,
  6 = ALL (
    SELECT quota
    FROM subquery_any_all_lookup
    WHERE grp = 30
      AND kind = 'base'
  ) AS int_vs_decimal_all;

-- 5. Correlated ANY / ALL / SOME projected as booleans
-- @query
SELECT
  o.id,
  o.grp,
  o.amt,
  o.amt > ANY (
    SELECT l.quota
    FROM subquery_any_all_lookup AS l
    WHERE l.grp = o.grp
  ) AS gt_any_in_group,
  o.amt = ALL (
    SELECT l.quota
    FROM (
      SELECT grp, quota
      FROM subquery_any_all_lookup
      WHERE kind = 'base'
    ) AS l
    WHERE l.grp = o.grp
  ) AS eq_all_base_quota,
  o.amt <> SOME (
    SELECT l.quota
    FROM subquery_any_all_lookup AS l
    WHERE l.grp = o.grp
  ) AS ne_some_in_group
FROM subquery_any_all_outer AS o
ORDER BY o.id;

-- 6. Correlated ANY in WHERE with a filtered derived table
-- @query
SELECT o.id, o.label
FROM subquery_any_all_outer AS o
WHERE o.amt > ANY (
  SELECT l.quota
  FROM (
    SELECT grp, quota
    FROM subquery_any_all_lookup
    WHERE kind <> 'nullable_only'
  ) AS l
  WHERE l.grp = o.grp
)
ORDER BY o.id;

-- 7. Correlated ALL in WHERE, including NULL outer key and empty subquery
-- @query
SELECT o.id, o.label
FROM subquery_any_all_outer AS o
WHERE o.amt = ALL (
  SELECT l.quota
  FROM (
    SELECT grp, quota
    FROM subquery_any_all_lookup
    WHERE kind = 'base'
  ) AS l
  WHERE l.grp = o.grp
)
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS subquery_any_all_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_any_all_lookup;
