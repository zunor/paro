-- EXISTS / NOT EXISTS coverage
-- The matrix is intentionally progressive:
-- 1) uncorrelated EXISTS / NOT EXISTS scalar booleans
-- 2) EXISTS with empty subquery in WHERE
-- 3) correlated EXISTS / NOT EXISTS projected as booleans
-- 4) correlated EXISTS in WHERE with filtered RHS
-- 5) correlated NOT EXISTS in WHERE with NULL outer key and missing match

-- @setup
DROP TABLE IF EXISTS subquery_exists_outer;

-- @setup
DROP TABLE IF EXISTS subquery_exists_lookup;

-- @setup
DROP TABLE IF EXISTS subquery_exists_empty;

-- @setup
CREATE TABLE subquery_exists_outer (
  id INT,
  grp INT,
  amt INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_exists_lookup (
  grp INT,
  kind TEXT,
  quota INT
);

-- @setup
CREATE TABLE subquery_exists_empty (marker INT);

-- @setup
INSERT INTO subquery_exists_outer VALUES
  (1, 10, 5, 'alpha'),
  (2, 20, 7, 'beta'),
  (3, 20, 3, 'gamma'),
  (4, 30, 6, 'delta'),
  (5, NULL, 9, 'null_grp'),
  (6, 40, 1, 'missing_grp');

-- @setup
INSERT INTO subquery_exists_lookup VALUES
  (10, 'open', 8),
  (10, 'blocked', 2),
  (20, 'closed', 4),
  (30, 'open', 6);

-- 1. Uncorrelated EXISTS / NOT EXISTS in SELECT
-- @query
SELECT
  EXISTS(
    SELECT NULL
    FROM subquery_exists_lookup
    WHERE kind = 'open'
  ) AS has_open_rows,
  NOT EXISTS(
    SELECT 1
    FROM subquery_exists_empty
  ) AS no_huge_quota;

-- 2. EXISTS with empty subquery in WHERE
-- @query
SELECT id, label
FROM subquery_exists_outer
WHERE EXISTS(
  SELECT 1
  FROM subquery_exists_empty
)
ORDER BY id;

-- 3. Correlated EXISTS / NOT EXISTS projected as booleans
-- @query
SELECT
  o.id,
  o.grp,
  EXISTS(
    SELECT 1
    FROM subquery_exists_lookup AS l
    WHERE l.grp = o.grp
      AND l.kind = 'open'
  ) AS has_open_match,
  NOT EXISTS(
    SELECT 1
    FROM subquery_exists_lookup AS l
    WHERE l.grp = o.grp
      AND l.kind = 'blocked'
  ) AS lacks_blocked_match
FROM subquery_exists_outer AS o
ORDER BY o.id;

-- 4. Correlated EXISTS in WHERE with filtered RHS derived table
-- @query
SELECT o.id, o.label
FROM subquery_exists_outer AS o
WHERE EXISTS(
  SELECT 1
  FROM (
    SELECT grp, quota
    FROM subquery_exists_lookup
    WHERE kind <> 'blocked'
  ) AS l
  WHERE l.grp = o.grp
    AND l.quota >= o.amt
)
ORDER BY o.id;

-- 5. Correlated NOT EXISTS in WHERE, including NULL outer key and missing match
-- @query
SELECT o.id, o.label
FROM subquery_exists_outer AS o
WHERE NOT EXISTS(
  SELECT 1
  FROM subquery_exists_lookup AS l
  WHERE l.grp = o.grp
)
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS subquery_exists_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_exists_lookup;

-- @teardown
DROP TABLE IF EXISTS subquery_exists_empty;
