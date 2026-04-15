-- Correlated subquery coverage beyond the basic scalar/exists/any-all matrix
-- The matrix is intentionally progressive:
-- 1) correlated scalar with ORDER BY + LIMIT
-- 2) correlated scalar with ORDER BY + LIMIT/OFFSET
-- 3) correlated scalar with GROUP BY / HAVING inside
-- 4) NULL outer key behavior, even when the RHS contains NULL keys
-- 5) nested correlated scalar through a derived subquery

-- @setup
DROP TABLE IF EXISTS subquery_correlated_outer;

-- @setup
DROP TABLE IF EXISTS subquery_correlated_detail;

-- @setup
CREATE TABLE subquery_correlated_outer (
  id INT,
  grp INT,
  threshold INT,
  label TEXT
);

-- @setup
CREATE TABLE subquery_correlated_detail (
  grp INT,
  seq INT,
  score INT,
  kind TEXT
);

-- @setup
INSERT INTO subquery_correlated_outer VALUES
  (1, 10, 18, 'alpha'),
  (2, 20, 16, 'beta'),
  (3, 20, 15, 'gamma'),
  (4, 30, 7, 'delta'),
  (5, NULL, 1, 'null_grp'),
  (6, 40, 1, 'missing_grp');

-- @setup
INSERT INTO subquery_correlated_detail VALUES
  (10, 1, 4, 'base'),
  (10, 2, 9, 'stretch'),
  (10, 3, 7, 'close'),
  (20, 1, 2, 'base'),
  (20, 2, 5, 'stretch'),
  (20, 3, 8, 'close'),
  (30, 1, 6, 'base'),
  (30, 2, 1, 'tail'),
  (NULL, 1, 50, 'null_bucket');

-- 1. Correlated scalar with ORDER BY + LIMIT
-- @query
SELECT
  o.id,
  o.grp,
  (
    SELECT d.score
    FROM subquery_correlated_detail AS d
    WHERE d.grp = o.grp
    ORDER BY d.score DESC, d.seq
    LIMIT 1
  ) AS top_score
FROM subquery_correlated_outer AS o
ORDER BY o.id;

-- 2. Correlated scalar with ORDER BY + LIMIT/OFFSET
-- @query
SELECT
  o.id,
  o.grp,
  (
    SELECT d.score
    FROM subquery_correlated_detail AS d
    WHERE d.grp = o.grp
    ORDER BY d.score DESC, d.seq
    LIMIT 1 OFFSET 1
  ) AS second_score
FROM subquery_correlated_outer AS o
ORDER BY o.id;

-- 3. Correlated scalar with GROUP BY / HAVING inside
-- @query
SELECT
  o.id,
  o.label,
  (
    SELECT d.grp
    FROM subquery_correlated_detail AS d
    WHERE d.grp = o.grp
    GROUP BY d.grp
    HAVING COUNT(*) >= 3
  ) AS dense_group
FROM subquery_correlated_outer AS o
ORDER BY o.id;

-- 4. NULL outer key semantics should still see no match under "=" correlation
-- @query
SELECT
  o.id,
  o.label,
  (
    SELECT d.grp
    FROM subquery_correlated_detail AS d
    WHERE d.grp = o.grp
    GROUP BY d.grp
    HAVING COUNT(*) >= 1
  ) AS eq_group
FROM subquery_correlated_outer AS o
WHERE o.grp IS NULL
ORDER BY o.id;

-- 5. Nested correlated scalar through a derived subquery
-- @query
SELECT
  o.id,
  (
    SELECT MAX(x)
    FROM (
      SELECT o.id AS x
    ) AS nested_rows
  ) AS nested_value
FROM subquery_correlated_outer AS o
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS subquery_correlated_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_correlated_detail;
