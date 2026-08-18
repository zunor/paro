-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

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
DROP TABLE IF EXISTS subquery_scalar_window_input;

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
CREATE TABLE subquery_scalar_window_input (id INT, balance INT);

-- @setup
CREATE TABLE subquery_correlated_unique_outer (
  key_a INT,
  key_b INT,
  threshold INT,
  UNIQUE (key_a, key_b) NOT ENFORCED
);

-- @setup
CREATE TABLE subquery_correlated_unique_inner (
  key_a INT,
  key_b INT,
  amount INT
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

-- @setup
INSERT INTO subquery_scalar_window_input VALUES
  (1, -5), (2, 10), (3, 20), (4, NULL), (5, 100000);

-- @setup
INSERT INTO subquery_correlated_unique_outer VALUES
  (1, 1, 10),
  (2, 2, 10),
  (NULL, 1, 10),
  (NULL, 1, 20);

-- @setup
INSERT INTO subquery_correlated_unique_inner VALUES
  (1, 1, 4),
  (1, 1, 5),
  (2, 2, 20),
  (NULL, 1, 100);

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

-- 6. Nullable UNIQUE keys are safe only through null-rejecting equality;
-- legal duplicate NULL tuples must not manufacture a GROUP BY dependency.
-- @query
SELECT o.key_a, o.key_b, o.threshold
FROM subquery_correlated_unique_outer AS o
WHERE o.threshold > (
  SELECT SUM(i.amount)
  FROM subquery_correlated_unique_inner AS i
  WHERE i.key_a = o.key_a
    AND i.key_b = o.key_b
)
ORDER BY o.key_a, o.key_b, o.threshold;

-- 7. A subset-filtered scalar aggregate is shared with the detail scan as a
-- full-partition window. Result semantics cover computed FILTER inputs and
-- the empty/NULL aggregate domain independently of the plan-shape unit test.
-- @query
SELECT c.id, c.balance
FROM subquery_scalar_window_input AS c
WHERE c.balance < 100000
  AND c.balance > (
    SELECT AVG(s.balance)
    FROM subquery_scalar_window_input AS s
    WHERE s.balance < 100000 AND 0 < s.balance
  )
ORDER BY c.id;

-- @teardown
DROP TABLE IF EXISTS subquery_correlated_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_correlated_detail;

-- @teardown
DROP TABLE IF EXISTS subquery_scalar_window_input;

-- @teardown
DROP TABLE IF EXISTS subquery_correlated_unique_outer;

-- @teardown
DROP TABLE IF EXISTS subquery_correlated_unique_inner;
