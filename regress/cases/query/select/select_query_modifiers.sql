-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Query modifiers belong to the query result, independent of the body kind.

-- VALUES honors ORDER BY, LIMIT, and OFFSET.
VALUES (3), (1), (2) ORDER BY 1 LIMIT 1 OFFSET 1;

-- Set-operation output names are visible to ORDER BY.
SELECT 4 AS n
UNION ALL SELECT 2
UNION ALL SELECT 3
ORDER BY n DESC
LIMIT 1 OFFSET 1;

-- Parenthesized query modifiers stay on the inner set operand.
-- @query nosort
(SELECT 2 AS n UNION ALL SELECT 1 ORDER BY n LIMIT 1)
UNION ALL SELECT 3
ORDER BY n;

-- Hidden sort columns are pruned at a parenthesized query boundary.
-- @query nosort
(SELECT n
 FROM (VALUES (2, 20), (1, 10)) AS t(n, hidden)
 ORDER BY hidden
 LIMIT 1)
UNION ALL SELECT 3
ORDER BY n;

-- Ordinary DISTINCT is sorted after duplicate removal.
SELECT DISTINCT n
FROM (VALUES (2), (1), (2)) AS t(n)
ORDER BY n ASC;

-- A non-DISTINCT SELECT may retain a hidden sort expression until sorting.
SELECT n
FROM (VALUES (3), (1), (2)) AS t(n)
ORDER BY n + 10
LIMIT 2;

-- DISTINCT and set operations cannot add hidden ORDER BY expressions.
SELECT DISTINCT n
FROM (VALUES (2, 20), (1, 10)) AS t(n, hidden)
ORDER BY hidden;

SELECT 1 AS n
UNION ALL SELECT 2
ORDER BY n + 1;
