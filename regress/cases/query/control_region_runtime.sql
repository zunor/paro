-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- End-to-end coverage for synchronous completed-output control regions.

-- @setup
DROP TABLE IF EXISTS control_region_outer;

-- @setup
DROP TABLE IF EXISTS control_region_lookup;

-- @setup
CREATE TABLE control_region_outer (
  id INT,
  grp INT,
  amt INT
);

-- @setup
CREATE TABLE control_region_lookup (
  grp INT,
  quota INT
);

-- @setup
INSERT INTO control_region_outer VALUES
  (1, 10, 5),
  (2, 10, 9),
  (3, 20, 3),
  (4, NULL, 7),
  (5, 30, 8);

-- @setup
INSERT INTO control_region_lookup VALUES
  (10, 5),
  (10, 6),
  (20, 3),
  (30, 7);

-- 1. Root recursive CTE runs as a synchronous control-region root.
-- @query
WITH RECURSIVE root_counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM root_counter WHERE n < 4
)
SELECT n FROM root_counter;

-- 2. Embedded recursive CTE feeds a parent pipeline.
-- @query
WITH RECURSIVE embedded_counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM embedded_counter WHERE n < 6
)
SELECT n FROM embedded_counter ORDER BY n DESC LIMIT 3;

-- 3. Correlated EXISTS lowers to a delim/correlated control region.
-- @query
SELECT
  o.id,
  EXISTS(
    SELECT 1
    FROM control_region_lookup AS l
    WHERE l.grp = o.grp
      AND l.quota >= o.amt
  ) AS has_quota
FROM control_region_outer AS o
ORDER BY o.id;

-- @teardown
DROP TABLE IF EXISTS control_region_outer;

-- @teardown
DROP TABLE IF EXISTS control_region_lookup;
