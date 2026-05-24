-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- End-to-end coverage for property repair lowering.

-- @setup
DROP TABLE IF EXISTS property_repair_left;

-- @setup
DROP TABLE IF EXISTS property_repair_right;

-- @setup
CREATE TABLE property_repair_left (
  id INT,
  sort_key INT,
  label TEXT
);

-- @setup
CREATE TABLE property_repair_right (
  id INT,
  payload TEXT
);

-- @setup
INSERT INTO property_repair_left VALUES
  (1, 30, 'left-c'),
  (2, 10, 'left-a'),
  (3, 20, 'left-b'),
  (4, 40, 'left-d');

-- @setup
INSERT INTO property_repair_right VALUES
  (1, 'right-c'),
  (2, 'right-a'),
  (3, 'right-b'),
  (5, 'right-e');

-- Join output is unordered; ORDER BY must lower to SortBuild + SortEmit roles.
-- @normalize explain_operator_timing,explain_summary_timing
-- @query
EXPLAIN ANALYZE
SELECT l.label, r.payload
FROM property_repair_left AS l
LEFT JOIN property_repair_right AS r ON l.id = r.id
ORDER BY l.sort_key;

-- @query
SELECT l.label, r.payload
FROM property_repair_left AS l
LEFT JOIN property_repair_right AS r ON l.id = r.id
ORDER BY l.sort_key;

-- @teardown
DROP TABLE IF EXISTS property_repair_left;

-- @teardown
DROP TABLE IF EXISTS property_repair_right;
