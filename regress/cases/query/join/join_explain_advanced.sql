-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS join_explain_piecewise_l;

-- @setup
DROP TABLE IF EXISTS join_explain_piecewise_r;

-- @setup
DROP TABLE IF EXISTS join_explain_ie_l;

-- @setup
DROP TABLE IF EXISTS join_explain_ie_r;

-- @setup
DROP TABLE IF EXISTS join_explain_hash_l;

-- @setup
DROP TABLE IF EXISTS join_explain_hash_r;

-- @setup
DROP TABLE IF EXISTS join_explain_spill_l;

-- @setup
DROP TABLE IF EXISTS join_explain_spill_r;

-- @setup
CREATE TABLE join_explain_piecewise_l (id INT);

-- @setup
CREATE TABLE join_explain_piecewise_r (id INT);

INSERT INTO join_explain_piecewise_l VALUES (1), (3), (5);

INSERT INTO join_explain_piecewise_r VALUES (2), (4), (6);

EXPLAIN
SELECT l.id, r.id
FROM join_explain_piecewise_l AS l
JOIN join_explain_piecewise_r AS r ON l.id < r.id;

EXPLAIN (VERBOSE)
SELECT l.id, r.id
FROM join_explain_piecewise_l AS l
JOIN join_explain_piecewise_r AS r ON l.id < r.id;

-- @query json
EXPLAIN
SELECT l.id, r.id
FROM join_explain_piecewise_l AS l
JOIN join_explain_piecewise_r AS r ON l.id < r.id
FORMAT JSON;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.id, r.id
FROM join_explain_piecewise_l AS l
JOIN join_explain_piecewise_r AS r ON l.id < r.id;

-- @query json
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.id, r.id
FROM join_explain_piecewise_l AS l
JOIN join_explain_piecewise_r AS r ON l.id < r.id
FORMAT JSON;

-- @setup
CREATE TABLE join_explain_ie_l (x INT);

-- @setup
CREATE TABLE join_explain_ie_r (lo INT, hi INT);

INSERT INTO join_explain_ie_l VALUES (2), (5), (9);

INSERT INTO join_explain_ie_r VALUES (1, 3), (4, 6), (7, 10);

EXPLAIN
SELECT l.x, r.lo, r.hi
FROM join_explain_ie_l AS l
JOIN join_explain_ie_r AS r ON l.x BETWEEN r.lo AND r.hi;

EXPLAIN (VERBOSE)
SELECT l.x, r.lo, r.hi
FROM join_explain_ie_l AS l
JOIN join_explain_ie_r AS r ON l.x BETWEEN r.lo AND r.hi;

-- @query json
EXPLAIN
SELECT l.x, r.lo, r.hi
FROM join_explain_ie_l AS l
JOIN join_explain_ie_r AS r ON l.x BETWEEN r.lo AND r.hi
FORMAT JSON;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.x, r.lo, r.hi
FROM join_explain_ie_l AS l
JOIN join_explain_ie_r AS r ON l.x BETWEEN r.lo AND r.hi;

-- @query json
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.x, r.lo, r.hi
FROM join_explain_ie_l AS l
JOIN join_explain_ie_r AS r ON l.x BETWEEN r.lo AND r.hi
FORMAT JSON;

-- @setup
CREATE TABLE join_explain_hash_l (k INT, payload TEXT);

-- @setup
CREATE TABLE join_explain_hash_r (k INT, payload TEXT);

INSERT INTO join_explain_hash_l VALUES
  (5, 'l5'),
  (10, 'l10'),
  (15, 'l15'),
  (20, 'l20'),
  (NULL, 'lnull');

INSERT INTO join_explain_hash_r VALUES
  (10, 'r10'),
  (15, 'r15');

EXPLAIN (VERBOSE)
SELECT l.k, r.payload
FROM join_explain_hash_l AS l
LEFT JOIN join_explain_hash_r AS r ON l.k = r.k;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.k, r.payload
FROM join_explain_hash_l AS l
LEFT JOIN join_explain_hash_r AS r ON l.k = r.k;

-- @query json
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.k, r.payload
FROM join_explain_hash_l AS l
LEFT JOIN join_explain_hash_r AS r ON l.k = r.k
FORMAT JSON;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.k
FROM join_explain_hash_l AS l
SEMI JOIN join_explain_hash_r AS r ON l.k IS NOT DISTINCT FROM r.k;

-- @query json
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.k
FROM join_explain_hash_l AS l
SEMI JOIN join_explain_hash_r AS r ON l.k IS NOT DISTINCT FROM r.k
FORMAT JSON;

-- @setup
CREATE TABLE join_explain_spill_l (k INT);

-- @setup
CREATE TABLE join_explain_spill_r (k INT);

INSERT INTO join_explain_spill_l
SELECT g
FROM generate_series(1, 512) AS t(g);

INSERT INTO join_explain_spill_r
SELECT g
FROM generate_series(1, 512) AS t(g);

SET temp_directory = '/tmp/paro_regress_join_explain_advanced';
SET max_temp_directory_size = '256MB';
SET force_external = true;
SET threads = 1;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.k
FROM join_explain_spill_l AS l
LEFT JOIN join_explain_spill_r AS r ON l.k = r.k;

-- @query json
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT l.k
FROM join_explain_spill_l AS l
LEFT JOIN join_explain_spill_r AS r ON l.k = r.k
FORMAT JSON;

SET force_external = DEFAULT;
SET threads = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;

-- @teardown
DROP TABLE IF EXISTS join_explain_piecewise_l;

-- @teardown
DROP TABLE IF EXISTS join_explain_piecewise_r;

-- @teardown
DROP TABLE IF EXISTS join_explain_ie_l;

-- @teardown
DROP TABLE IF EXISTS join_explain_ie_r;

-- @teardown
DROP TABLE IF EXISTS join_explain_hash_l;

-- @teardown
DROP TABLE IF EXISTS join_explain_hash_r;

-- @teardown
DROP TABLE IF EXISTS join_explain_spill_l;

-- @teardown
DROP TABLE IF EXISTS join_explain_spill_r;
