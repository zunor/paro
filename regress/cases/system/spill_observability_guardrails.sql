-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS spill_h_sort;

-- @setup
DROP TABLE IF EXISTS spill_h_agg;

-- @setup
DROP TABLE IF EXISTS spill_h_join_l;

-- @setup
DROP TABLE IF EXISTS spill_h_join_r;

-- @setup
CREATE TABLE spill_h_sort(id INT, payload INT);

INSERT INTO spill_h_sort
SELECT g, g
FROM generate_series(1, 10000) AS t(g);

-- @setup
CREATE TABLE spill_h_agg(k1 INT, k2 INT, v INT);

INSERT INTO spill_h_agg
SELECT g, g % 257, 1
FROM generate_series(1, 200000) AS t(g);

-- @setup
CREATE TABLE spill_h_join_l(id INT);

-- @setup
CREATE TABLE spill_h_join_r(id INT);

INSERT INTO spill_h_join_l
SELECT g
FROM generate_series(1, 1000) AS t(g);

INSERT INTO spill_h_join_r
SELECT g
FROM generate_series(1, 1000) AS t(g);

SET temp_directory = '/tmp/paro_regress_spill_h';

SET force_external = true;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE SELECT id FROM spill_h_sort ORDER BY id DESC;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT k1, k2, SUM(v)
FROM spill_h_agg
GROUP BY k1, k2;

SET force_external = DEFAULT;

EXPLAIN SELECT * FROM spill_h_join_l l SEMI JOIN spill_h_join_r r ON l.id = r.id;

SET temp_directory = DEFAULT;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE SELECT id FROM spill_h_sort ORDER BY id DESC;

SET force_external = true;

-- @statement error SQLSTATE=53200
EXPLAIN ANALYZE SELECT id FROM spill_h_sort ORDER BY id DESC;

SET force_external = DEFAULT;

SET max_temp_directory_size = DEFAULT;
SET memory_limit = DEFAULT;

-- @teardown
DROP TABLE IF EXISTS spill_h_sort;

-- @teardown
DROP TABLE IF EXISTS spill_h_agg;

-- @teardown
DROP TABLE IF EXISTS spill_h_join_l;

-- @teardown
DROP TABLE IF EXISTS spill_h_join_r;
