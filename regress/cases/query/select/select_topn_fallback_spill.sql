# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS topn_fallback_spill_case;

-- @setup
CREATE TABLE topn_fallback_spill_case(id INT, score INT);

INSERT INTO topn_fallback_spill_case
SELECT g, 7001 - g
FROM generate_series(1, 7000) AS t(g);

EXPLAIN
SELECT id
FROM topn_fallback_spill_case
ORDER BY score DESC
LIMIT 3;

SELECT id
FROM topn_fallback_spill_case
ORDER BY score DESC
LIMIT 3;

EXPLAIN
SELECT id
FROM topn_fallback_spill_case
ORDER BY id
LIMIT 5 OFFSET 6000;

SELECT id
FROM topn_fallback_spill_case
ORDER BY id
LIMIT 5 OFFSET 6000;

SET temp_directory = '/tmp/paro_regress_topn_spill';
SET force_external = true;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT id
FROM topn_fallback_spill_case
ORDER BY score DESC
LIMIT 3;

SELECT id
FROM topn_fallback_spill_case
ORDER BY score DESC
LIMIT 3;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

-- @teardown
DROP TABLE IF EXISTS topn_fallback_spill_case;
