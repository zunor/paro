-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Native UNION ALL writes retain shared state, row counts and COPY contents across branches.

CREATE TABLE union_sink_source(id INTEGER);

CREATE TABLE union_sink_target(id INTEGER);

CREATE TABLE union_sink_copy(id INTEGER);

INSERT INTO union_sink_source SELECT i FROM generate_series(1, 1025) t(i);

INSERT INTO union_sink_target SELECT id FROM union_sink_source WHERE id <= 512 UNION ALL SELECT id FROM union_sink_source WHERE id > 512 UNION ALL SELECT id FROM union_sink_source WHERE id IN (1, 1025) UNION ALL SELECT id FROM union_sink_source WHERE id < 0;

SELECT count(*) AS n, sum(id) AS total FROM union_sink_target;

COPY (SELECT id FROM union_sink_source WHERE id <= 2 UNION ALL SELECT id FROM union_sink_source WHERE id >= 1024) TO '/tmp/paro_union_shared_sink.csv' WITH (FORMAT csv);

COPY union_sink_copy FROM '/tmp/paro_union_shared_sink.csv' WITH (FORMAT csv);

SELECT id FROM union_sink_copy ORDER BY id;

INSERT INTO union_sink_target SELECT id FROM union_sink_source WHERE id < 0 UNION ALL SELECT id FROM union_sink_source WHERE id > 1025;

SELECT count(*) AS n FROM union_sink_target;

CREATE TABLE union_sink_atomic(id INTEGER);

INSERT INTO union_sink_atomic VALUES (0);

-- @statement error SQLSTATE=22P02
INSERT INTO union_sink_atomic SELECT id FROM union_sink_source UNION ALL SELECT (CASE WHEN id = 1025 THEN id::VARCHAR || 'x' ELSE id::VARCHAR END)::INTEGER FROM union_sink_source;

SELECT count(*) AS n, sum(id) AS total FROM union_sink_atomic;

DROP TABLE union_sink_atomic;

DROP TABLE union_sink_source;

DROP TABLE union_sink_target;

DROP TABLE union_sink_copy;

-- @teardown
DROP TABLE IF EXISTS union_sink_copy;

-- @teardown
DROP TABLE IF EXISTS union_sink_target;

-- @teardown
DROP TABLE IF EXISTS union_sink_source;

-- @teardown
DROP TABLE IF EXISTS union_sink_atomic;
