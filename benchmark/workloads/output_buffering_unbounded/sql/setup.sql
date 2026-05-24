-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Setup for unbounded output path verification.
-- Tests EXPLAIN ANALYZE, recursive CTE, and DML paths that use
-- QueryOutputPort::unbounded() or completed-output fallback.

DROP TABLE IF EXISTS unbuf_scan;
DROP TABLE IF EXISTS unbuf_join_l;
DROP TABLE IF EXISTS unbuf_join_r;
DROP TABLE IF EXISTS unbuf_sink;

CREATE TABLE unbuf_scan(id INT, v1 INT, v2 INT, payload VARCHAR);
INSERT INTO unbuf_scan
SELECT g, g % 1000, g * 7, 'row_' || CAST(g AS VARCHAR)
FROM generate_series(1, ${scan_rows}) AS t(g);

CREATE TABLE unbuf_join_l(id INT, fk INT);
INSERT INTO unbuf_join_l
SELECT g, g % 5000
FROM generate_series(1, ${scan_rows}) AS t(g);

CREATE TABLE unbuf_join_r(id INT, val INT);
INSERT INTO unbuf_join_r
SELECT g, g * 3
FROM generate_series(1, 5000) AS t(g);

CREATE TABLE unbuf_sink(id INT, v INT);
